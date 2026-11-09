#!/usr/bin/env python3
"""Flink ingest over a time window, from the tables' own commit history (read-only).

  ingest_stats.py --bucket B --iceberg-db D --paimon-db D --out <json>
                  (--pins <hot-interleave-*.pins> | --before <STAMP> | --from T --to T)
                  [--monitor-prefix querybench-results/<session>/monitor/]
                  [--rds-host H] [--flink-manifests A B] [--generator-manifest M]

Window: --pins = the hot suite's first pin commit minus 5 min to its last pin
commit; --before <STAMP> = the 30 min ending at the writers' last commit before
STAMP (the snapshot suite: the writers' last window before they were
suspended); or --from / --to (ISO UTC or epoch ms).

Per format and table, commits with a commit time in (from, to]:
  Iceberg  the current metadata.json (Glue parameter metadata_location):
           write commits (every operation but `replace`): rows (added-records),
           data files, delete files, equality / position deletes, DVs
           (added-dvs); `replace` = compaction, counted apart and never in the
           write numbers. State at the window end: the last snapshot's totals.
  Paimon   snapshot/snapshot-N, EARLIEST..LATEST: APPEND commits: rows
           (deltaRecordCount), data files (_NUM_ADDED_FILES of the delta
           manifest list); COMPACT commits apart, with the deletion-vector
           index files they wrote (index files new in a snapshot's index
           manifest). State: the last snapshot's totalRecordCount, live data
           files and DV index files. File counts need fastavro + zstandard
           (run.sh runs this through `uv` with them); without them they are null.
Plus, all best-effort: the job monitor's reports whose sampling window overlaps
this one (per format: median and max of its throughput rows/s and bytes/s,
median checkpoint duration, max backpressure share), RDS class and CPUUtilization, the writers'
parallelism and TaskManagers, the generator's settings.
A failed part is recorded under "errors"; the JSON is always written.
"""
import argparse
import concurrent.futures as cf
import datetime as dt
import io
import json
import os
import re
import statistics
import subprocess
import sys

import boto3

try:
    import fastavro
except ImportError:          # file counts on Paimon then stay null
    fastavro = None

TABLES = ["customers", "products", "orders", "order_items"]
REGION = os.environ.get("AWS_REGION", "us-west-2")
s3 = boto3.client("s3", region_name=REGION)
ERRORS = []


def ms_of(v):
    if re.fullmatch(r"\d{12,}", str(v)):
        return int(v)
    return int(dt.datetime.fromisoformat(str(v).replace("Z", "+00:00").replace(" ", "T"))
               .replace(tzinfo=dt.timezone.utc).timestamp() * 1000)


def iso(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_json(bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def avro(bucket, key):
    return list(fastavro.reader(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read())))


def median_interval_s(ts):
    ts = sorted(ts)
    d = [(b - a) / 1000.0 for a, b in zip(ts, ts[1:])]
    return round(statistics.median(d), 1) if d else None


# ── Iceberg ────────────────────────────────────────────────────────────────
def iceberg_table(db, t, t0, t1):
    loc = boto3.client("glue", region_name=REGION).get_table(DatabaseName=db, Name=t)["Table"]["Parameters"]["metadata_location"]
    b, _, k = loc[len("s3://"):].partition("/")
    snaps = sorted(get_json(b, k).get("snapshots") or [], key=lambda s: s["timestamp-ms"])
    n = lambda s, key: int((s.get("summary") or {}).get(key) or 0)
    inside = [s for s in snaps if t0 < s["timestamp-ms"] <= t1]
    writes = [s for s in inside if (s.get("summary") or {}).get("operation") != "replace"]
    comp = [s for s in inside if (s.get("summary") or {}).get("operation") == "replace"]
    end = [s for s in snaps if s["timestamp-ms"] <= t1]
    last = end[-1] if end else None
    return {
        "write_commits": len(writes),
        "median_commit_interval_s": median_interval_s([s["timestamp-ms"] for s in writes]),
        "rows": sum(n(s, "added-records") for s in writes),
        "bytes": sum(n(s, "added-files-size") for s in writes),     # data + delete files
        "data_files": sum(n(s, "added-data-files") for s in writes),
        "delete_files": sum(n(s, "added-delete-files") for s in writes),
        "equality_deletes": sum(n(s, "added-equality-deletes") for s in writes),
        "position_deletes": sum(n(s, "added-position-deletes") for s in writes),
        "dvs": sum(n(s, "added-dvs") for s in writes),
        "compaction_commits": len(comp),
        "compaction_removed_delete_files": sum(n(s, "removed-delete-files") for s in comp),
        "first_commit": iso(inside[0]["timestamp-ms"]) if inside else None,
        "last_commit": iso(inside[-1]["timestamp-ms"]) if inside else None,
        "state": None if not last else {
            "snapshot_time": iso(last["timestamp-ms"]),
            "records": n(last, "total-records"), "data_files": n(last, "total-data-files"),
            "delete_files": n(last, "total-delete-files"), "equality_deletes": n(last, "total-equality-deletes"),
            "position_deletes": n(last, "total-position-deletes"), "bytes": n(last, "total-files-size"),
            "snapshots_retained": len(snaps)},
    }


def iceberg_last_write_ms(db, before_ms):
    out = 0
    for t in TABLES:
        loc = boto3.client("glue", region_name=REGION).get_table(DatabaseName=db, Name=t)["Table"]["Parameters"]["metadata_location"]
        b, _, k = loc[len("s3://"):].partition("/")
        for s in get_json(b, k).get("snapshots") or []:
            if s["timestamp-ms"] < before_ms and (s.get("summary") or {}).get("operation") != "replace":
                out = max(out, s["timestamp-ms"])
    return out


# ── Paimon ─────────────────────────────────────────────────────────────────
def paimon_snapshots(bucket, db, t):
    pre = f"paimon-warehouse/{db}.db/{t}/snapshot/"
    hi = int(s3.get_object(Bucket=bucket, Key=pre + "LATEST")["Body"].read())
    try:
        lo = int(s3.get_object(Bucket=bucket, Key=pre + "EARLIEST")["Body"].read())
    except s3.exceptions.NoSuchKey:
        # Paimon writes the EARLIEST hint only when it expires a snapshot; a
        # write-only table (no inline expiry) has none: the lowest listed id.
        ids = [int(o["Key"].rsplit("-", 1)[1])
               for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre + "snapshot-")
               for o in p.get("Contents", [])]
        lo = min(ids) if ids else hi

    def one(i):
        try:
            return get_json(bucket, f"{pre}snapshot-{i}")
        except Exception:
            return None          # expired between the listing and the read
    with cf.ThreadPoolExecutor(16) as ex:
        return [s for s in ex.map(one, range(lo, hi + 1)) if s]


def paimon_table(bucket, db, t, t0, t1):
    pre = f"paimon-warehouse/{db}.db/{t}/manifest/"
    snaps = paimon_snapshots(bucket, db, t)
    inside = [s for s in snaps if t0 < s["timeMillis"] <= t1]
    app = [s for s in inside if s.get("commitKind") == "APPEND"]
    comp = [s for s in inside if s.get("commitKind") == "COMPACT"]
    end = [s for s in snaps if s["timeMillis"] <= t1]
    last = end[-1] if end else None
    out = {
        "write_commits": len(app),
        "median_commit_interval_s": median_interval_s([s["timeMillis"] for s in app]),
        "rows": sum(int(s.get("deltaRecordCount") or 0) for s in app),
        "bytes": None,
        "data_files": None, "delete_files": None, "equality_deletes": 0, "position_deletes": None, "dvs": None,
        "compaction_commits": len(comp),
        "compaction_dv_index_files": None,
        "first_commit": iso(inside[0]["timeMillis"]) if inside else None,
        "last_commit": iso(inside[-1]["timeMillis"]) if inside else None,
        "state": None if not last else {
            "snapshot_time": iso(last["timeMillis"]), "records": int(last.get("totalRecordCount") or 0),
            "data_files": None, "dv_index_files": None, "dv_deleted_rows": None,
            "snapshots_retained": len(snaps)},
    }
    if fastavro is None:
        return out
    try:
        paimon_files(bucket, pre, snaps, inside, app, last, out)
    except Exception as e:      # rows and commits stand; the file counts stay null
        ERRORS.append("paimon %s file counts: %s" % (t, str(e)[:200]))
    return out


def paimon_files(bucket, pre, snaps, inside, app, last, out):
    # Data files the APPEND commits added: their delta manifest lists' counters.
    lists = [avro(bucket, pre + s["deltaManifestList"]) for s in app if s.get("deltaManifestList")]
    out["data_files"] = sum(m["_NUM_ADDED_FILES"] for ml in lists for m in ml)
    # Bytes the APPEND commits added: the files' sizes in their delta manifests.
    with cf.ThreadPoolExecutor(8) as ex:
        out["bytes"] = sum(ex.map(lambda name: sum(e["_FILE"]["_FILE_SIZE"] for e in avro(bucket, pre + name)
                                                   if e["_KIND"] == 0),
                                  [m["_FILE_NAME"] for ml in lists for m in ml]))
    # DV index files: names new in each snapshot's index manifest (only
    # compactions write DVs on these tables; APPEND commits never do).
    cache = {}

    def dv_entries(s):
        im = s.get("indexManifest")
        if not im:
            return {}
        if im not in cache:
            cache[im] = {e["_FILE_NAME"]: e for e in avro(bucket, pre + im)
                         if e["_KIND"] == 0 and e["_INDEX_TYPE"] == "DELETION_VECTORS"}
        return cache[im]
    dv_names = lambda s: set(dv_entries(s))
    ids = {s["id"]: s for s in snaps}
    new_dv = 0
    for s in inside:
        prev = ids.get(s["id"] - 1)
        if prev is not None and s.get("indexManifest") != prev.get("indexManifest"):
            new_dv += len(dv_names(s) - dv_names(prev))
    out["compaction_dv_index_files"] = new_dv
    out["delete_files"] = new_dv
    if last:
        out["state"]["data_files"] = sum(m["_NUM_ADDED_FILES"] - m["_NUM_DELETED_FILES"]
                                         for ml in (last.get("baseManifestList"), last.get("deltaManifestList")) if ml
                                         for m in avro(bucket, pre + ml))
        out["state"]["dv_index_files"] = len(dv_names(last))
        # Rows the live DVs mark deleted (the DV analogue of Iceberg's position deletes).
        out["state"]["dv_deleted_rows"] = sum(r.get("_CARDINALITY") or 0 for e in dv_entries(last).values()
                                              for r in (e.get("_DELETIONS_VECTORS_RANGES") or []))


def paimon_last_append_ms(bucket, db, before_ms):
    out = 0
    for t in TABLES:
        for s in paimon_snapshots(bucket, db, t):
            if s["timeMillis"] < before_ms and s.get("commitKind") == "APPEND":
                out = max(out, s["timeMillis"])
    return out


# ── Optional context ───────────────────────────────────────────────────────
def monitor_reports(bucket, prefix, t0, t1):
    """The job monitor's reports (monitoring/, copied to <prefix>) whose sampling
    window [start_time, end_time] overlaps [t0, t1], aggregated per format:
    median and max of its throughput rows/s and bytes/s. The file name carries the
    save time (= the window end), so only names in [t0, t1 + 15 min] are read."""
    got = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            m = re.search(r"(paimon|iceberg)-cdc_(\d{8}_\d{6})\.json$", o["Key"])
            if not m:
                continue
            saved = int(dt.datetime.strptime(m.group(2), "%Y%m%d_%H%M%S").replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
            if not t0 <= saved <= t1 + 900_000:
                continue
            r = get_json(bucket, o["Key"])
            st = ms_of(r["start_time"]) if r.get("start_time") else saved
            en = ms_of(r["end_time"]) if r.get("end_time") else saved
            if st <= t1 and en >= t0 and not (r.get("errors") and not r.get("job_name")):
                got.setdefault(m.group(1), []).append(r)
    res = {}
    for fmt, rs in got.items():
        rs.sort(key=lambda r: r.get("start_time") or "")

        def vals(k, pos=True):
            v = [r.get(k) for r in rs if isinstance(r.get(k), (int, float))]
            return [x for x in v if x > 0] if pos else [x for x in v if x >= 0]
        med = lambda v: round(statistics.median(v)) if v else None
        res[fmt] = {
            "reports": len(rs), "first_start": rs[0].get("start_time"), "last_end": rs[-1].get("end_time"),
            "sampled_s": round(sum(r.get("duration_seconds") or 0 for r in rs)),
            # ingest rows/s = the sources' rate meter (the monitor's headline
            # "Throughput (rows/s)"; older reports' headline was committed rows/s,
            # so read the meter key, present in both), and committed bytes/s
            "rows_per_s_median": med(vals("meter_records_per_sec")),
            "rows_per_s_max": max(vals("meter_records_per_sec"), default=None),
            "rows_total": sum(vals("total_records_processed")) or None,   # rows the sources emitted, over the reports
            "committed_rows_per_s_median": med(vals("commit_records_per_sec")),
            "bytes_per_s_median": med(vals("throughput_bytes_per_sec")),
            "bytes_per_s_max": max(vals("throughput_bytes_per_sec"), default=None),
            "checkpoint_ms_median": med(vals("avg_checkpoint_duration_ms")),
            "backpressured_ratio_max": max(vals("backpressured_sample_ratio", pos=False), default=None),
        }
    for fmt in ("paimon", "iceberg"):
        if fmt not in res:
            res[fmt] = {"reports": 0}
    return res


def rds_info(host, t0, t1):
    ident = host.split(".")[0]
    i = boto3.client("rds", region_name=REGION).describe_db_instances(DBInstanceIdentifier=ident)["DBInstances"][0]
    cw = boto3.client("cloudwatch", region_name=REGION).get_metric_statistics(
        Namespace="AWS/RDS", MetricName="CPUUtilization", Dimensions=[{"Name": "DBInstanceIdentifier", "Value": ident}],
        StartTime=dt.datetime.fromtimestamp(t0 / 1000, dt.timezone.utc), EndTime=dt.datetime.fromtimestamp(t1 / 1000, dt.timezone.utc),
        Period=60, Statistics=["Average", "Maximum"])["Datapoints"]
    return {"id": ident, "class": i["DBInstanceClass"], "engine": "%s %s" % (i["Engine"], i["EngineVersion"]),
            "storage": "%s %s IOPS / %s GB" % (i["StorageType"], i.get("Iops", "?"), i["AllocatedStorage"]),
            "cpu_avg": round(statistics.mean(d["Average"] for d in cw), 1) if cw else None,
            "cpu_max": round(max(d["Maximum"] for d in cw), 1) if cw else None}


def flink_info(path):
    y = open(path).read()
    g = lambda rx: (re.search(rx, y, re.M) or [None, None])[1]
    par, slots = g(r"^\s+parallelism:\s*(\d+)"), g(r'taskmanager\.numberOfTaskSlots:\s*"?(\d+)')
    tm = re.search(r"taskManager:\s*\n\s+resource:\s*\n\s+memory:\s*(\S+)\s*\n\s+cpu:\s*(\S+)", y)
    return {"parallelism": int(par) if par else None, "slots_per_tm": int(slots) if slots else None,
            "tms": -(-int(par) // int(slots)) if par and slots else None,
            "tm_cpu": tm.group(2) if tm else None, "tm_memory": tm.group(1) if tm else None,
            "checkpoint_interval": g(r'execution\.checkpointing\.interval:\s*"?([^"\s]+)')}


def generator_info(manifest, pins):
    env = {}
    try:   # the live StatefulSet first (read-only), else the manifest
        sts = json.loads(subprocess.run(["kubectl", "get", "statefulset", "mysql-upsert-loadgen", "-n",
                                         os.environ.get("NAMESPACE", "emr-flink"), "-o", "json"],
                                        capture_output=True, text=True, timeout=20, check=True).stdout)
        env = {e["name"]: e.get("value") for e in sts["spec"]["template"]["spec"]["containers"][0].get("env", [])}
        src = "statefulset"
    except Exception:
        y = open(manifest).read()
        env = dict(re.findall(r"- name: ([A-Z_]+)\s*\n\s+value: \"?([^\"\n]*)\"?", y))
        src = "manifest"
    out = {k: env.get(k) for k in ("RATE_PER_POD", "THREADS", "BATCH_ROWS", "UPSERT_RATIO", "RECENT_KEY_RATIO")}
    out["source"] = src
    m = re.search(r"generator_before=\S*?=(\d+)/(\d+)", open(pins).read()) if pins else None
    out["replicas"] = int(m.group(1)) if m else None
    return out


def best_effort(name, fn, *a):
    try:
        return fn(*a)
    except Exception as e:
        ERRORS.append("%s: %s" % (name, str(e)[:300]))
        return None


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--iceberg-db", required=True)
    ap.add_argument("--paimon-db", required=True)
    ap.add_argument("--pins")
    ap.add_argument("--before")
    ap.add_argument("--from", dest="t_from")
    ap.add_argument("--to", dest="t_to")
    ap.add_argument("--lead-s", type=int, default=300, help="--pins: seconds before the first pin (300)")
    ap.add_argument("--monitor-prefix")
    ap.add_argument("--rds-host")
    ap.add_argument("--flink-manifests", nargs="*", default=[])
    ap.add_argument("--generator-manifest")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    res = {"window": None, "iceberg": {}, "paimon": {}, "config": {}, "monitor": None, "errors": ERRORS,
           "file_counts": fastavro is not None}
    try:
        if a.pins:
            c = [int(x) for x in re.findall(r"QB_PIN_COMMIT_MS_\w+=(\d+)", open(a.pins).read())]
            if not c:
                raise ValueError("no QB_PIN_COMMIT_MS_ lines in %s" % a.pins)
            t0, t1, how = min(c) - a.lead_s * 1000, max(c), "hot: first pin commit - %d s .. last pin commit" % a.lead_s
        elif a.before:
            b = ms_of(dt.datetime.strptime(a.before, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%dT%H:%M:%S"))
            t1 = max(iceberg_last_write_ms(a.iceberg_db, b), paimon_last_append_ms(a.bucket, a.paimon_db, b))
            if not t1:
                raise ValueError("no writer commit before %s" % a.before)
            t0, how = t1 - 1800_000, "snapshot: the writers' last 30 min before %s" % a.before
        else:
            t0, t1, how = ms_of(a.t_from), ms_of(a.t_to), "given"
        res["window"] = {"from": iso(t0), "to": iso(t1), "seconds": round((t1 - t0) / 1000), "how": how}
        with cf.ThreadPoolExecutor(8) as ex:
            fi = {t: ex.submit(best_effort, "iceberg " + t, iceberg_table, a.iceberg_db, t, t0, t1) for t in TABLES}
            fp = {t: ex.submit(best_effort, "paimon " + t, paimon_table, a.bucket, a.paimon_db, t, t0, t1) for t in TABLES}
            res["iceberg"] = {t: f.result() for t, f in fi.items()}
            res["paimon"] = {t: f.result() for t, f in fp.items()}
        if a.monitor_prefix:
            res["monitor"] = best_effort("monitor reports", monitor_reports, a.bucket, a.monitor_prefix, t0, t1)
        if a.rds_host:
            res["config"]["rds"] = best_effort("rds", rds_info, a.rds_host, t0, t1)
    except Exception as e:
        ERRORS.append("window: %s" % e)
    for p in a.flink_manifests:
        fmt = "paimon" if "paimon" in os.path.basename(p) else "iceberg"
        res["config"].setdefault("flink", {})[fmt] = best_effort("flink manifest " + p, flink_info, p)
    if a.generator_manifest:
        res["config"]["generator"] = best_effort("generator", generator_info, a.generator_manifest, a.pins)
    if fastavro is None:
        ERRORS.append("fastavro not installed: Paimon file counts not read")
    with open(a.out, "w") as f:
        json.dump(res, f, indent=2)
    print(a.out + ("" if not ERRORS else " (with %d error(s): %s)" % (len(ERRORS), "; ".join(ERRORS)[:300])), file=sys.stderr)


if __name__ == "__main__":
    main()
