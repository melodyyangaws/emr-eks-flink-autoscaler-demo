#!/usr/bin/env python3
"""
One-off full compaction of ONE CDC table, before the snapshot query test
========================================================================

Run by compaction/run.sh as a batch FlinkDeployment (cdc-compact.yaml), once
per table and format, with the writers STOPPED (after the generator stopped and
the writers committed the binlog tail):

  cdc-compact.py --format paimon|iceberg --table orders \\
      --catalog-sql s3://.../01-catalog-setup.sql --parallelism N --result-uri s3://.../x.json

The pattern is throughput-benchmark/compaction/flink-cid-compact.py's (writers'
own catalog SQL, RowDataRewriter for Iceberg, CALL sys.compact for Paimon, a
result JSON per run). The difference: these are primary-key UPSERT tables with
deletes, so the goal is to apply every delete, not only to merge small files.

- Paimon:  CALL sys.compact(compact_strategy => 'full'): every bucket is merged
  into its top LSM level, which applies the deletion vectors; the commit also
  refreshes Paimon's Iceberg export.
- Iceberg: every data file that carries any delete (equality delete, position
  delete or deletion vector, from FileScanTask.deletes()) or is under 70% of
  write.target-file-size-bytes is rewritten with its deletes applied
  (RowDataRewriter reads through the table's own delete filter), grouped per
  partition into groups of up to the target size (one output file per group).
  One RewriteFiles commit replaces those data files AND removes the delete
  files they carried: with every data file a delete applied to rewritten, those
  delete files apply to nothing any more. Then snapshots older than
  --expire-hours are expired, keeping --retain-last (the tables' history.expire
  rule), which deletes the replaced files. Row lineage (v3 _row_id) is NOT
  carried over: the new files get new row ids at commit.
  Cost: Iceberg's delete filter loads a data file's equality-delete set for each
  file it reads, so memory per subtask is that set (tens of millions of keys on
  a busy table): one slot per TaskManager.

The result (before/after data files, delete files, equality / position deletes,
records, bytes, timings) goes to --result-uri as JSON; a failed run writes its
JSON too, and commits nothing.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import boto3


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg):
    print(f"[{now()}] {msg}", flush=True)


def s3_split(uri):
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def run_catalog_sql(t_env, uri):
    """The writers' 01-catalog-setup.sql: ${VAR} / ${VAR:default} from the
    environment, -- comments dropped, one statement per ';'."""
    bucket, key = s3_split(uri)
    sql = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode()

    def var(m):
        v = os.environ.get(m.group(1), m.group(2))
        if v is None:
            raise ValueError(f"{uri}: ${{{m.group(1)}}} is not set and has no default")
        return v

    sql = re.sub(r"\$\{([A-Za-z0-9_]+)(?::([^}]*))?\}", var, sql)
    sql = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    for stmt in (s.strip() for s in sql.split(";")):
        if stmt:
            log(f"catalog: {' '.join(stmt.split())[:120]}")
            t_env.execute_sql(stmt)


# ── Paimon ──────────────────────────────────────────────────────────────────
def paimon(a, result):
    from pyflink.java_gateway import get_gateway
    from pyflink.table import EnvironmentSettings, TableEnvironment

    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    cfg = t_env.get_config()
    cfg.set("table.dml-sync", "true")          # CALL returns once the compaction job has finished
    cfg.set("parallelism.default", str(a.parallelism))
    run_catalog_sql(t_env, a.catalog_sql)
    db = os.environ["GLUE_DATABASE"]
    J = get_gateway().jvm
    catalog = t_env._j_tenv.getCatalog("paimon_catalog").get().catalog()
    ident = J.org.apache.paimon.catalog.Identifier.create(db, a.table)

    def state():
        # Through the FileStore (py4j does not reach Table's default methods).
        table = catalog.getTable(ident)
        entries = list(table.store().newScan().plan().files())
        files = [e.file() for e in entries]
        return {"snapshot": table.snapshotManager().latestSnapshotId(), "data_files": len(files),
                "bytes": sum(f.fileSize() for f in files), "records_in_files": sum(f.rowCount() for f in files),
                "buckets": len({(str(e.partition()), e.bucket()) for e in entries})}

    result["before"] = state()
    log(f"before: {result['before']}")
    opts = f"sink.parallelism={a.parallelism}"
    log(f"CALL sys.compact on {db}.{a.table} (full, options {opts})")
    t0 = time.time()
    res = t_env.execute_sql(f"CALL sys.compact(`table` => '{db}.{a.table}', compact_strategy => 'full', options => '{opts}')")
    with res.collect() as rows:
        for r in rows:
            log(f"  {r}")
    result["rewrite_s"] = round(time.time() - t0, 1)
    result["after"] = state()
    log(f"after: {result['after']} ({result['rewrite_s']} s)")
    if result["after"]["snapshot"] == result["before"]["snapshot"] and result["before"]["data_files"] > result["before"]["buckets"]:
        raise RuntimeError("sys.compact committed nothing although buckets hold more than one file")


# ── Iceberg ─────────────────────────────────────────────────────────────────
def iceberg(a, result):
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.java_gateway import get_gateway
    from pyflink.table import StreamTableEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(a.parallelism)
    t_env = StreamTableEnvironment.create(env)
    run_catalog_sql(t_env, a.catalog_sql)
    db = os.environ["GLUE_DATABASE"]
    J = get_gateway().jvm
    catalog = t_env._j_tenv.getCatalog("iceberg_catalog").get().catalog()
    table = catalog.loadTable(J.org.apache.iceberg.catalog.TableIdentifier.parse(f"{db}.{a.table}"))
    spec = table.spec()

    def scan(snapshot_id):
        tasks, it = [], table.newScan().useSnapshot(snapshot_id).ignoreResiduals().planFiles().iterator()
        try:
            while it.hasNext():
                tasks.append(it.next())
        finally:
            it.close()
        return tasks

    def stats(tasks):
        deletes = {}
        for t in tasks:
            for d in t.deletes():
                deletes[str(d.location())] = d
        eq = sum(d.recordCount() for d in deletes.values() if str(d.content()) == "EQUALITY_DELETES")
        pos = sum(d.recordCount() for d in deletes.values() if str(d.content()) == "POSITION_DELETES")
        parts = {}
        for t in tasks:
            p = spec.partitionToPath(t.file().partition()) or "(unpartitioned)"
            parts[p] = parts.get(p, 0) + t.file().recordCount()
        return {"data_files": len(tasks), "bytes": sum(t.file().fileSizeInBytes() for t in tasks),
                "records_in_files": sum(t.file().recordCount() for t in tasks),
                "delete_files": len(deletes), "equality_deletes": eq, "position_deletes": pos,
                "records_in_files_per_partition": parts}

    target = int(table.properties().getOrDefault("write.target-file-size-bytes", str(128 * 1024 * 1024)))
    threshold = target // 10 * 7
    start = table.currentSnapshot().snapshotId()
    tasks = scan(start)
    result.update(target_bytes=target, threshold_bytes=threshold, before=dict(stats(tasks), snapshot=start))
    log(f"before: snapshot {start}, { {k: v for k, v in result['before'].items() if k != 'records_in_files_per_partition'} }")

    # What to rewrite: every file a delete applies to, plus small ones; grouped
    # per partition (a group never spans two) up to the target size.
    parts = {}
    for t in tasks:
        if t.deletes().size() > 0 or t.file().fileSizeInBytes() < threshold:
            parts.setdefault(spec.partitionToPath(t.file().partition()), []).append(t)
    groups, old_data, old_deletes = [], J.java.util.HashSet(), {}
    for path, ts in sorted(parts.items()):
        cur, size = [], 0
        for t in sorted(ts, key=lambda x: str(x.file().location())):
            if cur and size + t.file().fileSizeInBytes() > target:
                groups.append(cur); cur, size = [], 0
            cur.append(t); size += t.file().fileSizeInBytes()
        if cur:
            groups.append(cur)
        for t in ts:
            old_data.add(t.file())
            for d in t.deletes():
                old_deletes[str(d.location())] = d
        log(f"  {path or '(unpartitioned)'}: {len(ts)} file(s) to rewrite")
    result.update(groups=len(groups), rewrite_files=old_data.size(), removed_delete_files=len(old_deletes))
    if not groups:
        if result["before"]["delete_files"]:
            raise RuntimeError("deletes present but nothing selected for rewrite")
        result.update(rewrite_s=0, after=result["before"])
        log("nothing to rewrite")
    else:
        jgroups = J.java.util.ArrayList()
        for g in groups:
            lst = J.java.util.ArrayList()
            for t in g:
                lst.add(t)
            jgroups.add(J.org.apache.iceberg.BaseCombinedScanTask(lst))
        par = max(1, min(a.parallelism, len(groups)))
        result["parallelism"] = par
        log(f"rewriting {old_data.size()} data file(s) in {len(groups)} group(s), "
            f"removing {len(old_deletes)} delete file(s), parallelism {par}")
        t0 = time.time()
        ds = env._j_stream_execution_environment.fromCollection(jgroups)
        rewriter = J.org.apache.iceberg.flink.source.RowDataRewriter(table, True, table.io(), table.encryption())
        added = rewriter.rewriteDataForTasks(ds, par)
        result["rewrite_s"] = round(time.time() - t0, 1)
        log(f"rewritten: {added.size()} new file(s) in {result['rewrite_s']} s; committing")
        new_data, del_set, empty = J.java.util.HashSet(), J.java.util.HashSet(), J.java.util.HashSet()
        it = added.iterator()
        while it.hasNext():
            new_data.add(it.next())
        for d in old_deletes.values():
            del_set.add(d)
        # Validated from the planning snapshot: a writer commit in between fails it.
        table.newRewrite().validateFromSnapshot(start).rewriteFiles(old_data, del_set, new_data, empty).commit()
        table.refresh()
        sid = table.currentSnapshot().snapshotId()
        result["after"] = dict(stats(scan(sid)), snapshot=sid)
        log(f"after: snapshot {sid}, { {k: v for k, v in result['after'].items() if k != 'records_in_files_per_partition'} }")
        if result["after"]["delete_files"]:
            raise RuntimeError(f"{result['after']['delete_files']} delete file(s) still apply after the rewrite")

    # Expiry: the tables' history.expire rule; deletes the files no snapshot keeps.
    t1 = time.time()
    older = int(time.time() * 1000) - int(a.expire_hours * 3600 * 1000)
    expired = table.expireSnapshots().expireOlderThan(older).retainLast(a.retain_last).commit()
    table.refresh()
    result.update(expire_s=round(time.time() - t1, 1), snapshots_after_expiry=len(list(table.snapshots())))
    log(f"expiry: {result['snapshots_after_expiry']} snapshot(s) left ({result['expire_s']} s)")


def main():
    ap = argparse.ArgumentParser(description="One-off full compaction of one CDC table (Paimon or Iceberg).")
    ap.add_argument("--format", required=True, choices=("paimon", "iceberg"))
    ap.add_argument("--table", required=True, choices=("customers", "products", "orders", "order_items"))
    ap.add_argument("--catalog-sql", required=True, help="s3:// URI of the writers' 01-catalog-setup.sql")
    ap.add_argument("--parallelism", type=int, required=True)
    ap.add_argument("--result-uri", required=True, help="s3:// URI for the result JSON")
    ap.add_argument("--expire-hours", type=float, default=12.0, help="Iceberg: expire snapshots older than this")
    ap.add_argument("--retain-last", type=int, default=5, help="Iceberg: keep at least this many snapshots")
    a = ap.parse_args()

    result = {"format": a.format, "table": f"{os.environ.get('GLUE_DATABASE')}.{a.table}",
              "parallelism_cap": a.parallelism, "started": now()}
    t0 = time.time()
    status = 0
    try:
        (paimon if a.format == "paimon" else iceberg)(a, result)
        result["status"] = "OK"
    except Exception as e:                      # recorded, then the job fails
        import traceback
        traceback.print_exc()
        err = str(e.java_exception.toString()) if hasattr(e, "java_exception") else (str(e) or repr(e))
        result.update(status="FAILED", error=err[:2000])
        status = 1
    result.update(finished=now(), duration_s=round(time.time() - t0, 1))
    bucket, key = s3_split(a.result_uri)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=json.dumps(result, indent=2).encode())
    log(f"result -> {a.result_uri}: {json.dumps(result)}")
    sys.exit(status)


if __name__ == "__main__":
    main()
