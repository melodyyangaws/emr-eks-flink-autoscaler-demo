#!/usr/bin/env python3
"""
One-off bin-pack compaction of one throughput-benchmark table (cid_events)
===========================================================================

Run by compaction/run.sh as a batch FlinkDeployment (flink-cid-compact.yaml),
once, with the writers stopped, before the snapshot query test:

  flink-cid-compact.py --format paimon|iceberg --catalog-sql s3://.../02-catalog-setup.sql \\
      --parallelism N --result-uri s3://.../compaction/<STAMP>-<fmt>-pass<P>.json

The same rule on both formats, Paimon's own (AppendCompactCoordinator, 1.3):
only files smaller than 70% of the table's 256 MiB target file size are
rewritten, bin-packed together; files at or near the target are left as they
are. The writers leave one ~256 MiB file and one ~118 MiB tail per subtask
per checkpoint, so it is the tails that get merged.

- Paimon:  CALL sys.compact (the default 'minor' strategy applies that rule).
  Bins compact once they hold >= 2 x target or >= 5 files; the output rolls at
  the 256 MiB target. snapshot.time-retained is raised for this commit only, so
  its expiry keeps the writers' APPEND snapshots: the throughput summary is
  computed from them.
- Iceberg: the same selection made here (files < 70% of
  write.target-file-size-bytes, per partition), packed with
  TableScanUtil.planTasks into groups of <= the target (Iceberg's
  RowDataRewriter writes ONE file per group: it never rolls), groups of one
  file dropped, then rewritten by RowDataRewriter (what Iceberg's own Flink
  RewriteDataFilesAction runs) and committed as one RewriteFiles. Iceberg's
  action is not used as is: it splits every file at the target and rewrites
  any file it had to split, i.e. nearly all of them here.
  At most --max-groups groups per run: the groups travel inside the job graph,
  and Application mode allows one job per deployment; run.sh starts another
  pass while groups remain.

The catalog is the writers' own 02-catalog-setup.sql (${VAR} filled from the
environment), so both jobs reach the tables exactly as the writers did. The
result (before/after file counts and bytes, what was rewritten, timings) goes
to --result-uri as JSON; the summary reads it from there.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import boto3

TABLE = "cid_events"
OPEN_FILE_COST = 4 * 1024 * 1024    # both formats' default (source.split.open-file-cost / read.split.open-file-cost)
LOOKBACK = 10                        # Iceberg's read.split.planning-lookback default


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg):
    print(f"[{now()}] {msg}", flush=True)


def s3_split(uri):
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def run_catalog_sql(t_env, uri):
    """The writers' 02-catalog-setup.sql: ${VAR} / ${VAR:default} from the
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


def size_stats(sizes, threshold):
    small = [s for s in sizes if s < threshold]
    return {"files": len(sizes), "bytes": sum(sizes),
            "small_files": len(small), "small_bytes": sum(small)}


# ── Paimon ──────────────────────────────────────────────────────────────────
def paimon(a, result):
    from pyflink.java_gateway import get_gateway
    from pyflink.table import EnvironmentSettings, TableEnvironment

    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    cfg = t_env.get_config()
    cfg.set("parallelism.default", str(a.parallelism))
    cfg.set("table.dml-sync", "true")          # CALL returns once the compaction job has finished
    run_catalog_sql(t_env, a.catalog_sql)
    db = os.environ["GLUE_DATABASE"]
    J = get_gateway().jvm
    catalog = t_env._j_tenv.getCatalog("paimon_catalog").get().catalog()
    ident = J.org.apache.paimon.catalog.Identifier.create(db, TABLE)

    def state():
        # Through the FileStore, not newReadBuilder(): py4j does not reach
        # Table's default (interface) methods.
        table = catalog.getTable(ident)
        sizes = [e.file().fileSize() for e in table.store().newScan().plan().files()]
        target = int(table.coreOptions().targetFileSize(False))
        return table.snapshotManager().latestSnapshotId(), target, sizes

    sid, target, sizes = state()
    threshold = target // 10 * 7               # CoreOptions.compactionFileSize: what Paimon itself compacts
    result.update(target_bytes=target, threshold_bytes=threshold, before=dict(size_stats(sizes, threshold), snapshot=sid))
    log(f"before: snapshot {sid}, {result['before']}")
    if a.dry_run:
        return

    opts = f"snapshot.time-retained={a.retain}"
    log(f"CALL sys.compact on {db}.{TABLE} (options {opts}, parallelism {a.parallelism})")
    t0 = time.time()
    res = t_env.execute_sql(f"CALL sys.compact(`table` => '{db}.{TABLE}', options => '{opts}')")
    with res.collect() as rows:
        for r in rows:
            log(f"  {r}")
    result["rewrite_s"] = round(time.time() - t0, 1)

    sid, _, sizes = state()
    result["after"] = dict(size_stats(sizes, threshold), snapshot=sid)
    result["remaining_groups"] = 0
    log(f"after: snapshot {sid}, {result['after']} ({result['rewrite_s']} s)")


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
    # parse(String), not of(String...): py4j cannot pass varargs.
    table = catalog.loadTable(J.org.apache.iceberg.catalog.TableIdentifier.parse(f"{db}.{TABLE}"))

    def scan(snapshot_id):
        """FileScanTasks of a snapshot, one per data file (planFiles never splits)."""
        tasks, it = [], table.newScan().useSnapshot(snapshot_id).ignoreResiduals().planFiles().iterator()
        try:
            while it.hasNext():
                tasks.append(it.next())
        finally:
            it.close()
        return tasks

    target = int(table.properties().getOrDefault("write.target-file-size-bytes", str(512 * 1024 * 1024)))
    threshold = target // 10 * 7               # the Paimon rule, applied here
    start = table.currentSnapshot().snapshotId()
    tasks = scan(start)
    sizes = [t.file().fileSizeInBytes() for t in tasks]
    result.update(target_bytes=target, threshold_bytes=threshold, before=dict(size_stats(sizes, threshold), snapshot=start))
    log(f"before: snapshot {start}, {result['before']}")

    # Per partition, as Iceberg's own planner does: a group never spans two.
    parts = {}
    spec = table.spec()
    for t, size in zip(tasks, sizes):
        if size < threshold:
            parts.setdefault(spec.partitionToPath(t.file().partition()), J.java.util.ArrayList()).add(t)
    # CloseableIterable.withNoopClose has an (Iterable) and an (E) overload, and
    # py4j picks (E) for a list: the Iterable one is called by reflection.
    gw = get_gateway()
    sig = gw.new_array(J.java.lang.Class, 1)
    sig[0] = J.java.lang.Class.forName("java.lang.Iterable")
    no_close = J.java.lang.Class.forName("org.apache.iceberg.io.CloseableIterable").getMethod("withNoopClose", sig)
    groups = []
    for path, small in sorted(parts.items()):
        arg = gw.new_array(J.java.lang.Object, 1)
        arg[0] = small
        it = J.org.apache.iceberg.util.TableScanUtil.planTasks(
            no_close.invoke(None, arg), target, LOOKBACK, OPEN_FILE_COST).iterator()
        n0 = len(groups)
        while it.hasNext():
            g = it.next()
            if g.files().size() > 1:
                groups.append(g)
        log(f"  {path}: {small.size()} small file(s) -> {len(groups) - n0} group(s) of >= 2")
    todo, rest = groups[:a.max_groups], groups[a.max_groups:]
    result.update(groups=len(todo), remaining_groups=len(rest))
    if a.dry_run:
        result.update(planned_groups=len(groups), remaining_groups=0)
        return
    if not todo:
        result.update(rewrite_s=0, after=result["before"])
        log("nothing to rewrite")
        return

    jgroups = J.java.util.ArrayList()
    for g in todo:
        jgroups.add(g)
    log(f"rewriting {len(todo)} group(s) ({len(rest)} left for another pass), parallelism {min(len(todo), a.parallelism)}")
    t0 = time.time()
    ds = env._j_stream_execution_environment.fromCollection(jgroups)
    rewriter = J.org.apache.iceberg.flink.source.RowDataRewriter(table, True, table.io(), table.encryption())
    added = rewriter.rewriteDataForTasks(ds, min(len(todo), a.parallelism))
    result["rewrite_s"] = round(time.time() - t0, 1)
    log(f"rewritten: {added.size()} new file(s) in {result['rewrite_s']} s; committing")

    # Iceberg's own commit (BaseRewriteDataFilesAction.doReplace): validated from
    # the planning snapshot, so a concurrent writer's commit would fail it.
    rw = table.newRewrite().validateFromSnapshot(start)
    for g in todo:
        it = g.files().iterator()
        while it.hasNext():
            rw.deleteFile(it.next().file())
    it = added.iterator()
    while it.hasNext():
        rw.addFile(it.next())
    rw.commit()
    table.refresh()
    sid = table.currentSnapshot().snapshotId()
    sizes = [t.file().fileSizeInBytes() for t in scan(sid)]
    result["after"] = dict(size_stats(sizes, threshold), snapshot=sid)
    log(f"after: snapshot {sid}, {result['after']}")


def main():
    ap = argparse.ArgumentParser(description="One-off bin-pack compaction of cid_events (Paimon or Iceberg).")
    ap.add_argument("--format", required=True, choices=("paimon", "iceberg"))
    ap.add_argument("--catalog-sql", required=True, help="s3:// URI of the writers' 02-catalog-setup.sql")
    ap.add_argument("--parallelism", type=int, required=True)
    ap.add_argument("--result-uri", required=True, help="s3:// URI for the result JSON")
    ap.add_argument("--max-groups", type=int, default=4000, help="Iceberg: groups per pass")
    ap.add_argument("--retain", default="7d", help="Paimon: snapshot.time-retained for the compaction commit")
    ap.add_argument("--dry-run", default="false", help="true: measure and plan only; rewrite and commit nothing")
    a = ap.parse_args()
    a.dry_run = a.dry_run.lower() == "true"

    result = {"format": a.format, "table": f"{os.environ.get('GLUE_DATABASE')}.{TABLE}",
              "parallelism": a.parallelism, "started": now()}
    t0 = time.time()
    status = 0
    try:
        (paimon if a.format == "paimon" else iceberg)(a, result)
        result["status"] = "DRY_RUN" if a.dry_run else "OK"
    except Exception as e:                      # recorded, then the job fails
        import traceback
        traceback.print_exc()
        # A Py4JJavaError's str() can be empty: its Java exception says what failed.
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
