#!/usr/bin/env python3
"""
Continuous Iceberg maintenance for the CDC pipeline, as ONE PyFlink streaming job
==================================================================================

A single script, the same pattern
throughput-benchmark/compaction/flink-cid-compact.py already uses for its own
tables: a .py file staged to S3 and run as a FlinkDeployment's PythonDriver
entry point. No jar to build, no Maven module to maintain -- the classes this
calls (org.apache.iceberg.flink.maintenance.api.TableMaintenance /
RewriteDataFiles / ExpireSnapshots) are stock Iceberg 1.12 classes already on
the writer image's classpath (iceberg-flink-runtime-1.20-1.12.0.jar).

  maintain.py maintain --db flink_iceberg_db_v3r2 --warehouse s3://.../iceberg-warehouse \\
      --tables customers,products,orders,order_items --region us-west-2
  maintain.py plan --db ... --warehouse ... --tables customers       # read-only, no job

Why ONE continuous job, not a bounded job run hourly
-----------------------------------------------------
Iceberg's Flink rewrite reloads the WHOLE equality-delete set for every data
file it touches (Flink has no delete cache across rewrite tasks -- the same
reason the Spark job, and then the hourly Java bin-pack job, both took hours
on `customers`: thousands of tiny checkpoint files times tens of millions of
accumulated deletes). The fix is not a faster rewriter, it's a shorter wait:
if a pass runs every --schedule-commits writer checkpoints (default 10, ~10
min at the writers' 60s interval) instead of once an hour, each pass only
ever has to apply the deletes the writers added since the PREVIOUS pass,
because that pass already cleared the older ones. The backlog that made
`customers` slow never gets the chance to build back up.

This script is a long-running STREAMING job (TableMaintenance is designed for
one): it sits inside the Flink job graph watching the table's own commits and
fires RewriteDataFiles + ExpireSnapshots on the schedule below, for as long as
the FlinkDeployment runs. That also means the harness controls it exactly like
the writers -- suspend/resume via `spec.job.state`, upgradeMode last-state --
with no separate CronJob suspend/resume logic needed.

What this does NOT do: run once and exit. It only fires on the writers'
commits, so with the writers suspended nothing triggers. Start it together
with the writers so the first pass never faces a large backlog (it OOMed an
8 Gi TaskManager at ~35M equality deletes on customers). `plan` is read-only
and safe to run any time to see where each table stands.

Catalog: the writers' own Glue + S3FileIO + urlconnection HTTP client setup
(ingest-deploy/sql-scripts/iceberg/01-catalog-setup.sql), built directly from args here
instead of parsed from the SQL file, since there is no Flink SQL involved on
this path (CatalogLoader.custom(...), the same Java class the writers' own
GlueCatalog resolves through).
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

TABLES_DEFAULT = ["customers", "products", "orders", "order_items"]
# Mirrors ingest-deploy/sql-scripts/iceberg/03-iceberg-sinks.sql's write.target-file-size-bytes
# per table (64 MiB customers/products, 128 MiB orders/order_items).
TARGET_BYTES_DEFAULT = {
    "customers": 64 * 1024 * 1024,
    "products": 64 * 1024 * 1024,
    "orders": 128 * 1024 * 1024,
    "order_items": 128 * 1024 * 1024,
}
GLUE_CATALOG_IMPL = "org.apache.iceberg.aws.glue.GlueCatalog"


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg):
    print(f"[{now()}] {msg}", flush=True)


def build_catalog(J, catalog_impl, warehouse, region):
    """CatalogLoader.custom(...), the writers' own catalog (01-catalog-setup.sql),
    built from args directly: no Flink SQL / table environment on this path."""
    props = J.java.util.HashMap()
    props.put(J.org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION, warehouse)
    if catalog_impl == GLUE_CATALOG_IMPL:
        props.put(J.org.apache.iceberg.CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO")
        props.put("client.region", region)
        # EMR 7.14's AWS SDK v2 bundle has neither apache5 nor the default http
        # client; urlconnection is the one it carries (see the writers' catalog SQL).
        props.put("http-client.type", "urlconnection")
    conf = J.org.apache.hadoop.conf.Configuration()
    return J.org.apache.iceberg.flink.CatalogLoader.custom("glue_catalog", props, conf, catalog_impl)


def table_stats(J, table):
    """Live (deletes-applied) and raw (files/deletes present) counts, cheap: from
    the current snapshot's summary and one manifest-list scan, no data read."""
    snap = table.currentSnapshot()
    if snap is None:
        return {"snapshot_id": None, "live_records": 0, "data_files": 0, "delete_files": 0}
    summary = snap.summary()

    def gi(key):  # getOrDefault on the java.util.Map<String,String> summary
        v = summary.get(key)
        return int(v) if v is not None else 0

    return {
        "snapshot_id": snap.snapshotId(),
        "live_records": gi("total-records"),
        "data_files": gi("total-data-files"),
        "delete_files": gi("total-delete-files"),
        "equality_deletes": gi("total-equality-deletes"),
        "position_deletes": gi("total-position-deletes"),
    }


def cmd_plan(a):
    """Read-only: today's per-table state, no job submitted, nothing committed."""
    from pyflink.java_gateway import get_gateway

    J = get_gateway().jvm
    catalog = build_catalog(J, a.catalog_impl, a.warehouse, a.region)
    result = {"db": a.db, "checked": now(), "tables": {}}
    for t in a.tables:
        # .of(String...) is varargs: py4j cannot match a plain multi-arg call to
        # it (same gotcha throughput-benchmark/compaction/flink-cid-compact.py
        # notes for a different class). .parse("db.table") takes one String.
        loader = J.org.apache.iceberg.flink.TableLoader.fromCatalog(
            catalog, J.org.apache.iceberg.catalog.TableIdentifier.parse(f"{a.db}.{t}")
        )
        loader.open()
        try:
            table = loader.loadTable()
            stats = table_stats(J, table)
            result["tables"][t] = stats
            log(f"{t}: {stats}")
        finally:
            loader.close()
    print(json.dumps(result, indent=2))


def cmd_maintain(a):
    """One continuous streaming job: for every table, Iceberg's own maintenance
    builders (RewriteDataFiles + ExpireSnapshots), scheduled off the table's own
    commits. env.execute() blocks for as long as the FlinkDeployment runs."""
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.java_gateway import get_gateway

    J = get_gateway().jvm
    env = StreamExecutionEnvironment.get_execution_environment()
    # The maintenance graph is driven by trigger events (one per table commit),
    # not by data volume: parallelism 1 is enough, the rewrite/expire tasks size
    # their own work from RewriteDataFiles'/ExpireSnapshots' own settings below.
    env.set_parallelism(1)
    jenv = env._j_stream_execution_environment

    catalog = build_catalog(J, a.catalog_impl, a.warehouse, a.region)
    log(
        f"start db={a.db} tables={','.join(a.tables)} schedule_commits={a.schedule_commits} "
        f"partial_progress_max_commits={a.partial_progress_max_commits} "
        f"expire={a.expire_hours}h/keep{a.expire_retain_last}"
    )

    for t in a.tables:
        target_bytes = a.target_bytes.get(t, a.target_bytes_default)
        loader = J.org.apache.iceberg.flink.TableLoader.fromCatalog(
            catalog, J.org.apache.iceberg.catalog.TableIdentifier.parse(f"{a.db}.{t}")
        )
        rewrite = (
            J.org.apache.iceberg.flink.maintenance.api.RewriteDataFiles.builder()
            .rewriteAll(True)  # every file: the writers' checkpoint files are all small
            .partialProgressEnabled(True)
            .partialProgressMaxCommits(a.partial_progress_max_commits)
            .targetFileSizeBytes(target_bytes)
            .scheduleOnCommitCount(a.schedule_commits)
        )
        expire = (
            J.org.apache.iceberg.flink.maintenance.api.ExpireSnapshots.builder()
            # ofHours(long) -- py4j won't match a Python float to it (--expire-hours
            # is a float so fractional hours work); go through minutes as an int.
            .maxSnapshotAge(J.java.time.Duration.ofMinutes(round(a.expire_hours * 60)))
            .retainLast(a.expire_retain_last)
            .scheduleOnCommitCount(a.schedule_commits)
        )
        # forTable(env, loader) with NO TriggerLockFactory: the 2-arg overload
        # passes a null lock (checked against the class file directly -- see
        # README). Safe here: one maintenance job per table, nothing else ever
        # commits a rewrite/expire against it concurrently.
        builder = J.org.apache.iceberg.flink.maintenance.api.TableMaintenance.forTable(jenv, loader)
        builder.add(rewrite)
        builder.add(expire)
        builder.append()
        log(
            f"{t}: appended (every {a.schedule_commits} commits; target "
            f"{target_bytes / 2**20:.0f} MiB; expire >{a.expire_hours}h keep {a.expire_retain_last})"
        )

    log(f"executing {a.job_name} (blocks: this is the job)")
    env.execute(a.job_name)


def parse_tables(csv):
    tables = [t.strip() for t in csv.split(",") if t.strip()]
    if not tables:
        raise argparse.ArgumentTypeError("--tables must list at least one table")
    return tables


def parse_target_bytes(csv):
    """table=bytes,table=bytes -> dict; empty/absent -> {} (callers fall back to
    TARGET_BYTES_DEFAULT per table)."""
    out = {}
    if not csv:
        return out
    for pair in csv.split(","):
        pair = pair.strip()
        if not pair:
            continue
        table, _, value = pair.partition("=")
        if not value:
            raise argparse.ArgumentTypeError(f"--target-bytes entry '{pair}' must be table=bytes")
        out[table.strip()] = int(value)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=("maintain", "plan"))
    ap.add_argument("--db", required=True, help="Glue database, e.g. flink_iceberg_db_v3r2")
    ap.add_argument("--warehouse", required=True, help="s3://bucket/iceberg-warehouse")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--catalog-impl", default=GLUE_CATALOG_IMPL)
    ap.add_argument("--tables", type=parse_tables, default=TABLES_DEFAULT,
                     help="comma-separated, default: " + ",".join(TABLES_DEFAULT))
    ap.add_argument("--target-bytes", type=parse_target_bytes, default="",
                     help="table=bytes,table=bytes overrides; default per table matches the sinks' settings")
    ap.add_argument("--target-bytes-default", type=int, default=64 * 1024 * 1024,
                     help="fallback for a table not named in --target-bytes and not in the built-in defaults")
    ap.add_argument("--schedule-commits", type=int, default=10,
                     help="run rewrite+expire every N writer commits (~N minutes at a 60s checkpoint interval)")
    ap.add_argument("--partial-progress-max-commits", type=int, default=10,
                     help="RewriteDataFiles: commit in up to this many pieces instead of one, so a single file "
                          "group failure does not lose the whole pass")
    ap.add_argument("--expire-hours", type=float, default=12.0,
                     help="ExpireSnapshots: drop snapshots older than this (the writers' history.expire rule)")
    ap.add_argument("--expire-retain-last", type=int, default=5)
    ap.add_argument("--job-name", default="iceberg-maintenance")
    a = ap.parse_args()
    # Merge the built-in per-table defaults under whatever --target-bytes gave explicitly.
    merged = dict(TARGET_BYTES_DEFAULT)
    merged.update(a.target_bytes)
    a.target_bytes = merged

    try:
        (cmd_plan if a.mode == "plan" else cmd_maintain)(a)
    except Exception:
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
