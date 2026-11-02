#!/usr/bin/env python3
"""
Compact one Iceberg CDC table (ICEBERG_DB.ICEBERG_TABLE) with Spark
`rewrite_data_files`. Run by compact-job.yaml through run-compaction.sh, which
sets every input below from cdc-pipeline/.bench-env. Written for, and sized by,
the v2 `order_items` table described next.

TABLE SETS
----------
ICEBERG_DB defaults to flink_iceberg_db<CDC_DB_SUFFIX>, with lib/common.sh's
rule: CDC_DB_SUFFIX unset -> _v3 (flink_iceberg_db_v3, format v3, the default),
CDC_DB_SUFFIX set empty -> flink_iceberg_db (format v2). run-compaction.sh always
passes ICEBERG_DB, so the default only matters when this file is run by hand.
v2 tables carry equality deletes plus position-delete files; v3 tables (Iceberg
1.12.0 Flink writer) carry equality deletes plus deletion vectors in Puffin
files. The image's Iceberg is 1.10.1-amzn-1 (EMR 7.14.0): compacting the 1.12.0
writer's v3 tables with it is UNTESTED. Run DRY_RUN=true first (it reads the
metadata tables only), and see compact-job.yaml for the 1.12.0-runtime fallback.

WHY THIS TABLE NEEDS SPARK
--------------------------
order_items is the one CDC table Athena OPTIMIZE could not compact: a single
partition (created_dt=2026-09-27) holding 137 data files, 124 delete files and
365,225,304 equality deletes. Athena failed twice with "Query exhausted resources
at this scale factor", Athena Spark is UNAUTHORIZED in this account, and BOTH
query engines fail to read the table:

  StarRocks : MEM_LIMIT_EXCEEDED on a bare COUNT(*)
  DuckDB    : OOMKilled (exit 137) at 52Gi AND at 72Gi, spill volume untouched

The data itself is small (~7.4 GB). The wall is the equality-delete set: a reader
must materialise the delete keys before it can emit a row, which is why extra
memory did not help DuckDB. Spark is used because it is the only engine reachable
here that can both apply those deletes and COMMIT the compacted result.

THE OPTIONS THAT MATTER
-----------------------
use-starting-sequence-number = false
    THE LOAD-BEARING ONE. Default TRUE gives rewritten data files the sequence
    number of the snapshot at the START of the rewrite. The existing equality
    deletes have HIGHER sequence numbers, so they would still apply to the freshly
    written files and the delete set would stay live — the rewrite would burn an
    hour and change nothing observable. False assigns a new, higher sequence
    number so the old deletes no longer apply.

rewrite-all = true
    Without it Iceberg only rewrites groups it judges worth rewriting BY SIZE, and
    these 137 files are already near the 64 MB target — the job would no-op and
    report success. The goal here is the DELETES, not file sizes.

remove-dangling-deletes = true
    Once every data file is replaced, the old delete files reference data that is
    no longer in the table. This drops them from the new snapshot, taking
    total-delete-files toward 0 so a reader never builds the delete set at all.

max-concurrent-file-group-rewrites = 1
    Memory safety, not throughput: one delete set in flight at a time.

After the rewrite, snapshots older than EXPIRE_OLDER_THAN_HOURS (12) are expired,
keeping at least EXPIRE_RETAIN_LAST (5): the same age rule Paimon's writer applies
in-job (snapshot.time-retained = 12h, num-retained.min = 5). Iceberg's Flink sink
never expires snapshots, so without this its metadata grows for the whole run.
EXPIRE_SNAPSHOTS=false skips it. Expiry adds no snapshot, so the current snapshot
stays the compaction's replace commit (the snapshot gate checks that). Orphan
files are NOT removed. Expiry deletes S3 data of old snapshots irreversibly.
"""
import json
import os
import time

from pyspark.sql import SparkSession

REGION = os.environ.get("AWS_REGION", "us-west-2")
# No default bucket: it is account-specific (.bench-env: s3://<BUCKET_NAME>/
# iceberg-warehouse), and a wrong warehouse fails late, in the Glue lookup.
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "")
if not WAREHOUSE.startswith("s3://"):
    raise SystemExit("ICEBERG_WAREHOUSE must be set to s3://<bucket>/iceberg-warehouse, "
                     "got %r" % WAREHOUSE)
# Unset CDC_DB_SUFFIX -> the v3 set (_v3); an explicit empty one -> v2.
DB = os.environ.get("ICEBERG_DB") or "flink_iceberg_db" + os.environ.get("CDC_DB_SUFFIX", "_v3")
TABLE = os.environ.get("ICEBERG_TABLE", "order_items")
CAT = "glue_catalog"
FQN = f"{CAT}.{DB}.{TABLE}"
TARGET_FILE_SIZE = os.environ.get("TARGET_FILE_SIZE_BYTES", "268435456")  # 256 MiB
# A dry run reports the before-state and exits without committing, so the catalog,
# credentials and classpath can be validated in ~1 minute instead of an hour.
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() in ("1", "true", "yes")
EXPIRE_SNAPSHOTS = os.environ.get("EXPIRE_SNAPSHOTS", "true").lower() in ("1", "true", "yes")
EXPIRE_OLDER_THAN_HOURS = float(os.environ.get("EXPIRE_OLDER_THAN_HOURS", "12"))
EXPIRE_RETAIN_LAST = int(os.environ.get("EXPIRE_RETAIN_LAST", "5"))
# Executor (delete-file) cache cap: half the driver heap, at most 48 GiB (the
# one-off Job's 100g heap -> 48 GiB, the hourly CronJob's 40g -> 20 GiB).
_heap = os.environ.get("DRIVER_MEMORY", "100g").lower()
_heap_gib = float(_heap[:-1]) if _heap[-1:] == "g" else float(_heap[:-1]) / 1024 if _heap[-1:] == "m" else 100.0
CACHE_MAX_TOTAL = int(min(48, _heap_gib / 2) * 1024**3)


def build_session():
    """Catalog wiring only.

    Memory/parallelism settings are NOT set here on purpose: for PySpark the JVM is
    already running by the time this executes, so spark.driver.memory set through
    the builder is silently ignored. Heap comes from spark-submit --driver-memory
    in compact-job.yaml (its args) — see the note there.
    """
    return (
        SparkSession.builder.appName(f"compact-{TABLE}")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CAT}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CAT}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{CAT}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CAT}.warehouse", WAREHOUSE)
        .config(f"spark.sql.catalog.{CAT}.client.region", REGION)
        # ---- S3 client tuning: REQUIRED, not an optimisation ----
        # The first real attempt failed with thousands of
        #   "Timeout waiting for connection from pool" / ConnectionPoolTimeoutException
        #   "HTTP request execution did not complete before ... 2000 millis"
        # from BaseDeleteLoader while loading the equality-delete files. Two causes,
        # both fixed here:
        #
        #  1. POOL EXHAUSTION. S3FileIO's Apache client defaults to 100 connections.
        #     BaseDeleteLoader opens a stream per delete file and reads them
        #     concurrently; with 124 delete files plus the data-file readers, demand
        #     exceeds the pool and tasks block until they time out acquiring one.
        #  2. A 2-SECOND TIMEOUT. EMR's spark-defaults.conf ships
        #     spark.hadoop.fs.s3.getObject.initialSocketTimeoutMilliseconds=2000,
        #     tuned for small random reads. These delete files are 9-54 MB each, so a
        #     single sequential read legitimately exceeds 2 s and gets cancelled —
        #     then retried, which makes the pool pressure worse. The failures were
        #     self-amplifying, which is why the job produced errors rather than
        #     merely running slowly.
        #
        # The socket timeout is per-read, not for the whole request, so 300 s is not
        # a hang risk: it is "let a 54 MB sequential read finish". Property names were
        # read out of iceberg-spark-runtime-3.5_2.12-1.10.0-amzn-1.jar
        # (S3FileIOProperties / HttpClientProperties; EMR 7.13.0, the image until
        # 2026-11-02) rather than guessed. The image is now EMR 7.14.0 (Spark
        # 3.5.8-amzn-0, Iceberg 1.10.1-amzn-1, a patch release); they were not
        # re-read from that jar. An unknown catalog property is ignored, not
        # rejected, so a rename would show up as the old timeouts, not an error.
        .config(f"spark.sql.catalog.{CAT}.http-client.type", "apache")
        .config(f"spark.sql.catalog.{CAT}.http-client.apache.max-connections", "512")
        .config(f"spark.sql.catalog.{CAT}.http-client.apache.socket-timeout-ms", "300000")
        .config(f"spark.sql.catalog.{CAT}.http-client.apache.connection-timeout-ms", "30000")
        .config(f"spark.sql.catalog.{CAT}.http-client.apache.connection-acquisition-timeout-ms",
                "120000")
        .config(f"spark.sql.catalog.{CAT}.s3.retry.num-retries", "10")
        # ---- Delete-set sharing: the difference between finishing and GC death ----
        # The second attempt ran 60 minutes with all 18 GC threads pegged and ZERO
        # task threads consuming CPU (verified per-thread via /proc/1/task/*/stat),
        # heap flat at 90.7 GiB of 100g: a full-GC death spiral, not slow progress.
        #
        # Cause: equality deletes apply to every data file with a lower sequence
        # number, so EVERY task rewriting a data file needs the delete keys. Iceberg
        # has a cache so concurrent tasks share loaded delete files instead of each
        # building its own — but executor-cache.max-total defaults to 128 MB, which
        # a 365 M-key set blows through instantly. Every task therefore materialised
        # its own copy, and N copies never fit regardless of heap size.
        #
        # So the fix is FEWER concurrent delete-set builders plus a cache large
        # enough to hold ONE shared set (see SPARK_LOCAL_THREADS in compact-job.yaml
        # for the parallelism half). Adding memory does not help — that was already
        # disproven on DuckDB at 52Gi and 72Gi.
        .config("spark.sql.iceberg.executor-cache.enabled", "true")
        .config("spark.sql.iceberg.executor-cache.max-total", str(CACHE_MAX_TOTAL))
        .config("spark.sql.iceberg.executor-cache.max-entry", str(CACHE_MAX_TOTAL // 2))
        # Spark's duration syntax: Iceberg 1.10.1 rejects ISO-8601 "PT2H" with
        # NumberFormatException "Time must be specified as seconds (s)...".
        .config("spark.sql.iceberg.executor-cache.timeout", "2h")
        # Independently of the Iceberg catalog, the EMRFS/hadoop-s3 default above
        # also applies to any read that goes through the Hadoop FileSystem, so it is
        # raised too rather than left to bite somewhere else.
        .config("spark.hadoop.fs.s3.getObject.initialSocketTimeoutMilliseconds", "300000")
        .getOrCreate()
    )


def state(spark, label):
    """Count files/deletes from the manifests.

    The metadata tables never apply deletes, so they stay cheap and readable even
    though a plain SELECT on this table OOMs both query engines. This is the only
    way to observe the before-state at all.
    """
    d = spark.sql(f"SELECT count(*) n, coalesce(sum(file_size_in_bytes),0) b "
                  f"FROM {FQN}.data_files").collect()[0]
    x = spark.sql(f"SELECT count(*) n, coalesce(sum(record_count),0) r "
                  f"FROM {FQN}.delete_files").collect()[0]
    s = {"data_files": d["n"], "data_bytes": d["b"],
         "delete_files": x["n"], "delete_records": x["r"]}
    print(f"  {label:<7} data_files={s['data_files']:<5} "
          f"data={s['data_bytes'] / 2**30:6.2f} GiB  "
          f"delete_files={s['delete_files']:<4} "
          f"delete_records={s['delete_records']:,}", flush=True)
    return s


# REWRITE_MODE picks the option sets.
#   full (default): the snapshot test, writers SUSPENDED. rewrite-all with
#     use-starting-sequence-number=false: the new files take a fresh sequence
#     number, so every old equality delete detaches and can be dropped. Unsafe
#     while Flink commits: an equality delete committed during the rewrite has a
#     LOWER sequence number than the new files and would no longer apply to them.
#   live: the hourly CronJob, writers RUNNING. use-starting-sequence-number=true
#     keeps the rewritten files at the rewrite's starting sequence number, so
#     equality deletes Flink commits meanwhile still apply to them; deletes the
#     rewrite already applied become dangling and remove-dangling-deletes drops
#     them. partial-progress commits file groups as they finish, so one conflict
#     with a Flink commit loses a group, not the run. Flink's deletion vectors
#     only point at files of their own commit, never at files being rewritten.
REWRITE_MODE = os.environ.get("REWRITE_MODE", "full").lower()
if REWRITE_MODE not in ("full", "live"):
    raise SystemExit(f"REWRITE_MODE must be full|live, got {REWRITE_MODE!r}")

# Tried in order. Iceberg rejects unknown procedure options outright and these
# option names have moved between releases, so a rejected option degrades to a
# still-correct call rather than aborting. Each mode keeps its
# use-starting-sequence-number value in EVERY fallback: in full mode without
# false the rewrite cannot detach the old deletes; in live mode without true it
# is unsafe (above).
if REWRITE_MODE == "full":
    OPTION_SETS = [
        {"rewrite-all": "true", "use-starting-sequence-number": "false",
         "remove-dangling-deletes": "true",
         "max-concurrent-file-group-rewrites": "1",
         "target-file-size-bytes": TARGET_FILE_SIZE},
        {"rewrite-all": "true", "use-starting-sequence-number": "false",
         "max-concurrent-file-group-rewrites": "1",
         "target-file-size-bytes": TARGET_FILE_SIZE},
        {"rewrite-all": "true", "use-starting-sequence-number": "false"},
    ]
else:
    OPTION_SETS = [
        {"rewrite-all": "true", "use-starting-sequence-number": "true",
         "remove-dangling-deletes": "true",
         "partial-progress.enabled": "true",
         "max-concurrent-file-group-rewrites": "4",
         "target-file-size-bytes": TARGET_FILE_SIZE},
        {"rewrite-all": "true", "use-starting-sequence-number": "true",
         "partial-progress.enabled": "true",
         "target-file-size-bytes": TARGET_FILE_SIZE},
        {"rewrite-all": "true", "use-starting-sequence-number": "true"},
    ]


def rewrite(spark):
    for i, opts in enumerate(OPTION_SETS, 1):
        lit = ", ".join(f"'{k}','{v}'" for k, v in opts.items())
        sql = (f"CALL {CAT}.system.rewrite_data_files("
               f"table => '{DB}.{TABLE}', options => map({lit}))")
        print(f"\n-- attempt {i}/{len(OPTION_SETS)} " + "-" * 48, flush=True)
        for k, v in opts.items():
            print(f"     {k} = {v}", flush=True)
        st = time.time()
        try:
            rows = spark.sql(sql).collect()
        except Exception as e:
            msg = str(e).replace("\n", " ")
            # Only degrade on option VALIDATION failures. An OOM or an S3/Glue error
            # recurs identically with fewer options set, so retrying would just burn
            # another full pass over 365 M deletes.
            bad_option = ("IllegalArgumentException" in msg
                          or "Cannot use option" in msg
                          or "unsupported option" in msg.lower()
                          or "invalid option" in msg.lower())
            if bad_option and i < len(OPTION_SETS):
                print(f"     option rejected, falling back: {msg[:200]}", flush=True)
                continue
            print(f"     FAILED after {time.time() - st:.0f}s: {msg[:800]}", flush=True)
            raise
        el = time.time() - st
        r = rows[0].asDict() if rows else {}
        print(f"     committed in {el:.0f}s: {r}", flush=True)
        return {"elapsed_s": round(el, 1),
                "result": {k: str(v) for k, v in r.items()}, "options": opts}
    raise RuntimeError("every option set was rejected")


def expire(spark):
    """expire_snapshots with Paimon's retention rule; never fails the job (the
    rewrite already committed, and the gate only needs that commit)."""
    cutoff = time.strftime("%Y-%m-%d %H:%M:%S",
                           time.gmtime(time.time() - EXPIRE_OLDER_THAN_HOURS * 3600))
    sql = (f"CALL {CAT}.system.expire_snapshots(table => '{DB}.{TABLE}', "
           f"older_than => TIMESTAMP '{cutoff}', retain_last => {EXPIRE_RETAIN_LAST})")
    print(f"\n-- expire_snapshots (older than {cutoff} UTC, retain {EXPIRE_RETAIN_LAST}) "
          + "-" * 8, flush=True)
    st = time.time()
    try:
        r = spark.sql(sql).collect()
        res = r[0].asDict() if r else {}
        print(f"     done in {time.time() - st:.0f}s: {res}", flush=True)
        return {"elapsed_s": round(time.time() - st, 1), "cutoff_utc": cutoff,
                "result": {k: str(v) for k, v in res.items()}}
    except Exception as e:
        msg = str(e).replace("\n", " ")[:600]
        print(f"     expire_snapshots FAILED (rewrite kept): {msg}", flush=True)
        return {"error": msg, "cutoff_utc": cutoff}


def main():
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    conf = spark.sparkContext.getConf()
    print("=" * 72)
    print(f"  Iceberg compaction — {DB}.{TABLE}")
    print("=" * 72)
    print(f"  spark      : {spark.version}")
    print(f"  warehouse  : {WAREHOUSE}")
    print(f"  driver mem : {conf.get('spark.driver.memory', '(default)')}")
    print(f"  dry run    : {DRY_RUN}")
    print(f"  mode       : {REWRITE_MODE}")
    print("=" * 72, flush=True)

    print("\n-- Table state " + "-" * 56, flush=True)
    before = state(spark, "BEFORE")
    payload = {"table": f"{DB}.{TABLE}", "spark": spark.version, "region": REGION,
               "before": before, "dry_run": DRY_RUN, "mode": REWRITE_MODE}

    if DRY_RUN:
        print("\nDRY_RUN — no rewrite committed.", flush=True)
    else:
        payload["rewrite"] = rewrite(spark)
        print("\n-- Table state " + "-" * 56, flush=True)
        after = state(spark, "AFTER")
        payload["after"] = after

        # The verification that matters: a bare COUNT(*) is precisely the query that
        # died with MEM_LIMIT_EXCEEDED on StarRocks and exit 137 on DuckDB. If it
        # returns, the delete set is genuinely gone rather than merely smaller.
        print("\n-- Verification " + "-" * 55, flush=True)
        st = time.time()
        n = spark.sql(f"SELECT count(*) c FROM {FQN}").collect()[0]["c"]
        print(f"  COUNT(*) = {n:,} in {time.time() - st:.1f}s", flush=True)
        payload["verify_count"] = n
        payload["delete_files_remaining"] = after["delete_files"]
        if EXPIRE_SNAPSHOTS:
            payload["expire"] = expire(spark)

    print("\n===JSON===")
    print(json.dumps(payload, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
