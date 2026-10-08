#!/usr/bin/env python3
"""Interactive DuckDB session against cid_events. NOT the benchmark runner.

THIS FILE NO LONGER DEFINES ANY QUERIES, AND THAT IS THE WHOLE POINT.

It used to be the runner, with the ten queries written out as Python f-strings
while starrocks-cid-audit.sh carried the same ten as shell heredocs. Both claimed
in their own comments to mirror the other "one-for-one", and on 2026-10-06 they
silently stopped doing so: the StarRocks side retired its `uuid % N` snapshot
sample and windowed S4, this side kept the sample on all four snapshot queries,
and NOTHING FAILED. Both harnesses ran, printed full tables and wrote results —
of two different experiments. A duplicated query set has no mechanism that can
notice that.

So the queries now live once, as files: authored in
querybench-script/{hot,snapshot}/*.sql, published to s3://.../flink/querybench/
by that directory's bench-sql.sh upload, and synced from there by every harness.
One object set, rendered by:

    the runner        run-duckdb-cid.sh --in-pod  (shell + the duckdb CLI)
    the other engine  starrocks-cid-audit.sh
    this file         a session for chasing a result down by hand

WHAT THIS IS FOR. A benchmark tells you S4 took 1,541 s; it does not tell you
why. This gives you a connection with the pod's real configuration and
credentials already in place, so an EXPLAIN, a row-group histogram or a
`SELECT * ... LIMIT 5` costs one command instead of a re-derivation of the setup.
Everything it does to get there is the same as the runner, because a
reproduction that configures itself differently reproduces something else.

GETTING IT INTO THE POD. It is NOT mounted. run-duckdb-cid.sh's ConfigMap ships
exactly one key — itself — because the measured path never calls this file, and a
second key would have to be kept in sync for a session that is used by hand. Copy
it in while a Job pod is running:

    kubectl cp duckdb-cid-bench.py \
        emr-flink/$(kubectl get pod -n emr-flink \
            -l app=duckdb-cid-bench -o jsonpath='{.items[0].metadata.name}'):/tmp/

`boto3` IS pre-installed by the Job's entrypoint for exactly this (see
duckdb-cid-job.yaml) — awscli 1.x vendors its own botocore and brings no boto3, so
the import below used to fail in-pod no matter how the file got there.

USAGE (inside the pod; `python3 /tmp/duckdb-cid-bench.py ...`)

    scan                    print the scan expression and its provenance
    sql '<statement>'       run one statement, print rows
    file <path>             run a .sql file, with ${...} rendered by bench-sql-lib.sh
    query <name>            run one named query from the shared suites, e.g.
                            `query S4_wide_json` — the exact SQL the runner sent
    explain <name>          EXPLAIN ANALYZE that named query
    repl                    read statements from stdin until EOF

Environment is identical to the runner's (FORMAT, ICEBERG_DB, PIN_SNAPSHOT,
DUCKDB_*, SNAPSHOT_DT, BENCH_SQL_DIR, ...); see run-duckdb-cid.sh.

NO TIMINGS FROM HERE ARE QUOTABLE. There is no warmup, no repeat, no median and
no writer-state check, and `query` resolves its own window literals, so its
bounds are not the ones the measured run used.

HOW EACH FORMAT IS READ, AND WHY THEY ARE NOT THE SAME PATH
-----------------------------------------------------------
  FORMAT=iceberg  iceberg_scan('<metadata.json>') — the native Iceberg reader,
                  against a metadata.json resolved from Glue. Resolved per
                  invocation here (the runner re-resolves per query) because only
                  a few metadata.json files are retained and the writer commits
                  one every ~60 s, so a stale location 404s mid-scan.

  FORMAT=paimon   read_parquet(<hive-partitioned glob>) — DuckDB has NO Paimon
                  reader. What makes the Paimon side readable anyway is that this
                  table is APPEND-ONLY: verified zero deletion-vector indexes
                  under the warehouse's /index/ prefix, so the data files are
                  plain Hive-partitioned Parquet with no merge state to apply.

READING THE PAIMON TABLE *AS ICEBERG* (the ICEBERG_COMPAT=true deployment)
--------------------------------------------------------------------------
With ICEBERG_COMPAT=true the Paimon table also publishes an Iceberg-compat
metadata tree, and the Glue entry for flink_cid_paimon_db.cid_events carries a
`metadata_location`. So the SAME physical table can be read BOTH ways:

    FORMAT=paimon                                 -> read_parquet (no manifest)
    FORMAT=iceberg ICEBERG_DB=flink_cid_paimon_db -> iceberg_scan (manifest)

That pairing is the one worth running, because both sides then read
byte-identical data files and the ONLY difference is the planning path. The
exported metadata's `location` points back into Paimon's own data directory, so
the manifests reference Paimon's Parquet files directly; nothing is copied.

PIN_SNAPSHOT=1 IS MANDATORY ON THAT PATH — Paimon rewrites and GCs its exported
metadata.json every commit (~60 s), far more aggressively than a native Iceberg
table. See pin_metadata().

DO NOT REPORT A DuckDB PAIMON-VS-ICEBERG NUMBER AS A FORMAT RESULT. It is a
READ-PATH result, and the asymmetry does not favour one side uniformly:
  - read_parquet SKIPS manifest planning. It pays an S3 LIST instead and prunes
    only by the dt= directory, not by column statistics.
  - iceberg_scan reads manifests to enumerate files: metadata I/O, then
    file-level pruning.
  - read_parquet reads whatever is in the prefix RIGHT NOW, whereas iceberg_scan
    is pinned to a snapshot, so with writers live the Paimon side can see
    slightly more data and can race a file mid-upload.
The format comparison that holds is StarRocks', which uses a native reader on
both sides.

Credentials: DuckDB's credential_chain CANNOT consume IRSA — both CHAIN 'sts'
("only supported with an ASSUME_ROLE_ARN value") and CHAIN 'config' ("Secret
Validation Failure") fail in this pod even with AWS_ROLE_ARN and
AWS_WEB_IDENTITY_TOKEN_FILE set. boto3 resolves the web-identity token fine, so
credentials are resolved there and injected as an explicit secret. Do not
"simplify" this back to credential_chain.
"""
import datetime
import os
import subprocess
import sys

import boto3
import duckdb

REGION = os.environ.get("AWS_REGION", "us-west-2")
FORMAT = os.environ.get("FORMAT", "iceberg").lower()
if FORMAT not in ("iceberg", "paimon"):
    sys.exit(f"ERROR: FORMAT must be 'iceberg' or 'paimon', got {FORMAT!r}")
BUCKET = os.environ.get("BUCKET_NAME") or sys.exit("ERROR: BUCKET_NAME is required")
ICEBERG_DB = os.environ.get("ICEBERG_DB", "flink_cid_iceberg_db")
TABLE = os.environ.get("TABLE_NAME", "cid_events")
PAIMON_TABLE_ROOT = os.environ.get(
    "PAIMON_TABLE_ROOT",
    f"s3://{BUCKET}/cid-warehouse-paimon/flink_cid_paimon_db.db/{TABLE}")
RUN_TAG = os.environ.get("RUN_TAG") or datetime.datetime.now(
    datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
PIN_SNAPSHOT = os.environ.get("PIN_SNAPSHOT", "0").lower() in ("1", "true", "yes")

MEM_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "56GB")
THREADS = int(os.environ.get("DUCKDB_THREADS", "18"))
TEMP_DIR = os.environ.get("DUCKDB_TEMP_DIR", "/tmp/duckdb-spill")
MAX_TEMP = os.environ.get("DUCKDB_MAX_TEMP_SIZE", "180GB")
BENCH_SQL_DIR = os.environ.get("BENCH_SQL_DIR", "/tmp/bench-sql")


def iceberg_metadata_location():
    """Current metadata.json from Glue.

    The runner calls this PER QUERY, not once per run, and that is a measured
    requirement rather than a style choice: only a few metadata.json files are
    retained and the writer commits one a minute, so a location cached at startup
    404s partway through a suite. One invocation of this script is short enough
    to resolve once.
    """
    glue = boto3.client("glue", region_name=REGION)
    tbl = glue.get_table(DatabaseName=ICEBERG_DB, Name=TABLE)["Table"]
    loc = tbl.get("Parameters", {}).get("metadata_location")
    if not loc:
        sys.exit(f"ERROR: {ICEBERG_DB}.{TABLE} has no metadata_location "
                 f"— not an Iceberg table?")
    return loc


def pin_metadata(loc):
    """Copy a metadata.json to a stable key and return the pinned s3:// URI.

    WHY THIS EXISTS — a MEASURED failure, not a precaution. Re-resolving the
    location per query is not sufficient when the table being read is Paimon's
    ICEBERG-COMPAT EXPORT rather than a native Iceberg table. Paimon rewrites the
    whole metadata.json on EVERY commit and deletes the previous version
    immediately, so the retention window is ONE commit interval (~60 s measured),
    not the several-snapshot window a native Iceberg table keeps. A location
    valid at resolve time is deleted while the query is still planning: measured
    6 of 10 queries failing with

      HTTPException: HTTP Error: HTTP GET error reading
      's3://.../iceberg/flink_cid_paimon_db/cid_events/metadata/v21.metadata.json'
      (HTTP 404 Not Found)

    and the version number advancing (v21, v22, v23, v26, v27) across the
    failures — i.e. each retry resolved a FRESH location and that one expired
    too. No amount of re-resolution fixes a race against a 60 s GC.

    Copying the object to a key this script owns removes the race, and it is
    semantically correct: a metadata.json fully describes one snapshot, so
    reading a pinned copy is ordinary Iceberg SNAPSHOT ISOLATION. The manifests
    and data files it references are NOT copied and are retained far longer
    (snapshot.num-retained.min=5 / snapshot.time-retained=1h on the Paimon side).
    """
    s3 = boto3.client("s3", region_name=REGION)
    src_bucket, src_key = loc.removeprefix("s3://").split("/", 1)
    dst_key = f"duckdb-cid-pinned/{RUN_TAG}/{os.path.basename(src_key)}"
    s3.copy_object(Bucket=BUCKET, Key=dst_key,
                   CopySource={"Bucket": src_bucket, "Key": src_key})
    return f"s3://{BUCKET}/{dst_key}"


def resolve_scan():
    """Return (scan_expression, provenance_dict) for the selected format."""
    if FORMAT == "iceberg":
        loc = iceberg_metadata_location()
        if PIN_SNAPSHOT:
            loc = pin_metadata(loc)
        return f"iceberg_scan('{loc}')", {"metadata_location": loc}

    # Paimon: hive_partitioning=1 is what makes `dt` a usable predicate column.
    # Without it dt is not projected at all and the filter fails to bind.
    #
    # union_by_name WAS SET HERE AND IS DELIBERATELY GONE (2026-10-05). It was
    # added to guard against a schema evolution mid-run, but it cost real time on
    # every query and the risk it guarded against does not exist for this table.
    #
    # WHAT IT COST. Name-based unification has to KNOW every file's schema to
    # build the union, so DuckDB reads the Parquet FOOTER OF EVERY FILE IN THE
    # GLOB before it applies the dt predicate — the footer read happens at bind,
    # the partition pruning after it. Reproduced locally on 1.5.4 by planting a
    # corrupt file in a partition the predicate prunes away: without the flag the
    # query returns fine (the file is never opened), with it the query dies with
    # "No magic bytes found at end of file" on that pruned file. So the flag is
    # not merely slower, it provably opens files partition pruning should have
    # excluded. It is also NOT cached — corrupting a pruned file after one
    # successful pass fails the NEXT pass on the same connection. On 400 local
    # files the tax was ~45 ms -> ~95 ms (2.1x) on a query whose own work is
    # trivial; here the glob spans tens of thousands of parquet files across two
    # dt partitions, so each hot query was reading ~12,800 footers it had no use
    # for, over S3.
    #
    # WHY DROPPING IT IS SAFE, not a trade. Paimon writes ONE file per schema
    # VERSION under the table's /schema/ prefix, and this table has exactly one:
    # schema-0, highestFieldId 33 — there has never been a second version, so no
    # data file can differ from any other. The table is append-only with a single
    # stable schema, the writers all run the same pinned SQL, and a schema change
    # would require redeploying the FlinkDeployment, which rewrites the warehouse
    # anyway.
    #
    # AND THE FAILURE MODE IS NOW LOUD RATHER THAN SILENT, which is the real
    # argument. Positional union on a genuinely evolved file set raises a binder
    # error naming the mismatch, so a schema change announces itself and the fix
    # is to add the flag back for that run. union_by_name would instead have
    # silently NULL-filled the missing column and reported a number — and a
    # SUM(STRLEN(event_json)) that is quietly short is indistinguishable from a
    # fast one. Prefer the version that cannot produce a wrong number.
    glob = f"{PAIMON_TABLE_ROOT}/dt=*/bucket-*/*.parquet"
    s3 = boto3.client("s3", region_name=REGION)
    prefix = PAIMON_TABLE_ROOT.removeprefix(f"s3://{BUCKET}/").rstrip("/") + "/"
    n, nbytes, tok = 0, 0, None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            if o["Key"].endswith(".parquet"):
                n += 1
                nbytes += o["Size"]
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    return (f"read_parquet('{glob}', hive_partitioning=1)",
            {"glob": glob, "paimon_files": n, "paimon_bytes": nbytes})


_SESSION = None


def refresh_secret(con):
    """(Re)create the s3 secret from freshly-resolved IRSA credentials.

    MEASURED FAILURE THIS FIXES. The secret used to be created once and never
    refreshed, but an IRSA web-identity session is only ~1 hour long while a full
    snapshot suite is not bounded by that. S4_wide_json (full-partition
    SUM(STRLEN(event_json))) exposed it on 2026-10-04, dying with the credential
    error spelled out rather than inferred:

        HTTP GET error reading '.../dt=2026-10-01/bucket-100/data-...parquet'
        (HTTP 400 Bad Request)
        ExpiredToken: The provided token has expired.

    It is NOT a timeout: raising QUERY_TIMEOUT_S from 900 to 3600 converted S4
    from TIMEOUT into this error, which is why no timeout value can fix it. It is
    also not a format result and must never be reported as one.

    boto3's credential object is self-refreshing for web-identity:
    get_frozen_credentials() re-resolves from the projected token file once the
    cached set is near expiry. So re-issuing the secret before each statement
    keeps DuckDB on valid credentials for anything whose INDIVIDUAL statements
    finish inside one session.

    RESIDUAL LIMITATION, stated rather than papered over: DuckDB reads the secret
    when it opens an S3 request, so a SINGLE statement lasting longer than the
    session can still expire mid-scan. This moves the ceiling from cumulative
    duration to longest single statement.
    """
    global _SESSION
    if _SESSION is None:
        _SESSION = boto3.Session()
    creds = _SESSION.get_credentials()
    if creds is None:
        sys.exit("ERROR: no AWS credentials resolvable in this pod (check IRSA).")
    c = creds.get_frozen_credentials()
    parts = ["TYPE s3", f"KEY_ID '{c.access_key}'", f"SECRET '{c.secret_key}'",
             f"REGION '{REGION}'"]
    if c.token:
        parts.append(f"SESSION_TOKEN '{c.token}'")
    # OR REPLACE so this is callable repeatedly; CREATE SECRET alone would fail
    # on the second call with "secret with name irsa already exists".
    con.execute(f"CREATE OR REPLACE SECRET irsa ({', '.join(parts)});")


def connect():
    # temp_directory MUST be explicit: for an in-memory database DuckDB has no
    # default spill location, so exceeding memory_limit KILLS the process (exit
    # 137, no traceback) rather than spilling. max_temp_directory_size keeps a
    # spill from filling the volume and getting the pod evicted for
    # ephemeral-storage instead.
    con = duckdb.connect(config={"memory_limit": MEM_LIMIT, "threads": THREADS,
                                 "temp_directory": TEMP_DIR,
                                 "max_temp_directory_size": MAX_TEMP})
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    refresh_secret(con)
    return con


def render_shared_query(name):
    """Render one query from the SHARED suites, by delegating to bench-sql-lib.sh.

    The rendering is NOT reimplemented here. bench-sql-lib.sh is the renderer for
    both engines, so calling it is what guarantees this session sees the same SQL
    the measured run did — a second implementation would be free to drift, which
    is the failure this whole redesign removes. Its guard also refuses to emit
    SQL still carrying an unresolved ${...}, which matters because such a
    placeholder is not a syntax error on either engine: it binds as an identifier
    or a string and returns a plausible wrong answer.

    The window literals are resolved FRESH, so they are not the bounds the
    measured run used. For the hot suite in particular that means a different
    (later) window.
    """
    lib = os.path.join(BENCH_SQL_DIR, "bench-sql-lib.sh")
    if not os.path.exists(lib):
        sys.exit(f"ERROR: no bench-sql-lib.sh under {BENCH_SQL_DIR}. The query "
                 f"tree is synced from $QUERYBENCH_S3 by run-duckdb-cid.sh --in-pod, "
                 f"so run that at least once in this pod, or sync it by hand: "
                 f"aws s3 sync $QUERYBENCH_S3/ {BENCH_SQL_DIR}/")
    for suite in ("hot", "snapshot"):
        path = os.path.join(BENCH_SQL_DIR, suite, f"{name}.sql")
        if os.path.exists(path):
            break
    else:
        avail = []
        for suite in ("hot", "snapshot"):
            d = os.path.join(BENCH_SQL_DIR, suite)
            if os.path.isdir(d):
                avail += [f[:-4] for f in sorted(os.listdir(d))
                          if f.endswith(".sql")]
        sys.exit(f"ERROR: no query named {name!r}. Available: "
                 f"{', '.join(avail) or '(none)'}")
    return render_file(path)


def render_file(path):
    """Render a .sql template through bench-sql-lib.sh, ${TABLE} deferred."""
    lib = os.path.join(BENCH_SQL_DIR, "bench-sql-lib.sh")
    script = (f'set -uo pipefail; source "{lib}" || exit 1; '
              f'export BENCH_BYTES_FN=STRLEN BENCH_DEFER=TABLE BENCH_TABLE=; '
              f'bench_resolve_hot_windows || exit 1; '
              f'bench_resolve_snapshot_window '
              f'"${{SNAPSHOT_DT:-$(bench_date_plus_days '
              f'"$(date -u +%Y-%m-%d)" -1)}}" || exit 1; '
              f'bench_render "{path}"')
    p = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
    if p.returncode != 0:
        sys.exit(f"ERROR rendering {path}:\n{p.stderr}")
    if p.stderr.strip():
        print(p.stderr.rstrip(), file=sys.stderr)
    return p.stdout


def show(con, sql, scan):
    sql = sql.replace("${TABLE}", scan)
    refresh_secret(con)
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in (con.description or [])]
    if cols:
        print(" | ".join(cols))
    for r in rows:
        print(" | ".join("NULL" if v is None else str(v) for v in r))
    print(f"({len(rows)} row{'' if len(rows) == 1 else 's'})")


def main(argv):
    cmd = argv[0] if argv else "scan"
    scan, prov = resolve_scan()

    if cmd == "scan":
        print(f"  format : {FORMAT}")
        print(f"  scan   : {scan}")
        for k, v in prov.items():
            print(f"  {k:<14}: {v}")
        return 0

    con = connect()
    if cmd == "sql":
        if len(argv) < 2:
            sys.exit("ERROR: sql needs a statement")
        show(con, argv[1], scan)
    elif cmd == "file":
        if len(argv) < 2:
            sys.exit("ERROR: file needs a path")
        show(con, render_file(argv[1]), scan)
    elif cmd in ("query", "explain"):
        if len(argv) < 2:
            sys.exit(f"ERROR: {cmd} needs a query name, e.g. S4_wide_json")
        sql = render_shared_query(argv[1]).strip().rstrip(";")
        print(f"-- rendered from {BENCH_SQL_DIR} (fresh windows, NOT the "
              f"measured run's bounds)")
        print(sql if cmd == "query" else f"-- EXPLAIN ANALYZE of:\n{sql}")
        print("--")
        show(con, f"EXPLAIN ANALYZE {sql}" if cmd == "explain" else sql, scan)
    elif cmd == "repl":
        print(f"-- {FORMAT}: ${{TABLE}} expands to {scan}")
        print("-- one statement per line, EOF to quit")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                show(con, line, scan)
            except Exception as e:
                print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
    else:
        sys.exit(__doc__)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
