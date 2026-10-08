#!/usr/bin/env bash
# DuckDB read benchmark over the throughput project's APPEND-ONLY cid_events
# table. ONE file, TWO roles, selected by argv:
#
#   ./run-duckdb-cid.sh [run|clean]     LOCAL: submit the Job(s), collect output
#   ./run-duckdb-cid.sh --in-pod        IN-POD: the benchmark itself
#
# ── LOCAL USAGE ─────────────────────────────────────────────────────────────
#   ./run-duckdb-cid.sh                       # native Iceberg (FORMAT=iceberg)
#   FORMAT=paimon ./run-duckdb-cid.sh         # Paimon's parquet via read_parquet
#   FORMAT=both ./run-duckdb-cid.sh           # both arms, one Job/pod/NODE each
#   SUITE=hot FORMAT=both ./run-duckdb-cid.sh # both arms CONCURRENTLY (see below)
#   PARALLEL_ARMS=0 FORMAT=both ./run-duckdb-cid.sh   # force serial
#   RUNS=5 ./run-duckdb-cid.sh
#   SUSPEND_WRITERS=0 ./run-duckdb-cid.sh     # do not touch the writers
#   DRY_RUN=1 FORMAT=both ./run-duckdb-cid.sh # render the manifests, apply nothing
#   ./run-duckdb-cid.sh clean                 # delete jobs + configmap
#
# A snapshot-scoped run runs the PRE-FLIGHT GATE first (snapshot-preflight.sh):
# it verifies no cid_events query is already running on the StarRocks FE, then
# stops both Flink writers and waits for their TaskManagers to disappear. A
# hot-only run does neither. SUSPEND_WRITERS is unset=auto / 0=skip / 1=force.
# Restarting the writers afterwards is MANUAL and no script does it.
#
# ── WHY THE ARMS RUN CONCURRENTLY FOR hot AND SERIALLY FOR snapshot ─────────
# FORMAT=both submits one INDEPENDENTLY NAMED Job per arm, each landing on its
# own m5.8xlarge. Whether the two run at the same time is a measurement choice
# with no free option, and the right answer differs per suite:
#
#   SUITE=hot      -> PARALLEL. The hot queries are bounded to TRAILING
#                     ingestionTime windows (1m/5m/10m) against a LIVE table, and
#                     each pod computes those literals from its OWN start time.
#                     Run serially, the second arm therefore measures a
#                     DIFFERENT, further-advanced table: the iceberg arm took 54
#                     min on 2026-10-05, so the paimon arm's 5-minute window
#                     opened an hour later over an hour more ingested data. That
#                     is a CORRECTNESS error — the two arms stop being comparable,
#                     which was the entire point. Running them together costs S3
#                     bandwidth contention instead, and contention is a UNIFORM
#                     TAX: both arms pay it, so the ratio survives even though
#                     neither absolute number is a clean-pipe number. A
#                     systematic bias applied equally to both arms is recoverable;
#                     two different tables are not. The two Flink writers are
#                     saturating the same S3 path throughout the hot regime
#                     anyway, so neither arm ever had an uncontended pipe to
#                     measure on.
#
#   SUITE=snapshot -> SERIAL. The writers are suspended and the dt partition is
#                     frozen (that is what makes it a snapshot run), so there is
#                     NO window to skew — an arm run an hour later reads the
#                     byte-identical file set. With the correctness argument gone,
#                     the only remaining consideration is contention, so serial
#                     wins: each arm gets the node's single NIC to itself, which
#                     matters most precisely here because S4_wide_json is
#                     bandwidth-bound at ~1.08 TiB of event_json per pass.
#
#   SUITE=all      -> SERIAL, following its snapshot half. SUITE=all is already a
#                     mixed-regime run (see the warning below) and its wall clock
#                     is dominated by S4 at 1,541 s/pass, so parallelising it
#                     would trade clean bandwidth on the half that is measurable
#                     for nothing on the half that is not.
#
# PARALLEL_ARMS overrides: unset=auto (the table above) / 0=serial / 1=force.
#
# Each arm's Job name, pod, log and JSON are per-arm; the ConfigMap is SHARED on
# purpose (both arms run byte-identical code, and a mounted ConfigMap is
# read-only to the pod, so concurrent readers are safe — duplicating it per arm
# would just add a way for the two arms to run different code). The two arms of
# one invocation share a single STAMP, so a matched pair is obvious from the
# filenames alone and a parallel pair's shared time window is self-evidencing.
#
# The code ships via ConfigMap rather than a baked image — the same pattern the
# Flink jobs use for flink-cid-executor.py — so editing this file is enough to
# change the next run, with no ECR push for a single script.
#
# ── IN-POD ROLE: WHY THE RUNNER IS SHELL AND NOT PYTHON ─────────────────────
# The queries are the measurement, so they are authored in
# ../querybench-script/{hot,snapshot}/ as plain .sql files, published to S3 by
# that directory's bench-sql.sh upload, and synced from there — the StarRocks
# harness syncs THE SAME OBJECTS. So the runner is the duckdb CLI driven by a
# rendered .sql script, and its only job is to assemble that script, time it, and
# report. There is no Python in the query path at all.
#
# WHAT WAS LOST BY DROPPING THE PYTHON RUNNER, AND HOW IT IS REPLACED:
#
#   per-query Glue resolve   The Iceberg metadata_location moves (see below), so
#                            it is resolved by `aws glue get-table` here, once
#                            per query, outside the timed region. Same cadence as
#                            before, no Python.
#   credential injection     DuckDB's credential_chain CANNOT consume IRSA (both
#                            CHAIN 'sts' and CHAIN 'config' fail in this pod even
#                            with AWS_ROLE_ARN/AWS_WEB_IDENTITY_TOKEN_FILE set).
#                            Credentials are therefore resolved with
#                            `aws sts assume-role-with-web-identity` via the CLI
#                            and written into a CREATE OR REPLACE SECRET. Do not
#                            "simplify" this to credential_chain.
#   secret refresh per query An IRSA web-identity session is ~1 h and a snapshot
#                            pass can run 25 min, so a secret minted once at
#                            startup expires mid-run — measured as
#                            "ExpiredToken: The provided token has expired" on a
#                            long S4, which no timeout value can fix. The secret
#                            is re-issued in every per-query prelude.
#                            RESIDUAL LIMIT, unchanged: a SINGLE query longer
#                            than the session can still expire mid-scan. This
#                            moves the ceiling from cumulative run duration to
#                            longest single query.
#   watchdog timeout         `timeout` kills the duckdb process. The old
#                            con.interrupt() was finer-grained but a dead CLI
#                            process cannot contend with the next query either,
#                            which is what that mattered for.
#   zero-scan guard          Implemented below over the CLI's output, with the
#                            exemptions read from the .sql files themselves
#                            (`-- bench: allow-zero-rows` /
#                            `leading-col-is-label`) rather than hardcoded.
#
# ── HOW EACH FORMAT IS READ, AND WHY THEY ARE NOT THE SAME PATH ─────────────
#   FORMAT=iceberg  iceberg_scan('<metadata.json>') — the native Iceberg reader,
#                   against a metadata.json resolved from Glue immediately BEFORE
#                   each timed statement. Re-resolved per query, not pinned once
#                   per run, and that is MEASURED: this table retains only ~4
#                   metadata.json files and the writer commits a new one every
#                   ~60 s, so a location resolved at startup is DELETED within
#                   minutes. A pinned-at-startup run failed 6 of 10 queries with
#                   "HTTP 404 Not Found" on the metadata object.
#
#                   The cost: a run is no longer ONE snapshot. Each query is
#                   internally consistent (its passes share one location) but a
#                   later query sees a later snapshot. The location is recorded
#                   per query so that drift is visible instead of assumed away.
#
#   FORMAT=paimon   read_parquet(<hive-partitioned glob>) — DuckDB has NO Paimon
#                   reader. What makes the Paimon side measurable anyway is that
#                   this table is APPEND-ONLY: verified zero deletion-vector
#                   indexes under the warehouse's /index/ prefix, so the data
#                   files are plain Hive-partitioned Parquet with no merge state
#                   to apply and no manifest needed for correctness.
#
# DO NOT REPORT A DuckDB PAIMON-VS-ICEBERG NUMBER AS A FORMAT RESULT. It is a
# READ-PATH result, and the asymmetry does not favour one side uniformly:
#   - read_parquet SKIPS manifest planning entirely. It pays an S3 LIST instead
#     and cannot prune by column statistics — only by the dt= directory.
#   - iceberg_scan reads manifests to enumerate files: metadata I/O, but then
#     file-level pruning.
#   - read_parquet reads whatever is in the prefix RIGHT NOW, whereas
#     iceberg_scan is pinned to a snapshot. With writers live the Paimon side can
#     see slightly more data and can race a file mid-upload.
# The format comparison that holds is StarRocks', which uses a native reader on
# both sides.
#
# union_by_name IS DELIBERATELY NOT SET on the Paimon glob. Name-based
# unification must KNOW every file's schema, so DuckDB reads the Parquet FOOTER
# OF EVERY FILE IN THE GLOB at bind — before the dt predicate is applied. The
# glob spans tens of thousands of files across two dt partitions, so each hot
# query was reading ~12,800 footers it had no use for, over S3, and it is not
# cached across passes. Reproduced on 1.5.4: with the flag, a query dies on a
# corrupt file that partition pruning should have excluded. It is safe to omit
# because Paimon writes one file per schema VERSION and this table has exactly
# one (schema-0, highestFieldId 33). And positional union FAILS LOUDLY on a
# genuinely evolved file set, whereas union_by_name would NULL-fill silently —
# and a SUM(STRLEN(event_json)) that is quietly short is indistinguishable from
# a fast one.

# ── Mode dispatch ───────────────────────────────────────────────────────────
# Before `set`, because the two roles need DIFFERENT options and the choice has
# to be made first. The pod half runs with -e OFF deliberately: run_attempt()
# returns the duckdb process's exit code as data (137 is a timeout, which is a
# RESULT, not a crash), and under -e the first slow query would kill the suite
# instead of being recorded as TIMEOUT.
CID_MODE=local
if [[ "${1:-}" == "--in-pod" ]]; then
    CID_MODE=pod
    shift
fi
if [[ "$CID_MODE" == pod ]]; then
    set -uo pipefail
else
    set -euo pipefail
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Shared config ───────────────────────────────────────────────────────────
# Everything below here is read by BOTH roles. This block is the reason the two
# scripts are one: each item was previously derived twice, once per file.

# BUCKET_NAME. Locally it comes from ../.bench-env so this and
# build-deploy-cid.sh read the same value; in the pod it is an env var set by the
# manifest. The manifest carries a ${BUCKET_NAME} placeholder rather than a
# hardcoded string — a hardcoded account number is how the repo's root-level
# files went stale. (.bench-env does not exist in the pod, hence the -f test.)
# shellcheck disable=SC1091
[[ -f "${SCRIPT_DIR}/../.bench-env" ]] && source "${SCRIPT_DIR}/../.bench-env"
: "${BUCKET_NAME:?BUCKET_NAME is required (set it or populate ../.bench-env)}"

# The SHARED query suites, in S3. Not a copy and not a local directory:
# starrocks-cid-audit.sh syncs this SAME prefix, which is what makes a
# divergence between the two engines' measurements impossible rather than merely
# discouraged. Published by querybench-script/bench-sql.sh upload. The local role
# only passes the URI to the pod — it does not download the queries itself and
# checks no local path, so a repo rename cannot break a run.
QUERYBENCH_S3="${QUERYBENCH_S3:-s3://${BUCKET_NAME}/flink/querybench}"
# A trailing slash would make the fetch ask for ".../querybench//hot/H1.sql",
# a DIFFERENT and non-existent key in S3 rather than a path to normalise away.
QUERYBENCH_S3="${QUERYBENCH_S3%/}"

SUITE_SEL="${SUITE:-all}"
ICEBERG_DB="${ICEBERG_DB:-flink_cid_iceberg_db}"
TABLE_NAME="${TABLE_NAME:-cid_events}"
NAMESPACE="${NAMESPACE:-emr-flink}"
REGION="${AWS_REGION:-us-west-2}"

# Hard wall-clock cap per attempt. Not hypothetical: an unbounded aggregate on
# this data does not come back, and without a cap one pathological query hangs
# the whole suite with no result at all.
#
# SUITE-DEPENDENT, because the two suites' slowest legitimate queries are an
# order of magnitude apart and no single cap is both a protection for one and a
# non-truncation for the other:
#   hot            — slowest is H5 at tens of seconds, so 1800 s is already a
#                    wide margin and a lower cap surfaces a genuine hang sooner.
#   all | snapshot — S4 is in scope at 1,541 s measured here, so 1800 s is only
#                    ~1.17x one observation; 3600 s is the smallest cap that does
#                    not truncate a slower or contended pass.
#
# THIS RULE IS DUPLICATED in starrocks-cid-audit.sh's QUERY_TIMEOUT and must stay
# identical: a cap that fires on one engine but not the other turns a
# cross-engine comparison into a comparison of the two caps. Change both or
# neither. An explicit QUERY_TIMEOUT_S wins over both.
cid_query_timeout() {   # $1 = suite
    [[ "$1" == hot ]] && { echo 1800; return; }
    echo 3600
}

# PIN_SNAPSHOT=1 copies the metadata.json to a key this script owns and reads
# that instead of the live location.
#
# IT DEFAULTS TO 1 ON THE ICEBERG ARM, and that is not a precaution: this table
# retains ~4 metadata.json files and the writer commits one every ~60 s, so a
# location valid at resolve time is deleted while the query is still planning. On
# 2026-10-05 an unpinned iceberg arm lost H5 and H6 to "HTTP 404 Not Found" on
# metadata.json; with PIN_SNAPSHOT=1 both completed. It is REQUIRED when
# ICEBERG_DB points at Paimon's Iceberg-compat export, whose metadata.json lives
# for only one commit interval. It is also the more correct thing to MEASURE — a
# metadata.json fully describes one snapshot, so reading a pinned copy is
# ordinary Iceberg snapshot isolation, and it fixes the data under test so the
# three passes of a query are passes over the same bytes rather than over a
# moving table. That matters more, not less, under a parallel launch, where the
# whole point is that the two arms see comparable data.
#
# The paimon arm has no metadata to pin (read_parquet takes a glob), so 0.
# An explicit PIN_SNAPSHOT in the environment wins for both arms.
#
# THIS USED TO DISAGREE WITH ITSELF. The submitter applied the rule above while
# the in-pod runner defaulted to 0, so a hand-run pod invocation measured the
# unpinned path the submitter existed to avoid. One function, one answer.
cid_pin_default() {   # $1 = format
    [[ "$1" == iceberg ]] && { echo 1; return; }
    echo 0
}

# ════════════════════════════════════════════════════════════════════════════
# IN-POD ROLE
# ════════════════════════════════════════════════════════════════════════════
if [[ "$CID_MODE" == pod ]]; then

FORMAT="${FORMAT:-iceberg}"
case "$FORMAT" in
    iceberg|paimon) ;;
    *) echo "ERROR: FORMAT must be iceberg|paimon, got '${FORMAT}'" >&2; exit 1 ;;
esac

SUITE="$SUITE_SEL"
RUNS="${RUNS:-3}"
WARMUP="${WARMUP:-1}"
MEM_LIMIT="${DUCKDB_MEMORY_LIMIT:-56GB}"
THREADS="${DUCKDB_THREADS:-18}"
TEMP_DIR="${DUCKDB_TEMP_DIR:-/tmp/duckdb-spill}"
MAX_TEMP="${DUCKDB_MAX_TEMP_SIZE:-180GB}"
RUN_TAG="${RUN_TAG:-$(date -u +%Y%m%dT%H%M%SZ)}"
# Default is WRITABLE, because this is a sync target rather than a mount point:
# the old /opt/bench-sql default was where the flattened ConfigMap was unpacked,
# and `aws s3 sync` into a read-only /opt would fail at startup.
BENCH_SQL_DIR="${BENCH_SQL_DIR:-/tmp/bench-sql}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
WORK="${WORK_DIR:-/tmp/bench-work}"

PAIMON_TABLE_ROOT="${PAIMON_TABLE_ROOT:-s3://${BUCKET_NAME}/cid-warehouse-paimon/flink_cid_paimon_db.db/${TABLE_NAME}}"
# Used only by the oldest-dt lookup. The Iceberg read path goes through
# metadata.json and never needs this.
ICEBERG_TABLE_ROOT="${ICEBERG_TABLE_ROOT:-s3://${BUCKET_NAME}/cid-warehouse-iceberg/${ICEBERG_DB}.db/${TABLE_NAME}}"

PIN_SNAPSHOT="${PIN_SNAPSHOT:-$(cid_pin_default "$FORMAT")}"
# `tr`, not ${var,,}: case-modifying expansion is bash 4+ and macOS ships bash
# 3.2 as /bin/bash. The pod image has bash 5, but this script is also run from a
# developer checkout, and a bad substitution under `set -uo pipefail` (no -e) is
# fatal to the enclosing compound command WITHOUT failing the script — measured:
# it skipped the entire query loop and still exited 0, printing an empty summary
# and `"results": {}`. A benchmark that reports nothing and succeeds is worse
# than one that crashes.
case "$(printf '%s' "$PIN_SNAPSHOT" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes) PIN_SNAPSHOT=1 ;;
    *)          PIN_SNAPSHOT=0 ;;
esac

QUERY_TIMEOUT_S="${QUERY_TIMEOUT_S:-$(cid_query_timeout "$SUITE")}"

mkdir -p "$WORK" "$TEMP_DIR"

# ── Fetch the query tree from S3 ────────────────────────────────────────────
# THE QUERIES ARE AN S3 ARTIFACT, NOT A MOUNT AND NOT A LOCAL DIRECTORY.
# starrocks-cid-audit.sh syncs this SAME prefix, so "both engines ran the same
# queries" is a property of one object set rather than of two copies that happen
# to agree.
#
# This replaced a flattened ConfigMap: a ConfigMap key cannot contain a "/", so
# the tree had to ship as hot__H1_freshness.sql and be split back into
# directories here — bench_query_files() globs per-suite directories, and that
# glob's lexical order over the H<n>/S<n> prefixes IS the execution order. S3
# keys carry the "/", so the tree arrives already shaped.
#
# Skipped entirely when BENCH_SQL_DIR already holds the library, which is what
# makes this role runnable from a developer checkout with a local tree.
if [[ ! -f "${BENCH_SQL_DIR}/bench-sql-lib.sh" ]]; then
    echo "Syncing query suites from ${QUERYBENCH_S3}/ ..."
    mkdir -p "$BENCH_SQL_DIR"
    aws s3 sync "${QUERYBENCH_S3}/" "${BENCH_SQL_DIR}/" --region "$REGION" \
        --exclude '*' --include 'bench-sql-lib.sh' \
        --include 'hot/*.sql' --include 'snapshot/*.sql' --only-show-errors || {
        echo "ERROR: cannot sync the query suites from ${QUERYBENCH_S3}/" >&2
        exit 1; }
fi

# ASSERTED, NOT ASSUMED: `aws s3 sync` of an empty or misspelled prefix succeeds
# with no output and exit 0. That would leave an empty suite, and an empty suite
# is indistinguishable from a suite whose queries all returned instantly — the
# same empty-result-looks-like-a-win failure the guards below exist to catch.
_qb_hot=$(ls -1 "${BENCH_SQL_DIR}/hot"/*.sql 2>/dev/null | grep -c .)
_qb_snap=$(ls -1 "${BENCH_SQL_DIR}/snapshot"/*.sql 2>/dev/null | grep -c .)
if [[ ! -f "${BENCH_SQL_DIR}/bench-sql-lib.sh" ]] || (( _qb_hot == 0 || _qb_snap == 0 )); then
    echo "ERROR: ${QUERYBENCH_S3}/ is missing or incomplete" >&2
    echo "  got: bench-sql-lib.sh=$([[ -f "${BENCH_SQL_DIR}/bench-sql-lib.sh" ]] && echo yes || echo NO)" \
         "hot=${_qb_hot} snapshot=${_qb_snap}" >&2
    echo "  Publish it from a checkout:" >&2
    echo "    cd querybench-script && ./bench-sql.sh upload" >&2
    exit 1
fi
echo "  ✓ ${_qb_hot} hot + ${_qb_snap} snapshot queries -> ${BENCH_SQL_DIR}"

# shellcheck disable=SC1091
source "${BENCH_SQL_DIR}/bench-sql-lib.sh" || {
    echo "ERROR: cannot source ${BENCH_SQL_DIR}/bench-sql-lib.sh" >&2
    exit 1; }

# ── TWO MEASURED CONSTRAINTS SHAPE EVERY QUERY (same as the StarRocks side) ──
#  1. Each table is ~8 TiB / ~9.7e9 rows with writers running. Unbounded
#     aggregates do not return in minutes — two StarRocks probes had to be
#     killed. Every query is bounded by dt, the sole partition column.
#  2. All timestamps in the data are UTC. Every bound is rendered as a UTC
#     literal by bench_resolve_*; a local-time slip would select an empty
#     partition, and an empty scan is the FASTEST possible result, so it would
#     read as a win rather than as a failure.

# ── The snapshot partition ──────────────────────────────────────────────────
# MUST be a CLOSED partition. The generator never writes a single dt:
# CID_DAY_SPREAD=N draws r_day from [0, N] INCLUSIVE, so the default N=1 keeps
# TWO live dt values and "yesterday" is STILL BEING WRITTEN.
#
# THE YESTERDAY-UTC DEFAULT IS GONE (2026-10-06) — it was wrong in BOTH
# directions and both were silent. The writers ran on 2026-10-05 and left
# dt={2026-10-04, 2026-10-05}: run this on 2026-10-06 and "yesterday" resolves to
# the NEWEST and therefore LIVE partition; run it on 2026-10-07 and it resolves
# to a dt that does not exist, which prunes to zero files and returns instantly —
# a spectacular-looking win that measures nothing.
#
# The OLDEST dt is the one guaranteed closed, because the r_day spread keeps the
# newest live. So the default comes from the DATA: one S3 LIST of the dt=
# prefixes with --delimiter / (the handful of partition prefixes, NOT the ~62,000
# files beneath them). If the lookup fails this falls back to yesterday-UTC and
# SAYS SO rather than pretending.
#
# ROOTS ARE PROBED IN ORDER, because FORMAT alone does not identify the layout.
# The native Iceberg table nests data under /data/; Paimon puts dt= directly
# under the table root; and the THIRD arm — FORMAT=iceberg with
# ICEBERG_DB=flink_cid_paimon_db, the Iceberg-compat export — is read by the
# Iceberg reader but its data files live in PAIMON's warehouse, so the
# Iceberg-layout root does not exist for it. A single-root version returned
# nothing for that arm and fell through to the yesterday fallback, i.e. to the
# live partition: the precise silent failure the oldest-dt rule prevents.
_dts_under() {   # $1 = s3://bucket/prefix
    local p="${1#s3://${BUCKET_NAME}/}"
    aws s3api list-objects-v2 --bucket "$BUCKET_NAME" --region "$REGION" \
        --prefix "${p%/}/" --delimiter / \
        --query 'CommonPrefixes[].Prefix' --output text 2>/dev/null \
      | tr '\t' '\n' | sed -n 's|.*/dt=\([^/]*\)/*$|\1|p'
}
_oldest_dt() {
    local roots root dts
    if [[ "$FORMAT" == paimon ]]; then
        roots=("$PAIMON_TABLE_ROOT")
    else
        roots=("${ICEBERG_TABLE_ROOT}/data" "$PAIMON_TABLE_ROOT")
    fi
    for root in "${roots[@]}"; do
        dts=$(_dts_under "$root" | sort | head -1)
        [[ -n "$dts" ]] && { echo "$dts"; return 0; }
    done
    return 1
}

if [[ -n "${SNAPSHOT_DT:-}" ]]; then
    SNAP_DT="$SNAPSHOT_DT"
    SNAP_DT_SRC="explicit SNAPSHOT_DT override"
elif SNAP_DT=$(_oldest_dt); then
    SNAP_DT_SRC="oldest dt= prefix under the table root"
else
    SNAP_DT=$(bench_date_plus_days "$(date -u +%Y-%m-%d)" -1)
    SNAP_DT_SRC="FALLBACK: yesterday-in-UTC (S3 partition listing failed)"
fi

bench_resolve_hot_windows || exit 1
bench_resolve_snapshot_window "$SNAP_DT" || exit 1

# ── Dialect ─────────────────────────────────────────────────────────────────
# STRLEN, not LENGTH: DuckDB's LENGTH counts CHARACTERS while StarRocks' counts
# BYTES. They agree only on pure ASCII, which is true of the generator's padding
# today and must not be relied on, because payload_bytes is the cross-engine
# equality check that proves both engines read the same rows. NOT octet_length()
# — in DuckDB that is BLOB/BIT-only and a VARCHAR argument is a BinderException.
export BENCH_BYTES_FN="STRLEN"
# ${TABLE} is resolved PER QUERY (the Iceberg metadata_location moves), so it is
# deferred through the render and substituted at the last moment.
export BENCH_DEFER="TABLE"
export BENCH_TABLE=""

# ── Credentials ─────────────────────────────────────────────────────────────
# Written to a file the duckdb prelude reads, so the keys never appear in a
# process argument list or in this script's own output.
SECRET_SQL="${WORK}/secret.sql"
: > "$SECRET_SQL"
chmod 600 "$SECRET_SQL"

refresh_secret() {
    local json ak sk tok
    # The CLI resolves the IRSA web-identity token from
    # AWS_ROLE_ARN/AWS_WEB_IDENTITY_TOKEN_FILE and returns a fresh session.
    json=$(aws sts assume-role-with-web-identity \
             --role-arn "${AWS_ROLE_ARN:?AWS_ROLE_ARN not set — IRSA not configured on this pod}" \
             --role-session-name "duckdb-cid-${RUN_TAG}" \
             --web-identity-token "$(cat "${AWS_WEB_IDENTITY_TOKEN_FILE:?AWS_WEB_IDENTITY_TOKEN_FILE not set}")" \
             --region "$REGION" --output json 2>/dev/null) || {
        echo "ERROR: assume-role-with-web-identity failed — cannot refresh S3 credentials" >&2
        return 1; }
    ak=$(printf '%s' "$json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["AccessKeyId"])')
    sk=$(printf '%s' "$json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["SecretAccessKey"])')
    tok=$(printf '%s' "$json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["SessionToken"])')
    [[ -n "$ak" && -n "$sk" ]] || { echo "ERROR: empty credentials from STS" >&2; return 1; }
    # OR REPLACE so this is callable repeatedly; CREATE SECRET alone fails the
    # second time with "secret with name irsa already exists".
    cat > "$SECRET_SQL" <<EOF
CREATE OR REPLACE SECRET irsa (TYPE s3, KEY_ID '${ak}', SECRET '${sk}', SESSION_TOKEN '${tok}', REGION '${REGION}');
EOF
    chmod 600 "$SECRET_SQL"
}

# ── Session prelude ─────────────────────────────────────────────────────────
# temp_directory MUST be explicit: for an in-memory database DuckDB has NO
# default spill location, so exceeding memory_limit KILLS the process (exit 137,
# no traceback) instead of spilling. max_temp_directory_size keeps a spill from
# filling the volume and getting the pod evicted for ephemeral-storage instead.
#
# memory_limit stays BELOW the container limit: DuckDB accounts for its buffer
# manager but not for Arrow/httpfs buffers, so an at-parity setting gets the
# process SIGKILLed by the kubelet.
PRELUDE="${WORK}/prelude.sql"
cat > "$PRELUDE" <<EOF
SET memory_limit='${MEM_LIMIT}';
SET threads=${THREADS};
SET temp_directory='${TEMP_DIR}';
SET max_temp_directory_size='${MAX_TEMP}';
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;
EOF

# ── The scan expression ─────────────────────────────────────────────────────
iceberg_metadata_location() {
    local loc
    loc=$(aws glue get-table --database-name "$ICEBERG_DB" --name "$TABLE_NAME" \
            --region "$REGION" \
            --query 'Table.Parameters.metadata_location' --output text 2>/dev/null)
    [[ -n "$loc" && "$loc" != None ]] || {
        echo "ERROR: ${ICEBERG_DB}.${TABLE_NAME} has no metadata_location — not an Iceberg table?" >&2
        return 1; }
    echo "$loc"
}

# Copy a metadata.json to a stable key and return the pinned s3:// URI.
#
# WHY THIS EXISTS — a MEASURED failure, not a precaution. Re-resolving per query
# is NOT sufficient when the table is Paimon's ICEBERG-COMPAT EXPORT: Paimon
# rewrites the whole metadata.json on EVERY commit and deletes the previous
# version immediately, so the retention window is ONE commit interval (~60 s),
# not the several-snapshot window a native Iceberg table keeps. A location valid
# at resolve time is deleted while the query is still planning — measured as 6 of
# 10 queries failing with HTTP 404 on metadata.json, the version advancing
# (v21, v22, v23, v26, v27) across the failures, i.e. each retry resolved a FRESH
# location and that one expired too. No amount of re-resolution beats a 60 s GC.
#
# Copying to a key this script owns removes the race, and it is semantically
# correct: a metadata.json fully describes one snapshot, so reading a pinned copy
# is ordinary Iceberg SNAPSHOT ISOLATION. The manifests and data files it
# references are NOT copied and are retained far longer
# (snapshot.num-retained.min=5 / snapshot.time-retained=1h on the Paimon side).
pin_metadata() {
    local loc="$1" src bkt key dst
    src="${loc#s3://}"; bkt="${src%%/*}"; key="${src#*/}"
    dst="duckdb-cid-pinned/${RUN_TAG}/$(basename "$key")"
    aws s3api copy-object --bucket "$BUCKET_NAME" --key "$dst" \
        --copy-source "${bkt}/${key}" --region "$REGION" >/dev/null 2>&1 || {
        echo "ERROR: could not pin ${loc}" >&2; return 1; }
    echo "s3://${BUCKET_NAME}/${dst}"
}

PAIMON_FILES=0; PAIMON_BYTES=0
if [[ "$FORMAT" == paimon ]]; then
    PAIMON_GLOB="${PAIMON_TABLE_ROOT}/dt=*/bucket-*/*.parquet"
    # Recorded so drift is visible rather than assumed away: read_parquet reads
    # whatever is in the prefix at query time, so with the writers live the
    # Paimon arm can see more files than it started with.
    read -r PAIMON_FILES PAIMON_BYTES < <(
        aws s3 ls --recursive --summarize "${PAIMON_TABLE_ROOT%/}/" \
            --region "$REGION" 2>/dev/null \
          | awk '/Total Objects:/{o=$3} /Total Size:/{b=$3} END{print o+0, b+0}')
    # An empty read leaves these unset under `read`, and the JSON block below
    # casts them with int(), which would die on "".
    PAIMON_FILES="${PAIMON_FILES:-0}"; PAIMON_BYTES="${PAIMON_BYTES:-0}"
fi

scan_expr() {   # $1 = metadata location (iceberg only)
    if [[ "$FORMAT" == iceberg ]]; then
        echo "iceberg_scan('$1')"
    else
        # hive_partitioning=1 is what makes `dt` a usable predicate column.
        # Without it dt is not projected at all and the filter fails to bind.
        echo "read_parquet('${PAIMON_GLOB}', hive_partitioning=1)"
    fi
}

# ── One timed attempt ───────────────────────────────────────────────────────
# Returns elapsed ms on stdout; the query's own result rows go to $2.
#
# TIMING IS MEASURED AROUND THE duckdb PROCESS, not parsed from `.timer`. The
# CLI prints one "Run Time" line PER STATEMENT, including every prelude SET, so
# attributing one of them to the query under test means counting lines and
# trusting that count — the same positional-alignment trap the query files exist
# to remove. Process wall time over a prelude of a few microseconds-to-ms SETs
# (measured: ~0.003 s for the whole prelude, ~0.3 s including INSTALL/LOAD from
# the extension cache) is the honest number, and it is what a user of the engine
# experiences. The prelude cost is CONSTANT across arms and passes, so it cannot
# bias a ratio.
#
# THE QUERY'S ROWS ARE CAPTURED VIA `.output`, NOT BY REDIRECTING STDOUT.
# Every statement in the session prints to stdout, and CREATE SECRET prints a
# one-row "true" — so a plain `> "$outf"` captured THAT as the query result.
# Measured: every query reported `first_row=true` and `rows=1`, which silently
# disarms the zero-scan guard (the leading value is "true", never "0") AND makes
# the recorded first_row useless as the cross-engine equality check, which is the
# only thing proving both engines read the same rows. A `.output` issued AFTER
# the prelude and the secret means $outf holds exactly the rows of the one
# statement under test. Errors still go to stderr and still exit 1 — verified
# with .output active, so the rc check below is unaffected.
RUNNER_REDIR="${WORK}/redirect.sql"
run_attempt() {   # $1 = sql file, $2 = out file -> prints elapsed_ms, rc in $?
    local sqlf="$1" outf="$2" st en
    printf '.output %s\n' "$outf" > "$RUNNER_REDIR"
    : > "$outf"
    st=$(date -u +%s%N)
    timeout --signal=KILL "${QUERY_TIMEOUT_S}" \
        "$DUCKDB_BIN" -init /dev/null -noheader -list \
            -f "$PRELUDE" -f "$SECRET_SQL" -f "$RUNNER_REDIR" -f "$sqlf" \
            >/dev/null 2>"${outf}.err"
    local rc=$?
    en=$(date -u +%s%N)
    echo $(( (en - st) / 1000000 ))
    return $rc
}

median() { sort -n | awk '{a[NR]=$1} END{ if(NR==0){print "";exit}
    if(NR%2) printf "%d\n", a[(NR+1)/2]; else printf "%d\n", (a[NR/2]+a[NR/2+1])/2 }'; }

# ── Header ──────────────────────────────────────────────────────────────────
LABEL="native Iceberg via iceberg_scan"
[[ "$FORMAT" == paimon ]] && LABEL="Paimon via read_parquet (NOT a Paimon reader — see header)"
echo "============================================================================"
echo "  DuckDB cid_events read benchmark — ${LABEL}"
echo "  queries: ${BENCH_SQL_DIR}/{hot,snapshot}/*.sql — shared verbatim with starrocks-cid-audit.sh"
echo "============================================================================"
printf '  duckdb      : %s\n' "$("$DUCKDB_BIN" --version 2>/dev/null)"
printf '  format      : %s\n' "$FORMAT"
printf '  region      : %s\n' "$REGION"
printf '  memory/thr  : %s / %s\n' "$MEM_LIMIT" "$THREADS"
printf '  spill       : %s (max %s)\n' "$TEMP_DIR" "$MAX_TEMP"
printf '  pin_snapshot: %s\n' "$PIN_SNAPSHOT"
if [[ "$FORMAT" == paimon ]]; then
    printf '  paimon glob : %s\n' "$PAIMON_GLOB"
    printf '  files/bytes : %s / %s  (at start; read_parquet sees the prefix LIVE)\n' \
        "$PAIMON_FILES" "$PAIMON_BYTES"
fi
printf '  dt source   : %s\n' "$SNAP_DT_SRC"
printf '  suite       : %s   runs: %s (+%s warmup, discarded)\n' "$SUITE" "$RUNS" "$WARMUP"
printf '  timeout     : %ss per attempt\n' "$QUERY_TIMEOUT_S"
echo "============================================================================"
echo ""

SUITES=()
[[ "$SUITE" == all || "$SUITE" == hot      ]] && SUITES+=(hot)
[[ "$SUITE" == all || "$SUITE" == snapshot ]] && SUITES+=(snapshot)
(( ${#SUITES[@]} > 0 )) || { echo "ERROR: SUITE must be all|hot|snapshot, got '${SUITE}'" >&2; exit 1; }

for s in "${SUITES[@]}"; do bench_print_plan "$s"; done

# Render EVERYTHING before the first timed query, so a template error aborts now
# rather than twenty minutes in.
for s in "${SUITES[@]}"; do
    while IFS= read -r f; do
        bench_render "$f" > "${WORK}/$(bench_query_name "$f").tmpl" \
          || { echo "ERROR: ${f} failed to render" >&2; exit 1; }
    done < <(bench_query_files "$s")
done

# ── Run ─────────────────────────────────────────────────────────────────────
RESULTS="${WORK}/results.tsv"      # name \t suite \t status \t median_ms \t rows \t first_row \t meta
: > "$RESULTS"

for s in "${SUITES[@]}"; do
    echo "-- ${s} suite ----------------------------------------------------"
    while IFS= read -r qfile; do
        name=$(bench_query_name "$qfile")
        tmpl="${WORK}/${name}.tmpl"
        meta="-"

        # Resolve the Iceberg metadata location PER QUERY (see the header).
        # Outside the timed region: it is one Glue call and must not be
        # attributed to query cost.
        if [[ "$FORMAT" == iceberg ]]; then
            if ! meta=$(iceberg_metadata_location); then
                printf '  %-22s ERROR resolving metadata\n' "$name"
                printf '%s\t%s\tERROR\t\t\t\t-\n' "$name" "$s" >> "$RESULTS"
                continue
            fi
            if (( PIN_SNAPSHOT == 1 )) && ! meta=$(pin_metadata "$meta"); then
                printf '  %-22s ERROR pinning metadata\n' "$name"
                printf '%s\t%s\tERROR\t\t\t\t-\n' "$name" "$s" >> "$RESULTS"
                continue
            fi
        fi

        # Substitute the deferred ${TABLE} now that the scan expression is known.
        # Pure bash, NOT sed: the value contains / and ' and (for Paimon) a *
        # glob, and a replacement containing & is special to sed and awk.
        qsql="${WORK}/${name}.sql"
        tmpl_text=$(cat "$tmpl")
        printf '%s\n' "${tmpl_text//\$\{TABLE\}/$(scan_expr "$meta")}" > "$qsql"
        if grep -qE '\$\{[A-Z0-9_]+\}' "$qsql"; then
            echo "  ${name} ERROR: placeholders still unresolved after TABLE substitution:" >&2
            grep -oE '\$\{[A-Z0-9_]+\}' "$qsql" | sort -u >&2
            printf '%s\t%s\tERROR\t\t\t\t%s\n' "$name" "$s" "$meta" >> "$RESULTS"
            continue
        fi

        status=ok; times=(); nrows=""; first="-"
        out="${WORK}/${name}.out"
        for (( i = 0; i < WARMUP + RUNS; i++ )); do
            # Re-issue S3 credentials immediately before each attempt — see
            # refresh_secret(). Outside the timed region.
            refresh_secret || { status=ERROR; break; }
            ms=$(run_attempt "$qsql" "$out"); rc=$?
            if (( rc == 137 )); then
                status=TIMEOUT
                printf '  %-22s TIMEOUT after %ss\n' "$name" "$QUERY_TIMEOUT_S"
                break
            elif (( rc != 0 )); then
                status=ERROR
                printf '  %-22s ERROR: %s\n' "$name" "$(head -2 "${out}.err" | tr '\n' ' ')"
                break
            fi
            (( i < WARMUP )) && continue
            times+=("$ms")
            nrows=$(grep -c . "$out")
            first=$(head -1 "$out" | tr '|' ',')
            [[ -z "$first" ]] && first="-"
            printf '  %-22s run %d: %9s ms\n' "$name" "$(( i - WARMUP + 1 ))" "$ms"
        done

        # ── AN EMPTY SCAN IS THE FASTEST POSSIBLE RESULT ────────────────────
        # So a predicate that matched nothing must be FLAGGED, never reported as
        # a win. An aggregate always returns exactly one row, so a row COUNT
        # cannot tell COUNT(*)=0 from COUNT(*)=58M — the first value is checked
        # too. The two exemptions are declared in the .sql files themselves
        # (`-- bench: allow-zero-rows` on H3, `leading-col-is-label` on S3), not
        # hardcoded here, so a query added later carries its own guard semantics.
        if [[ "$status" == ok ]] && ! bench_query_flag "$qfile" allow-zero-rows; then
            leading_is_count=1
            bench_query_flag "$qfile" leading-col-is-label && leading_is_count=0
            v="${first%%,*}"
            if [[ "${nrows:-0}" == 0 ]] || { (( leading_is_count == 1 )) && [[ "$v" == 0 || "$v" == NULL ]]; }; then
                status=ZERO_SCAN
                printf '  %-22s ZERO_SCAN: predicate matched no rows — the timing is\n' "$name"
                printf '  %-22s             meaningless, NOT a win\n' ""
            fi
        fi

        # NO TIMINGS MEANS NO MEDIAN, and the count has to be tested BEFORE the
        # pipeline rather than relying on median() to notice. `printf '%s\n'`
        # with zero arguments still prints ONE empty line, so median() saw NR==1
        # and %d-formatted "" to 0 — publishing `"median_ms": 0` for every
        # ERROR and TIMEOUT query. Zero is the fastest possible number, so a
        # failed query appeared in the JSON as the best result in the suite, and
        # the `[[ -n "$med" ]]` gate below printed a "MEDIAN 0 ms (ERROR)" line
        # to match. Same empty-result-looks-like-a-win failure the ZERO_SCAN
        # guard exists to catch, arriving through the summary instead of the data.
        #
        # ${#times[@]} is safe under `set -u` here only because `times=()` is
        # assigned at the top of every query iteration — bash 3.2 treats a
        # NEVER-assigned array as an unbound variable even for a count.
        med=""
        (( ${#times[@]} > 0 )) && med=$(printf '%s\n' "${times[@]}" | median)
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$name" "$s" "$status" "${med:-}" "${nrows:-}" "$first" "$meta" >> "$RESULTS"
        [[ -n "$med" ]] && printf '  %-22s MEDIAN %9s ms  (%s)  first_row=%s\n' \
            "$name" "$med" "$status" "$first"
    done < <(bench_query_files "$s")
    echo ""
done

# ── Summary ─────────────────────────────────────────────────────────────────
echo "-- Summary (${FORMAT}) --------------------------------------------------"
printf '  %-22s %-9s %10s %10s  %s\n' QUERY SUITE MEDIAN_ms STATUS FIRST_ROW
while IFS=$'\t' read -r name suite status med nrows first meta; do
    printf '  %-22s %-9s %10s %10s  %s\n' \
        "$name" "$suite" "${med:--}" "$status" "${first:0:60}"
done < "$RESULTS"

BAD=$(awk -F'\t' '$3!="ok"{n++} END{print n+0}' "$RESULTS")
TOT=$(grep -c . "$RESULTS")
if (( BAD > 0 )); then
    echo ""
    echo "  ${BAD}/${TOT} queries did NOT produce a usable timing:"
    awk -F'\t' '$3!="ok"{printf "    %-22s %s\n", $1, $3}' "$RESULTS"
    echo "  A non-ok status must NOT be read as a fast result."
fi

# ── Machine-readable payload ────────────────────────────────────────────────
# The local role extracts everything from the ===JSON=== marker onward, so
# nothing may be printed after this block.
echo ""
echo "===JSON==="
RESULTS_TSV="$RESULTS" \
BENCH_FORMAT="$FORMAT" \
BENCH_DUCKDB_VERSION="$("$DUCKDB_BIN" --version 2>/dev/null)" \
BENCH_TABLE_DESC="$([[ "$FORMAT" == iceberg ]] && echo "${ICEBERG_DB}.${TABLE_NAME}" || echo "$PAIMON_TABLE_ROOT")" \
BENCH_REGION="$REGION" BENCH_SUITE="$SUITE" BENCH_RUNS="$RUNS" BENCH_WARMUP="$WARMUP" \
BENCH_QUERYBENCH_S3="$QUERYBENCH_S3" \
BENCH_TIMEOUT="$QUERY_TIMEOUT_S" BENCH_MEM="$MEM_LIMIT" BENCH_THREADS="$THREADS" \
BENCH_PIN="$PIN_SNAPSHOT" BENCH_RUN_TAG="$RUN_TAG" \
BENCH_HOT_DT="$BENCH_HOT_DT" BENCH_HOT_1M="$BENCH_HOT_FROM_1M" \
BENCH_HOT_5M="$BENCH_HOT_FROM_5M" BENCH_HOT_10M="$BENCH_HOT_FROM_10M" \
BENCH_HOT_TO="$BENCH_HOT_TO" \
BENCH_SNAP_DT="$BENCH_SNAPSHOT_DT" BENCH_SNAP_SRC="$SNAP_DT_SRC" \
BENCH_S4_FROM="$BENCH_S4_FROM" BENCH_S4_TO="$BENCH_S4_TO" \
BENCH_PAIMON_FILES="$PAIMON_FILES" BENCH_PAIMON_BYTES="$PAIMON_BYTES" \
python3 -c '
import csv, json, os
rows = {}
with open(os.environ["RESULTS_TSV"]) as fh:
    for r in csv.reader(fh, delimiter="\t"):
        if not r:
            continue
        name, suite, status, med, nrows, first, meta = (r + [""] * 7)[:7]
        rows[name] = {
            "suite": suite, "status": status,
            "median_ms": int(med) if med else None,
            "result_rows": int(nrows) if nrows else None,
            "first_row": first.split(",") if first and first != "-" else None,
            "metadata_location": meta if meta != "-" else None,
        }
e = os.environ
prov = {"metadata_location_at_start": None}
if e["BENCH_FORMAT"] == "paimon":
    prov = {"paimon_files_at_start": int(e["BENCH_PAIMON_FILES"]),
            "paimon_bytes_at_start": int(e["BENCH_PAIMON_BYTES"])}
print(json.dumps({
    "engine": "duckdb", "runner": "run-duckdb-cid.sh --in-pod (duckdb CLI)",
    "duckdb_version": e["BENCH_DUCKDB_VERSION"],
    "format": e["BENCH_FORMAT"], "provenance": prov,
    "region": e["BENCH_REGION"], "table": e["BENCH_TABLE_DESC"],
    "query_source": e["BENCH_QUERYBENCH_S3"] + "/{hot,snapshot}/*.sql (shared with starrocks-cid-audit.sh)",
    "partitions": {"hot": e["BENCH_HOT_DT"], "snapshot": e["BENCH_SNAP_DT"],
                   "snapshot_dt_source": e["BENCH_SNAP_SRC"]},
    "hot_windows_utc": {"to": e["BENCH_HOT_TO"], "1m": e["BENCH_HOT_1M"],
                        "5m": e["BENCH_HOT_5M"], "10m": e["BENCH_HOT_10M"]},
    "snapshot_s4_window_utc": {"from": e["BENCH_S4_FROM"], "to": e["BENCH_S4_TO"]},
    "memory_limit": e["BENCH_MEM"], "threads": e["BENCH_THREADS"],
    "runs": int(e["BENCH_RUNS"]), "warmup": int(e["BENCH_WARMUP"]),
    "suite": e["BENCH_SUITE"], "query_timeout_s": int(e["BENCH_TIMEOUT"]),
    "pin_snapshot": e["BENCH_PIN"] == "1", "run_tag": e["BENCH_RUN_TAG"],
    "results": rows,
}, indent=2))'

(( BAD > 0 )) && exit 1
exit 0

fi   # end IN-POD ROLE

# ════════════════════════════════════════════════════════════════════════════
# LOCAL ROLE
# ════════════════════════════════════════════════════════════════════════════

FMT="${FORMAT:-iceberg}"
case "$FMT" in
    iceberg|paimon) ARMS=("$FMT") ;;
    both)           ARMS=(iceberg paimon) ;;
    *) echo "ERROR: FORMAT must be iceberg|paimon|both, got '${FMT}'" >&2; exit 1 ;;
esac
# The arm name is ALWAYS suffixed onto the Job name, even for a single-arm run.
# A Job's pod template is immutable, so two arms sharing a name means the second
# submission either fails or deletes the first — and that is exactly the collision
# a concurrent launch must not be able to hit. Suffixing unconditionally means
# there is no code path where the names can coincide, rather than a rule someone
# has to remember when they set JOB.
JOB_BASE="${JOB:-duckdb-cid}"
# Named for what it holds, not for a language: it carried duckdb-cid-bench.py
# when the runner was Python, and the "-py" outlived that by two rewrites.
CM="${CM:-duckdb-cid-bench}"
OUT_DIR="${OUT_DIR:-${SCRIPT_DIR}/../bench-results}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

if [[ "${1:-run}" == "clean" ]]; then
    # Both the default names and any JOB override, deduped — a custom JOB_BASE
    # does not mean the default-named jobs from an earlier run are gone.
    kubectl delete job -n "$NAMESPACE" --ignore-not-found \
        $(printf '%s\n' duckdb-cid-iceberg duckdb-cid-paimon \
            "${JOB_BASE}-iceberg" "${JOB_BASE}-paimon" | sort -u)
    kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found
    # Two dead ConfigMaps from earlier layouts, cleaned up so a cluster that ran
    # them does not keep an object around for someone to edit and wonder why the
    # pod ignored it. duckdb-cid-bench-sql carried the flattened query tree
    # before the queries moved to S3 (2026-10-06); duckdb-cid-bench-py carried
    # the two-file runner before this merge.
    kubectl delete configmap duckdb-cid-bench-sql -n "$NAMESPACE" --ignore-not-found
    kubectl delete configmap duckdb-cid-bench-py -n "$NAMESPACE" --ignore-not-found
    exit 0
fi

mkdir -p "$OUT_DIR"

# ── Parallel-vs-serial decision ─────────────────────────────────────────────
# Split from the action so it is testable without touching the cluster, the same
# way snapshot_stop_decision is: prints "parallel <reason>" or "serial <reason>"
# and returns 0/1 respectively. The rationale for each row is in the header.
cid_parallel_decision() {
    local suite="${1:-all}"
    case "${PARALLEL_ARMS:-auto}" in
        0) echo "serial PARALLEL_ARMS=0 (explicitly disabled)"; return 1 ;;
        1) echo "parallel PARALLEL_ARMS=1 (forced, regardless of suite)"; return 0 ;;
    esac
    case "$suite" in
        hot)      echo "parallel auto: suite=hot windows a LIVE table, so serial arms would measure different tables"; return 0 ;;
        snapshot) echo "serial auto: suite=snapshot reads a FROZEN partition — no window to skew, so keep each arm's bandwidth clean"; return 1 ;;
        all)      echo "serial auto: suite=all includes snapshot"; return 1 ;;
        *)        echo "serial auto: suite=${suite} does not window a live table"; return 1 ;;
    esac
}

DRY_RUN="${DRY_RUN:-0}"

PARALLEL_DECISION=$(cid_parallel_decision "$SUITE_SEL") || true
[[ "${PARALLEL_DECISION%% *}" == parallel ]] && PARALLEL=1 || PARALLEL=0
# One arm cannot contend with itself, and backgrounding it would only cost the
# streamed log. Reported rather than silently ignored so the printed decision
# always matches what actually happened.
if (( PARALLEL == 1 )) && (( ${#ARMS[@]} < 2 )); then
    PARALLEL=0
    PARALLEL_DECISION="serial (only one arm: ${ARMS[0]})"
fi

# Fail if the Iceberg table does not exist.
if (( DRY_RUN == 0 )); then
    for _arm in "${ARMS[@]}"; do
        [[ "$_arm" == iceberg ]] || continue
        if ! aws glue get-table --database-name "$ICEBERG_DB" \
                --name "$TABLE_NAME" --region "$REGION" &>/dev/null; then
            echo "ERROR: Glue table ${ICEBERG_DB}.${TABLE_NAME} does not exist." >&2
            echo "  Deploy the writer first:  ../build-deploy-cid.sh deploy iceberg" >&2
            exit 1
        fi
    done
fi

# ── Writer regime ───────────────────────────────────────────────────────────
# Before any Job is submitted, not after: each pod resolves its own pinned
# snapshot and (on FORMAT=paimon) its read_parquet file list at startup, so a
# suspend that lands after submission leaves the plan straddling the change. Done
# ONCE for the whole invocation rather than per arm — with FORMAT=both a per-arm
# stop would be a second wait for no benefit, and under PARALLEL=1 two concurrent
# stops would race each other's wait loop.
#
# THE TWO SUITES NEED OPPOSITE WRITER STATES and both failure modes are silent:
#   hot      — trailing ingestionTime windows over today's dt. Writers stopped
#              means ZERO matching rows, and an empty scan is the fastest
#              possible result, so it reads as a win. Never suspended for hot.
#   snapshot — one whole dt partition read as cold. At CID_DAY_SPREAD=1 the
#              generator writes TWO live dt partitions by design (datagen's
#              min/max are inclusive, so r_day draws {0,1}), so yesterday is not
#              closed while the writers run and its file set moves mid-suite.
# Measured cost of getting it wrong on the snapshot side: 332,520 ms live vs
# 3,742 ms suspended on the same partition and the same query — 89x.
#
# SUSPEND_WRITERS: unset = auto (stop iff the suite includes snapshot),
# 0 = skip, 1 = force. Restarting the writers afterwards is MANUAL and is not
# done by any script — see snapshot-preflight.sh's header.
if [[ "$SUITE_SEL" == all ]]; then
    echo "  ⚠ SUITE=all: the hot suite needs writers RUNNING and the snapshot"
    echo "    suite needs them SUSPENDED, so one of the two is being measured in"
    echo "    the wrong regime. Run them separately for quotable numbers."
fi

if (( DRY_RUN == 0 )); then
    # Shared with starrocks-cid-audit.sh rather than reimplemented. The two
    # harnesses' snapshot numbers are quoted against each other, so a stop step
    # that behaved differently on one side would make that a comparison of
    # writer regimes instead of engines. Not sourced under DRY_RUN=1 so a render
    # can be proved with no cluster at all.
    #
    # Two steps, in order: assert no cid_events query is already running on the
    # FE, then stop the streaming jobs and wait for the TaskManagers to go.
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/../snapshot-preflight.sh"
    snapshot_preflight "$SUITE_SEL" || exit 1
    WRITERS_AT_START="$(snapshot_writer_state)"
else
    WRITERS_AT_START="(dry-run: not sampled)"
fi

echo "arms: ${ARMS[*]}   mode: ${PARALLEL_DECISION}"
echo "  suite: ${SUITE_SEL}   runs: ${RUNS:-3}   stamp: ${STAMP}"

# ── Ship the code; point the pod at the queries ─────────────────────────────
# ONE ConfigMap, ONE key: this file, which the pod runs as `--in-pod`. The
# queries do not travel through Kubernetes at all — the pod syncs
# ${QUERYBENCH_S3} at startup with the `aws` CLI it already installs for the Glue
# and STS calls, under the IRSA role it already uses to read the warehouse.
if (( DRY_RUN == 0 )); then
    kubectl create configmap "$CM" -n "$NAMESPACE" \
        --from-file=run-duckdb-cid.sh="${BASH_SOURCE[0]}" \
        --dry-run=client -o yaml | kubectl apply -f - >/dev/null
fi
echo "  queries: ${QUERYBENCH_S3}/ — synced in-pod, shared with starrocks-cid-audit.sh"

# ── One arm, end to end ─────────────────────────────────────────────────────
# Submit, wait for scheduling, capture the log, extract the JSON, delete the Job.
# Self-contained so it can be backgrounded: everything it touches is either
# read-only shared state or named after $arm, and it returns non-zero on failure
# so `wait` reports the arm's status to the caller.
run_arm() {
    local arm="$1" stream="$2"
    local job="${JOB_BASE}-${arm}"
    local base="duckdb-cid-${arm}-${STAMP}"
    local log="${OUT_DIR}/${base}.log"
    local pin pod phase exit_code succeeded deadline manifest

    # One rule, defined once in the shared block and used by both roles — see
    # cid_pin_default(). An explicit PIN_SNAPSHOT in the environment wins.
    pin="${PIN_SNAPSHOT:-$(cid_pin_default "$arm")}"

    manifest="$(mktemp)"
    # shellcheck disable=SC2064
    trap "rm -f '${manifest}' '${manifest}.raw'" RETURN
    sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
        "${SCRIPT_DIR}/duckdb-cid-job.yaml" > "${manifest}.raw"
    # This manifest has only ONE render-time placeholder and no container-runtime
    # ${...} references, so an unqualified guard is correct here — unlike
    # iceberg-compaction/run-compaction.sh, whose spark-submit line carries
    # ${DRIVER_MEMORY}/${SPARK_LOCAL_THREADS} for in-container bash to expand. If
    # a runtime variable is ever added to the args below, this guard must learn to
    # allow it or it will reject a perfectly good manifest.
    if grep -qE '\$\{[A-Z0-9_]+\}' "${manifest}.raw"; then
        echo "ERROR: unsubstituted placeholders remain:" >&2
        grep -nE '\$\{[A-Z0-9_]+\}' "${manifest}.raw" >&2
        return 1
    fi

    # Env overrides and the Job name are patched in rather than templated, so the
    # YAML stays directly kubectl-appliable on its own. Four load-bearing details:
    #
    #  1. JOB is rewritten into metadata.name, not just used for the delete below.
    #     A Job's pod template is immutable, so applying the YAML's own name while
    #     a Job of that name exists fails with "field is immutable".
    #  2. `kubectl create`, NOT `apply --dry-run=client`: that flag still fetches
    #     the live object to compute a merge, so it echoes back the previous Job's
    #     generated labels (controller-uid, batch.kubernetes.io/job-name) and a
    #     rename then fails with "spec.selector: `selector` not auto-generated".
    #  3. Overrides apply to any var PRESENT in the environment, not any var that
    #     is non-empty, so an intentional empty value is not silently dropped.
    #  4. FORMAT and PIN_SNAPSHOT are passed PER ARM, not inherited: the caller's
    #     FORMAT may be "both", which is not a value the pod understands, and the
    #     two arms need different PIN_SNAPSHOT defaults.
    FORMAT="$arm" PIN_SNAPSHOT="$pin" JOB="$job" MANIFEST="${manifest}.raw" python3 -c '
import json, os, sys, yaml
d = yaml.safe_load(open(os.environ["MANIFEST"]))
d["metadata"]["name"] = os.environ["JOB"]
env = d["spec"]["template"]["spec"]["containers"][0]["env"]
#  5. QUERY_TIMEOUT_S is APPENDED, not overwritten: the manifest does not carry a
#     literal for it, because that literal used to win over the runner default and
#     the two drifted to 900 vs 3600. Unset, the pod derives the cap from SUITE
#     via cid_query_timeout -- and SUITE IS patched in below, so the pod sees the
#     suite actually being run. Set, this override wins.
for k in ("FORMAT", "SUITE", "RUNS", "WARMUP", "QUERY_TIMEOUT_S",
          "DUCKDB_MEMORY_LIMIT", "DUCKDB_THREADS", "ICEBERG_DB", "TABLE_NAME",
          "PAIMON_TABLE_ROOT", "PIN_SNAPSHOT", "RUN_TAG",
          # The partition pins. SNAPSHOT_DT is now OPTIONAL rather than
          # load-bearing: unset, the pod resolves the OLDEST dt= prefix from S3,
          # which is the partition the generator has stopped writing (the r_day
          # spread keeps the NEWEST live). Set it only to name a different day.
          # It is still passed through so a cross-engine comparison can pin both
          # engines to one partition explicitly.
          #
          # NOTE: no apostrophes in these comments. The whole block is the body
          # of a single-quoted python -c, so one would close the string and the
          # script would not parse.
          "SNAPSHOT_DT", "HOT_DT",
          # S4 ingestionTime window overrides, consumed by
          # bench_resolve_snapshot_window. SNAPSHOT_INGEST_DATE exists because
          # the window is on the INGESTION date, which is NOT the partition
          # date: dt comes from the event time minus a random day offset, so
          # dt=2026-10-04 was WRITTEN on 2026-10-05. Default is SNAPSHOT_DT plus
          # one day. The same three names work on the StarRocks harness.
          "SNAPSHOT_INGEST_DATE", "SNAPSHOT_S4_FROM", "SNAPSHOT_S4_TO",
          # The S3 prefix the pod syncs the query suites from, and the local
          # directory it syncs them INTO. QUERYBENCH_S3 is the one that matters:
          # it is how a run is pointed at a candidate query set without
          # republishing over the set the StarRocks harness is reading.
          # BENCH_SQL_DIR is passed through only so a debugging session can
          # point the runner at a hand-edited tree in the pod.
          "QUERYBENCH_S3", "BENCH_SQL_DIR"):
          # SNAPSHOT_SAMPLE_N IS GONE. It was the uuid modulo sample, retired on
          # 2026-10-06: it could not prune (uuid is MD5-derived, so every Parquet
          # row groups min/max spans the whole keyspace), so at N=1000 ScanRows
          # equalled the exact full partition count on every arm. It read every
          # byte and discarded 999/1000 of the rows at the aggregate. The
          # StarRocks harness dropped it the same day, and when only this side
          # kept it the two engines silently measured different row sets.
          #
          # NOTE: no apostrophes in these comments. The whole block is the body
          # of a single-quoted python -c, so one would close the string and the
          # script would not parse.
    if k not in os.environ:
        continue
    v = os.environ[k]
    for e in env:
        if e["name"] == k:
            e["value"] = v
            break
    else:
        env.append({"name": k, "value": v})
json.dump(d, sys.stdout)' > "$manifest"

    if (( DRY_RUN == 1 )); then
        echo "--- DRY_RUN arm=${arm} job=${job} pin_snapshot=${pin} ---"
        echo "    log would be: ${log}"
        python3 -c 'import json,sys,yaml; print(yaml.safe_dump(json.load(open(sys.argv[1])), sort_keys=False))' "$manifest"
        return 0
    fi

    kubectl delete job "$job" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    kubectl create -f "$manifest" >/dev/null
    echo "job/${job} submitted in ${NAMESPACE} (format=${arm} pin_snapshot=${pin})"

    for _ in $(seq 1 60); do
        pod=$(kubectl get pods -n "$NAMESPACE" -l "job-name=${job}" \
                -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
        [[ -n "$pod" ]] && break
        sleep 2
    done
    [[ -n "${pod:-}" ]] || { echo "ERROR: no pod created for job/${job}" >&2; return 1; }

    # Karpenter node provisioning (launch + boot + join) takes a few minutes,
    # during which the pod legitimately reports Unschedulable. Under PARALLEL=1
    # this is TWO nodes being provisioned, and the pod anti-affinity in the
    # manifest means the second cannot be satisfied by the first node — so expect
    # roughly the same wait, not double, but the whole grace window may be used.
    local sched_grace="${SCHED_GRACE:-900}"
    deadline=$(( $(date +%s) + sched_grace ))
    while :; do
        phase=$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
        [[ "$phase" == "Running" || "$phase" == "Succeeded" || "$phase" == "Failed" ]] && break
        if (( $(date +%s) >= deadline )); then
            kubectl get pod "$pod" -n "$NAMESPACE" \
              -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].message}' >&2
            echo "" >&2
            echo "ERROR: pod ${pod} (${arm}) still not Running after ${sched_grace}s." >&2
            echo "  Either no m5.8xlarge on-demand capacity in the nodepool's AZs, or" >&2
            echo "  executor-memorynodepool's cpu limit is exhausted — most likely by" >&2
            echo "  the benchmark's own TaskManagers, which share that nodepool." >&2
            echo "  A two-arm parallel launch needs TWO such nodes (the arms carry" >&2
            echo "  pod anti-affinity and cannot share one), so it hits that limit" >&2
            echo "  sooner than a single arm — PARALLEL_ARMS=0 trades the shared" >&2
            echo "  time window for half the capacity." >&2
            echo "  The m5/amd64 pins are deliberate (parity with StarRocks' BEs), so" >&2
            echo "  relaxing them invalidates the comparison — prefer retrying." >&2
            echo "  Check: kubectl get nodeclaims; kubectl describe pod ${pod} -n ${NAMESPACE}" >&2
            return 1
        fi
        sleep 10
    done

    echo "pod/${pod} ${phase} (${arm}) — logging to ${log} (pip install takes ~30s)"
    # Writer state goes into the .log as a header before the pod's own output, the
    # same way starrocks-cid-audit.sh puts it in the CSV: a result file found later
    # must still state the variable that dominates it. Sampled AFTER the suspend
    # step, so for a snapshot run it records the regime actually measured. The
    # arm-mode line is here for the same reason — a hot number measured against a
    # concurrently-running sibling arm paid a bandwidth tax, and a file found a
    # month later has to say so. Safe to prepend — the .json is extracted from the
    # ===JSON=== marker onward, which is below this.
    {
        echo "# writers: ${WRITERS_AT_START}  (suspend_writers=${SUSPEND_WRITERS:-auto} stopped_by_run=${CID_WRITERS_STOPPED:-0})"
        echo "# format: ${arm}  suite: ${SUITE_SEL}  runs: ${RUNS:-3}  pin_snapshot: ${pin}"
        echo "# arms: ${ARMS[*]}  mode: ${PARALLEL_DECISION}  stamp: ${STAMP}"
    } > "$log"
    if (( stream == 1 )); then
        kubectl logs -f "$pod" -n "$NAMESPACE" --pod-running-timeout=5m 2>&1 | tee -a "$log"
    else
        kubectl logs -f "$pod" -n "$NAMESPACE" --pod-running-timeout=5m >>"$log" 2>&1 || true
    fi

    exit_code=$(kubectl get pod "$pod" -n "$NAMESPACE" \
             -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || true)
    if [[ -z "$exit_code" ]]; then
        succeeded=$(kubectl get job "$job" -n "$NAMESPACE" \
                      -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
        if [[ "${succeeded:-0}" == "1" ]] || grep -q '^===JSON===$' "$log"; then
            exit_code=0
        else
            exit_code="?"
        fi
    fi

    echo ""
    echo "  [${arm}] log: ${log}"
    local rc=0
    if [[ "$exit_code" == "0" ]]; then
        if grep -q '^===JSON===$' "$log"; then
            sed -n '/^===JSON===$/,$p' "$log" | tail -n +2 > "${OUT_DIR}/${base}.json"
            echo "  [${arm}] json: ${OUT_DIR}/${base}.json"
        fi
    else
        echo "  [${arm}] container exit: ${exit_code}$([[ "$exit_code" == "137" ]] && echo '  (SIGKILL — OOM-killed by kubelet)')" >&2
        # backoffLimit: 0 — there is no retry and no second pod. Whatever this arm
        # produced is all it will ever produce, so the failure is terminal for the
        # arm and must be read as a missing data point, not a slow one.
        echo "  [${arm}] backoffLimit=0: the Job does NOT retry — this arm produced no result" >&2
        rc=1
    fi

    # Delete the Job only now that the log and JSON are extracted: the pod carries
    # the only copy of its output, and the generic ephemeral volume plus the
    # m5.8xlarge are released with it. Leaving it behind idles a 200Gi gp3 and a
    # 32-vcpu on-demand node (karpenter.sh/do-not-disrupt pins the node for as
    # long as the pod exists), so cleanup is not cosmetic. KEEP_JOBS=1 to retain
    # it for a post-mortem — kubectl describe on a failed pod needs it alive.
    if [[ "${KEEP_JOBS:-0}" == "1" ]]; then
        echo "  [${arm}] KEEP_JOBS=1: job/${job} retained — delete it to release the node and the 200Gi volume"
    else
        kubectl delete job "$job" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null
        echo "  [${arm}] job/${job} deleted (node + 200Gi ephemeral volume released)"
    fi
    return $rc
}

# ── Dispatch ────────────────────────────────────────────────────────────────
# Parallel runs each arm in a backgrounded subshell and collects per-arm status
# from `wait` by index. Status is NOT aggregated into a single boolean until every
# arm has been waited on: a failed arm must be named, and the run must not report
# success just because the other arm worked.
FAILED=()
if (( PARALLEL == 1 )); then
    PIDS=()
    for arm in "${ARMS[@]}"; do
        run_arm "$arm" 0 &
        PIDS+=("$!")
    done
    echo "  ${#PIDS[@]} arms in flight — logs are per-arm files, not streamed"
    for i in "${!PIDS[@]}"; do
        wait "${PIDS[$i]}" || FAILED+=("${ARMS[$i]}")
    done
else
    for arm in "${ARMS[@]}"; do
        # || rather than set -e: a failing first arm must not skip the second's
        # cleanup, and both arms' statuses are wanted.
        run_arm "$arm" 1 || FAILED+=("$arm")
    done
fi

# The writers are left STOPPED either way. Restarting them is deliberately not
# done here and there is no longer a hint: upgradeMode is stateless, so a restart
# is a fresh job reading whatever SQL is currently in S3 — the start of the next
# experiment, not cleanup of this one.
if [[ -n "${CID_WRITERS_STOPPED:-}" ]]; then
    echo ""
    echo "  The writers were STOPPED by this run and are still stopped."
fi

if (( ${#FAILED[@]} > 0 )); then
    echo ""
    echo "ERROR: ${#FAILED[@]}/${#ARMS[@]} arms FAILED: ${FAILED[*]}" >&2
    echo "  backoffLimit=0, so nothing retried. Re-run the failed arm(s) alone;" >&2
    echo "  a surviving arm's numbers are NOT comparable against a rerun, because" >&2
    echo "  the rerun's hot windows open at a different time." >&2
    exit 1
fi
