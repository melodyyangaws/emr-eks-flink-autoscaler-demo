#!/usr/bin/env bash
set -uo pipefail

# Time StarRocks queries against the throughput benchmark's Paimon and Iceberg
# tables side by side — the READ half of the comparison, run while (or after)
# the Flink jobs hammer the write path.
#
# Same mechanics as cdc-pipeline/sql-scripts/starrocks/bench-hot-data.sh:
# WARMUP+RUNS per query per catalog, client-side timing around the mysql call
# in the FE pod, median reported, file layout captured alongside so a "faster"
# line is never left unattributed.
#
# ── THIS SCRIPT NO LONGER CONTAINS QUERIES (2026-10-06) ─────────────────────
# It used to carry its own five — q1_count, q2_partition_prune, q3_needle_uuid,
# q4_narrow_agg, q5_wide_scan — as a `case` of inline SQL strings. They were
# the last duplicated query set in the project: four were the shared suite's
# queries with no bound, and the fifth (q1's unbounded COUNT(*)) could not
# return at this table size. They are MERGED into
# querybench-script/{hot,snapshot}/*.sql, each recorded in the header of the
# file that absorbed it; see bench-sql-lib.sh's LAYOUT section for the mapping.
#
# So this script now renders THE SAME .sql FILES as starrocks-cid-audit.sh and
# the DuckDB harness, synced from S3. What is still only here, and why it was
# not deleted:
#
#   client-side timing   Elapsed is measured around the `kubectl exec`, so it
#                        INCLUDES the exec and mysql round trip. The audit
#                        harness reads the FE audit log and so reports the
#                        engine's own time. Comparing the two separates
#                        client overhead from query cost — but never compare a
#                        number from this script against one from that one as
#                        if they measured the same thing.
#   interleaved A/B      run_suite is called paimon-then-iceberg PER QUERY, so
#                        with the writers live the volume gap between arms is
#                        one query's duration instead of a whole suite's.
#   -tablestats.txt      the file-LAYOUT stats (data-file count, mean size, and
#                        the delete-artifact counts that MUST read 0). An S3
#                        listing, not SQL, and nothing else produces it.
#
# NOTE: cid_events is APPEND-ONLY on both sides (no primary key, insert-only
# pipelines). Read latency here is scan economics — file count, file size,
# projection pushdown, partition pruning — NOT merge-on-read cost. Do not
# compare these numbers against the CDC upsert benchmarks' query timings; the
# workload is a different shape. See README's "This is an APPEND benchmark now".
#
# Catalogs are created idempotently here (IF NOT EXISTS) with the same
# properties as the CDC ones — filesystem catalog for Paimon (reads native S3
# metadata), Glue for Iceberg. IRSA credentials via use_aws_sdk_default_behavior.
#
# Usage:
#   BUCKET_NAME=... ./starrocks-bench.sh                    # hot suite (default)
#   SUITE=snapshot BUCKET_NAME=... ./starrocks-bench.sh     # the closed partition
#   RUNS=5 WARMUP=2 BUCKET_NAME=... ./starrocks-bench.sh
#   QUERY_NAMES="H1_freshness H2_hot_count_1m" BUCKET_NAME=... ./starrocks-bench.sh
#
# SUITE defaults to hot, NOT to the old whole-table behaviour. The queries this
# script used to run were unbounded; the shared suite's hot queries are a
# trailing window, so the default is now the cheap one. SUITE=snapshot reads a
# closed partition and S4_wide_json alone runs 18-41 min per pass per arm —
# budget for it, and prefer starrocks-cid-audit.sh for a real snapshot run,
# since it also suspends the writers.

NAMESPACE="${NAMESPACE:-emr-flink}"
FE_POD="${FE_POD:-kube-starrocks-fe-0}"
AWS_REGION="${AWS_REGION:-us-west-2}"
BUCKET_NAME="${BUCKET_NAME:?BUCKET_NAME is required}"
PAIMON_DB="${PAIMON_DB:-flink_cid_paimon_db}"
ICEBERG_DB="${ICEBERG_DB:-flink_cid_iceberg_db}"
RUNS="${RUNS:-3}"
WARMUP="${WARMUP:-1}"
SUITE="${SUITE:-hot}"

case "$SUITE" in
    hot|snapshot) ;;
    *) echo "ERROR: SUITE must be hot|snapshot, got '${SUITE}'" >&2; exit 1 ;;
esac

OUT_DIR="${OUT_DIR:-./bench-results}"
mkdir -p "$OUT_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
CSV="${OUT_DIR}/starrocks-cid-${STAMP}.csv"

sr() {
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- \
        mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1" 2>/dev/null
}

# ── The queries come from S3 ────────────────────────────────────────────────
# Same prefix and same assertions as starrocks-cid-audit.sh — see that script's
# "WHERE THE QUERIES LIVE" header. A fresh temp dir per run, so a query deleted
# from S3 cannot survive in a reused directory; an inherited BENCH_SQL_DIR is
# deliberately ignored for that reason.
QUERYBENCH_S3="${QUERYBENCH_S3:-s3://${BUCKET_NAME}/flink/querybench}"
QUERYBENCH_S3="${QUERYBENCH_S3%/}"
BENCH_SQL_DIR="$(mktemp -d "${TMPDIR:-/tmp}/querybench.XXXXXX")"
export BENCH_SQL_DIR
trap 'rm -rf "$BENCH_SQL_DIR"' EXIT

echo "Syncing query suites from ${QUERYBENCH_S3}/ ..."
aws s3 sync "${QUERYBENCH_S3}/" "${BENCH_SQL_DIR}/" --region "$AWS_REGION" \
    --exclude '*' --include 'bench-sql-lib.sh' \
    --include 'hot/*.sql' --include 'snapshot/*.sql' --only-show-errors || {
    echo "ERROR: cannot sync the query suites from ${QUERYBENCH_S3}/" >&2
    echo "  Publish them first:  (cd ../querybench-script && ./bench-sql.sh upload)" >&2
    exit 1; }

# Asserted, not assumed: `aws s3 sync` of an EMPTY or misspelled prefix succeeds
# with no output and exit 0, leaving an empty suite — which is indistinguishable
# from a suite whose queries all returned instantly.
_qb_hot=$(ls -1 "${BENCH_SQL_DIR}/hot"/*.sql 2>/dev/null | grep -c .)
_qb_snap=$(ls -1 "${BENCH_SQL_DIR}/snapshot"/*.sql 2>/dev/null | grep -c .)
if [[ ! -f "${BENCH_SQL_DIR}/bench-sql-lib.sh" ]] || (( _qb_hot == 0 || _qb_snap == 0 )); then
    echo "ERROR: ${QUERYBENCH_S3}/ is missing or incomplete" >&2
    echo "  got: bench-sql-lib.sh=$([[ -f "${BENCH_SQL_DIR}/bench-sql-lib.sh" ]] && echo yes || echo NO)" \
         "hot=${_qb_hot} snapshot=${_qb_snap}" >&2
    echo "  Publish it:  (cd ../querybench-script && ./bench-sql.sh upload)" >&2
    exit 1
fi
echo "  ✓ ${_qb_hot} hot + ${_qb_snap} snapshot queries -> ${BENCH_SQL_DIR}"

# shellcheck disable=SC1091
source "${BENCH_SQL_DIR}/bench-sql-lib.sh" || exit 1

# ── Catalogs (idempotent) ────────────────────────────────────────────────────
echo "Ensuring StarRocks catalogs exist..."
sr "CREATE EXTERNAL CATALOG IF NOT EXISTS cid_paimon_catalog PROPERTIES (
    \"type\" = \"paimon\",
    \"paimon.catalog.type\" = \"filesystem\",
    \"paimon.catalog.warehouse\" = \"s3://${BUCKET_NAME}/cid-warehouse-paimon/\",
    \"aws.s3.region\" = \"${AWS_REGION}\",
    \"aws.s3.use_aws_sdk_default_behavior\" = \"true\"
)" || { echo "Failed to create Paimon catalog" >&2; exit 1; }
sr "CREATE EXTERNAL CATALOG IF NOT EXISTS cid_iceberg_catalog PROPERTIES (
    \"type\" = \"iceberg\",
    \"iceberg.catalog.type\" = \"glue\",
    \"aws.glue.region\" = \"${AWS_REGION}\",
    \"aws.glue.use_aws_sdk_default_behavior\" = \"true\",
    \"aws.s3.region\" = \"${AWS_REGION}\",
    \"aws.s3.use_aws_sdk_default_behavior\" = \"true\"
)" || { echo "Failed to create Iceberg catalog" >&2; exit 1; }

# ── Queries ──────────────────────────────────────────────────────────────────
# The table is an APPEND-ONLY insert stream with NO primary key, so the read
# costs that used to dominate this suite are gone: there is no merge-on-read, no
# delete file or deletion vector to apply, and no PK index to look up. What is
# left is pure scan economics, which is what the shared suite isolates — one
# .sql file per step, each with a header saying what it measures and which other
# step to read it against.
#
# The names below ARE the filenames, and the H<n>/S<n> prefix is the order. To
# run a subset:
#   QUERY_NAMES="H2_hot_count_1m H5_hot_wide_json" ./starrocks-bench.sh
#
# Resolve the windows ONCE, here, for every arm and pass — so the two catalogs
# read the SAME rows. Computing a bound in SQL (the old q2's
# DATE_FORMAT(NOW(), ...)) gave each statement its own value, and NOW() is
# evaluated in the FE session timezone against UTC data besides.
if [[ "$SUITE" == hot ]]; then
    bench_resolve_hot_windows || exit 1
else
    # Yesterday in UTC: the newest partition that is closed while the writers
    # run. Overridable, because yesterday is itself one of the two live dt
    # partitions under CID_DAY_SPREAD=1.
    SNAPSHOT_DT="${SNAPSHOT_DT:-$(bench_date_plus_days "$(date -u +%Y-%m-%d)" -1)}"
    bench_resolve_snapshot_window "$SNAPSHOT_DT" || exit 1
fi

# StarRocks reaches the table by NAME; run_suite qualifies it per catalog below.
export BENCH_BYTES_FN=LENGTH

QUERY_FILES=()
if [[ -n "${QUERY_NAMES:-}" ]]; then
    read -r -a _want <<< "$QUERY_NAMES"
    for _n in "${_want[@]}"; do
        _f="${BENCH_SQL_DIR}/${SUITE}/${_n}.sql"
        if [[ ! -f "$_f" ]]; then
            echo "ERROR: no query '${_n}' in the ${SUITE} suite" >&2
            echo "  available: $(bench_query_files "$SUITE" | while read -r q; do printf '%s ' "$(bench_query_name "$q")"; done)" >&2
            # This script's own five queries were merged into the shared suite on
            # 2026-10-06, so a q* name is most likely a stale note or an old
            # command line rather than a typo. Name the successor instead of
            # leaving the reader to diff two files.
            case "$_n" in
                q1_count|q2_partition_prune)
                    echo "  NOTE: ${_n} was merged into SUITE=snapshot S1_partition_count on 2026-10-06." >&2
                    echo "        (q1's unbounded COUNT(*) is not ported — it cannot return at this size.)" >&2 ;;
                q3_needle_uuid)
                    echo "  NOTE: q3_needle_uuid was merged into SUITE=hot H3_hot_needle_uuid on 2026-10-06." >&2 ;;
                q4_narrow_agg)
                    echo "  NOTE: q4_narrow_agg was merged into SUITE=snapshot S2_narrow_agg on 2026-10-06." >&2 ;;
                q5_wide_scan)
                    echo "  NOTE: q5_wide_scan was merged into SUITE=snapshot S4_wide_json on 2026-10-06." >&2 ;;
            esac
            exit 1
        fi
        QUERY_FILES+=("$_f")
    done
else
    while IFS= read -r _f; do QUERY_FILES+=("$_f"); done < <(bench_query_files "$SUITE") || exit 1
fi
(( ${#QUERY_FILES[@]} > 0 )) || { echo "ERROR: no queries selected" >&2; exit 1; }

bench_print_plan "$SUITE"

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

# rows_at_start makes every latency line volume-attributable: if the writers
# are still running (or achieved different write throughput), the two tables
# hold different row counts and a raw latency comparison is meaningless.
echo "catalog,query,run,elapsed_ms,status,rows_at_start" > "$CSV"

# The partition each run is scored against, and the ONLY thing rows_at_start is
# allowed to count.
STATE_DT="$([[ "$SUITE" == hot ]] && echo "$BENCH_HOT_DT" || echo "$BENCH_SNAPSHOT_DT")"

# READ FROM METADATA, NOT WITH COUNT(*). This line used to be
# `SELECT COUNT(*) FROM <catalog>.<db>.cid_events` — unbounded, run once per
# query per catalog, i.e. the retired q1_count executed 2N times as a
# PREAMBLE. There is no COUNT(*) shortcut on an external catalog, so at ~8 TiB
# it does not return; the audit harness hit exactly this and had its preamble
# consume an entire run before a single timed query was issued.
#
# `cid_events$partitions` carries record_count per partition: exact,
# O(metadata), and scoped to the partition under test. The Paimon reader cannot
# serve it ("Failed to find latest snapshot id"), so that arm falls back to SHOW
# PARTITIONS — which must be split on TABS, because VisibleVersionTime contains
# a space and a default-FS awk would shift every later field. Field 5 of 7 is
# RowCount.
rows_in_partition() {   # $1=catalog $2=db $3=dt
    local v
    v=$(sr "SELECT record_count FROM ${1}.${2}.\`cid_events\$partitions\`
            WHERE partition_value.dt='${3}'" 2>/dev/null | head -1)
    [[ -n "$v" ]] && { echo "$v"; return 0; }
    v=$(sr "SHOW PARTITIONS FROM ${1}.${2}.cid_events" 2>/dev/null \
          | awk -F'\t' -v d="dt=${3}" '$1==d {print $5; exit}')
    [[ -n "$v" ]] && echo "$v" || echo "ERR"
}

run_suite() {
    local catalog=$1 db=$2 qfile=$3 qname
    qname=$(bench_query_name "$qfile")
    echo ""
    echo "═══ ${catalog} (${db}) — ${qname} ═══"
    local rowcount
    rowcount=$(rows_in_partition "$catalog" "$db" "$STATE_DT")
    echo "  rows in dt=${STATE_DT}: ${rowcount}"

    # Rendered per catalog, because ${TABLE} is the fully qualified name here.
    # One line, so a `--` comment in the file cannot comment out the rest of the
    # statement when mysql -e receives it.
    local sql i t0 t1 status elapsed
    sql="$(BENCH_TABLE="${catalog}.${db}.cid_events" bench_render_oneline "$qfile")" || return 1
    # SESSION SETTINGS, PREPENDED TO THE SAME -e SO THEY SHARE THE CONNECTION.
    # Each `sr` call is its own kubectl exec and therefore its own mysql
    # session, so a SET issued separately would be gone by the next statement.
    #
    # These are NOT tuning. They are what makes the snapshot suite complete at
    # all, and they are copied from starrocks-cid-audit.sh so the two scripts
    # measure the same engine configuration — a setting present on one and not
    # the other turns a comparison between them into a comparison of settings.
    # S4's OOM is SCAN-side (`read and decompress page`), which is why capping
    # the IO tasks is required and enable_spill alone is not enough; the second
    # SET is not optional, because the adaptive scheduler otherwise raises the
    # count back at runtime. See that script's long note.
    sql="SET enable_spill = true; ${sql}"
    if [[ "$SUITE" == snapshot ]]; then
        sql="SET connector_io_tasks_per_scan_operator = 2;
             SET enable_connector_adaptive_io_tasks = false; ${sql}"
    fi
    for ((i = 1; i <= WARMUP; i++)); do sr "$sql" >/dev/null || true; done
    local times=()
    for ((i = 1; i <= RUNS; i++)); do
        t0=$(now_ms)
        if sr "$sql" >/dev/null; then status=ok; else status=error; fi
        t1=$(now_ms)
        elapsed=$((t1 - t0))
        times+=("$elapsed")
        echo "${catalog},${qname},${i},${elapsed},${status},${rowcount}" >> "$CSV"
    done
    local median
    median=$(printf '%s\n' "${times[@]}" | sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}')
    printf "  %-18s median %6s ms  (runs: %s)\n" "$qname" "$median" "${times[*]}"
}

# Interleave per-query A/B instead of suite-after-suite: with the writers
# running, a full Paimon suite followed by a full Iceberg suite hands the
# second catalog minutes of extra accumulated data on every query. Paired
# runs keep the volume gap to one query's duration.
for q in "${QUERY_FILES[@]}"; do
    run_suite cid_paimon_catalog  "$PAIMON_DB"  "$q"
    run_suite cid_iceberg_catalog "$ICEBERG_DB" "$q"
done

# ── File-layout context ──────────────────────────────────────────────────────
# Capture what explains the timings, so a "faster" line is never left
# unattributed. On an append-only table the dominant read variable is file
# LAYOUT — count and mean size — because there is no merge work to do. So the
# per-warehouse listing is reduced to data files and their mean size.
#
# The delete-artifact counts are kept, but their meaning has inverted: they are
# now an ASSERTION, not a cost explanation. Append-only ingestion must never
# produce an Iceberg positional-delete file or a Paimon deletion-vector index.
# A non-zero count here means something wrote an update or a delete to the
# table, and the benchmark's premise is broken — investigate before trusting
# any number in the CSV.
STATS="${OUT_DIR}/starrocks-cid-${STAMP}-tablestats.txt"
{
    echo "captured_at=${STAMP}"
    for w in cid-warehouse-paimon cid-warehouse-iceberg; do
        echo "── s3://${BUCKET_NAME}/${w}/"
        listing=$(aws s3 ls --recursive "s3://${BUCKET_NAME}/${w}/" 2>/dev/null)
        printf '%s\n' "$listing" | awk '
            $4 ~ /\.parquet$/ { n++; b += $3 }
            END { if (n) printf "  data files: %d  bytes: %d  mean: %.1f MiB\n",
                                n, b, b / n / 1048576
                  else print "  data files: 0" }'
        printf '%s\n' "$listing" | awk 'END { print "  total objects: " NR }'
        if [[ "$w" == *iceberg ]]; then
            echo "  positional-delete files (MUST be 0): $(printf '%s\n' "$listing" | grep -c '/data/.*delete' || true)"
        else
            echo "  deletion-vector indexes (MUST be 0): $(printf '%s\n' "$listing" | grep -c '/index/' || true)"
        fi
    done
} | tee "$STATS"

echo ""
echo "Results -> $CSV"
echo "Table stats -> $STATS"
