#!/usr/bin/env bash
set -uo pipefail

# Time StarRocks queries against the Paimon and Iceberg catalogs side by side.
#
# Covers both halves of the comparison:
#   SNAPSHOT (Q1-Q6, from 01-/02-*.sql) — full-table scans, cost dominated by
#     total data volume.
#   HOT      (Q7-Q10, from 03-hot-data-queries.sql) — rows written in the last
#     few minutes, cost dominated by how many small files / delete files the
#     reader must merge. This is where Paimon's continuous LSM compaction and
#     Iceberg's merge-on-read diverge.
#
# Each query runs WARMUP+RUNS times per catalog; we report the median of the
# timed runs so one slow S3 list does not decide the result. Timing is measured
# client-side around the mysql call, which includes StarRocks planning + remote
# file IO — that is the number a dashboard user actually waits for.
#
# Alongside the timings we capture the file/delete/snapshot counts that explain
# them, so a "Paimon is faster" line is never left unattributed.
#
# Usage:
#   ./bench-hot-data.sh                 # snapshot + hot, both catalogs
#   SUITE=hot ./bench-hot-data.sh       # hot only
#   RUNS=5 WARMUP=2 ./bench-hot-data.sh
#   ICEBERG_MAINTENANCE=1 ./bench-hot-data.sh   # also re-run hot suite after
#                                               # rewrite_data_files + expire_snapshots

NAMESPACE="${NAMESPACE:-emr-flink}"
FE_POD="${FE_POD:-kube-starrocks-fe-0}"
PAIMON_CATALOG="${PAIMON_CATALOG:-paimon_catalogv3}"
PAIMON_DB="${PAIMON_DB:-flink_paimonv3_db}"
ICEBERG_CATALOG="${ICEBERG_CATALOG:-icebergv3_catalog}"
ICEBERG_DB="${ICEBERG_DB:-flink_icebergv3_db}"
SUITE="${SUITE:-all}"          # all | snapshot | hot
RUNS="${RUNS:-3}"              # timed runs per query
WARMUP="${WARMUP:-1}"          # untimed runs first (StarRocks caches file lists)
ICEBERG_MAINTENANCE="${ICEBERG_MAINTENANCE:-0}"

OUT_DIR="${OUT_DIR:-./bench-results}"
mkdir -p "$OUT_DIR"
# No date/timestamp from the shell's clock beyond this one stamp, so a rerun
# never overwrites a previous result set.
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
CSV="${OUT_DIR}/starrocks-bench-${STAMP}.csv"

# Run SQL in the FE pod. StarRocks' MySQL protocol port is 9030 and the root
# user has no password in this deployment.
sr() {
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- \
        mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1" 2>/dev/null
}

# ── Queries ─────────────────────────────────────────────────────────────────
# Kept inline rather than parsed out of the .sql files: the files are written for
# humans reading them top-to-bottom (with USE/SET CATALOG statements mixed in),
# and splitting them on ';' would also pick up the DDL and time-travel examples.
#
# ${PART} is the daily partition column, whose name differs by format:
# order_date_str (Paimon, an explicit STRING column) vs order_dt (Iceberg).
# Every hot predicate uses UTC_TIMESTAMP(), NOT NOW(): the FE session timezone
# here is +08:00 while CDC writes UTC values, so NOW() silently matches nothing.

declare -a SNAPSHOT_NAMES=(Q1_count Q2_group_by Q3_join Q4_product_sales Q5_time_window Q6_multi_join)
declare -a SNAPSHOT_SQL=(
"SELECT COUNT(*) FROM customers;"

"SELECT state, COUNT(*) c, COUNT(DISTINCT city) FROM customers GROUP BY state ORDER BY c DESC;"

"SELECT c.customer_name, c.city, COUNT(o.order_id) oc, SUM(o.total_amount) spent
 FROM customers c JOIN orders o ON c.customer_id = o.customer_id
 GROUP BY c.customer_id, c.customer_name, c.city ORDER BY spent DESC LIMIT 20;"

"SELECT p.product_name, p.category, SUM(oi.quantity) units, SUM(oi.subtotal) rev
 FROM products p JOIN order_items oi ON p.product_id = oi.product_id
 GROUP BY p.product_id, p.product_name, p.category ORDER BY rev DESC LIMIT 20;"

"SELECT DATE(order_date) d, COUNT(*) oc, SUM(total_amount) rev, AVG(total_amount) aov
 FROM orders WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
 GROUP BY DATE(order_date) ORDER BY d DESC;"

"SELECT DATE(o.order_date) d, c.state, p.category, COUNT(DISTINCT o.order_id) orders,
        SUM(oi.quantity) units, SUM(oi.subtotal) rev
 FROM orders o JOIN customers c ON o.customer_id = c.customer_id
 JOIN order_items oi ON o.order_id = oi.order_id
 JOIN products p ON oi.product_id = p.product_id
 WHERE o.order_status = 'DELIVERED'
 GROUP BY DATE(o.order_date), c.state, p.category
 ORDER BY d DESC, rev DESC LIMIT 100;"
)

declare -a HOT_NAMES=(Q7_freshness Q8_hot_pk_lookup Q9_recent_window Q10_hot_join)
declare -a HOT_SQL=(
"SELECT MAX(updated_at), TIMESTAMPDIFF(SECOND, MAX(updated_at), UTC_TIMESTAMP()), COUNT(*)
 FROM orders WHERE updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 5 MINUTE);"

"SELECT order_id, customer_id, order_status, total_amount, updated_at
 FROM orders WHERE order_id = (SELECT MAX(order_id) FROM orders);"

"SELECT order_status, COUNT(*) orders, SUM(total_amount) rev, AVG(total_amount) aov
 FROM orders WHERE updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE)
 GROUP BY order_status ORDER BY orders DESC;"

"SELECT o.order_status, COUNT(DISTINCT o.order_id) orders, SUM(oi.quantity) units,
        SUM(oi.subtotal) rev
 FROM orders o JOIN order_items oi ON o.order_id = oi.order_id
 WHERE o.updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE)
 GROUP BY o.order_status ORDER BY rev DESC;"
)

# ── Timing ──────────────────────────────────────────────────────────────────
# Median, not mean: a single cold S3 LIST can be 10x the steady-state cost and
# would otherwise dominate a 3-run average.
median() {
    local sorted n
    sorted=$(printf '%s\n' "$@" | sort -n)
    n=$#
    if (( n % 2 == 1 )); then
        printf '%s\n' "$sorted" | sed -n "$(( (n + 1) / 2 ))p"
    else
        printf '%s\n' "$sorted" | sed -n "$(( n / 2 )),$(( n / 2 + 1 ))p" \
            | awk '{s+=$1} END {printf "%.0f", s/2}'
    fi
}

# Echo the median wall-clock ms for one query, or FAIL if every run errored.
time_query() {
    local catalog=$1 db=$2 sql=$3
    local prelude="SET CATALOG ${catalog}; USE ${db};"
    local i start end out
    local -a times=()

    for (( i = 0; i < WARMUP; i++ )); do
        sr "${prelude} ${sql}" >/dev/null
    done
    for (( i = 0; i < RUNS; i++ )); do
        start=$(date +%s%N)
        # Capture the exit status of this same run rather than issuing a second
        # identical query to test it: re-running tripled the query count per
        # timed iteration and charged the extra load to the cluster while the
        # window it measures is supposed to be quiet.
        # An empty result is legitimate (a hot window can genuinely be empty),
        # so only a non-zero exit counts as a failure.
        if ! out=$(sr "${prelude} ${sql}"); then
            continue
        fi
        end=$(date +%s%N)
        times+=( $(( (end - start) / 1000000 )) )
    done

    if (( ${#times[@]} == 0 )); then
        echo "FAIL"
    else
        median "${times[@]}"
    fi
}

# Rows returned, so a suspiciously fast query can be spotted as "returned 0 rows".
row_count() {
    local catalog=$1 db=$2 sql=$3
    sr "SET CATALOG ${catalog}; USE ${db}; ${sql}" | grep -c . || echo 0
}

run_suite() {
    local suite_label=$1 state=$2 names_var=$3 sqls_var=$4

    # Copy the caller's arrays by name via eval rather than `local -n`.
    # macOS ships bash 3.2, which has no namerefs: `local -n` there is parsed as
    # `local` with an invalid -n flag, the function aborts before running a single
    # query, and the suite prints only its header — a silent zero-row benchmark.
    local -a names sqls
    eval "names=(\"\${${names_var}[@]}\")"
    eval "sqls=(\"\${${sqls_var}[@]}\")"

    printf '\n%s\n' "────────────────────────────────────────────────────────────────────"
    printf '  %s suite  (state: %s, runs: %s, warmup: %s)\n' "$suite_label" "$state" "$RUNS" "$WARMUP"
    printf '%s\n' "────────────────────────────────────────────────────────────────────"
    printf '%-20s %12s %12s %10s %8s %8s\n' QUERY PAIMON_ms ICEBERG_ms RATIO P_rows I_rows

    local i name sql p_ms i_ms p_rows i_rows ratio part_p part_i
    for i in "${!names[@]}"; do
        name=${names[$i]}
        sql=${sqls[$i]}
        # Substitute the per-format partition column if the query uses it.
        part_p=${sql//\$\{PART\}/order_date_str}
        part_i=${sql//\$\{PART\}/order_dt}

        p_ms=$(time_query "$PAIMON_CATALOG"  "$PAIMON_DB"  "$part_p")
        i_ms=$(time_query "$ICEBERG_CATALOG" "$ICEBERG_DB" "$part_i")
        p_rows=$(row_count "$PAIMON_CATALOG"  "$PAIMON_DB"  "$part_p")
        i_rows=$(row_count "$ICEBERG_CATALOG" "$ICEBERG_DB" "$part_i")

        if [[ "$p_ms" == FAIL || "$i_ms" == FAIL ]]; then
            ratio="n/a"
        else
            ratio=$(awk -v a="$i_ms" -v b="$p_ms" 'BEGIN{ if (b>0) printf "%.2fx", a/b; else print "n/a" }')
        fi

        printf '%-20s %12s %12s %10s %8s %8s\n' \
            "$name" "$p_ms" "$i_ms" "$ratio" "$p_rows" "$i_rows"
        echo "${STAMP},${suite_label},${state},${name},${p_ms},${i_ms},${ratio},${p_rows},${i_rows}" >> "$CSV"
    done
}

# ── Table state: the numbers that explain the timings ───────────────────────
capture_state() {
    local label=$1
    printf '\n%s\n' "── Table state (${label}) ──────────────────────────────────────────"

    printf '\nIceberg — data / delete files and snapshots per table:\n'
    for t in customers products orders order_items; do
        # $files$ and $snapshots$ are StarRocks' Iceberg metadata tables.
        local files snaps
        files=$(sr "SELECT COUNT(*) FROM ${ICEBERG_CATALOG}.${ICEBERG_DB}.\`${t}\$files\`;" | head -1)
        snaps=$(sr "SELECT COUNT(*) FROM ${ICEBERG_CATALOG}.${ICEBERG_DB}.\`${t}\$snapshots\`;" | head -1)
        printf '  %-14s files=%-8s snapshots=%-8s\n' "$t" "${files:-?}" "${snaps:-?}"
    done

    printf '\nRow counts (both catalogs):\n'
    for t in customers products orders order_items; do
        local pc ic
        pc=$(sr "SELECT COUNT(*) FROM ${PAIMON_CATALOG}.${PAIMON_DB}.${t};" | head -1)
        ic=$(sr "SELECT COUNT(*) FROM ${ICEBERG_CATALOG}.${ICEBERG_DB}.${t};" | head -1)
        printf '  %-14s paimon=%-12s iceberg=%-12s\n' "$t" "${pc:-?}" "${ic:-?}"
    done
}

# ── Main ────────────────────────────────────────────────────────────────────
echo "═══════════════════════════════════════════════════════════════════"
echo "  StarRocks query benchmark — Paimon vs Iceberg"
echo "═══════════════════════════════════════════════════════════════════"
echo "  FE pod   : ${FE_POD} (ns ${NAMESPACE})"
echo "  Paimon   : ${PAIMON_CATALOG}.${PAIMON_DB}"
echo "  Iceberg  : ${ICEBERG_CATALOG}.${ICEBERG_DB}"
echo "  Suite    : ${SUITE}"
echo "  Results  : ${CSV}"
echo "═══════════════════════════════════════════════════════════════════"

sr "SELECT 1;" >/dev/null || { echo "ERROR: cannot reach StarRocks FE in ${FE_POD}" >&2; exit 1; }

echo "stamp,suite,state,query,paimon_ms,iceberg_ms,ratio_iceberg_over_paimon,paimon_rows,iceberg_rows" > "$CSV"

capture_state "before"

if [[ "$SUITE" == all || "$SUITE" == snapshot ]]; then
    run_suite SNAPSHOT uncompacted SNAPSHOT_NAMES SNAPSHOT_SQL
fi
if [[ "$SUITE" == all || "$SUITE" == hot ]]; then
    run_suite HOT uncompacted HOT_NAMES HOT_SQL
fi

# Optional third state: Iceberg after compaction. Paimon has no equivalent step
# because its LSM compacts continuously in the background — that asymmetry is
# itself one of the findings, so we do not "fix it up" by compacting Paimon too.
if [[ "$ICEBERG_MAINTENANCE" == 1 ]]; then
    printf '\n%s\n' "── Running Iceberg maintenance (rewrite_data_files) ────────────────"
    echo "NOTE: run via Spark/Athena — StarRocks cannot execute Iceberg stored procedures."
    echo "      See 4-STARROCKS-OLAP-ENGINE.md for the exact CALL statements."
    echo "      Re-run this script with SUITE=hot after maintenance completes to get"
    echo "      the post-compaction row of the comparison."
fi

printf '\n%s\n' "═══════════════════════════════════════════════════════════════════"
echo "  ✓ Done. CSV: ${CSV}"
echo "  RATIO > 1.00x means Iceberg took longer than Paimon."
echo "═══════════════════════════════════════════════════════════════════"
