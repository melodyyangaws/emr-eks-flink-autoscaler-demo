#!/usr/bin/env bash
set -uo pipefail

# StarRocks Paimon-vs-Iceberg benchmark read from the FE AUDIT LOG.
#
# Why this exists next to bench-hot-data.sh
# -----------------------------------------
# bench-hot-data.sh timed each query client-side around a `kubectl exec ... mysql`
# invocation. On this cluster that wrapper costs ~5.5s, so every query — COUNT(*)
# through a four-table join — landed within a few hundred ms of the same number and
# the Paimon/Iceberg ratios were really ratios of process-spawn noise. Measured
# directly: a bare `SELECT 1;` took 5,665ms while `SELECT COUNT(*)` took 6,079ms.
# The same COUNT(*) appears in the audit log as Time=684. The harness was 89% of
# every reading.
#
# So this script does not time anything from the shell. It:
#   1. records the FE audit log size,
#   2. runs the whole suite down ONE mysql connection in ONE kubectl exec,
#   3. reads back only the audit lines appended since step 1.
#
# The audit log gives Time (ms, server-side), ScanRows and ScanBytes per statement.
# ScanRows/ScanBytes are the real prize: they show *why* one format is slower, which
# a wall-clock number alone never can. For a merge-on-read table with accumulated
# delete files, scan volume is the whole story.
#
# Attribution is by marker query. SQL comments are stripped from the audit log's
# Stmt field (verified: `/*BENCH:Q1*/ SELECT ...` logs as plain `SELECT ...`), so a
# tagging comment cannot be used. A literal `SELECT 'MARK:p0:q3'` however is logged
# verbatim, so each real query is attributed to the marker immediately before it.
#
# Usage:
#   ./bench-server-side.sh
#   RUNS=5 ./bench-server-side.sh
#   SUITE=hot ./bench-server-side.sh

NAMESPACE="${NAMESPACE:-emr-flink}"
FE_POD="${FE_POD:-kube-starrocks-fe-0}"
AUDIT="${AUDIT:-/opt/starrocks/fe/log/fe.audit.log}"
PAIMON_CATALOG="${PAIMON_CATALOG:-paimon_catalog}"
PAIMON_DB="${PAIMON_DB:-flink_paimon_db}"
ICEBERG_CATALOG="${ICEBERG_CATALOG:-iceberg_catalog}"
ICEBERG_DB="${ICEBERG_DB:-flink_iceberg_db}"
SUITE="${SUITE:-all}"
RUNS="${RUNS:-3}"
WARMUP="${WARMUP:-1}"
# S3 bucket both formats must live in. The preflight below resolves each format's
# real physical read path and fails the run if either one leaves this bucket or if
# the two ever resolve to the same prefix (which would mean the "comparison" is
# reading one dataset twice). Default is derived from the account, matching
# build-deploy-generic.sh.
EXPECT_BUCKET="${EXPECT_BUCKET:-}"
OUT_DIR="${OUT_DIR:-./bench-results}"
mkdir -p "$OUT_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
CSV="${OUT_DIR}/starrocks-audit-${STAMP}.csv"

# ── Queries ─────────────────────────────────────────────────────────────────
# One statement per line, no trailing blank lines: the runner pairs line N of this
# output with query index N, so the layout is load-bearing.
#
# Every hot predicate uses UTC_TIMESTAMP(), never NOW(). The FE session timezone is
# +08:00 while CDC writes UTC values, so NOW() silently matches zero rows and the
# hot suite would report a fast empty scan as a win.
snapshot_sql() {
cat <<'EOSQL'
SELECT COUNT(*) FROM customers;
SELECT state, COUNT(*) c, COUNT(DISTINCT city) FROM customers GROUP BY state ORDER BY c DESC LIMIT 10;
SELECT c.customer_name, c.city, COUNT(o.order_id) oc, SUM(o.total_amount) spent FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.customer_name, c.city ORDER BY spent DESC LIMIT 20;
SELECT p.product_name, p.category, SUM(oi.quantity) units, SUM(oi.subtotal) rev FROM products p JOIN order_items oi ON p.product_id = oi.product_id GROUP BY p.product_id, p.product_name, p.category ORDER BY rev DESC LIMIT 20;
SELECT DATE(order_date) d, COUNT(*) oc, SUM(total_amount) rev, AVG(total_amount) aov FROM orders WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY DATE(order_date) ORDER BY d DESC;
SELECT DATE(o.order_date) d, c.state, p.category, COUNT(DISTINCT o.order_id) orders, SUM(oi.quantity) units, SUM(oi.subtotal) rev FROM orders o JOIN customers c ON o.customer_id = c.customer_id JOIN order_items oi ON o.order_id = oi.order_id JOIN products p ON oi.product_id = p.product_id WHERE o.order_status = 'DELIVERED' GROUP BY DATE(o.order_date), c.state, p.category ORDER BY d DESC, rev DESC LIMIT 100;
EOSQL
}

hot_sql() {
cat <<'EOSQL'
SELECT MAX(updated_at), TIMESTAMPDIFF(SECOND, MAX(updated_at), UTC_TIMESTAMP()), COUNT(*) FROM orders WHERE updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 5 MINUTE);
SELECT order_id, customer_id, order_status, total_amount, updated_at FROM orders WHERE order_id = (SELECT MAX(order_id) FROM orders);
SELECT order_status, COUNT(*) orders, SUM(total_amount) rev, AVG(total_amount) aov FROM orders WHERE updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE) GROUP BY order_status ORDER BY orders DESC;
SELECT o.order_status, COUNT(DISTINCT o.order_id) orders, SUM(oi.quantity) units, SUM(oi.subtotal) rev FROM orders o JOIN order_items oi ON o.order_id = oi.order_id WHERE o.updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE) GROUP BY o.order_status ORDER BY rev DESC;
EOSQL
}

SNAPSHOT_NAMES="Q1_count Q2_group_by Q3_join Q4_product_sales Q5_time_window Q6_multi_join"
HOT_NAMES="Q7_freshness Q8_hot_pk_lookup Q9_recent_window Q10_hot_join"

audit_size() {
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- stat -c %s "$AUDIT" 2>/dev/null
}

# Emit the SQL script: a marker before every query so the audit log can be split.
build_script() {
    local catalog=$1 db=$2 suite=$3 tag=$4
    echo "SET CATALOG ${catalog};"
    echo "USE ${db};"
    local pass qi line
    for (( pass = 0; pass < WARMUP + RUNS; pass++ )); do
        qi=0
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            echo "SELECT 'MARK_${tag}_${pass}_${qi}';"
            echo "$line"
            qi=$(( qi + 1 ))
        done < <(if [[ "$suite" == snapshot ]]; then snapshot_sql; else hot_sql; fi)
    done
}

# Run one format's suite. --force so a single statement error does not abort the
# remaining passes; -N -B to keep the result payload small (we never read it).
run_suite_sql() {
    local catalog=$1 db=$2 suite=$3 tag=$4
    build_script "$catalog" "$db" "$suite" "$tag" \
      | kubectl exec -i -n "$NAMESPACE" "$FE_POD" -- \
          mysql -h127.0.0.1 -P9030 -uroot -N -B --force >/dev/null 2>&1
}

sr1() { kubectl exec -n "$NAMESPACE" "$FE_POD" -- mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1" 2>/dev/null; }

# ── Main ────────────────────────────────────────────────────────────────────
echo "═══════════════════════════════════════════════════════════════════════"
echo "  StarRocks benchmark — server-side Time/ScanRows from FE audit log"
echo "═══════════════════════════════════════════════════════════════════════"
echo "  Paimon  : ${PAIMON_CATALOG}.${PAIMON_DB}"
echo "  Iceberg : ${ICEBERG_CATALOG}.${ICEBERG_DB}"
echo "  Suite   : ${SUITE}   Runs: ${RUNS} timed (+${WARMUP} warmup, discarded)"
echo "  Results : ${CSV}"
echo "═══════════════════════════════════════════════════════════════════════"

sr1 "SELECT 1;" >/dev/null || { echo "ERROR: cannot reach StarRocks FE in ${FE_POD}" >&2; exit 1; }

# ── Preflight: both formats must read from the same S3 bucket ───────────────
# This guards a failure mode that produces plausible-looking numbers instead of an
# error. The cluster carries stale v3 catalogs alongside the live ones
# (paimon_catalogv3 -> paimonv3-warehouse/, icebergv3_catalog -> icebergv3-warehouse/),
# so a mistyped PAIMON_CATALOG/ICEBERG_CATALOG silently benchmarks a different,
# frozen dataset in a different prefix and still prints a full ratio table. Storage
# location is the one variable this benchmark must hold fixed — the whole claim is
# "same bucket, same region, same S3 endpoint, only the table format differs" — so
# it is asserted rather than assumed.
#
# The two paths are resolved from what StarRocks will actually read, not from the
# catalog properties:
#   Iceberg — the file_path of a real data file from the <table>$files metadata
#             table. Iceberg data can live outside the catalog warehouse (the Glue
#             table's own location wins), so only a data file proves the location.
#   Paimon  — paimon.catalog.warehouse from SHOW CREATE CATALOG. Paimon's $files
#             metadata table is not readable through this connector ("Failed to
#             find latest snapshot id"), so the catalog root is the best available
#             signal; it is a filesystem catalog, so the root does determine reads.
resolve_bucket() { sed -n 's#^s3[an]*://\([^/]*\)/.*#\1#p' <<<"$1" | head -1; }

ICE_FILE=$(sr1 "SELECT file_path FROM ${ICEBERG_CATALOG}.${ICEBERG_DB}.\`customers\$files\` LIMIT 1;" | head -1)
PAI_ROOT=$(kubectl exec -n "$NAMESPACE" "$FE_POD" -- mysql -h127.0.0.1 -P9030 -uroot -N -B \
             -e "SHOW CREATE CATALOG ${PAIMON_CATALOG};" 2>/dev/null \
           | grep -o '"paimon.catalog.warehouse"[^"]*"[^"]*"' | grep -o 's3[an]*://[^"]*' | head -1)

[[ -n "$ICE_FILE" ]] || { echo "ERROR: could not resolve an Iceberg data file path from ${ICEBERG_CATALOG}.${ICEBERG_DB}" >&2; exit 1; }
[[ -n "$PAI_ROOT" ]] || { echo "ERROR: could not resolve paimon.catalog.warehouse from ${PAIMON_CATALOG}" >&2; exit 1; }

PAI_BUCKET=$(resolve_bucket "$PAI_ROOT")
ICE_BUCKET=$(resolve_bucket "$ICE_FILE")
# Compare the table-level prefixes, not the full file path: a data file sits many
# levels below the table root, so the raw strings would never match anyway.
PAI_PREFIX="${PAI_ROOT#*://}"
ICE_PREFIX=$(sed -n 's#^s3[an]*://[^/]*/\(.*\)/customers/.*#\1#p' <<<"$ICE_FILE" | head -1)

printf '\n%s\n' "── Storage location (asserted, not assumed) ───────────────────────────"
printf '  %-8s %s\n' "Paimon"  "$PAI_ROOT"
printf '  %-8s %s\n' "Iceberg" "$ICE_FILE"

if [[ -z "$EXPECT_BUCKET" ]]; then
    EXPECT_BUCKET="$PAI_BUCKET"
    printf '  %-8s %s (from %s)\n' "bucket" "$EXPECT_BUCKET" "$PAIMON_CATALOG"
else
    printf '  %-8s %s (required)\n' "bucket" "$EXPECT_BUCKET"
fi

fail=0
[[ "$PAI_BUCKET" == "$EXPECT_BUCKET" ]] || {
    echo "  ✗ Paimon bucket '${PAI_BUCKET}' != expected '${EXPECT_BUCKET}'" >&2; fail=1; }
[[ "$ICE_BUCKET" == "$EXPECT_BUCKET" ]] || {
    echo "  ✗ Iceberg bucket '${ICE_BUCKET}' != expected '${EXPECT_BUCKET}'" >&2; fail=1; }
# Identical prefixes would mean both catalogs point at one dataset, so every ratio
# would be ~1.00x for reasons that have nothing to do with the table formats.
[[ "$PAI_PREFIX" != "$ICE_PREFIX" ]] || {
    echo "  ✗ Paimon and Iceberg resolve to the SAME prefix '${PAI_PREFIX}' — not a comparison" >&2; fail=1; }
(( fail == 0 )) || {
    echo "" >&2
    echo "ERROR: storage-location preflight failed. Both formats must be written by the" >&2
    echo "       CDC pipelines into bucket '${EXPECT_BUCKET}' under distinct prefixes." >&2
    echo "       Check PAIMON_CATALOG/ICEBERG_CATALOG — the stale *v3* catalogs on this" >&2
    echo "       cluster point at paimonv3-warehouse/ and icebergv3-warehouse/." >&2
    exit 1; }
printf '  ✓ same bucket, distinct prefixes: %s/ vs %s/\n' "$PAI_PREFIX" "$ICE_PREFIX"

# ── Table state: the numbers that explain the timings ───────────────────────
printf '\n%s\n' "── Table state ────────────────────────────────────────────────────────"
printf '%-14s %14s %14s %10s %10s\n' TABLE PAIMON_rows ICEBERG_rows ICE_files ICE_snaps
STATE_TSV=""
for t in customers products orders order_items; do
    pc=$(sr1 "SELECT COUNT(*) FROM ${PAIMON_CATALOG}.${PAIMON_DB}.${t};" | head -1)
    ic=$(sr1 "SELECT COUNT(*) FROM ${ICEBERG_CATALOG}.${ICEBERG_DB}.${t};" | head -1)
    f=$(sr1  "SELECT COUNT(*) FROM ${ICEBERG_CATALOG}.${ICEBERG_DB}.\`${t}\$files\`;" | head -1)
    s=$(sr1  "SELECT COUNT(*) FROM ${ICEBERG_CATALOG}.${ICEBERG_DB}.\`${t}\$snapshots\`;" | head -1)
    printf '%-14s %14s %14s %10s %10s\n' "$t" "${pc:-?}" "${ic:-?}" "${f:-?}" "${s:-?}"
    STATE_TSV+="${t},${pc:-},${ic:-},${f:-},${s:-}"$'\n'
done

# Mark the audit log here: everything appended from now on is benchmark traffic.
OFF=$(audit_size)
[[ -z "$OFF" ]] && { echo "ERROR: cannot stat ${AUDIT}" >&2; exit 1; }

RUN_SUITES=""
[[ "$SUITE" == all || "$SUITE" == snapshot ]] && RUN_SUITES="snapshot"
[[ "$SUITE" == all || "$SUITE" == hot      ]] && RUN_SUITES="${RUN_SUITES} hot"

for s in $RUN_SUITES; do
    echo "  running ${s} suite: paimon..."
    run_suite_sql "$PAIMON_CATALOG"  "$PAIMON_DB"  "$s" "P${s}"
    echo "  running ${s} suite: iceberg..."
    run_suite_sql "$ICEBERG_CATALOG" "$ICEBERG_DB" "$s" "I${s}"
done

# The FE appends audit records asynchronously after the result is returned; give it
# a moment so the last query of the last suite is not missing from the tail.
sleep 4

RAW=$(kubectl exec -n "$NAMESPACE" "$FE_POD" -- \
        bash -c "tail -c +$((OFF+1)) '${AUDIT}'" 2>/dev/null)

# ── Parse ───────────────────────────────────────────────────────────────────
# Walk the audit lines in order. A MARK_<tag>_<pass>_<qi> statement arms the next
# real query; that query's Time/ScanRows/ScanBytes are attributed to it. Only the
# first query after a marker counts, so an unexpected extra statement cannot be
# silently credited to the wrong slot.
echo "stamp,suite,query,metric,paimon,iceberg,ratio_iceberg_over_paimon" > "$CSV"

PARSED=$(printf '%s\n' "$RAW" | python3 -c '
import sys, re
cur = None
out = []
for line in sys.stdin:
    m = re.search(r"\|Stmt=(.*?)\|Digest=", line, re.S)
    if not m:
        continue
    stmt = m.group(1).strip()
    mk = re.match(r"^SELECT .MARK_(\w+)_(\d+)_(\d+).$", stmt)
    if mk:
        cur = (mk.group(1), int(mk.group(2)), int(mk.group(3)))
        continue
    if cur is None:
        continue
    def fld(name, default="0"):
        mm = re.search(r"\|" + name + r"=([^|]*)", line)
        return mm.group(1) if mm and mm.group(1) != "" else default
    tag, p, q = cur
    out.append("%s %d %d %s %s %s %s" % (tag, p, q, fld("Time"), fld("ScanRows"), fld("ScanBytes"), fld("State")))
    cur = None
print("\n".join(out))
')

med() { sort -n | awk '{a[NR]=$1} END{ if(NR==0){print "-";exit} if(NR%2) printf "%d\n", a[(NR+1)/2]; else printf "%d\n", (a[NR/2]+a[NR/2+1])/2 }'; }

pick() { # tag pass_min qi col
    printf '%s\n' "$PARSED" | awk -v t="$1" -v w="$2" -v q="$3" -v c="$4" \
        '$1==t && $2>=w && $3==q {print $c}' | med
}

for s in $RUN_SUITES; do
    if [[ "$s" == snapshot ]]; then names="$SNAPSHOT_NAMES"; else names="$HOT_NAMES"; fi
    set -- $names
    NAMES=( "$@" )

    printf '\n%s\n' "────────────────────────────────────────────────────────────────────────"
    printf '  %s suite — StarRocks server-side (median of %s runs)\n' \
        "$(echo "$s" | tr '[:lower:]' '[:upper:]')" "$RUNS"
    printf '%s\n' "────────────────────────────────────────────────────────────────────────"
    printf '%-20s %9s %9s %8s %13s %13s\n' QUERY PAI_ms ICE_ms RATIO PAI_scanrows ICE_scanrows

    for qi in "${!NAMES[@]}"; do
        name=${NAMES[$qi]}
        p_ms=$(pick "P${s}" "$WARMUP" "$qi" 4)
        i_ms=$(pick "I${s}" "$WARMUP" "$qi" 4)
        p_sr=$(pick "P${s}" "$WARMUP" "$qi" 5)
        i_sr=$(pick "I${s}" "$WARMUP" "$qi" 5)
        p_sb=$(pick "P${s}" "$WARMUP" "$qi" 6)
        i_sb=$(pick "I${s}" "$WARMUP" "$qi" 6)

        if [[ "$p_ms" == "-" || "$i_ms" == "-" || "$p_ms" == 0 ]]; then
            ratio="n/a"
        else
            ratio=$(awk -v a="$i_ms" -v b="$p_ms" 'BEGIN{printf "%.2fx", a/b}')
        fi
        printf '%-20s %9s %9s %8s %13s %13s\n' "$name" "$p_ms" "$i_ms" "$ratio" "$p_sr" "$i_sr"

        srr=$(awk -v a="$i_sr" -v b="$p_sr" 'BEGIN{ if(b>0) printf "%.2fx", a/b; else print "n/a" }')
        sbr=$(awk -v a="$i_sb" -v b="$p_sb" 'BEGIN{ if(b>0) printf "%.2fx", a/b; else print "n/a" }')
        {
          echo "${STAMP},${s},${name},time_ms,${p_ms},${i_ms},${ratio}"
          echo "${STAMP},${s},${name},scan_rows,${p_sr},${i_sr},${srr}"
          echo "${STAMP},${s},${name},scan_bytes,${p_sb},${i_sb},${sbr}"
        } >> "$CSV"
    done
done

printf '\n%s' "$STATE_TSV" | awk -F, 'NF>1 {print "'"${STAMP}"',state,"$1",rows_and_files,"$2","$3",ice_files="$4" ice_snaps="$5}' >> "$CSV"

# Record the asserted storage locations in the CSV too, so a result file carries its
# own proof that both formats were read from the same bucket.
{
  echo "${STAMP},location,bucket,s3_bucket,${PAI_BUCKET},${ICE_BUCKET},same"
  echo "${STAMP},location,prefix,s3_prefix,${PAI_PREFIX},${ICE_PREFIX},distinct"
} >> "$CSV"

printf '\n%s\n' "═══════════════════════════════════════════════════════════════════════"
echo "  ✓ CSV: ${CSV}"
echo "  RATIO > 1.00x means Iceberg cost more than Paimon."
echo "  ScanRows is the explanatory metric: merge-on-read reads deletes too."
echo "═══════════════════════════════════════════════════════════════════════"
