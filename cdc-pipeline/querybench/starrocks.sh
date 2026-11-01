#!/usr/bin/env bash
set -uo pipefail

# StarRocks read benchmark over the four CDC tables, three arms:
#   paimon          Paimon reader  -> Paimon tables (flink_paimon_db)
#   pmn_as_iceberg  Iceberg reader -> the SAME Paimon files, via Paimon's Iceberg export
#   iceberg         Iceberg reader -> the Iceberg tables (flink_iceberg_db)
# (databases and catalogs carry CDC_DB_SUFFIX). On an Iceberg v3 table set
# (ICEBERG_FORMAT_VERSION=3) only paimon runs: StarRocks 4.1.x cannot read v3
# deletion vectors, so the two Iceberg-reader arms are dropped from ARMS
# (SR_V3_ARMS=1 keeps them and records their errors; lib/querybench.sh).
# Adapted from throughput-benchmark/querybench/starrocks.sh: timed from the FE
# AUDIT LOG (a `kubectl exec ... mysql` costs ~5.5 s), one detached mysql
# session per arm inside the FE pod, a marker SELECT before every statement.
#
#   SUITE=hot|snapshot ./querybench/starrocks.sh    one suite, all arms, WARMUP=1 RUNS=3
#   ARMS="paimon iceberg" ./querybench/starrocks.sh
#   ./querybench/starrocks.sh catalogs              create this harness's catalogs, check each reads a row
#   ./querybench/starrocks.sh render <suite> [arm]  print the rendered statements (no cluster; dry run)
#
# The pins (QB_PIN_*, querybench/pin.sh) and windows come from the caller's
# environment: hot-interleave.sh for hot, run.sh's runner step for snapshot.
# Run by hand without pins, every arm reads its tables' CURRENT version.
#
# Output: $OUT_DIR/starrocks-<STAMP>.csv ($QB_CSV_HEADER) and
# starrocks-<STAMP>-rows.tsv (every result row of each query's last timed pass:
# arm, suite, query, row number, the row |-joined), which the summary compares
# across arms.

# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/querybench.sh"

AUDIT="/opt/starrocks/fe/log/fe.audit.log"
ARMS="$(qb_sr_arms_filter "${ARMS:-$SR_ARMS_ALL}")"
CSV="${OUT_DIR}/starrocks-${STAMP}.csv"
ROWS_TSV="${CSV%.csv}-rows.tsv"
RUN_ID="${STAMP//-/_}_$$"
QB_PLANNER_TIMEOUT_MS="${QB_PLANNER_TIMEOUT_MS:-300000}"

render_suite() {   # <suite> <arm> -> one statement per line, in file order
    local f files
    files=$(qb_query_files "$1") || return 1
    for f in $files; do qb_render_oneline "$f" starrocks "$2" || return 1; done
}
suite_names() { local f; for f in $(qb_query_files "$1"); do qb_query_name "$f"; done; }

# The arm's whole suite as one script; tables are fully qualified, so no SET CATALOG.
build_script() {   # <arm> <suite> <rendered sql>
    local pass qi line files narrow=" "
    echo "SET query_timeout = $(qb_query_timeout "$2");"
    # The planner's own limit (default 3 s): a fresh FE's first plan of a table
    # loads all its metadata (4.5 s for a compacted Paimon table, 2026-10-22).
    echo "SET new_planner_optimize_timeout = ${QB_PLANNER_TIMEOUT_MS};"
    # Datacache on (StarRocks' default), warm vs warm with DuckDB's block cache;
    # both emptied before the test (clear-caches.sh), filled by the same warm-up.
    echo "SET enable_scan_datacache = true;"
    echo "SET populate_datacache_mode = 'auto';"
    echo "SET enable_spill = true;"
    # Iceberg v3 set, paimon arm: the JNI reader (64-bit DVs; lib qb_sr_sets).
    [[ -n "$(qb_sr_sets "$1")" ]] && echo "$(qb_sr_sets "$1")"
    if [[ "$2" == snapshot ]]; then
        # Partition-wide scans OOM scan-side, which spill cannot reach (memory
        # note). Both SETs, or the adaptive scheduler raises the count again.
        echo "SET connector_io_tasks_per_scan_operator = ${SR_SNAPSHOT_IO_TASKS:-2};"
        echo "SET enable_connector_adaptive_io_tasks = false;"
    fi
    files=$(qb_query_files "$2"); qi=0
    for line in $files; do
        qb_query_flag "$line" io-tasks=1 && narrow+="${qi} "
        qi=$(( qi + 1 ))
    done
    for (( pass = 0; pass < WARMUP + RUNS; pass++ )); do
        qi=0
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            [[ "$narrow" == *" ${qi} "* ]] && echo "SET connector_io_tasks_per_scan_operator = 1;"
            echo "SELECT 'MARK_${RUN_ID}_${1}_${pass}_${qi}';"
            echo "$line"
            [[ "$narrow" == *" ${qi} "* ]] && echo "SET connector_io_tasks_per_scan_operator = ${SR_SNAPSHOT_IO_TASKS:-2};"
            qi=$(( qi + 1 ))
        done <<< "$3"
    done
}

# ── Audit log (unchanged from throughput-benchmark) ─────────────────────────
audit_mark() {
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- bash -c \
        'echo "$(stat -c %s "$1") $(date -d "1 minute ago" "+%Y-%m-%d %H:%M:%S")"' _ "$AUDIT" 2>/dev/null
}
audit_since() {   # <offset> <since>
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- bash -c '
        log=$1 off=$2 since=$3
        rot=$(find "$(dirname "$log")" -maxdepth 1 -name "$(basename "$log").*" -newermt "$since" | sort)
        if [ -n "$rot" ]; then cat $rot "$log"; else tail -c +"$((off + 1))" "$log"; fi
    ' _ "$AUDIT" "$1" "$2" 2>/dev/null
}
# Audit lines -> "arm pass qi time scan_rows scan_bytes return_rows state" for
# the FIRST statement after each of this run's markers in the same session.
parse_audit() {
    RUN_ID="$RUN_ID" python3 -c '
import os, re, sys
mark = re.compile(r"^SELECT .MARK_" + re.escape(os.environ["RUN_ID"]) + r"_([a-z_]+)_(\d+)_(\d+).;?$")
def fld(s, k):
    m = re.search(r"\|" + k + r"=([^|]*)", s)
    return (m.group(1) if m else "") or "-"
armed = {}
for line in sys.stdin:
    if "[slow_query]" in line:
        continue
    m = re.search(r"\|Stmt=(.*?)\|Digest=", line)
    if not m:
        continue
    head, stmt, sess = line[:m.start()], m.group(1).strip(), fld(line[m.end():], "SessionId")
    mk = mark.match(stmt)
    if mk:
        armed[sess] = mk.groups()
    elif stmt.startswith("SELECT ") and "MARK_" in stmt:
        continue
    elif sess in armed:
        a, p, q = armed.pop(sess)
        print(a, p, q, *(fld(head, k) for k in ("Time", "ScanRows", "ScanBytes", "ReturnRows", "State")))
'
}
arm_result() {   # <arm> <qi> ; records on stdin -> "status ms scan_rows scan_bytes result_rows"
    local recs
    recs=$(awk -v a="$1" -v q="$2" -v w="$WARMUP" '$1==a && $2>=w && $3==q')
    [[ -z "$recs" ]] && { echo "MISSING - - - -"; return; }
    awk '$8!="EOF" && $8!="OK"' <<<"$recs" | grep -q . && { echo "ERROR - - - -"; return; }
    local c v out="OK"
    for c in 4 5 6 7; do
        v=$(awk -v c="$c" '$c ~ /^[0-9]+$/ {print $c}' <<<"$recs" | qb_median)
        out+=" ${v:--}"
    done
    echo "$out"
}

# ── Catalogs ────────────────────────────────────────────────────────────────
# Paimon: filesystem catalog on the warehouse (the Flink side uses the Glue
# metastore, but the layout <warehouse>/<db>.db/<table> is read directly).
# Iceberg: Glue. Paimon's export: hadoop catalog on <warehouse>/iceberg/ (the
# Glue entry of a Paimon table has PaimonStorageHandler, which StarRocks'
# Glue Iceberg catalog cannot load). Credentials: starrocks-sa's IRSA role.
ensure_catalog() {   # <arm>
    local ddl
    case "$1" in
        paimon) ddl="CREATE EXTERNAL CATALOG IF NOT EXISTS ${SR_PAIMON_CATALOG} PROPERTIES (
    \"type\" = \"paimon\", \"paimon.catalog.type\" = \"filesystem\",
    \"paimon.catalog.warehouse\" = \"${PAIMON_WAREHOUSE}/\",
    \"aws.s3.region\" = \"${AWS_REGION}\", \"aws.s3.use_aws_sdk_default_behavior\" = \"true\")" ;;
        iceberg) ddl="CREATE EXTERNAL CATALOG IF NOT EXISTS ${SR_ICEBERG_CATALOG} PROPERTIES (
    \"type\" = \"iceberg\", \"iceberg.catalog.type\" = \"glue\",
    \"aws.glue.region\" = \"${AWS_REGION}\", \"aws.glue.use_aws_sdk_default_behavior\" = \"true\",
    \"aws.s3.region\" = \"${AWS_REGION}\", \"aws.s3.use_aws_sdk_default_behavior\" = \"true\")" ;;
        pmn_as_iceberg) ddl="CREATE EXTERNAL CATALOG IF NOT EXISTS ${SR_PMNICE_CATALOG} PROPERTIES (
    \"type\" = \"iceberg\", \"iceberg.catalog.type\" = \"hadoop\",
    \"iceberg.catalog.warehouse\" = \"${PAIMON_WAREHOUSE}/iceberg/\",
    \"aws.s3.region\" = \"${AWS_REGION}\", \"aws.s3.use_aws_sdk_default_behavior\" = \"true\")" ;;
        *) qb_die "unknown arm '$1'" ;;
    esac
    qb_sr "$ddl" >/dev/null
    local t arm="$1" r; set -- $(qb_sr_target "$1")
    if [[ -n "$(qb_sr_sets "$arm")" ]]; then
        # The FE must know the variable, or every paimon-arm read of the v3 set
        # would go through the native reader that cannot decode 64-bit DVs.
        r=$(qb_sr "SHOW VARIABLES LIKE 'paimon_force_jni_reader'")
        [[ "$r" == paimon_force_jni_reader* ]] \
            || qb_die "Iceberg v3 set: this StarRocks ($(qb_sr 'SELECT current_version()' | head -1)) has no session variable paimon_force_jni_reader; its native Paimon reader cannot decode the 64-bit deletion vectors of the v3 Paimon tables (SR_PAIMON_JNI=0 to run it anyway)"
    fi
    for t in $QB_TABLES; do
        r=$(qb_sr_try "$(qb_sr_sets "$arm")SET new_planner_optimize_timeout = ${QB_PLANNER_TIMEOUT_MS}; SELECT 1 FROM ${1}.${2}.${t} LIMIT 1")
        [[ "$r" == 0$'\t'?* ]] && continue
        # v3 with SR_V3_ARMS=1: the arm runs to record this very error.
        if qb_sr_v3_forced "$arm"; then
            qb_log "Iceberg v3, SR_V3_ARMS=1: ${1}.${2}.${t} not readable (${r#*$'\t'}); the ${arm} arm runs anyway and records the error"
            continue
        fi
        qb_die "cannot read a row from ${1}.${2}.${t}: ${r#*$'\t'}"
    done
}

# ── One arm, run INSIDE the FE pod (detached; see throughput-benchmark's
# starrocks.sh for why it is not piped down kubectl exec) ───────────────────
run_arm_in_pod() {   # <arm> <suite> <rendered sql>
    local f="/tmp/qb-${RUN_ID}-$1" rc="" t0 fails=0
    local limit=$(( (WARMUP + RUNS) * $(grep -c . <<<"$3") * $(qb_query_timeout "$2") + 600 ))
    build_script "$@" | kubectl exec -i -n "$NAMESPACE" "$FE_POD" -- sh -c "cat > ${f}.sql" \
        || qb_die "cannot copy the ${1} script into ${FE_POD}"
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- sh -c \
        "rm -f ${f}.rc; setsid nohup sh -c 'mysql -h127.0.0.1 -P9030 -uroot -N -B --force < ${f}.sql > ${f}.out 2>&1; echo \$? > ${f}.rc' > /dev/null 2>&1 < /dev/null &" \
        || qb_die "cannot start the ${1} arm in ${FE_POD}"
    t0=$(date +%s)
    while :; do
        sleep 30
        if rc=$(kubectl exec -n "$NAMESPACE" "$FE_POD" -- sh -c \
                "cat ${f}.rc 2>/dev/null || { [ -f ${f}.sql ] && pgrep -f 'mysql.*[<] ${f}.sql' >/dev/null && echo running; } || echo lost" 2>/dev/null); then
            fails=0
            [[ "$rc" == lost ]] && qb_die "${2}: arm ${1}: its script or mysql session is gone from ${FE_POD} (FE restarted?)"
            [[ "$rc" != running ]] && break
        else
            fails=$((fails + 1)); (( fails % 10 == 1 )) && qb_log "${2}: arm ${1}: cannot reach ${FE_POD} (retrying)"
        fi
        (( $(date +%s) - t0 < limit )) || qb_die "${2}: arm ${1} still running after ${limit}s"
    done
    qb_log "${2}: arm ${1} done in $(( $(date +%s) - t0 ))s (mysql exit ${rc})"
    save_rows "$1" "$2" "$f"
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- sh -c "rm -f ${f}.sql ${f}.out ${f}.rc" >/dev/null 2>&1 || true
}

# Every result row of each query's LAST pass -> ROWS_TSV (arm, suite, query,
# row number, fields |-joined), and its first row -> -firstrows.csv (as
# throughput writes it). With -N -B the lines between a query's marker and the
# next marker are its rows; an ERROR line from --force lands there too and is
# kept (it differs from the other arms' rows, which is the point).
save_rows() {   # <arm> <suite> <pod file stem>
    local out names last=$(( WARMUP + RUNS - 1 ))
    out=$(kubectl exec -n "$NAMESPACE" "$FE_POD" -- cat "${3}.out" 2>/dev/null) || return 0
    names=$(suite_names "$2" | tr '\n' ' ')
    awk -v run="MARK_${RUN_ID}_${1}_" -v arm="$1" -v suite="$2" -v names="$names" -v last="$last" \
        -v rows="$ROWS_TSV" -v first="${CSV%.csv}-firstrows.csv" '
        BEGIN { split(names, nm, " ") }
        index($0, "MARK_") == 1 { want = 0 }
        index($0, run) == 1 { split(substr($0, length(run) + 1), pq, "_"); want = (pq[1] == last); qi = pq[2]; n = 0; next }
        want { gsub(/\t/, "|"); n++; print arm "\t" suite "\t" nm[qi + 1] "\t" n "\t" $0 >> rows
               if (n == 1) print arm "," suite "," nm[qi + 1] "," $0 >> first }
    ' <<<"$out"
}

# The CURRENT data files per table on the Iceberg-reader arms ($files; with
# the writers stopped that is the pinned version). The pinned snapshots'
# delete-file and delete counts are in pin.sh's tables TSV.
table_state() {   # <suite>
    local s="$1" arm t c
    for arm in $ARMS; do
        [[ "$arm" == paimon ]] && continue
        set -- $(qb_sr_target "$arm")
        for t in $QB_TABLES; do
            c=$(qb_sr "SELECT COUNT(*), SUM(record_count), SUM(file_size_in_bytes) FROM ${1}.${2}.\`${t}\$files\`" | head -1 | tr '\t' ' ')
            echo "# state suite=${s} arm=${arm} table=${t} data_files/records/bytes=${c:-?}"
        done
    done
}

run_suite() {   # <suite>
    local s=$1 arm writers mark raw recs n names qi name r state sql
    state=$(table_state "$s"); echo "$state"
    writers=$(qb_writer_state)
    qb_log "${s}: writers ${writers}"
    mark=$(audit_mark) && [[ "$mark" == *" "* ]] || qb_die "cannot stat ${AUDIT} in ${FE_POD}"
    for arm in $ARMS; do
        sql=$(render_suite "$s" "$arm") || qb_die "${s} suite failed to render for ${arm}"
        qb_log "${s}: arm ${arm}..."
        run_arm_in_pod "$arm" "$s" "$sql"
    done
    qb_writers_unchanged "$writers" "$s" || exit 1
    sleep 4
    raw=$(audit_since "${mark%% *}" "${mark#* }")
    recs=$(parse_audit <<<"$raw")
    n=$(grep -c . <<<"$recs")
    (( n >= 1 )) || qb_die "parsed no audit records for ${s} since ${mark#* } (pod clock)"
    qb_csv_init "$CSV"
    { echo "# suite=${s} writers: ${writers}"
      echo "# suite=${s} runs=${RUNS} warmup=${WARMUP} arms=${ARMS} run_id=${RUN_ID} sr_pin_form=${SR_PIN_FORM:-clause}"
      echo "# suite=${s} iceberg_format_version=${ICEBERG_FORMAT_VERSION:-3} cdc_db_suffix=${CDC_DB_SUFFIX:-} skipped_arms=${QB_SR_SKIPPED// /,} sr_version=$(qb_sr 'SELECT current_version()' | head -1) paimon_force_jni_reader=$(qb_sr_jni && echo true || echo false)"
      echo "$state"; } >> "$CSV"
    names=$(suite_names "$s"); qi=0
    printf '\n  %s suite: StarRocks server-side ms, median of %s runs\n  %-30s' "$s" "$RUNS" QUERY
    for arm in $ARMS; do printf ' %15s' "$arm"; done; echo
    for name in $names; do
        printf '  %-30s' "$name"
        for arm in $ARMS; do
            r=$(arm_result "$arm" "$qi" <<<"$recs")
            set -- $r
            echo "${STAMP},starrocks,${arm},${s},${name},${1},${2#-},${5#-},${3#-},${4#-}" >> "$CSV"
            [[ "$1" == OK ]] && printf ' %15s' "$2" || printf ' %15s' "$1"
        done
        echo
        qi=$(( qi + 1 ))
    done
}

main() {
    local arm s
    [[ -n "${ARMS// /}" ]] || qb_die "ARMS is empty (Iceberg v3 leaves StarRocks the paimon arm only)"
    for arm in $ARMS; do qb_sr_target "$arm" >/dev/null || qb_die "unknown arm '${arm}' (paimon|pmn_as_iceberg|iceberg)"; done
    mkdir -p "$OUT_DIR"
    for arm in $ARMS; do ensure_catalog "$arm"; done
    qb_assert_alone starrocks
    for s in $(qb_suites); do
        qb_start_suite "$s"
        # H8's key: exported by hot-interleave.sh; resolved here when run by hand.
        if [[ "$s" == hot && -z "${QB_LOOKUP_ORDER_ID:-}" ]] && grep -q 'LOOKUP_ORDER' "${QB_DIR}"/hot/*.sql; then
            qb_resolve_lookup
        fi
        for arm in $ARMS; do render_suite "$s" "$arm" >/dev/null || qb_die "${s} suite failed to render for ${arm}"; done
        run_suite "$s"
    done
    echo ""
    echo "Results -> ${CSV}"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    case "${1:-}" in
        catalogs) for arm in $ARMS; do ensure_catalog "$arm"; qb_log "catalog for ${arm}: ok"; done ;;
        render)   # dry run: no cluster. Windows/pins from the environment, else placeholders.
                  s="${2:?render <hot|snapshot> [arm]}"; arms="${3:-$ARMS}"
                  for arm in $arms; do echo "-- arm ${arm}"; render_suite "$s" "$arm" || exit 1; done ;;
        "")       main ;;
        *)        qb_die "usage: $0 [catalogs | render <suite> [arm]]" ;;
    esac
fi
