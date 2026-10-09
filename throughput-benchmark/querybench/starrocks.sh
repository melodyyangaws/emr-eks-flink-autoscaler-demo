#!/usr/bin/env bash
set -uo pipefail

# StarRocks read benchmark over the append-only cid_events table, three arms:
#   paimon          Paimon reader  -> Paimon table
#   pmn_as_iceberg  Iceberg reader -> the SAME Paimon files, via Paimon's Iceberg export
#   iceberg         Iceberg reader -> natively written Iceberg table
# paimon vs pmn_as_iceberg changes only the reader; pmn_as_iceberg vs iceberg
# changes only the writer.
#
# Timed from the FE AUDIT LOG, never the shell: one `kubectl exec ... mysql`
# costs ~5.5s here, more than many of the queries. Each arm's whole suite goes
# down ONE connection, a marker SELECT before every statement, and the audit
# records after each marker give Time/ScanRows/ScanBytes/State.
#
# Hot runs first, against RUNNING writers (it reads the last minutes of data);
# then the writers are stopped for snapshot (live writers cost ~89x there).
# Writers are left stopped; resume with ../build-deploy-cid.sh.
#
#   ./starrocks.sh                      both suites, all arms, WARMUP=1 RUNS=3
#   SUITE=hot ./starrocks.sh            one suite (hot|snapshot)
#   RUNS=5 ./starrocks.sh
#   ARMS="paimon iceberg" ./starrocks.sh    e.g. when ICEBERG_COMPAT is off
#   SUSPEND_WRITERS=0 ./starrocks.sh    do not touch the writers
#
# Output: $OUT_DIR/starrocks-<STAMP>.csv ($QB_CSV_HEADER, one row per arm x
# query; `#` lines carry writer state and partition row counts per suite).

# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

AUDIT="${AUDIT:-/opt/starrocks/fe/log/fe.audit.log}"
ARMS="${ARMS:-paimon pmn_as_iceberg iceberg}"
PAIMON_CATALOG="${PAIMON_CATALOG:-cid_paimon_catalog}"
ICEBERG_CATALOG="${ICEBERG_CATALOG:-cid_iceberg_catalog}"
# hadoop, not glue: Paimon registers its export in Glue with PaimonStorageHandler,
# which StarRocks' Glue Iceberg catalog cannot load.
PMNICE_CATALOG="${PMNICE_CATALOG:-cid_pmn_as_ice}"
PMNICE_WAREHOUSE="${PMNICE_WAREHOUSE:-s3://${BUCKET_NAME}/cid-warehouse-paimon/iceberg/}"
CSV="${OUT_DIR}/starrocks-${STAMP}.csv"
# Unique per run: a killed earlier run's markers in the same log must never match.
RUN_ID="${STAMP}_$$"
QB_TABLE="cid_events"     # reached by name after SET CATALOG / USE
QB_BYTES_FN="LENGTH"      # StarRocks LENGTH counts bytes

arm_target() {   # <arm> -> "catalog db"
    case "$1" in
        paimon)         echo "$PAIMON_CATALOG $PAIMON_DB" ;;
        pmn_as_iceberg) echo "$PMNICE_CATALOG $PAIMON_DB" ;;
        iceberg)        echo "$ICEBERG_CATALOG $ICEBERG_DB" ;;
        *) return 1 ;;
    esac
}

# ── Queries ─────────────────────────────────────────────────────────────────
render_suite() {   # <suite> -> one statement per line, in file order
    local f files
    files=$(qb_query_files "$1") || return 1
    for f in $files; do qb_render_oneline "$f" || return 1; done
}
suite_names() { local f; for f in $(qb_query_files "$1"); do qb_query_name "$f"; done; }

# The arm's whole suite as one script. Markers are SELECTs, not comments: the
# audit log strips comments from Stmt.
build_script() {   # <arm> <suite> <rendered sql>
    local t pass qi line
    t=$(arm_target "$1")
    echo "SET CATALOG ${t% *};"
    echo "USE ${t#* };"
    # Per-suite cap shared with DuckDB, so a timeout means the same on both.
    echo "SET query_timeout = $(qb_query_timeout "$2");"
    # Datacache off: a fixed partition is otherwise served from BE cache after
    # pass 0 (ScanBytes=0, 70s -> 2s), a cache hit scored as a format result.
    echo "SET enable_scan_datacache = false;"
    echo "SET enable_datacache_sharing = false;"
    echo "SET populate_datacache_mode = 'never';"
    # Spill: wide aggregates and COUNT(DISTINCT) can reach the memory ceiling.
    echo "SET enable_spill = true;"
    if [[ "$2" == snapshot ]]; then
        # Full-partition scans OOM scan-side, which spill cannot reach. Both
        # SETs are needed or the adaptive scheduler raises the task count again.
        echo "SET connector_io_tasks_per_scan_operator = 2;"
        echo "SET enable_connector_adaptive_io_tasks = false;"
    fi
    for (( pass = 0; pass < WARMUP + RUNS; pass++ )); do
        qi=0
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            echo "SELECT 'MARK_${RUN_ID}_${1}_${pass}_${qi}';"
            echo "$line"
            qi=$(( qi + 1 ))
        done <<< "$3"
    done
}

# ── Audit log ───────────────────────────────────────────────────────────────
# "<byte offset> <pod time a minute ago>". Pod clock, since find -newermt runs there.
audit_mark() {
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- bash -c \
        'echo "$(stat -c %s "$1") $(date -d "1 minute ago" "+%Y-%m-%d %H:%M:%S")"' _ "$AUDIT" 2>/dev/null
}

# Everything appended since audit_mark. The FE rolls the log (seen at midnight)
# to fe.audit.log.<date>-<n>; if that happened, read the rotated files newer than
# the start plus the whole new file and let the RUN_ID markers do the slicing.
audit_since() {   # <offset> <since>
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- bash -c '
        log=$1 off=$2 since=$3
        rot=$(find "$(dirname "$log")" -maxdepth 1 -name "$(basename "$log").*" -newermt "$since" | sort)
        if [ -n "$rot" ]; then cat $rot "$log"; else tail -c +"$((off + 1))" "$log"; fi
    ' _ "$AUDIT" "$1" "$2" 2>/dev/null
}

# Audit lines on stdin -> "arm pass qi time scan_rows scan_bytes return_rows state".
# Credits the FIRST statement after one of this run's markers in the same
# session. Slow queries are logged twice ([slow_query] and [query]); keeping
# both would shift every later record one slot.
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
        continue          # a marker from another run: never a timed statement
    elif sess in armed:
        a, p, q = armed.pop(sess)
        print(a, p, q, *(fld(head, k) for k in ("Time", "ScanRows", "ScanBytes", "ReturnRows", "State")))
'
}

# Records on stdin -> "status ms scan_rows scan_bytes result_rows" for one
# arm/query over the timed passes. A failed pass logs a small Time and
# ScanRows=0 (the fastest possible result), so any non-EOF/OK pass makes it ERR.
arm_result() {   # <arm> <qi>
    local recs
    recs=$(awk -v a="$1" -v q="$2" -v w="$WARMUP" '$1==a && $2>=w && $3==q')
    [[ -z "$recs" ]] && { echo "MISSING - - - -"; return; }
    awk '$8!="EOF" && $8!="OK"' <<<"$recs" | grep -q . && { echo "ERR - - - -"; return; }
    local c v out="ok"
    for c in 4 5 6 7; do
        v=$(awk -v c="$c" '$c ~ /^[0-9]+$/ {print $c}' <<<"$recs" | qb_median)
        out+=" ${v:--}"
    done
    echo "$out"
}

# ── Cluster checks ──────────────────────────────────────────────────────────
ensure_catalog() {   # <arm>
    local ddl
    case "$1" in
        paimon) ddl="CREATE EXTERNAL CATALOG IF NOT EXISTS ${PAIMON_CATALOG} PROPERTIES (
    \"type\" = \"paimon\",
    \"paimon.catalog.type\" = \"filesystem\",
    \"paimon.catalog.warehouse\" = \"s3://${BUCKET_NAME}/cid-warehouse-paimon/\",
    \"aws.s3.region\" = \"${AWS_REGION}\",
    \"aws.s3.use_aws_sdk_default_behavior\" = \"true\"
)" ;;
        iceberg) ddl="CREATE EXTERNAL CATALOG IF NOT EXISTS ${ICEBERG_CATALOG} PROPERTIES (
    \"type\" = \"iceberg\",
    \"iceberg.catalog.type\" = \"glue\",
    \"aws.glue.region\" = \"${AWS_REGION}\",
    \"aws.glue.use_aws_sdk_default_behavior\" = \"true\",
    \"aws.s3.region\" = \"${AWS_REGION}\",
    \"aws.s3.use_aws_sdk_default_behavior\" = \"true\"
)" ;;
        pmn_as_iceberg) ddl="CREATE EXTERNAL CATALOG IF NOT EXISTS ${PMNICE_CATALOG} PROPERTIES (
    \"type\" = \"iceberg\",
    \"iceberg.catalog.type\" = \"hadoop\",
    \"iceberg.catalog.warehouse\" = \"${PMNICE_WAREHOUSE}\",
    \"aws.s3.region\" = \"${AWS_REGION}\",
    \"aws.s3.use_aws_sdk_default_behavior\" = \"true\"
)" ;;
    esac
    qb_sr "$ddl" >/dev/null
    local t; t=$(arm_target "$1")
    qb_sr "SELECT uuid FROM ${t% *}.${t#* }.cid_events LIMIT 1" | grep -q . \
        || qb_die "cannot read a row from ${t% *}.${t#* }.cid_events (arm ${1})"
}

# Rows in <dt> from metadata ($partitions record_count; the Paimon reader cannot
# serve it, so SHOW PARTITIONS, tab-split: field 5 is RowCount). Plus $files for
# the Iceberg-reader arms. A bounded COUNT(*) would be a full cold scan.
table_state() {   # <suite> <dt>
    local arm t c d rows files
    for arm in $ARMS; do
        t=$(arm_target "$arm"); c=${t% *}; d=${t#* }
        rows=$(qb_sr "SELECT record_count FROM ${c}.${d}.\`cid_events\$partitions\` WHERE partition_value.dt='${2}'" | head -1)
        [[ -n "$rows" ]] || rows=$(qb_sr "SHOW PARTITIONS FROM ${c}.${d}.cid_events" \
                                   | awk -F'\t' -v k="dt=${2}" '$1==k {print $5; exit}')
        files=""
        [[ "$arm" != paimon ]] && files=" files=$(qb_sr "SELECT COUNT(*) FROM ${c}.${d}.\`cid_events\$files\`" | head -1)"
        echo "# state suite=${1} arm=${arm} dt=${2} rows=${rows:-?}${files}"
    done
}

ratio() { [[ "$1" =~ ^[0-9]+$ && "$2" =~ ^[0-9]+$ && "$2" -gt 0 ]] \
              && awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", a/b}' || echo "-"; }
col()   { awk -v a="$1" -v c="$2" '$1==a {print $c}' <<<"$3"; }   # <arm> <field> <results>

# ── One suite ───────────────────────────────────────────────────────────────
run_suite() {   # <suite> <rendered sql> <dt>
    local s=$1 sql=$2 dt=$3 arm writers mark raw recs n names qi name res r state
    state=$(table_state "$s" "$dt")
    echo "$state"
    writers=$(qb_writer_state)
    qb_log "${s}: writers ${writers}"
    mark=$(audit_mark) && [[ "$mark" == *" "* ]] || qb_die "cannot stat ${AUDIT} in ${FE_POD}"
    for arm in $ARMS; do
        qb_log "${s}: arm ${arm}..."
        build_script "$arm" "$s" "$sql" \
          | kubectl exec -i -n "$NAMESPACE" "$FE_POD" -- \
              mysql -h127.0.0.1 -P9030 -uroot -N -B --force >/dev/null 2>&1
    done
    # The arms of a suite are ratioed against each other, so a writer change
    # between them (~89x on snapshot) makes the suite meaningless.
    [[ "$(qb_writer_state)" == "$writers" ]] \
        || qb_die "writer state changed during the ${s} suite (was: ${writers}, now: $(qb_writer_state)); ${s} not recorded"
    sleep 4   # the FE appends audit records asynchronously
    raw=$(audit_since "${mark%% *}" "${mark#* }")
    recs=$(parse_audit <<<"$raw")
    n=$(grep -c . <<<"$recs")
    (( n >= 3 )) || qb_die "parsed only ${n} audit records for ${s} since ${mark#* } (pod clock); no rows written"

    [[ -f "$CSV" ]] || echo "$QB_CSV_HEADER" > "$CSV"
    { echo "# suite=${s} writers: ${writers}"
      echo "# suite=${s} dt=${dt} runs=${RUNS} warmup=${WARMUP} arms=${ARMS} run_id=${RUN_ID}"
      echo "$state"; } >> "$CSV"

    printf '\n  %s suite: StarRocks server-side ms, median of %s runs\n' "$s" "$RUNS"
    printf '  %-22s' QUERY; for arm in $ARMS; do printf ' %15s' "$arm"; done
    printf ' %8s %8s' ice/pai pmn/pai; for arm in $ARMS; do printf ' %20s' "${arm}_rows"; done; echo
    names=$(suite_names "$s"); qi=0
    for name in $names; do
        res=""
        for arm in $ARMS; do
            r=$(arm_result "$arm" "$qi" <<<"$recs")
            res+="${arm} ${r}"$'\n'
            set -- $r
            # Unknown fields stay empty, and an ERR pass blanks its scan fields too.
            echo "${STAMP},starrocks,${arm},${s},${name},${1},${2#-},${5#-},${3#-},${4#-}" >> "$CSV"
        done
        printf '  %-22s' "$name"
        for arm in $ARMS; do
            r=$(col "$arm" 3 "$res"); [[ "$(col "$arm" 2 "$res")" == ok ]] || r=$(col "$arm" 2 "$res")
            printf ' %15s' "$r"
        done
        printf ' %8s %8s' "$(ratio "$(col iceberg 3 "$res")" "$(col paimon 3 "$res")")" \
                          "$(ratio "$(col pmn_as_iceberg 3 "$res")" "$(col paimon 3 "$res")")"
        for arm in $ARMS; do printf ' %20s' "$(col "$arm" 4 "$res")"; done
        echo
        qi=$(( qi + 1 ))
    done
    echo "  ratio = arm ms / paimon ms; >1 is slower than paimon. Rows differ per arm: normalise by S1 / rows=."
}

# ── Main ────────────────────────────────────────────────────────────────────
main() {
    local arm s HOT_SQL="" SNAP_SQL=""
    [[ -n "${ARMS// /}" ]] || qb_die "ARMS is empty"
    for arm in $ARMS; do arm_target "$arm" >/dev/null || qb_die "unknown arm '${arm}' (paimon|pmn_as_iceberg|iceberg)"; done
    mkdir -p "$OUT_DIR"

    # Render everything before touching the cluster, so a template error fails
    # now rather than after the writers are stopped. Hot is re-rendered later.
    for s in $(qb_suites); do
        if [[ "$s" == hot ]]; then qb_resolve_hot; HOT_SQL=$(render_suite hot) || qb_die "hot suite failed to render"
        else qb_resolve_snapshot; SNAP_SQL=$(render_suite snapshot) || qb_die "snapshot suite failed to render"; fi
    done

    for arm in $ARMS; do ensure_catalog "$arm"; done
    qb_log "run ${RUN_ID}: suites=$(qb_suites) arms=${ARMS} runs=${RUNS}+${WARMUP} warmup"

    for s in $(qb_suites); do
        if [[ "$s" == hot ]]; then
            qb_resolve_hot   # the windows end now, not at script start
            HOT_SQL=$(render_suite hot) || qb_die "hot suite failed to render"
            qb_log "hot window: dt=${QB_HOT_DT} [${QB_HOT_FROM_10M}, ${QB_HOT_TO})"
            run_suite hot "$HOT_SQL" "$QB_HOT_DT"
        else
            qb_assert_fe_idle
            qb_stop_writers
            qb_log "snapshot: dt=${QB_SNAPSHOT_DT} S4=[${QB_S4_FROM}, ${QB_S4_TO})"
            run_suite snapshot "$SNAP_SQL" "$QB_SNAPSHOT_DT"
        fi
    done
    echo ""
    echo "Results -> ${CSV}"
}

# Sourced (e.g. to test a function) -> define only.
[[ "${BASH_SOURCE[0]}" == "$0" ]] && main "$@"
