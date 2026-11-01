# bench-lib.sh: the table set and Iceberg v3 handling shared by
# bench-hot-data.sh and bench-server-side.sh. Sourced, never run.
#
# These two scripts are the older ad-hoc StarRocks tools. The maintained CDC
# query benchmark is cdc-pipeline/querybench/ (lib/querybench.sh); the v3 rules
# below mirror it (QB_SR_V3_SKIP, SR_V3_ARMS, qb_sr_jni / SR_PAIMON_JNI).
#
# Table set: cdc-pipeline/lib/common.sh is sourced, which sources .bench-env
# (and .bench-env.local) with the same override rule as querybench, so
#   ./bench-server-side.sh                                     -> v3 set (_v3), the branch default
#   ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX= ./bench-server-side.sh   -> v2 set (flink_*_db)
# CDC_DB_SUFFIX unset means _v3; an explicit empty value means the unsuffixed v2
# set. PAIMON_DB / ICEBERG_DB, if set, still win over the derived names.
#
# Iceberg v3 (ICEBERG_FORMAT_VERSION=3):
#   - Paimon writes 64-bit deletion vectors ('deletion-vectors.bitmap64' =
#     'true'), which StarRocks 4.1.x's native C++ Paimon reader cannot decode.
#     Every Paimon-catalog session therefore starts with
#     SET paimon_force_jni_reader = true; (SR_PAIMON_JNI=0 opts out). The JNI
#     reader is slower: v3 Paimon times are not comparable with v2 native-reader
#     times, which is why every result row carries the flag.
#   - StarRocks 4.1.x refuses any Iceberg v3 scan that touches a deletion vector
#     ("Iceberg V3 Deletion Vectors are not supported"), so the Iceberg arm is
#     skipped. SR_V3_ARMS=1 runs it anyway: a probe records the error, and its
#     queries come out as FAIL/ERR.

_BL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_BL_COMMON="${_BL_DIR}/../../lib/common.sh"
_bl_pdb="${PAIMON_DB-}"
_bl_idb="${ICEBERG_DB-}"
if [[ -f "$_BL_COMMON" ]]; then
    # .bench-env expands ${BUCKET_NAME} / ${AWS_ACCOUNT_ID}, which are unset
    # without a .bench-env.local; these scripts need neither.
    set +u
    # shellcheck disable=SC1090
    source "$_BL_COMMON"
    set -u
else
    # common.sh's defaults, if the script was copied out of the repository.
    ICEBERG_FORMAT_VERSION="${ICEBERG_FORMAT_VERSION:-3}"
    CDC_DB_SUFFIX="${CDC_DB_SUFFIX-_v3}"
fi
[[ "$ICEBERG_FORMAT_VERSION" =~ ^[23]$ ]] || { echo "ICEBERG_FORMAT_VERSION must be 2 or 3, got '${ICEBERG_FORMAT_VERSION}'" >&2; exit 1; }
PAIMON_DB="${_bl_pdb:-flink_paimon_db${CDC_DB_SUFFIX}}"
ICEBERG_DB="${_bl_idb:-flink_iceberg_db${CDC_DB_SUFFIX}}"
unset _bl_pdb _bl_idb

NAMESPACE="${NAMESPACE:-emr-flink}"
FE_POD="${FE_POD:-kube-starrocks-fe-0}"
PAIMON_CATALOG="${PAIMON_CATALOG:-paimon_catalog}"     # 01-starrocks-paimon-catalog.sql
ICEBERG_CATALOG="${ICEBERG_CATALOG:-iceberg_catalog}"  # 02-starrocks-iceberg-catalog.sql
SR_V3_ARMS="${SR_V3_ARMS:-0}"
SR_PAIMON_JNI="${SR_PAIMON_JNI:-1}"
DRY_RUN="${DRY_RUN:-0}"   # 1: print the session SQL each arm would send, touch nothing

# 0 if the Paimon sessions force the JNI reader (v3 and not opted out).
bl_jni() { [[ "$ICEBERG_FORMAT_VERSION" == 3 && "$SR_PAIMON_JNI" == 1 ]]; }
PAIMON_JNI=$(bl_jni && echo true || echo false)

# The Iceberg arm: run (v2) | skipped (v3) | forced (v3 with SR_V3_ARMS=1).
if [[ "$ICEBERG_FORMAT_VERSION" != 3 ]]; then ICEBERG_ARM=run
elif [[ "$SR_V3_ARMS" == 1 ]];          then ICEBERG_ARM=forced
else                                         ICEBERG_ARM=skipped; fi

# The session SETs a Paimon-catalog session needs before its reads ("" if none).
bl_paimon_sets() { bl_jni && echo "SET paimon_force_jni_reader = true;"; return 0; }

# The statements that open a session on <paimon|iceberg> (one line).
bl_prelude() {   # <paimon|iceberg> <catalog> <db>
    local sets=""
    [[ "$1" == paimon ]] && sets="$(bl_paimon_sets)"
    echo "${sets:+${sets} }SET CATALOG ${2}; USE ${3};"
}

# Labels for every result: one line for logs, extra columns for CSV rows.
RUN_LABEL="iceberg_format_version=${ICEBERG_FORMAT_VERSION} cdc_db_suffix=${CDC_DB_SUFFIX} paimon_force_jni_reader=${PAIMON_JNI} iceberg_arm=${ICEBERG_ARM} sr_v3_arms=${SR_V3_ARMS}"
CSV_META_COLS="iceberg_format_version,cdc_db_suffix,paimon_force_jni_reader,iceberg_arm"
CSV_META="${ICEBERG_FORMAT_VERSION},${CDC_DB_SUFFIX},${PAIMON_JNI},${ICEBERG_ARM}"

bl_sr()     { kubectl exec -n "$NAMESPACE" "$FE_POD" -- mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1" 2>/dev/null; }
# Same, with the error text on stdout (for recording why a statement failed).
bl_sr_err() { kubectl exec -n "$NAMESPACE" "$FE_POD" -- mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1" 2>&1; }
# One CSV-safe line out of a mysql/kubectl error.
bl_oneline() { tr '\n,' ' ;' | sed -e 's/  */ /g' -e 's/ $//' | cut -c1-400; }

# Banner lines about the format version, the JNI reader and the Iceberg arm.
bl_print_arms() {
    echo "  Format   : Iceberg v${ICEBERG_FORMAT_VERSION} set (CDC_DB_SUFFIX='${CDC_DB_SUFFIX}'), paimon_force_jni_reader=${PAIMON_JNI}"
    case "$ICEBERG_ARM" in
        skipped) echo "  Iceberg  : SKIPPED on v3: StarRocks 4.1.x refuses Iceberg v3 scans with deletion vectors (SR_V3_ARMS=1 runs it to record the error)" ;;
        forced)  echo "  Iceberg  : v3, SR_V3_ARMS=1: runs only to record the expected deletion-vector error" ;;
    esac
    if [[ "$ICEBERG_FORMAT_VERSION" == 3 && "$PAIMON_JNI" == false ]]; then
        echo "  WARNING  : SR_PAIMON_JNI=0 on v3: the native Paimon reader cannot decode the 64-bit deletion vectors (crash or wrong rows)"
    fi
    return 0
}

# On a JNI run the FE must know the variable, or every Paimon read of the v3
# set would silently go through the native reader.
bl_check_jni() {
    bl_jni || return 0
    local r
    r=$(bl_sr "SHOW VARIABLES LIKE 'paimon_force_jni_reader';")
    [[ "$r" == paimon_force_jni_reader* ]] || {
        echo "ERROR: Iceberg v3 set: this StarRocks ($(bl_sr 'SELECT current_version();' | head -1)) has no session variable paimon_force_jni_reader; its native Paimon reader cannot decode the 64-bit deletion vectors of the v3 Paimon tables (SR_PAIMON_JNI=0 to run it anyway)" >&2
        exit 1; }
}

# SR_V3_ARMS=1: read one row of each Iceberg table and record why it fails.
# Sets ICEBERG_PROBE_ERR ("" if every table was readable).
ICEBERG_PROBE_ERR=""
bl_probe_iceberg() {
    [[ "$ICEBERG_ARM" == forced ]] || return 0
    local t r
    for t in customers products orders order_items; do
        if ! r=$(bl_sr_err "SELECT 1 FROM ${ICEBERG_CATALOG}.${ICEBERG_DB}.${t} LIMIT 1;"); then
            r=$(bl_oneline <<<"$r")
            echo "  Iceberg v3, SR_V3_ARMS=1: ${ICEBERG_CATALOG}.${ICEBERG_DB}.${t} not readable (${r}); the arm runs anyway and records the error"
            ICEBERG_PROBE_ERR+="${t}: ${r}; "
        fi
    done
    return 0
}
