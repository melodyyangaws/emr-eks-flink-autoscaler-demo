#!/usr/bin/env bash
# Shared by the CDC query benchmark's drivers (querybench/starrocks.sh,
# duckdb-warm.sh, hot-interleave.sh, pin.sh, run.sh, qb-watch.sh). Sourced,
# never run. Adapted from throughput-benchmark/lib/querybench.sh: one copy of
# everything both engines must agree on (the query files and how they render,
# the windows, the pins, the timeout, the median, the writer stop and the
# results format).
#
# What differs from throughput: four tables instead of one (customers,
# products, orders, order_items), each with its OWN pinned version, and the
# partition column names differ between the formats.
#
# Placeholders in querybench/hot/*.sql and querybench/snapshot/*.sql
# (qb_render; any other ${...} left after rendering stops the run):
#   ${CUSTOMERS} ${PRODUCTS} ${ORDERS} ${ORDER_ITEMS}
#       the table, per engine and arm, at its pinned version:
#         StarRocks  <catalog>.<db>.<table> FOR VERSION AS OF CAST(<id> AS BIGINT)
#         DuckDB     iceberg_scan('<copied metadata.json>')
#       An alias may follow (`FROM ${ORDERS} o`, `${ORDERS} AS o`): StarRocks'
#       grammar takes it after the version clause (relationPrimary #tableAtom:
#       qualifiedName queryPeriod? ... (AS? alias)?, StarRocks.g4 on main,
#       checked 2026-11-02). SR_PIN_FORM=subquery renders
#       (SELECT * FROM <table> FOR VERSION AS OF ...) instead, adding
#       `AS <table>` where no alias follows; pin.sh switches to it by itself if
#       the clause form does not parse on the running FE.
#   ${ORDERS_PART} ${ORDER_ITEMS_PART}
#       the partition column: Paimon order_date_str / created_date_str (also
#       for pmn_as_iceberg and DuckDB's paimon arm: the export keeps Paimon's
#       names), Iceberg order_dt / created_dt (flink_iceberg_db, confirmed from
#       the tables' partition-specs 2026-11-02)
#   ${BYTES_FN}        LENGTH (StarRocks) / STRLEN (DuckDB): both count bytes
#   ${HOT_DT} ${HOT_FROM} ${HOT_TO}            hot window (UTC, 'YYYY-MM-DD HH:MM:SS')
#   ${SNAPSHOT_DT} ${S_FROM} ${S_TO}           snapshot partition and the slice inside it
#                                              (S_FROM/S_TO: bounds on orders.order_date)
#   ${LOOKUP_ORDER_ID} ${LOOKUP_ORDER_DT}      hot H8's key: an order updated in the
#                                              window, picked once per run (qb_resolve_lookup)
# Every value is a literal resolved once per run (per pass for the hot tables),
# never a clock function in the SQL: the StarRocks FE session timezone is +08:00
# and the data is UTC.
#
# File name = query name, lexical order = run order. A `-- bench: <flag>` line
# sets a per-query flag: allow-zero-rows, leading-col-is-label (DuckDB's
# ZERO_SCAN guard), io-tasks=1 (StarRocks: one connector io task per scan).

[[ -n "${_CDC_QUERYBENCH_LIB:-}" ]] && return 0
_CDC_QUERYBENCH_LIB=1

# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
QB_DIR="${CDC_DIR}/querybench"

FE_POD="${FE_POD:-kube-starrocks-fe-0}"
: "${BUCKET_NAME:?BUCKET_NAME is not set (.bench-env / .bench-env.local)}"
QB_TABLES="customers products orders order_items"
PAIMON_WAREHOUSE="${PAIMON_WAREHOUSE:-s3://${BUCKET_NAME}/paimon-warehouse}"; PAIMON_WAREHOUSE="${PAIMON_WAREHOUSE%/}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-s3://${BUCKET_NAME}/iceberg-warehouse}"; ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE%/}"
# flink_{paimon,iceberg}_db<CDC_DB_SUFFIX>: set by common.sh.
PAIMON_DB="${PAIMON_DB:-flink_paimon_db${CDC_DB_SUFFIX:-}}"
ICEBERG_DB="${ICEBERG_DB:-flink_iceberg_db${CDC_DB_SUFFIX:-}}"
# Paimon's Iceberg export ('metadata.iceberg.storage' = 'hive-catalog' in
# 03-paimon-sinks.sql): <warehouse>/iceberg/<db>/<table>/metadata/vN.metadata.json
# plus version-hint.text, the hadoop-catalog layout. N is Paimon's own snapshot
# id (LATEST=118 <-> v118, current-snapshot-id 118 on orders, 2026-11-02). Glue
# also points at it (metadata_location on the Paimon table), but the Glue entry
# carries PaimonStorageHandler, which StarRocks' Glue Iceberg catalog cannot load.
PMNICE_ROOT="${PMNICE_ROOT:-${PAIMON_WAREHOUSE}/iceberg/${PAIMON_DB}}"
FLINK_JOBS="${FLINK_JOBS:-flink-cdc-paimon flink-cdc-iceberg}"
GENERATOR_RESOURCES="${GENERATOR_RESOURCES:-statefulset/mysql-upsert-loadgen deployment/mysql-data-generator}"
# StarRocks catalogs of this harness: own names, so they never collide with the
# throughput benchmark's (cid_*) or the CDC guide's (paimon_catalog, iceberg_catalog),
# and suffixed with CDC_DB_SUFFIX, so the v2 and v3 table sets get their own
# (cdc_paimon_catalog_v3, ...; an existing catalog is never re-created).
SR_PAIMON_CATALOG="${SR_PAIMON_CATALOG:-cdc_paimon_catalog${CDC_DB_SUFFIX:-}}"
SR_ICEBERG_CATALOG="${SR_ICEBERG_CATALOG:-cdc_iceberg_catalog${CDC_DB_SUFFIX:-}}"
SR_PMNICE_CATALOG="${SR_PMNICE_CATALOG:-cdc_pmn_as_ice${CDC_DB_SUFFIX:-}}"
# Copies of the pinned metadata.json files (one key per run and table).
QB_PIN_PREFIX="${QB_PIN_PREFIX:-querybench-pins/cdc-session${CDC_SET_TAG:-}${SESSION_SUFFIX}}"
SUITE="${SUITE:-all}"
RUNS="${RUNS:-3}"
WARMUP="${WARMUP:-1}"
OUT_DIR="${OUT_DIR:-${CDC_DIR}/bench-results}"
STAMP="${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"

qb_log() { echo "  [querybench] $*" >&2; }
qb_die() { echo "  [querybench] ERROR: $*" >&2; exit 1; }

case "$SUITE" in all|hot|snapshot) ;; *) qb_die "SUITE must be all|hot|snapshot, got '${SUITE}'" ;; esac
qb_suites() { case "$SUITE" in all) echo "hot snapshot" ;; *) echo "$SUITE" ;; esac; }
qb_upper() { tr '[:lower:]' '[:upper:]' <<<"$1"; }

# The StarRocks FE's MySQL port; root has no password in this deployment.
qb_sr() {
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- \
        mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1" 2>/dev/null
}

# ── Dates (BSD date first, GNU second: run.sh runs on macOS, the rest in a pod) ──
qb_utc_minus_sec() {   # "<YYYY-MM-DD HH:MM:SS>" <seconds>
    date -u -j -v-"$2"S -f '%Y-%m-%d %H:%M:%S' "$1" '+%Y-%m-%d %H:%M:%S' 2>/dev/null \
      || date -u -d "@$(( $(date -u -d "$1" +%s) - $2 ))" '+%Y-%m-%d %H:%M:%S'
}

# ── Arms, tables, partition columns ─────────────────────────────────────────
# StarRocks arms: paimon (Paimon reader), pmn_as_iceberg (Iceberg reader on
# Paimon's export), iceberg (Iceberg reader on the Iceberg table).
# DuckDB arms: paimon (iceberg_scan on Paimon's export), iceberg.
SR_ARMS_ALL="paimon pmn_as_iceberg iceberg"
DUCK_ARMS_ALL="paimon iceberg"
# Iceberg v3 (ICEBERG_FORMAT_VERSION=3): StarRocks 4.1.x (4.1.7, branch-4.2
# and main as of 2026-11-02) fails ANY scan of an Iceberg v3 table that has a
# deletion vector ("Iceberg V3 Deletion Vectors are not supported",
# IcebergConnectorScanRangeSource.buildScanRanges; PR #76228 open), and both
# StarRocks Iceberg-reader arms read v3 metadata with DVs. They are skipped:
# SR_ARMS_ALL is then the native Paimon arm alone, whose reader does not go
# through the Iceberg metadata. SR_V3_ARMS=1 keeps them, to record the failure
# (their catalog check and pin check then warn instead of stopping the run, and
# their queries come out as ERROR). DuckDB keeps both arms.
QB_SR_V3_SKIP="pmn_as_iceberg iceberg"
QB_SR_SKIPPED=""
if [[ "${ICEBERG_FORMAT_VERSION:-3}" == 3 && "${SR_V3_ARMS:-0}" != 1 ]]; then
    SR_ARMS_ALL="paimon"; QB_SR_SKIPPED="$QB_SR_V3_SKIP"
fi
# 0 if <arm> is a StarRocks Iceberg-reader arm run on v3 only to record its failure.
qb_sr_v3_forced() { [[ "${ICEBERG_FORMAT_VERSION:-3}" == 3 && " ${QB_SR_V3_SKIP} " == *" $1 "* ]]; }
# <arms> without the skipped ones (logged once per call when one is dropped).
qb_sr_arms_filter() {   # <arms...>
    local a out="" drop=""
    for a in $*; do
        if [[ " ${QB_SR_SKIPPED} " == *" ${a} "* ]]; then drop+=" ${a}"; else out+=" ${a}"; fi
    done
    [[ -z "$drop" ]] || qb_log "Iceberg v3: StarRocks arm(s)${drop} skipped (deletion vectors unsupported on 4.1.x; SR_V3_ARMS=1 runs them)"
    echo "${out# }"
}
# Iceberg v3 set: Paimon writes 64-bit deletion vectors
# ('deletion-vectors.bitmap64' = 'true'), which StarRocks 4.1.x's native C++
# Paimon reader does not decode (be/src/exec/paimon/paimon_delete_file_builder.cpp
# handles 32-bit DVs; its magic check is only a DCHECK: a crash or wrong rows).
# Its JNI reader (Paimon's Java reader) reads both, so on v3 every StarRocks
# session that reads the paimon arm first runs SET paimon_force_jni_reader =
# true (SR_PAIMON_JNI=0 opts out). The JNI reader is slower: v3 "SR paimon"
# times are NOT the native-reader numbers of a v2 run (the pins and the summary
# say so). Unset on v2.
qb_sr_jni() { [[ "${ICEBERG_FORMAT_VERSION:-3}" == 3 && "${SR_PAIMON_JNI:-1}" == 1 ]]; }
qb_sr_sets() {   # <arm> -> the session SETs that arm needs before its reads ("" if none)
    [[ "$1" == paimon ]] && qb_sr_jni && echo "SET paimon_force_jni_reader = true; "
    return 0
}
# The table set and engine settings of this run, one key=value per line, for
# every .pins file (and through them the summary's header).
qb_run_meta() {
    echo "iceberg_format_version=${ICEBERG_FORMAT_VERSION:-3}"
    echo "cdc_db_suffix=${CDC_DB_SUFFIX:-} paimon_db=${PAIMON_DB} iceberg_db=${ICEBERG_DB}"
    echo "sr_catalogs=${SR_PAIMON_CATALOG},${SR_PMNICE_CATALOG},${SR_ICEBERG_CATALOG} sr_arms=${SR_ARMS_ALL// /,} sr_skipped_arms=${QB_SR_SKIPPED// /,} sr_v3_arms=${SR_V3_ARMS:-0}"
    echo "sr_image_tag=${SR_IMAGE_TAG:-} sr_chart_version=${SR_CHART_VERSION:-} sr_paimon_force_jni_reader=$(qb_sr_jni && echo true || echo false)"
    local sp="${SKIP_PUFFIN_VERIFICATION_ARMS-$([[ "${ICEBERG_FORMAT_VERSION:-3}" == 3 ]] && echo paimon)}"
    echo "duckdb_image=${DUCKDB_IMAGE:-pip duckdb==${DUCKDB_VERSION:-2.0.0}} duckdb_skip_puffin_verification_arms=${sp// /,}"
}

# The partition column of <table> in the files <arm> reads. Paimon names for
# every arm that reads Paimon's files (the export keeps them); overridable.
qb_part_col() {   # <arm> <table>
    case "$1:$2" in
        iceberg:orders)       echo "${ICEBERG_ORDERS_PART:-order_dt}" ;;
        iceberg:order_items)  echo "${ICEBERG_ORDER_ITEMS_PART:-created_dt}" ;;
        *:orders)             echo "${PAIMON_ORDERS_PART:-order_date_str}" ;;
        *:order_items)        echo "${PAIMON_ORDER_ITEMS_PART:-created_date_str}" ;;
        *)                    return 1 ;;
    esac
}

qb_sr_target() {   # <arm> -> "catalog db pin-key"
    case "$1" in
        paimon)         echo "$SR_PAIMON_CATALOG $PAIMON_DB PAIMON" ;;
        pmn_as_iceberg) echo "$SR_PMNICE_CATALOG $PAIMON_DB PMNICE" ;;
        iceberg)        echo "$SR_ICEBERG_CATALOG $ICEBERG_DB ICEBERG" ;;
        *) return 1 ;;
    esac
}

# The table expression for one engine, arm and table, at its pin. Pins are
# the QB_PIN_* variables pin.sh prints (eval'd by the caller):
#   QB_PIN_SNAP_<PAIMON|PMNICE|ICEBERG>_<TABLE>   StarRocks: snapshot id
#   QB_PIN_META_<PAIMON|ICEBERG>_<TABLE>          DuckDB: the copied metadata.json
# StarRocks without a pin reads the current version (a native Paimon arm whose
# FOR VERSION AS OF failed is unset by hot-interleave.sh / pin.sh and logged).
# DuckDB without a pin is an error: the live metadata.json is deleted within
# minutes by the writers, mid-scan (memory note: pinned metadata on every arm).
qb_table_expr() {   # <starrocks|duckdb> <arm> <table>
    local e="$1" arm="$2" t="$3" T v tgt
    T=$(qb_upper "$t")
    case "$e" in
        starrocks)
            tgt=$(qb_sr_target "$arm") || return 1
            set -- $tgt
            v="QB_PIN_SNAP_${3}_${T}"
            # CAST: StarRocks types a small literal (the export's ids are Paimon
            # snapshot numbers, e.g. 118) as TINYINT and rejects it as a version.
            if [[ -z "${!v:-}" ]]; then echo "${1}.${2}.${t}"
            elif [[ "${SR_PIN_FORM:-clause}" == subquery ]]; then echo "(SELECT * FROM ${1}.${2}.${t} FOR VERSION AS OF CAST(${!v} AS BIGINT))"
            else echo "${1}.${2}.${t} FOR VERSION AS OF CAST(${!v} AS BIGINT)"; fi ;;
        duckdb)
            case "$arm" in paimon) v="QB_PIN_META_PAIMON_${T}" ;; iceberg) v="QB_PIN_META_ICEBERG_${T}" ;; *) return 1 ;; esac
            [[ -n "${!v:-}" ]] || { qb_log "no ${v}: DuckDB reads only pinned metadata (run pin.sh first)"; return 1; }
            echo "iceberg_scan('${!v}')" ;;
        *) return 1 ;;
    esac
}

# ── Pins ────────────────────────────────────────────────────────────────────
# Pin every table of both formats (QB_PIN_FORMATS: "iceberg paimon", or one) at its CURRENT version: copy each current
# metadata.json to a key this run owns (s3://$BUCKET_NAME/$QB_PIN_PREFIX/<tag>/),
# and read its current snapshot id. Prints shell exports (QB_PIN_*, above) plus
# QB_PIN_COMMIT_MS_<FMT>_<TABLE> (the pinned snapshot's commit time, for the
# record and the hot test's committed-past-the-window check). Read-only on the
# tables; needs aws CLI and s3:PutObject on the bucket (the runner's role).
# With QB_TABLE_STATE_OUT set, appends one TSV line per format and table: the
# pinned snapshot's id, commit time and summary counts (records, data files,
# delete files, equality / position deletes, bytes; for Paimon native: its
# snapshot's totalRecordCount and commitKind), the state the times depend on.
qb_pin_tables() {   # <tag>
    ICEBERG_FORMAT_VERSION="${ICEBERG_FORMAT_VERSION:-3}" QB_PIN_FORMATS="${QB_PIN_FORMATS:-iceberg paimon}" QB_TABLES="$QB_TABLES" PMNICE_ROOT="$PMNICE_ROOT" QB_PIN_PREFIX="$QB_PIN_PREFIX" PAIMON_WAREHOUSE="$PAIMON_WAREHOUSE" \
    PAIMON_DB="$PAIMON_DB" ICEBERG_DB="$ICEBERG_DB" python3 - "$1" <<'EOF'
import json, os, subprocess, sys, time
E = os.environ
tag, bucket, region = sys.argv[1], E["BUCKET_NAME"], E["AWS_REGION"]
pmn_root, prefix = E["PMNICE_ROOT"].rstrip("/"), E["QB_PIN_PREFIX"].strip("/")
def aws(*a):
    return subprocess.run(["aws", "--region", region, *a], check=True, capture_output=True, text=True).stdout
def pin(fmt, t, loc):
    key = "%s/%s/%s/%s/%s" % (prefix, tag, fmt, t, loc.rsplit("/", 1)[1])
    aws("s3", "cp", "--only-show-errors", loc, "s3://%s/%s" % (bucket, key))
    meta = json.loads(aws("s3", "cp", "s3://%s/%s" % (bucket, key), "-"))
    sid = meta.get("current-snapshot-id")
    snap = next((s for s in meta.get("snapshots", []) if s.get("snapshot-id") == sid), {})
    ts, sm = snap.get("timestamp-ms", ""), snap.get("summary", {})
    fv = meta.get("format-version")
    state.append([fmt, t, sid, ts] + [sm.get(k, "") for k in STATE_KEYS] + ["format-version=%s" % fv])
    if str(fv) != E.get("ICEBERG_FORMAT_VERSION", "3"):
        print("  [querybench] WARNING: %s %s metadata is format-version %s, not ICEBERG_FORMAT_VERSION=%s (%s)"
              % (fmt, t, fv, E.get("ICEBERG_FORMAT_VERSION", "3"), loc), file=sys.stderr)
    out.append("QB_PIN_FORMAT_VERSION_%s_%s=%s" % (fmt.upper(), t.upper(), fv))
    return "s3://%s/%s" % (bucket, key), sid, ts
STATE_KEYS = ("total-records", "total-data-files", "total-delete-files", "total-equality-deletes",
              "total-position-deletes", "total-files-size")
out, state = [], []
FORMATS = (E.get("QB_PIN_FORMATS") or "iceberg paimon").split()
for t in E["QB_TABLES"].split():
    T = t.upper()
    for attempt in range(3):   # a writer may delete the file between the lookup and the copy
        try:
          if "iceberg" in FORMATS:
            loc = aws("glue", "get-table", "--database-name", E["ICEBERG_DB"], "--name", t,
                      "--query", "Table.Parameters.metadata_location", "--output", "text").strip()
            if not loc or loc == "None":
                sys.exit("no metadata_location on %s.%s" % (E["ICEBERG_DB"], t))
            m, sid, ts = pin("iceberg", t, loc)
            out += ["QB_PIN_META_ICEBERG_%s=%s" % (T, m), "QB_PIN_SNAP_ICEBERG_%s=%s" % (T, sid),
                    "QB_PIN_COMMIT_MS_ICEBERG_%s=%s" % (T, ts)]
          if "paimon" in FORMATS:
            v = aws("s3", "cp", "%s/%s/metadata/version-hint.text" % (pmn_root, t), "-").strip()
            if not v.isdigit():
                sys.exit("no version-hint.text under %s/%s/metadata (metadata.iceberg.storage unset?)" % (pmn_root, t))
            m, sid, ts = pin("paimon", t, "%s/%s/metadata/v%s.metadata.json" % (pmn_root, t, v))
            # The export's snapshot id IS Paimon's snapshot id (vN <-> snapshot-N),
            # which the native Paimon arm pins to.
            out += ["QB_PIN_META_PAIMON_%s=%s" % (T, m), "QB_PIN_SNAP_PMNICE_%s=%s" % (T, sid),
                    "QB_PIN_SNAP_PAIMON_%s=%s" % (T, v), "QB_PIN_COMMIT_MS_PAIMON_%s=%s" % (T, ts)]
            try:   # the native snapshot the paimon arm pins to (record only)
                ps = json.loads(aws("s3", "cp", "%s/%s.db/%s/snapshot/snapshot-%s" % (
                    E["PAIMON_WAREHOUSE"].rstrip("/"), E["PAIMON_DB"], t, v), "-"))
                state.append(["paimon_native", t, v, ps.get("timeMillis", ""), ps.get("totalRecordCount", ""),
                              "", "", "", "", "", ps.get("commitKind", "")])
            except subprocess.CalledProcessError:
                state.append(["paimon_native", t, v, "", "", "", "", "", "", "", "snapshot file gone"])
          break
        except subprocess.CalledProcessError as e:
            if attempt == 2:
                sys.exit("pin %s failed: %s" % (t, (e.stderr or "").strip()[:300]))
            time.sleep(5)
if E.get("QB_TABLE_STATE_OUT"):
    new = not os.path.exists(E["QB_TABLE_STATE_OUT"])
    with open(E["QB_TABLE_STATE_OUT"], "a") as f:
        if new:
            f.write("\t".join(["pin_tag", "format", "table", "snapshot_id", "commit_ms"] + list(STATE_KEYS) + ["note"]) + "\n")
        for r in state:
            f.write("\t".join([tag] + [str(x) for x in r]) + "\n")
print("export " + " ".join(out))
EOF
}

# The newest commit time (ms) of <table> in <iceberg|paimon> (read-only: the
# Iceberg metadata via Glue, Paimon's latest snapshot file).
qb_commit_ms() {   # <iceberg|paimon> <table>
    if [[ "$1" == iceberg ]]; then
        local loc
        loc=$(aws glue get-table --region "$AWS_REGION" --database-name "$ICEBERG_DB" --name "$2" \
                --query 'Table.Parameters.metadata_location' --output text 2>/dev/null) || return 1
        aws s3 cp --region "$AWS_REGION" "$loc" - 2>/dev/null | python3 -c '
import json, sys
m = json.load(sys.stdin); sid = m.get("current-snapshot-id")
print(next((s["timestamp-ms"] for s in m.get("snapshots", []) if s.get("snapshot-id") == sid), ""))'
    else
        local root="${PAIMON_WAREHOUSE}/${PAIMON_DB}.db/$2/snapshot" n
        n=$(aws s3 cp --region "$AWS_REGION" "${root}/LATEST" - 2>/dev/null | tr -dc 0-9) && [[ -n "$n" ]] || return 1
        aws s3 cp --region "$AWS_REGION" "${root}/snapshot-${n}" - 2>/dev/null \
            | python3 -c 'import json, sys; print(json.load(sys.stdin)["timeMillis"])'
    fi
}

# ── Windows ─────────────────────────────────────────────────────────────────
# Hot: HOT_WINDOW_SECONDS (300) ending HOT_LAG_SECONDS (300) before now, on
# whole minutes, inside one day (moved back to end at midnight if it would
# cross it). Each bound can be pinned with the same-named variable. 5 min, not
# throughput's 30 s: the CDC load is ~40k rows/s (mysql-upsert-loadgen), not
# ~3M, so 30 s would be a handful of files. ESTIMATE, not measured: size it
# from the first run's H* row counts.
qb_resolve_hot() {
    local w="${HOT_WINDOW_SECONDS:-300}" lag="${HOT_LAG_SECONDS:-300}" to
    [[ "$w" =~ ^[1-9][0-9]*$ && "$lag" =~ ^[0-9]+$ ]] || qb_die "HOT_WINDOW_SECONDS / HOT_LAG_SECONDS must be whole seconds"
    if [[ -n "${HOT_TO:-}" ]]; then to="$HOT_TO"
    else to=$(qb_utc_minus_sec "$(date -u '+%Y-%m-%d %H:%M:%S')" "$lag"); to="${to:0:16}:00"; fi
    QB_HOT_TO="$to"
    QB_HOT_FROM="${HOT_FROM:-$(qb_utc_minus_sec "$QB_HOT_TO" "$w")}"
    if [[ -z "${HOT_FROM:-}" && "${QB_HOT_FROM%% *}" != "${QB_HOT_TO%% *}" && "${QB_HOT_TO#* }" != "00:00:00" ]]; then
        QB_HOT_TO="${QB_HOT_TO%% *} 00:00:00"; QB_HOT_FROM=$(qb_utc_minus_sec "$QB_HOT_TO" "$w")
        qb_log "hot window would cross midnight: moved to end at ${QB_HOT_TO}"
    fi
    QB_HOT_DT="${HOT_DT:-${QB_HOT_FROM%% *}}"
}

# partition values (YYYY-MM-DD) under an s3:// prefix whose children are <col>=<v>/.
_qb_parts_under() {   # <s3 prefix> <col>
    local p="${1#s3://${BUCKET_NAME}/}"
    aws s3api list-objects-v2 --bucket "$BUCKET_NAME" --region "$AWS_REGION" \
        --prefix "${p%/}/" --delimiter / --query 'CommonPrefixes[].Prefix' \
        --output text 2>/dev/null | tr '\t' '\n' | sed -n "s|.*/${2}=\([0-9-]*\)/*\$|\1|p" | sort -u
}

# Snapshot partition: SNAPSHOT_DT, or the NEWEST date before today (UTC) that
# both fact tables hold in both formats (SNAPSHOT_DT_PICK=oldest: the oldest).
# "Closed" here = not today; the writers are stopped before the test anyway.
qb_resolve_snapshot_dt() {
    if [[ -z "${SNAPSHOT_DT:-}" ]]; then
        local d today all
        d=$(mktemp -d); today=$(date -u +%Y-%m-%d)
        _qb_parts_under "${PAIMON_WAREHOUSE}/${PAIMON_DB}.db/orders"        "$(qb_part_col paimon orders)"       > "$d/1"
        _qb_parts_under "${PAIMON_WAREHOUSE}/${PAIMON_DB}.db/order_items"   "$(qb_part_col paimon order_items)"  > "$d/2"
        _qb_parts_under "${ICEBERG_WAREHOUSE}/${ICEBERG_DB}.db/orders/data"      "$(qb_part_col iceberg orders)"      > "$d/3"
        _qb_parts_under "${ICEBERG_WAREHOUSE}/${ICEBERG_DB}.db/order_items/data" "$(qb_part_col iceberg order_items)" > "$d/4"
        all=$(cat "$d/1" "$d/2" "$d/3" "$d/4" | sort | uniq -c | awk '$1 == 4 {print $2}' | awk -v t="$today" '$1 < t')
        rm -rf "$d"
        [[ -n "$all" ]] || qb_die "no date before ${today} in both formats' orders and order_items partitions; set SNAPSHOT_DT"
        if [[ "${SNAPSHOT_DT_PICK:-newest}" == oldest ]]; then SNAPSHOT_DT=$(head -1 <<<"$all"); else SNAPSHOT_DT=$(tail -1 <<<"$all"); fi
    fi
    [[ "$SNAPSHOT_DT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || qb_die "SNAPSHOT_DT='${SNAPSHOT_DT}' is not YYYY-MM-DD"
    QB_SNAPSHOT_DT="$SNAPSHOT_DT"
}

# Snapshot slice [S_FROM, S_TO): SNAPSHOT_SLICE_MINUTES (10) on whole minutes,
# bounds on orders.order_date (S4, the one sliced query), centred in the span
# where BOTH formats hold rows in SNAPSHOT_DT (MIN/MAX(order_date) per format:
# the data was bulk-loaded by the CDC snapshot phase, so the commit history
# says nothing about event times), SNAPSHOT_SLICE_MARGIN_S (120) clear of each
# end. SNAPSHOT_SLICE_COLUMNS ("orders:order_date") lists the table:column
# spans intersected. 0 = the whole partition. Pin with S_FROM and S_TO.
# Needs the StarRocks FE (MIN/MAX per table, datacache off): run.sh resolves it
# once StarRocks is up and before the caches are emptied. ESTIMATE: 10 min is a
# guess at "short"; size it from the first run's S* row counts.
qb_resolve_snapshot_slice() {
    local ts_re='^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
    local M="${SNAPSHOT_SLICE_MINUTES:-10}" margin="${SNAPSHOT_SLICE_MARGIN_S:-120}" spans="" arm t col tc r
    [[ -n "${QB_SNAPSHOT_DT:-}" ]] || qb_resolve_snapshot_dt
    if [[ -n "${S_FROM:-}${S_TO:-}" ]]; then
        [[ "${S_FROM:-}" =~ $ts_re && "${S_TO:-}" =~ $ts_re ]] || qb_die "S_FROM and S_TO must both be set, as 'YYYY-MM-DD HH:MM:SS'"
        QB_S_FROM="$S_FROM"; QB_S_TO="$S_TO"; QB_S_HOW="pinned"; return 0
    fi
    [[ "$M" =~ ^[0-9]+$ ]] || qb_die "SNAPSHOT_SLICE_MINUTES='${M}' is not a whole number (0 = the whole partition)"
    if (( M == 0 )); then
        QB_S_FROM="${QB_SNAPSHOT_DT} 00:00:00"
        QB_S_TO="$(python3 -c 'import datetime, sys; print(datetime.date.fromisoformat(sys.argv[1]) + datetime.timedelta(days=1))' "$QB_SNAPSHOT_DT") 00:00:00"
        QB_S_HOW="the whole partition (SNAPSHOT_SLICE_MINUTES=0)"; return 0
    fi
    kubectl get pod "$FE_POD" -n "$NAMESPACE" >/dev/null 2>&1 \
        || qb_die "deriving S_FROM/S_TO needs the StarRocks FE (${FE_POD}); set S_FROM and S_TO (e.g. from the StarRocks run's .pins) or SNAPSHOT_SLICE_MINUTES=0"
    # Both formats' spans, each on its own reader. On v3 the StarRocks Iceberg
    # reader cannot read the Iceberg tables (deletion vectors): the span is the
    # native Paimon arm's alone (both CDC jobs replicate the same MySQL rows; the
    # summary's row checks catch a slice one format does not hold).
    # Keyed on the format version, not QB_SR_SKIPPED: with SR_V3_ARMS=1 the
    # Iceberg arm still runs (to record its failure) but must not feed setup.
    local slice_arms="paimon iceberg"
    [[ "${ICEBERG_FORMAT_VERSION:-3}" == 3 ]] && slice_arms="paimon"
    for arm in $slice_arms; do
        for tc in ${SNAPSHOT_SLICE_COLUMNS:-orders:order_date}; do
            t="${tc%%:*}" col="${tc#*:}"
            set -- $(qb_sr_target "$arm")
            r=$(qb_sr "$(qb_sr_sets "$arm")SET enable_scan_datacache = false; SET populate_datacache_mode = 'never'; SET query_timeout = 1800;
                       SELECT MIN(${col}), MAX(${col}) FROM ${1}.${2}.${t} WHERE $(qb_part_col "$arm" "$t") = '${QB_SNAPSHOT_DT}'" | tail -1)
            [[ "$r" == *$'\t'* && "$r" != *NULL* ]] || qb_die "no ${t}.${col} span for ${QB_SNAPSHOT_DT} on the ${arm} arm (got '${r}')"
            spans+="${r}"$'\n'
            qb_log "snapshot span ${arm} ${t}.${col}: ${r/$'\t'/ .. }"
        done
    done
    r=$(python3 - "$QB_SNAPSHOT_DT" "$M" "$margin" "$spans" <<'EOF'
import sys
from datetime import datetime, timezone, timedelta
dt, M, margin = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
p = lambda s: datetime.strptime(s.strip()[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
f = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
spans = [tuple(p(x) for x in l.split("\t")) for l in sys.argv[4].splitlines() if l.strip()]
day0 = datetime.strptime(dt, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
lo = max(max(a for a, _ in spans), day0) + margin
hi = min(min(b for _, b in spans), day0 + 86400) - margin
span = "rows in both formats (SNAPSHOT_SLICE_COLUMNS), less %ds each end: [%s, %s)" % (margin, f(lo), f(hi))
if hi - lo < M * 60:
    print("ERR %s is %.1f min, under the %d-min slice" % (span, max(hi - lo, 0) / 60, M)); sys.exit()
st = max(int((lo + hi) / 2 - M * 30) // 60 * 60, int(lo + 59) // 60 * 60)
print("OK|%s|%s|%d min, centred in the span %s" % (f(st), f(st + M * 60), M, span))
EOF
)
    [[ "$r" == OK\|* ]] || qb_die "snapshot slice: ${r#ERR }; lower SNAPSHOT_SLICE_MINUTES or set S_FROM/S_TO"
    IFS='|' read -r _ QB_S_FROM QB_S_TO QB_S_HOW <<<"$r"
    [[ "$slice_arms" == paimon ]] && QB_S_HOW+="; Paimon span only (Iceberg v3: StarRocks cannot read the Iceberg tables)"
    return 0
}

# ── Queries ─────────────────────────────────────────────────────────────────
qb_query_files() { ls "${QB_DIR}/$1"/*.sql 2>/dev/null || qb_die "no .sql files in ${QB_DIR}/$1"; }
qb_query_name()  { basename "$1" .sql; }
qb_query_flag()  { grep -qE "^-- bench:.*(^|[^A-Za-z0-9_-])$2([^A-Za-z0-9_=-]|\$)" "$1"; }

# Render one file for one engine and arm: every placeholder above, from the
# QB_* windows and the pins. ANY ${...} left is fatal and named: it is not a
# syntax error on either engine (it binds as an identifier or string and
# returns a wrong answer), and it is how a placeholder the SQL authors added
# shows up here first. In Python, so a value's characters are never
# interpreted (bash 5.2's patsub_replacement treats & in a replacement).
qb_render() {   # <file> <starrocks|duckdb> <arm>
    local f="$1" e="$2" arm="$3" t T x args=()
    for t in $QB_TABLES; do
        T=$(qb_upper "$t")
        x=$(qb_table_expr "$e" "$arm" "$t") || { qb_log "no table expression for ${e}/${arm}/${t}"; return 1; }
        args+=("${T}=${x}")
    done
    args+=("ORDERS_PART=$(qb_part_col "$arm" orders)" "ORDER_ITEMS_PART=$(qb_part_col "$arm" order_items)"
           "BYTES_FN=$([[ "$e" == starrocks ]] && echo LENGTH || echo STRLEN)")
    # A window value that is not resolved stays unset, so its placeholder fails.
    [[ -n "${QB_HOT_DT:-}" ]]      && args+=("HOT_DT=${QB_HOT_DT}" "HOT_FROM=${QB_HOT_FROM}" "HOT_TO=${QB_HOT_TO}")
    [[ -n "${QB_SNAPSHOT_DT:-}" ]] && args+=("SNAPSHOT_DT=${QB_SNAPSHOT_DT}")
    [[ -n "${QB_S_FROM:-}" ]]      && args+=("S_FROM=${QB_S_FROM}" "S_TO=${QB_S_TO}")
    [[ -n "${QB_LOOKUP_ORDER_ID:-}" ]] && args+=("LOOKUP_ORDER_ID=${QB_LOOKUP_ORDER_ID}" "LOOKUP_ORDER_DT=${QB_LOOKUP_ORDER_DT}")
    python3 - "$f" "${args[@]}" <<'EOF'
import re, sys
f, kv = sys.argv[1], dict(a.split("=", 1) for a in sys.argv[2:])
TABLES = ("CUSTOMERS", "PRODUCTS", "ORDERS", "ORDER_ITEMS")
NOT_ALIAS = {"WHERE", "GROUP", "ORDER", "LIMIT", "JOIN", "LEFT", "RIGHT", "INNER", "FULL", "CROSS", "ON",
             "USING", "UNION", "HAVING", "WINDOW", "QUALIFY", "SEMI", "ANTI", "OUTER", "LATERAL", "EXCEPT",
             "INTERSECT", "FOR", "TABLESAMPLE"}
def sub(m):
    k, after = m.group(1), m.group(2) or ""
    v = kv.get(k)
    if v is None:
        return m.group(0)
    # A parenthesised table expression (SR_PIN_FORM=subquery) gets the table's
    # name as its alias where the SQL gives none, so `orders.col` still binds.
    if k in TABLES and v.startswith("("):
        w = re.match(r"\s+(AS\s+)?([A-Za-z_][A-Za-z0-9_]*)", after, re.I)
        if not w or (not w.group(1) and w.group(2).upper() in NOT_ALIAS):
            return v + " AS " + k.lower() + after
    return v + after
sql = re.sub(r"\$\{([A-Za-z0-9_]+)\}((?:\s+(?:AS\s+)?[A-Za-z_][A-Za-z0-9_]*)?)", sub, open(f).read(), flags=re.I)
left = sorted(set(re.findall(r"\$\{[^}]*\}", sql)))
if left:
    sys.exit("  [querybench] unresolved in %s: %s (a new placeholder? add it to lib/querybench.sh qb_render)" % (f.rsplit("/", 1)[-1], " ".join(left)))
sys.stdout.write(sql if sql.endswith("\n") else sql + "\n")
EOF
}

# One line, comments stripped: the StarRocks audit log records a statement as
# one Stmt= field, and starrocks.sh pairs each with the marker sent before it.
# A string literal containing "--" would be cut: do not write one.
qb_render_oneline() {   # <file> <engine> <arm>
    local sql
    sql=$(qb_render "$@") || return 1
    sql=$(sed 's/--.*$//' <<<"$sql" | tr '\n' ' ' | tr -s ' ' | sed -e 's/^ *//' -e 's/ *;* *$/;/')
    [[ "$sql" != *";"*";"* ]] || { qb_log "$(basename "$1"): more than one statement"; return 1; }
    printf '%s\n' "$sql"
}

# Per-query cap, the same on both engines, so a TIMEOUT means the same thing.
qb_query_timeout() { [[ "$1" == hot ]] && echo "${HOT_QUERY_TIMEOUT_S:-1800}" || echo "${SNAPSHOT_QUERY_TIMEOUT_S:-3600}"; }

qb_median() {
    sort -n | awk '{a[NR]=$1} END { if (NR==0) exit
        if (NR%2) printf "%d\n", a[(NR+1)/2]; else printf "%d\n", (a[NR/2]+a[NR/2+1])/2 }'
}

# One long-format CSV per engine and run, the same columns for both engines
# (= throughput-benchmark's).
QB_CSV_HEADER="stamp,engine,arm,suite,query,status,median_ms,result_rows,scan_rows,scan_bytes"
qb_csv_init() { mkdir -p "$(dirname "$1")"; [[ -f "$1" ]] || echo "$QB_CSV_HEADER" > "$1"; }

# ── Writers and generator ───────────────────────────────────────────────────
qb_writer_state() {
    local j out=""
    for j in $FLINK_JOBS; do
        out+="${j}=$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" -o jsonpath='{.spec.job.state}' 2>/dev/null || true) "
    done
    echo "${out% }"
}

# Suspend the CDC writers and wait until no job is RUNNING and no TaskManager
# is left, so the last commit has landed. upgradeMode is last-state in both
# FlinkDeployments, so `kubectl patch ... state: running` resumes them from
# their last checkpoint (no new MySQL snapshot). SUSPEND_WRITERS=0 skips it.
# Patched, never re-applied: an apply over an existing EMR FlinkDeployment
# empties /opt/flink/usrlib (jarURI is staged on CREATE only).
qb_stop_writers() {
    [[ "${SUSPEND_WRITERS:-1}" == 0 ]] && { qb_log "SUSPEND_WRITERS=0: writers left as they are"; return 0; }
    local j deadline=$(( $(date +%s) + 600 )) busy
    for j in $FLINK_JOBS; do
        kubectl get flinkdeployment "$j" -n "$NAMESPACE" >/dev/null 2>&1 || continue
        kubectl patch flinkdeployment "$j" -n "$NAMESPACE" --type=merge \
            -p '{"spec":{"job":{"state":"suspended"}}}' >/dev/null || qb_die "cannot suspend ${j}"
    done
    while :; do
        busy=""
        for j in $FLINK_JOBS; do
            [[ "$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)" == RUNNING ]] && busy+=" ${j}"
            kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | grep -q "^${j}-taskmanager-" && busy+=" ${j}-tm"
        done
        [[ -z "$busy" ]] && { qb_log "writers stopped: $(qb_writer_state)"; return 0; }
        (( $(date +%s) < deadline )) || qb_die "writers still running after 600s:${busy}"
        qb_log "waiting for writers to stop:${busy}"
        sleep 15
    done
}

# "<resource>=<ready>/<spec>" for each generator resource that exists.
qb_generator_state() {
    local r out="" s
    for r in $GENERATOR_RESOURCES; do
        s=$(kubectl get "$r" -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>/dev/null) || continue
        out+="${r##*/}=${s:-0/0} "
    done
    echo "${out% }"
}
qb_generator_live() { [[ "$(qb_generator_state)" =~ =[1-9][0-9]*/ ]]; }

# Scale every generator resource that exists to 0 (deploy-data-generator.sh
# scale brings it back). Then, unless WRITER_DRAIN_S=0, wait that long (180 s =
# three 60 s checkpoints) before the writers are suspended, so both commit the
# binlog tail and hold the same rows: a suspend keeps only the last checkpoint.
qb_stop_generator() {
    [[ "${STOP_GENERATOR:-1}" == 0 ]] && { qb_log "STOP_GENERATOR=0: generator left as it is"; return 0; }
    local r was any=0
    for r in $GENERATOR_RESOURCES; do
        was=$(kubectl get "$r" -n "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null) || continue
        [[ "${was:-0}" == 0 ]] && continue
        kubectl scale "$r" -n "$NAMESPACE" --replicas=0 >/dev/null || qb_die "cannot scale ${r} to 0"
        qb_log "generator ${r} scaled ${was} -> 0"; any=1
    done
    (( any )) || { qb_log "generator: nothing running ($(qb_generator_state))"; return 0; }
    local drain="${WRITER_DRAIN_S:-180}"
    if (( drain > 0 )); then qb_log "letting the writers commit the binlog tail: ${drain}s"; sleep "$drain"; fi
}

# ── One engine at a time ────────────────────────────────────────────────────
# A CDC-table query already running on the FE is contention bigger than the
# format effect; an unreadable FE is not an idle one.
qb_assert_fe_idle() {
    local pl busy
    pl=$(qb_sr "SHOW PROCESSLIST") && [[ -n "$pl" ]] || qb_die "cannot read SHOW PROCESSLIST on ${FE_POD}"
    busy=$(awk -F'\t' '$10 != "SHOW PROCESSLIST" && $10 != "" && $10 != "NULL" && $8 != "Sleep" && $5 != "information_schema" {print "    id=" $2 " " $5 " " $7 " " $8 "s " substr($10,1,70)}' <<<"$pl" \
           | grep -E 'cdc_|cid_|flink_' || true)
    [[ -z "$busy" ]] || qb_die "a lakehouse query is already running on ${FE_POD} (KILL QUERY <id>):
${busy}"
}

# Checked on the CLUSTER, so it also catches a run from another laptop, the
# in-cluster runner, or a throughput-benchmark test on the same engines:
#   - no CDC/throughput query on the FE; duckdb-warm not busy;
#   - no throughput-benchmark qb-runner active (it shares StarRocks and duckdb-warm);
#   - locally, no other harness process (<self> = starrocks | duckdb-warm).
# ALLOW_CONCURRENT=1 skips it.
qb_assert_alone() {   # <self: starrocks|duckdb-warm>
    [[ "${ALLOW_CONCURRENT:-0}" == 1 ]] && return 0
    local other busy a d
    case "$1" in
        starrocks)   other=$(pgrep -fl 'querybench/duckdb-warm\.sh run' | grep -v 'bash -c' || true) ;;
        duckdb-warm) other=$(pgrep -fl 'querybench/starrocks\.sh' | grep -v 'bash -c' || true) ;;
        *)           qb_die "qb_assert_alone: unknown harness '$1'" ;;
    esac
    [[ -z "$other" ]] || qb_die "another benchmark is running (ALLOW_CONCURRENT=1 to override):
${other}"
    a=$(kubectl get job qb-runner -n "$NAMESPACE" -o jsonpath='{.status.active}' 2>/dev/null || true)
    [[ "${a:-0}" == 0 ]] || qb_die "throughput-benchmark's qb-runner is running on the same engines (ALLOW_CONCURRENT=1 to override)"
    if kubectl get pod "$FE_POD" -n "$NAMESPACE" >/dev/null 2>&1; then qb_assert_fe_idle; fi
    # Both DuckDB servers: this harness's and the throughput benchmark's (it
    # shares the bucket and, on a small nodepool, the nodes).
    for d in duckdb-warm-cdc duckdb-warm; do
        kubectl get deploy "$d" -n "$NAMESPACE" >/dev/null 2>&1 || continue
        busy=$(kubectl exec -n "$NAMESPACE" "deploy/${d}" -c duckdb -- python3 -c \
            'import json, urllib.request; print(json.load(urllib.request.urlopen("http://localhost:8080/health", timeout=10))["busy"])' 2>/dev/null || echo unreachable)
        case "$busy" in
            False) ;;
            True)  qb_die "${d} is busy with another run (ALLOW_CONCURRENT=1 to override)" ;;
            *)     qb_log "${d}: busy state unreadable (${busy}); assuming idle" ;;
        esac
    done
}

# ── Suite plumbing shared by both drivers ───────────────────────────────────
# Resolve and log the suite's values the same way on both engines. Snapshot
# values come from run.sh (exported SNAPSHOT_DT / S_FROM / S_TO); resolved
# here only when a driver runs by hand.
qb_start_suite() {   # <hot|snapshot>
    if [[ "$1" == hot ]]; then
        qb_resolve_hot
        qb_log "hot: dt=${QB_HOT_DT} [${QB_HOT_FROM}, ${QB_HOT_TO})"
    else
        [[ -n "${SNAPSHOT_DT:-}" && -n "${S_FROM:-}" ]] || qb_stop_writers
        qb_resolve_snapshot_dt
        qb_resolve_snapshot_slice
        qb_log "snapshot: dt=${QB_SNAPSHOT_DT} slice=[${QB_S_FROM}, ${QB_S_TO}) (${QB_S_HOW:-})"
    fi
}

# The arms of a suite are ratioed against each other, so a writer change
# between them makes the suite meaningless: 0 = unchanged, else logged and 1.
qb_writers_unchanged() {   # <writer state at suite start> <suite>
    local now; now=$(qb_writer_state)
    [[ "$now" == "$1" ]] && return 0
    qb_log "writer state changed during the $2 suite (was: $1, now: ${now}); $2 not recorded"
    return 1
}

# Every QB_PIN_* variable set in this shell, one per line (for .pins files).
qb_pins_dump() { local v; for v in $(compgen -v | grep '^QB_PIN_' | sort); do echo "${v}=${!v}"; done; }

# ── StarRocks pin checks and the H8 key (need the FE and the catalogs) ──────
# qb_sr with the error text kept: "<rc><TAB><last output or error line>".
qb_sr_try() {
    local out rc
    out=$(kubectl exec -n "$NAMESPACE" "$FE_POD" -- mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1" 2>&1); rc=$?
    printf '%s\t%s\n' "$rc" "$(tail -1 <<<"$out" | tr '\t' ' ')"
}

# Every StarRocks arm must read every pinned table, alias included (the SQL
# writes `${ORDERS} AS o`). An Iceberg-reader arm whose FE metadata cache does
# not hold the newest snapshot yet ("snapshot ID does not exists") gets
# REFRESH EXTERNAL TABLE and retries for up to 5 min. If the clause form does
# not parse, SR_PIN_FORM=subquery is used. If the native Paimon reader cannot
# read its version, that table is UNPINNED on the paimon arm and the pins say
# so (throughput-benchmark does the same). Prints the changed exports, if any.
qb_sr_check_pins() {
    local arm t T tgt r try v changes=""
    for arm in ${ARMS-$SR_ARMS_ALL}; do
        tgt=$(qb_sr_target "$arm") || qb_die "unknown arm ${arm}"
        for t in $QB_TABLES; do
            T=$(qb_upper "$t"); set -- $tgt; v="QB_PIN_SNAP_${3}_${T}"
            [[ -n "${!v:-}" ]] || continue
            for (( try = 1; try <= 10; try++ )); do
                r=$(qb_sr_try "$(qb_sr_sets "$arm")SET new_planner_optimize_timeout = 300000; SELECT 1 FROM $(qb_table_expr starrocks "$arm" "$t") AS q LIMIT 1")
                [[ "$r" == 0$'\t'* ]] && break
                if [[ "$r" == *[Ss]yntax* && "${SR_PIN_FORM:-clause}" == clause ]]; then
                    qb_log "pin form 'FOR VERSION AS OF ... AS q' does not parse on this FE (${r#*$'\t'}); using SR_PIN_FORM=subquery"
                    SR_PIN_FORM=subquery; changes+=" SR_PIN_FORM=subquery"; continue
                fi
                [[ "$arm" == paimon ]] && (( try >= 2 )) && break
                # v3, SR_V3_ARMS=1: the expected deletion-vector refusal is the result.
                qb_sr_v3_forced "$arm" && [[ "$r" == *[Dd]eletion* ]] && break
                [[ "$arm" == paimon ]] || qb_sr "REFRESH EXTERNAL TABLE ${1}.${2}.${t}" >/dev/null || true
                qb_log "pinned ${arm}.${t}@${!v}: ${r#*$'\t'} (try ${try}/10), retrying in 30s"
                sleep 30
            done
            [[ "$r" == 0$'\t'* ]] && continue
            if [[ "$arm" == paimon ]]; then
                qb_log "WARNING: StarRocks native Paimon cannot read ${t} at snapshot ${!v} (${r#*$'\t'}); that table runs UNPINNED on the paimon arm"
                printf -v "$v" '%s' ""; changes+=" ${v}="
            elif qb_sr_v3_forced "$arm"; then
                qb_log "Iceberg v3, SR_V3_ARMS=1: StarRocks ${arm} cannot read ${t} at version ${!v}: ${r#*$'\t'} (expected; its queries will record the error)"
            else
                qb_die "StarRocks ${arm} cannot read ${t} at version ${!v}: ${r#*$'\t'}"
            fi
        done
    done
    [[ -z "$changes" ]] || echo "export${changes}"
}

# H8's key: an order UPDATED in the hot window (updated_at in [HOT_FROM, HOT_TO),
# created before it), picked on the pinned Iceberg arm and confirmed on the
# pinned native Paimon arm; the next candidates are tried (OFFSET 1..4). Sets
# QB_LOOKUP_ORDER_ID / QB_LOOKUP_ORDER_DT (LOOKUP_ORDER_ID / LOOKUP_ORDER_DT
# pin them) and QB_LOOKUP_NOTE (its partition: the hot day or older). The
# selection reads updated_at over the whole table (no partition filter, like
# H4): untimed, datacache off.
qb_resolve_lookup() {
    if [[ -n "${LOOKUP_ORDER_ID:-}" ]]; then
        [[ "$LOOKUP_ORDER_ID" =~ ^[0-9]+$ && "${LOOKUP_ORDER_DT:-}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] \
            || qb_die "LOOKUP_ORDER_ID must be an integer and LOOKUP_ORDER_DT a YYYY-MM-DD"
        QB_LOOKUP_ORDER_ID="$LOOKUP_ORDER_ID"; QB_LOOKUP_ORDER_DT="$LOOKUP_ORDER_DT"; QB_LOOKUP_NOTE="pinned by hand"
    elif [[ "${ICEBERG_FORMAT_VERSION:-3}" == 3 ]]; then
        # v3 (also with SR_V3_ARMS=1): StarRocks cannot read the Iceberg tables, so the key is picked on
        # the pinned native Paimon arm and not confirmed on Iceberg (DuckDB's
        # iceberg arm returns what it finds; the summary compares the rows).
        local r
        r=$(qb_sr "$(qb_sr_sets paimon)SET enable_scan_datacache = false; SET populate_datacache_mode = 'never'; SET query_timeout = 1800;
                   SELECT o.order_id, o.$(qb_part_col paimon orders) FROM $(qb_table_expr starrocks paimon orders) AS o
                   WHERE o.updated_at >= '${QB_HOT_FROM}' AND o.updated_at < '${QB_HOT_TO}' AND o.order_date < '${QB_HOT_FROM}'
                   ORDER BY o.updated_at DESC, o.order_id LIMIT 1" | tail -1)
        [[ "$r" =~ ^[0-9]+$'\t'[0-9-]+$ ]] || qb_die "no H8 candidate on the paimon arm (got '${r}'): no update of an older order in the window? Set LOOKUP_ORDER_ID and LOOKUP_ORDER_DT"
        QB_LOOKUP_ORDER_ID="${r%%$'\t'*}" QB_LOOKUP_ORDER_DT="${r#*$'\t'}"
        QB_LOOKUP_NOTE="picked on paimon, not confirmed on iceberg (Iceberg v3: StarRocks cannot read it)"
    else
        local off r id dt n
        for off in 0 1 2 3 4; do
            r=$(qb_sr "SET enable_scan_datacache = false; SET populate_datacache_mode = 'never'; SET query_timeout = 1800;
                       SELECT o.order_id, o.$(qb_part_col iceberg orders) FROM $(qb_table_expr starrocks iceberg orders) AS o
                       WHERE o.updated_at >= '${QB_HOT_FROM}' AND o.updated_at < '${QB_HOT_TO}' AND o.order_date < '${QB_HOT_FROM}'
                       ORDER BY o.updated_at DESC, o.order_id LIMIT 1 OFFSET ${off}" | tail -1)
            [[ "$r" =~ ^[0-9]+$'\t'[0-9-]+$ ]] || qb_die "no H8 candidate on the iceberg arm at OFFSET ${off} (got '${r}'): no update of an older order in the window? Set LOOKUP_ORDER_ID and LOOKUP_ORDER_DT"
            id="${r%%$'\t'*}" dt="${r#*$'\t'}"
            n=$(qb_sr "$(qb_sr_sets paimon)SET enable_scan_datacache = false; SET populate_datacache_mode = 'never';
                       SELECT COUNT(*) FROM $(qb_table_expr starrocks paimon orders) AS o
                       WHERE o.$(qb_part_col paimon orders) = '${dt}' AND o.order_id = ${id}" | tail -1)
            [[ "$n" == 1 ]] && { QB_LOOKUP_ORDER_ID="$id"; QB_LOOKUP_ORDER_DT="$dt"; break; }
            qb_log "H8 candidate ${id} (${dt}) is not on the native Paimon arm (count '${n}'); trying the next"
        done
        [[ -n "${QB_LOOKUP_ORDER_ID:-}" ]] || qb_die "no H8 key on both formats in 5 candidates; set LOOKUP_ORDER_ID and LOOKUP_ORDER_DT"
        QB_LOOKUP_NOTE="picked on iceberg, confirmed on paimon"
    fi
    if [[ "$QB_LOOKUP_ORDER_DT" == "${QB_HOT_DT:-}" ]]; then QB_LOOKUP_NOTE+="; partition = HOT_DT"
    else QB_LOOKUP_NOTE+="; partition older than HOT_DT (more equality deletes on Iceberg)"; fi
    qb_log "H8 key: order_id=${QB_LOOKUP_ORDER_ID} ${QB_LOOKUP_ORDER_DT} (${QB_LOOKUP_NOTE})"
}

# ── Snapshot phase gates (snapshot.sh; run.sh for SNAPSHOT_FORMAT=iceberg) ──
# Each prints "<name>=<UTC time>" lines for the phase's .pins file on stdout.

# StarRocks FE + all BEs Ready (when starrocks is in ENGINES) and the DuckDB
# server answering busy=false (when duckdb is), waiting up to
# ENGINE_GATE_TIMEOUT_S (1800).
qb_ready_count() {
    kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/component=$1" \
        -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null \
        | { grep -o True || true; } | wc -l | tr -d ' '
}
qb_engines_gate() {
    local deadline=$(( $(date +%s) + ${ENGINE_GATE_TIMEOUT_S:-1800} )) fe be h why
    while :; do
        why=""
        if [[ "${ENGINES:-starrocks duckdb}" == *starrocks* ]]; then
            fe=$(qb_ready_count fe); be=$(qb_ready_count be)
            (( fe >= 1 && be >= ${SR_BE_COUNT:-3} )) || why+=" StarRocks FE ${fe}/1 BE ${be}/${SR_BE_COUNT:-3} Ready;"
        fi
        if [[ "${ENGINES:-starrocks duckdb}" == *duckdb* ]]; then
            h=$(kubectl exec -n "$NAMESPACE" "deploy/${DUCKDB_NAME:-duckdb-warm-cdc}" -c duckdb -- python3 -c \
                'import json, urllib.request; print(json.load(urllib.request.urlopen("http://localhost:8080/health", timeout=10))["busy"])' 2>/dev/null || echo down)
            [[ "$h" == False ]] || why+=" ${DUCKDB_NAME:-duckdb-warm-cdc} ${h};"
        fi
        [[ -z "$why" ]] && { echo "gate_engines_ready_at=$(date -u '+%Y-%m-%d %H:%M:%SZ')"; return 0; }
        (( $(date +%s) < deadline )) || qb_die "engines not ready after ${ENGINE_GATE_TIMEOUT_S:-1800}s:${why}"
        qb_log "waiting for the engines:${why}"
        sleep 30
    done
}

# "<operation> <commit ms>" of the Iceberg table's current snapshot (Glue).
qb_iceberg_current() {   # <table>
    local loc
    loc=$(aws glue get-table --region "$AWS_REGION" --database-name "$ICEBERG_DB" --name "$1" \
            --query 'Table.Parameters.metadata_location' --output text 2>/dev/null) || return 1
    aws s3 cp --region "$AWS_REGION" "$loc" - 2>/dev/null | python3 -c '
import json, sys
m = json.load(sys.stdin); sid = m.get("current-snapshot-id")
s = next((s for s in m.get("snapshots", []) if s.get("snapshot-id") == sid), {})
print(s.get("summary", {}).get("operation", "?"), s.get("timestamp-ms", 0))'
}

# The Iceberg phase's gate: for every COMPACT_TABLES table, the Job
# iceberg-compact-<table> (../iceberg-compaction/run-compaction.sh) has
# SUCCEEDED, was created at or after COMPACT_NOT_BEFORE (epoch s; run.sh sets
# it when it submits the Jobs, so an older Job never counts), and the table's
# current snapshot is that compaction's `replace` commit (committed after the
# Job started): the compacted snapshot is what gets pinned and measured, and a
# write after it would void that. <wait>=1 waits (up to COMPACT_GATE_TIMEOUT_S,
# 21600) while a Job is missing or running; 0 refuses at once. A failed Job
# always refuses. ICEBERG_REQUIRE_COMPACTION=0 skips the gate (the uncompacted
# table is measured, and the pins say so).
qb_compaction_gate() {   # <wait 0|1>
    if [[ "${ICEBERG_REQUIRE_COMPACTION:-1}" == 0 ]]; then
        echo "compaction_gate=skipped (ICEBERG_REQUIRE_COMPACTION=0: the tables are measured as they are)"; return 0
    fi
    local wait_="$1" deadline=$(( $(date +%s) + ${COMPACT_GATE_TIMEOUT_S:-21600} )) t job st pending out cur
    while :; do
        pending="" out=""
        for t in ${COMPACT_TABLES:-customers products orders order_items}; do
            job="iceberg-compact-${t//_/-}"
            st=$(kubectl get job "$job" -n "$NAMESPACE" -o json 2>/dev/null | python3 -c '
import json, sys, datetime as d
j = json.load(sys.stdin); s = j.get("status", {})
ep = lambda v: int(d.datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=d.timezone.utc).timestamp()) if v else 0
state = "succeeded" if s.get("succeeded") else "failed" if s.get("failed") else "running"
print(state, ep(j["metadata"].get("creationTimestamp")), ep(s.get("startTime")), s.get("completionTime") or "-")' 2>/dev/null || echo "missing 0 0 -")
            set -- $st
            if [[ "$1" != missing ]] && (( $2 < ${COMPACT_NOT_BEFORE:-0} )); then set -- missing 0 0 -; fi
            case "$1" in
                failed)  qb_die "Iceberg phase refused: compaction Job ${job} FAILED (kubectl logs job/${job} -n ${NAMESPACE}); fix it and rerun SNAPSHOT_FORMAT=iceberg" ;;
                missing) pending+=" ${job}: not submitted${COMPACT_NOT_BEFORE:+ since $(date -u -r "$COMPACT_NOT_BEFORE" '+%H:%M:%SZ' 2>/dev/null || date -u -d "@$COMPACT_NOT_BEFORE" '+%H:%M:%SZ')};" ;;
                running) pending+=" ${job}: running;" ;;
                succeeded)
                    cur=$(qb_iceberg_current "$t") || qb_die "cannot read ${ICEBERG_DB}.${t}'s current snapshot"
                    set -- $st $cur
                    [[ "$5" == replace ]] && (( $6 >= $3 * 1000 )) \
                        || qb_die "Iceberg phase refused: ${ICEBERG_DB}.${t}'s current snapshot is '${5}' at ${6} ms, not the compaction's replace commit after ${job} started (a write after it, or a no-op compaction); recompact, or ICEBERG_REQUIRE_COMPACTION=0 to measure it as is"
                    out+="compaction_${t}=${job} completed ${4}, current snapshot replace@${6}"$'\n' ;;
            esac
        done
        if [[ -z "$pending" ]]; then
            printf '%s' "$out"; echo "gate_compaction_finished_at=$(date -u '+%Y-%m-%d %H:%M:%SZ')"; return 0
        fi
        [[ "$wait_" == 1 ]] || qb_die "Iceberg phase refused: compaction not complete:${pending} (start it with COMPACT_BEFORE_SNAPSHOT=true and SNAPSHOT_FORMAT=paimon|both, or wait)"
        (( $(date +%s) < deadline )) || qb_die "Iceberg phase refused: compaction not complete after ${COMPACT_GATE_TIMEOUT_S:-21600}s:${pending}"
        qb_log "Iceberg phase waits for compaction:${pending}"
        sleep 60
    done
}
