#!/usr/bin/env bash
# Shared by the CDC query benchmark's drivers (querybench/duckdb-warm.sh,
# trino.sh, hot-interleave.sh, snapshot.sh, pin.sh, run.sh, qb-watch.sh,
# clear-caches.sh) and engine-deploy/deploy-duckdb-cdc.sh. Sourced, never run.
# One copy of everything both engines must agree on: the query files and how
# they render, the windows, the pins, the timeout, the median, the writer stop
# and the results format.
#
# Two engines x two storage formats = four arms (ENGINES x FORMATS), every arm
# on a pinned version of each table. Flink writes both formats (the two CDC
# jobs); DuckDB and Trino only query:
#   duckdb:iceberg   the warm DuckDB server (engine-deploy/deploy-duckdb-cdc.sh),
#                    iceberg_scan() of a COPY of the Iceberg table's pinned metadata.json
#   trino:iceberg    Trino (engine-deploy/deploy-trino.sh), catalog `iceberg`
#                    (Glue), <ICEBERG_DB>.<t> FOR VERSION AS OF the pinned snapshot id
#   duckdb:paimon    the server's NATIVE Paimon extension (polardb/duckdb-paimon on
#                    paimon-cpp, bitmap64 DVs; duckdb-image/README.md):
#                    paimon_scan('<warehouse>/<PAIMON_DB>.db/<t>', snapshot_from_id=N)
#   trino:paimon     the NATIVE Paimon connector (paimon-trino 2.1.0 ported to 484,
#                    image trino-cdc:484-paimon-2.1.0), catalog QB_TRINO_PAIMON_CATALOG
#                    (`paimon`, filesystem metastore on the warehouse):
#                    paimon.<PAIMON_DB>.<t> FOR VERSION AS OF BIGINT 'N' (scan.snapshot-id)
# One pin.sh call pins both formats; both engines of a format read the same
# snapshot (Iceberg: the copied metadata.json / its snapshot id; Paimon: native
# snapshot N from <table>/snapshot/LATEST, no copy: a snapshot's files stay until
# it expires, >= 12 h). Paimon's Iceberg export is NOT read any more (it listed
# only top-LSM-level files until a full compaction, and DuckDB could not read its
# v3 manifests). The two formats are pinned at the same moment, not the same binlog
# point, so iceberg vs paimon rows can differ by about a checkpoint (summarize.py).
# An arm that cannot read a table records ERROR per query and the run goes on.
#
# Placeholders in querybench/hot/*.sql and querybench/snapshot/*.sql
# (qb_render; any other ${...} left after rendering stops the run):
#   ${CUSTOMERS} ${PRODUCTS} ${ORDERS} ${ORDER_ITEMS}
#       the table at its pinned version, per engine and arm (qb_table_expr):
#         duckdb  iceberg: iceberg_scan('<copied metadata.json>')
#                 paimon:  paimon_scan('<table path>', snapshot_from_id=<N>)
#         trino   iceberg: <TRINO_CATALOG>.<ICEBERG_DB>.<table> FOR VERSION AS OF BIGINT '<snapshot id>'
#                 paimon:  <QB_TRINO_PAIMON_CATALOG>.<PAIMON_DB>.<table> FOR VERSION AS OF BIGINT '<N>'
#       An alias may follow (`FROM ${ORDERS} o`, `${ORDERS} AS o`): Trino's
#       grammar (aliasedRelation: relationPrimary (AS? identifier)?, with
#       relationPrimary: qualifiedName queryPeriod?) takes it after the
#       version clause.
#   ${ORDERS_PART} ${ORDER_ITEMS_PART}
#       the partition column (STRING, 'YYYY-MM-DD'), per arm: iceberg
#       order_dt / created_dt (ingest-deploy/sql-scripts/iceberg/03-iceberg-sinks.sql),
#       paimon order_date_str / created_date_str (ingest-deploy/sql-scripts/paimon/03-paimon-sinks.sql)
#   ${HOT_DT} ${HOT_FROM} ${HOT_TO}            hot window (UTC, 'YYYY-MM-DD HH:MM:SS')
#   ${SNAPSHOT_DT} ${S_FROM} ${S_TO}           snapshot partition and the slice inside it
#                                              (S_FROM/S_TO: bounds on orders.order_date)
#   ${LOOKUP_ORDER_ID} ${LOOKUP_ORDER_DT}      hot H8's key: an order updated in the
#                                              window, picked once per run (qb_resolve_lookup)
#   dialect keys    every KEY=VALUE line of querybench/dialect-<engine>.env
#                   (BYTE_LEN_OPEN / BYTE_LEN_CLOSE), written with the SQL; a key may not
#                   shadow one of the placeholders above. The file must exist.
# Every value is a literal resolved once per run (per pass for the hot pins),
# never a clock function in the SQL.
#
# File name = query name, lexical order = run order. A `-- bench: <flag>` line
# sets a per-query flag: allow-zero-rows, leading-col-is-label (the ZERO_SCAN
# guard, the same on every arm).

[[ -n "${_CDC_QUERYBENCH_LIB:-}" ]] && return 0
_CDC_QUERYBENCH_LIB=1

# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
QB_DIR="${CDC_DIR}/querybench"
QB_DIALECT_DIR="${QB_DIALECT_DIR:-${QB_DIR}}"

: "${BUCKET_NAME:?BUCKET_NAME is not set (.bench-env / .bench-env.local)}"
QB_TABLES="customers products orders order_items"
PAIMON_WAREHOUSE="${PAIMON_WAREHOUSE:-s3://${BUCKET_NAME}/paimon-warehouse}"; PAIMON_WAREHOUSE="${PAIMON_WAREHOUSE%/}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-s3://${BUCKET_NAME}/iceberg-warehouse}"; ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE%/}"
# flink_{paimon,iceberg}_db<SESSION_SUFFIX>: set by common.sh.
PAIMON_DB="${PAIMON_DB:-flink_paimon_db${SESSION_SUFFIX:-}}"
ICEBERG_DB="${ICEBERG_DB:-flink_iceberg_db${SESSION_SUFFIX:-}}"
GENERATOR_RESOURCES="${GENERATOR_RESOURCES:-statefulset/mysql-upsert-loadgen}"
DUCKDB_NAME="${DUCKDB_NAME:-duckdb-warm-cdc}"
# Trino (engine-deploy/deploy-trino.sh: the trino Helm chart, release `trino`).
# The CLI ships in the coordinator image (/usr/bin/trino, same version as the
# server, its own JDK), so trino.sh runs it there through kubectl exec.
TRINO_COORDINATOR="${TRINO_COORDINATOR:-deploy/trino-coordinator}"
TRINO_CONTAINER="${TRINO_CONTAINER:-trino-coordinator}"
TRINO_WORKER_DEPLOY="${TRINO_WORKER_DEPLOY:-trino-worker}"
TRINO_WORKERS="${TRINO_WORKERS:-3}"
TRINO_CATALOG="${TRINO_CATALOG:-iceberg}"
# The native Paimon connector's catalog (engine-deploy/trino-values.yaml `paimon`).
QB_TRINO_PAIMON_CATALOG="${QB_TRINO_PAIMON_CATALOG:-paimon}"
TRINO_USER="${TRINO_USER:-cdcbench}"
# Not TRINO_PORT: Kubernetes service links set TRINO_PORT=tcp://<ip>:8080 in every pod
# of the namespace (Service "trino"), and the runner then called
# http://localhost:tcp://... ("Catalog name is empty"), failing every pin (2026-11-02).
QB_TRINO_PORT="${QB_TRINO_PORT:-8080}"
# The coordinator's REST API from the runner pod, for /v1/query/<id> (CPU and
# input bytes; system.runtime.queries has neither).
TRINO_URL="${TRINO_URL:-http://trino.${NAMESPACE}.svc.cluster.local:8080}"
# Heap of the CLI's own JVM in the coordinator container (it only streams results).
TRINO_CLI_JAVA_OPTS="${TRINO_CLI_JAVA_OPTS:--Xmx512m -XX:TieredStopAtLevel=1}"
# Copies of the pinned metadata.json files (one key per run and table).
QB_PIN_PREFIX="${QB_PIN_PREFIX:-querybench-pins/cdc-session${CDC_SET_TAG:-}}"
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

# ── Formats (arms) ──────────────────────────────────────────────────────────
# FORMATS=iceberg|paimon|both (default both) -> "iceberg paimon" or one.
qb_formats() {
    case "${FORMATS:-both}" in
        both|"iceberg paimon"|"paimon iceberg"|iceberg,paimon|paimon,iceberg) echo "iceberg paimon" ;;
        iceberg|paimon) echo "$FORMATS" ;;
        *) qb_die "FORMATS must be iceberg|paimon|both, got '${FORMATS}'" ;;
    esac
}
qb_has_format() { [[ " $(qb_formats) " == *" $1 "* ]]; }
# The writer(s) whose tables are queried (one per format in FORMATS): the hot
# test needs them RUNNING and a suite is void if their state changes under it.
# FLINK_JOBS (both) is what the snapshot test and the watcher suspend.
QB_QUERY_WRITERS="${QB_QUERY_WRITERS:-$(for _f in $(qb_formats); do printf 'flink-cdc-%s ' "$_f"; done)}"
QB_QUERY_WRITERS="${QB_QUERY_WRITERS% }"

# ── Engines ─────────────────────────────────────────────────────────────────
QB_ENGINES_ALL="duckdb trino"
# ENGINES (default both) normalised: space-separated, validated, in the
# order given (= the order hot passes and the snapshot suite run them).
qb_engines() {
    local e out="" raw="${ENGINES:-$QB_ENGINES_ALL}"
    for e in ${raw//,/ }; do
        case "$e" in duckdb|trino) [[ " $out " == *" $e "* ]] || out+=" $e" ;;
                     *) qb_die "ENGINES must name duckdb and/or trino, got '${raw}'" ;; esac
    done
    [[ -n "$out" ]] || qb_die "ENGINES is empty"
    echo "${out# }"
}
qb_has_engine() { [[ " $(qb_engines) " == *" $1 "* ]]; }
# The engine for the untimed setup reads (S4 slice span, H8 key): Trino when it
# runs (no cache of its own unless one is configured), else DuckDB.
qb_setup_engine() {
    if [[ -n "${QB_SETUP_ENGINE:-}" ]]; then echo "$QB_SETUP_ENGINE"
    elif qb_has_engine trino; then echo trino
    else echo duckdb; fi
}
# The CSV `engine` value of each engine.
qb_engine_label() { case "$1" in duckdb) echo duckdb_warm ;; *) echo "$1" ;; esac; }

# The table set and engine settings of this run, one key=value line each, for
# every .pins file (and through them the summary's header).
qb_run_meta() {
    echo "iceberg_format_version=${ICEBERG_FORMAT_VERSION:-3}"
    echo "cdc_db_suffix=${SESSION_SUFFIX:-} iceberg_db=${ICEBERG_DB} paimon_db=${PAIMON_DB} engines=$(qb_engines | tr ' ' ',') formats=$(qb_formats | tr ' ' ',')"
    local sp="${SKIP_PUFFIN_VERIFICATION_ARMS-}"
    echo "duckdb_image=${DUCKDB_IMAGE:-pip duckdb==${DUCKDB_VERSION:-2.0.0}} duckdb_cpu=${DUCKDB_CPU:-27} duckdb_pod_memory=${DUCKDB_POD_MEMORY:-114Gi} duckdb_skip_puffin_verification_arms=${sp// /,}"
    echo "trino_version=${TRINO_VERSION:-484} trino_catalog=${TRINO_CATALOG} trino_paimon_catalog=${QB_TRINO_PAIMON_CATALOG} trino_workers=${TRINO_WORKERS} paimon_reader=native"
}

# The partition column of <table> in the files <arm> reads (overridable).
qb_part_col() {   # <iceberg|paimon> <table>
    case "$1:$2" in
        iceberg:orders)       echo "${ICEBERG_ORDERS_PART:-order_dt}" ;;
        iceberg:order_items)  echo "${ICEBERG_ORDER_ITEMS_PART:-created_dt}" ;;
        paimon:orders)        echo "${PAIMON_ORDERS_PART:-order_date_str}" ;;
        paimon:order_items)   echo "${PAIMON_ORDER_ITEMS_PART:-created_date_str}" ;;
        *)                    return 1 ;;
    esac
}

# The table expression for one engine, arm and table, at its pin. Pins are
# the QB_PIN_* variables pin.sh prints (eval'd by the caller):
#   QB_PIN_META_ICEBERG_<TABLE>   the copied metadata.json (DuckDB)
#   QB_PIN_META_PAIMON_<TABLE>    the native table path (DuckDB paimon_scan)
#   QB_PIN_SNAP_<ICEBERG|PAIMON>_<TABLE>   the snapshot id (both engines for paimon)
# Without a pin both are an error: DuckDB's live metadata.json is deleted by
# the writer mid-scan (memory note: pinned metadata on every arm), and an
# unpinned Trino read is a different version from DuckDB's.
qb_table_expr() {   # <duckdb|trino> <iceberg|paimon> <table>
    local e="$1" arm="$2" t="$3" T v A db
    T=$(qb_upper "$t"); A=$(qb_upper "$arm")
    case "$arm" in iceberg) db="$ICEBERG_DB" ;; paimon) db="$PAIMON_DB" ;; *) return 1 ;; esac
    case "$e" in
        duckdb)
            v="QB_PIN_META_${A}_${T}"
            [[ -n "${!v:-}" ]] || { qb_log "no ${v}: DuckDB reads only pinned tables (run pin.sh first)"; return 1; }
            if [[ "$arm" == paimon ]]; then
                local s="QB_PIN_SNAP_${A}_${T}"
                [[ "${!s:-}" =~ ^[0-9]+$ ]] || { qb_log "no ${s}: DuckDB reads Paimon only at the pinned snapshot"; return 1; }
                echo "paimon_scan('${!v}', snapshot_from_id=${!s})"
            else
                echo "iceberg_scan('${!v}')"
            fi ;;
        trino)
            v="QB_PIN_SNAP_${A}_${T}"
            [[ "${!v:-}" =~ ^[0-9]+$ ]] || { qb_log "no ${v}: Trino reads only the pinned snapshot (run pin.sh first)"; return 1; }
            if [[ "$arm" == paimon ]]; then
                echo "${QB_TRINO_PAIMON_CATALOG}.${db}.${t} FOR VERSION AS OF BIGINT '${!v}'"
            else
                echo "${TRINO_CATALOG}.${db}.${t} FOR VERSION AS OF BIGINT '${!v}'"
            fi ;;
        *) return 1 ;;
    esac
}

# ── Pins ────────────────────────────────────────────────────────────────────
# Pin every table of each format in QB_PIN_FORMATS (default: FORMATS) at its
# CURRENT version:
#   iceberg  copy Glue's metadata_location to a key this run owns
#            (s3://$BUCKET_NAME/$QB_PIN_PREFIX/<tag>/iceberg/<table>/; the writer
#            deletes old metadata files) and take its current-snapshot-id
#   paimon   the NATIVE snapshot N in <warehouse>/<db>.db/<table>/snapshot/LATEST,
#            checked against snapshot-N (its commit time and record count); no copy:
#            a Paimon snapshot's files stay until the snapshot expires (>= 12 h)
# Prints shell exports: QB_PIN_META_<FMT>_<T> (iceberg: copied metadata.json;
# paimon: the table path), QB_PIN_SNAP_<FMT>_<T>, QB_PIN_COMMIT_MS_<FMT>_<T>
# (the pinned snapshot's commit time), QB_PIN_FORMAT_VERSION_ICEBERG_<T>.
# Read-only on the tables; needs the aws CLI and s3:PutObject on the bucket.
# With QB_TABLE_STATE_OUT set, appends one TSV line per format and table: the
# pinned snapshot's id, commit time and summary totals (records, data files,
# delete files, equality / position deletes, bytes; for Paimon: its snapshot's
# totalRecordCount and commitKind), the state the times depend on.
qb_pin_tables() {   # <tag>
    ICEBERG_FORMAT_VERSION="${ICEBERG_FORMAT_VERSION:-3}" QB_PIN_FORMATS="${QB_PIN_FORMATS:-$(qb_formats)}" QB_TABLES="$QB_TABLES" \
    QB_PIN_PREFIX="$QB_PIN_PREFIX" PAIMON_WAREHOUSE="$PAIMON_WAREHOUSE" \
    PAIMON_DB="$PAIMON_DB" ICEBERG_DB="$ICEBERG_DB" python3 - "$1" <<'EOF'
import json, os, subprocess, sys, time
E = os.environ
tag, bucket, region = sys.argv[1], E["BUCKET_NAME"], E["AWS_REGION"]
prefix, pwh = E["QB_PIN_PREFIX"].strip("/"), E["PAIMON_WAREHOUSE"].rstrip("/")
def aws(*a):
    return subprocess.run(["aws", "--region", region, *a], check=True, capture_output=True, text=True).stdout
STATE_KEYS = ("total-records", "total-data-files", "total-delete-files", "total-equality-deletes",
              "total-position-deletes", "total-files-size")
out, state = [], []
def pin_paimon(t):
    """The native Paimon snapshot N (snapshot/LATEST, checked against snapshot-N)."""
    root = "%s/%s.db/%s" % (pwh, E["PAIMON_DB"], t)
    n = aws("s3", "cp", root + "/snapshot/LATEST", "-").strip()
    if not n.isdigit():
        sys.exit("no snapshot/LATEST under %s (is it a Paimon table?)" % root)
    ps = json.loads(aws("s3", "cp", "%s/snapshot/snapshot-%s" % (root, n), "-"))
    ts = ps.get("timeMillis", "")
    state.append(["paimon", t, n, ts, ps.get("totalRecordCount", ""), "", "", "", "", "",
                  "commitKind=%s" % ps.get("commitKind", "?")])
    T = t.upper()
    out.extend(["QB_PIN_META_PAIMON_%s=%s" % (T, root), "QB_PIN_SNAP_PAIMON_%s=%s" % (T, n),
                "QB_PIN_COMMIT_MS_PAIMON_%s=%s" % (T, ts)])
def pin(fmt, t, loc, want_sid=None):
    """Copy <loc>; returns (pinned s3 uri, snapshot id, commit ms)."""
    key = "%s/%s/%s/%s/%s" % (prefix, tag, fmt, t, loc.rsplit("/", 1)[1])
    aws("s3", "cp", "--only-show-errors", loc, "s3://%s/%s" % (bucket, key))
    meta = json.loads(aws("s3", "cp", "s3://%s/%s" % (bucket, key), "-"))
    ids = {s.get("snapshot-id") for s in meta.get("snapshots", [])}
    sid, note = meta.get("current-snapshot-id"), ""
    if want_sid is not None:
        if want_sid in ids:
            sid = want_sid
        else:
            note = " export snapshot id %s not in its snapshots: current-snapshot-id %s used" % (want_sid, sid)
            print("  [querybench] WARNING: paimon %s:%s" % (t, note), file=sys.stderr)
    if sid in (None, -1):
        sys.exit("%s %s has no current snapshot (%s)" % (fmt, t, loc))
    snap = next((s for s in meta.get("snapshots", []) if s.get("snapshot-id") == sid), {})
    ts, sm, fv = snap.get("timestamp-ms", ""), snap.get("summary", {}), meta.get("format-version")
    if str(fv) != E.get("ICEBERG_FORMAT_VERSION", "3"):
        print("  [querybench] WARNING: %s %s metadata is format-version %s, not ICEBERG_FORMAT_VERSION=%s (%s)"
              % (fmt, t, fv, E.get("ICEBERG_FORMAT_VERSION", "3"), loc), file=sys.stderr)
    state.append([fmt, t, sid, ts] + [sm.get(k, "") for k in STATE_KEYS]
                 + ["format-version=%s operation=%s%s" % (fv, sm.get("operation", "?"), note.replace(" ", "_"))])
    F, T = fmt.upper(), t.upper()
    out.extend(["QB_PIN_META_%s_%s=s3://%s/%s" % (F, T, bucket, key), "QB_PIN_SNAP_%s_%s=%s" % (F, T, sid),
                "QB_PIN_COMMIT_MS_%s_%s=%s" % (F, T, ts), "QB_PIN_FORMAT_VERSION_%s_%s=%s" % (F, T, fv)])
FORMATS = E["QB_PIN_FORMATS"].split()
for t in E["QB_TABLES"].split():
    for attempt in range(3):   # a writer may delete the file between the lookup and the copy
        mark = (len(out), len(state))
        try:
            if "iceberg" in FORMATS:
                loc = aws("glue", "get-table", "--database-name", E["ICEBERG_DB"], "--name", t,
                          "--query", "Table.Parameters.metadata_location", "--output", "text").strip()
                if not loc or loc == "None":
                    sys.exit("no metadata_location on %s.%s" % (E["ICEBERG_DB"], t))
                pin("iceberg", t, loc)
            if "paimon" in FORMATS:
                pin_paimon(t)
            break
        except subprocess.CalledProcessError as e:
            del out[mark[0]:], state[mark[1]:]
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

# The newest commit time (ms) of <table> in <iceberg|paimon> (read-only: the
# Iceberg metadata via Glue, Paimon's latest snapshot file).
qb_commit_ms() {   # <iceberg|paimon> <table>
    if [[ "$1" == iceberg ]]; then
        local c; c=$(qb_iceberg_current "$2") || return 1
        echo "${c##* }"
    else
        local root="${PAIMON_WAREHOUSE}/${PAIMON_DB}.db/$2/snapshot" n
        n=$(aws s3 cp --region "$AWS_REGION" "${root}/LATEST" - 2>/dev/null | tr -dc 0-9) && [[ -n "$n" ]] || return 1
        aws s3 cp --region "$AWS_REGION" "${root}/snapshot-${n}" - 2>/dev/null \
            | python3 -c 'import json, sys; print(json.load(sys.stdin)["timeMillis"])'
    fi
}

# ── Windows ─────────────────────────────────────────────────────────────────
# Hot: HOT_WINDOW_SECONDS (30) ending HOT_LAG_SECONDS (300) before now; the end
# on a whole minute, inside one day (moved back to end at midnight if it would
# cross it). Each bound can be pinned with the same-named variable. ESTIMATE,
# not measured: size it from the first run's H* row counts.
qb_utc_minus_sec() {   # "<YYYY-MM-DD HH:MM:SS>" <seconds> (BSD date first, GNU second)
    date -u -j -v-"$2"S -f '%Y-%m-%d %H:%M:%S' "$1" '+%Y-%m-%d %H:%M:%S' 2>/dev/null \
      || date -u -d "@$(( $(date -u -d "$1" +%s) - $2 ))" '+%Y-%m-%d %H:%M:%S'
}
qb_resolve_hot() {
    local w="${HOT_WINDOW_SECONDS:-30}" lag="${HOT_LAG_SECONDS:-300}" to
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
# both fact tables hold in every format of FORMATS (SNAPSHOT_DT_PICK=oldest:
# the oldest).
# "Closed" here = not today; the writers are stopped before the test anyway.
qb_resolve_snapshot_dt() {
    if [[ -z "${SNAPSHOT_DT:-}" ]]; then
        local d today all n=0
        d=$(mktemp -d); today=$(date -u +%Y-%m-%d)
        if qb_has_format iceberg; then
            _qb_parts_under "${ICEBERG_WAREHOUSE}/${ICEBERG_DB}.db/orders/data"      "$(qb_part_col iceberg orders)"      > "$d/1"
            _qb_parts_under "${ICEBERG_WAREHOUSE}/${ICEBERG_DB}.db/order_items/data" "$(qb_part_col iceberg order_items)" > "$d/2"
            n=$((n + 2))
        fi
        if qb_has_format paimon; then
            _qb_parts_under "${PAIMON_WAREHOUSE}/${PAIMON_DB}.db/orders"      "$(qb_part_col paimon orders)"      > "$d/3"
            _qb_parts_under "${PAIMON_WAREHOUSE}/${PAIMON_DB}.db/order_items" "$(qb_part_col paimon order_items)" > "$d/4"
            n=$((n + 2))
        fi
        all=$(cat "$d"/* | sort | uniq -c | awk -v n="$n" '$1 == n {print $2}' | awk -v t="$today" '$1 < t')
        rm -rf "$d"
        [[ -n "$all" ]] || qb_die "no date before ${today} in the orders and order_items partitions of every format ($(qb_formats)); set SNAPSHOT_DT"
        if [[ "${SNAPSHOT_DT_PICK:-newest}" == oldest ]]; then SNAPSHOT_DT=$(head -1 <<<"$all"); else SNAPSHOT_DT=$(tail -1 <<<"$all"); fi
    fi
    [[ "$SNAPSHOT_DT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || qb_die "SNAPSHOT_DT='${SNAPSHOT_DT}' is not YYYY-MM-DD"
    QB_SNAPSHOT_DT="$SNAPSHOT_DT"
}

# ── Untimed setup reads (slice span, H8 key) on one engine ─────────────────
# Rows of one small statement, fields tab-separated, NULL as NULL. Both engines
# accept the SQL below (TIMESTAMP '...' literals, no dialect keys).
qb_duck_query() {   # <sql> [timeout s]
    kubectl exec -i -n "$NAMESPACE" "deploy/${DUCKDB_NAME}" -c duckdb -- python3 -c '
import json, sys, time, urllib.request
sql, to = sys.stdin.read(), int(sys.argv[1])
def call(path, body=None):
    req = urllib.request.Request("http://localhost:8080" + path, method="GET" if body is None else "POST",
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=60))
jid = call("/query", {"sql": sql, "suite": "setup", "arm": "iceberg", "name": "setup", "runs": 1, "warmup": 0,
                      "timeout_s": to, "allow_zero_rows": True, "leading_col_is_label": True})["id"]
deadline = time.time() + to + 120
while time.time() < deadline:
    time.sleep(2)
    j = call("/job/" + jid)
    if j.get("state") == "done":
        if j.get("status") != "OK":
            sys.exit("duckdb: %s %s" % (j.get("status"), j.get("error", "")))
        for r in j.get("rows") or []:
            print(r.replace("|", "\t"))
        sys.exit(0)
sys.exit("duckdb: no result in %ds" % (to + 120))' "${2:-1800}" <<<"$1"
}

# The trino CLI in the coordinator container. Extra CLI flags may follow the SQL.
qb_trino_cli() {   # <sql> [cli flags...]
    local sql="$1"; shift
    kubectl exec -i -n "$NAMESPACE" "$TRINO_COORDINATOR" -c "$TRINO_CONTAINER" -- \
        env JAVA_TOOL_OPTIONS="$TRINO_CLI_JAVA_OPTS" \
        trino --server "http://localhost:${QB_TRINO_PORT}" --user "$TRINO_USER" --catalog "$TRINO_CATALOG" \
              --schema "$ICEBERG_DB" "$@" --execute "$sql"
}
# TSV rows (NULL for null), stderr noise (JAVA_TOOL_OPTIONS banner) dropped.
qb_trino() {   # <sql> [timeout s]
    qb_trino_cli "$1" --output-format TSV --session "query_max_run_time=${2:-1800}s" --source querybench-setup 2>/dev/null
}
# qb_trino with the error kept: "<rc><TAB><last output or error line>".
qb_trino_try() {   # <sql>
    local out rc
    out=$(qb_trino_cli "$1" --output-format TSV --source querybench-setup 2>&1); rc=$?
    out=$(grep -v -E '^Picked up JAVA_TOOL_OPTIONS|^command terminated with exit code' <<<"$out" || true)
    # The Trino CLI's own error ("Query ... failed: ...") rather than kubectl's
    # trailing "command terminated with exit code 1", which hid it on 2026-11-02.
    if [[ "$rc" != 0 ]] && grep -q -E 'failed:|Error|Exception' <<<"$out"; then
        out=$(grep -E 'failed:|Error|Exception' <<<"$out" | head -1)
    fi
    printf '%s\t%s\n' "$rc" "$(tail -1 <<<"$out" | tr '\t' ' ' | cut -c1-400)"
}

qb_setup_query() {   # <engine> <arm> <sql with ${ORDERS}-style table placeholders> [timeout s]
    local e="$1" arm="$2" sql="$3" t T x
    for t in $QB_TABLES; do
        T=$(qb_upper "$t")
        [[ "$sql" == *"\${${T}}"* ]] || continue
        x=$(qb_table_expr "$e" "$arm" "$t") || return 1
        sql="${sql//\$\{${T}\}/${x}}"
    done
    case "$e" in
        duckdb) qb_duck_query "$sql" "${4:-1800}" ;;
        trino)  qb_trino "$sql" "${4:-1800}" ;;
        *)      return 1 ;;
    esac
}

# Snapshot slice [S_FROM, S_TO): SNAPSHOT_SLICE_MINUTES (10) on whole minutes,
# bounds on orders.order_date (S4, the one sliced query), centred in the span
# the PINNED orders of every format hold in SNAPSHOT_DT (MIN/MAX(order_date): the data
# was bulk-loaded by the CDC snapshot phase, so the commit history says
# nothing about event times), SNAPSHOT_SLICE_MARGIN_S (120) clear of each
# end. SNAPSHOT_SLICE_COLUMNS ("orders:order_date") lists the table:column
# spans intersected. 0 = the whole partition. Pin with S_FROM and S_TO.
# Read on qb_setup_engine, untimed. snapshot.sh resolves it after its gate and
# pin, before the cold start, so it reads the (compacted) pinned snapshots the
# arms then measure. Without pins in the shell, the tables are pinned for it
# (tag <STAMP>-slice, not in tables-<STAMP>.tsv). ESTIMATE: 10 min is a guess
# at "short"; size it from the first run's S4 row counts.
qb_resolve_snapshot_slice() {
    local ts_re='^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
    local M="${SNAPSHOT_SLICE_MINUTES:-10}" margin="${SNAPSHOT_SLICE_MARGIN_S:-120}" spans="" t col tc r e arm used=""
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
    if [[ -z "${QB_PIN_SNAP_ICEBERG_ORDERS:-}${QB_PIN_SNAP_PAIMON_ORDERS:-}" ]]; then
        r=$(QB_TABLE_STATE_OUT="" qb_pin_tables "${STAMP}-slice") || qb_die "pinning for the snapshot slice failed"
        eval "$r"
    fi
    # Every format's span, intersected (both CDC jobs replicate the same rows).
    # Read on qb_setup_engine, else on the other engine; a format no engine can
    # read is left out, and logged.
    for arm in $(qb_formats); do
        for tc in ${SNAPSHOT_SLICE_COLUMNS:-orders:order_date}; do
            t="${tc%%:*}" col="${tc#*:}" r=""
            for e in $( { qb_setup_engine; qb_engines | tr ' ' '\n'; } | awk '!seen[$0]++'); do
                r=$(qb_setup_query "$e" "$arm" "SELECT MIN(${col}), MAX(${col}) FROM \${$(qb_upper "$t")} AS q WHERE q.$(qb_part_col "$arm" "$t") = '${QB_SNAPSHOT_DT}'" | tail -1) || r=""
                [[ "$r" == *$'\t'* && "${r%%$'\t'*}" =~ ^[0-9]{4}- && "${r#*$'\t'}" =~ ^[0-9]{4}- ]] && break
                qb_log "snapshot span ${arm} ${t}.${col}: not readable on ${e} (got '${r}')"; r=""
            done
            [[ -n "$r" ]] || { qb_log "WARNING: no engine reads the ${arm} ${t}.${col} span; the slice leaves ${arm} out"; continue 2; }
            spans+="${r}"$'\n'
            qb_log "snapshot span ${arm} ${t}.${col} (${e}, pinned): ${r/$'\t'/ .. }"
        done
        used+=" ${arm}"
    done
    [[ -n "$spans" ]] || qb_die "no snapshot slice span on any format; set S_FROM/S_TO, or SNAPSHOT_SLICE_MINUTES=0"
    r=$(python3 - "$QB_SNAPSHOT_DT" "$M" "$margin" "$spans" <<'EOF'
import sys
from datetime import datetime, timezone
dt, M, margin = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
p = lambda s: datetime.strptime(s.strip()[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
f = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
spans = [tuple(p(x) for x in l.split("\t")) for l in sys.argv[4].splitlines() if l.strip()]
day0 = datetime.strptime(dt, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
lo = max(max(a for a, _ in spans), day0) + margin
hi = min(min(b for _, b in spans), day0 + 86400) - margin
span = "rows in every pinned format read (SNAPSHOT_SLICE_COLUMNS), less %ds each end: [%s, %s)" % (margin, f(lo), f(hi))
if hi - lo < M * 60:
    print("ERR %s is %.1f min, under the %d-min slice" % (span, max(hi - lo, 0) / 60, M)); sys.exit()
st = max(int((lo + hi) / 2 - M * 30) // 60 * 60, int(lo + 59) // 60 * 60)
print("OK|%s|%s|%d min, centred in the span %s" % (f(st), f(st + M * 60), M, span))
EOF
)
    [[ "$r" == OK\|* ]] || qb_die "snapshot slice: ${r#ERR }; lower SNAPSHOT_SLICE_MINUTES or set S_FROM/S_TO"
    IFS='|' read -r _ QB_S_FROM QB_S_TO QB_S_HOW <<<"$r"
    QB_S_HOW+="; formats:${used}"
    return 0
}

# H8's key: an order UPDATED in the hot window (updated_at in [HOT_FROM, HOT_TO),
# created before it), picked on the pinned orders of the first format in
# FORMATS (iceberg when it runs) on qb_setup_engine. Not confirmed on the other
# arms: the summary compares their rows. Sets QB_LOOKUP_ORDER_ID / QB_LOOKUP_ORDER_DT
# (LOOKUP_ORDER_ID / LOOKUP_ORDER_DT pin them) and QB_LOOKUP_NOTE. The
# selection reads updated_at over the whole table (no partition filter, like
# H4), untimed, before the warm-up pass.
qb_resolve_lookup() {
    if [[ -n "${LOOKUP_ORDER_ID:-}" ]]; then
        [[ "$LOOKUP_ORDER_ID" =~ ^[0-9]+$ && "${LOOKUP_ORDER_DT:-}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] \
            || qb_die "LOOKUP_ORDER_ID must be an integer and LOOKUP_ORDER_DT a YYYY-MM-DD"
        QB_LOOKUP_ORDER_ID="$LOOKUP_ORDER_ID"; QB_LOOKUP_ORDER_DT="$LOOKUP_ORDER_DT"; QB_LOOKUP_NOTE="pinned by hand"
    else
        local e r arm
        e=$(qb_setup_engine); arm=$(qb_formats); arm="${arm%% *}"   # iceberg when it is in FORMATS
        r=$(qb_setup_query "$e" "$arm" "SELECT o.order_id, o.$(qb_part_col "$arm" orders) FROM \${ORDERS} AS o
                 WHERE o.updated_at >= TIMESTAMP '${QB_HOT_FROM}' AND o.updated_at < TIMESTAMP '${QB_HOT_TO}'
                   AND o.order_date < TIMESTAMP '${QB_HOT_FROM}'
                 ORDER BY o.updated_at DESC, o.order_id LIMIT 1" | tail -1) || true
        [[ "$r" =~ ^[0-9]+$'\t'[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] \
            || qb_die "no H8 candidate on ${e}:${arm} (got '${r}'): no update of an older order in the window? Set LOOKUP_ORDER_ID and LOOKUP_ORDER_DT"
        QB_LOOKUP_ORDER_ID="${r%%$'\t'*}" QB_LOOKUP_ORDER_DT="${r#*$'\t'}"
        QB_LOOKUP_NOTE="picked on ${e}:${arm} (pinned orders), not confirmed on the other arms (the summary compares their rows)"
    fi
    if [[ "$QB_LOOKUP_ORDER_DT" == "${QB_HOT_DT:-}" ]]; then QB_LOOKUP_NOTE+="; partition = HOT_DT"
    else QB_LOOKUP_NOTE+="; partition older than HOT_DT (more equality deletes)"; fi
    qb_log "H8 key: order_id=${QB_LOOKUP_ORDER_ID} ${QB_LOOKUP_ORDER_DT} (${QB_LOOKUP_NOTE})"
}

# ── Queries ─────────────────────────────────────────────────────────────────
qb_query_files() { ls "${QB_DIR}/$1"/*.sql 2>/dev/null || qb_die "no .sql files in ${QB_DIR}/$1"; }
qb_query_name()  { basename "$1" .sql; }
qb_query_flag()  { grep -qE "^-- bench:.*(^|[^A-Za-z0-9_-])$2([^A-Za-z0-9_=-]|\$)" "$1"; }
qb_dialect_file() { echo "${QB_DIALECT_DIR}/dialect-$1.env"; }

# Render one file for one engine: every placeholder above, from the QB_*
# windows, the pins and the engine's dialect file. ANY ${...} left is fatal and
# named: it is not a syntax error on either engine (it binds as an identifier
# or string and returns a wrong answer), and it is how a placeholder the SQL
# authors added shows up here first. In Python, so a value's characters are
# never interpreted (bash 5.2's patsub_replacement treats & in a replacement).
qb_render() {   # <file> <duckdb|trino> <iceberg|paimon>
    local f="$1" e="$2" arm="$3" t T x args=() dialect
    dialect=$(qb_dialect_file "$e")
    [[ -f "$dialect" ]] || { qb_log "no dialect file ${dialect#${CDC_DIR}/} for ${e} (KEY=VALUE lines, e.g. BYTE_LEN_OPEN=...)"; return 1; }
    for t in $QB_TABLES; do
        T=$(qb_upper "$t")
        x=$(qb_table_expr "$e" "$arm" "$t") || { qb_log "no table expression for ${e}/${arm}/${t}"; return 1; }
        args+=("${T}=${x}")
    done
    args+=("ORDERS_PART=$(qb_part_col "$arm" orders)" "ORDER_ITEMS_PART=$(qb_part_col "$arm" order_items)")
    # A window value that is not resolved stays unset, so its placeholder fails.
    [[ -n "${QB_HOT_DT:-}" ]]      && args+=("HOT_DT=${QB_HOT_DT}" "HOT_FROM=${QB_HOT_FROM}" "HOT_TO=${QB_HOT_TO}")
    [[ -n "${QB_SNAPSHOT_DT:-}" ]] && args+=("SNAPSHOT_DT=${QB_SNAPSHOT_DT}")
    [[ -n "${QB_S_FROM:-}" ]]      && args+=("S_FROM=${QB_S_FROM}" "S_TO=${QB_S_TO}")
    [[ -n "${QB_LOOKUP_ORDER_ID:-}" ]] && args+=("LOOKUP_ORDER_ID=${QB_LOOKUP_ORDER_ID}" "LOOKUP_ORDER_DT=${QB_LOOKUP_ORDER_DT}")
    python3 - "$f" "$dialect" "${args[@]}" <<'EOF'
import re, sys
f, dialect = sys.argv[1], sys.argv[2]
kv = dict(a.split("=", 1) for a in sys.argv[3:])
BUILTIN = {"CUSTOMERS", "PRODUCTS", "ORDERS", "ORDER_ITEMS", "ORDERS_PART", "ORDER_ITEMS_PART", "HOT_DT", "HOT_FROM",
           "HOT_TO", "SNAPSHOT_DT", "S_FROM", "S_TO", "LOOKUP_ORDER_ID", "LOOKUP_ORDER_DT"}
for n, line in enumerate(open(dialect), 1):
    s = line.strip()
    if not s or s.startswith("#"):
        continue
    if s.startswith("export "):
        s = s[7:].lstrip()
    m = re.fullmatch(r"([A-Z][A-Z0-9_]*)=(.*)", s)
    if not m:
        sys.exit("  [querybench] %s:%d: not KEY=VALUE: %r" % (dialect, n, line.rstrip("\n")))
    k, v = m.group(1), m.group(2)
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
        v = v[1:-1]
    if k in BUILTIN:
        sys.exit("  [querybench] %s:%d: %s is a harness placeholder; a dialect key may not set it" % (dialect, n, k))
    kv[k] = v
sql = re.sub(r"\$\{([A-Za-z0-9_]+)\}", lambda m: kv.get(m.group(1), m.group(0)), open(f).read())
left = sorted(set(re.findall(r"\$\{[^}]*\}", sql)))
if left:
    sys.exit("  [querybench] unresolved in %s: %s (a new placeholder? add it to the dialect files or lib/querybench.sh qb_render)"
             % (f.rsplit("/", 1)[-1], " ".join(left)))
sys.stdout.write(sql if sql.endswith("\n") else sql + "\n")
EOF
}

# Per-query cap, the same on both engines, so a TIMEOUT means the same thing.
qb_query_timeout() { [[ "$1" == hot ]] && echo "${HOT_QUERY_TIMEOUT_S:-1800}" || echo "${SNAPSHOT_QUERY_TIMEOUT_S:-3600}"; }

qb_median() {
    sort -n | awk '{a[NR]=$1} END { if (NR==0) exit
        if (NR%2) printf "%d\n", a[(NR+1)/2]; else printf "%d\n", (a[NR/2]+a[NR/2+1])/2 }'
}

# One long-format CSV per engine and run, the same columns for both engines
# (= throughput-benchmark's).
# cold_ms: pass 0's time when COLD_PASS=1 (snapshot: the first pass after the
# caches were emptied); median_ms is then the median of the other passes.
QB_CSV_HEADER="stamp,engine,arm,suite,query,status,median_ms,result_rows,scan_rows,scan_bytes,cold_ms"
qb_csv_init() { mkdir -p "$(dirname "$1")"; [[ -f "$1" ]] || echo "$QB_CSV_HEADER" > "$1"; }

# ── Writers and generator ───────────────────────────────────────────────────
qb_writer_state() {   # [jobs; default QB_QUERY_WRITERS]
    local j out=""
    for j in ${1:-$QB_QUERY_WRITERS}; do
        out+="${j}=$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" -o jsonpath='{.spec.job.state}' 2>/dev/null || true) "
    done
    echo "${out% }"
}

# Suspend the CDC writers (FLINK_JOBS: both formats, so the Paimon job is not
# left writing alone) and wait until no job is RUNNING and no TaskManager is
# left, so the last commit has landed. upgradeMode is last-state in both
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
        [[ -z "$busy" ]] && { qb_log "writers stopped: $(qb_writer_state "$FLINK_JOBS")"; return 0; }
        (( $(date +%s) < deadline )) || qb_die "writers still running after 600s:${busy}"
        qb_log "waiting for writers to stop:${busy}"
        sleep 15
    done
}

# Completion time (epoch ms) of the newest completed checkpoint's TRIGGER of writer
# <job>, from its JobManager's REST API (kubectl exec; empty if not RUNNING / unknown).
qb_writer_last_checkpoint_ms() {   # <flinkdeployment>
    local j="$1" id jm
    id=$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" -o jsonpath='{.status.jobStatus.jobId}' 2>/dev/null)
    jm=$(kubectl get pods -n "$NAMESPACE" -l "app=${j},component=jobmanager" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    [[ -n "$id" && -n "$jm" ]] || return 0
    kubectl exec -n "$NAMESPACE" "$jm" -c flink-main-container -- \
        curl -s --max-time 20 "http://localhost:8081/jobs/${id}/checkpoints" 2>/dev/null \
        | python3 -c 'import json, sys
try:
    c = (json.load(sys.stdin).get("latest") or {}).get("completed") or {}
    print(c.get("trigger_timestamp") or "")
except Exception:
    print("")'
}

# Wait until every RUNNING writer (FLINK_JOBS) has COMPLETED a checkpoint triggered at
# least WRITER_CATCHUP_MARGIN_S (60) after <since> (the moment the generator stopped):
# its commit then covers the binlog tail, so suspending the writer leaves its tables
# equal to MySQL. Without it a last-state suspend keeps the tail uncommitted (it is
# replayed on resume); on 2026-11-02 that left both formats ~55 s behind MySQL and a
# MySQL-gated compaction correctly refused to commit. Gives up after
# WRITER_CATCHUP_TIMEOUT_S (900) with a warning and status 1.
qb_wait_writers_committed() {   # <since epoch s>
    local since="$1" margin="${WRITER_CATCHUP_MARGIN_S:-60}" j pending ts
    local deadline=$(( $(date +%s) + ${WRITER_CATCHUP_TIMEOUT_S:-900} ))
    while :; do
        pending=""
        for j in $FLINK_JOBS; do
            [[ "$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)" == RUNNING ]] || continue
            ts=$(qb_writer_last_checkpoint_ms "$j")
            [[ "$ts" =~ ^[0-9]+$ ]] && (( ts >= (since + margin) * 1000 )) || pending+=" ${j}"
        done
        [[ -z "$pending" ]] && { qb_log "writers committed a checkpoint >= ${margin}s after the generator stopped"; return 0; }
        (( $(date +%s) < deadline )) || { qb_log "WARNING: no checkpoint >= ${margin}s after the generator stopped within ${WRITER_CATCHUP_TIMEOUT_S:-900}s:${pending}"; return 1; }
        qb_log "waiting for a checkpoint after the generator stopped:${pending}"
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
# three 60 s checkpoints) before the writers are suspended, so the writers
# commit the binlog tail: a suspend keeps only the last checkpoint.
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

# ── Engine state ────────────────────────────────────────────────────────────
qb_duck_health() {   # -> /health JSON, or nothing
    kubectl exec -n "$NAMESPACE" "deploy/${DUCKDB_NAME}" -c duckdb -- python3 -c \
        'import json, urllib.request; print(json.dumps(json.load(urllib.request.urlopen("http://localhost:8080/health", timeout=10))))' 2>/dev/null || true
}
# Active Trino workers (system.runtime.nodes), or nothing if Trino does not answer.
qb_trino_workers() {
    { qb_trino "SELECT count(*) FROM system.runtime.nodes WHERE coordinator = false AND state = 'active'" 60 || true; } | tail -1 | tr -dc 0-9
}
qb_trino_ready() { local n; n=$(qb_trino_workers); [[ -n "$n" ]] && (( n >= TRINO_WORKERS )); }

# Queries on Trino other than this check itself (and other `system` reads):
# "<id> <state> <source> <user> <first 70 chars>" per line, or nothing.
qb_trino_busy() {
    { qb_trino "SELECT query_id, state, coalesce(source, '-'), \"user\", substr(regexp_replace(query, '\\s+', ' '), 1, 70)
                FROM system.runtime.queries
                WHERE state NOT IN ('FINISHED', 'FAILED') AND query NOT LIKE '%system.runtime.queries%'" 60 || true; } | tr '\t' ' '
}

# Checked on the CLUSTER, so it also catches a run from another laptop or the
# in-cluster runner:
#   - no other query on Trino; neither DuckDB server (this harness's and the
#     throughput benchmark's duckdb-warm, which share the bucket and on a small
#     nodepool the nodes) busy;
#   - no throughput-benchmark qb-runner active (same cluster, bucket, nodes);
#   - locally, no other driver process (<self> = duckdb-warm | trino).
# ALLOW_CONCURRENT=1 skips it.
qb_assert_alone() {   # <self: duckdb-warm|trino>
    [[ "${ALLOW_CONCURRENT:-0}" == 1 ]] && return 0
    local other busy a d
    case "$1" in
        trino)       other=$(pgrep -fl 'querybench/duckdb-warm\.sh run' | grep -v 'bash -c' || true) ;;
        duckdb-warm) other=$(pgrep -fl 'querybench/trino\.sh run' | grep -v 'bash -c' || true) ;;
        *)           qb_die "qb_assert_alone: unknown driver '$1'" ;;
    esac
    [[ -z "$other" ]] || qb_die "another benchmark driver is running (ALLOW_CONCURRENT=1 to override):
${other}"
    a=$(kubectl get job qb-runner -n "$NAMESPACE" -o jsonpath='{.status.active}' 2>/dev/null || true)
    [[ "${a:-0}" == 0 ]] || qb_die "throughput-benchmark's qb-runner is running on the same cluster (ALLOW_CONCURRENT=1 to override)"
    if kubectl get "$TRINO_COORDINATOR" -n "$NAMESPACE" >/dev/null 2>&1; then
        busy=$(qb_trino_busy)
        [[ -z "$busy" ]] || qb_die "Trino is running other queries (CALL system.runtime.kill_query('<id>') or ALLOW_CONCURRENT=1):
${busy}"
    fi
    for d in "$DUCKDB_NAME" duckdb-warm; do
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
# values come from snapshot.sh (exported SNAPSHOT_DT / S_FROM / S_TO); resolved
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

# The engines of a suite are ratioed against each other, so a writer change
# between them makes the suite meaningless: 0 = unchanged, else logged and 1.
qb_writers_unchanged() {   # <writer state at suite start> <suite>
    local now; now=$(qb_writer_state)
    [[ "$now" == "$1" ]] && return 0
    qb_log "writer state changed during the $2 suite (was: $1, now: ${now}); $2 not recorded"
    return 1
}

# Every QB_PIN_* variable set in this shell, one per line (for .pins files).
qb_pins_dump() { local v; for v in $(compgen -v | grep '^QB_PIN_' | sort); do echo "${v}=${!v}"; done; }

# Trino must read every pinned table of each arm in FORMATS at its snapshot
# (FOR VERSION AS OF, alias included, as the SQL writes `${ORDERS} AS o`). A
# coordinator whose view of the table is older than the pin ("Snapshot ... not
# found") is retried for up to 5 min. Iceberg: anything else stops the run.
# Paimon (the native connector, catalog QB_TRINO_PAIMON_CATALOG): logged as a
# WARNING, and the arm's queries then record the error, so the run goes on.
qb_trino_check_pins() {
    local arm t r try tries
    for arm in $(qb_formats); do
        for t in $QB_TABLES; do
            tries=10; [[ "$arm" == paimon ]] && tries=4
            for (( try = 1; try <= tries; try++ )); do
                r=$(qb_trino_try "SELECT 1 FROM $(qb_table_expr trino "$arm" "$t") AS q LIMIT 1")
                [[ "$r" == 0$'\t'* ]] && break
                qb_log "Trino pinned ${arm}.${t}: ${r#*$'\t'} (try ${try}/${tries}), retrying in 30s"
                sleep 30
            done
            [[ "$r" == 0$'\t'* ]] && continue
            if [[ "$arm" == paimon ]]; then
                qb_log "WARNING: Trino cannot read ${QB_TRINO_PAIMON_CATALOG}.${PAIMON_DB}.${t} (native Paimon) at its pinned snapshot: ${r#*$'\t'}; trino:paimon queries will record the error"
            else
                qb_die "Trino cannot read ${ICEBERG_DB}.${t} at its pinned snapshot: ${r#*$'\t'}"
            fi
        done
    done
}

# ── Snapshot gates (snapshot.sh) ─
# Each prints "<name>=<UTC time>" lines for the .pins file on stdout.

# Every engine in ENGINES up, waiting up to ENGINE_GATE_TIMEOUT_S (1800):
# duckdb-warm-cdc answering busy=false; Trino answering with TRINO_WORKERS
# active workers and no query running.
qb_engines_gate() {
    local deadline=$(( $(date +%s) + ${ENGINE_GATE_TIMEOUT_S:-1800} )) h n why
    while :; do
        why=""
        if qb_has_engine duckdb; then
            h=$(qb_duck_health)
            [[ "$h" == *'"busy": false'* ]] || why+=" ${DUCKDB_NAME} ${h:-down};"
        fi
        if qb_has_engine trino; then
            n=$(qb_trino_workers)
            [[ -n "$n" ]] && (( n >= TRINO_WORKERS )) || why+=" Trino ${n:-down}/${TRINO_WORKERS} workers active;"
        fi
        [[ -z "$why" ]] && { echo "gate_engines_ready_at=$(date -u '+%Y-%m-%d %H:%M:%SZ')"; return 0; }
        (( $(date +%s) < deadline )) || qb_die "engines not ready after ${ENGINE_GATE_TIMEOUT_S:-1800}s:${why}"
        qb_log "waiting for the engines:${why}"
        sleep 30
    done
}

