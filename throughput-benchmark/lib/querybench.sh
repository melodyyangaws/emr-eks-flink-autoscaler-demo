#!/usr/bin/env bash
# Shared by starrocks.sh and duckdb-warm.sh, sourced, never run. One copy of
# everything both engines must agree on: the query files, how they render, the
# windows and partition they read, the timeout, the median, the writer stop and
# the results format. If two engines disagreed on any of these, the comparison
# would measure that disagreement instead of the table formats.
#
# Placeholders in hot/*.sql and snapshot/*.sql:
#   ${TABLE}         the table expression — engine- and arm-specific
#   ${BYTES_FN}      LENGTH (StarRocks) / STRLEN (DuckDB): both count BYTES.
#                    DuckDB's LENGTH counts characters, and octet_length() is
#                    BLOB-only there.
#   ${HOT_DT} ${HOT_FROM} ${HOT_TO}                 hot window
#   ${SNAPSHOT_DT} ${S4_FROM} ${S4_TO}              snapshot partition + S4 slice (derived)
# Every bound is a literal resolved ONCE per run, never a clock function in the
# SQL, so all arms and passes read the same rows. (The FE session timezone is
# +08:00 and the data is UTC, so NOW() would also be simply wrong.)
#
# File name = query name, lexical order = run order. A `-- bench: <flag>` line
# sets a per-query flag (allow-zero-rows, leading-col-is-label).

[[ -n "${_QUERYBENCH_LIB:-}" ]] && return 0
_QUERYBENCH_LIB=1

# .bench-env, NAMESPACE, TEST_SUFFIX (checked), TEST_DB_SUFFIX, render.
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
# The queries (hot/, snapshot/) live in querybench/, beside the harnesses.
QB_DIR="${BENCH_DIR}/querybench"

FE_POD="${FE_POD:-kube-starrocks-fe-0}"
: "${BUCKET_NAME:?BUCKET_NAME is not set (.bench-env)}"
TABLE_NAME="${TABLE_NAME:-cid_events}"
# TEST_SUFFIX (.bench-env) selects the session's table set: its warehouses
# (cid-warehouse-<fmt><suffix>/) and Glue databases (flink_cid_<fmt>_db<_suffix>),
# as flinkwriter/run.sh writes them. Empty = the original set.
QB_DB_SUFFIX="$TEST_DB_SUFFIX"
PAIMON_DB="${PAIMON_DB:-flink_cid_paimon_db${QB_DB_SUFFIX}}"
ICEBERG_DB="${ICEBERG_DB:-flink_cid_iceberg_db${QB_DB_SUFFIX}}"
PAIMON_WAREHOUSE="${PAIMON_WAREHOUSE:-s3://${BUCKET_NAME}/cid-warehouse-paimon${TEST_SUFFIX}}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-s3://${BUCKET_NAME}/cid-warehouse-iceberg${TEST_SUFFIX}}"
PAIMON_TABLE_ROOT="${PAIMON_TABLE_ROOT:-${PAIMON_WAREHOUSE}/${PAIMON_DB}.db/${TABLE_NAME}}"
ICEBERG_TABLE_ROOT="${ICEBERG_TABLE_ROOT:-${ICEBERG_WAREHOUSE}/${ICEBERG_DB}.db/${TABLE_NAME}}"
# Paimon's Iceberg-compatibility export (ICEBERG_COMPAT=true in .bench-env).
PMNICE_TABLE_ROOT="${PMNICE_TABLE_ROOT:-${PAIMON_WAREHOUSE}/iceberg/${PAIMON_DB}/${TABLE_NAME}}"
FLINK_JOBS="${FLINK_JOBS:-flink-cid-paimon flink-cid-iceberg}"
SUITE="${SUITE:-all}"
RUNS="${RUNS:-3}"
WARMUP="${WARMUP:-1}"
OUT_DIR="${OUT_DIR:-${QB_DIR}/../bench-results}"
STAMP="${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"

qb_log() { echo "  [querybench] $*" >&2; }
qb_die() { echo "  [querybench] ERROR: $*" >&2; exit 1; }

case "$SUITE" in all|hot|snapshot) ;; *) qb_die "SUITE must be all|hot|snapshot, got '${SUITE}'" ;; esac
qb_suites() { case "$SUITE" in all) echo "hot snapshot" ;; *) echo "$SUITE" ;; esac; }

# The StarRocks FE's MySQL port; root has no password in this deployment.
qb_sr() {
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- \
        mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1" 2>/dev/null
}

# ── Dates (the harnesses run on macOS: BSD date first, GNU second) ──────────
# "<YYYY-MM-DD HH:MM:SS>" minus N seconds, both UTC.
qb_utc_minus_sec() {
    date -u -j -v-"$2"S -f '%Y-%m-%d %H:%M:%S' "$1" '+%Y-%m-%d %H:%M:%S' 2>/dev/null \
      || date -u -d "@$(( $(date -u -d "$1" +%s) - $2 ))" '+%Y-%m-%d %H:%M:%S'
}

# ── Windows ─────────────────────────────────────────────────────────────────
# Hot: the last HOT_WINDOW_SECONDS (30) of today's (live) partition, one window
# for every hot query. At ~3M rows/s that is ~90M rows, seconds per query; the
# old 1/5/10-minute windows were 180M-1.8B rows and took minutes. Each bound
# can be pinned with the same-named env var, e.g. to rerun an earlier window.
qb_resolve_hot() {
    local w="${HOT_WINDOW_SECONDS:-30}"
    [[ "$w" =~ ^[1-9][0-9]*$ ]] || qb_die "HOT_WINDOW_SECONDS='${w}' is not a positive integer"
    QB_HOT_TO="${HOT_TO:-$(date -u '+%Y-%m-%d %H:%M:%S')}"
    QB_HOT_FROM="${HOT_FROM:-$(qb_utc_minus_sec "$QB_HOT_TO" "$w")}"
    QB_HOT_DT="${HOT_DT:-${QB_HOT_TO%% *}}"
}

# dt partitions under an s3:// table root, one per line.
_qb_dts_under() {
    local p="${1#s3://${BUCKET_NAME}/}"
    aws s3api list-objects-v2 --bucket "$BUCKET_NAME" --region "$AWS_REGION" \
        --prefix "${p%/}/" --delimiter / --query 'CommonPrefixes[].Prefix' \
        --output text 2>/dev/null | tr '\t' '\n' | sed -n 's|.*/dt=\([^/]*\)/*$|\1|p'
}

# Snapshot: the OLDEST dt present in BOTH tables, i.e. the most-closed
# partition every arm can read. Not "yesterday": at benchmark time yesterday is
# often still being written (event time lags ingest), or does not exist yet.
#
# S4 slices that partition by eventTimeDT, a fixed SNAPSHOT_S4_MINUTES (3) wide.
# eventTimeDT is set by the GENERATOR, so both tables hold exactly the same rows
# in any eventTimeDT window, and dt is derived from it, so the window is always
# inside the partition. ingestionTime is each WRITER's processing time: the
# same ingestionTime window held 471M rows in Paimon and 480M in Iceberg
# (2026-10-17), which is not a like-for-like comparison. A row-sized slice was
# rejected for the same reason: the comparison wants the same rows in the same
# time slot, not the same share of each table.
# The window is centred in the span where BOTH tables were committing (their
# commit history), clipped to SNAPSHOT_DT's own day with QB_S4_MARGIN_S (120 s,
# two checkpoints) kept clear of each end. Event time runs ahead of commit time
# by the ingest lag, so the centre of the committed span is well inside the
# generator's run. At ~2.6M rows/s, 3 min is ~465M rows; S4's own n is the exact
# count and must match across arms. Writers must be stopped and drained (the
# snapshot rule), or the newest rows are not in every table yet.
# Pin it with SNAPSHOT_S4_FROM and SNAPSHOT_S4_TO, both 'YYYY-MM-DD HH:MM:SS'.
QB_S4_MARGIN_S=120

# "<commit_ms> <rows_added>" per commit of the Iceberg table, from the
# snapshots in its current metadata.json (Glue metadata_location).
_qb_commits_iceberg() {
    local loc
    loc=$(aws glue get-table --region "$AWS_REGION" --database-name "$ICEBERG_DB" --name "$TABLE_NAME" \
            --query 'Table.Parameters.metadata_location' --output text 2>/dev/null)
    [[ -n "$loc" && "$loc" != None ]] || return 1
    aws s3 cp --region "$AWS_REGION" "$loc" - 2>/dev/null | python3 -c '
import json, sys
for s in json.load(sys.stdin).get("snapshots", []):
    print(s["timestamp-ms"], s.get("summary", {}).get("added-records", 0))'
}

# The same for Paimon, from every RETAINED snapshot file (APPEND commits carry
# the rows; COMPACT ones add none). Expiry drops old snapshots, which can only
# move the start of the common span later, never the slice outside the data.
_qb_commits_paimon() {
    local snap="${PAIMON_TABLE_ROOT}/snapshot" d rc=0
    d=$(mktemp -d)
    aws s3 ls --region "$AWS_REGION" "${snap}/" 2>/dev/null | sed -n 's/.* \(snapshot-[0-9]*\)$/\1/p' \
        | xargs -P 8 -I{} aws s3 cp --quiet --region "$AWS_REGION" "${snap}/{}" "${d}/{}" 2>/dev/null || true
    ls "$d" | grep -q . || rc=1
    (( rc )) || python3 - "$d" <<'EOF'
import json, os, sys
for f in os.listdir(sys.argv[1]):
    j = json.load(open(os.path.join(sys.argv[1], f)))
    print(j["timeMillis"], j.get("deltaRecordCount", 0) if j.get("commitKind") == "APPEND" else 0)
EOF
    rm -rf "$d"
    return $rc
}

qb_resolve_snapshot() {
    if [[ -z "${SNAPSHOT_DT:-}" ]]; then
        local p i
        p=$(_qb_dts_under "$PAIMON_TABLE_ROOT" | sort | head -1)
        i=$(_qb_dts_under "${ICEBERG_TABLE_ROOT}/data" | sort | head -1)
        [[ -n "$p" && -n "$i" ]] || qb_die "no dt= partitions under both table roots; set SNAPSHOT_DT"
        SNAPSHOT_DT=$([[ "$p" > "$i" ]] && echo "$p" || echo "$i")
    fi
    [[ "$SNAPSHOT_DT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || qb_die "SNAPSHOT_DT='${SNAPSHOT_DT}' is not YYYY-MM-DD"
    QB_SNAPSHOT_DT="$SNAPSHOT_DT"

    local ts_re='^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
    if [[ -n "${SNAPSHOT_S4_FROM:-}${SNAPSHOT_S4_TO:-}" ]]; then
        [[ "${SNAPSHOT_S4_FROM:-}" =~ $ts_re && "${SNAPSHOT_S4_TO:-}" =~ $ts_re ]] \
            || qb_die "SNAPSHOT_S4_FROM and SNAPSHOT_S4_TO must both be set, as 'YYYY-MM-DD HH:MM:SS'"
        QB_S4_FROM="$SNAPSHOT_S4_FROM"; QB_S4_TO="$SNAPSHOT_S4_TO"; QB_S4_HOW="pinned"
        return 0
    fi

    local m="${SNAPSHOT_S4_MINUTES:-3}" d r
    [[ "$m" =~ ^[1-9][0-9]*$ ]] || qb_die "SNAPSHOT_S4_MINUTES='${m}' is not a positive integer"
    d=$(mktemp -d)
    _qb_commits_paimon  > "${d}/p" || { rm -rf "$d"; qb_die "no Paimon snapshots under ${PAIMON_TABLE_ROOT}/snapshot; set SNAPSHOT_S4_FROM/TO"; }
    _qb_commits_iceberg > "${d}/i" && [[ -s "${d}/i" ]] \
        || { rm -rf "$d"; qb_die "no Iceberg snapshots for ${ICEBERG_DB}.${TABLE_NAME}; set SNAPSHOT_S4_FROM/TO"; }
    r=$(python3 - "$SNAPSHOT_DT" "$m" "$QB_S4_MARGIN_S" "${d}/p" "${d}/i" <<'EOF'
import sys
from datetime import datetime, timezone
dt, mins, margin = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
day0 = datetime.strptime(dt, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
f = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
spans = []
for path in sys.argv[4:6]:
    t = sorted(int(l.split()[0]) / 1000 for l in open(path) if l.strip())
    spans.append((t[0], t[-1]))
lo = max(max(a for a, _ in spans), day0) + margin
hi = min(min(b for _, b in spans), day0 + 86400) - margin
span = "both committing, clipped to %s and less %ds each end: [%s, %s)" % (dt, margin, f(lo), f(hi))
if hi - lo < mins * 60:
    print("ERR %s is %.1f min, under SNAPSHOT_S4_MINUTES=%d" % (span, max(hi - lo, 0) / 60, mins)); sys.exit()
start = int((lo + hi) / 2 - mins * 30) // 60 * 60       # centred, on a whole minute
start = max(start, int(lo + 59) // 60 * 60)
print("OK|%s|%s|%d min of eventTimeDT, centred in the span %s" % (f(start), f(start + mins * 60), mins, span))
EOF
)
    rm -rf "$d"
    [[ "$r" == OK\|* ]] || qb_die "S4: ${r#ERR }; lower SNAPSHOT_S4_MINUTES or set SNAPSHOT_S4_FROM/TO"
    IFS='|' read -r _ QB_S4_FROM QB_S4_TO QB_S4_HOW <<<"$r"
}

# ── Queries ─────────────────────────────────────────────────────────────────
qb_query_files() { ls "${QB_DIR}/$1"/*.sql 2>/dev/null || qb_die "no .sql files in ${QB_DIR}/$1"; }
qb_query_name()  { basename "$1" .sql; }
qb_query_flag()  { grep -qE "^-- bench:.*\b$2\b" "$1"; }

# Render one file. QB_TABLE and QB_BYTES_FN come from the caller; the one
# placeholder named in QB_DEFER is left as-is for a later step (DuckDB resolves
# ${TABLE} in the pod). Any OTHER surviving ${...} is fatal: it is not a syntax error on
# either engine, it binds as an identifier or string and returns a wrong answer.
qb_render() {
    local content kv k left
    content=$(<"$1")
    for kv in "TABLE=${QB_TABLE:-}" "BYTES_FN=${QB_BYTES_FN:-}" \
              "HOT_DT=${QB_HOT_DT:-}" "HOT_TO=${QB_HOT_TO:-}" \
              "HOT_FROM=${QB_HOT_FROM:-}" "SNAPSHOT_DT=${QB_SNAPSHOT_DT:-}" \
              "S4_FROM=${QB_S4_FROM:-}" "S4_TO=${QB_S4_TO:-}"; do
        k="${kv%%=*}"
        [[ "$k" == "${QB_DEFER:-}" ]] && continue
        content="${content//\$\{$k\}/${kv#*=}}"
    done
    left=$(grep -oE '\$\{[A-Z0-9_]+\}' <<<"$content" | sort -u | grep -vxF "\${${QB_DEFER:-}}" || true)
    [[ -z "$left" ]] || { qb_log "unresolved in $(basename "$1"): $(echo $left)"; return 1; }
    printf '%s\n' "$content"
}

# One line, comments stripped: the StarRocks audit log logs a statement as one
# Stmt= field, and the runner pairs each with the marker sent before it.
qb_render_oneline() {
    local sql
    sql=$(qb_render "$1") || return 1
    sql=$(sed 's/--.*$//' <<<"$sql" | tr '\n' ' ' | tr -s ' ' | sed -e 's/^ *//' -e 's/ *;* *$/;/')
    printf '%s\n' "$sql"
}

# Per-query cap, the same on both engines so a TIMEOUT means the same thing.
# The hot window is seconds of data; the snapshot partition is a full day.
qb_query_timeout() { [[ "$1" == hot ]] && echo 1800 || echo 3600; }

# Median of the numbers on stdin; empty input prints nothing.
qb_median() {
    sort -n | awk '{a[NR]=$1} END { if (NR==0) exit
        if (NR%2) printf "%d\n", a[(NR+1)/2]; else printf "%d\n", (a[NR/2]+a[NR/2+1])/2 }'
}

# ── Results: one long-format CSV per run, the same columns for both engines ──
QB_CSV_HEADER="stamp,engine,arm,suite,query,status,median_ms,result_rows,scan_rows,scan_bytes"

# ── Writers ─────────────────────────────────────────────────────────────────
# The hot suite needs the Flink writers RUNNING (it measures fresh data); the
# snapshot suite needs them STOPPED: with writers live, the "closed" partition
# measured 89x slower from compaction and commit contention. So SUITE=all stops
# them too, after the hot suite has run. SUSPEND_WRITERS=0 skips the stop.
# Writers are left suspended; resume with flinkwriter/run.sh.
qb_writer_state() {
    local j out=""
    for j in $FLINK_JOBS; do
        out+="${j}=$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" \
                       -o jsonpath='{.spec.job.state}' 2>/dev/null || true) "
    done
    echo "${out% }"
}

# A cid_events query already running on the FE (an abandoned run keeps
# scanning after its harness is killed) is contention bigger than the format
# effect. An unreadable FE is not an idle one.
qb_assert_fe_idle() {
    local pl busy
    pl=$(qb_sr "SHOW PROCESSLIST") && [[ -n "$pl" ]] || qb_die "cannot read SHOW PROCESSLIST on ${FE_POD}"
    busy=$(awk -F'\t' '$10 != "SHOW PROCESSLIST" && ($10 ~ /cid_events/ || $5 ~ /^flink_cid_/) {print "    id=" $2 " " $8 "s " substr($10,1,70)}' <<<"$pl")
    [[ -z "$busy" ]] || qb_die "a cid_events query is already running on ${FE_POD} (KILL QUERY <id>):
${busy}"
}

# Suspend the writers and wait until no job is RUNNING and no TaskManager pod
# is left: only then has the last commit landed.
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

# ── Suite plumbing shared by both harnesses ─────────────────────────────────
# The results CSV, with its header when new.
qb_csv_init() { mkdir -p "$(dirname "$1")"; [[ -f "$1" ]] || echo "$QB_CSV_HEADER" > "$1"; }

# Start <suite> the same way on both engines: snapshot stops the writers first
# (it needs them stopped), then the window is resolved and logged in one format.
qb_start_suite() {   # <hot|snapshot>
    if [[ "$1" == hot ]]; then
        qb_resolve_hot
        qb_log "hot: dt=${QB_HOT_DT} [${QB_HOT_FROM}, ${QB_HOT_TO}) on eventTimeDT"
    else
        qb_stop_writers
        qb_resolve_snapshot
        qb_log "snapshot: dt=${QB_SNAPSHOT_DT} S4=[${QB_S4_FROM}, ${QB_S4_TO}) (${QB_S4_HOW:-})"
    fi
}

# The arms of a suite are ratioed against each other, so a writer change
# between them (~89x on snapshot) makes the suite meaningless. 0 = unchanged;
# otherwise it logs and returns 1, and the caller does not record the suite.
qb_writers_unchanged() {   # <writer state at suite start> <suite>
    local now; now=$(qb_writer_state)
    [[ "$now" == "$1" ]] && return 0
    qb_log "writer state changed during the $2 suite (was: $1, now: ${now}); $2 not recorded"
    return 1
}

# One engine at a time: the two share the bucket, and each side's numbers are
# only fair with the other idle. Checked on the CLUSTER, so it also catches a
# run from another laptop or the in-cluster runner (querybench/run.sh):
#   - no cid_events query on the StarRocks FE (qb_assert_fe_idle)
#   - the warm DuckDB server not busy (its /health, read inside the pod)
#   - locally, no other harness process (<self> = starrocks | duckdb-warm)
# Before 2026-10-19 starrocks.sh checked only the FE, and only before snapshot,
# so it would start while a DuckDB server was mid-query. ALLOW_CONCURRENT=1
# skips all of it.
qb_assert_alone() {   # <self: starrocks|duckdb-warm>
    [[ "${ALLOW_CONCURRENT:-0}" == 1 ]] && return 0
    local other busy
    case "$1" in
        starrocks)   other=$(pgrep -fl 'querybench/duckdb-warm\.sh run' | grep -v 'bash -c' || true) ;;
        duckdb-warm) other=$(pgrep -fl 'querybench/starrocks\.sh' | grep -v 'bash -c' || true) ;;
        *)           qb_die "qb_assert_alone: unknown harness '$1'" ;;
    esac
    [[ -z "$other" ]] || qb_die "another benchmark is running (ALLOW_CONCURRENT=1 to override):
${other}"
    qb_assert_fe_idle
    if kubectl get deploy duckdb-warm -n "$NAMESPACE" >/dev/null 2>&1; then
        busy=$(kubectl exec -n "$NAMESPACE" deploy/duckdb-warm -c duckdb -- python3 -c \
            'import json, urllib.request; print(json.load(urllib.request.urlopen("http://localhost:8080/health", timeout=10))["busy"])' 2>/dev/null || echo unreachable)
        case "$busy" in
            False)       ;;
            True)        qb_die "duckdb-warm is busy with another run (ALLOW_CONCURRENT=1 to override)" ;;
            *)           qb_log "duckdb-warm: busy state unreadable (${busy}); assuming idle" ;;
        esac
    fi
}
