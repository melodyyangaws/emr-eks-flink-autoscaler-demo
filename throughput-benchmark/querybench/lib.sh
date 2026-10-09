#!/usr/bin/env bash
# Shared by starrocks.sh and duckdb.sh, sourced, never run. One copy of
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
#   ${HOT_DT} ${HOT_FROM_1M|5M|10M} ${HOT_TO}       hot windows
#   ${SNAPSHOT_DT} ${S4_FROM} ${S4_TO}              snapshot partition + S4 slice
# Every bound is a literal resolved ONCE per run, never a clock function in the
# SQL, so all arms and passes read the same rows. (The FE session timezone is
# +08:00 and the data is UTC, so NOW() would also be simply wrong.)
#
# File name = query name, lexical order = run order. A `-- bench: <flag>` line
# sets a per-query flag (allow-zero-rows, leading-col-is-label).

[[ -n "${_QUERYBENCH_LIB:-}" ]] && return 0
_QUERYBENCH_LIB=1

QB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${QB_DIR}/../.bench-env"

NAMESPACE="${NAMESPACE:-emr-flink}"
FE_POD="${FE_POD:-kube-starrocks-fe-0}"
AWS_REGION="${AWS_REGION:-us-west-2}"
: "${BUCKET_NAME:?BUCKET_NAME is not set (.bench-env)}"
TABLE_NAME="${TABLE_NAME:-cid_events}"
PAIMON_DB="${PAIMON_DB:-flink_cid_paimon_db}"
ICEBERG_DB="${ICEBERG_DB:-flink_cid_iceberg_db}"
PAIMON_TABLE_ROOT="${PAIMON_TABLE_ROOT:-s3://${BUCKET_NAME}/cid-warehouse-paimon/${PAIMON_DB}.db/${TABLE_NAME}}"
ICEBERG_TABLE_ROOT="${ICEBERG_TABLE_ROOT:-s3://${BUCKET_NAME}/cid-warehouse-iceberg/${ICEBERG_DB}.db/${TABLE_NAME}}"
# Paimon's Iceberg-compatibility export (ICEBERG_COMPAT=true in .bench-env).
PMNICE_TABLE_ROOT="${PMNICE_TABLE_ROOT:-s3://${BUCKET_NAME}/cid-warehouse-paimon/iceberg/${PAIMON_DB}/${TABLE_NAME}}"
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
qb_utc_minus_min() {
    date -u -v-"$1"M '+%Y-%m-%d %H:%M:%S' 2>/dev/null \
      || date -u -d "$1 minutes ago" '+%Y-%m-%d %H:%M:%S'
}
qb_date_plus_days() {
    date -u -j -v+"$2"d -f %Y-%m-%d "$1" +%Y-%m-%d 2>/dev/null \
      || date -u -d "$1 +$2 day" +%Y-%m-%d
}

# ── Windows ─────────────────────────────────────────────────────────────────
# Hot: the last 1/5/10 minutes of today's (live) partition. Each bound can be
# pinned with the same-named env var, e.g. to rerun an earlier window.
qb_resolve_hot() {
    QB_HOT_TO="${HOT_TO:-$(date -u '+%Y-%m-%d %H:%M:%S')}"
    QB_HOT_FROM_1M="${HOT_FROM_1M:-$(qb_utc_minus_min 1)}"
    QB_HOT_FROM_5M="${HOT_FROM_5M:-$(qb_utc_minus_min 5)}"
    QB_HOT_FROM_10M="${HOT_FROM_10M:-$(qb_utc_minus_min 10)}"
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
# S4 slices that partition by ingestionTime: rows of dt=D are ingested on D+1,
# so the slice is [D+1 04:30, D+1 05:00) unless SNAPSHOT_S4_FROM/TO move it.
qb_resolve_snapshot() {
    if [[ -z "${SNAPSHOT_DT:-}" ]]; then
        local p i
        p=$(_qb_dts_under "$PAIMON_TABLE_ROOT" | sort | head -1)
        i=$(_qb_dts_under "${ICEBERG_TABLE_ROOT}/data" | sort | head -1)
        [[ -n "$p" && -n "$i" ]] || qb_die "no dt= partitions under both table roots; set SNAPSHOT_DT"
        SNAPSHOT_DT=$([[ "$p" > "$i" ]] && echo "$p" || echo "$i")
    fi
    [[ "$SNAPSHOT_DT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || qb_die "SNAPSHOT_DT='${SNAPSHOT_DT}' is not YYYY-MM-DD"
    local ing
    ing=$(qb_date_plus_days "$SNAPSHOT_DT" 1)
    QB_SNAPSHOT_DT="$SNAPSHOT_DT"
    QB_S4_FROM="${ing} ${SNAPSHOT_S4_FROM:-04:30:00}"
    QB_S4_TO="${ing} ${SNAPSHOT_S4_TO:-05:00:00}"
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
              "HOT_FROM_1M=${QB_HOT_FROM_1M:-}" "HOT_FROM_5M=${QB_HOT_FROM_5M:-}" \
              "HOT_FROM_10M=${QB_HOT_FROM_10M:-}" "SNAPSHOT_DT=${QB_SNAPSHOT_DT:-}" \
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
# Hot windows are minutes of data; the snapshot partition is a full day.
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
# Writers are left suspended; resume with ../build-deploy-cid.sh.
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
