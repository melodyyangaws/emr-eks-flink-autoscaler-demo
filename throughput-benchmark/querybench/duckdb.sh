#!/usr/bin/env bash
# DuckDB side of the query benchmark: hot/*.sql and snapshot/*.sql against the
# cid_events Iceberg and Paimon tables, one Job per arm on its own m5.8xlarge
# (CPU/RAM parity with StarRocks' 3 BEs, see duckdb-job.yaml).
#
# ONE file, TWO roles. On the laptop it renders the queries (lib.sh, so both
# engines read the same windows and partition), ships them plus itself in one
# ConfigMap, runs the Jobs and collects the results. In the pod (--in-pod) it
# fills in ${TABLE} per arm and per query, times each query and prints CSV rows.
#
#   ./duckdb.sh                      iceberg arm, SUITE=all
#   FORMAT=paimon|both ./duckdb.sh   both = one Job per arm, run in parallel
#   SUITE=hot|snapshot|all           all = hot with writers RUNNING, then stop
#                                    them (qb_stop_writers), then snapshot
#   RUNS=5 WARMUP=1                  timed passes / discarded passes per query
#   KEEP_JOBS=1                      keep finished Jobs (and their nodes)
#   ./duckdb.sh clean                delete the Jobs and ConfigMaps
#   ./duckdb.sh debug [arm]          an idle pod with the same spec, for
#                                    `duckdb.sh --shell` (prelude + secret loaded)
#
# Arms: iceberg = iceberg_scan over the Glue metadata_location (ICEBERG_DB can
# point at Paimon's Iceberg-compat export). paimon = read_parquet over the raw
# files: DuckDB has no Paimon reader, so that arm is a READ-PATH number, not a
# format one (no manifest planning, no stats pruning, only dt= pruning).
#
# Output: $OUT_DIR/duckdb-<STAMP>.csv ($QB_CSV_HEADER; scan_rows/scan_bytes are
# empty, DuckDB has no such counters) and duckdb-<arm>-<STAMP>.log per arm.
# Hot windows are pinned at render time, so a slow Karpenter launch ages them;
# all arms share them either way.

# ════════════════════════════════════════════════════════════════════════════
# IN-POD ROLE. No lib (it is not in the ConfigMap) and no -e: a failed or
# killed duckdb process is a RESULT (ERROR/TIMEOUT), not a reason to stop.
# ════════════════════════════════════════════════════════════════════════════
if [[ "${1:-}" == --in-pod || "${1:-}" == --shell ]]; then
set -uo pipefail
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${WORK:-/tmp/bench-work}"
PIN_PREFIX="duckdb-cid-pinned/${RUN_TAG:?}"

pod_init() {
    umask 077                                   # the secret file holds live keys
    mkdir -p "$WORK" "$SPILL_DIR"
    PRELUDE="${WORK}/prelude.sql"; SECRET="${WORK}/secret.sql"
    # memory_limit is below the 72Gi container limit: DuckDB does not account
    # for httpfs/Arrow buffers, so at-parity it gets OOM-killed. temp_directory
    # must be explicit: an in-memory database has none, and an over-budget
    # query is then SIGKILLed instead of spilling.
    cat > "$PRELUDE" <<EOF
SET memory_limit='${DUCKDB_MEMORY_LIMIT}';
SET threads=${DUCKDB_THREADS};
SET temp_directory='${SPILL_DIR}';
SET max_temp_directory_size='${DUCKDB_MAX_TEMP_SIZE}';
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;
EOF
    trap unpin EXIT
}

# Pinned metadata copies are only needed while this pod runs.
unpin() {
    aws s3 rm --recursive --quiet "s3://${BUCKET_NAME}/${PIN_PREFIX}/" >/dev/null 2>&1 || true
}

# DuckDB's credential_chain cannot use IRSA (CHAIN 'sts' and 'config' both
# fail in this pod), so the session comes from the CLI and goes into a SECRET.
# Called before EVERY attempt: an IRSA session is ~1h, a snapshot pass up to
# ~25 min, and a secret minted once expired mid-run (ExpiredToken).
refresh_secret() {
    local ak="" sk="" tok=""
    read -r ak sk tok < <(aws sts assume-role-with-web-identity \
        --role-arn "${AWS_ROLE_ARN:?IRSA not configured}" \
        --role-session-name "duckdb-${RUN_TAG}" \
        --web-identity-token "$(<"${AWS_WEB_IDENTITY_TOKEN_FILE:?IRSA not configured}")" \
        --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text 2>/dev/null)
    [[ -n "$tok" ]] || { echo "ERROR: assume-role-with-web-identity failed" >&2; return 1; }
    printf "CREATE OR REPLACE SECRET irsa (TYPE s3, KEY_ID '%s', SECRET '%s', SESSION_TOKEN '%s', REGION '%s');\n" \
        "$ak" "$sk" "$tok" "$AWS_REGION" > "$SECRET"
}

# The scan expression ${TABLE} becomes, resolved per query: the writer commits
# a new metadata.json every ~60s and keeps ~4, so a location resolved at start
# is gone minutes later. PIN_SNAPSHOT=1 copies it to a key this pod owns,
# because even a fresh location is GC'd within ~60s and 404s mid-scan; the
# copy is ordinary snapshot isolation (manifests and data are retained ~1h).
table_expr() {
    if [[ "$ARM" == paimon ]]; then
        # hive_partitioning makes dt a column (and prunes on it). No
        # union_by_name: it reads EVERY file's footer at bind, before dt
        # pruning, and the table has one schema version anyway.
        echo "read_parquet('${PAIMON_TABLE_ROOT}/dt=*/bucket-*/*.parquet', hive_partitioning=1)"
        return
    fi
    local loc
    loc=$(aws glue get-table --database-name "$ICEBERG_DB" --name "$TABLE_NAME" \
            --query 'Table.Parameters.metadata_location' --output text 2>/dev/null)
    [[ -n "$loc" && "$loc" != None ]] || { echo "ERROR: no metadata_location on ${ICEBERG_DB}.${TABLE_NAME}" >&2; return 1; }
    if [[ "$PIN_SNAPSHOT" == 1 ]]; then
        aws s3api copy-object --bucket "$BUCKET_NAME" --key "${PIN_PREFIX}/${loc##*/}" \
            --copy-source "${loc#s3://}" >/dev/null 2>&1 || { echo "ERROR: cannot pin ${loc}" >&2; return 1; }
        loc="s3://${BUCKET_NAME}/${PIN_PREFIX}/${loc##*/}"
    fi
    echo "iceberg_scan('${loc}')"
}

# One timed attempt of $1, result rows to $2; prints elapsed ms, returns the
# duckdb exit code (137 = killed by the timeout). Timed around the process,
# prelude included: it is constant across arms and passes. `.output` comes
# after the prelude and secret, because CREATE SECRET prints a "true" row that
# would otherwise be read as the query's result (and disarm the guard below).
run_attempt() {
    local st en rc
    { cat "$PRELUDE" "$SECRET"; printf '.output %s\n' "$2"; cat "$1"; } > "${WORK}/attempt.sql"
    : > "$2"
    st=$(date +%s%N)
    timeout --signal=KILL "$QUERY_TIMEOUT_S" \
        duckdb -init /dev/null -bail -noheader -list -f "${WORK}/attempt.sql" >/dev/null 2>"${2}.err"
    rc=$?
    en=$(date +%s%N)
    echo $(( (en - st) / 1000000 ))
    return $rc
}

# A copy of lib.sh's qb_median (the pod has no lib). Keep the two identical.
median() {
    sort -n | awk '{a[NR]=$1} END { if (NR==0) exit
        if (NR%2) printf "%d\n", a[(NR+1)/2]; else printf "%d\n", (a[NR/2]+a[NR/2+1])/2 }'
}

flag() { grep -qE "^-- bench:.*\b$2\b" "$1"; }   # = lib.sh qb_query_flag

pod_run() {
    local f name tbl sql status times rows first v ms rc i med out="${WORK}/out.txt"
    local results="${WORK}/results.csv"
    pod_init; : > "$results"
    echo "== duckdb $(duckdb --version 2>/dev/null) arm=${ARM} suite=${SUITE} runs=${RUNS}+${WARMUP} warmup"
    echo "   timeout=${QUERY_TIMEOUT_S}s memory=${DUCKDB_MEMORY_LIMIT} threads=${DUCKDB_THREADS} pin=${PIN_SNAPSHOT}"
    for f in "$BENCH_DIR"/*_"${SUITE}"_*.sql; do        # NN_ prefix = run order
        [[ -f "$f" ]] || { echo "ERROR: no ${SUITE} queries in ${BENCH_DIR}" >&2; exit 1; }
        name=$(basename "$f" .sql); name="${name#*_"${SUITE}"_}"
        status=OK; times=""; rows=""; first=""
        if tbl=$(table_expr); then
            sql=$(<"$f"); sql=${sql//'${TABLE}'/"$tbl"}   # quoted: & stays literal
            printf '%s\n' "$sql" > "${WORK}/q.sql"
            if grep -qE '\$\{[A-Z0-9_]+\}' "${WORK}/q.sql"; then
                echo "  ${name}: unresolved placeholder" >&2; status=ERROR
            fi
        else
            status=ERROR
        fi
        echo "  ${name}: ${tbl:-}"
        for (( i = 1; i <= WARMUP + RUNS; i++ )); do
            [[ "$status" == OK ]] || break
            refresh_secret || { status=ERROR; break; }
            ms=$(run_attempt "${WORK}/q.sql" "$out"); rc=$?
            if (( rc == 137 )); then
                status=TIMEOUT; echo "  ${name}: TIMEOUT after ${QUERY_TIMEOUT_S}s"
            elif (( rc != 0 )); then
                status=ERROR; echo "  ${name}: ERROR $(head -3 "${out}.err" | tr '\n' ' ')"
            elif (( i > WARMUP )); then
                times+="${ms}"$'\n'
                rows=$(grep -c . "$out"); first=$(head -1 "$out")
                printf '  %-22s run %d: %9s ms\n' "$name" "$(( i - WARMUP ))" "$ms"
            fi
        done
        # An empty scan is the fastest possible result, so it must be flagged,
        # never reported as a win. An aggregate always returns one row, so the
        # leading value is checked too, unless the file says it is a label.
        if [[ "$status" == OK ]] && ! flag "$f" allow-zero-rows; then
            v="${first%%|*}"
            if [[ "${rows:-0}" == 0 ]] || { ! flag "$f" leading-col-is-label && [[ "$v" == 0 || "$v" == NULL || -z "$v" ]]; }; then
                status=ZERO_SCAN; echo "  ${name}: ZERO_SCAN, the predicate matched nothing: NOT a win"
            fi
        fi
        # No timings, no median: a 0 here would read as the fastest result.
        med=""; [[ -n "$times" ]] && med=$(printf '%s' "$times" | median)
        [[ -z "$med" && "$status" == OK ]] && status=ERROR
        printf '  %-22s MEDIAN %9s ms  %s  first_row=%s\n' "$name" "${med:--}" "$status" "${first:0:80}"
        printf '%s,duckdb,%s,%s,%s,%s,%s,%s,,\n' \
            "$STAMP" "$ARM" "$SUITE" "$name" "$status" "$med" "$rows" >> "$results"
    done
    unpin
    echo "===CSV==="                      # the submitter takes the rows after this
    cat "$results"
    awk -F, '$6 != "OK" {bad=1} END {exit bad}' "$results"
}

# Interactive: the same prelude and a fresh secret, plus a view `t` over this
# arm's scan expression. Queries: sed 's/${TABLE}/t/g' /opt/bench/NN_*.sql.
pod_shell() {
    local tbl init="${WORK}/init.sql"
    pod_init
    refresh_secret || exit 1
    tbl=$(table_expr) || exit 1
    { cat "$PRELUDE" "$SECRET"; echo "CREATE VIEW t AS SELECT * FROM ${tbl};"; } > "$init"
    echo "view t = ${tbl}   (credentials last ~1h: rerun --shell to refresh)"
    duckdb -init "$init"
}

if [[ "$1" == --shell ]]; then pod_shell; else pod_run; fi
exit $?
fi

# ════════════════════════════════════════════════════════════════════════════
# SUBMITTER ROLE (laptop, bash 3.2)
# ════════════════════════════════════════════════════════════════════════════
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh"

FORMAT="${FORMAT:-iceberg}"
CM="${CM:-duckdb-bench}"
DUCKDB_VERSION="${DUCKDB_VERSION:-1.5.6}"
DUCKDB_MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-56GB}"    # < 72Gi limit, see pod_init
DUCKDB_THREADS="${DUCKDB_THREADS:-18}"                 # = the cpu limit
SPILL_DIR="${SPILL_DIR:-/tmp/duckdb-spill}"            # the 200Gi scratch volume
DUCKDB_MAX_TEMP_SIZE="${DUCKDB_MAX_TEMP_SIZE:-180GB}"  # < 200Gi, so spill cannot fill it
PIN_SNAPSHOT="${PIN_SNAPSHOT:-1}"                      # iceberg only, see table_expr
SCHED_GRACE="${SCHED_GRACE:-900}"                      # Karpenter launch + join
CSV="${OUT_DIR}/duckdb-${STAMP}.csv"
QB_DEFER=TABLE        # resolved in the pod, per arm and per query
QB_BYTES_FN=STRLEN    # bytes, like StarRocks' LENGTH (DuckDB's LENGTH counts characters)
TPL_VARS='${JOB_NAME} ${NAMESPACE} ${CM} ${POD_CMD} ${DUCKDB_VERSION} ${AWS_REGION} ${BUCKET_NAME} ${ARM} ${SUITE} ${STAMP} ${RUN_TAG} ${RUNS} ${WARMUP} ${QUERY_TIMEOUT_S} ${ICEBERG_DB} ${TABLE_NAME} ${PAIMON_TABLE_ROOT} ${PIN_SNAPSHOT} ${DUCKDB_MEMORY_LIMIT} ${DUCKDB_THREADS} ${SPILL_DIR} ${DUCKDB_MAX_TEMP_SIZE}'

case "$FORMAT" in
    iceberg|paimon) ARMS="$FORMAT" ;;
    both)           ARMS="iceberg paimon" ;;
    *) qb_die "FORMAT must be iceberg|paimon|both, got '${FORMAT}'" ;;
esac

# Render every suite's queries into $1 as <NN>_<suite>_<name>.sql, leaving only
# ${TABLE} for the pod. All windows are resolved here, once, for every arm.
render_queries() {
    local dir="$1" n=0 s f
    for s in $(qb_suites); do
        if [[ "$s" == hot ]]; then
            qb_resolve_hot
            qb_log "hot: dt=${QB_HOT_DT} [${QB_HOT_FROM_10M} | ${QB_HOT_FROM_5M} | ${QB_HOT_FROM_1M}, ${QB_HOT_TO})"
        else
            qb_resolve_snapshot
            qb_log "snapshot: dt=${QB_SNAPSHOT_DT} S4=[${QB_S4_FROM}, ${QB_S4_TO})"
        fi
        for f in $(qb_query_files "$s"); do
            n=$((n + 1))
            qb_render "$f" \
                > "${dir}/$(printf '%02d' "$n")_${s}_$(qb_query_name "$f").sql" || qb_die "render failed: ${f}"
        done
    done
}

# One ConfigMap: the rendered queries plus this script. Replaced, not applied:
# apply's last-applied annotation is capped at 256KB.
ship_configmap() {
    local dir
    dir=$(mktemp -d)
    render_queries "$dir"
    cp "${BASH_SOURCE[0]}" "${dir}/duckdb.sh"
    kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl create configmap "$CM" -n "$NAMESPACE" --from-file="$dir" >/dev/null
    qb_log "configmap/${CM}: $(ls "$dir" | tr '\n' ' ')"
    rm -rf "$dir"
}

# Job manifest on stdout. $1 arm, $2 suite, $3 job name, $4 pod command.
render_job() {
    local y
    y=$(export JOB_NAME="$3" POD_CMD="$4" ARM="$1" SUITE="$2" \
               RUN_TAG="${STAMP}-$1-$2" QUERY_TIMEOUT_S="$(qb_query_timeout "$2")" \
               NAMESPACE CM DUCKDB_VERSION AWS_REGION BUCKET_NAME STAMP RUNS WARMUP \
               ICEBERG_DB TABLE_NAME PAIMON_TABLE_ROOT PIN_SNAPSHOT \
               DUCKDB_MEMORY_LIMIT DUCKDB_THREADS SPILL_DIR DUCKDB_MAX_TEMP_SIZE
        envsubst "$TPL_VARS" < "${HERE}/duckdb-job.yaml")
    ! grep -n '\${' <<<"$y" >&2 || qb_die "unrendered placeholder in duckdb-job.yaml"
    printf '%s\n' "$y"
}

# Create the Job and print its pod name once it is Running (or already done).
start_job() {   # $1 arm, $2 suite, $3 job name, $4 pod command
    local pod="" phase="" deadline=$(( $(date +%s) + SCHED_GRACE ))
    local y
    y=$(render_job "$@") || return 1
    kubectl delete job "$3" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null || true
    kubectl create -f - <<<"$y" >/dev/null || return 1
    while :; do
        pod=$(kubectl get pods -n "$NAMESPACE" -l "job-name=$3" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
        [[ -n "$pod" ]] && phase=$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
        case "$phase" in Running|Succeeded|Failed) echo "$pod"; return 0 ;; esac
        if (( $(date +%s) >= deadline )); then
            [[ -n "$pod" ]] && kubectl get pod "$pod" -n "$NAMESPACE" \
                -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].message}{"\n"}' >&2
            qb_log "job/$3 not Running after ${SCHED_GRACE}s: no on-demand m5.8xlarge, or the nodepool's cpu limit"
            qb_log "  is used up (often by the Flink TaskManagers). kubectl get nodeclaims; retry, don't relax the pins"
            [[ "${KEEP_JOBS:-0}" == 1 ]] || kubectl delete job "$3" -n "$NAMESPACE" --wait=false >/dev/null
            return 1
        fi
        sleep 10
    done
}

# One arm of one suite, end to end. $3=1 streams the log to the terminal.
run_arm() {   # $1 arm, $2 suite, $3 stream
    local job="duckdb-$1" log="${OUT_DIR}/duckdb-$1-${STAMP}.log" part pod rows bad rc=0
    part=$(mktemp)
    qb_log "[$1/$2] job/${job} submitted"
    pod=$(start_job "$1" "$2" "$job" "bash /opt/bench/duckdb.sh --in-pod") || return 1
    qb_log "[$1/$2] pod/${pod} running"
    if [[ "$3" == 1 ]]; then
        kubectl logs -f "$pod" -n "$NAMESPACE" 2>&1 | tee "$part" || true
    else
        kubectl logs -f "$pod" -n "$NAMESPACE" > "$part" 2>&1 || true
    fi
    # A follow can drop on a multi-hour run: wait for the end, then refetch it all.
    while [[ "$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null)" == Running ]]; do sleep 15; done
    kubectl logs "$pod" -n "$NAMESPACE" > "$part" 2>&1 || true
    { echo "# ---- arm=$1 suite=$2 job=${job} writers: $(qb_writer_state)"; cat "$part"; } >> "$log"
    if grep -qx '===CSV===' "$part"; then
        sed '1,/^===CSV===$/d' "$part" | grep -E "^${STAMP},duckdb," > "${part}.rows" || true
        cat "${part}.rows" >> "$CSV"
        rows=$(grep -c . "${part}.rows" || true)
        bad=$(grep -cv ',OK,' "${part}.rows" || true)
        qb_log "[$1/$2] done: ${rows} queries, ${bad} not OK. log: ${log}"
        (( bad == 0 && rows > 0 )) || rc=1
    else
        qb_log "[$1/$2] FAILED: no ===CSV=== in the log (pod crashed, OOM or never started). log: ${log}"
        rc=1
    fi
    rm -f "$part" "${part}.rows"
    if [[ "${KEEP_JOBS:-0}" == 1 ]]; then
        qb_log "[$1/$2] KEEP_JOBS=1: job/${job} kept; it holds an m5.8xlarge until deleted"
    else
        kubectl delete job "$job" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null
    fi
    return $rc
}

# All arms of one suite. More than one arm always runs in parallel: hot arms
# must (serial arms would read a live table an hour apart), and snapshot arms
# read frozen data, so running them together costs only S3 bandwidth.
run_suite() {   # $1 suite
    local arm pids="" pid failed=""
    set -- "$1" $ARMS
    local suite="$1"; shift
    if (( $# == 1 )); then
        run_arm "$1" "$suite" 1 || failed="$1"
    else
        for arm in "$@"; do run_arm "$arm" "$suite" 0 & pids="${pids} $!:${arm}"; done
        for pid in $pids; do wait "${pid%%:*}" || failed="${failed} ${pid#*:}"; done
    fi
    [[ -z "$failed" ]] || { qb_log "${suite}: failed arms:${failed}"; return 1; }
}

cmd_run() {
    local s rc=0
    command -v envsubst >/dev/null || qb_die "envsubst not found (brew install gettext)"
    case " $ARMS " in *" iceberg "*)
        aws glue get-table --database-name "$ICEBERG_DB" --name "$TABLE_NAME" --region "$AWS_REGION" >/dev/null 2>&1 \
            || qb_die "Glue table ${ICEBERG_DB}.${TABLE_NAME} not found (deploy the writer first)" ;;
    esac
    mkdir -p "$OUT_DIR"
    [[ -f "$CSV" ]] || echo "$QB_CSV_HEADER" > "$CSV"
    qb_log "duckdb: arms=${ARMS} suite=${SUITE} runs=${RUNS}+${WARMUP} stamp=${STAMP}"
    ship_configmap
    for s in $(qb_suites); do
        [[ "$s" == snapshot ]] && qb_stop_writers        # hot needs them RUNNING
        run_suite "$s" || rc=1
    done
    qb_log "results: ${CSV}"
    return $rc
}

cmd_debug() {   # $1 arm
    local arm="${1:-${ARMS%% *}}" job pod
    case "$arm" in iceberg|paimon) ;; *) qb_die "debug arm must be iceberg|paimon" ;; esac
    job="duckdb-debug-${arm}"
    ship_configmap
    pod=$(start_job "$arm" debug "$job" "sleep infinity") || exit 1
    cat <<EOF
pod/${pod} is up (pip install takes ~30s more). A DuckDB shell with this arm's
prelude, a fresh S3 secret and a view t over the scan expression:
  kubectl exec -it -n ${NAMESPACE} ${pod} -- bash /opt/bench/duckdb.sh --shell
Or a bash shell (then run the line above without kubectl): kubectl exec -it -n ${NAMESPACE} ${pod} -- bash
Queries are /opt/bench/NN_<suite>_<name>.sql; replace \${TABLE} with t.
It holds an m5.8xlarge: ./duckdb.sh clean when done.
EOF
}

cmd_clean() {
    kubectl delete job -n "$NAMESPACE" -l app=duckdb-bench --ignore-not-found --wait=false
    # Legacy names from duckdb-cid/run-duckdb-cid.sh, which may still be on the cluster.
    kubectl delete job -n "$NAMESPACE" duckdb-cid-iceberg duckdb-cid-paimon --ignore-not-found --wait=false
    kubectl delete configmap -n "$NAMESPACE" "$CM" duckdb-cid-bench duckdb-cid-bench-sql duckdb-cid-bench-py --ignore-not-found
}

case "${1:-run}" in
    run)   cmd_run ;;
    debug) cmd_debug "${2:-}" ;;
    clean) cmd_clean ;;
    *)     qb_die "usage: $0 [run|debug [arm]|clean]" ;;
esac
