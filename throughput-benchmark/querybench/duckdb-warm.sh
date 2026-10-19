#!/usr/bin/env bash
# Warm DuckDB, the like-for-like counterpart of the always-up StarRocks cluster.
# The old cold harness (duckdb.sh, removed 2026-10-18) started a fresh duckdb process per attempt (cold: extensions, secret,
# metadata.json and every manifest re-read from S3), which put a 20-30 s floor
# under every hot query that StarRocks, warm and timed server-side, never pays.
# Here each suite gets a long-lived server (duckdb_warm_server.py) that keeps one
# DuckDB process, its S3 secret and a block cache sized like StarRocks' datacache.
#
#   SUITE=hot ./duckdb-warm.sh up               start the server(s) for SUITE only (hot ->
#                                               duckdb-warm-hot); or up hot|snapshot|both
#   ./duckdb-warm.sh status                     health, busy flag, cache size
#   SUITE=hot|snapshot|all ./duckdb-warm.sh run run the suite(s) on the server(s)
#   ./duckdb-warm.sh clear-cache [suite]        empty the block cache (cold start); default: SUITE
#   ./duckdb-warm.sh down [hot|snapshot|both]   delete the server(s) and their cache
#
# Parity with StarRocks (checked on the cluster 2026-10-13, StarRocks 4.1.4):
#   CPU/RAM   3 BEs x 6 cpu / 24Gi limit = 18 cpu / 72Gi  -> one 18 cpu / 72Gi pod, m5
#   cache     3 x 150 GiB disk + 3 x ~4.2 GB memory        -> CACHE_DISK_BYTES on disk + CACHE_MEM_BYTES
#             (both engines' default: cache on). DuckDB's disk is gp3-fast (1000 MB/s,
#             gp3-fast-sc.yaml): on default gp3 (125 MB/s) cache_httpfs served hits from
#             disk slower than S3 and, unlike StarRocks' io adaptor, cannot fall back to S3.
#             CACHE_MODE=in_mem keeps only the memory cache.
#   process   always up, warm                             -> always up, warm
#   timing    server-side, audit log, planning included   -> in-process, execute + fetch
#   arms      serial, one cluster                         -> serial, one process per suite
#   SQL       lib.sh renders the same files and windows for both engines
# Same RUNS/WARMUP, median, timeouts (qb_query_timeout) and ZERO_SCAN guard as
# the old duckdb.sh. Two pods, but never two suites at once: hot runs with the writers
# live and snapshot with them stopped, and `run` refuses to start while another
# benchmark (either engine) is running, unless ALLOW_CONCURRENT=1.
#
# Output: $OUT_DIR/duckdb_warm-<STAMP>.csv ($QB_CSV_HEADER, engine=duckdb_warm)
# and duckdb_warm-<STAMP>.log (every pass, the pinned scan, the first row).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh"

FORMAT="${FORMAT:-both}"
CM="${CM:-duckdb-warm}"
DUCKDB_VERSION="${DUCKDB_VERSION:-1.5.6}"           # latest stable; 2.0 is dev builds only (2026-10)
DUCKDB_THREADS="${DUCKDB_THREADS:-18}"               # = the cpu limit
# 44GB + the 11.7 GB cache memory (not counted by DuckDB) stays under the 72Gi
# limit with room for httpfs/Arrow buffers, which DuckDB does not count either.
DUCKDB_MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-44GB}"
DUCKDB_MAX_TEMP_SIZE="${DUCKDB_MAX_TEMP_SIZE:-180GB}"
CACHE_DISK_BYTES="${CACHE_DISK_BYTES:-483183820800}" # 3 x 161061273600 = BE block_cache_disk_quota_bytes
CACHE_MEM_BYTES="${CACHE_MEM_BYTES:-12524124633}"    # 3 x 4174708211 = BE page_cache_mem_quota_bytes
CACHE_MODE="${CACHE_MODE:-on_disk}"                  # on_disk | in_mem, see duckdb_warm_server.py
CACHE_STORAGE_CLASS="${CACHE_STORAGE_CLASS:-gp3-fast}"  # gp3-fast-sc.yaml; plain gp3 caps the cache at 125 MB/s
case "$CACHE_MODE" in
    in_mem)  CACHE_VOLUME="${CACHE_VOLUME:-1Gi}" ;;     # unused, the template still mounts it
    on_disk) CACHE_VOLUME="${CACHE_VOLUME:-500Gi}" ;;   # 450 GiB cap + filesystem overhead
    *) qb_die "CACHE_MODE must be in_mem|on_disk, got '${CACHE_MODE}'" ;;
esac
PIN_SNAPSHOT="${PIN_SNAPSHOT:-1}"
READY_GRACE="${READY_GRACE:-1200}"                   # Karpenter launch + pip + extensions
POLL_S=5
CSV="${OUT_DIR}/duckdb_warm-${STAMP}.csv"
LOG="${OUT_DIR}/duckdb_warm-${STAMP}.log"
QB_DEFER=TABLE        # the server resolves it per arm and query (pinned metadata.json)
QB_BYTES_FN=STRLEN    # bytes, like StarRocks' LENGTH (DuckDB's LENGTH counts characters)
TPL_VARS='${NAME} ${NAMESPACE} ${SUITE} ${CM} ${DUCKDB_VERSION} ${AWS_REGION} ${BUCKET_NAME} ${ICEBERG_DB} ${TABLE_NAME} ${PMNICE_TABLE_ROOT} ${PIN_SNAPSHOT} ${DUCKDB_MEMORY_LIMIT} ${DUCKDB_THREADS} ${DUCKDB_MAX_TEMP_SIZE} ${CACHE_MODE} ${CACHE_STORAGE_CLASS} ${CACHE_DISK_BYTES} ${CACHE_MEM_BYTES} ${CACHE_VOLUME}'

case "$FORMAT" in
    iceberg|paimon) ARMS="$FORMAT" ;;
    both)           ARMS="paimon iceberg" ;;            # StarRocks' order: paimon first
    *) qb_die "FORMAT must be iceberg|paimon|both, got '${FORMAT}'" ;;
esac

name_of() { echo "duckdb-warm-$1"; }
port_of() { [[ "$1" == hot ]] && echo 18181 || echo 18182; }
suites_arg() {
    case "${1:-both}" in hot|snapshot) echo "$1" ;; both|all) echo "hot snapshot" ;;
        *) qb_die "suite must be hot|snapshot|both, got '$1'" ;; esac
}

# ── Port-forwards (one per suite, restarted if they drop) ───────────────────
PF_PIDS=""
cleanup() { local p; for p in $PF_PIDS; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

pf_up() {   # $1 suite
    local port; port=$(port_of "$1")
    curl -sf -m 5 "localhost:${port}/health" >/dev/null 2>&1 && return 0
    kubectl port-forward -n "$NAMESPACE" "deploy/$(name_of "$1")" "${port}:8080" >/dev/null 2>&1 &
    PF_PIDS="${PF_PIDS} $!"
    local i; for i in $(seq 1 30); do
        curl -sf -m 5 "localhost:${port}/health" >/dev/null 2>&1 && return 0; sleep 1
    done
    return 1
}
api() {     # $1 suite, $2 GET|POST, $3 path, [$4 json body] -> body on stdout
    local port i body="${4:-}"; port=$(port_of "$1"); [[ -n "$body" ]] || body='{}'
    for i in 1 2 3; do
        if [[ "$2" == GET ]]; then curl -s -m 30 "localhost:${port}$3" && return 0
        else curl -s -m 30 -X POST -H 'Content-Type: application/json' --data-binary "$body" "localhost:${port}$3" && return 0
        fi
        pf_up "$1" || true
    done
    return 1
}
jget() { python3 -c 'import json,sys; d=json.load(sys.stdin); v=d.get(sys.argv[1]); print("" if v is None else v)' "$1"; }

# ── up / down / status ──────────────────────────────────────────────────────
render_server() {   # $1 suite
    local y
    y=$(export NAME="$(name_of "$1")" SUITE="$1" NAMESPACE CM DUCKDB_VERSION AWS_REGION BUCKET_NAME \
               ICEBERG_DB TABLE_NAME PMNICE_TABLE_ROOT PIN_SNAPSHOT DUCKDB_MEMORY_LIMIT DUCKDB_THREADS \
               DUCKDB_MAX_TEMP_SIZE CACHE_MODE CACHE_STORAGE_CLASS CACHE_DISK_BYTES CACHE_MEM_BYTES CACHE_VOLUME
        envsubst "$TPL_VARS" < "${HERE}/duckdb-warm.yaml")
    ! grep -n '\${' <<<"$y" >&2 || qb_die "unrendered placeholder in duckdb-warm.yaml"
    printf '%s\n' "$y"
}

cmd_up() {
    local s deadline
    command -v envsubst >/dev/null || qb_die "envsubst not found (brew install gettext)"
    if [[ "$CACHE_MODE" == on_disk ]] && ! kubectl get storageclass "$CACHE_STORAGE_CLASS" >/dev/null 2>&1; then
        [[ "$CACHE_STORAGE_CLASS" == gp3-fast ]] || qb_die "storageclass ${CACHE_STORAGE_CLASS} not found"
        kubectl apply -f "${HERE}/gp3-fast-sc.yaml" >/dev/null
    fi
    kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl create configmap "$CM" -n "$NAMESPACE" --from-file="${HERE}/duckdb_warm_server.py" >/dev/null
    for s in $(suites_arg "${1:-both}"); do
        render_server "$s" | kubectl apply -f - >/dev/null
        # A changed server.py only reaches a running pod through a restart.
        kubectl rollout restart -n "$NAMESPACE" "deploy/$(name_of "$s")" >/dev/null
        qb_log "deploy/$(name_of "$s") applied (duckdb ${DUCKDB_VERSION}, cache ${CACHE_MODE}: $((CACHE_MEM_BYTES / 1000000000)) GB mem$([[ "$CACHE_MODE" == on_disk ]] && echo " + $((CACHE_DISK_BYTES >> 30)) GiB disk"))"
    done
    deadline=$(( $(date +%s) + READY_GRACE ))
    for s in $(suites_arg "${1:-both}"); do
        until kubectl rollout status -n "$NAMESPACE" "deploy/$(name_of "$s")" --timeout=30s >/dev/null 2>&1; do
            (( $(date +%s) < deadline )) || qb_die "deploy/$(name_of "$s") not ready after ${READY_GRACE}s (kubectl describe pod -l app=duckdb-warm,suite=${s})"
            qb_log "waiting for deploy/$(name_of "$s")..."
        done
        pf_up "$s" || qb_die "no /health from $(name_of "$s")"
        qb_log "${s}: $(api "$s" GET /health)"
    done
}

cmd_status() {
    local s
    for s in hot snapshot; do
        if kubectl get deploy "$(name_of "$s")" -n "$NAMESPACE" >/dev/null 2>&1 && pf_up "$s"; then
            echo "${s}: $(api "$s" GET '/health?cache=1')"
        else
            echo "${s}: not running"
        fi
    done
}

cmd_down() {
    local s
    for s in $(suites_arg "${1:-both}"); do
        kubectl delete deploy "$(name_of "$s")" -n "$NAMESPACE" --ignore-not-found --wait=false
    done
    kubectl get deploy -n "$NAMESPACE" -l app=duckdb-warm -o name 2>/dev/null | grep -q . \
        || kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found
}

cmd_clear() {
    local s
    for s in $(suites_arg "${1:-both}"); do
        pf_up "$s" || qb_die "$(name_of "$s") is not up"
        qb_log "${s}: cache cleared: $(api "$s" POST /clear_cache)"
    done
}

# ── run ─────────────────────────────────────────────────────────────────────
# One pass of one query: submit, poll until done, job JSON on stdout. The
# poll has a deadline (the query timeout + 5 min): the server's in-process
# timeout (con.interrupt) did not stop an I/O-bound scan on 2026-10-17, which was
# still running ~6 h past a 1 h cap. On the deadline, or if the server lost the
# job (a restart: /job/<id> 404s), the pass is TIMEOUT/ERROR and the server is
# restarted so the next query is not stuck behind it. A restart empties the
# block cache; the log says so.
run_pass() {   # $1 suite, $2 arm, $3 query file, $4 rendered sql file
    local s="$1" arm="$2" f="$3" name body id j state="" to deadline
    name=$(qb_query_name "$f"); to=$(qb_query_timeout "$s")
    # A pinned table version (hot-interleave.sh): DUCK_PIN_ICEBERG / DUCK_PIN_PAIMON
    # hold the copied metadata.json every query of the run reads.
    local pin=""; [[ "$arm" == iceberg ]] && pin="${DUCK_PIN_ICEBERG:-}" || pin="${DUCK_PIN_PAIMON:-}"
    body=$(python3 - "$4" "$s" "$arm" "$name" "$to" \
             "$(qb_query_flag "$f" allow-zero-rows && echo 1 || echo 0)" \
             "$(qb_query_flag "$f" leading-col-is-label && echo 1 || echo 0)" "$pin" <<'EOF'
import json, sys
a = sys.argv
d = {"sql": open(a[1]).read(), "suite": a[2], "arm": a[3], "name": a[4],
     "runs": 1, "warmup": 0, "timeout_s": int(a[5]),
     "allow_zero_rows": a[6] == "1", "leading_col_is_label": a[7] == "1"}
if a[8]:
    d["metadata"] = a[8]
print(json.dumps(d))
EOF
)
    id=$(api "$s" POST /query "$body" | jget id 2>/dev/null || true)
    [[ -n "$id" ]] || { echo '{"status": "ERROR", "error": "server refused the query"}'; return 0; }
    deadline=$(( $(date +%s) + to + 300 ))
    while :; do
        sleep "$POLL_S"
        if (( $(date +%s) >= deadline )); then
            qb_log "[${arm}/${s}] ${name}: no result after $((to + 300))s; restarting $(name_of "$s") (its cache is lost)"
            echo "# ${name}: TIMEOUT by client deadline; server restarted, block cache emptied" >> "$LOG"
            restart_server "$s"
            echo '{"status": "TIMEOUT", "error": "client deadline"}'; return 0
        fi
        j=$(api "$s" GET "/job/${id}") || continue
        [[ "$j" == *'"no such job"'* ]] && { echo '{"status": "ERROR", "error": "job lost (server restarted?)"}'; return 0; }
        state=$(jget state <<<"$j" 2>/dev/null || true)
        [[ "$state" == done ]] && { printf '%s\n' "$j"; return 0; }
    done
}

restart_server() {   # $1 suite
    kubectl rollout restart -n "$NAMESPACE" "deploy/$(name_of "$1")" >/dev/null
    kubectl rollout status -n "$NAMESPACE" "deploy/$(name_of "$1")" --timeout="${READY_GRACE}s" >/dev/null \
        || qb_die "$(name_of "$1") did not come back after a restart"
    pf_up "$1" || qb_die "no /health from $(name_of "$1") after a restart"
}

# All passes of all queries of one arm, in starrocks.sh's order: PASS-major
# (S1 S2 S3 S4 warm-up, then S1 S2 S3 S4 timed), so each engine's cache sees
# the same sequence. Query-major (S1 S1, S2 S2, ...) let each DuckDB timed pass
# run straight after its own warm-up, while StarRocks' S4 warm-up had already
# pushed S1-S3 out of its cache. A query that fails on any pass is not run again
# (the old duckdb.sh's rule), and its CSV status is the first non-OK one.
run_arm() {   # $1 suite, $2 arm, $3 dir of rendered sql, $4 rows file
    local s="$1" arm="$2" dir="$3" rows="$4" files f pass n name j st q
    files=$(qb_query_files "$s")
    q="${dir}/${arm}"; mkdir -p "$q"
    for (( pass = 0; pass < WARMUP + RUNS; pass++ )); do
        n=0
        for f in $files; do
            n=$((n + 1)); name=$(qb_query_name "$f")
            [[ -f "${q}/${n}.bad" ]] && continue
            j=$(run_pass "$s" "$arm" "$f" "${dir}/$(printf '%02d' "$n").sql")
            st=$(jget status <<<"$j")
            python3 - "$j" "$name" "$pass" "$WARMUP" "$q" "$n" 2>>"$LOG" <<'EOF'
import json, sys
j, name, p, w, q, n = json.loads(sys.argv[1]), sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5], sys.argv[6]
t = (j.get("times") or [None])[0]
tag = "warm-up" if p < w else "run %d" % (p - w + 1)
print("  %-22s %-8s %9s ms  %s  first_row=%s%s  scan=%s" % (name, tag, t if t is not None else "-", j["status"],
      j.get("first_row", "")[:80], ("  error=" + j["error"]) if j.get("error") else "", j.get("table", "")),
      file=sys.stderr)
if j["status"] == "OK" and p >= w:
    open("%s/%s.times" % (q, n), "a").write("%d\n" % t)
    open("%s/%s.last" % (q, n), "w").write(json.dumps(j))
EOF
            if [[ "$st" != OK ]]; then
                echo "$st" > "${q}/${n}.bad"
                qb_log "[${arm}/${s}] ${name} pass ${pass}: ${st}; not run again"
            else
                qb_log "[${arm}/${s}] ${name} pass ${pass}: OK $(jget median_ms <<<"$j") ms"
            fi
        done
    done
    n=0
    for f in $files; do
        n=$((n + 1)); name=$(qb_query_name "$f")
        local med="" st="OK" rr=0
        [[ -f "${q}/${n}.times" ]] && med=$(qb_median < "${q}/${n}.times")
        [[ -f "${q}/${n}.bad" ]] && st=$(<"${q}/${n}.bad")
        [[ -z "$med" && "$st" == OK ]] && st=ERROR
        [[ -f "${q}/${n}.last" ]] && rr=$(jget result_rows < "${q}/${n}.last")
        echo "${STAMP},duckdb_warm,${arm},${s},${name},${st},${med},${rr},," >> "$rows"
        printf '  %-22s MEDIAN %9s ms  %s\n' "$name" "${med:--}" "$st" >> "$LOG"
    done
}

run_suite() {   # $1 suite
    local s="$1" dir arm f n=0 writers rows bad
    dir=$(mktemp -d); rows="${dir}/rows.csv"; : > "$rows"
    qb_start_suite "$s"
    for f in $(qb_query_files "$s"); do
        n=$((n + 1)); qb_render "$f" > "${dir}/$(printf '%02d' "$n").sql" || qb_die "render failed: ${f}"
    done
    pf_up "$s" || qb_die "$(name_of "$s") is not up: ./duckdb-warm.sh up ${s}"
    writers=$(qb_writer_state)
    { echo "# ---- suite=${s} arms=${ARMS} writers: ${writers}"; echo "# server: $(api "$s" GET /health)"; } >> "$LOG"
    for arm in $ARMS; do
        qb_log "${s}: arm ${arm}..."
        echo "# arm=${arm}" >> "$LOG"
        run_arm "$s" "$arm" "$dir" "$rows"
    done
    qb_writers_unchanged "$writers" "$s" || { rm -rf "$dir"; return 1; }
    cat "$rows" >> "$CSV"
    bad=$(grep -cv ',OK,' "$rows" || true)
    qb_log "${s}: $(grep -c . "$rows" || true) queries, ${bad} not OK"
    rm -rf "$dir"
    (( bad == 0 ))
}

cmd_run() {
    local s rc=0
    qb_assert_alone duckdb-warm
    qb_csv_init "$CSV"
    qb_log "duckdb_warm: arms=${ARMS} suite=${SUITE} runs=${RUNS}+${WARMUP} stamp=${STAMP}"
    for s in $(qb_suites); do run_suite "$s" || rc=1; done
    qb_log "results: ${CSV}  log: ${LOG}"
    return $rc
}

case "${1:-run}" in
    up)          cmd_up "${2:-$SUITE}" ;;          # SUITE=hot -> only the hot server
    status)      cmd_status ;;
    run)         cmd_run ;;
    clear-cache) cmd_clear "${2:-$SUITE}" ;;
    pin)         # Pin both tables' current version for one run; prints shell exports.
                 pf_up "${2:-$SUITE}" || qb_die "$(name_of "${2:-$SUITE}") is not up"
                 api "${2:-$SUITE}" POST /pin "{\"tag\": \"${STAMP}\"}" | python3 -c 'import json, sys
d = json.load(sys.stdin)
if "error" in d: sys.exit("pin failed: " + d["error"])
print("export DUCK_PIN_ICEBERG=%s DUCK_PIN_PAIMON=%s" % (d["iceberg"]["metadata"], d["paimon"]["metadata"]))
print("export SR_PIN_ICEBERG=%s SR_PIN_PMNICE=%s SR_PIN_PAIMON=%s" % (d["iceberg"]["snapshot_id"], d["paimon"]["snapshot_id"], d["paimon"]["paimon_snapshot_id"] or ""))' ;;
    down)        cmd_down "${2:-both}" ;;
    *)           qb_die "usage: $0 up|status|run|clear-cache|down [hot|snapshot|both]" ;;
esac
