#!/usr/bin/env bash
# The DuckDB side of the CDC query tests: runs a suite on the warm DuckDB server
# (Deployment duckdb-warm-cdc, engine-deploy/deploy-duckdb-cdc.sh), the
# counterpart of trino.sh. Adapted from throughput-benchmark/querybench/duckdb-warm.sh.
#
#   SUITE=hot|snapshot ./querybench/duckdb-warm.sh run
#   ./querybench/duckdb-warm.sh render <suite> [arm]   print the rendered SQL (no cluster; dry run)
#
# Arms (FORMATS, default "iceberg paimon"): iceberg (the Iceberg tables) and
# paimon (Paimon-as-iceberg, its Iceberg v3 metadata). Every table is read at its
# pinned version: iceberg_scan() of the copied metadata.json for `iceberg` and for
# `paimon` (the export's v<N>.metadata.json) (QB_PIN_* from pin.sh), the same pins
# Trino reads by snapshot id. The
# client renders the whole statement, the server only runs it. An arm that cannot
# read its tables records ERROR per query; the other arms still run.
#
# Same RUNS/WARMUP, pass-major order, median, timeouts and ZERO_SCAN guard
# as trino.sh (allow-zero-rows, leading-col-is-label). Also:
#   - every result row of the last OK pass -> duckdb_warm-<STAMP>-rows.tsv
#     (the summary compares all rows across the engines);
#   - a server restart under a query (an OOM kill: the container restarts) is
#     detected from /health's started_at: that query is CRASH, and the rest of
#     that pass runs on emptied memory caches, recorded as `cold` in
#     duckdb_warm-<STAMP>-events.csv.
#
# Output: $OUT_DIR/duckdb_warm-<STAMP>.csv ($QB_CSV_HEADER), .log, -rows.tsv,
# -firstrows.csv, -events.csv.
#
# Exit: non-zero only when the driver itself fails: the server is not up (or
# not answering at the end of a suite), a render fails (a pin missing), the
# writers changed under the suite, or no result row was written. A query that
# is not OK (ERROR, TIMEOUT, CRASH, ZERO_SCAN: duckdb:paimon on v3, say) stays
# in the CSV and the summary, and in the suite's last log line
# ("N of M queries not OK: duckdb:<arm>:<query>=<status> ..."); it does not
# change the exit.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"

READY_GRACE="${READY_GRACE:-1200}"
POLL_S=5
CSV="${OUT_DIR}/duckdb_warm-${STAMP}.csv"
LOG="${OUT_DIR}/duckdb_warm-${STAMP}.log"
EVENTS="${CSV%.csv}-events.csv"
NAME="$DUCKDB_NAME"
PORT="${DUCKDB_LOCAL_PORT:-18182}"
ARMS="$(qb_formats)"

PF_PIDS=""
cleanup() { local p; for p in $PF_PIDS; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT
pf_up() {
    curl -sf -m 5 "localhost:${PORT}/health" >/dev/null 2>&1 && return 0
    kubectl port-forward -n "$NAMESPACE" "deploy/${NAME}" "${PORT}:8080" >/dev/null 2>&1 &
    PF_PIDS="${PF_PIDS} $!"
    local i; for i in $(seq 1 30); do
        curl -sf -m 5 "localhost:${PORT}/health" >/dev/null 2>&1 && return 0; sleep 1
    done
    return 1
}
api() {     # GET|POST <path> [json body]
    local i body="${3:-}"; [[ -n "$body" ]] || body='{}'
    for i in 1 2 3; do
        if [[ "$1" == GET ]]; then curl -sf -m 30 "localhost:${PORT}$2" && return 0
        else curl -s -m 30 -X POST -H 'Content-Type: application/json' --data-binary "$body" "localhost:${PORT}$2" && return 0
        fi
        pf_up || true
    done
    return 1
}
started_at() { api GET /health 2>/dev/null | qb_jget started_at 2>/dev/null || true; }

restart_server() {
    kubectl rollout restart -n "$NAMESPACE" "deploy/${NAME}" >/dev/null
    kubectl rollout status -n "$NAMESPACE" "deploy/${NAME}" --timeout="${READY_GRACE}s" >/dev/null \
        || qb_die "${NAME} did not come back after a restart"
    pf_up || qb_die "no /health from ${NAME} after a restart"
}

# Wait (up to READY_GRACE) for the server to answer again, e.g. after its
# container was OOM-killed and restarted (pip fallback: pip install + extensions,
# minutes; the DUCKDB_IMAGE image: seconds).
wait_server() {
    local deadline=$(( $(date +%s) + READY_GRACE ))
    until pf_up; do
        (( $(date +%s) < deadline )) || qb_die "${NAME} not answering ${READY_GRACE}s after a restart"
        sleep 10
    done
}

# One pass of one query: submit, poll until done, job JSON on stdout. The poll
# has a deadline (query timeout + 5 min; con.interrupt did not stop an I/O-bound
# scan in throughput's tests): on it the server is restarted and the pass is
# TIMEOUT. A job the server lost (it restarted) is CRASH.
run_pass() {   # suite arm query-file rendered-sql-file
    local s="$1" arm="$2" f="$3" name body id j state to deadline
    name=$(qb_query_name "$f"); to=$(qb_query_timeout "$s")
    body=$(python3 - "$4" "$s" "$arm" "$name" "$to" \
             "$(qb_query_flag "$f" allow-zero-rows && echo 1 || echo 0)" \
             "$(qb_query_flag "$f" leading-col-is-label && echo 1 || echo 0)" <<'EOF'
import json, sys
a = sys.argv
print(json.dumps({"sql": open(a[1]).read(), "suite": a[2], "arm": a[3], "name": a[4], "runs": 1, "warmup": 0,
                  "timeout_s": int(a[5]), "allow_zero_rows": a[6] == "1", "leading_col_is_label": a[7] == "1"}))
EOF
)
    id=$(api POST /query "$body" | qb_jget id 2>/dev/null || true)
    [[ -n "$id" ]] || { echo '{"status": "ERROR", "error": "server refused the query"}'; return 0; }
    deadline=$(( $(date +%s) + to + 300 ))
    while :; do
        sleep "$POLL_S"
        if (( $(date +%s) >= deadline )); then
            qb_log "[${arm}/${s}] ${name}: no result after $((to + 300))s; restarting ${NAME} (its cache is lost)"
            echo "# ${name}: TIMEOUT by client deadline; server restarted, caches emptied" >> "$LOG"
            restart_server
            echo '{"status": "TIMEOUT", "error": "client deadline"}'; return 0
        fi
        j=$(curl -s -m 30 "localhost:${PORT}/job/${id}" 2>/dev/null) || { pf_up || true; continue; }
        [[ -n "$j" ]] || { pf_up || true; continue; }
        [[ "$j" == *'"no such job"'* ]] && { echo '{"status": "CRASH", "error": "job lost: the server restarted under it (OOM kill?)"}'; return 0; }
        state=$(qb_jget state <<<"$j" 2>/dev/null || true)
        [[ "$state" == done ]] && { printf '%s\n' "$j"; return 0; }
    done
}

# Bytes from S3 and rows out of the scan, cache bypassed; untimed, after the
# arm's timed passes. "<bytes> <rows>" or nothing.
profile_pass() {   # suite arm query-file rendered-sql-file
    local s="$1" arm="$2" f="$3" body id j deadline to
    to=$(qb_query_timeout "$s")
    body=$(python3 -c 'import json, sys; print(json.dumps({"sql": open(sys.argv[1]).read(), "suite": sys.argv[2], "arm": sys.argv[3], "name": sys.argv[4], "timeout_s": int(sys.argv[5]), "profile": True}))' \
             "$4" "$s" "$arm" "$(qb_query_name "$f")" "$to")
    id=$(api POST /query "$body" | qb_jget id 2>/dev/null || true)
    [[ -n "$id" ]] || return 0
    deadline=$(( $(date +%s) + to + 300 ))
    while (( $(date +%s) < deadline )); do
        sleep "$POLL_S"
        j=$(curl -s -m 30 "localhost:${PORT}/job/${id}" 2>/dev/null) || { pf_up || true; continue; }
        [[ "$j" == *'"no such job"'* ]] && { wait_server; return 0; }
        [[ "$(qb_jget state <<<"$j" 2>/dev/null)" == done ]] || continue
        [[ "$(qb_jget status <<<"$j")" == OK ]] && echo "$(qb_jget scan_bytes <<<"$j") $(qb_jget scan_rows <<<"$j")"
        return 0
    done
}

# All passes of all queries of one arm, pass-major (as trino.sh). A query
# that fails on any pass is not run again; its CSV status is the first non-OK.
run_arm() {   # suite arm rendered-dir rows-file
    local s="$1" arm="$2" dir="$3" rows="$4" files f pass n name j st q t0 t1 cold
    files=$(qb_query_files "$s")
    q="${dir}/${arm}"; mkdir -p "$q"
    for (( pass = 0; pass < WARMUP + RUNS; pass++ )); do
        n=0; cold=0
        for f in $files; do
            n=$((n + 1)); name=$(qb_query_name "$f")
            [[ -f "${q}/${n}.bad" ]] && continue
            (( cold )) && echo "${arm},${s},${pass},${name},cold" >> "$EVENTS"
            t0=$(started_at)
            j=$(run_pass "$s" "$arm" "$f" "${dir}/${arm}.$(printf '%02d' "$n").sql")
            st=$(qb_jget status <<<"$j")
            [[ "$st" == CRASH ]] && wait_server
            t1=$(started_at)
            if [[ -n "$t0" && -n "$t1" && "$t0" != "$t1" ]]; then
                [[ "$st" == TIMEOUT ]] || st=CRASH
                j=$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); d["status"]=sys.argv[2]; print(json.dumps(d))' "$j" "$st")
                echo "${arm},${s},${pass},${name},restart" >> "$EVENTS"
                echo "# ${name} pass ${pass}: the server restarted under it (${st}); later queries of this pass run on emptied memory caches" >> "$LOG"
                cold=1
            fi
            python3 - "$j" "$name" "$pass" "$WARMUP" "$q" "$n" 2>>"$LOG" <<'EOF'
import json, os, sys
j, name, p, w, q, n = json.loads(sys.argv[1]), sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5], sys.argv[6]
t = (j.get("times") or [None])[0]
cold = os.environ.get("COLD_PASS") == "1" and p == 0
tag = "cold" if cold else "warm-up" if p < w else "run %d" % (p - w + 1)
print("  %-30s %-8s %9s ms  %s  first_row=%s%s" % (name, tag, t if t is not None else "-", j["status"],
      j.get("first_row", "")[:80], ("  error=" + j["error"]) if j.get("error") else ""), file=sys.stderr)
if j["status"] == "OK" and cold:
    open("%s/%s.cold" % (q, n), "w").write("%d\n" % t)
elif j["status"] == "OK" and p >= w:
    open("%s/%s.times" % (q, n), "a").write("%d\n" % t)
if j["status"] == "OK" and (cold or p >= w):
    open("%s/%s.last" % (q, n), "w").write(json.dumps(j))
EOF
            if [[ "$st" != OK ]]; then
                echo "$st" > "${q}/${n}.bad"
                qb_log "[${arm}/${s}] ${name} pass ${pass}: ${st}; not run again"
            else
                qb_log "[${arm}/${s}] ${name} pass ${pass}: OK $(qb_jget median_ms <<<"$j") ms"
            fi
        done
    done
    if [[ "${PROFILE_SCAN:-1}" != 0 ]]; then
        n=0
        for f in $files; do
            n=$((n + 1)); [[ -f "${q}/${n}.bad" ]] && continue
            profile_pass "$s" "$arm" "$f" "${dir}/${arm}.$(printf '%02d' "$n").sql" > "${q}/${n}.scan"
            qb_log "[${arm}/${s}] $(qb_query_name "$f") profile: $(awk '{printf "%.1f GB, %s rows out of the scan", $1/1e9, $2}' "${q}/${n}.scan")"
        done
    fi
    n=0
    for f in $files; do
        n=$((n + 1)); name=$(qb_query_name "$f")
        local med="" st="OK" rr=0 sb="" srr="" cms=""
        [[ -f "${q}/${n}.times" ]] && med=$(qb_median < "${q}/${n}.times")
        [[ -f "${q}/${n}.cold" ]] && cms=$(<"${q}/${n}.cold")
        [[ -f "${q}/${n}.bad" ]] && st=$(<"${q}/${n}.bad")
        [[ -z "$med" && "$st" == OK ]] && st=ERROR
        [[ -f "${q}/${n}.last" ]] && rr=$(qb_jget result_rows < "${q}/${n}.last")
        [[ -s "${q}/${n}.scan" ]] && read -r sb srr < "${q}/${n}.scan"
        echo "${STAMP},duckdb_warm,${arm},${s},${name},${st},${med},${rr},${srr},${sb},${cms}" >> "$rows"
        if [[ -f "${q}/${n}.last" ]]; then qb_write_last_rows "${q}/${n}.last" "$arm" "$s" "$name" "$CSV"; fi
        printf '  %-30s MEDIAN %9s ms  %s\n' "$name" "${med:--}" "$st" >> "$LOG"
    done
}

run_suite() {   # suite
    local s="$1" dir arm writers rows n
    dir=$(mktemp -d); rows="${dir}/rows.csv"; : > "$rows"
    qb_start_suite "$s"
    qb_render_suite_dir duckdb "$s" "$dir"
    pf_up || qb_die "${NAME} is not up: ./engine-deploy/deploy-duckdb-cdc.sh up"
    writers=$(qb_writer_state)
    { echo "# ---- suite=${s} arms=${ARMS} writers: ${writers}"; echo "# server: $(api GET /health)"; } >> "$LOG"
    for arm in $ARMS; do
        qb_log "${s}: arm ${arm}..."
        echo "# arm=${arm}" >> "$LOG"
        run_arm "$s" "$arm" "$dir" "$rows"
    done
    qb_writers_unchanged "$writers" "$s" || { rm -rf "$dir"; return 1; }
    n=$(grep -c . "$rows" || true)
    (( n > 0 )) || { qb_log "ERROR: ${s}: no result rows written"; rm -rf "$dir"; return 1; }
    cat "$rows" >> "$CSV"
    qb_log_not_ok duckdb "$s" "$rows" "$n" "$LOG"
    rm -rf "$dir"
    pf_up || { qb_log "ERROR: ${NAME} stopped answering during the ${s} suite"; return 1; }
}

cmd_run() {
    local s rc=0
    qb_assert_alone duckdb-warm
    qb_csv_init "$CSV"
    [[ -f "$EVENTS" ]] || echo "arm,suite,pass,query,event" > "$EVENTS"
    qb_log "duckdb_warm: arms=${ARMS} suite=${SUITE} runs=${RUNS}+${WARMUP} stamp=${STAMP}"
    for s in $(qb_suites); do run_suite "$s" || rc=1; done
    qb_log "results: ${CSV}  log: ${LOG}"
    return $rc
}

case "${1:-run}" in
    run)    cmd_run ;;
    render) s="${2:?render <hot|snapshot> [arm]}"; qb_print_rendered duckdb "$s" "${3:-}" ;;
    *)      qb_die "usage: $0 run | render <suite> [arm]" ;;
esac
