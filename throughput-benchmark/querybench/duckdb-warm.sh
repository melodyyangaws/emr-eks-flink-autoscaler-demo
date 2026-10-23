#!/usr/bin/env bash
# The DuckDB side of the query tests: runs the suites on the warm DuckDB server
# (Deployment duckdb-warm, deployed by ../engine-deploy/deploy-duckdb-warm.sh:
# up / status / clear-cache / down), the counterpart of starrocks.sh.
#
#   SUITE=hot|snapshot|all ./duckdb-warm.sh run run the suite(s) on the server
#   ./duckdb-warm.sh pin                        pin both tables' current version (shell exports)
#
# Parity with StarRocks (sizing: see deploy-duckdb-warm.sh):
#   process   always up, warm                             -> always up, warm
#   timing    server-side, audit log, planning included   -> in-process, execute + fetch
#   arms      serial, one cluster                         -> serial, one process
#   SQL       lib/querybench.sh renders the same files and windows for both engines
# Same RUNS/WARMUP, median, timeouts (qb_query_timeout) and ZERO_SCAN guard as
# starrocks.sh. One query at a time: the server has a lock, and `run`
# refuses to start while another benchmark (either engine) is running, unless
# ALLOW_CONCURRENT=1.
#
# Output: $OUT_DIR/duckdb_warm-<STAMP>.csv ($QB_CSV_HEADER, engine=duckdb_warm)
# and duckdb_warm-<STAMP>.log (every pass, the pinned scan, the first row).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"

FORMAT="${FORMAT:-both}"
READY_GRACE="${READY_GRACE:-1200}"                   # a restart: Karpenter launch + pip + extensions
POLL_S=5
CSV="${OUT_DIR}/duckdb_warm-${STAMP}.csv"
LOG="${OUT_DIR}/duckdb_warm-${STAMP}.log"
QB_DEFER=TABLE        # the server resolves it per arm and query (pinned metadata.json)
QB_BYTES_FN=STRLEN    # bytes, like StarRocks' LENGTH (DuckDB's LENGTH counts characters)
NAME=duckdb-warm
PORT=18181
case "$FORMAT" in
    iceberg|paimon) ARMS="$FORMAT" ;;
    both)           ARMS="paimon iceberg" ;;            # StarRocks' order: paimon first
    *) qb_die "FORMAT must be iceberg|paimon|both, got '${FORMAT}'" ;;
esac

# ── Port-forward (restarted if it drops) ────────────────────────────────────
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
api() {     # $1 GET|POST, $2 path, [$3 json body] -> body on stdout
    local i body="${3:-}"; [[ -n "$body" ]] || body='{}'
    for i in 1 2 3; do
        if [[ "$1" == GET ]]; then curl -s -m 30 "localhost:${PORT}$2" && return 0
        else curl -s -m 30 -X POST -H 'Content-Type: application/json' --data-binary "$body" "localhost:${PORT}$2" && return 0
        fi
        pf_up || true
    done
    return 1
}
jget() { python3 -c 'import json,sys; d=json.load(sys.stdin); v=d.get(sys.argv[1]); print("" if v is None else v)' "$1"; }

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
    id=$(api POST /query "$body" | jget id 2>/dev/null || true)
    [[ -n "$id" ]] || { echo '{"status": "ERROR", "error": "server refused the query"}'; return 0; }
    deadline=$(( $(date +%s) + to + 300 ))
    while :; do
        sleep "$POLL_S"
        if (( $(date +%s) >= deadline )); then
            qb_log "[${arm}/${s}] ${name}: no result after $((to + 300))s; restarting ${NAME} (its cache is lost)"
            echo "# ${name}: TIMEOUT by client deadline; server restarted, block cache emptied" >> "$LOG"
            restart_server
            echo '{"status": "TIMEOUT", "error": "client deadline"}'; return 0
        fi
        j=$(api GET "/job/${id}") || continue
        [[ "$j" == *'"no such job"'* ]] && { echo '{"status": "ERROR", "error": "job lost (server restarted?)"}'; return 0; }
        state=$(jget state <<<"$j" 2>/dev/null || true)
        [[ "$state" == done ]] && { printf '%s\n' "$j"; return 0; }
    done
}

restart_server() {
    kubectl rollout restart -n "$NAMESPACE" "deploy/${NAME}" >/dev/null
    kubectl rollout status -n "$NAMESPACE" "deploy/${NAME}" --timeout="${READY_GRACE}s" >/dev/null \
        || qb_die "${NAME} did not come back after a restart"
    pf_up || qb_die "no /health from ${NAME} after a restart"
}

# All passes of all queries of one arm, in starrocks.sh's order: PASS-major
# (S1 S2 S3 S4 warm-up, then S1 S2 S3 S4 timed), so each engine's cache sees
# the same sequence. Query-major (S1 S1, S2 S2, ...) let each DuckDB timed pass
# run straight after its own warm-up, while StarRocks' S4 warm-up had already
# pushed S1-S3 out of its cache. A query that fails on any pass is not run again,
# and its CSV status is the first non-OK one.
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
        # The last OK pass's first row, as starrocks.sh records it: summary/
        # summarize.py checks every arm read the same rows (S1's, S4's counts).
        [[ -f "${q}/${n}.last" ]] && echo "${arm},${s},${name},$(jget first_row < "${q}/${n}.last")" >> "${CSV%.csv}-firstrows.csv"
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
    pf_up || qb_die "${NAME} is not up: ./engine-deploy/deploy-duckdb-warm.sh up"
    writers=$(qb_writer_state)
    { echo "# ---- suite=${s} arms=${ARMS} writers: ${writers}"; echo "# server: $(api GET /health)"; } >> "$LOG"
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
    run)         cmd_run ;;
    pin)         # Pin both tables' current version for one run; prints shell exports.
                 pf_up || qb_die "${NAME} is not up"
                 api POST /pin "{\"tag\": \"${STAMP}\"}" | python3 -c 'import json, sys
d = json.load(sys.stdin)
if "error" in d: sys.exit("pin failed: " + d["error"])
print("export DUCK_PIN_ICEBERG=%s DUCK_PIN_PAIMON=%s" % (d["iceberg"]["metadata"], d["paimon"]["metadata"]))
print("export SR_PIN_ICEBERG=%s SR_PIN_PMNICE=%s SR_PIN_PAIMON=%s" % (d["iceberg"]["snapshot_id"], d["paimon"]["snapshot_id"], d["paimon"]["paimon_snapshot_id"] or ""))' ;;
    up|status|clear-cache|down)
                 qb_die "moved: ./engine-deploy/deploy-duckdb-warm.sh $1" ;;
    *)           qb_die "usage: $0 run|pin (the server: ./engine-deploy/deploy-duckdb-warm.sh)" ;;
esac
