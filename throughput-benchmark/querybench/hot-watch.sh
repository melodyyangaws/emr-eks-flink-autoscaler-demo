#!/usr/bin/env bash
# Stop the generator and the Flink writers once the hot query test is done, so
# the writers stay live for the whole test without a fixed generator length
# (2026-10-19: a 1-hour generator stopped during the second timed pass).
#
# Start the generator until stopped, the hot test in the runner, then this in a
# second in-cluster Job (it survives the laptop):
#   JSONGZ_REPLICAS=7 DURATION_SECONDS=0 ./jsongz-gen/run-jsongz-gen.sh start
#   ./querybench/qb-runner.sh start '... ./querybench/hot-interleave.sh ...'
#   QB_RUNNER_NAME=hot-watch ./querybench/qb-runner.sh start ./querybench/hot-watch.sh
#
# Every WATCH_INTERVAL_S (600) it reads the runner's log. The test is over when
# the runner prints "status: runner-exit=", or when its Job or pod is gone or
# failed (nothing would ever print it). Then it deletes the generator Job,
# suspends the writers (qb_stop_writers; SUSPEND_WRITERS=0 leaves them running),
# scales the job monitor (flink-cid-monitor) to 0 once the writers have
# stopped, and takes duckdb-warm-hot down (duckdb-warm.sh down hot). The
# laptop's metric collectors (deploy-cid-monitor.sh) are not reachable from
# here: `./monitoring/deploy-cid-monitor.sh stop` ends them.
# Knobs: WATCH_INTERVAL_S, WATCH_RUNNER (qb-runner), GEN_JOB (cid-jsongz-gen).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh"
WATCH_INTERVAL_S="${WATCH_INTERVAL_S:-600}"
WATCH_RUNNER="${WATCH_RUNNER:-qb-runner}"
GEN_JOB="${GEN_JOB:-cid-jsongz-gen}"

# Prints why the test is over, or nothing while it runs.
test_over() {
    local phase
    kubectl get job "$WATCH_RUNNER" -n "$NAMESPACE" >/dev/null 2>&1 || { echo "runner Job ${WATCH_RUNNER} is gone"; return; }
    phase=$(kubectl get pods -n "$NAMESPACE" -l job-name="$WATCH_RUNNER" -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)
    case "$phase" in
        Failed)    echo "runner pod failed"; return ;;
        "")        echo "runner pod is gone"; return ;;
    esac
    # Only the status: lines: the log also echoes the command, which names them.
    kubectl logs -n "$NAMESPACE" "job/${WATCH_RUNNER}" 2>/dev/null | grep -m1 '^status: runner-exit=' || true
}

qb_log "hot-watch: checking ${WATCH_RUNNER} every ${WATCH_INTERVAL_S}s; then stopping ${GEN_JOB}, the writers (${FLINK_JOBS}), the job monitor and duckdb-warm-hot"
while :; do
    why=$(test_over)
    [[ -n "$why" ]] && break
    last=$(kubectl logs -n "$NAMESPACE" "job/${WATCH_RUNNER}" --tail=200 2>/dev/null | grep '\[querybench\]' | tail -1 || true)
    qb_log "hot test still running: ${last:-no progress line yet}"
    sleep "$WATCH_INTERVAL_S"
done
qb_log "hot test over: ${why}"

if ! kubectl get job "$GEN_JOB" -n "$NAMESPACE" >/dev/null 2>&1; then
    qb_log "WARNING: no generator Job ${GEN_JOB}: it had already stopped, so the writers were not live to the end"
elif kubectl delete job "$GEN_JOB" -n "$NAMESPACE" >/dev/null; then
    qb_log "generator stopped (Job ${GEN_JOB} deleted)"
else
    qb_log "WARNING: could not delete Job ${GEN_JOB}"
fi
# In a subshell: qb_stop_writers exits on failure, and DuckDB should still stop.
# The monitor only stops once the writers have: it is what shows they did not.
if ( qb_stop_writers ); then
    if [[ "${SUSPEND_WRITERS:-1}" == 0 ]]; then
        qb_log "SUSPEND_WRITERS=0: job monitor left running"
    elif kubectl scale deployment/flink-cid-monitor -n "$NAMESPACE" --replicas=0 >/dev/null 2>&1; then
        qb_log "job monitor stopped (flink-cid-monitor scaled to 0)"
    else
        qb_log "WARNING: could not stop the job monitor (flink-cid-monitor)"
    fi
else
    qb_log "WARNING: could not stop the writers (see above); job monitor left running"
fi
"${HERE}/duckdb-warm.sh" down hot >/dev/null \
    && qb_log "duckdb-warm-hot stopped" \
    || qb_log "WARNING: could not stop duckdb-warm-hot"
echo "status: hot-watch-done $(date -u)"
