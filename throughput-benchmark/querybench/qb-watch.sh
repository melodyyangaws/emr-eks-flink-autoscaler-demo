#!/usr/bin/env bash
# Watch one query suite's run (SUITE=hot|snapshot) and shut the run down once
# it is done, so nothing keeps writing or idling after the test.
#   hot:      the writers stay live for the whole test without a fixed
#             generator length, and stop right after it.
#   snapshot: querybench/run.sh has already stopped the generator and the writers
#             before the test; this stops what is left.
#
# Not started by hand: `SUITE=hot|snapshot ./querybench/run.sh start`
# starts it in a second in-cluster Job (qb-watch) right after the runner, in the
# one safe order (see querybench/run.sh):
#   ./datagen/run.sh start      # .bench-env: JSONGZ_REPLICAS=7, DURATION_SECONDS=0
#   SUITE=hot      ./querybench/run.sh start
#   SUITE=snapshot ./querybench/run.sh start
#
# Every WATCH_INTERVAL_S (600) it reads the runner's log. The test is over when
# the runner prints "status: runner-exit=", or when its Job or pod is gone or
# failed (nothing would ever print it). Then, for either suite, it:
#   1. deletes the generator Job (snapshot: normally gone already);
#   2. suspends the writers (qb_stop_writers; SUSPEND_WRITERS=0 leaves them);
#   3. scales the job monitor (flink-cid-monitor) to 0, once the writers have
#      stopped;
#   4. takes the warm DuckDB server down (duckdb-warm.sh down);
#   5. deletes the runner's ConfigMap and Secret (its bundle and .bench-env.local);
#      the finished Jobs delete themselves after a day.
# The laptop's metric collectors (monitoring/run.sh) are not reachable from
# here: `./monitoring/run.sh stop` ends them.
# Knob: WATCH_INTERVAL_S.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
case "$SUITE" in hot|snapshot) ;; *) qb_die "qb-watch needs SUITE=hot|snapshot, got '${SUITE}'" ;; esac
WATCH_INTERVAL_S="${WATCH_INTERVAL_S:-600}"
WATCH_RUNNER=qb-runner
GEN_JOB=cid-jsongz-gen
SERVER=duckdb-warm

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

qb_log "qb-watch (${SUITE}): checking ${WATCH_RUNNER} every ${WATCH_INTERVAL_S}s; then stopping ${GEN_JOB}, the writers (${FLINK_JOBS}), the job monitor and ${SERVER}"
while :; do
    why=$(test_over)
    [[ -n "$why" ]] && break
    last=$(kubectl logs -n "$NAMESPACE" "job/${WATCH_RUNNER}" --tail=200 2>/dev/null | grep '\[querybench\]' | tail -1 || true)
    qb_log "${SUITE} test still running: ${last:-no progress line yet}"
    sleep "$WATCH_INTERVAL_S"
done
qb_log "${SUITE} test over: ${why}"

if ! kubectl get job "$GEN_JOB" -n "$NAMESPACE" >/dev/null 2>&1; then
    # Expected for snapshot (stopped before the test); for hot it means the
    # writers were not live to the end.
    [[ "$SUITE" == hot ]] \
        && qb_log "WARNING: no generator Job ${GEN_JOB}: it had already stopped, so the writers were not live to the end" \
        || qb_log "generator already stopped"
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
"${HERE}/duckdb-warm.sh" down >/dev/null \
    && qb_log "${SERVER} stopped" \
    || qb_log "WARNING: could not stop ${SERVER}"
kubectl delete configmap "$WATCH_RUNNER" -n "$NAMESPACE" --ignore-not-found >/dev/null \
    && kubectl delete secret "$WATCH_RUNNER" -n "$NAMESPACE" --ignore-not-found >/dev/null \
    && qb_log "${WATCH_RUNNER}'s ConfigMap and Secret deleted" \
    || qb_log "WARNING: could not delete ${WATCH_RUNNER}'s ConfigMap/Secret"
echo "status: qb-watch-done $(date -u)"
