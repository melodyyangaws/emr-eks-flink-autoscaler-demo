#!/usr/bin/env bash
# Watch one CDC query test's runner (cdc-qb-runner) and shut the run down once
# it ends. Not started by hand: `SUITE=hot|snapshot ./querybench/run.sh start`
# starts it as the Job cdc-qb-watch right after the runner. Adapted from
# throughput-benchmark/querybench/qb-watch.sh.
#
# Every WATCH_INTERVAL_S (600) it reads the runner's log; the test is over when
# the runner prints "status: runner-exit=", or its Job or pod is gone or failed.
# Then:
#   1. scales the MySQL generator to 0 (STOP_GENERATOR=0 leaves it);
#   2. suspends the CDC writers (SUSPEND_WRITERS=0 leaves them; they resume
#      from their last checkpoint: upgradeMode last-state);
#   3. scales the CDC job monitor (flink-cdc-monitor) to 0 once the writers stopped;
#   4. takes duckdb-warm-cdc down (STOP_DUCKDB=0 leaves it);
#   5. takes StarRocks down (STOP_STARROCKS=0 leaves it): the StarRocksCluster
#      and its FE/BE volumes, as throughput-benchmark/engine-deploy/
#      deploy-starrocks.sh down does (the Helm release and operator stay;
#      run.sh start installs it again);
#   6. deletes the runner's ConfigMap and Secret.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
case "$SUITE" in hot|snapshot) ;; *) qb_die "qb-watch needs SUITE=hot|snapshot, got '${SUITE}'" ;; esac
WATCH_INTERVAL_S="${WATCH_INTERVAL_S:-600}"
WATCH_RUNNER=cdc-qb-runner
DUCK="${DUCKDB_NAME:-duckdb-warm-cdc}"
MONITOR="${CDC_MONITOR:-flink-cdc-monitor}"

test_over() {
    local phase
    kubectl get job "$WATCH_RUNNER" -n "$NAMESPACE" >/dev/null 2>&1 || { echo "runner Job ${WATCH_RUNNER} is gone"; return; }
    phase=$(kubectl get pods -n "$NAMESPACE" -l job-name="$WATCH_RUNNER" -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)
    case "$phase" in
        Failed) echo "runner pod failed"; return ;;
        "")     echo "runner pod is gone"; return ;;
    esac
    kubectl logs -n "$NAMESPACE" "job/${WATCH_RUNNER}" 2>/dev/null | grep -m1 '^status: runner-exit=' || true
}

# = throughput-benchmark/engine-deploy/deploy-starrocks.sh down, in kubectl only
# (that script needs throughput's .bench-env, which this pod does not carry).
starrocks_down() {
    local i n
    kubectl delete starrocksclusters.starrocks.com kube-starrocks -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=300s >/dev/null || return 1
    for (( i = 0; i < 60; i++ )); do
        n=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=fe -o name 2>/dev/null; \
            kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=be -o name 2>/dev/null)
        [[ -z "$n" ]] && break
        sleep 5
    done
    kubectl delete pvc -n "$NAMESPACE" -l app.kubernetes.io/component=fe --ignore-not-found >/dev/null || true
    kubectl delete pvc -n "$NAMESPACE" -l app.kubernetes.io/component=be --ignore-not-found >/dev/null || true
}

qb_log "cdc-qb-watch (${SUITE}): checking ${WATCH_RUNNER} every ${WATCH_INTERVAL_S}s"
while :; do
    why=$(test_over)
    [[ -n "$why" ]] && break
    last=$(kubectl logs -n "$NAMESPACE" "job/${WATCH_RUNNER}" --tail=200 2>/dev/null | grep '\[querybench\]' | tail -1 || true)
    qb_log "${SUITE} test still running: ${last:-no progress line yet}"
    sleep "$WATCH_INTERVAL_S"
done
qb_log "${SUITE} test over: ${why}"

( WRITER_DRAIN_S=0 qb_stop_generator ) || qb_log "WARNING: could not stop the generator"
if ( qb_stop_writers ); then
    if [[ "${SUSPEND_WRITERS:-1}" == 0 ]]; then
        qb_log "SUSPEND_WRITERS=0: job monitor left running"
    elif ! kubectl get deployment "$MONITOR" -n "$NAMESPACE" >/dev/null 2>&1; then
        qb_log "no ${MONITOR} deployment"
    elif kubectl scale "deployment/${MONITOR}" -n "$NAMESPACE" --replicas=0 >/dev/null 2>&1; then
        qb_log "job monitor stopped (${MONITOR} scaled to 0)"
    else
        qb_log "WARNING: could not stop the job monitor (${MONITOR})"
    fi
else
    qb_log "WARNING: could not stop the writers (see above); job monitor left running"
fi
if [[ "${STOP_DUCKDB:-1}" == 0 ]]; then
    qb_log "STOP_DUCKDB=0: ${DUCK} left running"
elif kubectl delete deploy "$DUCK" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null \
     && kubectl delete configmap "$DUCK" -n "$NAMESPACE" --ignore-not-found >/dev/null; then
    qb_log "${DUCK} stopped"
else
    qb_log "WARNING: could not stop ${DUCK}"
fi
if [[ "${STOP_STARROCKS:-1}" == 0 ]]; then
    qb_log "STOP_STARROCKS=0: StarRocks left running"
elif starrocks_down; then
    qb_log "StarRocks stopped (FE/BE and volumes deleted; run.sh start installs it again)"
else
    qb_log "WARNING: could not stop StarRocks (throughput-benchmark/engine-deploy/deploy-starrocks.sh down by hand)"
fi
kubectl delete configmap "$WATCH_RUNNER" -n "$NAMESPACE" --ignore-not-found >/dev/null \
    && kubectl delete secret "$WATCH_RUNNER" -n "$NAMESPACE" --ignore-not-found >/dev/null \
    && qb_log "${WATCH_RUNNER}'s ConfigMap and Secret deleted" \
    || qb_log "WARNING: could not delete ${WATCH_RUNNER}'s ConfigMap/Secret"
echo "status: qb-watch-done $(date -u)"
