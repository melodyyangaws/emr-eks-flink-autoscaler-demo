#!/usr/bin/env bash
# Watch one CDC query test's runner (cdc-qb-runner) and shut the run down once
# it ends. Not started by hand: `SUITE=hot|snapshot ./querybench/run.sh start`
# starts it as the Job cdc-qb-watch right after the runner. Adapted from
# throughput-benchmark/querybench/qb-watch.sh.
#
# Every WATCH_INTERVAL_S (600) it reads the runner's log; the test is over when
# the runner prints "status: runner-exit=", or its Job or pod is gone or failed.
#
# Only a COMPLETED test is torn down: "status: runner-exit=0". Any other end (a
# non-zero exit, a failed or vanished runner pod or Job) leaves the generator,
# writers, job monitor, DuckDB and Trino RUNNING and prints a report: why it
# ended, the runner's last error lines and what is still up. A setup error then
# costs a fix and a re-run, not a new MySQL snapshot. Tear down by hand with
# `./querybench/run.sh stop` (= this script's `teardown`, steps 1-7 below).
#
#   ./querybench/qb-watch.sh            watch (the cdc-qb-watch Job runs this)
#   ./querybench/qb-watch.sh teardown   steps 1-7 now, no watching (run.sh stop)
#
# Teardown steps:
#   1. scales the MySQL generator to 0 (STOP_GENERATOR=0 leaves it);
#   2. suspends the CDC writers (SUSPEND_WRITERS=0 leaves them; they resume
#      from their last checkpoint: upgradeMode last-state);
#   3. scales the CDC job monitor (flink-cdc-monitor) to 0 once the writers stopped;
#   4. takes duckdb-warm-cdc down (STOP_DUCKDB=0 leaves it);
#   5. scales Trino's coordinator and workers to 0 (STOP_TRINO=0 leaves them):
#      this pod has no helm, so not deploy-trino.sh down; the release stays,
#      the cache volumes go with their pods, and run.sh start scales them back
#      and runs deploy-trino.sh up;
#   6. suspends the hourly Iceberg compaction CronJob and cancels a run in
#      progress (STOP_COMPACTION_CRON=0 leaves it; a hot run.sh start resumes it);
#   7. deletes the runner's ConfigMap and Secret.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
case "$SUITE" in hot|snapshot) ;; *) qb_die "qb-watch needs SUITE=hot|snapshot, got '${SUITE}'" ;; esac
WATCH_INTERVAL_S="${WATCH_INTERVAL_S:-600}"
WATCH_RUNNER=cdc-qb-runner
DUCK="$DUCKDB_NAME"
MONITOR="${CDC_MONITOR:-flink-cdc-monitor}"
COMPACT_CRON="${CRON_NAME:-iceberg-compact-hourly}"

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

trino_down() {
    local d ok=0
    for d in trino-coordinator "$TRINO_WORKER_DEPLOY"; do
        kubectl get deploy "$d" -n "$NAMESPACE" >/dev/null 2>&1 || continue
        kubectl scale "deploy/${d}" -n "$NAMESPACE" --replicas=0 >/dev/null || ok=1
    done
    return $ok
}

teardown() {
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
if [[ "${STOP_TRINO:-1}" == 0 ]]; then
    qb_log "STOP_TRINO=0: Trino left running"
elif trino_down; then
    qb_log "Trino scaled to 0 (run.sh start brings it back; engine-deploy/deploy-trino.sh down removes the release)"
else
    qb_log "WARNING: could not scale Trino to 0 (engine-deploy/deploy-trino.sh down by hand)"
fi
if [[ "${STOP_COMPACTION_CRON:-1}" == 0 ]]; then
    qb_log "STOP_COMPACTION_CRON=0: cronjob/${COMPACT_CRON} left as is"
elif ! kubectl get cronjob "$COMPACT_CRON" -n "$NAMESPACE" >/dev/null 2>&1; then
    qb_log "no cronjob/${COMPACT_CRON}"
elif kubectl patch cronjob "$COMPACT_CRON" -n "$NAMESPACE" --type merge -p '{"spec":{"suspend":true}}' >/dev/null; then
    for j in $(kubectl get jobs -n "$NAMESPACE" -l "app=${COMPACT_CRON}" -o jsonpath='{range .items[?(@.status.active)]}{.metadata.name}{" "}{end}' 2>/dev/null); do
        kubectl delete job "$j" -n "$NAMESPACE" --wait=false >/dev/null && qb_log "cancelled running job/${j}"
    done
    qb_log "cronjob/${COMPACT_CRON} suspended (a hot run.sh start resumes it)"
else
    qb_log "WARNING: could not suspend cronjob/${COMPACT_CRON}"
fi
kubectl delete configmap "$WATCH_RUNNER" -n "$NAMESPACE" --ignore-not-found >/dev/null \
    && kubectl delete secret "$WATCH_RUNNER" -n "$NAMESPACE" --ignore-not-found >/dev/null \
    && qb_log "${WATCH_RUNNER}'s ConfigMap and Secret deleted" \
    || qb_log "WARNING: could not delete ${WATCH_RUNNER}'s ConfigMap/Secret"
}

# What is still up after a failed test, one line each (read-only).
report_running() {
    local d st
    for d in ${FLINK_JOBS:-flink-cdc-paimon flink-cdc-iceberg}; do
        st=$(kubectl get flinkdeployment "$d" -n "$NAMESPACE" -o jsonpath='{.status.jobStatus.state}/{.spec.job.state}' 2>/dev/null || true)
        qb_log "  writer ${d}: ${st:-not deployed}"
    done
    for d in ${GENERATOR_RESOURCES:-statefulset/mysql-upsert-loadgen}; do
        st=$(kubectl get "$d" -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>/dev/null || true)
        qb_log "  generator ${d}: ${st:-not deployed} ready"
    done
    for d in "$MONITOR" "$DUCK" trino-coordinator "$TRINO_WORKER_DEPLOY"; do
        st=$(kubectl get deploy "$d" -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>/dev/null || true)
        qb_log "  ${d}: ${st:-not deployed} ready"
    done
}

if [[ "${1:-}" == teardown ]]; then
    qb_log "teardown requested (${SUITE})"
    teardown
    echo "status: qb-teardown-done $(date -u)"
    exit 0
fi

qb_log "cdc-qb-watch (${SUITE}): checking ${WATCH_RUNNER} every ${WATCH_INTERVAL_S}s"
while :; do
    why=$(test_over)
    [[ -n "$why" ]] && break
    last=$(kubectl logs -n "$NAMESPACE" "job/${WATCH_RUNNER}" --tail=200 2>/dev/null | grep '\[querybench\]' | tail -1 || true)
    qb_log "${SUITE} test still running: ${last:-no progress line yet}"
    sleep "$WATCH_INTERVAL_S"
done
qb_log "${SUITE} test over: ${why}"

if [[ "$why" == "status: runner-exit=0"* ]]; then
    qb_log "${SUITE} test completed: tearing the run down"
    teardown
    echo "status: qb-watch-done $(date -u)"
else
    qb_log "${SUITE} test did NOT complete (${why}): leaving everything RUNNING, no teardown"
    qb_log "last runner errors:"
    kubectl logs -n "$NAMESPACE" "job/${WATCH_RUNNER}" --tail=400 2>/dev/null \
        | grep -E 'ERROR|FAILED|Exception|error:|refus|die' | tail -15 | sed 's/^/    /' \
        || qb_log "    (runner log unavailable)"
    qb_log "still running:"
    report_running
    qb_log "fix and re-run with SUITE=${SUITE} ./querybench/run.sh start, or tear down with SUITE=${SUITE} ./querybench/run.sh stop"
    echo "status: qb-watch-failed-left-running $(date -u)"
fi
