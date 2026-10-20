#!/usr/bin/env bash
# One session of the benchmark, step by step. Each step calls one component's
# run.sh; the session's values (TEST_SUFFIX, JSONGZ_REPLICAS, DURATION_SECONDS)
# come from .bench-env, through lib/common.sh.
#
#   ./bench.sh throughput     flink writers deploy both -> both RUNNING -> datagen start -> monitoring start
#   ./bench.sh hot            without a wait, start the hot query test (+ its watcher), while writers live
#   ./bench.sh snapshot       wait until the hot query test is complete, then start the snapshot query test (+ watcher); It auto-stops the writers first
#   ./bench.sh summary        bench-results/BENCH-SUMMARY-<session>.md, from the
#                             session's (TEST_SUFFIX's) newest hot and snapshot runs
#   ./bench.sh status         what is running
#   ./bench.sh cleanup        stop generator, suspend writers, stop monitoring, DuckDB down
#                             (no data is deleted: wiping a table set is README "Teardown")
#
# A typical session: edit TEST_SUFFIX in .bench-env, then
#   PAIMON_WRITE_ONLY=true PAIMON_WRITE_BUFFER_SPILLABLE=false ./bench.sh throughput
#   ./bench.sh hot                 # once both writers are RUNNING; its watcher ends the run
#   ./bench.sh snapshot            # after the hot test
#   ./bench.sh summary
# The Paimon write path is not a session value: set PAIMON_WRITE_* on the
# throughput command itself (flinkwriter/run.sh, "Paimon write path").
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "${HERE}/lib/common.sh"
cd "$HERE"

writers_running() {
    [[ "$(kubectl get flinkdeployment flink-cid-paimon flink-cid-iceberg -n "$NAMESPACE" \
           -o jsonpath='{.items[*].status.jobStatus.state}' 2>/dev/null)" == "RUNNING RUNNING" ]]
}

# Writers first, generator only once both are RUNNING: a failed deploy or a
# writer that never comes up then stops the run before any generator pod starts.
# The other way round, with DURATION_SECONDS=0, it left 7 generator pods
# writing with nothing to read them and nothing to stop them.
cmd_throughput() {
    log "1/4 writers on TEST_SUFFIX='${TEST_SUFFIX}' (PAIMON_WRITE_ONLY=${PAIMON_WRITE_ONLY:-default}, PAIMON_WRITE_BUFFER_SPILLABLE=${PAIMON_WRITE_BUFFER_SPILLABLE:-default})"
    ./flinkwriter/run.sh deploy both
    log "2/4 waiting for both writers to be RUNNING (up to 20 min)"
    local i
    for (( i = 0; i < 40; i++ )); do writers_running && break; sleep 30; done
    writers_running || die "the writers are not both RUNNING after 20 min: kubectl get flinkdeployment -n ${NAMESPACE}; the generator was not started"
    log "3/4 generator (JSONGZ_REPLICAS=${JSONGZ_REPLICAS:-?}, DURATION_SECONDS=${DURATION_SECONDS:-?})"
    ./datagen/run.sh start
    log "4/4 monitoring"
    ./monitoring/run.sh start
    log "throughput running. Next: ./bench.sh hot"
}

cmd_status() {
    echo "== .bench-env: TEST_SUFFIX='${TEST_SUFFIX}' JSONGZ_REPLICAS=${JSONGZ_REPLICAS:-} DURATION_SECONDS=${DURATION_SECONDS:-}"
    echo "== generator:";  kubectl get pods -n "$NAMESPACE" -l job-name=cid-jsongz-gen --no-headers 2>/dev/null | awk '{print "  " $1, $3}' | sort -k2 | uniq -c -f1 || true
    echo "== writers:";    kubectl get flinkdeployment -n "$NAMESPACE" -o custom-columns='  NAME:.metadata.name,SPEC:.spec.job.state,JOB:.status.jobStatus.state' --no-headers 2>/dev/null || true
    echo "== monitoring:"; kubectl get deploy flink-cid-monitor -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print "  flink-cid-monitor", $2}' || true
    echo "== query test:"; kubectl get jobs qb-runner qb-watch -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print "  " $1, $2}' || true
    echo "== duckdb-warm:"; kubectl get deploy duckdb-warm -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print "  duckdb-warm", $2}' || echo "  not running"
}

# Every step runs even if an earlier one fails (e.g. no monitor Deployment):
# stopping at the first failure left the DuckDB node, or the generator, up.
cmd_cleanup() {
    ./datagen/run.sh stop                                    || warn "cleanup: stopping the generator failed; carrying on"
    ( source "${HERE}/lib/querybench.sh"; qb_stop_writers )  || warn "cleanup: suspending the writers failed; carrying on"
    ./monitoring/run.sh stop                                 || warn "cleanup: stopping monitoring failed; carrying on"
    ./querybench/duckdb-warm.sh down                         || warn "cleanup: taking duckdb-warm down failed"
    log "stopped. Data is kept; to wipe the table set see README \"Teardown\"."
}

case "${1:-}" in
    throughput) cmd_throughput ;;
    hot)        SUITE=hot ./querybench/run.sh start ;;
    snapshot)   SUITE=snapshot ./querybench/run.sh start ;;
    summary)    ./summary/run.sh ;;
    status)     cmd_status ;;
    cleanup)    cmd_cleanup ;;
    *)          sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
