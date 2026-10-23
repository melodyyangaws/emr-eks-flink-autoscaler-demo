#!/usr/bin/env bash
# Run a query test from INSIDE the cluster, so closing the laptop does not stop
# it, with a watcher that shuts the run down once the test ends.
#
#   SUITE=hot      ./querybench/run.sh start   the hot test (hot-interleave.sh)
#   SUITE=snapshot ./querybench/run.sh start   the snapshot test (starrocks.sh, then duckdb-warm.sh run)
#   ./querybench/run.sh logs [watch]           follow the runner's output (watch: the watcher's)
#
# Results: when the test ends, the runner uploads its bench-results/ to
# s3://$BUCKET_NAME/querybench-results/session<TEST_SUFFIX>/<STAMP>-<suite>/
# (STAMP: set it, or now), where summary/run.sh finds the session's runs
# and the Job ends; `start` prints the `aws s3 sync` that copies them here.
# Finished Jobs delete themselves after a day (their logs stay readable until
# then); the watcher deletes the runner's ConfigMap and Secret.
#
# `start` runs, in this order (any failure stops it before a Job is created):
#   0. wait while a qb-runner is still running a test (FORCE=1: don't);
#   1. delete any old watcher Job (qb-watch);
#   2. snapshot only: stop the generator, suspend the writers and wait until
#      they are down, then resolve SNAPSHOT_DT and the S4 slice from the
#      tables' commit history (set ones are kept);
#   2b. snapshot only, when .bench-env sets COMPACT_BEFORE_SNAPSHOT=true:
#      compact both tables once (compaction/run.sh run both), after the
#      windows are fixed and before the caches are emptied;
#   3a. make sure StarRocks is up: installed (engine-deploy/deploy-starrocks.sh
#      install) if its FE or BEs are missing, e.g. after the previous test's
#      watcher took it down;
#   3. make sure duckdb-warm is up (started if not; a ready one is left as is),
#      then validate it: rollout ready and /health, read inside the pod,
#      answering busy=false;
#   4. empty both engines' caches (clear-caches.sh), so the test starts cold.
#      Hot only: skipped when step 3 started BOTH engines just now (a fresh
#      StarRocks and DuckDB have empty caches already); the snapshot test
#      always clears;
#   5. start the runner Job (qb-runner) with the suite's test;
#   6. start the watcher Job (qb-watch, qb-watch.sh), always, for both suites.
# The order matters: a missing qb-runner Job counts as a finished test, so a
# watcher started before the runner, or left over while the runner is
# replaced, would stop everything at once.
#
# Steps 1-4 run here, on the laptop: they need envsubst (engine-deploy/deploy-duckdb-warm.sh up),
# which the pod lacks, and the writers stopped before the windows. The pods
# get the run's settings from this shell: every PASS_VARS variable that is set
# here is exported to them (e.g. `SUITE=hot RUNS=1 ./querybench/run.sh start`). The
# session's values (TEST_SUFFIX, ...) come from .bench-env, here and in the pod.
#
# Both Jobs, their ServiceAccount (with the execution role, for the S3 upload)
# and namespace Role come from qb-runner.yaml. The pod has kubectl, bash,
# python3, curl and the AWS CLI.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# .bench-env, NAMESPACE, TEST_SUFFIX, log/warn, render.
# shellcheck source=../lib/common.sh
source "${HERE}/../lib/common.sh"
TB="$BENCH_DIR"
# Exported to the pods when set in this shell: the run knobs that the scripts
# running there (hot-interleave.sh, starrocks.sh, duckdb-warm.sh run, lib/querybench.sh,
# qb-watch.sh) read. Not here: names derived from TEST_SUFFIX, the pins
# hot-interleave.sh sets itself, deploy-duckdb-warm.sh's server sizing (`up` runs on
# the laptop), SNAPSHOT_MINUTES / SNAPSHOT_S4_MINUTES (resolved on the laptop) and SUITE, which
# each Job's step sets itself.
PASS_VARS="STAMP TEST_SUFFIX RUNS WARMUP ARMS FORMAT ALLOW_CONCURRENT
HOT_TO HOT_FROM HOT_DT HOT_WINDOW_SECONDS STABLE_CHECKS WAIT_COMMIT_S
SNAPSHOT_DT SNAPSHOT_FROM SNAPSHOT_TO SNAPSHOT_S4_FROM SNAPSHOT_S4_TO
WATCH_INTERVAL_S SUSPEND_WRITERS STOP_STARROCKS PROFILE_SCAN"
# kubectl pinned to the cluster's own version. "stable" moved on 2026-10-18 to a
# release that segfaults on these nodes, and the runner sat in its first kubectl
# loop for an hour without a single error line.
KUBECTL_VERSION="${KUBECTL_VERSION:-$(kubectl version -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"].split("-")[0])' 2>/dev/null || echo v1.35.6)}"

die() { echo "$*; no query Job left running" >&2; exit 1; }
# lib/querybench.sh functions, in throughput-benchmark/, as the harnesses run.
lib() { ( cd "$TB"; source "${TB}/lib/querybench.sh"; "$@" ); }

# ── One Job: the bundle (querybench/, .bench-env, the step) + the template ───
start_job() {   # <name> <step>
    local name="$1" step="$2" d v
    d=$(mktemp -d)
    { echo '#!/bin/bash'; echo 'set -uo pipefail'; echo 'cd /work && source .bench-env'
      for v in $PASS_VARS; do [[ -n "${!v:-}" ]] && printf 'export %s=%q\n' "$v" "${!v}"; done
      printf '# step: %s\n' "${step%%$'\n'*}"      # what the pod's log shows first
      echo "$step"; } > "${TB}/.qb-runner-cmd.sh"
    tar -C "$TB" -czf "${d}/bundle.tgz" querybench engine-deploy lib .bench-env .qb-runner-cmd.sh
    rm -f "${TB}/.qb-runner-cmd.sh"
    delete_job "$name"
    # Each kubectl checked: start_job also runs inside `( ... ) ||` (the
    # watcher), where set -e is off and a failure would otherwise pass.
    kubectl create configmap "$name" -n "$NAMESPACE" --from-file="${d}/bundle.tgz" >/dev/null \
        || die "could not create ConfigMap ${name}"
    kubectl create secret generic "$name" -n "$NAMESPACE" --from-file=bench-env-local="${TB}/.bench-env.local" >/dev/null \
        || die "could not create Secret ${name}"
    rm -rf "$d"
    local out="${HERE}/${name}-deployed.yaml" NAME="$name" ROLE_ARN="$EMR_EXECUTION_ROLE_ARN"
    render "${HERE}/qb-runner.yaml" "$out" NAME NAMESPACE KUBECTL_VERSION ROLE_ARN || die "could not render ${out##*/}"
    kubectl apply -f "$out" >/dev/null || die "could not apply ${out##*/}"
    echo "job/${name} started: ${step%%$'\n'*} (${out##*/})"
}

delete_job() {   # <name>
    kubectl delete job "$1" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    kubectl delete configmap "$1" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl delete secret "$1" -n "$NAMESPACE" --ignore-not-found >/dev/null
}

# ── Steps 2-3 ────────────────────────────────────────────────────────────────
stop_writers() {
    echo "snapshot: stopping the generator and the writers"
    kubectl delete job cid-jsongz-gen -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    lib qb_stop_writers || die "could not stop the writers"
}

# Writers stopped, so the partition and its commits are final.
resolve_snapshot() {
    local r
    r=$(lib eval 'qb_resolve_snapshot >&2; printf "%s\n" "$QB_SNAPSHOT_DT" "$QB_SNAP_FROM" "$QB_SNAP_TO" "$QB_S4_FROM" "$QB_S4_TO"') \
        || die "could not resolve the snapshot windows"
    { read -r SNAPSHOT_DT; read -r SNAPSHOT_FROM; read -r SNAPSHOT_TO; read -r SNAPSHOT_S4_FROM; read -r SNAPSHOT_S4_TO; } <<<"$r"
    export SNAPSHOT_DT SNAPSHOT_FROM SNAPSHOT_TO SNAPSHOT_S4_FROM SNAPSHOT_S4_TO
    echo "  snapshot windows: dt=${SNAPSHOT_DT} S1-S3=[${SNAPSHOT_FROM}, ${SNAPSHOT_TO}) S4=[${SNAPSHOT_S4_FROM}, ${SNAPSHOT_S4_TO})"
}

# /health read inside the pod (no port-forward); empty if not ready or no answer.
duckdb_health() {
    kubectl rollout status -n "$NAMESPACE" deploy/duckdb-warm --timeout=5s >/dev/null 2>&1 || return 0
    kubectl exec -n "$NAMESPACE" deploy/duckdb-warm -c duckdb -- python3 -c \
        'import json, urllib.request; print(json.dumps(json.load(urllib.request.urlopen("http://localhost:8080/health", timeout=10))))' \
        2>/dev/null || true
}

# The FE and all 3 BEs Ready, else a fresh install (Helm, on the laptop).
STARTED_FRESH=""   # the engines step 3 started from scratch (step 4 reads it)
starrocks_up() {
    local fe be
    fe=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=fe -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null | grep -o True | wc -l | tr -d ' ')
    be=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=be -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null | grep -o True | wc -l | tr -d ' ')
    if (( fe >= 1 && be >= 3 )); then
        echo "${SUITE}: StarRocks already up (FE ${fe}, BE ${be})"
    else
        echo "${SUITE}: starting StarRocks (FE ${fe}, BE ${be} ready)"
        STARTED_FRESH+=" starrocks"
        "${TB}/engine-deploy/deploy-starrocks.sh" install >/dev/null || die "StarRocks did not come up (engine-deploy/deploy-starrocks.sh install)"
        local i; for (( i = 0; i < 80; i++ )); do
            be=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=be -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null | grep -o True | wc -l | tr -d ' ')
            (( be >= 3 )) && break; sleep 15
        done
        (( be >= 3 )) || die "StarRocks BEs not Ready 20 min after install"
        echo "  StarRocks installed (FE + ${be} BEs)"
    fi
}

duckdb_up() {
    local h; h=$(duckdb_health)
    if [[ -n "$h" ]]; then
        echo "${SUITE}: duckdb-warm already up"
    else
        echo "${SUITE}: starting duckdb-warm"
        STARTED_FRESH+=" duckdb"
        "${TB}/engine-deploy/deploy-duckdb-warm.sh" up || die "duckdb-warm did not come up"
        h=$(duckdb_health)
    fi
    [[ "$h" == *'"busy": false'* ]] || die "duckdb-warm is not ready and idle (/health: ${h:-no answer})"
    echo "  duckdb-warm validated: ${h:0:160}"
}

# ── start: steps 1-6 ─────────────────────────────────────────────────────────
# A runner still active (a hot test, typically, when the snapshot test is
# started): wait for it instead of breaking it. Every later step would, i.e.
# stop its writers, restart the BEs under it, delete its Job before it uploads,
# and its StarRocks arm (running detached in the FE pod) would overlap the new
# test. Checked before anything changes. FORCE=1 skips the wait.
wait_for_runner() {
    local active
    while :; do
        active=$(kubectl get job qb-runner -n "$NAMESPACE" -o jsonpath='{.status.active}' 2>/dev/null || true)
        [[ "${active:-0}" == 0 ]] && return 0
        [[ "${FORCE:-0}" == 1 ]] && { warn "FORCE=1: replacing the qb-runner that is still running"; return 0; }
        log "qb-runner is still running a test; waiting for it to finish (checked every 60 s; FORCE=1 to replace it)"
        sleep 60
    done
}

cmd_start() {
    local step results
    : "${BUCKET_NAME:?not set in .bench-env(.local)}" "${EMR_EXECUTION_ROLE_ARN:?not set in .bench-env(.local)}"
    case "${SUITE:-}" in
        hot)      step='./querybench/hot-interleave.sh' ;;    # sets SUITE=hot itself
        snapshot) step='export SUITE=snapshot; ./querybench/starrocks.sh; echo "status: starrocks-exit=$?"; ./querybench/duckdb-warm.sh run' ;;
        *)        die "set SUITE=hot or SUITE=snapshot" ;;
    esac
    # One STAMP for the CSV names and the results prefix.
    export STAMP="${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
    # The bucket stays $BUCKET_NAME here and in the pod (filled in there from
    # .bench-env.local): its name carries the account ID, which must not land in
    # the pod's log or in what is copied from it.
    results="${QB_RESULTS_PREFIX}/${STAMP}-${SUITE}/"
    step+=$'\n''rc=$?; aws s3 sync bench-results "s3://${BUCKET_NAME}/'"${results}"'" --only-show-errors \
  && echo "status: results-uploaded '"${results}"'" || echo "status: results-upload-failed"; exit $rc'
    wait_for_runner                                                # 0
    delete_job qb-watch                                            # 1
    if [[ "$SUITE" == snapshot ]]; then stop_writers; resolve_snapshot; fi   # 2
    if [[ "$SUITE" == snapshot && "${COMPACT_BEFORE_SNAPSHOT:-false}" == true ]]; then  # 2b
        "${TB}/compaction/run.sh" run both || die "the compaction before the snapshot test failed"
    fi
    starrocks_up                                                   # 3a
    duckdb_up                                                      # 3
    if [[ "$SUITE" == hot && "$STARTED_FRESH" == *starrocks* && "$STARTED_FRESH" == *duckdb* ]]; then   # 4
        echo "hot: both engines started fresh just now, so their caches are empty: not clearing them"
    else
        "${HERE}/clear-caches.sh" || die "could not clear the caches"
    fi
    start_job qb-runner "$step"                                    # 5
    # A runner without its watcher would never stop the generator or the
    # writers (DURATION_SECONDS=0): if the watcher fails to start, the runner goes too.
    ( start_job qb-watch "SUITE=${SUITE} ./querybench/qb-watch.sh" ) \
        || { delete_job qb-runner; die "the watcher did not start, so the runner was deleted"; }   # 6
    echo "  logs: $0 logs (runner), $0 logs watch (watcher)"
    echo "  results, once the runner logs \"status: results-uploaded\":"
    echo "    aws s3 sync s3://\$BUCKET_NAME/${results} bench-results/    # in throughput-benchmark/, after source .bench-env"
}

case "${1:-}" in
    start) cmd_start ;;
    logs)  kubectl logs -f -n "$NAMESPACE" "job/$([[ "${2:-}" == watch ]] && echo qb-watch || echo qb-runner)" ;;
    *)     echo "usage: SUITE=hot|snapshot $0 start | $0 logs [watch]" >&2; exit 1 ;;
esac
