#!/usr/bin/env bash
# Run a query-benchmark step from INSIDE the cluster, so closing the laptop
# does not stop it. starrocks.sh already runs each arm inside the FE pod, but the
# harness itself (polling, reading the audit log, the warm DuckDB half through
# port-forwards) ran on the laptop and died with it.
#
#   ./qb-runner.sh start <cmd...>   ship querybench/ + .bench-env(.local) and run
#                                   <cmd> in a Job (qb-runner) on driver-nodepool
#   ./qb-runner.sh logs             follow the Job's output
#   ./qb-runner.sh fetch [dir]      copy its bench-results/ out (default ../bench-results),
#                                   then delete the Job, its ConfigMap and Secret
#                                   (KEEP=1 keeps them for another fetch)
#
# <cmd> runs in throughput-benchmark/ with .bench-env sourced, e.g.
#   ./qb-runner.sh start 'WAIT_FOR_SR_RUN=20261017T140929Z_71523 STAMP=... ./querybench/duckdb-warm.sh run'
# WAIT_FOR_SR_RUN=<RUN_ID> first waits until that starrocks.sh run has nothing
# left in the FE pod (no running arm file, no cid_events query), so the two
# engines never overlap.
#
# The Job, its ServiceAccount and namespace Role are in qb-runner.yaml (pods
# exec/port-forward, deployments restart, flinkdeployments read/patch, jobs
# read/delete for hot-watch.sh); this
# script only ships the bundle and applies that template. The pod has kubectl,
# bash, python3, curl. It has NO AWS credentials: give it pinned windows
# (SNAPSHOT_DT, SNAPSHOT_S4_FROM/TO; HOT_TO) so lib.sh never calls aws. The DuckDB
# servers and StarRocks read S3 with their own roles. After <cmd> the pod stays
# up 1 day so `fetch` can copy the results; `fetch` then ends it.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TB="$(cd "${HERE}/.." && pwd)"
NAMESPACE="${NAMESPACE:-emr-flink}"
# QB_RUNNER_NAME runs a second Job next to qb-runner, e.g. hot-watch.sh while
# the hot test runs; logs/fetch/delete take the same variable.
NAME="${QB_RUNNER_NAME:-qb-runner}"
# kubectl pinned to the cluster's own version. "stable" moved on 2026-10-18 to a
# release that segfaults on these nodes, and the runner sat in its first kubectl
# loop for an hour without a single error line.
KUBECTL_VERSION="${KUBECTL_VERSION:-$(kubectl version -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"].split("-")[0])' 2>/dev/null || echo v1.35.6)}"

# The ServiceAccount, Role, RoleBinding and Job, from qb-runner.yaml. Only these
# three variables are substituted; the container's own $... stays literal.
render_manifest() {
    local y
    command -v envsubst >/dev/null || { echo "envsubst not found (brew install gettext)" >&2; exit 1; }
    y=$(export NAME NAMESPACE KUBECTL_VERSION; envsubst '${NAME} ${NAMESPACE} ${KUBECTL_VERSION}' < "${HERE}/qb-runner.yaml")
    if grep -nE '\$\{(NAME|NAMESPACE|KUBECTL_VERSION)\}' <<<"$y" >&2; then
        echo "unrendered placeholder in qb-runner.yaml" >&2; exit 1
    fi
    printf '%s\n' "$y"
}

cmd_start() {
    [[ $# -gt 0 ]] || { echo "usage: $0 start '<cmd>'" >&2; exit 1; }
    local d; d=$(mktemp -d)
    # The command and the wait go in a file in the bundle: no quoting through
    # the Job's YAML.
    cat > "${d}/cmd.sh" <<'EOS'
#!/bin/bash
set -uo pipefail
cd /work
source .bench-env
FE_POD="${FE_POD:-kube-starrocks-fe-0}"
fe() { kubectl exec -n "$NAMESPACE" "$FE_POD" -c fe -- "$@" 2>/dev/null; }
# Wait until the given starrocks.sh run has nothing left in the FE pod: no arm
# script without its exit file, and no cid_events query running.
# grep -c exits 1 when it counts 0, hence the `; true`: without it the idle
# answer "0" also tripped `|| echo unreachable` and the wait never ended (2026-10-17).
wait_for_sr_run() {   # <RUN_ID>
    local busy
    while :; do
        busy=$(fe sh -c "for s in /tmp/qb-$1-*.sql; do [ -f \"\$s\" ] && [ ! -f \"\${s%.sql}.rc\" ] && echo \"\$s\"; done; mysql -h127.0.0.1 -P9030 -uroot -N -B -e 'SHOW PROCESSLIST' | grep -c cid_events; true" || echo unreachable)
        [[ "$busy" == 0 ]] && { echo "== StarRocks run $1 finished $(date -u)"; return 0; }
        echo "   waiting for StarRocks run $1: $(echo $busy | tr '\n' ' ') $(date -u +%H:%M:%SZ)"
        sleep 60
    done
}
[[ -n "${WAIT_FOR_SR_RUN:-}" ]] && wait_for_sr_run "$WAIT_FOR_SR_RUN"
EOS
    printf '%s\n' "$*" >> "${d}/cmd.sh"
    cp "${d}/cmd.sh" "${TB}/.qb-runner-cmd.sh"
    tar -C "$TB" -czf "${d}/bundle.tgz" querybench .bench-env .qb-runner-cmd.sh
    rm -f "${TB}/.qb-runner-cmd.sh"
    kubectl delete job "$NAME" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    kubectl delete configmap "$NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl delete secret "$NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl create configmap "$NAME" -n "$NAMESPACE" --from-file="${d}/bundle.tgz" >/dev/null
    kubectl create secret generic "$NAME" -n "$NAMESPACE" --from-file=bench-env-local="${TB}/.bench-env.local" >/dev/null
    rm -rf "$d"
    render_manifest | kubectl apply -f - >/dev/null
    echo "job/${NAME} started: $*"
}

case "${1:-}" in
    start)  shift; cmd_start "$@" ;;
    logs)   kubectl logs -f -n "$NAMESPACE" "job/${NAME}" ;;
    # The pod only sleeps after the step so its results can be copied out (it has
    # no AWS credentials to upload them). A successful fetch therefore deletes the
    # Job; KEEP=1 keeps it for another fetch.
    fetch)  p=$(kubectl get pods -n "$NAMESPACE" -l job-name="$NAME" -o jsonpath='{.items[0].metadata.name}')
            kubectl cp -n "$NAMESPACE" "${p}:/work/bench-results" "${2:-${TB}/bench-results}" || exit 1
            if [[ "${KEEP:-0}" != 1 ]]; then
                kubectl delete job "$NAME" -n "$NAMESPACE" --ignore-not-found
                kubectl delete configmap "$NAME" -n "$NAMESPACE" --ignore-not-found
                kubectl delete secret "$NAME" -n "$NAMESPACE" --ignore-not-found
            fi ;;
    *)      echo "usage: $0 start '<cmd>' | logs | fetch [dir]" >&2; exit 1 ;;
esac
