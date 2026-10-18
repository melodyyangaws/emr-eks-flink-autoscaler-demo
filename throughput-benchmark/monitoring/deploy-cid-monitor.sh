#!/usr/bin/env bash
set -euo pipefail

# Deploy the throughput-benchmark monitor — the same monitoring mechanics as
# cdc-pipeline/monitoring, as a second Deployment pointed at the flink-cid-*
# jobs. The monitor code lives in this directory: it started as a copy of
# cdc-pipeline/monitoring and has since diverged (operator-scoped record
# counts, so a fully chained Iceberg job no longer reads ~3 rec/s).
#
# The code travels via S3 (s3://${BUCKET_NAME}/flink/monitoring/), next to
# every other artifact the cluster runs; the pod fetches it at boot, so
# `kubectl rollout restart` alone re-pulls the latest code.
#
# It also starts collect-metrics.sh locally for each RUNNING flink-cid-* job
# (Paimon on LOCAL_PORT 8091, Iceberg on 8092), so the per-30 s CSVs in
# bench-results/ come with the side-by-side table. They outlive this script;
# `stop` ends them. Starting again replaces any collectors already running.
#
# Usage:
#   ./deploy-cid-monitor.sh          upload the code, apply, restart, wait, start collectors
#   ./deploy-cid-monitor.sh stop     scale to 0 (Deployment and config kept), stop collectors
#
# Knobs: COLLECT=0 skips the collectors; COLLECT_INTERVAL (30) and
# COLLECT_DURATION (7200) are passed to collect-metrics.sh.

export AWS_REGION="${AWS_REGION:-us-west-2}"
export NAMESPACE="${NAMESPACE:-emr-flink}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)/bench-results"
# One "<pid> <format>" line per collector this script started.
COLLECTOR_PIDS="${SCRIPT_DIR}/.collectors.pid"

stop_collectors() {
    [[ -f "${COLLECTOR_PIDS}" ]] || return 0
    local pid fmt
    while read -r pid fmt; do
        # The collector's port-forward is its child; kill it first, because a
        # SIGTERM'd bash does not run the collector's EXIT trap.
        pkill -P "${pid}" 2>/dev/null || true
        kill "${pid}" 2>/dev/null && echo "  ✓ stopped ${fmt} collector (pid ${pid})"
    done < "${COLLECTOR_PIDS}"
    rm -f "${COLLECTOR_PIDS}"
}

start_collectors() {
    [[ "${COLLECT:-1}" == 0 ]] && { echo "  COLLECT=0: collectors not started"; return 0; }
    mkdir -p "${RESULTS_DIR}"
    local fmt port=8091 stamp state log
    stamp=$(date -u +%Y%m%dT%H%M%SZ)
    for fmt in paimon iceberg; do
        state=$(kubectl get flinkdeployment "flink-cid-${fmt}" -n "${NAMESPACE}" \
                  -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)
        if [[ "${state}" != RUNNING ]]; then
            echo "  - ${fmt}: job ${state:-not deployed}, no collector"
        else
            log="${RESULTS_DIR}/collect-${fmt}-${stamp}.log"
            LOCAL_PORT=${port} nohup "${SCRIPT_DIR}/collect-metrics.sh" "${fmt}" \
                "${COLLECT_INTERVAL:-30}" "${COLLECT_DURATION:-7200}" > "${log}" 2>&1 &
            echo "$! ${fmt}" >> "${COLLECTOR_PIDS}"
            echo "  ✓ ${fmt} collector pid $! on :${port} -> bench-results/cid-${fmt}-*.csv (log ${log##*/})"
        fi
        port=$(( port + 1 ))
    done
}

if [[ "${1:-}" == "stop" ]]; then
    kubectl scale deployment/flink-cid-monitor -n "${NAMESPACE}" --replicas=0
    stop_collectors
    echo "✓ Monitor stopped. Start it again with: $0"
    exit 0
fi

export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="${BUCKET_NAME:-emr-on-eks-test-${AWS_ACCOUNT_ID}-${AWS_REGION}}"
export EMR_EXECUTION_ROLE_ARN="${EMR_EXECUTION_ROLE_ARN:-arn:aws:iam::${AWS_ACCOUNT_ID}:role/emr-on-eks-test-execution-role}"
export MONITOR_SERVICE_ACCOUNT="${MONITOR_SERVICE_ACCOUNT:-starrocks-sa}"
# The pod reads this same value from the config ConfigMap. No trailing slash:
# "flink/monitoring//entrypoint.py" is a different, non-existent S3 key.
export MONITOR_CODE_PREFIX="${MONITOR_CODE_PREFIX:-s3://${BUCKET_NAME}/flink/monitoring}"
MONITOR_CODE_PREFIX="${MONITOR_CODE_PREFIX%/}"

DEPLOYED_MANIFEST="${SCRIPT_DIR}/monitor-deployment-deployed.yaml"
# Must match the FETCH list in monitor-deployment.yaml.
MONITOR_FILES=(flink_cid_monitor.py entrypoint.py requirements.txt)

[[ -f "${SCRIPT_DIR}/flink_cid_monitor.py" ]] || {
    echo "ERROR: monitor code not found at ${SCRIPT_DIR}" >&2; exit 1; }

echo "Flink throughput monitor -> ${NAMESPACE} (SA ${MONITOR_SERVICE_ACCOUNT}, code ${MONITOR_CODE_PREFIX})"

# IRSA — idempotent, never clobbers an existing annotation.
CURRENT_ROLE=$(kubectl get sa "${MONITOR_SERVICE_ACCOUNT}" -n "${NAMESPACE}" \
  -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}' 2>/dev/null || true)
if [[ -z "${CURRENT_ROLE}" ]]; then
  kubectl annotate serviceaccount -n "${NAMESPACE}" "${MONITOR_SERVICE_ACCOUNT}" \
    eks.amazonaws.com/role-arn="${EMR_EXECUTION_ROLE_ARN}"
fi

for f in "${MONITOR_FILES[@]}"; do
    aws s3 cp "${SCRIPT_DIR}/${f}" "${MONITOR_CODE_PREFIX}/${f}" \
      --region "${AWS_REGION}" --only-show-errors
    echo "  ✓ uploaded ${f}"
done

# Leftover from when the code rode in a ConfigMap; a no-op once it is gone.
kubectl delete configmap flink-cid-monitor-code -n "${NAMESPACE}" --ignore-not-found

envsubst '${AWS_REGION} ${BUCKET_NAME} ${NAMESPACE} ${MONITOR_SERVICE_ACCOUNT} ${MONITOR_CODE_PREFIX}' \
  < "${SCRIPT_DIR}/monitor-deployment.yaml" > "${DEPLOYED_MANIFEST}"
# envsubst leaves anything outside its allow-list as a literal ${...}, which is
# valid YAML and only fails later, inside the pod.
if grep -n '\${' "${DEPLOYED_MANIFEST}"; then
    echo "ERROR: unsubstituted \${...} above; add it to the envsubst list." >&2
    exit 1
fi

kubectl apply -f "${DEPLOYED_MANIFEST}"
kubectl rollout restart deployment/flink-cid-monitor -n "${NAMESPACE}"
kubectl rollout status deployment/flink-cid-monitor -n "${NAMESPACE}" --timeout=300s

stop_collectors
start_collectors

echo "✓ Deployed:  kubectl logs -f deployment/flink-cid-monitor -n ${NAMESPACE}"
