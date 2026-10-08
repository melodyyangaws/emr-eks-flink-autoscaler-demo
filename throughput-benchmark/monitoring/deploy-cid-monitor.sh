#!/usr/bin/env bash
set -euo pipefail

# Deploy the throughput-benchmark monitor — the SAME monitoring mechanics as
# cdc-pipeline/monitoring, as a second Deployment pointed at the flink-cid-*
# jobs. The monitor code is taken from ../../cdc-pipeline/monitoring so both
# monitors always run identical logic; only the config ConfigMap differs.
#
# The code travels via S3 (s3://${BUCKET_NAME}/flink/monitoring/), next to
# every other artifact the cluster runs; the pod fetches it at boot, so
# `kubectl rollout restart` alone re-pulls the latest code.
#
# Usage:
#   ./deploy-cid-monitor.sh          upload the code, apply, restart, wait
#   ./deploy-cid-monitor.sh stop     scale to 0 (Deployment and config kept)

export AWS_REGION="${AWS_REGION:-us-west-2}"
export NAMESPACE="${NAMESPACE:-emr-flink}"

if [[ "${1:-}" == "stop" ]]; then
    kubectl scale deployment/flink-cid-monitor -n "${NAMESPACE}" --replicas=0
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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDC_MONITOR_DIR="${SCRIPT_DIR}/../../cdc-pipeline/monitoring"
DEPLOYED_MANIFEST="${SCRIPT_DIR}/monitor-deployment-deployed.yaml"
# Must match the FETCH list in monitor-deployment.yaml.
MONITOR_FILES=(flink_cdc_monitor.py entrypoint.py requirements.txt)

[[ -f "${CDC_MONITOR_DIR}/flink_cdc_monitor.py" ]] || {
    echo "ERROR: CDC monitor code not found at ${CDC_MONITOR_DIR}" >&2; exit 1; }

echo "Flink throughput monitor -> ${NAMESPACE} (SA ${MONITOR_SERVICE_ACCOUNT}, code ${MONITOR_CODE_PREFIX})"

# IRSA — idempotent, never clobbers an existing annotation.
CURRENT_ROLE=$(kubectl get sa "${MONITOR_SERVICE_ACCOUNT}" -n "${NAMESPACE}" \
  -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}' 2>/dev/null || true)
if [[ -z "${CURRENT_ROLE}" ]]; then
  kubectl annotate serviceaccount -n "${NAMESPACE}" "${MONITOR_SERVICE_ACCOUNT}" \
    eks.amazonaws.com/role-arn="${EMR_EXECUTION_ROLE_ARN}"
fi

for f in "${MONITOR_FILES[@]}"; do
    aws s3 cp "${CDC_MONITOR_DIR}/${f}" "${MONITOR_CODE_PREFIX}/${f}" \
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

echo "✓ Deployed:  kubectl logs -f deployment/flink-cid-monitor -n ${NAMESPACE}"
