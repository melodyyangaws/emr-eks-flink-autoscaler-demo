#!/usr/bin/env bash
set -euo pipefail

# Deploy the Flink CDC monitor.
#
# No image build of any kind: the monitor runs stock python:3.11-slim, gets its
# code from a ConfigMap, and pip-installs requirements.txt at boot. See the
# header of monitor-deployment.yaml for why. This script therefore needs only
# kubectl + aws CLI — no Docker daemon, no ECR repository, no ECR push grant.

# ── Variables ────────────────────────────────────────────────────────────────
export AWS_REGION="${AWS_REGION:-us-west-2}"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="${BUCKET_NAME:-emr-on-eks-test-${AWS_ACCOUNT_ID}-${AWS_REGION}}"
export NAMESPACE="${NAMESPACE:-emr-flink}"
export EMR_EXECUTION_ROLE_ARN="${EMR_EXECUTION_ROLE_ARN:-arn:aws:iam::${AWS_ACCOUNT_ID}:role/emr-on-eks-test-execution-role}"
# Must already be annotated with an IRSA role that can read Glue + S3.
export MONITOR_SERVICE_ACCOUNT="${MONITOR_SERVICE_ACCOUNT:-starrocks-sa}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOYMENT_TEMPLATE="${SCRIPT_DIR}/monitor-deployment.yaml"
DEPLOYED_MANIFEST="${SCRIPT_DIR}/monitor-deployment-deployed.yaml"

echo "═══════════════════════════════════════════════════════════════"
echo "  Flink CDC Monitor — Deploy"
echo "═══════════════════════════════════════════════════════════════"
echo "  Region:    ${AWS_REGION}"
echo "  Account:   ${AWS_ACCOUNT_ID}"
echo "  Bucket:    ${BUCKET_NAME}"
echo "  Namespace: ${NAMESPACE}"
echo "  SA:        ${MONITOR_SERVICE_ACCOUNT}"
echo "═══════════════════════════════════════════════════════════════"

# ── IRSA ─────────────────────────────────────────────────────────────────────
# Idempotent: skip if the SA already carries a role-arn, so we never clobber a
# pre-existing (possibly more tightly scoped) annotation.
CURRENT_ROLE=$(kubectl get sa "${MONITOR_SERVICE_ACCOUNT}" -n "${NAMESPACE}" \
  -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}' 2>/dev/null || true)
if [[ -z "${CURRENT_ROLE}" ]]; then
  echo "Annotating SA ${MONITOR_SERVICE_ACCOUNT} with ${EMR_EXECUTION_ROLE_ARN}..."
  kubectl annotate serviceaccount -n "${NAMESPACE}" "${MONITOR_SERVICE_ACCOUNT}" \
    eks.amazonaws.com/role-arn="${EMR_EXECUTION_ROLE_ARN}"
else
  echo "✓ SA ${MONITOR_SERVICE_ACCOUNT} already bound to ${CURRENT_ROLE}"
fi

# ── Ship the monitor code as a ConfigMap ─────────────────────────────────────
# Recreated on every deploy so a code edit only needs a re-run of this script
# plus the rollout restart below — this replaces what used to be an image build.
echo "Refreshing ConfigMap flink-cdc-monitor-code..."
kubectl create configmap flink-cdc-monitor-code -n "${NAMESPACE}" \
  --from-file=flink_cdc_monitor.py="${SCRIPT_DIR}/flink_cdc_monitor.py" \
  --from-file=entrypoint.py="${SCRIPT_DIR}/entrypoint.py" \
  --from-file=requirements.txt="${SCRIPT_DIR}/requirements.txt" \
  --dry-run=client -o yaml | kubectl apply -f -

# ── Resolve variables in deployment manifest and apply ───────────────────────
echo "Resolving variables in monitor-deployment.yaml..."
envsubst '${AWS_REGION} ${BUCKET_NAME} ${NAMESPACE} ${MONITOR_SERVICE_ACCOUNT}' \
  < "${DEPLOYMENT_TEMPLATE}" > "${DEPLOYED_MANIFEST}"

echo "Applying ${DEPLOYED_MANIFEST}..."
kubectl apply -f "${DEPLOYED_MANIFEST}"

# Restart so the pod picks up the refreshed ConfigMap (a ConfigMap change alone
# does not roll a Deployment, and the code is copied at container start).
kubectl rollout restart deployment/flink-cdc-monitor -n "${NAMESPACE}"
# Generous timeout: the first probe-free start still has to pip install
# pyiceberg + pyarrow, which takes a couple of minutes on a cold pod.
kubectl rollout status deployment/flink-cdc-monitor -n "${NAMESPACE}" --timeout=300s

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  ✓ Deployed! View logs:"
echo "    kubectl logs -f deployment/flink-cdc-monitor -n ${NAMESPACE}"
echo "═══════════════════════════════════════════════════════════════"
