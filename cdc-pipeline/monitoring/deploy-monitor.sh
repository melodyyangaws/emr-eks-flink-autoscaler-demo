#!/usr/bin/env bash
set -euo pipefail

# ── Variables ────────────────────────────────────────────────────────────────
export AWS_REGION="${AWS_REGION:-us-west-2}"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="emr-on-eks-test-${AWS_ACCOUNT_ID}-${AWS_REGION}"
export IMAGE_NAME="flink-cdc-monitor"
export ECR_REPO="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE_NAME}"
export NAMESPACE="${NAMESPACE:-emr-flink}"
export EMR_EXECUTION_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/emr-on-eks-test-execution-role"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOYMENT_TEMPLATE="${SCRIPT_DIR}/monitor-deployment.yaml"
DEPLOYED_MANIFEST="${SCRIPT_DIR}/monitor-deployment-deployed.yaml"

echo "═══════════════════════════════════════════════════════════════"
echo "  Flink CDC Monitor — Build & Deploy"
echo "═══════════════════════════════════════════════════════════════"
echo "  Region:    ${AWS_REGION}"
echo "  Account:   ${AWS_ACCOUNT_ID}"
echo "  Bucket:    ${BUCKET_NAME}"
echo "  Image:     ${ECR_REPO}:icebergv3"
echo "  Namespace: ${NAMESPACE}"
echo "  IAM Role:  ${EMR_EXECUTION_ROLE_ARN}"
echo "═══════════════════════════════════════════════════════════════"

# ── ECR ──────────────────────────────────────────────────────────────────────
aws ecr describe-repositories --repository-names "${IMAGE_NAME}" --region "${AWS_REGION}" 2>/dev/null || \
  aws ecr create-repository --repository-name "${IMAGE_NAME}" --region "${AWS_REGION}"

aws ecr get-login-password --region "${AWS_REGION}" | \
  docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# ── Resolve variables in source files before Docker build ────────────────────
# Copy to a staging directory so source templates stay untouched
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "${BUILD_DIR}"' EXIT

cp "${SCRIPT_DIR}"/*.py "${SCRIPT_DIR}"/*.txt "${SCRIPT_DIR}"/Dockerfile.monitor "${BUILD_DIR}/"

echo "Resolving variables in entrypoint.py..."
envsubst '${AWS_REGION} ${BUCKET_NAME}' \
  < "${SCRIPT_DIR}/entrypoint.py" > "${BUILD_DIR}/entrypoint.py"

echo "  AWS_REGION  : \${AWS_REGION}  → ${AWS_REGION}"
echo "  BUCKET_NAME : \${BUCKET_NAME} → ${BUCKET_NAME}"

# ── Build & Push (multi-arch) ────────────────────────────────────────────────
echo "Building ${ECR_REPO}..."
docker buildx build --platform linux/amd64,linux/arm64 \
  --push -t "${ECR_REPO}:icebergv3" \
  -f "${BUILD_DIR}/Dockerfile.monitor" \
  "${BUILD_DIR}"
# ── IRSA ─────────────────────────────────────────────────────────────────────
echo "Annotating default SA with IAM role..."
kubectl annotate serviceaccount -n "${NAMESPACE}" default \
  eks.amazonaws.com/role-arn="${EMR_EXECUTION_ROLE_ARN}" --overwrite

# ── Resolve variables in deployment manifest and apply ───────────────────────
echo "Resolving variables in monitor-deployment.yaml..."
envsubst '${AWS_ACCOUNT_ID} ${AWS_REGION} ${BUCKET_NAME} ${NAMESPACE}' \
  < "${DEPLOYMENT_TEMPLATE}" > "${DEPLOYED_MANIFEST}"

echo "Applying ${DEPLOYED_MANIFEST}..."
kubectl apply -f "${DEPLOYED_MANIFEST}"

# Restart to pick up latest image
kubectl rollout restart deployment/flink-cdc-monitor -n "${NAMESPACE}"
kubectl rollout status deployment/flink-cdc-monitor -n "${NAMESPACE}" --timeout=120s

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  ✓ Deployed! View logs:"
echo "    kubectl logs -f deployment/flink-cdc-monitor -n ${NAMESPACE}"
echo "═══════════════════════════════════════════════════════════════"
