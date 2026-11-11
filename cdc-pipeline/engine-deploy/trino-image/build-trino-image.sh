#!/usr/bin/env bash
# Build the CDC benchmark's Trino image (trinodb/trino:484 + the native Paimon
# connector built from apache/paimon-trino, see Dockerfile) and push it to ECR
# as ${REPO}:${IMAGE_TAG}. deploy-trino.sh uses it by default (TRINO_IMAGE;
# empty = the stock trinodb/trino image; the harness configures no paimon catalog
# either way, so the bundled connector is unused).
#
#   ./engine-deploy/trino-image/build-trino-image.sh
#   NO_PUSH=1 ./engine-deploy/trino-image/build-trino-image.sh   build only (Kaniko --no-push)
#
# Push needs ecr push rights on ${REPO} for image-builder-sa's role (its
# FlinkCDCImagePush policy lists the repositories it may push to).
#
# Same pattern as duckdb-image/build-duckdb-image.sh: the Dockerfile and the
# patch go into a ConfigMap, copied into the Kaniko build context; one Kaniko pod
# builds it. amd64 only (Trino runs on m5), so no manifest list.
# Settings from cdc-pipeline/.bench-env (AWS_ACCOUNT_ID, AWS_REGION, NAMESPACE).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "${HERE}/../../lib/common.sh"
: "${AWS_ACCOUNT_ID:?not set (.bench-env.local)}"

REPO="${TRINO_IMAGE_REPO_NAME:-trino-cdc}"
IMAGE_TAG="${TRINO_IMAGE_TAG:-484-paimon-2.1.0}"
NODEPOOL="${NODEPOOL:-executor-memorynodepool}"
IMAGE="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO}"
POD=trino-cdc-image-build
TIMEOUT_S=3600
NO_PUSH="${NO_PUSH:-0}"
PUSH_ARG=""; [[ "$NO_PUSH" == 1 ]] && PUSH_ARG=', "--no-push"'

[[ "$NO_PUSH" == 1 ]] || aws ecr describe-repositories --repository-names "$REPO" --region "$AWS_REGION" >/dev/null 2>&1 \
    || aws ecr create-repository --repository-name "$REPO" --region "$AWS_REGION" >/dev/null

kubectl create configmap trino-cdc-image-context -n "$NAMESPACE" \
    --from-file="${HERE}/Dockerfile" --from-file="${HERE}/paimon-trino-484.patch" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl delete pod "$POD" -n "$NAMESPACE" --ignore-not-found >/dev/null
kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NAMESPACE}
  annotations: {karpenter.sh/do-not-disrupt: "true"}
spec:
  restartPolicy: Never
  serviceAccountName: image-builder-sa
  nodeSelector: {kubernetes.io/arch: amd64, karpenter.sh/capacity-type: on-demand, karpenter.sh/nodepool: ${NODEPOOL}}
  # ConfigMap files are symlinks into ..data/, which Kaniko's COPY copies as
  # dangling links: dereference them into an emptyDir first.
  initContainers:
  - name: context
    image: busybox:1.37
    command: ["sh", "-c", "cp -L /context-cm/Dockerfile /context-cm/paimon-trino-484.patch /workspace/"]
    volumeMounts: [{name: context-cm, mountPath: /context-cm}, {name: context, mountPath: /workspace}]
  containers:
  - name: kaniko
    image: gcr.io/kaniko-project/executor:v1.23.2
    args: ["--context=dir:///workspace", "--dockerfile=Dockerfile",
           "--destination=${IMAGE}:${IMAGE_TAG}"${PUSH_ARG},
           "--cache=false", "--snapshot-mode=redo", "--use-new-run"]
    env: [{name: AWS_REGION, value: "${AWS_REGION}"}]
    # maven pulls ~1 GiB of dependencies; the plugin directory is ~350 MiB.
    resources:
      requests: {cpu: "4", memory: 16Gi, ephemeral-storage: 40Gi}
      limits:   {cpu: "4", memory: 16Gi, ephemeral-storage: 40Gi}
    volumeMounts: [{name: context, mountPath: /workspace}]
  volumes:
  - {name: context-cm, configMap: {name: trino-cdc-image-context}}
  - {name: context, emptyDir: {}}
EOF
log "building ${IMAGE}:${IMAGE_TAG} (kubectl logs -f ${POD} -n ${NAMESPACE})"
deadline=$(( SECONDS + TIMEOUT_S ))
while (( SECONDS < deadline )); do
    phase=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    case "$phase" in
        Succeeded) log "$([[ $NO_PUSH == 1 ]] && echo "built (not pushed)" || echo pushed) ${IMAGE}:${IMAGE_TAG}"; exit 0 ;;
        Failed) kubectl logs "$POD" -n "$NAMESPACE" --tail=40 >&2 || true; die "build failed" ;;
    esac
    sleep 20
done
die "build timed out after ${TIMEOUT_S}s"
