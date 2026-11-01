#!/usr/bin/env bash
# Build the DuckDB 2.0.0 image from the Dockerfile next to this script, for
# linux/amd64 and linux/arm64, and push it to ECR as one multi-arch tag.
#
#   ./build-duckdb-image.sh
#
# The Dockerfile goes into a ConfigMap mounted as the build context; one Kaniko pod per architecture builds it
# natively (Kaniko cannot cross-build) and pushes a per-arch tag; the two are
# then joined into ${IMAGE_TAG} with the ECR API. No S3.
# Settings come from cdc-pipeline/.bench-env (AWS_ACCOUNT_ID, AWS_REGION,
# NAMESPACE). COMPILE_JOBS (default 30): parallel C++ compile jobs in each
# arch build (make -j); the pod gets COMPILE_JOBS cpu and 2*COMPILE_JOBS+8 GiB,
# so 30 cpu / 68 GiB lands on an 8xlarge node.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/common.sh
source "${HERE}/../../lib/common.sh"
: "${AWS_ACCOUNT_ID:?not set (.bench-env.local)}"

REPO=duckdb-cdc-server
IMAGE_TAG=2.0.0
COMPILE_JOBS="${COMPILE_JOBS:-30}"
IMAGE="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO}"
TIMEOUT_S=14400   # a cold build is ~1.5-3 h per arch

aws ecr describe-repositories --repository-names "$REPO" --region "$AWS_REGION" >/dev/null 2>&1 \
    || aws ecr create-repository --repository-name "$REPO" --region "$AWS_REGION" >/dev/null

kubectl create configmap duckdb-cdc-image-context -n "$NAMESPACE" \
    --from-file="${HERE}/Dockerfile" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

build() {   # <arch>: one Kaniko pod on that arch, waits for it
    local arch=$1 pod="duckdb-cdc-image-build-$1" phase
    kubectl delete pod "$pod" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${NAMESPACE}
  annotations: {karpenter.sh/do-not-disrupt: "true"}
spec:
  restartPolicy: Never
  serviceAccountName: image-builder-sa
  nodeSelector: {kubernetes.io/arch: ${arch}, karpenter.sh/capacity-type: on-demand}
  containers:
  - name: kaniko
    image: gcr.io/kaniko-project/executor:v1.23.2
    args: ["--context=dir:///workspace", "--dockerfile=Dockerfile",
           "--destination=${IMAGE}:${IMAGE_TAG}-${arch}", "--build-arg=COMPILE_JOBS=${COMPILE_JOBS}",
           "--cache=false", "--snapshot-mode=redo", "--use-new-run"]
    env: [{name: AWS_REGION, value: "${AWS_REGION}"}]
    # Kaniko compiles in its own root filesystem, i.e. on the node's disk.
    resources:
      requests: {cpu: "${COMPILE_JOBS}", memory: $(( COMPILE_JOBS * 2 + 8 ))Gi, ephemeral-storage: 80Gi}
      limits:   {cpu: "${COMPILE_JOBS}", memory: $(( COMPILE_JOBS * 2 + 8 ))Gi, ephemeral-storage: 80Gi}
    volumeMounts: [{name: context, mountPath: /workspace}]
  volumes: [{name: context, configMap: {name: duckdb-cdc-image-context}}]
EOF
    log "${arch}: building (kubectl logs -f ${pod} -n ${NAMESPACE})"
    local deadline=$(( SECONDS + TIMEOUT_S ))
    while (( SECONDS < deadline )); do
        phase=$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
        case "$phase" in
            Succeeded) log "${arch}: pushed ${IMAGE}:${IMAGE_TAG}-${arch}"; return 0 ;;
            Failed) kubectl logs "$pod" -n "$NAMESPACE" --tail=40 >&2 || true; die "${arch} build failed" ;;
        esac
        sleep 30
    done
    die "${arch} build timed out after ${TIMEOUT_S}s"
}

# Both arches build at the same time, on separate nodes.
build amd64 & a=$!
build arm64 & b=$!
wait $a && wait $b || die "a build failed"

# Join the two per-arch images into one manifest list under ${IMAGE_TAG}.
entries=""
for arch in amd64 arm64; do
    m=$(aws ecr batch-get-image --repository-name "$REPO" --image-ids imageTag="${IMAGE_TAG}-${arch}" \
          --accepted-media-types application/vnd.docker.distribution.manifest.v2+json application/vnd.oci.image.manifest.v1+json \
          --region "$AWS_REGION" --query 'images[0].imageManifest' --output text)
    entries+="${entries:+,}{\"mediaType\":\"$(python3 -c 'import sys,json; print(json.loads(sys.argv[1])["mediaType"])' "$m")\",\
\"size\":$(printf '%s' "$m" | wc -c | tr -d ' '),\"digest\":\"sha256:$(printf '%s' "$m" | shasum -a 256 | cut -d' ' -f1)\",\
\"platform\":{\"architecture\":\"${arch}\",\"os\":\"linux\"}}"
done
aws ecr put-image --repository-name "$REPO" --image-tag "$IMAGE_TAG" --region "$AWS_REGION" \
    --image-manifest-media-type application/vnd.docker.distribution.manifest.list.v2+json \
    --image-manifest "{\"schemaVersion\":2,\"mediaType\":\"application/vnd.docker.distribution.manifest.list.v2+json\",\"manifests\":[${entries}]}" >/dev/null
log "pushed ${IMAGE}:${IMAGE_TAG} (linux/amd64, linux/arm64)"
