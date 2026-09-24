#!/bin/bash
###############################################################################
# Generic Flink CDC Deployment Script
#
# Supports deploying to:
# - Paimon lakehouse (file-based catalog on S3)
# - Iceberg lakehouse (AWS Glue catalog)
# - Both lakehouses simultaneously
#
# Uses generic SQL executor with modular SQL scripts
###############################################################################

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" >&2
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Default values
LAKEHOUSE_FORMAT=${LAKEHOUSE_FORMAT:-both}
ACTION=${1:-help}

# Default the region rather than leaving it unset. An empty AWS_REGION renders
# empty into the manifests, and the AWS SDK then silently falls back to
# us-east-1 ("No region info found, using SDK default region: us-east-1") — which
# does not fail loudly, it hangs on cross-region Glue calls.
export AWS_REGION=${AWS_REGION:-us-west-2}
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-$AWS_REGION}

# Check required environment variables
check_env_vars() {
    log "Checking required environment variables..."

    local required_vars=(
        "AWS_REGION"
        "AWS_ACCOUNT_ID"
        "BUCKET_NAME"
        "EMR_EXECUTION_ROLE_ARN"
        "MYSQL_HOST"
        "MYSQL_USER"
        "MYSQL_PASSWORD"
    )

    local missing_vars=()
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            missing_vars+=("$var")
        fi
    done

    # Check lakehouse-specific vars
    if [[ "$LAKEHOUSE_FORMAT" == "iceberg" || "$LAKEHOUSE_FORMAT" == "both" ]]; then
        if [[ -z "${GLUE_DATABASE}" ]]; then
            warn "GLUE_DATABASE not set, using default: flink_iceberg_db"
            export GLUE_DATABASE="flink_iceberg_db"
        fi
    fi

    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        error "Missing required environment variables: ${missing_vars[*]}"
    fi

    log "✓ All required environment variables are set"
}

# Display configuration
display_config() {
    log "Configuration:"
    echo "  AWS Region:       $AWS_REGION"
    echo "  AWS Account ID:   $AWS_ACCOUNT_ID"
    echo "  S3 Bucket:        $BUCKET_NAME"
    echo "  MySQL Host:       $MYSQL_HOST"
    echo "  Namespace:        ${NAMESPACE:-emr-flink}"
    echo "  EMR Version:      ${EMR_VERSION:-7.13.0}"
    echo "  Lakehouse Format: ${LAKEHOUSE_FORMAT}"

    if [[ "$LAKEHOUSE_FORMAT" == "paimon" || "$LAKEHOUSE_FORMAT" == "both" ]]; then
        echo "  Paimon Warehouse: s3://${BUCKET_NAME}/paimon-warehouse/"
    fi

    if [[ "$LAKEHOUSE_FORMAT" == "iceberg" || "$LAKEHOUSE_FORMAT" == "both" ]]; then
        echo "  Iceberg Warehouse: s3://${BUCKET_NAME}/iceberg-warehouse/"
        echo "  Glue Database:     ${GLUE_DATABASE}"
    fi
}

# Create ECR repository
create_ecr_repo() {
    local repo_name=$1
    log "Creating ECR repository: $repo_name"

    if aws ecr describe-repositories --repository-names "$repo_name" --region "$AWS_REGION" &>/dev/null; then
        log "✓ ECR repository already exists: $repo_name"
    else
        aws ecr create-repository \
            --repository-name "$repo_name" \
            --image-scanning-configuration scanOnPush=true \
            --region "$AWS_REGION" || error "Failed to create ECR repository"
        log "✓ ECR repository created: $repo_name"
    fi
}

# Build and push the Flink CDC image.
#
# The build always runs in-cluster with Kaniko — there is no local-docker path.
# The image is ~6 GB, more than a typical laptop has free, and building remotely
# means no Docker daemon, no ECR docker login, and no dependence on the laptop's
# architecture. Requires ServiceAccount image-builder-sa in $NAMESPACE (IRSA ->
# a role allowed to push to ECR and read the build context from S3).
build_and_push_image() {
    local emr_version=${EMR_VERSION:-7.13.0}
    local repo_name="emr-flink-cdc-paimon"
    # Tag must match the image reference in flink-cdc-*-sql.yaml (:${IMAGE_TAG})
    local image_tag=${IMAGE_TAG:-$emr_version}
    local image_uri="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${repo_name}:${image_tag}"

    create_ecr_repo "$repo_name"

    # Multi-platform build. PLATFORMS is a space-separated list of GOARCH values.
    # Default to both because executor-memorynodepool provisions Graviton
    # (m8g/m9g/m9gd) as well as x86: a single-arch image leaves TaskManagers
    # dying with exitCode=255 on whichever arch it lacks, and the JobManager
    # survives, so the job churns TaskManagers and surfaces a misleading
    # NoResourceAvailableException.
    local platforms=${PLATFORMS:-"amd64 arm64"}

    # One Kaniko pod per architecture, each scheduled ON that architecture.
    # Kaniko cannot cross-build (it executes the Dockerfile's RUN steps natively)
    # and cannot emit a manifest list, so each arch is built separately and the
    # results are stitched into one index afterwards.
    #
    # Kaniko can only push to a tag, never to a bare digest, so each arch build
    # lands on a throwaway tag first. Those tags are deleted once the index is
    # published — the repository is left with exactly ONE tag, ${image_tag},
    # which is a manifest list serving both architectures. The per-arch child
    # manifests stay reachable by digest through the index; ECR does not garbage
    # collect a manifest that an index in the same repository references.
    local count=0 arch
    for arch in $platforms; do
        build_image_with_kaniko "$repo_name" "${image_uri}-${BUILD_TAG_SUFFIX}${arch}" \
            "$emr_version" "$arch"
        count=$((count + 1))
    done

    if [[ $count -eq 1 ]]; then
        # Single platform requested: retag rather than build a pointless index.
        retag_image "$repo_name" "${image_tag}-${BUILD_TAG_SUFFIX}${platforms}" "$image_tag"
    else
        create_manifest_list "$repo_name" "$image_tag" "$platforms"
    fi

    delete_build_tags "$repo_name" "$image_tag" "$platforms"

    log "✓ Image built and pushed: $image_uri (single tag, ${platforms// //})"
    echo "$image_uri"
}

# Prefix for the disposable per-arch tags. Anything under this prefix is build
# scaffolding, never something a manifest should reference.
BUILD_TAG_SUFFIX="buildstage-"

# Drop the throwaway per-arch tags so the repository shows only the single
# multi-arch tag.
#
# batch-delete-image on a per-arch tag is REJECTED once the index exists:
#   ImageReferencedByManifestList: Requested image referenced by manifest list
# ECR treats "delete this tag" as "delete this digest" when the tag is the only
# reference, and it refuses because the published index points at that digest.
#
# The way through is to first re-point each per-arch tag at the INDEX digest
# (put-image with the same tag but the manifest-list body — ECR allows moving a
# tag). The tag is then just a duplicate alias of ${image_tag}, deleting it
# touches no unique digest, and the child manifests are left untagged but still
# referenced by the index, which is exactly how buildx leaves a repository.
#
# Note there is deliberately no lifecycle policy expiring untagged images on
# this repository: such a rule WOULD delete these children and silently break
# the multi-arch tag.
delete_build_tags() {
    local repo_name=$1 image_tag=$2 platforms=$3
    local arch ids=()

    local list
    list=$(aws ecr batch-get-image --repository-name "$repo_name" \
             --image-ids imageTag="$image_tag" \
             --accepted-media-types \
                 "application/vnd.docker.distribution.manifest.list.v2+json" \
                 "application/vnd.oci.image.index.v1+json" \
             --region "$AWS_REGION" \
             --query 'images[0].imageManifest' --output text 2>/dev/null)

    if [[ -n "$list" && "$list" != "None" ]]; then
        for arch in $platforms; do
            # Move the build tag onto the index digest, making it redundant.
            aws ecr put-image --repository-name "$repo_name" \
                --image-tag "${image_tag}-${BUILD_TAG_SUFFIX}${arch}" \
                --image-manifest "$list" \
                --image-manifest-media-type \
                    "application/vnd.docker.distribution.manifest.list.v2+json" \
                --region "$AWS_REGION" >/dev/null 2>&1 || true
            ids+=("imageTag=${image_tag}-${BUILD_TAG_SUFFIX}${arch}")
        done

        aws ecr batch-delete-image \
            --repository-name "$repo_name" \
            --image-ids "${ids[@]}" \
            --region "$AWS_REGION" >/dev/null 2>&1 || true
    fi

    verify_multiarch_tag "$repo_name" "$image_tag" "$platforms"
}

# Confirm ${image_tag} is still a manifest list and that every child manifest is
# still pullable by digest after the build tags were removed. Without this the
# tag cleanup could silently leave a dangling index that only fails at pod
# scheduling time, which is exactly the class of failure that cost a day here
# (an arch mismatch surfaces as NoResourceAvailableException, naming neither the
# image nor the architecture).
verify_multiarch_tag() {
    local repo_name=$1 image_tag=$2 platforms=$3

    local list
    list=$(aws ecr batch-get-image --repository-name "$repo_name" \
             --image-ids imageTag="$image_tag" \
             --accepted-media-types \
                 "application/vnd.docker.distribution.manifest.list.v2+json" \
                 "application/vnd.oci.image.index.v1+json" \
             --region "$AWS_REGION" \
             --query 'images[0].imageManifest' --output text 2>/dev/null)
    [[ -n "$list" && "$list" != "None" ]] || \
        error "${repo_name}:${image_tag} is not a manifest list"

    local arch
    for arch in $platforms; do
        local child_digest
        child_digest=$(printf '%s' "$list" | python3 -c "
import sys, json
arch = '$arch'
for m in json.load(sys.stdin)['manifests']:
    if m['platform']['architecture'] == arch:
        print(m['digest'])
        break
")
        [[ -n "$child_digest" ]] || \
            error "${repo_name}:${image_tag} has no ${arch} entry"
        aws ecr batch-get-image --repository-name "$repo_name" \
            --image-ids imageDigest="$child_digest" --region "$AWS_REGION" \
            --query 'images[0].imageId.imageDigest' --output text 2>/dev/null \
            | grep -q "$child_digest" \
            || error "${arch} child ${child_digest} is no longer pullable from ${repo_name}"
        log "✓ ${image_tag} -> linux/${arch} ${child_digest:0:19}…"
    done

    log "✓ Verified ${repo_name}:${image_tag} is one tag serving ${platforms// //}"
}

# Combine the per-arch tags into one multi-platform manifest list under the
# plain tag, so Kubernetes pulls the right arch on every node automatically.
#
# Done with the ECR API rather than `docker manifest`/`buildx imagetools`: there
# is deliberately no Docker daemon in this workflow (see build_and_push_image).
# ECR accepts a manifest list via put-image as long as every child manifest is
# already present in the repository, which the per-arch builds guarantee.
create_manifest_list() {
    local repo_name=$1
    local image_tag=$2
    local platforms=$3

    log "Creating multi-platform manifest list ${repo_name}:${image_tag} (${platforms})..."

    local manifests_json="" digest size media_type
    for arch in $platforms; do
        # Fetch the child manifest exactly as stored; the digest must be over the
        # identical bytes ECR holds, so ask for the raw manifest and hash that.
        local raw
        raw=$(aws ecr batch-get-image \
                --repository-name "$repo_name" \
                --image-ids imageTag="${image_tag}-${BUILD_TAG_SUFFIX}${arch}" \
                --accepted-media-types \
                    "application/vnd.docker.distribution.manifest.v2+json" \
                    "application/vnd.oci.image.manifest.v1+json" \
                --region "$AWS_REGION" \
                --query 'images[0].imageManifest' --output text 2>/dev/null) \
            || error "Could not read manifest for ${image_tag}-${BUILD_TAG_SUFFIX}${arch}"
        [[ -n "$raw" && "$raw" != "None" ]] || \
            error "Missing per-arch build-stage image ${image_tag}-${BUILD_TAG_SUFFIX}${arch}; build it first"

        digest="sha256:$(printf '%s' "$raw" | shasum -a 256 | cut -d' ' -f1)"
        size=$(printf '%s' "$raw" | wc -c | tr -d ' ')
        media_type=$(printf '%s' "$raw" | python3 -c "import sys,json;print(json.load(sys.stdin)['mediaType'])")

        [[ -n "$manifests_json" ]] && manifests_json+=","
        manifests_json+="{\"mediaType\":\"${media_type}\",\"size\":${size},\"digest\":\"${digest}\",\"platform\":{\"architecture\":\"${arch}\",\"os\":\"linux\"}}"
    done

    local list_json="{\"schemaVersion\":2,\"mediaType\":\"application/vnd.docker.distribution.manifest.list.v2+json\",\"manifests\":[${manifests_json}]}"

    aws ecr put-image \
        --repository-name "$repo_name" \
        --image-tag "$image_tag" \
        --image-manifest "$list_json" \
        --image-manifest-media-type "application/vnd.docker.distribution.manifest.list.v2+json" \
        --region "$AWS_REGION" >/dev/null 2>&1 \
        || aws ecr put-image \
            --repository-name "$repo_name" \
            --image-tag "$image_tag" \
            --image-manifest "$list_json" \
            --image-manifest-media-type "application/vnd.docker.distribution.manifest.list.v2+json" \
            --region "$AWS_REGION" >/dev/null \
        || error "Failed to put manifest list ${repo_name}:${image_tag}"

    log "✓ Manifest list pushed: ${repo_name}:${image_tag}"
}

# Point an additional tag at an image that is already in the repository, without
# rebuilding or pulling it.
retag_image() {
    local repo_name=$1 source_ref=$2 new_tag=$3
    # Accepts either a full image URI or a bare tag.
    local source_tag=${source_ref##*:}

    local raw
    raw=$(aws ecr batch-get-image --repository-name "$repo_name" \
            --image-ids imageTag="$source_tag" --region "$AWS_REGION" \
            --query 'images[0].imageManifest' --output text) \
        || error "Could not read manifest for ${source_tag}"

    aws ecr put-image --repository-name "$repo_name" --image-tag "$new_tag" \
        --image-manifest "$raw" --region "$AWS_REGION" >/dev/null 2>&1 || true
    log "✓ Tagged ${repo_name}:${new_tag} -> ${source_tag}"
}

# Build the image inside the EKS cluster with Kaniko.
# The Dockerfile is shipped to S3 as a tarball build context, then a one-shot Pod
# builds it and pushes straight to ECR.
build_image_with_kaniko() {
    local repo_name=$1
    local image_uri=$2
    local emr_version=$3
    # Target architecture (amd64|arm64). The pod is pinned to a node of this arch
    # so Kaniko runs the Dockerfile's RUN steps natively — it cannot cross-build.
    local target_arch=${4:-amd64}
    local namespace=${NAMESPACE:-emr-flink}
    local pod_name="flink-cdc-image-build-${target_arch}"
    local context_s3="s3://${BUCKET_NAME}/flink/build/flink-cdc-build-context.tar.gz"
    local context_tar="/tmp/flink-cdc-build-context.tar.gz"

    log "Building ${target_arch} image in-cluster with Kaniko (namespace: ${namespace})..."

    if ! kubectl get serviceaccount image-builder-sa -n "$namespace" &>/dev/null; then
        error "ServiceAccount image-builder-sa not found in ${namespace}. It needs an
       eks.amazonaws.com/role-arn annotation for a role that can push to ECR."
    fi

    # Pack only the Dockerfile — every dependency is fetched from Maven/ECR at
    # build time, so no other local file is part of the context.
    log "Packing build context -> ${context_s3}"
    tar -czf "$context_tar" -C docker Dockerfile.cdc-paimon || error "Failed to pack build context"
    aws s3 cp "$context_tar" "$context_s3" --region "$AWS_REGION" >&2 || \
        error "Failed to upload build context to S3"

    kubectl delete pod "$pod_name" -n "$namespace" --ignore-not-found --timeout=120s >&2

    # Pin the builder to the architecture it is building for: Kaniko executes the
    # Dockerfile's RUN steps in the running container, so an arm64 image must be
    # assembled on an arm64 node (and vice versa). The Kaniko executor image and
    # both base images (EMR flink, amazonlinux:2023) are multi-arch, so the same
    # Dockerfile works unchanged on either — every JAR it downloads is pure Java.
    kubectl apply -n "$namespace" -f - >&2 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod_name}
  namespace: ${namespace}
spec:
  restartPolicy: Never
  serviceAccountName: image-builder-sa
  nodeSelector:
    kubernetes.io/arch: ${target_arch}
  containers:
  - name: kaniko
    image: gcr.io/kaniko-project/executor:v1.23.2
    args:
    - "--context=${context_s3}"
    - "--dockerfile=Dockerfile.cdc-paimon"
    - "--destination=${image_uri}"
    - "--build-arg=EMR_VERSION=${emr_version}"
    - "--cache=false"
    - "--verbosity=info"
    env:
    - name: AWS_REGION
      value: ${AWS_REGION}
    - name: AWS_SDK_LOAD_CONFIG
      value: "true"
    resources:
      requests:
        cpu: "2"
        memory: 8Gi
      limits:
        cpu: "4"
        memory: 16Gi
    volumeMounts:
    - name: workspace
      mountPath: /kaniko/.cache
  volumes:
  - name: workspace
    emptyDir:
      sizeLimit: 40Gi
EOF

    log "Waiting for Kaniko build to finish (this takes several minutes)..."
    local deadline=$((SECONDS + 1800))
    local phase=""
    while (( SECONDS < deadline )); do
        phase=$(kubectl get pod "$pod_name" -n "$namespace" -o jsonpath='{.status.phase}' 2>/dev/null || true)
        case "$phase" in
            Succeeded) log "✓ Kaniko ${target_arch} build succeeded"; return 0 ;;
            Failed)
                kubectl logs "$pod_name" -n "$namespace" --tail=60 >&2 || true
                error "Kaniko ${target_arch} build failed (pod ${pod_name})" ;;
        esac
        sleep 20
    done

    error "Kaniko ${target_arch} build timed out after 30m (pod ${pod_name}, last phase: ${phase:-unknown})"
}

# Upload PyFlink executor and SQL scripts to S3
upload_scripts_to_s3() {
    log "Uploading PyFlink executor and SQL scripts to S3..."

    # Upload generic executor
    local executor_s3_path="s3://${BUCKET_NAME}/flink/scripts/flink-cdc-executor.py"
    aws s3 cp flink-cdc-executor.py "$executor_s3_path" --region "$AWS_REGION" || \
        error "Failed to upload executor to S3"
    log "✓ Executor uploaded: $executor_s3_path"

    # Upload common CDC source scripts
    aws s3 sync sql-scripts/common/ "s3://${BUCKET_NAME}/flink/sql-scripts/common/" \
        --region "$AWS_REGION" --delete || \
        error "Failed to upload common SQL scripts"
    log "✓ Common SQL scripts uploaded"

   # Upload Paimon scripts if needed
    if [[ "$LAKEHOUSE_FORMAT" == "paimon" || "$LAKEHOUSE_FORMAT" == "both" ]]; then
        aws s3 sync sql-scripts/paimon/ "s3://${BUCKET_NAME}/flink/sql-scripts/paimon/" \
            --region "$AWS_REGION" --delete || \
            error "Failed to upload Paimon SQL scripts"

        # Copy common scripts to Paimon directory
        aws s3 cp "s3://${BUCKET_NAME}/flink/sql-scripts/common/02-cdc-sources.sql" \
            "s3://${BUCKET_NAME}/flink/sql-scripts/paimon/02-cdc-sources.sql" \
            --region "$AWS_REGION"
        log "✓ Paimon SQL scripts uploaded"
    fi

    # Upload Iceberg scripts if needed
    if [[ "$LAKEHOUSE_FORMAT" == "iceberg" || "$LAKEHOUSE_FORMAT" == "both" ]]; then
        aws s3 sync sql-scripts/iceberg/ "s3://${BUCKET_NAME}/flink/sql-scripts/iceberg/" \
            --region "$AWS_REGION" --delete || \
            error "Failed to upload Iceberg SQL scripts"
        
        # Copy common scripts to Iceberg directory
        aws s3 cp "s3://${BUCKET_NAME}/flink/sql-scripts/common/02-cdc-sources.sql" \
            "s3://${BUCKET_NAME}/flink/sql-scripts/iceberg/02-cdc-sources.sql" \
            --region "$AWS_REGION"    
        log "✓ Iceberg SQL scripts uploaded"
    fi
}

# Create AWS Glue database for Iceberg
create_glue_database() {
    local glue_db=${GLUE_DATABASE:-flink_iceberg_db}

    log "Creating AWS Glue database: $glue_db"

    # Check if database exists
    if aws glue get-database --name "$glue_db" --region "$AWS_REGION" &>/dev/null; then
        log "✓ Glue database already exists: $glue_db"
    else
        aws glue create-database \
            --database-input "{\"Name\":\"$glue_db\",\"Description\":\"Flink CDC Iceberg lakehouse\"}" \
            --region "$AWS_REGION" || error "Failed to create Glue database"
        log "✓ Glue database created: $glue_db"
    fi
}

# Create the "ebs-sc" StorageClass required by task-local recovery by EBS.
#
# One-off, cluster-scoped, idempotent: run it once per EKS cluster. Both jobs set
# `task.local-recovery.ebs.enable: "true"`, and EMR then auto-creates one PVC per
# TaskManager with the hardcoded storageClassName "ebs-sc" — the name is not
# configurable. Without the class, TaskManager pods stay Pending with
# "failed to get storage class, StorageClass storage.k8s.io \"ebs-sc\" not found".
create_ebs_storage_class() {
    local manifest="k8s/ebs-sc-storageclass.yaml"

    if kubectl get storageclass ebs-sc &>/dev/null; then
        log "✓ StorageClass ebs-sc already exists"
        return 0
    fi

    log "Creating StorageClass ebs-sc (gp3) for Flink task-local recovery..."
    [[ -f "$manifest" ]] || error "Manifest not found: $manifest"
    kubectl apply -f "$manifest" >&2 || error "Failed to create StorageClass ebs-sc"
    log "✓ StorageClass ebs-sc created"
}

# Install Kyverno and the single-AZ policy for Flink CDC jobs.
#
# One-off, cluster-scoped, idempotent: run it once per EKS cluster. Karpenter
# chooses an AZ per node independently, so without this a job's JobManager and
# TaskManagers land in different AZs — cross-AZ transfer cost on every heartbeat
# and shuffle, and an unfair paimon-vs-iceberg comparison when only one of the two
# jobs happens to be split. The policy mutates TaskManager pods with a required
# zone podAffinity keyed on the per-deployment label
# eks-subscription.amazonaws.com/emr.internal.id. See
# k8s/kyverno-flink-az-affinity.yaml for the full rationale.
install_kyverno_az_policy() {
    local policy="k8s/kyverno-flink-az-affinity.yaml"

    if kubectl get crd clusterpolicies.kyverno.io &>/dev/null; then
        log "✓ Kyverno already installed"
    else
        log "Installing Kyverno via Helm..."
        command -v helm >/dev/null || error "helm not found; needed to install Kyverno"
        helm repo add kyverno https://kyverno.github.io/kyverno/ >&2 2>/dev/null || true
        helm repo update kyverno >&2 || true
        helm upgrade --install kyverno kyverno/kyverno \
            --namespace kyverno --create-namespace --wait --timeout 10m >&2 || \
            error "Failed to install Kyverno"
        log "✓ Kyverno installed"
    fi

    log "Applying ClusterPolicy flink-cdc-single-az..."
    [[ -f "$policy" ]] || error "Policy not found: $policy"
    kubectl apply -f "$policy" >&2 || error "Failed to apply $policy"
    log "✓ ClusterPolicy flink-cdc-single-az applied"
}

# Generate FlinkDeployment YAML
generate_flink_deployment() {
    local format=$1
    local template_file="flink-cdc-${format}-sql.yaml"
    local output_file="flink-cdc-${format}-deployed.yaml"

    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Generating FlinkDeployment manifest for ${format}..." >&2

    if [[ ! -f "$template_file" ]]; then
        error "Template file not found: $template_file"
    fi

    # Substitute environment variables.
    # NOTE: MYSQL_PASSWORD is deliberately NOT substituted — the manifest pulls it
    # in via `envFrom: secretRef: ${MYSQL_ENV_SECRET_NAME}` so no credential is
    # ever written to the generated file.
    #
    # TM_NODEPOOL defaults to driver-nodepool, NOT executor-memorynodepool to avoid Karpenter consolidation interruptping TM
    sed -e "s|\${AWS_REGION}|${AWS_REGION}|g" \
        -e "s|\${AWS_ACCOUNT_ID}|${AWS_ACCOUNT_ID}|g" \
        -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
        -e "s|\${EMR_EXECUTION_ROLE_ARN}|${EMR_EXECUTION_ROLE_ARN}|g" \
        -e "s|\${EMR_VERSION}|${EMR_VERSION:-7.13.0}|g" \
        -e "s|\${IMAGE_TAG}|${IMAGE_TAG:-${EMR_VERSION:-7.13.0}}|g" \
        -e "s|\${GLUE_DATABASE}|${GLUE_DATABASE:-flink_iceberg_db}|g" \
        -e "s|\${MYSQL_HOST}|${MYSQL_HOST}|g" \
        -e "s|\${MYSQL_USER}|${MYSQL_USER}|g" \
        -e "s|\${MYSQL_ENV_SECRET_NAME}|${MYSQL_ENV_SECRET_NAME:-flink-cdc-mysql-env}|g" \
        -e "s|\${JM_NODEPOOL}|${JM_NODEPOOL:-driver-nodepool}|g" \
        -e "s|\${TM_NODEPOOL}|${TM_NODEPOOL:-driver-nodepool}|g" \
        "$template_file" > "$output_file"

    # Fail loudly on an unsubstituted or empty placeholder. Left alone, an empty
    # region or bucket renders as a blank value that Kubernetes accepts happily
    # and the job only fails minutes later, with an error naming a heartbeat
    # timeout rather than the missing variable.
    if grep -qE '\$\{[A-Z_]+\}' "$output_file"; then
        error "Unsubstituted placeholders remain in ${output_file}:
$(grep -nE '\$\{[A-Z_]+\}' "$output_file" | head)"
    fi
    if grep -qE '^\s+value: ""\s*$' "$output_file"; then
        error "Empty env value(s) rendered into ${output_file}:
$(grep -nB2 -E '^\s+value: ""\s*$' "$output_file" | head)"
    fi

    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ✓ FlinkDeployment manifest generated: $output_file" >&2
    echo "$output_file"
}

# Deploy FlinkDeployment
deploy_flink_job() {
    local namespace=${NAMESPACE:-emr-flink}
    local manifest_file=$1

    log "Deploying FlinkDeployment to Kubernetes..."

    kubectl apply -f "$manifest_file" -n "$namespace" || \
        error "Failed to deploy FlinkDeployment"

    log "✓ FlinkDeployment applied successfully"
}

# Monitor deployment status
monitor_deployment() {
    local namespace=${NAMESPACE:-emr-flink}
    local deployment_name=$1

    log "Monitoring deployment status for: $deployment_name"
    echo ""

    log "FlinkDeployment status:"
    kubectl get flinkdeployment "$deployment_name" -n "$namespace" || true

    echo ""
    log "Pods:"
    kubectl get pods -n "$namespace" -l app="$deployment_name" || true

    echo ""
    log "To view logs, run:"
    echo "  kubectl logs -f -l app=$deployment_name -n $namespace"

    echo ""
    log "To access Flink UI, run:"
    echo "  kubectl port-forward svc/${deployment_name}-rest 8081:8081 -n $namespace"
    echo "  Then open: http://localhost:8081"
}

# Cleanup function
cleanup_deployment() {
    local namespace=${NAMESPACE:-emr-flink}
    local format=${2:-both}

    warn "This will delete the Flink CDC job(s)!"
    read -p "Are you sure? (yes/no): " -r
    echo

    if [[ $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        log "Cleaning up..."

        if [[ "$format" == "paimon" || "$format" == "both" ]]; then
            kubectl delete -f flink-cdc-paimon-deployed.yaml -n "$namespace" || true
        fi

        if [[ "$format" == "iceberg" || "$format" == "both" ]]; then
            kubectl delete -f flink-cdc-iceberg-deployed.yaml -n "$namespace" || true
        fi

        log "✓ Cleanup complete"
    else
        log "Cleanup cancelled"
    fi
}

# Main function
main() {
    local action=${1:-help}
    local format=${LAKEHOUSE_FORMAT:-both}

    echo "============================================================================"
    echo "  Flink CDC Generic Deployment Tool"
    echo "============================================================================"
    echo ""

    case $action in
        build)
            check_env_vars
            display_config
            build_and_push_image
            upload_scripts_to_s3
            ;;

        render)
            # Render the manifests without applying them.
            #
            # `deploy both` applies paimon, waits for it to reconcile, then applies
            # iceberg — the two jobs start ~30s apart. That is fine normally, but it
            # invalidates a paimon-vs-iceberg benchmark: with scan.startup.mode =
            # initial, whichever job starts first snapshots a smaller MySQL table and
            # gets a head start on the binlog. Render first, then apply both
            # manifests in one kubectl call so their snapshot phases overlap:
            #
            #   ./build-deploy-generic.sh render
            #   kubectl apply -n emr-flink -f flink-cdc-paimon-deployed.yaml \
            #                              -f flink-cdc-iceberg-deployed.yaml
            check_env_vars
            display_config
            create_ebs_storage_class
            install_kyverno_az_policy

            if [[ "$format" == "paimon" || "$format" == "both" ]]; then
                generate_flink_deployment "paimon" >/dev/null
            fi
            if [[ "$format" == "iceberg" || "$format" == "both" ]]; then
                create_glue_database
                generate_flink_deployment "iceberg" >/dev/null
            fi

            info "Manifests rendered but NOT applied. Apply both at once with:"
            echo "  kubectl apply -n ${NAMESPACE:-emr-flink} \\"
            [[ "$format" == "paimon"  || "$format" == "both" ]] && echo "    -f flink-cdc-paimon-deployed.yaml \\"
            [[ "$format" == "iceberg" || "$format" == "both" ]] && echo "    -f flink-cdc-iceberg-deployed.yaml"
            ;;

        deploy)
            check_env_vars
            display_config
            create_ebs_storage_class
            install_kyverno_az_policy
            # upload_scripts_to_s3
            # create_mysql_secret

            if [[ "$format" == "paimon" || "$format" == "both" ]]; then
                log "Deploying Paimon lakehouse..."
                manifest=$(generate_flink_deployment "paimon")
                deploy_flink_job "$manifest"
                monitor_deployment "flink-cdc-paimon"
            fi

            if [[ "$format" == "iceberg" || "$format" == "both" ]]; then
                log "Deploying Iceberg lakehouse..."
                create_glue_database
                manifest=$(generate_flink_deployment "iceberg")
                deploy_flink_job "$manifest"
                monitor_deployment "flink-cdc-iceberg"
            fi
            ;;

        all)
            check_env_vars
            display_config
            build_and_push_image
            upload_scripts_to_s3
            create_ebs_storage_class
            install_kyverno_az_policy
            # create_mysql_secret

            if [[ "$format" == "paimon" || "$format" == "both" ]]; then
                log "Deploying Paimon lakehouse..."
                manifest=$(generate_flink_deployment "paimon")
                deploy_flink_job "$manifest"
            fi

            if [[ "$format" == "iceberg" || "$format" == "both" ]]; then
                log "Deploying Iceberg lakehouse..."
                create_glue_database
                manifest=$(generate_flink_deployment "iceberg")
                deploy_flink_job "$manifest"
            fi

            if [[ "$format" == "paimon" ]]; then
                monitor_deployment "flink-cdc-paimon"
            elif [[ "$format" == "iceberg" ]]; then
                monitor_deployment "flink-cdc-iceberg"
            else
                info "Deployed both Paimon and Iceberg. Monitor separately:"
                echo "  kubectl get flinkdeployment -n emr-flink"
            fi
            ;;

        cleanup)
            cleanup_deployment "$@"
            ;;

        monitor)
            format=${2:-paimon}
            if [[ "$format" == "paimon" ]]; then
                monitor_deployment "flink-cdc-paimon"
            elif [[ "$format" == "iceberg" ]]; then
                monitor_deployment "flink-cdc-iceberg"
            else
                kubectl get flinkdeployment -n emr-flink
            fi
            ;;

        help|*)
            echo "Usage: $0 {build|render|deploy|all|cleanup|monitor} [FORMAT]"
            echo ""
            echo "Commands:"
            echo "  build    - Build Docker image and upload scripts to S3"
            echo "  render   - Render manifests only (apply both at once for a fair benchmark)"
            echo "  deploy   - Deploy Flink job to Kubernetes"
            echo "  all      - Build and deploy (complete setup)"
            echo "  cleanup  - Remove deployed resources"
            echo "  monitor  - Show deployment status"
            echo ""
            echo "Formats:"
            echo "  paimon   - Deploy Paimon lakehouse only"
            echo "  iceberg  - Deploy Iceberg lakehouse only"
            echo "  both     - Deploy both lakehouses (default)"
            echo ""
            echo "Set format with:"
            echo "  export LAKEHOUSE_FORMAT=paimon  # or iceberg or both"
            echo ""
            echo "Required environment variables:"
            echo "  AWS_REGION, AWS_ACCOUNT_ID, BUCKET_NAME,"
            echo "  EMR_EXECUTION_ROLE_ARN, MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD"
            echo ""
            echo "Optional for Iceberg:"
            echo "  GLUE_DATABASE (default: flink_iceberg_db)"
            echo ""
            echo "Examples:"
            echo "  # Deploy Paimon only"
            echo "  export LAKEHOUSE_FORMAT=paimon"
            echo "  ./deploy-generic.sh all"
            echo ""
            echo "  # Deploy Iceberg only"
            echo "  export LAKEHOUSE_FORMAT=iceberg"
            echo "  ./deploy-generic.sh all"
            echo ""
            echo "  # Deploy both"
            echo "  export LAKEHOUSE_FORMAT=both"
            echo "  ./deploy-generic.sh all"
            exit 1
            ;;
    esac

    echo ""
    log "Done!"
}

# Run main function
main "$@"
