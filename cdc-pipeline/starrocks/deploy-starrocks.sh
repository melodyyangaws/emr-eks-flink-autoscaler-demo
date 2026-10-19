#!/bin/bash
###############################################################################
# StarRocks on EKS Deployment Script
#
# Deploys StarRocks cluster for lakehouse analytics on Paimon and Iceberg
#
# Chart layout notes (kube-starrocks 1.11.x) — these drive the shape of this
# script, so do not "simplify" them back:
#
#   * The chart has exactly two top-level values keys, `operator:` and
#     `starrocks:` (verify: helm show values starrocks/kube-starrocks
#     --version 1.11.7 | grep '^[a-zA-Z]'). There is no top-level
#     `starrocksCluster` key and no `operator.enabled` key — the real switches
#     are `operator.starrocksOperator.enabled` and `starrocks.starrocksCluster.*`.
#     `--set operator.enabled=…` / `--set starrocksCluster.enabled=…` are
#     silently ignored: `helm template` output is byte-identical with them set
#     either way.
#
#   * Consequence: the operator and the cluster CANNOT be split into two Helm
#     releases by passing those flags — both releases render the full chart.
#     The first release takes Helm ownership of ServiceAccount/starrocks,
#     ConfigMap/kube-starrocks-{fe,be}-cm and the RBAC objects, and the second
#     then dies with:
#       Error: unable to continue with install: ServiceAccount "starrocks" in
#       namespace "emr-flink" exists and cannot be imported into the current
#       release: invalid ownership metadata; annotation validation error: key
#       "meta.helm.sh/release-name" must equal "starrocks": current value is
#       "starrocks-operator"
#     So this script installs operator + cluster as ONE release ($RELEASE_NAME).
#
#   * The chart appVersion for 1.11.7 is the MUTABLE tag `4.1-latest`. Both the
#     chart version and the FE/BE image tags are therefore pinned here
#     (CHART_VERSION / IMAGE_TAG) so a reinstall lands on the same engine build.
###############################################################################

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
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
#
# NAMESPACE defaults to emr-flink, not a dedicated starrocks namespace: the
# emr-on-eks-test-execution-role trust policy allows
# system:serviceaccount:emr-flink:* only, and that role is what gives the FE/BE
# pods their S3 + Glue access via IRSA. A starrocks-sa in any other namespace is
# not covered by the trust policy and the external catalogs fail on S3/Glue.
# See the header comments in helm/starrocks-eks-values.yaml.
NAMESPACE=${NAMESPACE:-emr-flink}
RELEASE_NAME=${RELEASE_NAME:-starrocks}
# Pin the chart. Without --version, helm takes whatever the repo's latest is.
CHART_VERSION=${CHART_VERSION:-1.11.7}
# Pin the FE/BE image tags. The values file leaves image.tag empty, which falls
# back to the chart appVersion — `4.1-latest` on chart 1.11.7, a mutable tag that
# drifts on every pod restart. Keep it pinned for anything you intend to compare
# across runs.
# `${IMAGE_TAG-…}` (no colon) on purpose: an explicitly exported IMAGE_TAG="" must
# survive as empty so `IMAGE_TAG= ./deploy-starrocks.sh install` really does opt
# back into the floating tag. With `:-` an empty value would be replaced too.
IMAGE_TAG=${IMAGE_TAG-4.1.4}

# Paths are resolved from the script's own location, so the script works from any
# cwd (the values file and the catalog SQL live one level up, in cdc-pipeline/).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VALUES_FILE=${VALUES_FILE:-${REPO_DIR}/helm/starrocks-eks-values.yaml}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."

    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        error "kubectl not found. Please install kubectl."
    fi

    # Check helm
    if ! command -v helm &> /dev/null; then
        error "helm not found. Please install Helm 3."
    fi

    # Check cluster access
    if ! kubectl cluster-info &> /dev/null; then
        error "Cannot access Kubernetes cluster. Please check kubectl configuration."
    fi

    log "✓ Prerequisites check passed"
}

# Create IAM service account for StarRocks
#
# The chart does not create the FE/BE pod ServiceAccount — it only references one
# by name (starrocks.starrocks{FE,Be}Spec.serviceAccount: starrocks-sa). So the
# SA must exist, in $NAMESPACE, before deploy_starrocks() runs. Two ways to get it:
#
#   1. This function: eksctl mints a dedicated role carrying StarRocksS3GluePolicy
#      and annotates a new starrocks-sa with it.
#   2. Reuse the EMR execution role, which already has the S3 + Glue permissions
#      the external catalogs need (this is what the live cluster does):
#        kubectl create serviceaccount starrocks-sa -n emr-flink
#        kubectl annotate serviceaccount starrocks-sa -n emr-flink \
#          eks.amazonaws.com/role-arn=arn:aws:iam::${AWS_ACCOUNT_ID}:role/emr-on-eks-test-execution-role
#
# Either works; option 2 needs no eksctl and no new IAM policy. Both depend on the
# SA living in a namespace the role's trust policy allows — hence NAMESPACE=emr-flink.
create_iam_service_account() {
    local cluster_name=${EKS_CLUSTER_NAME}

    log "Creating IAM service account for StarRocks..."

    # Create policy for S3 access
    cat > /tmp/starrocks-s3-policy.json <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::${BUCKET_NAME}/*",
                "arn:aws:s3:::${BUCKET_NAME}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetPartitions",
                "glue:GetTables",
                "glue:GetDatabases"
            ],
            "Resource": "*"
        }
    ]
}
EOF

    # Create IAM policy
    local policy_arn=$(aws iam create-policy \
        --policy-name StarRocksS3GluePolicy \
        --policy-document file:///tmp/starrocks-s3-policy.json \
        --query 'Policy.Arn' \
        --output text 2>/dev/null || \
        aws iam list-policies --query "Policies[?PolicyName=='StarRocksS3GluePolicy'].Arn" --output text)

    log "Policy ARN: $policy_arn"

    # Create service account with eksctl
    if command -v eksctl &> /dev/null; then
        eksctl create iamserviceaccount \
            --name starrocks-sa \
            --namespace $NAMESPACE \
            --cluster $cluster_name \
            --attach-policy-arn $policy_arn \
            --approve \
            --override-existing-serviceaccounts || warn "Service account may already exist"
    else
        warn "eksctl not found. Please create IAM service account manually."
        info "Service account name: starrocks-sa"
        info "Namespace: $NAMESPACE"
        info "Policy ARN: $policy_arn"
    fi

    log "✓ IAM service account configuration complete"
}

# Add Helm repository
add_helm_repo() {
    log "Adding StarRocks Helm repository..."

    helm repo add starrocks https://starrocks.github.io/starrocks-kubernetes-operator || true
    helm repo update

    log "✓ Helm repository added"
}

# Create namespace
create_namespace() {
    log "Creating namespace: $NAMESPACE"

    if kubectl get namespace $NAMESPACE &> /dev/null; then
        log "✓ Namespace already exists: $NAMESPACE"
    else
        kubectl create namespace $NAMESPACE
        log "✓ Namespace created: $NAMESPACE"
    fi
}

# Deploy StarRocks (operator + FE/BE cluster) as a single Helm release
#
# One release, deliberately. See the chart layout notes at the top of this file:
# the flags that would have split operator and cluster into two releases do not
# exist on chart 1.11.x, so a two-release install collides on the shared
# ServiceAccount/ConfigMaps and the second release fails with an
# "invalid ownership metadata" error.
deploy_starrocks() {
    log "Deploying StarRocks (operator + cluster) as release '$RELEASE_NAME'..."

    if [[ ! -f "$VALUES_FILE" ]]; then
        error "Values file not found: $VALUES_FILE"
    fi

    # Build the --set list. The image tag lives under starrocks.starrocks*Spec,
    # matching the nesting introduced in chart 1.8 / required by 1.11.
    local -a set_args=()
    if [[ -n "$IMAGE_TAG" ]]; then
        info "Pinning FE/BE images to tag: $IMAGE_TAG"
        set_args+=(--set "starrocks.starrocksFESpec.image.tag=${IMAGE_TAG}")
        set_args+=(--set "starrocks.starrocksBeSpec.image.tag=${IMAGE_TAG}")
    else
        warn "IMAGE_TAG is empty — falling back to the chart appVersion, which on"
        warn "chart ${CHART_VERSION} is the MUTABLE tag '4.1-latest'. Benchmark"
        warn "numbers captured this way are not reproducible across pod restarts."
    fi

    info "Chart: starrocks/kube-starrocks --version $CHART_VERSION"
    info "Values: $VALUES_FILE"

    helm upgrade --install $RELEASE_NAME \
        starrocks/kube-starrocks \
        --version $CHART_VERSION \
        --namespace $NAMESPACE \
        --values "$VALUES_FILE" \
        "${set_args[@]}" \
        --wait --timeout 15m || error "Failed to deploy StarRocks"

    log "✓ StarRocks operator + cluster deployed"
}

# Wait for StarRocks to be ready
wait_for_starrocks() {
    log "Waiting for StarRocks to be ready..."

    local max_wait=300
    local waited=0

    while [[ $waited -lt $max_wait ]]; do
        local fe_ready=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/component=fe \
            -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' | grep -c "True" || echo 0)
        local be_ready=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/component=be \
            -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' | grep -c "True" || echo 0)

        if [[ $fe_ready -ge 1 && $be_ready -ge 1 ]]; then
            log "✓ StarRocks is ready!"
            return 0
        fi

        echo -n "."
        sleep 5
        waited=$((waited + 5))
    done

    error "Timeout waiting for StarRocks to be ready"
}

# Get StarRocks connection info
#
# There is no external endpoint to report. helm/starrocks-eks-values.yaml sets
# starrocksFESpec.service.type: ClusterIP, so the FE service has no
# .status.loadBalancer.ingress and never will — querying that jsonpath just
# returns empty forever. The two working access paths are kubectl exec into the
# FE pod (what sql-scripts/starrocks/bench-*.sh do) and kubectl port-forward.
get_connection_info() {
    log "Getting StarRocks connection information..."

    # The fe label selector matches two services: the headless "fe-search" one
    # used for peer discovery (clusterIP: None) and the real "fe-service". Filter
    # on clusterIP != None rather than taking items[0] — the headless service
    # sorts first alphabetically, which is the one you do NOT want.
    local fe_service=$(kubectl get svc -n $NAMESPACE -l app.kubernetes.io/component=fe \
        -o jsonpath='{.items[?(@.spec.clusterIP!="None")].metadata.name}' | awk '{print $1}')

    if [[ -z "$fe_service" ]]; then
        error "StarRocks FE service not found in namespace $NAMESPACE"
    fi

    local svc_type=$(kubectl get svc $fe_service -n $NAMESPACE -o jsonpath='{.spec.type}')
    local cluster_ip=$(kubectl get svc $fe_service -n $NAMESPACE -o jsonpath='{.spec.clusterIP}')
    local fe_pod=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/component=fe \
        -o jsonpath='{.items[0].metadata.name}')

    echo ""
    log "StarRocks Connection Information:"
    echo "  Namespace:    $NAMESPACE"
    echo "  FE Service:   $fe_service ($svc_type, $cluster_ip)"
    echo "  FE Pod:       ${fe_pod:-<none running>}"
    echo "  Query Port:   9030 (MySQL protocol)"
    echo "  HTTP Port:    8030 (FE web UI / REST)"
    echo ""
    echo "In-cluster (no LoadBalancer is created — the FE service is ClusterIP):"
    echo "  mysql -h ${fe_service}.${NAMESPACE}.svc.cluster.local -P 9030 -u root"
    echo ""
    echo "From your workstation, via the FE pod:"
    echo "  kubectl exec -it -n $NAMESPACE ${fe_pod:-<fe-pod>} -- mysql -h127.0.0.1 -P9030 -uroot"
    echo ""
    echo "From your workstation, via port-forward (then use a local mysql client):"
    echo "  kubectl port-forward -n $NAMESPACE svc/$fe_service 9030:9030 8030:8030"
    echo "  mysql -h 127.0.0.1 -P 9030 -u root"

    if [[ "$svc_type" != "ClusterIP" ]]; then
        echo ""
        warn "FE service type is $svc_type, not the ClusterIP this repo's values"
        warn "file configures. If you switched it to LoadBalancer, read the external"
        warn "hostname with: kubectl get svc $fe_service -n $NAMESPACE"
    fi
}

# Configure catalogs
configure_catalogs() {
    log "Configuring Paimon and Iceberg catalogs..."

    # Get FE pod
    local fe_pod=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/component=fe -o jsonpath='{.items[0].metadata.name}')

    if [[ -z "$fe_pod" ]]; then
        error "StarRocks FE pod not found"
    fi

    # The catalog SQL carries ${BUCKET_NAME} / ${AWS_REGION} placeholders verbatim
    # (not a YOUR_BUCKET literal), so render them with envsubst-style sed into
    # /tmp and leave the originals untouched.
    local sql_dir="${REPO_DIR}/sql-scripts/starrocks"
    local region=${AWS_REGION:-us-west-2}

    if [[ -z "$BUCKET_NAME" ]]; then
        warn "BUCKET_NAME is not set — the Paimon warehouse path will not render."
    fi

    for f in 01-starrocks-paimon-catalog.sql 02-starrocks-iceberg-catalog.sql; do
        sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
            -e "s|\${AWS_REGION}|${region}|g" \
            "${sql_dir}/${f}" > "/tmp/${f}"
    done

    info "SQL scripts rendered to /tmp. Run manually after deployment:"
    echo "  kubectl exec -i -n $NAMESPACE $fe_pod -- mysql -h127.0.0.1 -P9030 -uroot \\"
    echo "    < /tmp/01-starrocks-paimon-catalog.sql"
    echo "  kubectl exec -i -n $NAMESPACE $fe_pod -- mysql -h127.0.0.1 -P9030 -uroot \\"
    echo "    < /tmp/02-starrocks-iceberg-catalog.sql"
    echo ""
    echo "Sources (placeholders unrendered):"
    echo "  ${sql_dir}/01-starrocks-paimon-catalog.sql"
    echo "  ${sql_dir}/02-starrocks-iceberg-catalog.sql"
}

# Cleanup function
#
# Deliberately does NOT delete the namespace. The default namespace is emr-flink,
# which also holds the Flink operator and the CDC jobs — deleting it would take
# the whole pipeline with it. Only the Helm release and the StarRocks PVCs go.
#
# The PVCs must be deleted explicitly: they belong to StatefulSets and outlive
# them by design, so `helm uninstall` leaves 3 x 200 Gi BE data volumes, 3 x 20 Gi
# BE log volumes, a 50 Gi FE meta volume and a 5 Gi FE log volume billing. They
# are selectable by the chart's own component labels.
cleanup() {
    warn "This will delete the StarRocks release '$RELEASE_NAME' in namespace '$NAMESPACE'"
    warn "and its FE/BE PersistentVolumeClaims (all StarRocks data is lost)."
    read -p "Are you sure? (yes/no): " -r
    echo

    if [[ $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        log "Cleaning up StarRocks..."

        # Single release now — see deploy_starrocks(). The starrocks-operator
        # uninstall is kept as a no-op safety net for clusters installed by an
        # older version of this script, which did create a second release.
        helm uninstall $RELEASE_NAME -n $NAMESPACE || true
        helm uninstall starrocks-operator -n $NAMESPACE 2>/dev/null || true

        log "Deleting StarRocks PVCs (helm uninstall does not)..."
        kubectl delete pvc -n $NAMESPACE -l app.kubernetes.io/component=fe || true
        kubectl delete pvc -n $NAMESPACE -l app.kubernetes.io/component=be || true

        info "Namespace $NAMESPACE was NOT deleted — it is shared with Flink."
        log "✓ Cleanup complete"
    else
        log "Cleanup cancelled"
    fi
}

# Monitor function
#
# Everything is filtered to StarRocks resources: the default namespace is the
# shared emr-flink, so an unfiltered `kubectl get pods -n $NAMESPACE` buries the
# StarRocks pods (1 operator + 1 FE + 3 BE) under the Flink TaskManagers.
# Grep on the name rather than a label selector — the operator pod carries plain
# `app=kube-starrocks-operator`, not the app.kubernetes.io/component label the
# FE/BE pods use, so a label selector would drop it.
monitor() {
    log "Monitoring StarRocks cluster in namespace: $NAMESPACE"

    echo ""
    log "Helm release:"
    helm list -n $NAMESPACE --filter "^${RELEASE_NAME}$"

    echo ""
    log "StarRocksCluster CR:"
    kubectl get starrockscluster -n $NAMESPACE 2>/dev/null || \
        warn "No StarRocksCluster CR (operator CRD missing or cluster not installed)"

    echo ""
    log "Pods:"
    kubectl get pods -n $NAMESPACE | grep -E "NAME|starrocks" || true

    echo ""
    log "Services:"
    kubectl get svc -n $NAMESPACE | grep -E "NAME|starrocks" || true

    echo ""
    log "PVCs:"
    kubectl get pvc -n $NAMESPACE | grep -E "NAME|starrocks" || true
}

# Main function
main() {
    local action=${1:-help}

    echo "============================================================================"
    echo "  StarRocks on EKS Deployment Tool"
    echo "============================================================================"
    echo ""

    case $action in
        install)
            check_prerequisites
            create_namespace
            add_helm_repo
            deploy_starrocks
            wait_for_starrocks
            get_connection_info
            configure_catalogs
            ;;

        install-iam)
            check_prerequisites
            create_namespace
            create_iam_service_account
            add_helm_repo
            deploy_starrocks
            wait_for_starrocks
            get_connection_info
            configure_catalogs
            ;;

        uninstall)
            cleanup
            ;;

        monitor)
            monitor
            ;;

        connection)
            get_connection_info
            ;;

        help|*)
            echo "Usage: $0 {install|install-iam|uninstall|monitor|connection}"
            echo ""
            echo "Commands:"
            echo "  install       - Install StarRocks operator + cluster (one Helm release)"
            echo "  install-iam   - Install with IAM service account creation"
            echo "  uninstall     - Uninstall the release and delete the FE/BE PVCs"
            echo "  monitor       - Monitor cluster status"
            echo "  connection    - Show connection information"
            echo ""
            echo "Environment variables:"
            echo "  NAMESPACE      - Kubernetes namespace (default: emr-flink — the only"
            echo "                   namespace the execution role's trust policy covers)"
            echo "  RELEASE_NAME   - Helm release name (default: starrocks)"
            echo "  CHART_VERSION  - kube-starrocks chart version (default: 1.11.7)"
            echo "  IMAGE_TAG      - FE/BE image tag (default: 4.1.4; empty = chart"
            echo "                   appVersion, i.e. the mutable '4.1-latest' tag)"
            echo "  VALUES_FILE    - Helm values file (default: ../helm/starrocks-eks-values.yaml)"
            echo "  BUCKET_NAME    - S3 bucket for lakehouse data"
            echo "  AWS_REGION     - AWS region (default: us-west-2)"
            echo "  EKS_CLUSTER_NAME - EKS cluster name (for IAM)"
            echo ""
            echo "Example:"
            echo "  export BUCKET_NAME=my-lakehouse-bucket"
            echo "  export AWS_REGION=us-west-2"
            echo "  ./deploy-starrocks.sh install"
            exit 1
            ;;
    esac

    echo ""
    log "Done!"
}

# Run main function
main "$@"
