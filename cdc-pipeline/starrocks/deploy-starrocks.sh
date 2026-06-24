#!/bin/bash
###############################################################################
# StarRocks on EKS Deployment Script
#
# Deploys StarRocks cluster for lakehouse analytics on Paimon and Iceberg
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
NAMESPACE=${NAMESPACE:-starrocks}
RELEASE_NAME=${RELEASE_NAME:-starrocks}

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

# Deploy StarRocks operator
deploy_operator() {
    log "Deploying StarRocks operator..."

    helm upgrade --install starrocks-operator \
        starrocks/kube-starrocks \
        --namespace $NAMESPACE \
        --set operator.enabled=true \
        --set starrocksCluster.enabled=false \
        --wait || error "Failed to deploy StarRocks operator"

    log "✓ StarRocks operator deployed"
}

# Deploy StarRocks cluster
deploy_cluster() {
    log "Deploying StarRocks cluster..."

    # Update values file with environment variables
    local values_file="helm/starrocks-eks-values.yaml"

    if [[ ! -f "$values_file" ]]; then
        error "Values file not found: $values_file"
    fi

    helm upgrade --install $RELEASE_NAME \
        starrocks/kube-starrocks \
        --namespace $NAMESPACE \
        --values $values_file \
        --set starrocksCluster.enabled=true \
        --set operator.enabled=false \
        --wait --timeout 15m || error "Failed to deploy StarRocks cluster"

    log "✓ StarRocks cluster deployed"
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
get_connection_info() {
    log "Getting StarRocks connection information..."

    # Get FE service
    local fe_service=$(kubectl get svc -n $NAMESPACE -l app.kubernetes.io/component=fe -o jsonpath='{.items[0].metadata.name}')

    if [[ -z "$fe_service" ]]; then
        error "StarRocks FE service not found"
    fi

    # Get external endpoint (LoadBalancer)
    local external_ip=$(kubectl get svc $fe_service -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')

    if [[ -z "$external_ip" ]]; then
        external_ip=$(kubectl get svc $fe_service -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
    fi

    echo ""
    log "StarRocks Connection Information:"
    echo "  FE Service: $fe_service"
    echo "  External Endpoint: $external_ip"
    echo "  Query Port: 9030"
    echo "  HTTP Port: 8030"
    echo ""
    echo "Connect using MySQL client:"
    echo "  mysql -h $external_ip -P 9030 -u root"
    echo ""
    echo "Port forward for local access:"
    echo "  kubectl port-forward -n $NAMESPACE svc/$fe_service 9030:9030 8030:8030"
}

# Configure catalogs
configure_catalogs() {
    log "Configuring Paimon and Iceberg catalogs..."

    # Get FE pod
    local fe_pod=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/component=fe -o jsonpath='{.items[0].metadata.name}')

    if [[ -z "$fe_pod" ]]; then
        error "StarRocks FE pod not found"
    fi

    # Substitute environment variables in SQL scripts
    local paimon_sql=$(cat sql-scripts/starrocks/01-starrocks-paimon-catalog.sql | \
        sed "s|s3://YOUR_BUCKET|s3://${BUCKET_NAME}|g" | \
        sed "s|us-west-2|${AWS_REGION:-us-west-2}|g")

    local iceberg_sql=$(cat sql-scripts/starrocks/02-starrocks-iceberg-catalog.sql | \
        sed "s|us-west-2|${AWS_REGION:-us-west-2}|g")

    info "SQL scripts prepared. Run manually after deployment:"
    echo "  kubectl exec -it -n $NAMESPACE $fe_pod -- mysql -u root"
    echo ""
    echo "Then execute SQL from:"
    echo "  sql-scripts/starrocks/01-starrocks-paimon-catalog.sql"
    echo "  sql-scripts/starrocks/02-starrocks-iceberg-catalog.sql"
}

# Cleanup function
cleanup() {
    warn "This will delete the StarRocks cluster!"
    read -p "Are you sure? (yes/no): " -r
    echo

    if [[ $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        log "Cleaning up StarRocks..."

        helm uninstall $RELEASE_NAME -n $NAMESPACE || true
        helm uninstall starrocks-operator -n $NAMESPACE || true
        kubectl delete namespace $NAMESPACE || true

        log "✓ Cleanup complete"
    else
        log "Cleanup cancelled"
    fi
}

# Monitor function
monitor() {
    log "Monitoring StarRocks cluster..."

    echo ""
    log "Pods:"
    kubectl get pods -n $NAMESPACE

    echo ""
    log "Services:"
    kubectl get svc -n $NAMESPACE

    echo ""
    log "PVCs:"
    kubectl get pvc -n $NAMESPACE
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
            deploy_operator
            sleep 10
            deploy_cluster
            wait_for_starrocks
            get_connection_info
            configure_catalogs
            ;;

        install-iam)
            check_prerequisites
            create_namespace
            create_iam_service_account
            add_helm_repo
            deploy_operator
            sleep 10
            deploy_cluster
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
            echo "  install       - Install StarRocks cluster"
            echo "  install-iam   - Install with IAM service account creation"
            echo "  uninstall     - Uninstall StarRocks cluster"
            echo "  monitor       - Monitor cluster status"
            echo "  connection    - Show connection information"
            echo ""
            echo "Environment variables:"
            echo "  NAMESPACE      - Kubernetes namespace (default: starrocks)"
            echo "  RELEASE_NAME   - Helm release name (default: starrocks)"
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
