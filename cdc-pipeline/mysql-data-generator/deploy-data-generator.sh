#!/bin/bash
###############################################################################
# MySQL Data Generator Deployment Script
#
# Deploys a Kubernetes pod that continuously generates transactional data
# to test CDC pipelines with streaming INSERT, UPDATE, DELETE operations
###############################################################################

set -e

# Color output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Configuration
NAMESPACE=${NAMESPACE:-emr-flink}
ACTION=${1:-help}

# Check if MySQL credentials secret exists
check_mysql_secret() {
    if ! kubectl get secret mysql-credentials -n "$NAMESPACE" &>/dev/null; then
        [[ -z "${MYSQL_HOST:-}" || -z "${MYSQL_USER:-}" || -z "${MYSQL_PASSWORD:-}" ]] && \
            error "Secret mysql-credentials not found. Set MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD and re-run"
        create_mysql_secret
    fi
    log "✓ MySQL credentials secret found"
}


# Create Kubernetes secret for MySQL credentials
create_mysql_secret() {
    local namespace=${NAMESPACE:-emr-flink}

    log "Creating Kubernetes secret for MySQL credentials..."

    # Check if namespace exists
    if ! kubectl get namespace "$namespace" &>/dev/null; then
        log "Creating namespace: $namespace"
        kubectl create namespace "$namespace" || error "Failed to create namespace"
    fi

    # Delete existing secret if it exists
    if kubectl get secret mysql-credentials -n "$namespace" &>/dev/null; then
        warn "Secret already exists, deleting..."
        kubectl delete secret mysql-credentials -n "$namespace"
    fi

    # Create new secret
    kubectl create secret generic mysql-credentials \
        --from-literal=hostname="$MYSQL_HOST" \
        --from-literal=username="$MYSQL_USER" \
        --from-literal=password="$MYSQL_PASSWORD" \
        --namespace "$namespace" || error "Failed to create Kubernetes secret"

    log "✓ Kubernetes secret created: mysql-credentials"
}

# Deploy data generator
deploy_generator() {
    log "Deploying MySQL data generator..."

    check_mysql_secret

    kubectl apply -f mysql-data-generator/mysql-data-generator.yaml -n "$NAMESPACE" || \
        error "Failed to deploy data generator"

    log "✓ Data generator deployed successfully"
    echo ""
    log "Monitor logs with:"
    echo "  kubectl logs -f -l app=mysql-data-generator -n $NAMESPACE"
}

# Stop data generator
stop_generator() {
    log "Stopping MySQL data generator..."

    kubectl delete -f mysql-data-generator/mysql-data-generator.yaml -n "$NAMESPACE" || \
        error "Failed to stop data generator"

    log "✓ Data generator stopped"
}

# Show data generator status
status_generator() {
    log "Data generator status:"
    echo ""
    kubectl get deployment mysql-data-generator -n "$NAMESPACE" || true
    echo ""
    kubectl get pods -l app=mysql-data-generator -n "$NAMESPACE" || true
}

# Show logs
logs_generator() {
    log "Streaming data generator logs..."
    kubectl logs -f -l app=mysql-data-generator -n "$NAMESPACE"
}

# Scale data generator
scale_generator() {
    local replicas=${2:-1}
    log "Scaling data generator to $replicas replicas..."

    kubectl scale deployment mysql-data-generator \
        --replicas="$replicas" \
        -n "$NAMESPACE" || error "Failed to scale"

    log "✓ Scaled to $replicas replicas"
}

# Configure generation rate
configure_rate() {
    local batch_size=${2:-10}
    local sleep_seconds=${3:-5}

    log "Configuring data generation rate..."
    log "  Batch size: $batch_size"
    log "  Sleep interval: ${sleep_seconds}s"

    # Update the deployment with new env vars
    kubectl set env deployment/mysql-data-generator \
        BATCH_SIZE="$batch_size" \
        SLEEP_SECONDS="$sleep_seconds" \
        -n "$NAMESPACE" || error "Failed to update configuration"

    log "✓ Configuration updated. Pods will restart automatically."
}

# Show help
show_help() {
    echo "MySQL Data Generator Management Tool"
    echo ""
    echo "Usage: $0 {deploy|stop|status|logs|scale|config|help}"
    echo ""
    echo "Commands:"
    echo "  deploy   - Deploy data generator pod"
    echo "  stop     - Stop and remove data generator"
    echo "  status   - Show deployment status"
    echo "  logs     - Stream generator logs"
    echo "  scale    - Scale number of generator pods"
    echo "  config   - Configure generation rate"
    echo "  help     - Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Deploy generator"
    echo "  ./deploy-data-generator.sh deploy"
    echo ""
    echo "  # Watch live logs"
    echo "  ./deploy-data-generator.sh logs"
    echo ""
    echo "  # Scale to 3 pods (3x data rate)"
    echo "  ./deploy-data-generator.sh scale 3"
    echo ""
    echo "  # Configure to generate 20 records every 2 seconds"
    echo "  ./deploy-data-generator.sh config 20 2"
    echo ""
    echo "  # Check status"
    echo "  ./deploy-data-generator.sh status"
    echo ""
    echo "  # Stop generator"
    echo "  ./deploy-data-generator.sh stop"
    echo ""
    echo "Environment variables:"
    echo "  NAMESPACE - Kubernetes namespace (default: emr-flink)"
}

# Main
main() {
    case $ACTION in
        deploy)
            deploy_generator
            status_generator
            echo ""
            info "To watch live data generation:"
            echo "  ./deploy-data-generator.sh logs"
            ;;
        stop)
            stop_generator
            ;;
        status)
            status_generator
            ;;
        logs)
            logs_generator
            ;;
        scale)
            scale_generator "$@"
            ;;
        config|configure)
            configure_rate "$@"
            ;;
        help|*)
            show_help
            ;;
    esac
}

main "$@"
