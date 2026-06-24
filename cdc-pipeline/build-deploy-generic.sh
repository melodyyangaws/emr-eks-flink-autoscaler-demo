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
            warn "GLUE_DATABASE not set, using default: flink_icebergv3_db"
            export GLUE_DATABASE="flink_icebergv3_db"
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
    echo "  EMR Version:      ${EMR_VERSION:-7.12.0}"
    echo "  Lakehouse Format: ${LAKEHOUSE_FORMAT}"

    if [[ "$LAKEHOUSE_FORMAT" == "paimon" || "$LAKEHOUSE_FORMAT" == "both" ]]; then
        echo "  Paimon Warehouse: s3://${BUCKET_NAME}/paimonv3-warehouse/"
    fi

    if [[ "$LAKEHOUSE_FORMAT" == "iceberg" || "$LAKEHOUSE_FORMAT" == "both" ]]; then
        echo "  Iceberg Warehouse: s3://${BUCKET_NAME}/icebergv3-warehouse/"
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

# Build and push Docker image
build_and_push_image() {
    local emr_version=${EMR_VERSION:-7.12.0}
    local repo_name="emr-flink-cdc-paimon"
    local image_uri="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${repo_name}:oss-iceberg"

    log "Building Docker image..."

    # Login to ECR
    log "Logging into ECR..."
    aws ecr get-login-password --region "$AWS_REGION" | \
        docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com" || \
        error "Failed to login to ECR"

    # Create repository
    create_ecr_repo "$repo_name"

    # Build multi-platform image
    log "Building and pushing multi-platform image (amd64, arm64)..."
    docker buildx build \
        --platform linux/amd64,linux/arm64 \
        -t "$image_uri" \
        -f docker/Dockerfile.cdc-paimon \
        --build-arg EMR_VERSION="$emr_version" \
        --push . || error "Failed to build and push image"

    log "✓ Image built and pushed: $image_uri"
    echo "$image_uri"
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
        aws s3 sync sql-scripts/paimonv3/ "s3://${BUCKET_NAME}/flink/sql-scripts/paimonv3/" \
            --region "$AWS_REGION" --delete || \
            error "Failed to upload Paimon SQL scripts"

        # Copy common scripts to Paimon directory
        aws s3 cp "s3://${BUCKET_NAME}/flink/sql-scripts/common/02-cdc-sources.sql" \
            "s3://${BUCKET_NAME}/flink/sql-scripts/paimonv3/02-cdc-sources.sql" \
            --region "$AWS_REGION"
        log "✓ Paimon SQL scripts uploaded"
    fi

    # Upload Iceberg scripts if needed
    if [[ "$LAKEHOUSE_FORMAT" == "iceberg" || "$LAKEHOUSE_FORMAT" == "both" ]]; then
        aws s3 sync sql-scripts/icebergv3/ "s3://${BUCKET_NAME}/flink/sql-scripts/icebergv3/" \
            --region "$AWS_REGION" --delete || \
            error "Failed to upload Iceberg SQL scripts"
        
        # Copy common scripts to Iceberg directory
        aws s3 cp "s3://${BUCKET_NAME}/flink/sql-scripts/common/02-cdc-sources.sql" \
            "s3://${BUCKET_NAME}/flink/sql-scripts/icebergv3/02-cdc-sources.sql" \
            --region "$AWS_REGION"    
        log "✓ Iceberg SQL scripts uploaded"
    fi
}

# Create AWS Glue database for Iceberg
create_glue_database() {
    local glue_db=${GLUE_DATABASE:-flink_icebergv3_db}

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

# Generate FlinkDeployment YAML
generate_flink_deployment() {
    local format=$1
    local template_file="flink-cdc-${format}-sql.yaml"
    local output_file="flink-cdc-${format}-deployed.yaml"

    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Generating FlinkDeployment manifest for ${format}..." >&2

    if [[ ! -f "$template_file" ]]; then
        error "Template file not found: $template_file"
    fi

    # Substitute environment variables
    sed -e "s|\${AWS_REGION}|${AWS_REGION}|g" \
        -e "s|\${AWS_ACCOUNT_ID}|${AWS_ACCOUNT_ID}|g" \
        -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
        -e "s|\${EMR_EXECUTION_ROLE_ARN}|${EMR_EXECUTION_ROLE_ARN}|g" \
        -e "s|\${EMR_VERSION}|${EMR_VERSION:-7.12.0}|g" \
        -e "s|\${GLUE_DATABASE}|${GLUE_DATABASE:-flink_icebergv3_db}|g" \
        -e "s|\${MYSQL_HOST}|${MYSQL_HOST}|g" \
        -e "s|\${MYSQL_USER}|${MYSQL_USER}|g" \
        -e "s|\${MYSQL_PASSWORD}|${MYSQL_PASSWORD}|g" \
        "$template_file" > "$output_file"

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

        deploy)
            check_env_vars
            display_config
            # upload_scripts_to_s3
            # create_mysql_secret

            if [[ "$format" == "paimon" || "$format" == "both" ]]; then
                log "Deploying Paimon lakehouse..."
                manifest=$(generate_flink_deployment "paimon")
                deploy_flink_job "$manifest"
                monitor_deployment "flink-cdc-paimonv3"
            fi

            if [[ "$format" == "iceberg" || "$format" == "both" ]]; then
                log "Deploying Iceberg lakehouse..."
                create_glue_database
                manifest=$(generate_flink_deployment "iceberg")
                deploy_flink_job "$manifest"
                monitor_deployment "flink-cdc-icebergv3"
            fi
            ;;

        all)
            check_env_vars
            display_config
            build_and_push_image
            upload_scripts_to_s3
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
            echo "Usage: $0 {build|deploy|all|cleanup|monitor} [FORMAT]"
            echo ""
            echo "Commands:"
            echo "  build    - Build Docker image and upload scripts to S3"
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
            echo "  GLUE_DATABASE (default: flink_icebergv3_db)"
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
