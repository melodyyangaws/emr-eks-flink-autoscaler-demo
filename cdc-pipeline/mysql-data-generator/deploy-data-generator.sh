#!/bin/bash
###############################################################################
# MySQL Data Generator Deployment Script
#
# Manages two different generator workloads against the same MySQL database.
# Pick one with WORKLOAD= (or the `upsert` / `mixed` alias as the last argument):
#
#   WORKLOAD=mixed   (default)  mysql-data-generator.yaml
#       Row-at-a-time INSERT / UPDATE / DELETE mix. Exercises all three CDC
#       event types including deletes, which the upsert workload never emits.
#       Ceiling is roughly 400 rows/s — it issues one statement per row and
#       picks rows with ORDER BY RAND(), so it does not scale. Use it for
#       correctness and event-type coverage, not for load.
#
#   WORKLOAD=upsert             mysql-upsert-loadgen.yaml
#       Batched INSERT ... ON DUPLICATE KEY UPDATE at ~40,000 rows/s across 8
#       pods. This is the storage-format benchmark workload: repeated upserts on
#       a skewed hot-key window are what make Iceberg accumulate equality-delete
#       files while Paimon compacts them away in the background. A uniformly
#       random key stream over 14M keys would measure raw ingest only and show
#       neither effect.
#
# The two rate models are NOT interchangeable, which is why `config` branches on
# the workload: mixed is driven by BATCH_SIZE + SLEEP_SECONDS (rows per tick),
# upsert by RATE_PER_POD + THREADS + BATCH_ROWS (a token-bucket rows/sec target).
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

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Configuration
NAMESPACE=${NAMESPACE:-emr-flink}
ACTION=${1:-help}

# ── Workload selection ──────────────────────────────────────────────────────
# Accept `mixed`/`upsert` as a trailing argument as well as WORKLOAD=, so both
# `WORKLOAD=upsert ./deploy-data-generator.sh deploy` and
# `./deploy-data-generator.sh deploy upsert` work. The trailing form is stripped
# from the argument list before the positional rate arguments are read, so it
# cannot be mistaken for a batch size or replica count.
WORKLOAD=${WORKLOAD:-mixed}
ARGS=()
for arg in "$@"; do
    case $arg in
        upsert|mixed) WORKLOAD=$arg ;;
        *) ARGS+=("$arg") ;;
    esac
done
set -- "${ARGS[@]}"

case $WORKLOAD in
    mixed)
        MANIFEST="mysql-data-generator/mysql-data-generator.yaml"
        DEPLOYMENT="mysql-data-generator"
        APP_LABEL="mysql-data-generator"
        ;;
    upsert)
        MANIFEST="mysql-data-generator/mysql-upsert-loadgen.yaml"
        DEPLOYMENT="mysql-upsert-loadgen"
        APP_LABEL="mysql-upsert-loadgen"
        ;;
    *)
        error "Unknown WORKLOAD '$WORKLOAD' (expected 'mixed' or 'upsert')"
        ;;
esac

# Both manifests pin `namespace: emr-flink` on every object, so kubectl rejects
# an -n that disagrees rather than silently redirecting the deployment.
if [[ "$NAMESPACE" != "emr-flink" ]]; then
    warn "NAMESPACE=$NAMESPACE but the manifests hardcode namespace: emr-flink."
    warn "Edit $MANIFEST if you really need a different namespace."
fi

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
    log "Deploying MySQL data generator (workload: $WORKLOAD)..."

    check_mysql_secret

    kubectl apply -f "$MANIFEST" || error "Failed to deploy data generator"

    log "✓ Data generator deployed successfully"

    if [[ "$WORKLOAD" == upsert ]]; then
        echo ""
        info "Target rate: RATE_PER_POD x replicas (default 5000 x 8 = 40,000 rows/s)"
        info "80% of rows are upserts on existing primary keys; no DELETEs are"
        info "emitted, so use the mixed workload if you need delete-event coverage."
        echo ""
        info "The per-pod counter line is the authoritative rate — the target is a"
        info "token-bucket ceiling, and rolled-back batches would show up as a"
        info "shortfall there rather than as an error:"
        echo "  kubectl logs -l app=$APP_LABEL -n $NAMESPACE --tail=40 | grep '^\\['"
    fi

    echo ""
    log "Monitor logs with:"
    echo "  kubectl logs -f -l app=$APP_LABEL -n $NAMESPACE"
}

# Stop data generator
stop_generator() {
    log "Stopping MySQL data generator (workload: $WORKLOAD)..."

    kubectl delete -f "$MANIFEST" --ignore-not-found || \
        error "Failed to stop data generator"

    log "✓ Data generator stopped"
}

# Show data generator status
status_generator() {
    log "Data generator status (workload: $WORKLOAD):"
    echo ""
    kubectl get deployment "$DEPLOYMENT" -n "$NAMESPACE" || true
    echo ""
    kubectl get pods -l app="$APP_LABEL" -n "$NAMESPACE" || true

    if [[ "$WORKLOAD" == upsert ]]; then
        echo ""
        log "Achieved rate per pod (avg is the number to quote):"
        local pods
        pods=$(kubectl get pods -n "$NAMESPACE" -l app="$APP_LABEL" \
                 -o name 2>/dev/null) || true
        [[ -z "$pods" ]] && { info "no pods running"; return; }
        local total=0
        for p in $pods; do
            local line
            line=$(kubectl logs -n "$NAMESPACE" "$p" --tail=60 2>/dev/null \
                     | grep '^\[' | tail -1)
            [[ -z "$line" ]] && continue
            printf '  %-42s %s\n' "$(basename "$p")" \
                "$(echo "$line" | grep -oE 'inst=[0-9,]+/s avg=[0-9,]+/s errors=[0-9]+' || echo "$line")"
            # Sum the averages rather than the instantaneous values: `inst` is a
            # 15s sample and the pods report on staggered clocks, so summing it
            # reads high or low depending on when this ran.
            local avg
            avg=$(echo "$line" | grep -oE 'avg=[0-9,]+' | head -1 | tr -d 'avg=,')
            [[ -n "$avg" ]] && total=$(( total + avg ))
        done
        [[ $total -gt 0 ]] && log "  aggregate ≈ $(printf "%'d" "$total") rows/s"
    fi
}

# Show logs
logs_generator() {
    log "Streaming data generator logs (workload: $WORKLOAD)..."
    kubectl logs -f -l app="$APP_LABEL" -n "$NAMESPACE"
}

# Scale data generator
scale_generator() {
    local replicas=${2:-1}
    log "Scaling data generator to $replicas replicas..."

    kubectl scale deployment "$DEPLOYMENT" \
        --replicas="$replicas" \
        -n "$NAMESPACE" || error "Failed to scale"

    log "✓ Scaled to $replicas replicas"

    if [[ "$WORKLOAD" == upsert ]]; then
        local per_pod
        per_pod=$(kubectl get deployment "$DEPLOYMENT" -n "$NAMESPACE" \
                    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="RATE_PER_POD")].value}' \
                    2>/dev/null)
        [[ -n "$per_pod" ]] && \
            info "Aggregate target is now ${replicas} x ${per_pod} = $(( replicas * per_pod )) rows/s"
    fi
}

# Configure generation rate. The two workloads have different rate models, so
# the positional arguments mean different things for each.
configure_rate() {
    if [[ "$WORKLOAD" == upsert ]]; then
        # upsert: rows/sec per pod, worker threads, rows per statement.
        local rate=${2:-5000}
        local threads=${3:-4}
        local batch_rows=${4:-500}

        log "Configuring upsert load rate..."
        log "  Rate per pod : ${rate} rows/s"
        log "  Threads      : ${threads}"
        log "  Rows/batch   : ${batch_rows}"

        kubectl set env deployment/"$DEPLOYMENT" \
            RATE_PER_POD="$rate" \
            THREADS="$threads" \
            BATCH_ROWS="$batch_rows" \
            -n "$NAMESPACE" || error "Failed to update configuration"

        local replicas
        replicas=$(kubectl get deployment "$DEPLOYMENT" -n "$NAMESPACE" \
                     -o jsonpath='{.spec.replicas}' 2>/dev/null)
        [[ -n "$replicas" ]] && \
            info "Aggregate target: ${replicas} x ${rate} = $(( replicas * rate )) rows/s"
        # Raising RATE_PER_POD past ~5000 on one pod hits the GIL before it hits
        # MySQL — the driver burns real CPU building the multi-row statement text.
        # Add replicas instead.
        [[ $rate -gt 6000 ]] && \
            warn "RATE_PER_POD > 6000 is usually GIL-bound; prefer more replicas"
    else
        # mixed: rows per tick, and seconds between ticks.
        local batch_size=${2:-10}
        local sleep_seconds=${3:-5}

        log "Configuring data generation rate..."
        log "  Batch size: $batch_size"
        log "  Sleep interval: ${sleep_seconds}s"

        kubectl set env deployment/"$DEPLOYMENT" \
            BATCH_SIZE="$batch_size" \
            SLEEP_SECONDS="$sleep_seconds" \
            -n "$NAMESPACE" || error "Failed to update configuration"
    fi

    log "✓ Configuration updated. Pods will restart automatically."
}

# Tune the upsert workload's key distribution and insert/update balance. These
# are the knobs that decide WHAT is being measured rather than how fast, which is
# why they are a separate command from `config`: UPSERT_RATIO sets how much of
# the stream updates existing keys, and HOT_KEY_RATIO / HOT_KEY_WINDOW set how
# tightly those updates concentrate. Concentration is what drives Iceberg
# delete-file accumulation, so widening the window weakens the very effect the
# Paimon-vs-Iceberg comparison is looking for.
configure_upsert_mix() {
    [[ "$WORKLOAD" == upsert ]] || \
        error "upsert-mix applies to the upsert workload only (pass 'upsert')"

    local upsert_ratio=${2:-0.8}
    local hot_ratio=${3:-0.6}
    local hot_window=${4:-50000}

    log "Configuring upsert mix..."
    log "  Upsert ratio   : ${upsert_ratio} (rest are new inserts)"
    log "  Hot-key ratio  : ${hot_ratio}"
    log "  Hot-key window : ${hot_window} keys"

    kubectl set env deployment/"$DEPLOYMENT" \
        UPSERT_RATIO="$upsert_ratio" \
        HOT_KEY_RATIO="$hot_ratio" \
        HOT_KEY_WINDOW="$hot_window" \
        -n "$NAMESPACE" || error "Failed to update configuration"

    log "✓ Upsert mix updated. Pods will restart automatically."
}

# Show help
show_help() {
    echo "MySQL Data Generator Management Tool"
    echo ""
    echo "Usage: $0 {deploy|stop|status|logs|scale|config|upsert-mix|help} [mixed|upsert]"
    echo ""
    echo "Workloads (append 'mixed' or 'upsert' to any command; default mixed):"
    echo "  mixed    - INSERT/UPDATE/DELETE mix, row-at-a-time. ~400 rows/s ceiling."
    echo "             The only workload that emits DELETE events."
    echo "  upsert   - Batched INSERT ... ON DUPLICATE KEY UPDATE, ~40,000 rows/s"
    echo "             across 8 pods. The storage-format benchmark workload: hot-key"
    echo "             upserts make Iceberg accumulate equality-delete files while"
    echo "             Paimon compacts them in the background. Emits no DELETEs."
    echo ""
    echo "Commands:"
    echo "  deploy      - Deploy data generator pods"
    echo "  stop        - Stop and remove data generator"
    echo "  status      - Show deployment status (+ achieved rate for upsert)"
    echo "  logs        - Stream generator logs"
    echo "  scale       - Scale number of generator pods"
    echo "  config      - Configure generation rate (arguments differ per workload)"
    echo "  upsert-mix  - Tune upsert ratio and hot-key skew (upsert only)"
    echo "  help        - Show this help message"
    echo ""
    echo "Examples — mixed workload (CDC correctness, all three event types):"
    echo "  ./deploy-data-generator.sh deploy"
    echo "  ./deploy-data-generator.sh config 20 2      # 20 records every 2s"
    echo "  ./deploy-data-generator.sh scale 3"
    echo ""
    echo "Examples — upsert workload (40k/s benchmark load):"
    echo "  ./deploy-data-generator.sh deploy upsert          # 8 pods x 5000 = 40k/s"
    echo "  ./deploy-data-generator.sh status upsert          # achieved rate per pod"
    echo "  ./deploy-data-generator.sh config 5000 4 500 upsert   # rate, threads, rows/batch"
    echo "  ./deploy-data-generator.sh scale 12 upsert        # 12 x 5000 = 60k/s"
    echo "  ./deploy-data-generator.sh upsert-mix 0.9 0.8 20000 upsert"
    echo "        # 90% upserts, 80% of them into a 20k-key window — a tighter"
    echo "        # window accumulates Iceberg delete files faster"
    echo "  ./deploy-data-generator.sh stop upsert"
    echo ""
    echo "  # WORKLOAD= works too, if you prefer it to the trailing argument:"
    echo "  WORKLOAD=upsert ./deploy-data-generator.sh deploy"
    echo ""
    echo "Environment variables:"
    echo "  NAMESPACE - Kubernetes namespace (default: emr-flink)"
    echo "  WORKLOAD  - mixed | upsert (default: mixed)"
}

# Main
main() {
    case $ACTION in
        deploy)
            deploy_generator
            status_generator
            echo ""
            info "To watch live data generation:"
            echo "  ./deploy-data-generator.sh logs $WORKLOAD"
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
        upsert-mix|mix)
            configure_upsert_mix "$@"
            ;;
        help|*)
            show_help
            ;;
    esac
}

main "$@"
