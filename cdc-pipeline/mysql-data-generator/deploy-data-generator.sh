#!/bin/bash
###############################################################################
# MySQL Data Generator Deployment Script
#
# Manages the upsert load generator (mysql-upsert-loadgen.yaml, StatefulSet
# mysql-upsert-loadgen): batched INSERT ... ON DUPLICATE KEY UPDATE, the only
# CDC source for the benchmark. Repeated upserts on a skewed hot-key window are
# what make Iceberg accumulate equality-delete files while Paimon compacts them
# away in the background. A uniformly random key stream over 14M keys would
# measure raw ingest only and show neither effect.
#
# The target rate is RATE_PER_POD x replicas (manifest: 80,000 x 4). That is a
# token-bucket ceiling set deliberately out of reach, so the pods run flat out
# and the achieved rate (the `inst=` counter in the pod logs) is the real limit
# of MySQL plus the driver. It emits no DELETE events.
#
# The row-at-a-time INSERT/UPDATE/DELETE "mixed" generator
# (mysql-data-generator.yaml, Deployment mysql-data-generator) was removed on
# 2026-11-02; it is recoverable from git history. A trailing `upsert` argument
# and WORKLOAD=upsert are still accepted so older commands keep working.
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

# ── Backward compatibility ─────────────────────────────────────────────────
# There used to be two workloads, picked with WORKLOAD= or a trailing
# `mixed`/`upsert` argument. Only upsert remains; the trailing `upsert` is
# stripped from the argument list before the positional rate arguments are read
# (so `scale 4 upsert` still works), and WORKLOAD=mixed is rejected rather than
# silently deploying something else.
WORKLOAD=${WORKLOAD:-upsert}
ARGS=()
for arg in "$@"; do
    case $arg in
        upsert) ;;
        mixed) WORKLOAD=mixed ;;
        *) ARGS+=("$arg") ;;
    esac
done
set -- "${ARGS[@]}"

case $WORKLOAD in
    upsert) ;;
    mixed)
        error "The mixed workload (mysql-data-generator.yaml) was removed on 2026-11-02; the upsert generator is the only one. Drop WORKLOAD=mixed / the 'mixed' argument (the old manifest is in git history)."
        ;;
    *)
        error "Unknown WORKLOAD '$WORKLOAD' (only 'upsert' is supported)"
        ;;
esac

# A StatefulSet so that each pod gets a stable 0..N-1 ordinal to shard the
# hot-key window by (a Deployment's random name suffix had to be hashed, and
# hashing collided). Every kubectl call below names the kind.
MANIFEST="mysql-data-generator/mysql-upsert-loadgen.yaml"
DEPLOYMENT="mysql-upsert-loadgen"
APP_LABEL="mysql-upsert-loadgen"
KIND="statefulset"

# The manifest pins `namespace: emr-flink` on every object, so kubectl rejects
# an -n that disagrees rather than silently redirecting the deployment.
if [[ "$NAMESPACE" != "emr-flink" ]]; then
    warn "NAMESPACE=$NAMESPACE but the manifest hardcodes namespace: emr-flink."
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
    log "Deploying MySQL upsert load generator..."

    check_mysql_secret

    kubectl apply -f "$MANIFEST" || error "Failed to deploy data generator"

    log "✓ Data generator deployed successfully"

    echo ""
    info "Target rate: RATE_PER_POD x replicas (manifest: 80,000 x 4 = 320,000 rows/s),"
    info "a deliberately unreachable token-bucket ceiling so the pods run flat out."
    info "UPSERT_RATIO 0.5: half the rows update existing primary keys, half are new"
    info "inserts. No DELETE events are emitted."
    echo ""
    info "The per-pod counter line is the authoritative rate — rolled-back batches"
    info "show up as a shortfall there rather than as an error:"
    echo "  kubectl logs -l app=$APP_LABEL -n $NAMESPACE --tail=40 | grep '^\\['"

    echo ""
    log "Monitor logs with:"
    echo "  kubectl logs -f -l app=$APP_LABEL -n $NAMESPACE"
}

# Stop data generator
stop_generator() {
    log "Stopping MySQL upsert load generator..."

    kubectl delete -f "$MANIFEST" --ignore-not-found || \
        error "Failed to stop data generator"

    log "✓ Data generator stopped"
}

# Show data generator status
status_generator() {
    log "Upsert load generator status:"
    echo ""
    kubectl get "$KIND" "$DEPLOYMENT" -n "$NAMESPACE" || true
    echo ""
    kubectl get pods -l app="$APP_LABEL" -n "$NAMESPACE" || true

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
}

# Show logs
logs_generator() {
    log "Streaming upsert load generator logs..."
    kubectl logs -f -l app="$APP_LABEL" -n "$NAMESPACE"
}

# Scale data generator
scale_generator() {
    local replicas=${2:-1}
    log "Scaling data generator to $replicas replicas..."

    # The generator shards the hot-key window by POD_COUNT, so that value
    # has to move with the replica count. Setting it BEFORE scaling means the
    # rollout the env change triggers already carries the right divisor, instead
    # of pods starting on a stale one and restarting a second time. A POD_COUNT
    # below the real replica count makes slices overlap and brings back the
    # cross-pod lock convoy that capped throughput at ~69,000 rows/s.
    local shard
    shard=$(kubectl get "$KIND" "$DEPLOYMENT" -n "$NAMESPACE" \
              -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="SHARD_HOT_KEYS")].value}' \
              2>/dev/null)
    if [[ "${shard:-true}" == "true" ]]; then
        log "Setting POD_COUNT=$replicas to keep hot-key slices disjoint..."
        kubectl set env "$KIND"/"$DEPLOYMENT" POD_COUNT="$replicas" \
            -n "$NAMESPACE" >/dev/null || warn "Could not update POD_COUNT"
    fi

    kubectl scale "$KIND" "$DEPLOYMENT" \
        --replicas="$replicas" \
        -n "$NAMESPACE" || error "Failed to scale"

    log "✓ Scaled to $replicas replicas"

    local per_pod
    per_pod=$(kubectl get "$KIND" "$DEPLOYMENT" -n "$NAMESPACE" \
                -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="RATE_PER_POD")].value}' \
                2>/dev/null)
    [[ -n "$per_pod" ]] && \
        info "Aggregate target is now ${replicas} x ${per_pod} = $(( replicas * per_pod )) rows/s"
}

# Configure generation rate: rows/sec per pod, worker threads, rows per
# statement. The defaults are the manifest's current values.
configure_rate() {
    local rate=${2:-80000}
    local threads=${3:-8}
    local batch_rows=${4:-5000}

    log "Configuring upsert load rate..."
    log "  Rate per pod : ${rate} rows/s"
    log "  Threads      : ${threads}"
    log "  Rows/batch   : ${batch_rows}"

    kubectl set env "$KIND"/"$DEPLOYMENT" \
        RATE_PER_POD="$rate" \
        THREADS="$threads" \
        BATCH_ROWS="$batch_rows" \
        -n "$NAMESPACE" || error "Failed to update configuration"

    local replicas
    replicas=$(kubectl get "$KIND" "$DEPLOYMENT" -n "$NAMESPACE" \
                 -o jsonpath='{.spec.replicas}' 2>/dev/null)
    [[ -n "$replicas" ]] && \
        info "Aggregate target: ${replicas} x ${rate} = $(( replicas * rate )) rows/s"
    # With the pure-Python driver (USE_PURE=true) a pod hits the GIL at about
    # 5,000 rows/s — it burns real CPU building the multi-row statement text.
    # The C extension (the manifest default) lifts that ceiling.
    local use_pure
    use_pure=$(kubectl get "$KIND" "$DEPLOYMENT" -n "$NAMESPACE" \
                 -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="USE_PURE")].value}' \
                 2>/dev/null)
    [[ "$use_pure" == "true" && $rate -gt 6000 ]] && \
        warn "USE_PURE=true: RATE_PER_POD > 6000 is usually GIL-bound; prefer more replicas"

    log "✓ Configuration updated. Pods will restart automatically."
}

# Tune the generator's key distribution and insert/update balance. These
# are the knobs that decide WHAT is being measured rather than how fast, which is
# why they are a separate command from `config`: UPSERT_RATIO sets how much of
# the stream updates existing keys, and HOT_KEY_RATIO / HOT_KEY_WINDOW set how
# tightly those updates concentrate. Concentration is what drives Iceberg
# delete-file accumulation, so widening the window weakens the very effect the
# Paimon-vs-Iceberg comparison is looking for.
configure_upsert_mix() {
    # Defaults are the manifest's current values.
    local upsert_ratio=${2:-0.5}
    local hot_ratio=${3:-0.6}
    local hot_window=${4:-5000000}

    log "Configuring upsert mix..."
    log "  Upsert ratio   : ${upsert_ratio} (rest are new inserts)"
    log "  Hot-key ratio  : ${hot_ratio}"
    log "  Hot-key window : ${hot_window} keys"

    kubectl set env "$KIND"/"$DEPLOYMENT" \
        UPSERT_RATIO="$upsert_ratio" \
        HOT_KEY_RATIO="$hot_ratio" \
        HOT_KEY_WINDOW="$hot_window" \
        -n "$NAMESPACE" || error "Failed to update configuration"

    log "✓ Upsert mix updated. Pods will restart automatically."
}

# Show help
show_help() {
    echo "MySQL Data Generator Management Tool (upsert load generator)"
    echo ""
    echo "Usage: $0 {deploy|stop|status|logs|scale|config|upsert-mix|help} [upsert]"
    echo ""
    echo "Manages StatefulSet mysql-upsert-loadgen (mysql-upsert-loadgen.yaml):"
    echo "batched INSERT ... ON DUPLICATE KEY UPDATE on a skewed hot-key window, so"
    echo "Iceberg accumulates equality-delete files while Paimon compacts them in the"
    echo "background. Target rate = RATE_PER_POD x replicas (manifest: 80,000 x 4), a"
    echo "deliberately unreachable ceiling; read the achieved rate from 'status'."
    echo "Emits no DELETE events. (The mixed INSERT/UPDATE/DELETE generator was"
    echo "removed on 2026-11-02; a trailing 'upsert' argument is still accepted.)"
    echo ""
    echo "Commands:"
    echo "  deploy      - Deploy generator pods"
    echo "  stop        - Stop and remove the generator"
    echo "  status      - Show StatefulSet status and achieved rate per pod"
    echo "  logs        - Stream generator logs"
    echo "  scale       - Scale generator pods (keeps POD_COUNT in step)"
    echo "  config      - Set RATE_PER_POD, THREADS, BATCH_ROWS"
    echo "  upsert-mix  - Set UPSERT_RATIO, HOT_KEY_RATIO, HOT_KEY_WINDOW"
    echo "  help        - Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./deploy-data-generator.sh deploy                 # 4 pods x 80,000 target"
    echo "  ./deploy-data-generator.sh status                 # achieved rate per pod"
    echo "  ./deploy-data-generator.sh config 80000 8 5000    # rate, threads, rows/batch"
    echo "  ./deploy-data-generator.sh scale 4                # POD_COUNT follows"
    echo "  ./deploy-data-generator.sh upsert-mix 0.5 0.6 5000000"
    echo "        # upsert ratio, hot-key ratio, hot-key window — a tighter"
    echo "        # window accumulates Iceberg delete files faster"
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
        upsert-mix|mix)
            configure_upsert_mix "$@"
            ;;
        help|*)
            show_help
            ;;
    esac
}

main "$@"
