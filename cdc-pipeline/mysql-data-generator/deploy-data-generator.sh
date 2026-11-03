#!/bin/bash
###############################################################################
# MySQL Data Generator Deployment Script
#
# Manages the upsert load generator (mysql-upsert-loadgen.yaml, StatefulSet
# mysql-upsert-loadgen): batched INSERT ... ON DUPLICATE KEY UPDATE, the only
# CDC source for the benchmark. Repeated upserts on a skewed hot-key window are
# what make Iceberg accumulate equality-delete files while Paimon compacts them
# away in the background. A uniformly random key stream over 14M keys would
# measure raw ingest only and show neither effect. RECENT_KEY_RATIO of each
# batch re-writes keys the same thread wrote seconds earlier, so about half of
# the Iceberg writer's deletes are position deletes (DVs on v3) — a design
# target, see 2-DATA-GEN-GUIDE.md for how to measure it. DELETE_RATIO (0.05) of
# every orders / order_items batch is a DELETE of existing keys (an order's
# items first, in the same transaction: the FKs are RESTRICT); customers and
# products are never deleted.
#
# The target rate is RATE_PER_POD x replicas (manifest: 80,000 x 4). That is a
# token-bucket ceiling set deliberately out of reach, so the pods run flat out
# and the achieved rate (the `inst=` counter in the pod logs) is the real limit
# of MySQL plus the driver.
#
# The row-at-a-time INSERT/UPDATE/DELETE "mixed" generator
# (mysql-data-generator.yaml, Deployment mysql-data-generator) was removed on
# 2026-11-02; it is recoverable from git history. From then until DELETE_RATIO
# was added (2026-11-03) the benchmark had no DELETE events. A trailing `upsert` argument
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
# MYSQL_HOST / MYSQL_USER from .bench-env (+ .bench-env.local); a value already
# in the environment wins.
_gen_env="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.bench-env"
if [[ -f "$_gen_env" ]]; then
    _keep_host="${MYSQL_HOST:-}" _keep_user="${MYSQL_USER:-}"
    # shellcheck source=../.bench-env
    source "$_gen_env" >/dev/null 2>&1 || true
    MYSQL_HOST="${_keep_host:-${MYSQL_HOST:-}}" MYSQL_USER="${_keep_user:-${MYSQL_USER:-}}"
    unset _keep_host _keep_user
fi
unset _gen_env
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

# The ONE MySQL Secret, shared with the Flink writers (they load it with envFrom,
# so its keys are env-var names). Created by hand, from a prompt (README Step 0);
# this script never writes it, so no password passes through here.
check_mysql_secret() {
    local keys
    keys=$(kubectl get secret mysql-credentials -n "$NAMESPACE" -o jsonpath='{.data}' 2>/dev/null \
        | python3 -c 'import json,sys; print(" ".join(sorted(json.load(sys.stdin))))' 2>/dev/null) \
        || error "Secret mysql-credentials not found in ${NAMESPACE}: create it first (README Step 0)"
    for k in MYSQL_HOST MYSQL_USER MYSQL_PASSWORD; do
        [[ " $keys " == *" $k "* ]] || error "Secret mysql-credentials has no key $k (keys: ${keys}): see README Step 0"
    done
    log "✓ MySQL credentials secret found"
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
    info "RECENT_KEY_RATIO 0.6: that share of each batch re-writes keys the same thread"
    info "wrote in the last RECENT_KEY_MAX_AGE_S (20) s, targeting ~50% Iceberg position"
    info "deletes. UPSERT_RATIO 0.9 splits the rest: 90% update hot/cold keys, 10% are"
    info "new inserts. DELETE_RATIO 0.05 of orders / order_items rows are DELETEs"
    info "(an order's items first); customers and products are never deleted."
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

# One numeric field (name=1,234) from a generator counter line, digits only;
# empty if the field is absent. Anchored on a preceding space or ']' so that
# e.g. `rows=` cannot match inside another field name.
counter_field() {
    echo "$1" | grep -oE "(^|[] ])$2=[0-9,]+" | head -1 | tr -cd '0-9'
}

# Show data generator status
status_generator() {
    log "Upsert load generator status:"
    echo ""
    kubectl get "$KIND" "$DEPLOYMENT" -n "$NAMESPACE" || true
    echo ""
    kubectl get pods -l app="$APP_LABEL" -n "$NAMESPACE" || true

    echo ""
    log "Achieved rate per pod (inst= is the current rate; recent= is the share of"
    log "rows that re-wrote a recent key, cumulative since pod start):"
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
        # Pick the fields one by one: the counter line carries upserts=,
        # inserts=, recent= and batches= between them, so a single
        # 'inst=… avg=… errors=…' pattern never matched and the whole line was
        # printed instead. recent= and deletes= are absent on pods that
        # predate them.
        local inst avg_s errs batches rows recent deletes share=""
        rows=$(counter_field "$line" rows);  inst=$(counter_field "$line" inst)
        avg_s=$(counter_field "$line" avg);  errs=$(counter_field "$line" errors)
        batches=$(counter_field "$line" batches)
        recent=$(counter_field "$line" recent)
        deletes=$(counter_field "$line" deletes)
        [[ -n "$recent" && -n "$rows" && "$rows" -gt 0 ]] && \
            share=" recent=$(( 100 * recent / rows ))%"
        # One decimal: at DELETE_RATIO 0.05 the share is ~3.7%.
        [[ -n "$deletes" && -n "$rows" && "$rows" -gt 0 ]] && \
            share="$share deletes=$(( 1000 * deletes / rows / 10 )).$(( 1000 * deletes / rows % 10 ))%"
        if [[ -n "$inst" && -n "$avg_s" ]]; then
            printf '  %-42s inst=%s/s avg=%s/s errors=%s batches=%s%s\n' \
                "$(basename "$p")" "$inst" "$avg_s" "${errs:-?}" "${batches:-?}" "$share"
        else
            printf '  %-42s %s\n' "$(basename "$p")" "$line"
        fi
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
#
# Arguments 5 and 6 (RECENT_KEY_RATIO, RECENT_KEY_MAX_AGE_S) are optional and
# left untouched on the live StatefulSet when omitted, so a 3-argument call
# keeps working exactly as before. UPSERT_RATIO applies only to the rows that
# are not recent re-writes.
configure_upsert_mix() {
    # Defaults are the manifest's current values.
    local upsert_ratio=${2:-0.5}
    local hot_ratio=${3:-0.6}
    local hot_window=${4:-5000000}
    local recent_ratio=${5:-}
    local recent_age=${6:-}

    log "Configuring upsert mix..."
    log "  Upsert ratio   : ${upsert_ratio} (of the non-recent rows; rest are new inserts)"
    log "  Hot-key ratio  : ${hot_ratio}"
    log "  Hot-key window : ${hot_window} keys"
    local extra=()
    if [[ -n "$recent_ratio" ]]; then
        log "  Recent ratio   : ${recent_ratio}"
        extra+=(RECENT_KEY_RATIO="$recent_ratio")
    fi
    if [[ -n "$recent_age" ]]; then
        log "  Recent max age : ${recent_age}s"
        extra+=(RECENT_KEY_MAX_AGE_S="$recent_age")
    fi

    kubectl set env "$KIND"/"$DEPLOYMENT" \
        UPSERT_RATIO="$upsert_ratio" \
        HOT_KEY_RATIO="$hot_ratio" \
        HOT_KEY_WINDOW="$hot_window" \
        "${extra[@]}" \
        -n "$NAMESPACE" || error "Failed to update configuration"

    log "✓ Upsert mix updated. Pods will restart automatically."
}

# Set only the recent-key knobs: the share of each batch that re-writes a key
# the same thread wrote within RECENT_KEY_MAX_AGE_S seconds, which is what turns
# Iceberg's deletes into position deletes (DVs on v3) instead of equality
# deletes. Keep the age well under the 60s checkpoint interval: a re-write a
# seconds after the previous write shares its checkpoint with probability
# 1 - a/60. RECENT_KEY_RATIO=0 (with TABLE_RUN_BATCHES=1) is the old generator.
configure_recent_mix() {
    local recent_ratio=${2:-0.6}
    local recent_age=${3:-20}

    log "Configuring recent-key re-writes..."
    log "  Recent ratio   : ${recent_ratio} (share of all rows; target ~50% position deletes at 0.6)"
    log "  Recent max age : ${recent_age}s"
    if awk -v a="$recent_age" 'BEGIN { exit !(a >= 60) }'; then
        warn "RECENT_KEY_MAX_AGE_S >= 60 (the checkpoint interval): most re-writes will"
        warn "land in a later checkpoint and become equality deletes, not position deletes."
    fi

    kubectl set env "$KIND"/"$DEPLOYMENT" \
        RECENT_KEY_RATIO="$recent_ratio" \
        RECENT_KEY_MAX_AGE_S="$recent_age" \
        -n "$NAMESPACE" || error "Failed to update configuration"

    log "✓ Recent-key mix updated. Pods will restart automatically."
}

# Set DELETE_RATIO: the share of every orders / order_items batch that is a
# multi-row DELETE of existing keys (orders take their items with them, in the
# same transaction). customers and products are never deleted, so the
# stream-wide delete share is ~0.75 x this, plus the deleted orders' items.
# 0 turns deletes off. Above ~0.05 the fact tables shrink: only ~4-5% of rows
# are new inserts at UPSERT_RATIO 0.9.
configure_delete_mix() {
    local delete_ratio=${2:-0.05}

    log "Configuring deletes..."
    log "  Delete ratio   : ${delete_ratio} (of orders / order_items rows)"
    if ! awk -v a="$delete_ratio" 'BEGIN { exit !(a >= 0 && a < 1) }'; then
        error "DELETE_RATIO must be in [0, 1)"
    fi
    if awk -v a="$delete_ratio" 'BEGIN { exit !(a > 0.1) }'; then
        warn "DELETE_RATIO > 0.1: deletes then clearly outpace inserts and orders /"
        warn "order_items shrink for as long as the generator runs."
    fi

    kubectl set env "$KIND"/"$DEPLOYMENT" \
        DELETE_RATIO="$delete_ratio" \
        -n "$NAMESPACE" || error "Failed to update configuration"

    log "✓ Delete mix updated. Pods will restart automatically."
}

# Show help
show_help() {
    echo "MySQL Data Generator Management Tool (upsert load generator)"
    echo ""
    echo "Usage: $0 {deploy|stop|status|logs|scale|config|upsert-mix|recent-mix|delete-mix|help} [upsert]"
    echo ""
    echo "Manages StatefulSet mysql-upsert-loadgen (mysql-upsert-loadgen.yaml):"
    echo "batched INSERT ... ON DUPLICATE KEY UPDATE on a skewed hot-key window, so"
    echo "Iceberg accumulates equality-delete files while Paimon compacts them in the"
    echo "background. Target rate = RATE_PER_POD x replicas (manifest: 80,000 x 4), a"
    echo "deliberately unreachable ceiling; read the achieved rate from 'status'."
    echo "DELETE_RATIO of orders / order_items rows are DELETEs (customers/products"
    echo "never). (The mixed INSERT/UPDATE/DELETE generator was removed on"
    echo "2026-11-02; a trailing 'upsert' argument is still accepted.)"
    echo ""
    echo "Commands:"
    echo "  deploy      - Deploy generator pods"
    echo "  stop        - Stop and remove the generator"
    echo "  status      - Show StatefulSet status and achieved rate per pod"
    echo "  logs        - Stream generator logs"
    echo "  scale       - Scale generator pods (keeps POD_COUNT in step)"
    echo "  config      - Set RATE_PER_POD, THREADS, BATCH_ROWS"
    echo "  upsert-mix  - Set UPSERT_RATIO, HOT_KEY_RATIO, HOT_KEY_WINDOW"
    echo "                [, RECENT_KEY_RATIO, RECENT_KEY_MAX_AGE_S]"
    echo "  recent-mix  - Set RECENT_KEY_RATIO, RECENT_KEY_MAX_AGE_S only"
    echo "  delete-mix  - Set DELETE_RATIO (orders / order_items only)"
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
    echo "  ./deploy-data-generator.sh recent-mix 0.6 20"
    echo "        # share of rows re-writing a key written <20s ago -> Iceberg"
    echo "        # position deletes (DVs on v3); target ~50% at 0.6, measure it"
    echo "  ./deploy-data-generator.sh delete-mix 0.05"
    echo "        # 5% of orders / order_items rows are DELETEs; 0 turns them off"
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
        recent-mix)
            configure_recent_mix "$@"
            ;;
        delete-mix)
            configure_delete_mix "$@"
            ;;
        help|*)
            show_help
            ;;
    esac
}

main "$@"
