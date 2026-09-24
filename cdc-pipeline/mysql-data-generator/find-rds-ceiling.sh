#!/bin/bash
###############################################################################
# Find the real MySQL/RDS upsert ceiling by ramping generator replicas.
#
# Why this exists: the aggregate target rate (replicas x RATE_PER_POD) is only a
# token-bucket ceiling. Past some replica count the writer stops keeping up and
# the ACHIEVED rate flattens (or falls, as rolled-back batches cost their whole
# BATCH_ROWS). That flattening point is the answer — not the requested number.
#
# At each step it records, side by side:
#   - achieved rows/s, summed from the per-pod `avg=` counters
#   - generator-side deadlock/error count
#   - RDS CPU, write IOPS, write throughput, write latency, connections
# so a shortfall can be attributed to a cause rather than just observed. The
# four candidate bottlenecks look different in this data:
#   GIL-bound generator  -> per-pod avg sags, RDS CPU stays low
#   RDS CPU-bound        -> CPUUtilization pegs near 100
#   IOPS/durability-bound-> WriteIOPS flattens near provisioned, latency climbs
#   lock-contention-bound-> errors climb superlinearly, CPU and IOPS both slack
#
# Usage:  ./find-rds-ceiling.sh [hold_seconds] [step1 step2 ...]
#   ./find-rds-ceiling.sh                 # 300s holds, 8 16 24 32 40 replicas
#   ./find-rds-ceiling.sh 180 8 16 24     # quick 3-minute holds
#
# Stop early with Ctrl-C; results collected so far are already in the CSV.
###############################################################################
set -uo pipefail

NAMESPACE=${NAMESPACE:-emr-flink}
# A StatefulSet, not a Deployment: each pod owns a disjoint slice of the hot-key
# window and derives the slice from its ordinal, which only a StatefulSet hands
# out exactly (0..N-1). Every kubectl call below therefore names the kind.
WORKLOAD_KIND=${WORKLOAD_KIND:-statefulset}
DEPLOYMENT=mysql-upsert-loadgen
APP_LABEL=mysql-upsert-loadgen
DB_ID=${DB_ID:-flink-cdc-mysql-8-0}
REGION=${AWS_REGION:-us-west-2}

HOLD=${1:-300}
shift || true
STEPS=("${@:-}")
[[ -z "${STEPS[0]:-}" ]] && STEPS=(8 16 24 32 40)

OUT_DIR=${OUT_DIR:-./bench-results}
mkdir -p "$OUT_DIR"
# Timestamp comes from the shell, not the script, so reruns never clobber.
CSV="$OUT_DIR/rds-ceiling-$(date -u +%Y%m%dT%H%M%SZ).csv"

echo "replicas,target_rows_s,achieved_rows_s,pct_of_target,per_pod_avg,pods_reporting,errors_total,rds_cpu_pct,write_iops,write_tput_mb_s,write_latency_ms,db_connections,read_latency_ms" > "$CSV"

log()  { echo -e "\033[0;32m[$(date +%H:%M:%S)]\033[0m $*"; }
warn() { echo -e "\033[1;33m[WARN]\033[0m $*"; }

# Average a CloudWatch RDS metric over the window just held. 60s period, and we
# take the mean of the datapoints inside the hold rather than the last one, so a
# single spike at the boundary does not become the reported value.
rds_metric() {
    local metric=$1 secs=$2
    aws cloudwatch get-metric-statistics \
        --namespace AWS/RDS --metric-name "$metric" \
        --dimensions Name=DBInstanceIdentifier,Value="$DB_ID" \
        --start-time "$(date -u -v-"${secs}"S +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d "-${secs} seconds" +%Y-%m-%dT%H:%M:%S)" \
        --end-time   "$(date -u +%Y-%m-%dT%H:%M:%S)" \
        --period 60 --statistics Average --region "$REGION" \
        --query 'Datapoints[].Average' --output text 2>/dev/null \
      | tr '\t' '\n' | awk 'NF{s+=$1;n++} END{if(n)printf "%.2f",s/n; else print "NA"}'
}

# Sum the per-pod `avg=` counters. avg is cumulative-since-pod-start, which is
# what we want after a full hold: `inst=` is a 15s sample on staggered clocks and
# reads high or low depending on when the poll lands.
collect_rate() {
    local pods total=0 n=0 errs=0
    pods=$(kubectl get pods -n "$NAMESPACE" -l app="$APP_LABEL" \
             --field-selector=status.phase=Running -o name 2>/dev/null)
    for p in $pods; do
        local line avg e
        line=$(kubectl logs -n "$NAMESPACE" "$p" --tail=40 2>/dev/null | grep '^\[' | tail -1)
        [[ -z "$line" ]] && continue
        avg=$(echo "$line" | grep -oE 'avg=[0-9,]+' | head -1 | tr -cd '0-9')
        e=$(echo "$line"  | grep -oE 'errors=[0-9,]+' | head -1 | tr -cd '0-9')
        [[ -n "$avg" ]] && { total=$((total+avg)); n=$((n+1)); }
        [[ -n "$e"   ]] && errs=$((errs+e))
    done
    echo "$total $n $errs"
}

trap 'warn "interrupted — partial results in $CSV"; exit 130' INT TERM

RATE_PER_POD=$(kubectl get "$WORKLOAD_KIND" "$DEPLOYMENT" -n "$NAMESPACE" \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="RATE_PER_POD")].value}' 2>/dev/null)
RATE_PER_POD=${RATE_PER_POD:-5000}
log "RATE_PER_POD=$RATE_PER_POD  hold=${HOLD}s  steps: ${STEPS[*]}"
log "CSV -> $CSV"

for r in "${STEPS[@]}"; do
    log "──────── scaling to $r replicas (target $((r*RATE_PER_POD)) rows/s) ────────"
    # POD_COUNT is the divisor that sizes each pod's hot-key slice, so it has to
    # track the replica count at every step or the ramp measures the wrong thing:
    # left low, slices overlap and the cross-pod lock convoy comes back (which is
    # exactly what the pre-sharding ramp hit at ~69,000 rows/s); left high, part
    # of the window goes unwritten. Set it BEFORE scaling so the rollout the env
    # change triggers already carries the right value instead of restarting twice.
    kubectl set env "$WORKLOAD_KIND"/"$DEPLOYMENT" POD_COUNT="$r" \
        -n "$NAMESPACE" >/dev/null || warn "could not set POD_COUNT=$r"
    # A StatefulSet's RollingUpdate replaces pods ONE at a time in reverse ordinal
    # order, waiting for each to become Ready — at 40 pods and ~20s of startup
    # that is a 13-minute rollout, longer than the readiness cap below, so the
    # step would be recorded as "capacity-limited" when it is merely mid-rollout.
    # Deleting them all instead lets podManagementPolicy: Parallel bring the whole
    # set back at once. Every pod has to restart anyway: POD_COUNT changed, and a
    # pod reads it only at startup. They are stateless writers, so there is
    # nothing to drain.
    kubectl delete pod -l app="$APP_LABEL" -n "$NAMESPACE" \
        --wait=false >/dev/null 2>&1 || true
    kubectl scale "$WORKLOAD_KIND" "$DEPLOYMENT" --replicas="$r" -n "$NAMESPACE" >/dev/null || {
        warn "scale to $r failed; stopping ramp"; break; }

    # Restarting pods reset their cumulative avg, so every pod must be Running
    # before the hold starts or the sum is taken over a mix of warm and cold
    # counters. Cap the wait: Karpenter may simply not have capacity for this
    # step, which is itself a finding worth recording rather than hanging on.
    local_wait=0
    while :; do
        ready=$(kubectl get pods -n "$NAMESPACE" -l app="$APP_LABEL" \
                  --field-selector=status.phase=Running --no-headers 2>/dev/null | grep -c '1/1')
        [[ "$ready" -ge "$r" ]] && break
        [[ $local_wait -ge 420 ]] && { warn "only $ready/$r pods Running after 7m — recording anyway (capacity-limited)"; break; }
        sleep 20; local_wait=$((local_wait+20))
    done

    log "holding ${HOLD}s at $r replicas..."
    sleep "$HOLD"

    read -r achieved n errs <<<"$(collect_rate)"
    cpu=$(rds_metric CPUUtilization "$HOLD")
    wio=$(rds_metric WriteIOPS "$HOLD")
    wtp=$(rds_metric WriteThroughput "$HOLD")
    wlat=$(rds_metric WriteLatency "$HOLD")
    rlat=$(rds_metric ReadLatency "$HOLD")
    conn=$(rds_metric DatabaseConnections "$HOLD")

    target=$((r*RATE_PER_POD))
    pct=$(awk -v a="$achieved" -v t="$target" 'BEGIN{if(t)printf "%.1f",100*a/t; else print "NA"}')
    perpod=$(awk -v a="$achieved" -v n="$n" 'BEGIN{if(n)printf "%.0f",a/n; else print 0}')
    wtpmb=$(awk -v v="$wtp" 'BEGIN{if(v=="NA")print "NA"; else printf "%.1f",v/1048576}')
    wlatms=$(awk -v v="$wlat" 'BEGIN{if(v=="NA")print "NA"; else printf "%.2f",v*1000}')
    rlatms=$(awk -v v="$rlat" 'BEGIN{if(v=="NA")print "NA"; else printf "%.2f",v*1000}')

    echo "$r,$target,$achieved,$pct,$perpod,$n,$errs,$cpu,$wio,$wtpmb,$wlatms,$conn,$rlatms" >> "$CSV"
    log "  achieved $(printf "%'d" "$achieved") rows/s (${pct}% of target) | per-pod ${perpod} | pods ${n}/${r}"
    log "  RDS cpu=${cpu}% wIOPS=${wio} wTput=${wtpmb}MB/s wLat=${wlatms}ms conn=${conn} errors=${errs}"
done

echo ""
log "═══════════════ ramp complete ═══════════════"
column -s, -t < "$CSV"
echo ""
log "CSV: $CSV"
