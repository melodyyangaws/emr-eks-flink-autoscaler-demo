#!/bin/bash
###############################################################################
# Raw landing-zone generator — gzipped NDJSON on S3 — submit / observe
#
# The simple path: cid_jsongz_gen.py renders CID telemetry rows (same fields
# and distributions as ../sql-scripts/common/01-datagen-source.sql) and PUTs
# .json.gz files in the source's S3 layout. No Flink, no table format. The
# Flink datagen -> Paimon/Iceberg path (../build-deploy-cid.sh) is untouched.
#
# Usage:
#   ./run-jsongz-gen.sh start     # ConfigMap + Indexed Job, runs 30 min then completes
#   ./run-jsongz-gen.sh status    # pods + last stats line per pod
#   ./run-jsongz-gen.sh logs      # follow pod 0 (or POD=N)
#   ./run-jsongz-gen.sh stop      # delete the Job (data already written stays)
#   ./run-jsongz-gen.sh clean     # stop + delete the ConfigMap
#   ./run-jsongz-gen.sh local     # 30 s on this machine -> /tmp/cid-jsongz
#   ./run-jsongz-gen.sh render    # print the rendered Job manifest
###############################################################################

set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()   { echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" >&2; }
error() { echo -e "${RED}[ERROR]${NC} $1" >&2; exit 1; }
warn()  { echo -e "${YELLOW}[WARNING]${NC} $1" >&2; }
info()  { echo -e "${BLUE}[INFO]${NC} $1" >&2; }

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
[ -f "$HERE/../.bench-env" ] && source "$HERE/../.bench-env"

export AWS_REGION=${AWS_REGION:-us-west-2}
export NAMESPACE=${NAMESPACE:-emr-flink}
export OUTPUT_URI=${OUTPUT_URI:-s3://${BUCKET_NAME:?BUCKET_NAME not set}/cid-landing-jsongz}

# The rate knob has its OWN name. .bench-env's SOURCE_ROWS_PER_SECOND drives the
# Flink datagen path, and sourcing it here must not silently retarget this one.
# 1,500,000 rows/s x ~4,096 B = ~6.1 GB/s uncompressed (~0.3 GB/s gzipped).
export JSONGZ_ROWS_PER_SECOND=${JSONGZ_ROWS_PER_SECOND:-1500000}
# Fixed 30-minute run: every pod stops writing at 1800 s and the Job completes.
# DURATION_SECONDS may only SHORTEN it (1..1800, e.g. a quick test); there is
# no run-until-stopped mode. Validated in check_duration below.
MAX_DURATION_SECONDS=1800
export DURATION_SECONDS=${DURATION_SECONDS:-$MAX_DURATION_SECONDS}
# activeDeadlineSeconds backstop on the Job: duration + slack for pip install,
# pod start and the final upload flush. A hung pod is killed, not left running.
DEADLINE_SLACK_SECONDS=300
export WORKERS=${WORKERS:-60}
export GZIP_LEVEL=${GZIP_LEVEL:-6}
# MB-sized files (~7 MB gz mean, ~1.3-10 MB), not the real extract's ~0.4 MB:
# ~13x fewer objects for the Flink source to list, track and open. 2500 gives
# back the sample's small-file shape. See ROWS_PER_FILE in cid_jsongz_gen.py.
export ROWS_PER_FILE=${ROWS_PER_FILE:-40000}
export CID_CUSTOMER_COUNT=${CID_CUSTOMER_COUNT:-16}
export CID_HOST_COUNT=${CID_HOST_COUNT:-1024}

# Placement — see the note in jsongz-gen-job.yaml for why m6i, not c-family.
export GEN_NODEPOOL=${GEN_NODEPOOL:-executor-memorynodepool}
export GEN_INSTANCE_FAMILY=${GEN_INSTANCE_FAMILY:-m6i}
export GEN_INSTANCE_SIZE=${GEN_INSTANCE_SIZE:-16xlarge}

# Sizing. ~37K rows/s (~150 MB/s raw) per worker was measured on an M-series
# Mac; a Xeon vCPU is a hyperthread, so plan on 25K rows/s per worker. The
# rate is a CAP: under-provisioning shows up as a status rate below target.
ROWS_PER_WORKER_PLAN=${ROWS_PER_WORKER_PLAN:-25000}
if [ -z "${TOTAL_PODS:-}" ]; then
    per_pod=$(( WORKERS * ROWS_PER_WORKER_PLAN ))
    TOTAL_PODS=$(( (JSONGZ_ROWS_PER_SECOND + per_pod - 1) / per_pod ))
    SIZING="ceil(${JSONGZ_ROWS_PER_SECOND} / (${WORKERS} workers x ${ROWS_PER_WORKER_PLAN} rows/s)) = ${TOTAL_PODS} pods"
else
    SIZING="TOTAL_PODS=${TOTAL_PODS} (set explicitly)"
fi
export TOTAL_PODS

JOB=cid-jsongz-gen
CM=cid-jsongz-gen
MANIFEST="$HERE/jsongz-gen-job.yaml"

check_duration() {
    [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] \
        || error "DURATION_SECONDS='${DURATION_SECONDS}' is not a whole number of seconds (1..${MAX_DURATION_SECONDS})"
    DURATION_SECONDS=$((10#$DURATION_SECONDS))
    [ "$DURATION_SECONDS" -ge 1 ] && [ "$DURATION_SECONDS" -le "$MAX_DURATION_SECONDS" ] \
        || error "DURATION_SECONDS=${DURATION_SECONDS} out of range: the run is fixed at ${MAX_DURATION_SECONDS}s (30 min); only a shorter override 1..${MAX_DURATION_SECONDS} is allowed"
    export DURATION_SECONDS
    export JOB_DEADLINE_SECONDS=$(( DURATION_SECONDS + DEADLINE_SLACK_SECONDS ))
}

render() {
    local out
    out=$(envsubst < "$MANIFEST")
    if grep -q '\${' <<<"$out"; then
        error "unrendered placeholder in $MANIFEST: $(grep -o '\${[A-Za-z_]*}' <<<"$out" | sort -u | tr '\n' ' ')"
    fi
    echo "$out"
}

summary() {
    info "Output:      $OUTPUT_URI"
    info "Rate cap:    ${JSONGZ_ROWS_PER_SECOND} rows/s ≈ $(awk -v r="$JSONGZ_ROWS_PER_SECOND" 'BEGIN{printf "%.2f GB/s raw, %.2f GB/s gz", r*4096/1e9, r*205/1e9}')"
    info "Sizing:      $SIZING"
    info "Placement:   ${GEN_NODEPOOL} ${GEN_INSTANCE_FAMILY}.${GEN_INSTANCE_SIZE}, ${WORKERS} cpu/pod"
    info "Duration:    ${DURATION_SECONDS}s ≈ $(awk -v r="$JSONGZ_ROWS_PER_SECOND" -v d="$DURATION_SECONDS" 'BEGIN{printf "%.1f TB raw / %.2f TB gz", r*d*4096/1e12, r*d*205/1e12}') if the cap is reached, then the Job completes"
    if [ "$DURATION_SECONDS" -lt "$MAX_DURATION_SECONDS" ]; then
        warn "DURATION_SECONDS=${DURATION_SECONDS}: shorter than the standard ${MAX_DURATION_SECONDS}s run"
    fi
    info "Deadline:    activeDeadlineSeconds=${JOB_DEADLINE_SECONDS} (duration + ${DEADLINE_SLACK_SECONDS}s backstop)"
}

cmd_start() {
    check_duration
    summary
    log "ConfigMap $CM <- cid_jsongz_gen.py"
    kubectl create configmap "$CM" -n "$NAMESPACE" \
        --from-file=cid_jsongz_gen.py="$HERE/cid_jsongz_gen.py" \
        --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    # A Job's pod template is immutable, so a re-run is delete + create.
    kubectl delete job "$JOB" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    log "Job $JOB: ${TOTAL_PODS} pods"
    render | kubectl create -f - >/dev/null
    log "Started. ./run-jsongz-gen.sh status  (stats every 10 s per pod)"
}

cmd_status() {
    kubectl get pods -n "$NAMESPACE" -l app=cid-jsongz-gen -o wide 2>/dev/null || true
    echo
    local total=0 pods
    pods=$(kubectl get pods -n "$NAMESPACE" -l app=cid-jsongz-gen -o name 2>/dev/null)
    for p in $pods; do
        line=$(kubectl logs "$p" -n "$NAMESPACE" --tail=20 2>/dev/null | grep -E 'rows/s|^DONE' | tail -1 || true)
        printf '%-45s %s\n' "${p#pod/}" "${line:-<no stats yet>}"
        # The number just before "rows/s"; the "[   80s]" prefix has a variable
        # field count, so a fixed $2 picks up "80s]".
        r=$(awk '{for (i = 2; i <= NF; i++) if ($i == "rows/s") {print $(i-1); exit}}' <<<"$line")
        [ -n "$r" ] && total=$(awk -v a="$total" -v b="$r" 'BEGIN{print a+b}')
    done
    echo
    info "Cluster rate: $(awk -v t="$total" 'BEGIN{printf "%.0f rows/s ≈ %.2f GB/s raw", t, t*4096/1e9}') (cap ${JSONGZ_ROWS_PER_SECOND})"
}

cmd_logs() {
    local idx=${POD:-0} p
    p=$(kubectl get pods -n "$NAMESPACE" -l "app=cid-jsongz-gen,batch.kubernetes.io/job-completion-index=${idx}" \
        -o name | head -1)
    [ -n "$p" ] || error "no pod with completion index ${idx}"
    kubectl logs -f "$p" -n "$NAMESPACE"
}

cmd_stop() {
    kubectl delete job "$JOB" -n "$NAMESPACE" --ignore-not-found --wait=true
    log "Stopped. Files already written under $OUTPUT_URI are kept."
}

cmd_clean() {
    cmd_stop
    kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found
}

cmd_local() {
    rm -rf /tmp/cid-jsongz
    log "30 s local run -> /tmp/cid-jsongz (uncapped, all cores)"
    OUTPUT_URI=file:///tmp/cid-jsongz DURATION_SECONDS=30 TARGET_ROWS_PER_SECOND=1e12 \
        WORKERS=${LOCAL_WORKERS:-$(python3 -c 'import os; print(os.cpu_count())')} \
        TOTAL_PODS=1 python3 "$HERE/cid_jsongz_gen.py"
}

case "${1:-}" in
    start)  cmd_start ;;
    status) cmd_status ;;
    logs)   cmd_logs ;;
    stop)   cmd_stop ;;
    clean)  cmd_clean ;;
    local)  cmd_local ;;
    render) check_duration; summary; render ;;
    *)      sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
