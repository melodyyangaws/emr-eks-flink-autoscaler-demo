#!/bin/bash
###############################################################################
# Raw landing-zone generator — gzipped NDJSON on S3 — submit / observe
#
# The simple path: cid_jsongz_gen.py renders CID telemetry rows (the fields
# and distributions of the real sample extract) and PUTs .json.gz files in the
# source's S3 layout. No Flink, no table format. S3 event notifications
# announce each file on SQS, which is how the Flink -> Paimon/Iceberg jobs
# (../build-deploy-cid.sh) read them.
#
# Usage:
#   ./run-jsongz-gen.sh start     # render + ConfigMap + Job; pods write 30 min, then exit (nodes released)
#   ./run-jsongz-gen.sh scale N   # change the Job parallelism (rate = N x JSONGZ_ROWS_PER_POD)
#   ./run-jsongz-gen.sh status    # pods + last stats line per pod
#   ./run-jsongz-gen.sh logs      # follow the first pod (or POD=N, Nth by name)
#   ./run-jsongz-gen.sh stop      # delete the Job now, before the window ends (data already written stays)
#   ./run-jsongz-gen.sh clean     # delete the Job, legacy Deployment and ConfigMap
#   ./run-jsongz-gen.sh local     # 30 s on this machine -> /tmp/cid-jsongz
#   ./run-jsongz-gen.sh render    # write jsongz-gen-job-deployed.yaml, don't apply
#
# Size: JSONGZ_REPLICAS (default ceil(JSONGZ_ROWS_PER_SECOND / JSONGZ_ROWS_PER_POD)
# = ceil(1.5M / 500K) = 3); no TOTAL_PODS needed.
#
# Deploy pattern (same as ../build-deploy-cid.sh and ../monitoring/):
# jsongz-gen-job.yaml is rendered with envsubst over an explicit variable list
# to jsongz-gen-job-deployed.yaml (gitignored), checked for leftover ${...},
# and that file is applied with `kubectl apply -f`.
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

# The rate knobs are JSONGZ_*: this generator alone sets the benchmark's
# offered rate (the Flink jobs read as fast as they can). The cap is PER POD;
# cluster rate = replicas x per-pod.
# 500,000 rows/s/pod (60 workers, ~8.3K rows/s each) was delivered at the cap
# on m6i.16xlarge in the 2026-10-09 run, i.e. ~2 GB/s raw per pod.
export JSONGZ_ROWS_PER_POD=${JSONGZ_ROWS_PER_POD:-500000}
# Replicas: JSONGZ_REPLICAS if set, else ceil(JSONGZ_ROWS_PER_SECOND / per-pod).
# The default 1,500,000 rows/s (~6.1 GB/s raw, ~0.3 GB/s gz) gives 3 pods.
# `scale N` changes it on a running Job.
export JSONGZ_ROWS_PER_SECOND=${JSONGZ_ROWS_PER_SECOND:-1500000}
if [ -z "${JSONGZ_REPLICAS:-}" ] && [ -n "${TOTAL_PODS:-}" ]; then
    warn "TOTAL_PODS is the old Job knob; using it as JSONGZ_REPLICAS=${TOTAL_PODS}"
    JSONGZ_REPLICAS=$TOTAL_PODS
fi
if [ -z "${JSONGZ_REPLICAS:-}" ]; then
    JSONGZ_REPLICAS=$(( (JSONGZ_ROWS_PER_SECOND + JSONGZ_ROWS_PER_POD - 1) / JSONGZ_ROWS_PER_POD ))
    SIZING="ceil(${JSONGZ_ROWS_PER_SECOND} / ${JSONGZ_ROWS_PER_POD} rows/s/pod) = ${JSONGZ_REPLICAS} replicas"
else
    SIZING="JSONGZ_REPLICAS=${JSONGZ_REPLICAS} (set explicitly)"
fi
export JSONGZ_REPLICAS
# Fixed 30-minute window: every pod writes for 1800 s, then exits, the Job
# completes and Karpenter releases the nodes. DURATION_SECONDS may only SHORTEN it (1..1800, e.g. a quick test);
# there is no run-until-stopped mode. Validated in check_duration below.
MAX_DURATION_SECONDS=1800
export DURATION_SECONDS=${DURATION_SECONDS:-$MAX_DURATION_SECONDS}
# Hard stop shared by all pods (RUN_DEADLINE_EPOCH = render time + duration +
# this slack for Karpenter node launch, pip install and pod start). Replaces
# the Job's activeDeadlineSeconds: a restarted or scaled-up pod never writes
# past it. 600, not the Job's old 300: on 2026-10-09 node launch + pip took
# ~330 s, so the 2100 s Job deadline killed the pods just short of 1800 s.
DEADLINE_SLACK_SECONDS=600
export WORKERS=${WORKERS:-60}
export GZIP_LEVEL=${GZIP_LEVEL:-6}
# MB-sized files (~7 MB gz mean, ~1.3-10 MB), not the real extract's ~0.4 MB:
# ~13x fewer objects (SQS messages) for the Flink source to track and open.
# 2500 gives back the sample's small-file shape. See ROWS_PER_FILE in
# cid_jsongz_gen.py.
export ROWS_PER_FILE=${ROWS_PER_FILE:-40000}
export CID_CUSTOMER_COUNT=${CID_CUSTOMER_COUNT:-16}
export CID_HOST_COUNT=${CID_HOST_COUNT:-1024}

# Placement: nodepool only; Karpenter sizes the node from the pod's requests
# (cpu = WORKERS, memory 24Gi). WORKERS=60 needs a >=64-vCPU node, i.e. a
# 16xlarge in executor-memorynodepool (its largest size). See the note in
# jsongz-gen-job.yaml.
export GEN_NODEPOOL=${GEN_NODEPOOL:-executor-memorynodepool}

JOB=cid-jsongz-gen
LEGACY_DEPLOY=cid-jsongz-gen  # the generator used to be a Deployment (pods idled after the window)
# activeDeadlineSeconds backstop, past the shared write deadline: only a hung
# pod ever hits it.
JOB_DEADLINE_MARGIN_SECONDS=300
CM=cid-jsongz-gen
MANIFEST="$HERE/jsongz-gen-job.yaml"
DEPLOYED_MANIFEST="$HERE/jsongz-gen-job-deployed.yaml"
# Every placeholder in jsongz-gen-job.yaml. envsubst leaves anything outside
# this list as a literal ${...}, which render() then rejects.
RENDER_VARS='${NAMESPACE} ${JSONGZ_REPLICAS} ${RUN_DEADLINE_EPOCH} ${GEN_NODEPOOL}
${AWS_REGION} ${OUTPUT_URI} ${JSONGZ_ROWS_PER_POD} ${DURATION_SECONDS} ${JOB_DEADLINE_SECONDS}
${WORKERS} ${GZIP_LEVEL} ${ROWS_PER_FILE} ${CID_CUSTOMER_COUNT} ${CID_HOST_COUNT}'

check_duration() {
    [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] \
        || error "DURATION_SECONDS='${DURATION_SECONDS}' is not a whole number of seconds (1..${MAX_DURATION_SECONDS})"
    DURATION_SECONDS=$((10#$DURATION_SECONDS))
    [ "$DURATION_SECONDS" -ge 1 ] && [ "$DURATION_SECONDS" -le "$MAX_DURATION_SECONDS" ] \
        || error "DURATION_SECONDS=${DURATION_SECONDS} out of range: the run is fixed at ${MAX_DURATION_SECONDS}s (30 min); only a shorter override 1..${MAX_DURATION_SECONDS} is allowed"
    export DURATION_SECONDS
    [[ "$JSONGZ_REPLICAS" =~ ^[0-9]+$ ]] || error "JSONGZ_REPLICAS='${JSONGZ_REPLICAS}' is not a whole number"
    export RUN_DEADLINE_EPOCH=$(( $(date +%s) + DURATION_SECONDS + DEADLINE_SLACK_SECONDS ))
    export JOB_DEADLINE_SECONDS=$(( DURATION_SECONDS + DEADLINE_SLACK_SECONDS + JOB_DEADLINE_MARGIN_SECONDS ))
}

render() {
    envsubst "$RENDER_VARS" < "$MANIFEST" > "$DEPLOYED_MANIFEST"
    if grep -n '\${' "$DEPLOYED_MANIFEST" >&2; then
        error "unsubstituted \${...} above in $DEPLOYED_MANIFEST; add it to RENDER_VARS"
    fi
    log "Rendered $DEPLOYED_MANIFEST"
}

summary() {
    info "Output:      $OUTPUT_URI"
    local total=$(( JSONGZ_REPLICAS * JSONGZ_ROWS_PER_POD ))
    info "Sizing:      $SIZING"
    info "Rate cap:    ${JSONGZ_REPLICAS} x ${JSONGZ_ROWS_PER_POD} = ${total} rows/s ≈ $(awk -v r="$total" 'BEGIN{printf "%.2f GB/s raw, %.2f GB/s gz", r*4096/1e9, r*205/1e9}')"
    info "Pod size:    ${WORKERS} cpu / 24Gi per pod on ${GEN_NODEPOOL}, one pod per node"
    info "Duration:    ${DURATION_SECONDS}s ≈ $(awk -v r="$total" -v d="$DURATION_SECONDS" 'BEGIN{printf "%.1f TB raw / %.2f TB gz", r*d*4096/1e12, r*d*205/1e12}') if the cap is reached, then pods exit and nodes are released"
    if [ "$DURATION_SECONDS" -lt "$MAX_DURATION_SECONDS" ]; then
        warn "DURATION_SECONDS=${DURATION_SECONDS}: shorter than the standard ${MAX_DURATION_SECONDS}s run"
    fi
    info "Deadline:    no writes after $(date -u -r "$RUN_DEADLINE_EPOCH" +%H:%M:%SZ 2>/dev/null || date -u -d "@$RUN_DEADLINE_EPOCH" +%H:%M:%SZ) (now + duration + ${DEADLINE_SLACK_SECONDS}s)"
}

cmd_start() {
    check_duration
    summary
    render    # before touching the cluster: a bad render fails here
    log "ConfigMap $CM <- cid_jsongz_gen.py"
    kubectl create configmap "$CM" -n "$NAMESPACE" \
        --from-file=cid_jsongz_gen.py="$HERE/cid_jsongz_gen.py" \
        --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    # The old Indexed Job (same name, same pod label) would double the rate.
    kubectl delete deployment "$LEGACY_DEPLOY" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    # A Job's pod template is immutable, so a re-start replaces the Job (and
    # any pods still writing) rather than applying over it.
    kubectl delete job "$JOB" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    log "Job $JOB: parallelism ${JSONGZ_REPLICAS}, hard stop after ${JOB_DEADLINE_SECONDS}s"
    kubectl apply -f "$DEPLOYED_MANIFEST" >/dev/null
    log "Started. ./run-jsongz-gen.sh status  (stats every 10 s per pod)"
}

cmd_scale() {
    local n=${1:-}
    [[ "$n" =~ ^[0-9]+$ ]] || error "usage: ./run-jsongz-gen.sh scale N"
    kubectl patch job "$JOB" -n "$NAMESPACE" --type=merge -p "{\"spec\":{\"parallelism\":${n}}}"
    log "Scaled to ${n}. New pods write only until the deadline set by the last start."
}

cmd_status() {
    kubectl get job "$JOB" -n "$NAMESPACE" 2>/dev/null || true
    kubectl get pods -n "$NAMESPACE" -l app=cid-jsongz-gen -o wide 2>/dev/null || true
    echo
    local total=0 pods
    pods=$(kubectl get pods -n "$NAMESPACE" -l app=cid-jsongz-gen -o name 2>/dev/null)
    for p in $pods; do
        line=$(kubectl logs "$p" -n "$NAMESPACE" --tail=20 2>/dev/null | grep -E 'rows/s|^DONE|^IDLE' | tail -1 || true)
        printf '%-45s %s\n' "${p#pod/}" "${line:-<no stats yet>}"
        # The number just before "rows/s"; the "[   80s]" prefix has a variable
        # field count, so a fixed $2 picks up "80s]".
        r=$(awk '{for (i = 2; i <= NF; i++) if ($i == "rows/s") {print $(i-1); exit}}' <<<"$line")
        [ -n "$r" ] && total=$(awk -v a="$total" -v b="$r" 'BEGIN{print a+b}')
    done
    echo
    local reps per
    reps=$(kubectl get job "$JOB" -n "$NAMESPACE" -o jsonpath='{.spec.parallelism}' 2>/dev/null || true)
    per=$(kubectl get job "$JOB" -n "$NAMESPACE" \
        -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="POD_ROWS_PER_SECOND")].value}' 2>/dev/null || true)
    info "Cluster rate: $(awk -v t="$total" 'BEGIN{printf "%.0f rows/s ≈ %.2f GB/s raw", t, t*4096/1e9}') (cap ${reps:-?} x ${per:-?} rows/s)"
}

cmd_logs() {
    local idx=${POD:-0} p
    # Job pods have no index: POD=N is the Nth pod by name.
    p=$(kubectl get pods -n "$NAMESPACE" -l app=cid-jsongz-gen -o name | sort | sed -n "$((idx + 1))p")
    [ -n "$p" ] || error "no pod #${idx}"
    kubectl logs -f "$p" -n "$NAMESPACE"
}

cmd_stop() {
    # Only needed to end a run early: the Job finishes on its own after the
    # window. Pods get SIGTERM and flush in-flight PUTs; start re-creates it.
    kubectl delete job "$JOB" -n "$NAMESPACE" --ignore-not-found --wait=true
    log "Stopped. Files already written under $OUTPUT_URI are kept."
}

cmd_clean() {
    kubectl delete job "$JOB" -n "$NAMESPACE" --ignore-not-found --wait=true
    kubectl delete deployment "$LEGACY_DEPLOY" -n "$NAMESPACE" --ignore-not-found --wait=true
    kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found
    log "Cleaned. Files already written under $OUTPUT_URI are kept."
}

cmd_local() {
    rm -rf /tmp/cid-jsongz
    log "30 s local run -> /tmp/cid-jsongz (uncapped, all cores)"
    OUTPUT_URI=file:///tmp/cid-jsongz DURATION_SECONDS=30 POD_ROWS_PER_SECOND=1e12 \
        WORKERS=${LOCAL_WORKERS:-$(python3 -c 'import os; print(os.cpu_count())')} \
        RUN_DEADLINE_EPOCH= IDLE_AFTER_DONE=0 python3 "$HERE/cid_jsongz_gen.py"
}

case "${1:-}" in
    start)  cmd_start ;;
    status) cmd_status ;;
    logs)   cmd_logs ;;
    scale)  cmd_scale "${2:-}" ;;
    stop)   cmd_stop ;;
    clean)  cmd_clean ;;
    local)  cmd_local ;;
    render) check_duration; summary; render ;;
    *)      sed -n '2,27p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
