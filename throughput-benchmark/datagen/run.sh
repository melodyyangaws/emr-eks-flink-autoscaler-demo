#!/bin/bash
###############################################################################
# Raw landing-zone generator — gzipped NDJSON on S3 — submit / observe
#
# The simple path: cid_jsongz_gen.py renders CID telemetry rows (the fields
# and distributions of the real sample extract) and PUTs .json.gz files in the
# source's S3 layout. No Flink, no table format. S3 event notifications
# announce each file on SQS, which is how the Flink -> Paimon/Iceberg jobs
# (flinkwriter/run.sh) read them. The generator's code and Job template are in
# jsongz-gen/; the Flink source that reads the files is sqs-delta-filename/.
#
# Usage (from throughput-benchmark/; .bench-env is sourced by lib/common.sh):
#   ./datagen/run.sh start     # render + ConfigMap + Job; pods write DURATION_SECONDS, then exit (0 = until stop)
#   ./datagen/run.sh scale N   # change the Job parallelism (rate = N x JSONGZ_ROWS_PER_POD)
#   ./datagen/run.sh status    # pods + last stats line per pod
#   ./datagen/run.sh logs      # follow the first pod (or POD=N, Nth by name)
#   ./datagen/run.sh stop      # delete the Job and ConfigMap; ends a DURATION_SECONDS=0 run (data already written stays)
#   ./datagen/run.sh render    # write jsongz-gen/jsongz-gen-job-deployed.yaml, don't apply
#
# Size: JSONGZ_REPLICAS (.bench-env: 7); no TOTAL_PODS needed.
#
# Deploy pattern (as every component's run.sh): jsongz-gen/jsongz-gen-job.yaml
# is rendered by lib/common.sh's render() to jsongz-gen-job-deployed.yaml
# (gitignored), and that file is applied with `kubectl apply -f`.
###############################################################################

set -e
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# .bench-env, NAMESPACE, AWS_REGION, log/info/warn/error, render.
# shellcheck source=../lib/common.sh
source "$HERE/../lib/common.sh"
GEN="$HERE/jsongz-gen"
# The one landing location, from .bench-env only: the jobs read nothing else
# ('key-prefix' in 01-raw-s3sqs.sql), so no fallback path here.
export OUTPUT_URI=${OUTPUT_URI:?OUTPUT_URI is not set in .bench-env}

# The rate knobs are JSONGZ_*: this generator alone sets the benchmark's
# offered rate (the Flink jobs read as fast as they can). The cap is PER POD;
# cluster rate = replicas x per-pod.
# 500,000 rows/s/pod (60 workers, ~8.3K rows/s each) was delivered at the cap
# on m6i.16xlarge in the 2026-10-09 run, i.e. ~2 GB/s raw per pod.
export JSONGZ_ROWS_PER_POD=${JSONGZ_ROWS_PER_POD:-500000}
# Replicas: JSONGZ_REPLICAS (.bench-env). `scale N` changes it on a running Job.
if [ -z "${JSONGZ_REPLICAS:-}" ] && [ -n "${TOTAL_PODS:-}" ]; then
    warn "TOTAL_PODS is the old Job knob; using it as JSONGZ_REPLICAS=${TOTAL_PODS}"
    JSONGZ_REPLICAS=$TOTAL_PODS
fi
[ -n "${JSONGZ_REPLICAS:-}" ] || die "JSONGZ_REPLICAS is not set (.bench-env)"
export JSONGZ_REPLICAS
# DURATION_SECONDS is the only time setting:
#   > 0  every pod writes that long from its own start (default 1800),
#        then exits; the Job completes and Karpenter releases the nodes.
#   0    every pod writes until stopped: `stop`, or querybench/qb-watch.sh once
#        the hot query test is done (2026-10-19: a 1-hour generator stopped
#        mid-test, so a hot run is not given a fixed length).
export DURATION_SECONDS=${DURATION_SECONDS:-1800}
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
CM=cid-jsongz-gen
MANIFEST="$GEN/jsongz-gen-job.yaml"
DEPLOYED_MANIFEST="$GEN/jsongz-gen-job-deployed.yaml"
# Every placeholder in jsongz-gen-job.yaml (lib/common.sh's render rejects one left out).
RENDER_VARS="NAMESPACE JSONGZ_REPLICAS GEN_NODEPOOL AWS_REGION OUTPUT_URI JSONGZ_ROWS_PER_POD
DURATION_SECONDS WORKERS GZIP_LEVEL ROWS_PER_FILE CID_CUSTOMER_COUNT CID_HOST_COUNT"

check_duration() {
    [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] \
        || error "DURATION_SECONDS='${DURATION_SECONDS}' is not a whole number of seconds (0 = until stopped)"
    DURATION_SECONDS=$((10#$DURATION_SECONDS))
    export DURATION_SECONDS
    [[ "$JSONGZ_REPLICAS" =~ ^[0-9]+$ ]] || error "JSONGZ_REPLICAS='${JSONGZ_REPLICAS}' is not a whole number"
}

render_job() {
    # shellcheck disable=SC2086
    render "$MANIFEST" "$DEPLOYED_MANIFEST" $RENDER_VARS || error "could not render $DEPLOYED_MANIFEST"
    log "Rendered $DEPLOYED_MANIFEST"
}

summary() {
    info "Output:      $OUTPUT_URI"
    local total=$(( JSONGZ_REPLICAS * JSONGZ_ROWS_PER_POD ))
    info "Rate cap:    ${JSONGZ_REPLICAS} x ${JSONGZ_ROWS_PER_POD} = ${total} rows/s ≈ $(awk -v r="$total" 'BEGIN{printf "%.2f GB/s raw, %.2f GB/s gz", r*4096/1e9, r*205/1e9}')"
    info "Pod size:    ${WORKERS} cpu / 24Gi per pod on ${GEN_NODEPOOL}, one pod per node"
    if [ "$DURATION_SECONDS" -eq 0 ]; then
        info "Duration:    until stopped (./datagen/run.sh stop, or querybench/qb-watch.sh); ≈ $(awk -v r="$total" 'BEGIN{printf "%.1f TB raw / %.2f TB gz", r*3600*4096/1e12, r*3600*205/1e12}') per hour if the cap is reached"
        warn "DURATION_SECONDS=0: nothing stops it by itself; run ./datagen/run.sh stop when done"
    else
        info "Duration:    ${DURATION_SECONDS}s ≈ $(awk -v r="$total" -v d="$DURATION_SECONDS" 'BEGIN{printf "%.1f TB raw / %.2f TB gz", r*d*4096/1e12, r*d*205/1e12}') if the cap is reached, then pods exit and nodes are released"
    fi
}

cmd_start() {
    check_duration
    summary
    render_job    # before touching the cluster: a bad render fails here
    log "ConfigMap $CM <- cid_jsongz_gen.py"
    kubectl create configmap "$CM" -n "$NAMESPACE" \
        --from-file=cid_jsongz_gen.py="$GEN/cid_jsongz_gen.py" \
        --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    # The old Indexed Job (same name, same pod label) would double the rate.
    kubectl delete deployment "$LEGACY_DEPLOY" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    # A Job's pod template is immutable, so a re-start replaces the Job (and
    # any pods still writing) rather than applying over it.
    kubectl delete job "$JOB" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    log "Job $JOB: parallelism ${JSONGZ_REPLICAS}, $([ "$DURATION_SECONDS" -eq 0 ] && echo 'until stopped' || echo "${DURATION_SECONDS}s per pod")"
    kubectl apply -f "$DEPLOYED_MANIFEST" >/dev/null
    log "Started. ./datagen/run.sh status  (stats every 10 s per pod)"
}

cmd_scale() {
    local n=${1:-}
    [[ "$n" =~ ^[0-9]+$ ]] || error "usage: ./datagen/run.sh scale N"
    kubectl patch job "$JOB" -n "$NAMESPACE" --type=merge -p "{\"spec\":{\"parallelism\":${n}}}"
    log "Scaled to ${n}. New pods write DURATION_SECONDS from their own start (0: until stopped)."
}

cmd_status() {
    kubectl get job "$JOB" -n "$NAMESPACE" 2>/dev/null || true
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
    # Ends a DURATION_SECONDS=0 run, or a timed one early. Pods get SIGTERM and
    # flush in-flight PUTs; start re-creates the Job and ConfigMap.
    kubectl delete job "$JOB" -n "$NAMESPACE" --ignore-not-found --wait=true
    kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found
    log "Stopped. Files already written under $OUTPUT_URI are kept."
}

case "${1:-}" in
    start)  cmd_start ;;
    status) cmd_status ;;
    logs)   cmd_logs ;;
    scale)  cmd_scale "${2:-}" ;;
    stop)   cmd_stop ;;
    render) check_duration; summary; render_job ;;
    *)      sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
