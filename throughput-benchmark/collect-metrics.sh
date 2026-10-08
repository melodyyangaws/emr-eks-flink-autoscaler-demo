#!/bin/bash
###############################################################################
# Throughput metric collector — samples the Flink REST API through a
# port-forward and appends CSV rows to bench-results/.
#
# Usage:   ./collect-metrics.sh paimon|iceberg [interval-seconds] [duration-seconds]
# Output:  bench-results/cid-<format>-<UTC timestamp>.csv
#
# Columns:
#   ts                         sample time (UTC, ISO8601)
#   job_state                  RUNNING/RESTARTING/...
#   source_rec_per_s           sum of numRecordsOutPerSecond over source subtasks
#                              — the headline number. Source rows are ~4 KiB, so
#                              GiB/s = rec_per_s * 4096 / 2^30. If this sits at
#                              the configured rate the format KEEPS UP; if it
#                              sits lower, backpressure from the writer is the
#                              ceiling and THIS is that format's write-path limit.
#   sink_rec_per_s             sum over writer subtasks
#   source_backpressure_ms     avg backPressuredTimeMsPerSecond on the source
#                              (1000 = fully blocked)
#   busy_ms                    avg busyTimeMsPerSecond on the writer
#   ckpt_completed/failed      checkpoint counters (commit health)
#   last_ckpt_duration_ms      end-to-end duration of the latest completed
#                              checkpoint — snapshot-commit cost lives here
#   last_ckpt_size_bytes       state size of the latest completed checkpoint
###############################################################################

set -euo pipefail

FORMAT=${1:?usage: $0 paimon|iceberg [interval] [duration]}
[[ "$FORMAT" == "paimon" || "$FORMAT" == "iceberg" ]] || { echo "Unknown format '$FORMAT'" >&2; exit 1; }
INTERVAL=${2:-30}
DURATION=${3:-7200}
NAMESPACE=${NAMESPACE:-emr-flink}
DEPLOYMENT="flink-cid-${FORMAT}"
PORT=${LOCAL_PORT:-8091}

OUTDIR="$(dirname "$0")/bench-results"
mkdir -p "$OUTDIR"
OUTFILE="${OUTDIR}/cid-${FORMAT}-$(date -u +%Y%m%dT%H%M%SZ).csv"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

kubectl port-forward "svc/${DEPLOYMENT}-rest" "${PORT}:8081" -n "$NAMESPACE" >/dev/null 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true' EXIT

BASE="http://localhost:${PORT}"
# Wait for the port-forward to actually serve; 3s flat was flaky.
for _ in $(seq 1 20); do
    curl -sf "${BASE}/jobs" >/dev/null 2>&1 && break
    sleep 2
done
curl -sf "${BASE}/jobs" >/dev/null || { echo "Flink REST not reachable through port-forward" >&2; exit 1; }

JOB_ID=$(curl -sf "${BASE}/jobs" | jq -r '.jobs[] | select(.status=="RUNNING") | .id' | head -1)
[[ -n "$JOB_ID" ]] || { echo "No RUNNING job found on ${DEPLOYMENT}" >&2; exit 1; }

# Vertex ids are stable for the life of the job — resolve once. The writer
# vertex name differs per format: Iceberg deploys "IcebergStreamWriter"/
# "IcebergFilesCommitter", Paimon deploys "Writer(write-only)"/"Writer" plus a
# "Global Committer". Match the writer, never the committer — committer
# records/sec counts committables (files), not rows.
VERTICES=$(curl -sf "${BASE}/jobs/${JOB_ID}")
SOURCE_VID=$(echo "$VERTICES" | jq -r '.vertices[] | select(.name | test("datagen_source|Source:")) | .id' | head -1)
SINK_VID=$(echo "$VERTICES"   | jq -r '.vertices[] | select(.name | test("IcebergStreamWriter|Writer")) | select(.name | test("Committer") | not) | .id' | head -1)
[[ -n "$SOURCE_VID" && -n "$SINK_VID" ]] || { echo "Could not resolve source/sink vertex ids; vertices are:" >&2; echo "$VERTICES" | jq -r '.vertices[].name' >&2; exit 1; }

echo "Sampling job ${JOB_ID} on ${DEPLOYMENT} every ${INTERVAL}s for ${DURATION}s -> ${OUTFILE}" >&2
echo "ts,job_state,source_rec_per_s,sink_rec_per_s,source_backpressure_ms,busy_ms,ckpt_completed,ckpt_failed,last_ckpt_duration_ms,last_ckpt_size_bytes" > "$OUTFILE"

vertex_metric_sum() {  # $1 vertex id, $2 metric name -> sum across subtasks
    curl -sf "${BASE}/jobs/${JOB_ID}/vertices/$1/subtasks/metrics?get=$2" \
        | jq -r '.[0].sum // 0'
}
vertex_metric_avg() {
    curl -sf "${BASE}/jobs/${JOB_ID}/vertices/$1/subtasks/metrics?get=$2" \
        | jq -r '.[0].avg // 0'
}

END=$((SECONDS + DURATION))
while (( SECONDS < END )); do
    TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    STATE=$(curl -sf "${BASE}/jobs/${JOB_ID}" | jq -r '.state' || echo "UNKNOWN")

    SRC_RATE=$(vertex_metric_sum "$SOURCE_VID" "numRecordsOutPerSecond")
    SINK_RATE=$(vertex_metric_sum "$SINK_VID" "numRecordsInPerSecond")
    SRC_BP=$(vertex_metric_avg "$SOURCE_VID" "backPressuredTimeMsPerSecond")
    SINK_BUSY=$(vertex_metric_avg "$SINK_VID" "busyTimeMsPerSecond")

    CKPT=$(curl -sf "${BASE}/jobs/${JOB_ID}/checkpoints")
    CKPT_OK=$(echo "$CKPT"   | jq -r '.counts.completed // 0')
    CKPT_FAIL=$(echo "$CKPT" | jq -r '.counts.failed // 0')
    CKPT_DUR=$(echo "$CKPT"  | jq -r '.latest.completed.end_to_end_duration // 0')
    CKPT_SIZE=$(echo "$CKPT" | jq -r '.latest.completed.state_size // 0')

    echo "${TS},${STATE},${SRC_RATE},${SINK_RATE},${SRC_BP},${SINK_BUSY},${CKPT_OK},${CKPT_FAIL},${CKPT_DUR},${CKPT_SIZE}" | tee -a "$OUTFILE"
    sleep "$INTERVAL"
done

echo "Done -> ${OUTFILE}" >&2
