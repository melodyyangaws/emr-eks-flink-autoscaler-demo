#!/usr/bin/env bash
set -uo pipefail

# Sample the Flink REST API for both CDC jobs over a fixed window and report a
# side-by-side Paimon vs Iceberg comparison.
#
# Why this exists alongside the in-cluster monitor deployment: the monitor reports
# on a rolling schedule from inside the cluster, which is right for watching a long
# run, but a benchmark needs a *bounded* window whose start and end are both known
# so the delta is attributable. This script takes two samples N seconds apart and
# prints the difference, so throughput is measured rather than inferred from
# counters that have been accumulating since the snapshot phase.
#
# Everything is read through `kubectl exec ... curl localhost:8081` rather than a
# port-forward: port-forwards drop under load, and a dropped forward mid-window
# silently truncates the sample.
#
# Usage:
#   ./capture-flink-metrics.sh                 # 120s window
#   WINDOW=300 ./capture-flink-metrics.sh
#   OUT_DIR=./results ./capture-flink-metrics.sh

NAMESPACE="${NAMESPACE:-emr-flink}"
WINDOW="${WINDOW:-120}"
OUT_DIR="${OUT_DIR:-./bench-results}"
mkdir -p "$OUT_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
CSV="${OUT_DIR}/flink-metrics-${STAMP}.csv"

# Resolve the JobManager pod per job. Names carry a random suffix, so match on the
# deployment prefix and the jobmanager component label rather than hardcoding.
jm_pod() {
    kubectl get pods -n "$NAMESPACE" \
        -l "app=$1,component=jobmanager" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

# curl inside the JM container; the REST port is not exposed outside the pod.
rest() {
    kubectl exec -n "$NAMESPACE" "$1" -c flink-main-container -- \
        curl -s --max-time 20 "localhost:8081$2" 2>/dev/null
}

job_id() {
    rest "$1" /jobs | python3 -c "
import sys, json
try:
    jobs = json.load(sys.stdin)['jobs']
except Exception:
    sys.exit(1)
# Prefer a RUNNING job: a failed//cancelled attempt can linger in the list and
# picking it would report zeros for a pipeline that is actually healthy.
for j in jobs:
    if j['status'] == 'RUNNING':
        print(j['id']); break
else:
    print(jobs[0]['id'] if jobs else '')
" 2>/dev/null
}

# One sample: records/bytes written plus checkpoint counters, as a flat KEY=VAL
# list so the caller can eval it without a JSON dependency in bash.
sample() {
    local pod=$1 jid=$2
    local vert ckpt
    vert=$(rest "$pod" "/jobs/${jid}")
    ckpt=$(rest "$pod" "/jobs/${jid}/checkpoints")
    python3 -c "
import sys, json
vert = json.loads(sys.argv[1]) if sys.argv[1].strip() else {}
ck   = json.loads(sys.argv[2]) if sys.argv[2].strip() else {}

recs  = sum(v['metrics'].get('write-records', 0) for v in vert.get('vertices', []))
bytes_ = sum(v['metrics'].get('write-bytes', 0)  for v in vert.get('vertices', []))
counts = ck.get('counts', {})
latest = (ck.get('latest') or {}).get('completed') or {}

print('RECS=%d'      % recs)
print('BYTES=%d'     % bytes_)
print('CP_DONE=%d'   % counts.get('completed', 0))
print('CP_FAIL=%d'   % counts.get('failed', 0))
print('CP_DUR=%d'    % (latest.get('end_to_end_duration') or 0))
print('CP_SIZE=%d'   % (latest.get('state_size') or 0))
print('UPTIME=%d'    % vert.get('timestamps', {}).get('RUNNING', 0))
print('STATUS=%s'    % vert.get('state', 'UNKNOWN'))
" "$vert" "$ckpt" 2>/dev/null
}

PAIMON_POD=$(jm_pod flink-cdc-paimon)
ICEBERG_POD=$(jm_pod flink-cdc-iceberg)
[[ -z "$PAIMON_POD"  ]] && { echo "ERROR: no paimon JobManager pod found"  >&2; exit 1; }
[[ -z "$ICEBERG_POD" ]] && { echo "ERROR: no iceberg JobManager pod found" >&2; exit 1; }

PAIMON_JID=$(job_id "$PAIMON_POD")
ICEBERG_JID=$(job_id "$ICEBERG_POD")

echo "═══════════════════════════════════════════════════════════════════"
echo "  Flink CDC metrics — Paimon vs Iceberg"
echo "═══════════════════════════════════════════════════════════════════"
echo "  Window   : ${WINDOW}s"
echo "  Paimon   : ${PAIMON_POD} job ${PAIMON_JID}"
echo "  Iceberg  : ${ICEBERG_POD} job ${ICEBERG_JID}"
echo "  Results  : ${CSV}"
echo "═══════════════════════════════════════════════════════════════════"

eval "$(sample "$PAIMON_POD"  "$PAIMON_JID"  | sed 's/^/P_/')"
eval "$(sample "$ICEBERG_POD" "$ICEBERG_JID" | sed 's/^/I_/')"
T0=$(date +%s)

echo "  sampling... (${WINDOW}s)"
sleep "$WINDOW"

eval "$(sample "$PAIMON_POD"  "$PAIMON_JID"  | sed 's/^/P2_/')"
eval "$(sample "$ICEBERG_POD" "$ICEBERG_JID" | sed 's/^/I2_/')"
T1=$(date +%s)
EL=$(( T1 - T0 ))

# Report deltas over the window, not lifetime totals: lifetime numbers are
# dominated by the snapshot phase and hide the steady-state CDC behaviour.
report() {
    printf '%-26s %16s %16s %12s\n' "$1" "$2" "$3" "$4"
}

printf '\n%-26s %16s %16s %12s\n' METRIC PAIMON ICEBERG "ICE vs PAI"
printf '%s\n' "────────────────────────────────────────────────────────────────────────────"

P_DR=$(( P2_RECS  - P_RECS  )); I_DR=$(( I2_RECS  - I_RECS  ))
P_DB=$(( P2_BYTES - P_BYTES )); I_DB=$(( I2_BYTES - I_BYTES ))
P_DC=$(( P2_CP_DONE - P_CP_DONE )); I_DC=$(( I2_CP_DONE - I_CP_DONE ))
P_DF=$(( P2_CP_FAIL - P_CP_FAIL )); I_DF=$(( I2_CP_FAIL - I_CP_FAIL ))

pct() { awk -v a="$1" -v b="$2" 'BEGIN{ if (b>0) printf "%+.1f%%", 100*(a-b)/b; else print "n/a" }'; }
num() { printf "%'d" "$1" 2>/dev/null || echo "$1"; }

report "Job status"            "$P2_STATUS"        "$I2_STATUS"        "-"
report "Records written"       "$(num $P_DR)"      "$(num $I_DR)"      "$(pct $I_DR $P_DR)"
report "Throughput (rec/s)"    "$(awk -v d=$P_DR -v e=$EL 'BEGIN{printf "%.1f", d/e}')" \
                               "$(awk -v d=$I_DR -v e=$EL 'BEGIN{printf "%.1f", d/e}')" \
                               "$(pct $I_DR $P_DR)"
report "Bytes written (MB)"    "$(awk -v b=$P_DB 'BEGIN{printf "%.2f", b/1048576}')" \
                               "$(awk -v b=$I_DB 'BEGIN{printf "%.2f", b/1048576}')" \
                               "$(pct $I_DB $P_DB)"
report "Checkpoints completed" "$P_DC"             "$I_DC"             "-"
report "Checkpoints failed"    "$P_DF"             "$I_DF"             "-"
report "Last ckpt duration ms" "$(num $P2_CP_DUR)" "$(num $I2_CP_DUR)" "$(pct $I2_CP_DUR $P2_CP_DUR)"
report "Last ckpt size (MB)"   "$(awk -v b=$P2_CP_SIZE 'BEGIN{printf "%.2f", b/1048576}')" \
                               "$(awk -v b=$I2_CP_SIZE 'BEGIN{printf "%.2f", b/1048576}')" \
                               "$(pct $I2_CP_SIZE $P2_CP_SIZE)"
report "Lifetime records"      "$(num $P2_RECS)"   "$(num $I2_RECS)"   "$(pct $I2_RECS $P2_RECS)"
report "Lifetime ckpt failed"  "$P2_CP_FAIL"       "$I2_CP_FAIL"       "-"

{
  echo "stamp,window_s,metric,paimon,iceberg"
  echo "${STAMP},${EL},records_written,${P_DR},${I_DR}"
  echo "${STAMP},${EL},bytes_written,${P_DB},${I_DB}"
  echo "${STAMP},${EL},checkpoints_completed,${P_DC},${I_DC}"
  echo "${STAMP},${EL},checkpoints_failed,${P_DF},${I_DF}"
  echo "${STAMP},${EL},last_ckpt_duration_ms,${P2_CP_DUR},${I2_CP_DUR}"
  echo "${STAMP},${EL},last_ckpt_size_bytes,${P2_CP_SIZE},${I2_CP_SIZE}"
  echo "${STAMP},${EL},lifetime_records,${P2_RECS},${I2_RECS}"
  echo "${STAMP},${EL},lifetime_ckpt_failed,${P2_CP_FAIL},${I2_CP_FAIL}"
} > "$CSV"

printf '\n%s\n' "═══════════════════════════════════════════════════════════════════"
echo "  ✓ CSV: ${CSV}"
echo "═══════════════════════════════════════════════════════════════════"
