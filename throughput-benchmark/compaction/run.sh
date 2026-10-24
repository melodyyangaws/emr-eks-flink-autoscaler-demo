#!/usr/bin/env bash
# One-off bin-pack compaction of the session's two tables, run once before the
# snapshot query test, with the writers stopped.
#
#   ./compaction/run.sh run [paimon|iceberg|both]   (default both) compact, wait, record, clean up
#   ./compaction/run.sh status                      the compaction FlinkDeployments
#   ./compaction/run.sh logs <paimon|iceberg>       follow a compaction JobManager
#   ./compaction/run.sh cleanup                     delete the compaction FlinkDeployments
#
# The snapshot test runs it itself when .bench-env sets COMPACT_BEFORE_SNAPSHOT=true
# (querybench/run.sh: after the writers stop and the windows are resolved,
# before the caches are emptied). By hand, run it after the writers have stopped.
#
# Both formats get the same rule (see flink-cid-compact.py): files under 70% of
# the 256 MiB target are bin-packed; the rest stay. Each format is one batch
# FlinkDeployment, flink-cid-compact-<fmt>, on the writers' image and nodepools,
# sized per pass from its data, up to COMPACT_PARALLELISM (320): both side by side, as the
# writers ran. Iceberg runs in passes of ICEBERG_MAX_GROUPS groups (default
# 4000) until none is left; Paimon in passes until its small files are under
# PAIMON_SMALL_PCT (1%) of all files, or a pass cuts them by under 10%.
#
# Each pass writes a JSON (before/after files and bytes, what was rewritten,
# timings) to s3://$BUCKET_NAME/querybench-results/session<TEST_SUFFIX>/compaction/,
# where summary/run.sh reads it. A pass that fails commits nothing; its
# FlinkDeployment is left for its logs (`cleanup` deletes it).
#
# DRY_RUN=true measures both tables and plans the groups (JobManagers only, a
# few minutes), rewrites and commits nothing; its JSONs go to compaction/dry-run/.
#
# Knobs: COMPACT_PARALLELISM (cap), COMPACT_TARGET_S (300), ICEBERG_MAX_GROUPS, PAIMON_SMALL_PCT (1), COMPACT_MAX_PASSES (20),
# COMPACT_TIMEOUT_S (per pass, 14400), COMPACT_RETAIN (Paimon's
# snapshot.time-retained for the compaction commit, 7d), JM_NODEPOOL, TM_NODEPOOL.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${HERE}/../lib/common.sh"
cd "$HERE"

: "${BUCKET_NAME:?not set in .bench-env(.local)}" "${EMR_EXECUTION_ROLE_ARN:?not set in .bench-env(.local)}"
export EMR_VERSION=${EMR_VERSION:-7.13.0}
export IMAGE_TAG=${IMAGE_TAG:-$EMR_VERSION}
export JM_NODEPOOL=${JM_NODEPOOL:-driver-nodepool}
export TM_NODEPOOL=${TM_NODEPOOL:-executor-memorynodepool}
# The CAP per pass: each pass sizes itself from the bytes it rewrites
# (flink-cid-compact.py sized_parallelism: ~COMPACT_TARGET_S of rewrite at
# ~10.5 MB/s per subtask, at most one subtask per ~512 MiB bin). Only a big
# first pass reaches it: 3.2 TB at 320 is ~15 min (80 TMs per format); the
# 5,215-file pass of 2026-10-24 sizes to 62, a final mop-up to 1-2.
export COMPACT_PARALLELISM=${COMPACT_PARALLELISM:-320}
export COMPACT_TARGET_S=${COMPACT_TARGET_S:-300}
export ICEBERG_MAX_GROUPS=${ICEBERG_MAX_GROUPS:-4000}
export S3A_MULTIPART_SIZE=${S3A_MULTIPART_SIZE:-16M}
export S3A_MULTIPART_THRESHOLD=${S3A_MULTIPART_THRESHOLD:-16M}
COMPACT_MAX_PASSES=${COMPACT_MAX_PASSES:-20}
COMPACT_TIMEOUT_S=${COMPACT_TIMEOUT_S:-14400}
export COMPACT_RETAIN=${COMPACT_RETAIN:-7d}
export DRY_RUN=${DRY_RUN:-false}
[[ "$DRY_RUN" =~ ^(true|false)$ ]] || die "DRY_RUN must be true or false, got '${DRY_RUN}'"
RESULTS="s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}/compaction"
# A dry run's JSONs go one level down, where the summary does not look.
[[ "$DRY_RUN" == true ]] && RESULTS+="/dry-run"
WRITERS="flink-cid-paimon flink-cid-iceberg"

formats() {   # paimon | iceberg | both -> the list
    case "${1:-both}" in
        paimon|iceberg) echo "$1" ;;
        both)           echo "paimon iceberg" ;;
        *)              die "format must be paimon, iceberg or both, got '$1'" ;;
    esac
}

# A running writer would commit under the compaction: Iceberg's commit would
# fail its validation, and Paimon's would compact a moving table.
check_writers_stopped() {
    local j
    for j in $WRITERS; do
        [[ "$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)" == RUNNING ]] \
            && die "${j} is RUNNING: stop the writers first (./bench.sh cleanup, or the snapshot test does it)"
        kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | grep -q "^${j}-taskmanager-" \
            && die "${j} still has TaskManagers: wait until they are gone"
    done
    return 0
}

upload() {   # the job script and the writers' catalog SQL, checked non-empty
    local fmt uri size
    for uri in "s3://${BUCKET_NAME}/flink/scripts/flink-cid-compact.py=${HERE}/flink-cid-compact.py" \
               $(for fmt in $(formats both); do
                     echo "s3://${BUCKET_NAME}/flink/cid-compact/${fmt}/02-catalog-setup.sql=${BENCH_DIR}/flinkwriter/sql-scripts/${fmt}/02-catalog-setup.sql"
                 done); do
        aws s3 cp --only-show-errors "${uri#*=}" "${uri%%=*}" --region "$AWS_REGION" || die "could not upload ${uri#*=}"
        # `s3 ls` prints nothing for a missing key, so an empty size is "absent".
        size=$(aws s3 ls "${uri%%=*}" --region "$AWS_REGION" | awk '{print $3}')
        [[ "${size:-0}" -gt 0 ]] || die "not in S3 or empty: ${uri%%=*}"
        echo "  ✓ ${size} B  ${uri%%=*}"
    done
}

deploy() {   # <fmt> <result-uri>: a fresh CR (EMR stages the script on CREATE only)
    local fmt="$1" out="${HERE}/flink-cid-compact-${1}-deployed.yaml"
    FMT="$fmt" RESULT_URI="$2" render "${HERE}/flink-cid-compact.yaml" "$out" \
        NAMESPACE FMT AWS_REGION AWS_ACCOUNT_ID BUCKET_NAME EMR_EXECUTION_ROLE_ARN EMR_VERSION IMAGE_TAG \
        COMPACT_PARALLELISM ICEBERG_MAX_GROUPS JM_NODEPOOL TM_NODEPOOL TEST_SUFFIX TEST_DB_SUFFIX \
        S3A_MULTIPART_SIZE S3A_MULTIPART_THRESHOLD COMPACT_RETAIN DRY_RUN COMPACT_TARGET_S RESULT_URI || die "could not render ${out##*/}"
    kubectl delete flinkdeployment "flink-cid-compact-${fmt}" -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=300s >/dev/null
    kubectl create -f "$out" >/dev/null || die "could not create flink-cid-compact-${fmt}"
}

# Until the pass's result JSON is in S3 (the job script writes it last, also on
# failure), the job FAILED or CANCELED, or COMPACT_TIMEOUT_S: prints RESULT,
# FAILED, CANCELED or TIMEOUT. Not FINISHED: an application that never submits
# a job (a dry run, or a failure before it) stays RECONCILING in the operator.
wait_job() {   # <fmt> <result-uri>
    local name="flink-cid-compact-$1" st="" last="" deadline=$(( $(date +%s) + COMPACT_TIMEOUT_S ))
    while (( $(date +%s) < deadline )); do
        aws s3 ls --region "$AWS_REGION" "$2" >/dev/null 2>&1 && { echo RESULT; return; }
        st=$(kubectl get flinkdeployment "$name" -n "$NAMESPACE" -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)
        case "$st" in FAILED|CANCELED) echo "$st"; return ;; esac
        [[ "$st" != "$last" ]] && { log "${name}: ${st:-starting}" ; last="$st"; }
        sleep 30
    done
    echo TIMEOUT
}

result() { aws s3 cp --only-show-errors --region "$AWS_REGION" "$1" - 2>/dev/null; }

# One line per pass, from its result JSON.
report() {
    python3 - "$1" <<'EOF'
import json, sys
r = json.loads(sys.argv[1])
def g(d):
    return "{:,} files / {:.2f} TB ({:,} under {} MiB)".format(
        d.get("files", 0), d.get("bytes", 0) / 1e12, d.get("small_files", 0), r.get("threshold_bytes", 0) >> 20)
left = r.get("remaining_groups") or 0
print("  {} pass: {} in {} s; before {}; after {}{}".format(
    r["format"], r["status"], r.get("duration_s"), g(r.get("before", {})), g(r.get("after", {})),
    "; {:,} groups left".format(left) if left else ""))
EOF
}

# Whether a format needs another pass, from its pass's JSON: prints why (and
# exits 0), or nothing (exit 1) when it is done.
#   Iceberg: groups left over (ICEBERG_MAX_GROUPS per pass).
#   Paimon: one sys.compact leaves about one new small file per bin (a bin of
#   ~5 tails is written as 256 MiB files plus a remainder; 28,799 -> 5,215 on
#   2026-10-23), so it runs again while its small files are above
#   PAIMON_SMALL_PCT of all files and the last pass cut them by >= 10%.
more_passes() {   # <json>
    PAIMON_SMALL_PCT="${PAIMON_SMALL_PCT:-1}" python3 - "$1" <<'EOF'
import json, os, sys
r = json.loads(sys.argv[1]); b, a = r.get("before", {}), r.get("after", {})
if r.get("format") == "iceberg":
    left = r.get("remaining_groups") or 0
    sys.exit(print(f"{left:,} groups left") if left else 1)
small, files, was = a.get("small_files", 0), a.get("files", 0) or 1, b.get("small_files", 0)
pct = float(os.environ["PAIMON_SMALL_PCT"])
if small <= files * pct / 100:
    sys.exit(1)                                  # few enough small files
if small > was * 0.9:
    print(f"{small:,} small files left, but the last pass cut them by under 10%: stopping", file=sys.stderr)
    sys.exit(1)
print(f"{small:,} small files left, {100 * small / files:.1f}% of {files:,}, above {pct:g}%")
EOF
}

# One format, pass after pass until nothing is left (more_passes).
compact_one() {   # <fmt> <stamp>
    local fmt="$1" stamp="$2" pass st uri json left
    for (( pass = 1; pass <= COMPACT_MAX_PASSES; pass++ )); do
        uri="${RESULTS}/${stamp}-${fmt}-pass${pass}.json"
        log "${fmt}: pass ${pass} (parallelism sized from its data, cap ${COMPACT_PARALLELISM}) -> ${uri#s3://${BUCKET_NAME}/}"
        deploy "$fmt" "$uri"
        st=$(wait_job "$fmt" "$uri")
        json=$(result "$uri" || true)
        [[ -n "$json" ]] && report "$json" >&2
        if [[ "$json" == *'"status": "DRY_RUN"'* ]]; then
            kubectl delete flinkdeployment "flink-cid-compact-${fmt}" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null
            log "${fmt}: dry run done (nothing rewritten)"
            return 0
        fi
        if [[ "$json" != *'"status": "OK"'* ]]; then
            warn "${fmt}: pass ${pass} failed (${st}); flink-cid-compact-${fmt} is left for its logs (./compaction/run.sh logs ${fmt}; cleanup deletes it)"
            return 1
        fi
        kubectl delete flinkdeployment "flink-cid-compact-${fmt}" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null
        left=$(more_passes "$json") || left=""
        [[ -z "$left" ]] && { log "${fmt}: done"; return 0; }
        log "${fmt}: another pass (${left})"
    done
    warn "${fmt}: still not done after ${COMPACT_MAX_PASSES} passes (raise COMPACT_MAX_PASSES; Iceberg: ICEBERG_MAX_GROUPS)"
    return 1
}

cmd_run() {
    local fmts stamp f rc=0 pids=()
    fmts=$(formats "${1:-both}")
    check_writers_stopped
    stamp=$(date -u +%Y%m%dT%H%M%SZ)
    log "compaction of TEST_SUFFIX='${TEST_SUFFIX}' (${fmts// /, }): files under 70% of the target, bin-packed"
    upload
    # Side by side, as the writers ran; each in its own subshell.
    for f in $fmts; do compact_one "$f" "$stamp" & pids+=($!); done
    for f in "${pids[@]}"; do wait "$f" || rc=1; done
    (( rc == 0 )) || die "compaction failed (above); the tables are as the last committed pass left them"
    log "compaction done; results under ${RESULTS#s3://${BUCKET_NAME}/}/${stamp}-*"
}

cmd_status() {
    kubectl get flinkdeployment -n "$NAMESPACE" --no-headers \
        -o custom-columns='NAME:.metadata.name,JOB:.status.jobStatus.state' 2>/dev/null | grep '^flink-cid-compact-' \
        || echo "no compaction FlinkDeployment"
}

case "${1:-}" in
    run)     cmd_run "${2:-both}" ;;
    status)  cmd_status ;;
    logs)    [[ "${2:-}" =~ ^(paimon|iceberg)$ ]] || die "usage: $0 logs <paimon|iceberg>"
             kubectl logs -f -n "$NAMESPACE" "deploy/flink-cid-compact-$2" -c flink-main-container ;;
    cleanup) for f in paimon iceberg; do
                 kubectl delete flinkdeployment "flink-cid-compact-${f}" -n "$NAMESPACE" --ignore-not-found
             done ;;
    *)       sed -n '2,31p' "${HERE}/run.sh" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
