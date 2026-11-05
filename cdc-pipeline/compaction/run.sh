#!/usr/bin/env bash
# One-off full compaction of the CDC tables, run once before the snapshot query
# test, with the writers stopped (throughput-benchmark/compaction/'s pattern).
#
#   ./compaction/run.sh run [paimon|iceberg|both]   (default both) compact every table, wait, record
#   ./compaction/run.sh render [paimon|iceberg]     render the manifests only (no cluster / AWS call)
#   ./compaction/run.sh status                      the compaction FlinkDeployments
#   ./compaction/run.sh logs <fmt> <table>          follow one compaction JobManager
#   ./compaction/run.sh down                        delete every compaction FlinkDeployment
#
# The hot test's watcher (querybench/qb-watch.sh) runs `run both` once, right
# after its teardown stopped the generator and the writers (COMPACT_AFTER_HOT=0
# skips it); the snapshot start waits for that watcher while a cdc-compact-*
# job runs. Paimon also compacts inline during ingest; Iceberg does not.
#
# One batch FlinkDeployment per format and table, cdc-compact-<fmt>-<table>
# (cdc-compact.py), on the writers' image; the two formats side by side, their
# tables one after another. Paimon: CALL sys.compact, full. Iceberg: every data
# file with deletes (or small) rewritten with its deletes applied, the delete
# files removed in the same commit, then snapshots expired (12 h, keep 5).
# Each run writes a JSON to s3://$BUCKET_NAME/<QB_RESULTS_PREFIX>/compaction/
# <STAMP>-<fmt>-<table>.json; a failed run commits nothing and its
# FlinkDeployment is left for its logs (`down` deletes it).
#
# Knobs: COMPACT_TABLES (all four), COMPACT_PARALLELISM (16), COMPACT_TIMEOUT_S
# (per table, 14400), ICEBERG_TM_SLOTS (1: a rewrite subtask holds a file's
# equality-delete set) / PAIMON_TM_SLOTS (4), TM_CPU / TM_MEMORY (8 / 32Gi),
# ICEBERG_BIG_TM_TABLES (orders order_items) / ICEBERG_BIG_TM_MEMORY (96Gi):
# Iceberg tables whose TaskManagers get the bigger memory instead,
# JM_NODEPOOL, TM_NODEPOOL.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${HERE}/../lib/common.sh"
cd "$HERE"

: "${BUCKET_NAME:?not set in .bench-env(.local)}" "${EMR_EXECUTION_ROLE_ARN:?not set in .bench-env(.local)}"
export EMR_VERSION=${EMR_VERSION:-7.14.0}
export IMAGE_TAG=${IMAGE_TAG:-$EMR_VERSION}
export JM_NODEPOOL=${JM_NODEPOOL:-driver-nodepool}
export TM_NODEPOOL=${TM_NODEPOOL:-executor-memorynodepool}
export TM_CPU=${TM_CPU:-8} TM_MEMORY=${TM_MEMORY:-32Gi}
# The fact tables' rewrite subtasks OOMed at 32Gi loading a data file's
# equality deletes (Iceberg orders, 208M equality deletes, _readonlytest
# 2026-11-05; OK at 64Gi in 427 s) and order_items OOMed at 64Gi too (246M;
# OK at 96Gi in 522 s). 96Gi still fits one 128 GiB node per TaskManager.
ICEBERG_BIG_TM_TABLES=${ICEBERG_BIG_TM_TABLES:-orders order_items}
ICEBERG_BIG_TM_MEMORY=${ICEBERG_BIG_TM_MEMORY:-96Gi}
export COMPACT_PARALLELISM=${COMPACT_PARALLELISM:-16}
export S3A_MULTIPART_SIZE=${S3A_MULTIPART_SIZE:-16M} S3A_MULTIPART_THRESHOLD=${S3A_MULTIPART_THRESHOLD:-16M}
export S3A_ACTIVE_BLOCKS=${S3A_ACTIVE_BLOCKS:-6}
export ICEBERG_S3_MULTIPART_SIZE_BYTES=${ICEBERG_S3_MULTIPART_SIZE_BYTES:-16777216}
export ICEBERG_S3_MULTIPART_THREADS=${ICEBERG_S3_MULTIPART_THREADS:-24}
COMPACT_TABLES=${COMPACT_TABLES:-customers products orders order_items}
COMPACT_TIMEOUT_S=${COMPACT_TIMEOUT_S:-14400}
RESULTS="s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}/compaction"
SCRIPT_S3="s3://${BUCKET_NAME}/flink/scripts/cdc-compact.py"

formats() {
    case "${1:-both}" in
        paimon|iceberg) echo "$1" ;;
        both)           echo "paimon iceberg" ;;
        *)              die "format must be paimon, iceberg or both, got '$1'" ;;
    esac
}
name_of() { echo "cdc-compact-$1-${2//_/-}"; }

# A running writer would commit under the compaction (Iceberg's commit would
# fail its validation; Paimon's would compact a moving table).
check_writers_stopped() {
    local j
    for j in $FLINK_JOBS; do
        [[ "$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)" == RUNNING ]] \
            && die "${j} is RUNNING: stop the writers first (the snapshot test does it)"
        kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | grep -q "^${j}-taskmanager-" \
            && die "${j} still has TaskManagers: wait until they are gone"
    done
    return 0
}

upload() {   # the job script; the catalog SQL is the writers' own copy on S3 (build uploaded it)
    local f size
    aws s3 cp --only-show-errors "${HERE}/cdc-compact.py" "$SCRIPT_S3" || die "could not upload cdc-compact.py"
    for f in "$SCRIPT_S3" "s3://${BUCKET_NAME}/flink/sql-scripts/paimon/01-catalog-setup.sql" \
             "s3://${BUCKET_NAME}/flink/sql-scripts/iceberg/01-catalog-setup.sql"; do
        size=$(aws s3 ls "$f" | awk '{print $3}')
        [[ "${size:-0}" -gt 0 ]] || die "not in S3 or empty: ${f} (run ./ingest-deploy/build-deploy-generic.sh build)"
    done
}

render_one() {   # <fmt> <table> <result-uri> -> path of the rendered manifest
    local out="${HERE}/$(name_of "$1" "$2")-deployed.yaml"
    export FMT="$1" TABLE="$2" RESULT_URI="$3" NAME; NAME=$(name_of "$1" "$2")
    export GLUE_DATABASE; [[ "$1" == paimon ]] && GLUE_DATABASE="$PAIMON_DB" || GLUE_DATABASE="$ICEBERG_DB"
    export TM_SLOTS; [[ "$1" == paimon ]] && TM_SLOTS=${PAIMON_TM_SLOTS:-4} || TM_SLOTS=${ICEBERG_TM_SLOTS:-1}
    local tm_memory="$TM_MEMORY"
    [[ "$1" == iceberg && " ${ICEBERG_BIG_TM_TABLES} " == *" $2 "* ]] && tm_memory="$ICEBERG_BIG_TM_MEMORY"
    TM_MEMORY="$tm_memory" render "${HERE}/cdc-compact.yaml" "$out" \
        NAMESPACE NAME FMT TABLE AWS_REGION AWS_ACCOUNT_ID BUCKET_NAME EMR_EXECUTION_ROLE_ARN EMR_VERSION IMAGE_TAG \
        COMPACT_PARALLELISM TM_SLOTS TM_CPU TM_MEMORY JM_NODEPOOL TM_NODEPOOL GLUE_DATABASE ICEBERG_FORMAT_VERSION \
        SESSION_SUFFIX S3A_MULTIPART_SIZE S3A_MULTIPART_THRESHOLD S3A_ACTIVE_BLOCKS \
        ICEBERG_S3_MULTIPART_SIZE_BYTES ICEBERG_S3_MULTIPART_THREADS RESULT_URI \
        || die "could not render ${out##*/}"
    echo "$out"
}

# Until the run's result JSON is in S3 (written last, also on failure), the job
# FAILED / CANCELED, or COMPACT_TIMEOUT_S: prints RESULT, FAILED, CANCELED or TIMEOUT.
wait_job() {   # <name> <result-uri>
    local st="" last="" deadline=$(( $(date +%s) + COMPACT_TIMEOUT_S ))
    while (( $(date +%s) < deadline )); do
        aws s3 ls "$2" >/dev/null 2>&1 && { echo RESULT; return; }
        st=$(kubectl get flinkdeployment "$1" -n "$NAMESPACE" -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)
        case "$st" in FAILED|CANCELED) echo "$st"; return ;; esac
        [[ "$st" != "$last" ]] && { log "$1: ${st:-starting}"; last="$st"; }
        sleep 30
    done
    echo TIMEOUT
}

report() {   # <json>
    python3 - "$1" <<'EOF'
import json, sys
r = json.loads(sys.argv[1]); b, a = r.get("before", {}), r.get("after", {})
f = lambda d: "{:,} data files / {:.2f} GiB / {:,} rows in files, {:,} delete files ({:,} eq, {:,} pos)".format(
    d.get("data_files", 0), d.get("bytes", 0) / 2**30, d.get("records_in_files", 0), d.get("delete_files", 0),
    d.get("equality_deletes", 0), d.get("position_deletes", 0))
print("  {} {}: {} in {} s; before {}; after {}{}".format(r["format"], r["table"], r["status"], r.get("duration_s"),
      f(b), f(a), ("; error: " + r["error"][:300]) if r.get("error") else ""))
EOF
}

compact_format() {   # <fmt> <stamp>: its tables one after another
    local fmt="$1" stamp="$2" t name uri out st json
    for t in $COMPACT_TABLES; do
        name=$(name_of "$fmt" "$t"); uri="${RESULTS}/${stamp}-${fmt}-${t}.json"
        out=$(render_one "$fmt" "$t" "$uri")
        kubectl delete flinkdeployment "$name" -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=300s >/dev/null
        kubectl create -f "$out" >/dev/null || { warn "could not create ${name}"; return 1; }
        log "${name}: started (-> ${uri#s3://${BUCKET_NAME}/})"
        st=$(wait_job "$name" "$uri")
        json=$(aws s3 cp --only-show-errors "$uri" - 2>/dev/null || true)
        [[ -n "$json" ]] && report "$json" >&2
        if [[ "$json" != *'"status": "OK"'* ]]; then
            warn "${name}: ${st}; left for its logs (./compaction/run.sh logs ${fmt} ${t}; down deletes it)"
            return 1
        fi
        kubectl delete flinkdeployment "$name" -n "$NAMESPACE" --ignore-not-found --wait=false >/dev/null
    done
}

cmd_run() {
    local fmts stamp f rc=0 pids=()
    fmts=$(formats "${1:-both}")
    check_writers_stopped
    stamp=$(date -u +%Y%m%dT%H%M%SZ)
    log "compaction of ${PAIMON_DB} / ${ICEBERG_DB} (${fmts// /, }; tables: ${COMPACT_TABLES})"
    upload
    for f in $fmts; do compact_format "$f" "$stamp" & pids+=($!); done
    for f in "${pids[@]}"; do wait "$f" || rc=1; done
    (( rc == 0 )) || die "compaction failed (above): each table is as its last committed run left it"
    log "compaction done; results under ${RESULTS#s3://${BUCKET_NAME}/}/${stamp}-*"
}

case "${1:-}" in
    run)    cmd_run "${2:-both}" ;;
    render) for f in $(formats "${2:-both}"); do for t in $COMPACT_TABLES; do
                o=$(render_one "$f" "$t" "${RESULTS}/render-${f}-${t}.json"); log "rendered ${o##*/}"; done; done ;;
    status) kubectl get flinkdeployment -n "$NAMESPACE" --no-headers \
                -o custom-columns='NAME:.metadata.name,JOB:.status.jobStatus.state' 2>/dev/null | grep '^cdc-compact-' \
                || echo "no compaction FlinkDeployment" ;;
    logs)   [[ "${2:-}" =~ ^(paimon|iceberg)$ && -n "${3:-}" ]] || die "usage: $0 logs <paimon|iceberg> <table>"
            kubectl logs -f -n "$NAMESPACE" "deploy/$(name_of "$2" "$3")" -c flink-main-container ;;
    down)   kubectl get flinkdeployment -n "$NAMESPACE" -o name 2>/dev/null | grep '/cdc-compact-' \
                | xargs -r kubectl delete -n "$NAMESPACE" ;;
    *)      sed -n '2,27p' "${HERE}/run.sh" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
