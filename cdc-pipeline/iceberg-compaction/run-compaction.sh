#!/usr/bin/env bash
# Run the Iceberg compaction as a Kubernetes Job and collect its output.
#
# Usage:
#   ./run-compaction.sh                     # compact flink_iceberg_db<CDC_DB_SUFFIX>.order_items (.bench-env's set)
#   DRY_RUN=true ./run-compaction.sh        # report table state only, commit nothing
#   EXPIRE_SNAPSHOTS=false ./run-compaction.sh   # rewrite only; by default it then expires
#                                          # snapshots older than 12 h, keeping 5 (Paimon's rule)
#   ICEBERG_TABLE=orders ./run-compaction.sh
#   CDC_DB_SUFFIX= ./run-compaction.sh      # the unsuffixed v2 set: flink_iceberg_db
#   ./run-compaction.sh render              # print the Job manifest it would create; no cluster call
#   ./run-compaction.sh clean               # delete job + configmap
#
# Settings come from cdc-pipeline/.bench-env (+ .bench-env.local) through
# lib/common.sh, the same as querybench/run.sh, which submits this script once
# per COMPACT_TABLES table:
#   ICEBERG_DB         default flink_iceberg_db<CDC_DB_SUFFIX>, the live set from
#                      .bench-env (CDC_DB_SUFFIX=_v3r1 there now); an explicit
#                      CDC_DB_SUFFIX= -> the v2 set (flink_iceberg_db). An ICEBERG_DB in the
#                      environment (run.sh passes one) wins over the derived name.
#   ICEBERG_WAREHOUSE  s3://<BUCKET_NAME>/iceberg-warehouse (.bench-env)
#   AWS_REGION         .bench-env.local (default us-west-2); also AWS_DEFAULT_REGION
#   SPARK_IMAGE        default the EMR 7.14.0 Spark image in us-west-2's EMR
#                      registry (895885662937). The EMR registry account differs
#                      per region, so outside us-west-2 SPARK_IMAGE must be set
#                      (EMR on EKS docs: "How to select a base image URI").
# The rendered values replace the ${...} placeholders in compact-job.yaml; the
# script refuses to submit if one is left or the warehouse is not an s3:// URI.
#
# Start with DRY_RUN=true. It validates the image classpath, the Glue catalog
# wiring and the IRSA credentials in about a minute, whereas a real rewrite over
# 365 M equality deletes takes far longer — and a credential typo discovered at
# minute 50 is an expensive way to learn it. On the v3 set it also shows whether
# the image's Iceberg 1.10.1 reads the metadata the 1.12.0 Flink writer wrote
# (it reads the data_files / delete_files metadata tables); it does NOT exercise
# the rewrite itself. See compact-job.yaml for the fallback if 1.10.1 refuses.
#
# The Python script ships via ConfigMap rather than a baked image, matching how
# flink-cdc-executor.py and the monitor code are delivered in this repo: no ECR
# push for a single file, and editing the .py is enough to change the next run.
#
# lib/common.sh is sourced before `set -u`: .bench-env expands BUCKET_NAME /
# AWS_ACCOUNT_ID from .bench-env.local, and a missing .bench-env.local is then
# reported below as a bad ICEBERG_WAREHOUSE rather than as an unbound variable.
set -eo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_db_given="${ICEBERG_DB+set}" _db_value="${ICEBERG_DB-}"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"
[[ -n "$_db_given" ]] && export ICEBERG_DB="$_db_value"
unset _db_given _db_value
set -u
NAMESPACE="${NAMESPACE:-emr-flink}"
export JOB="${JOB:-iceberg-compact-order-items}"
CM="${CM:-iceberg-compact-py}"
OUT_DIR="${OUT_DIR:-${SCRIPT_DIR}/../bench-results}"
export MANIFEST="${SCRIPT_DIR}/compact-job.yaml"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
export ICEBERG_TABLE="${ICEBERG_TABLE:-order_items}"
TBL="$ICEBERG_TABLE"
BASE="iceberg-compact-${TBL}-${STAMP}"
export ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE-}"
export AWS_REGION
if [[ -z "${SPARK_IMAGE:-}" ]]; then
    [[ "$AWS_REGION" == us-west-2 ]] || die "SPARK_IMAGE is required outside us-west-2: the EMR 7.14.0 Spark image's registry account differs per region"
    SPARK_IMAGE="895885662937.dkr.ecr.us-west-2.amazonaws.com/spark/emr-7.14.0:latest"
fi
export SPARK_IMAGE

# The manifest is rendered locally: the YAML is parsed and patched, then
# `kubectl create`d. NOT `kubectl apply --dry-run=client`: that flag still fetches
# the live object to compute a merge, so it echoes back the PREVIOUS Job's
# generated labels (controller-uid, batch.kubernetes.io/job-name) and a rename
# then fails with "spec.selector: `selector` not auto-generated". ICEBERG_DB,
# ICEBERG_TABLE, ICEBERG_WAREHOUSE, AWS_REGION, AWS_DEFAULT_REGION (= AWS_REGION,
# in the Job only) and the image are always set: their placeholders in the YAML
# have no usable default. The rest override
# the YAML only when PRESENT in the environment, not when non-empty, so an
# intentional empty value is not silently dropped. Rendered before anything is
# deleted, so a bad setting leaves the previous Job and ConfigMap in place.
render_job() {
    python3 -c '
import json, os, re, sys, yaml
d = yaml.safe_load(open(os.environ["MANIFEST"]))
d["metadata"]["name"] = os.environ["JOB"]
c = d["spec"]["template"]["spec"]["containers"][0]
c["image"] = os.environ["SPARK_IMAGE"]
env = c["env"]
given = {k: os.environ[k] for k in ("ICEBERG_DB", "ICEBERG_TABLE", "ICEBERG_WAREHOUSE",
         "AWS_REGION", "DRY_RUN", "TARGET_FILE_SIZE_BYTES", "DRIVER_MEMORY",
         "SPARK_LOCAL_THREADS", "EXPIRE_SNAPSHOTS", "EXPIRE_OLDER_THAN_HOURS",
         "EXPIRE_RETAIN_LAST") if k in os.environ}
given["AWS_DEFAULT_REGION"] = os.environ["AWS_REGION"]
for k, v in given.items():
    for e in env:
        if e["name"] == k:
            e["value"] = v
            break
    else:
        env.append({"name": k, "value": v})
bad = [e["name"] for e in env if "${" in str(e.get("value", ""))]
if "${" in c["image"]:
    bad.append("image")
if bad:
    sys.exit("compact-job.yaml: unrendered placeholder(s) in " + ", ".join(bad))
vals = {e["name"]: str(e.get("value", "")) for e in env}
if not re.match(r"^s3://[^/]+/", vals["ICEBERG_WAREHOUSE"]):
    sys.exit("ICEBERG_WAREHOUSE must be s3://<bucket>/<prefix>, got %r: set BUCKET_NAME in "
             "cdc-pipeline/.bench-env.local (see .bench-env)" % vals["ICEBERG_WAREHOUSE"])
if not re.match(r"^[a-z0-9_]+$", vals["ICEBERG_DB"]) or not re.match(r"^[a-z0-9_]+$", vals["ICEBERG_TABLE"]):
    sys.exit("ICEBERG_DB / ICEBERG_TABLE must be Glue names ([a-z0-9_]+), got %r / %r"
             % (vals["ICEBERG_DB"], vals["ICEBERG_TABLE"]))
if os.environ.get("RENDER_FORMAT") == "yaml":
    yaml.safe_dump(d, sys.stdout, sort_keys=False, width=200)
else:
    json.dump(d, sys.stdout)'
}

case "${1:-run}" in
    clean)
        kubectl delete job "$JOB" -n "$NAMESPACE" --ignore-not-found
        kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found
        exit 0 ;;
    render)
        RENDER_FORMAT=yaml render_job
        exit 0 ;;
    run) ;;
    *) die "usage: $0 [run|render|clean]" ;;
esac

JOB_JSON=$(render_job) || die "compact-job.yaml did not render (see above); nothing was submitted"

mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/${BASE}.log"

# A Job's pod template is immutable, so a same-named Job blocks resubmission.
kubectl delete job "$JOB" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null

kubectl create configmap "$CM" -n "$NAMESPACE" \
    --from-file=compact-order-items.py="${SCRIPT_DIR}/compact-order-items.py" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl create -f - <<<"$JOB_JSON" >/dev/null

echo "job/${JOB} submitted in ${NAMESPACE} (table: ${ICEBERG_DB}.${TBL}, warehouse: ${ICEBERG_WAREHOUSE}, dry_run: ${DRY_RUN:-false})"

for _ in $(seq 1 60); do
    POD=$(kubectl get pods -n "$NAMESPACE" -l "job-name=${JOB}" \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    [[ -n "$POD" ]] && break
    sleep 2
done
[[ -n "${POD:-}" ]] || { echo "ERROR: no pod created for job/${JOB}" >&2; exit 1; }

# The job asks for 24 cpu / 150Gi on an m5.12xlarge, which no existing node can
# hold, so Karpenter must launch one. "Unschedulable" is the NORMAL transient
# state for the first few minutes of provisioning, not a failure — hence the
# generous grace period rather than an immediate exit on that reason.
SCHED_GRACE="${SCHED_GRACE:-900}"
DEADLINE=$(( $(date +%s) + SCHED_GRACE ))
while :; do
    PHASE=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ "$PHASE" == "Running" || "$PHASE" == "Succeeded" || "$PHASE" == "Failed" ]] && break
    if (( $(date +%s) >= DEADLINE )); then
        kubectl get pod "$POD" -n "$NAMESPACE" \
          -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].message}' >&2
        echo "" >&2
        echo "ERROR: pod still not Running after ${SCHED_GRACE}s." >&2
        echo "  Either no m5.12xlarge on-demand capacity in the nodepool's AZs, or" >&2
        echo "  executor-memorynodepool's cpu limit is exhausted by other workloads." >&2
        echo "  Check: kubectl get nodeclaims; kubectl describe pod ${POD} -n ${NAMESPACE}" >&2
        exit 1
    fi
    sleep 10
done

echo "pod/${POD} ${PHASE} — following logs"
# `logs -f` can drop the stream on a long-running job (idle connection reset);
# --pod-running-timeout keeps it from returning instantly during a brief restart.
kubectl logs -f "$POD" -n "$NAMESPACE" --pod-running-timeout=5m 2>&1 | tee "$LOG"

# Container exit code, not kubectl's. The pod can be garbage-collected between the
# log stream ending and this query, which yields an empty value; fall back to the
# Job's succeeded count and treat a log carrying the JSON marker as success. (A
# DuckDB run was nearly discarded this way after completing all six queries.)
EXIT=$(kubectl get pod "$POD" -n "$NAMESPACE" \
         -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || true)
if [[ -z "$EXIT" ]]; then
    SUCCEEDED=$(kubectl get job "$JOB" -n "$NAMESPACE" \
                  -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
    if [[ "${SUCCEEDED:-0}" == "1" ]] || grep -q '^===JSON===$' "$LOG"; then
        EXIT=0
    else
        EXIT="?"
    fi
fi
echo ""
echo "  log: ${LOG}"
if [[ "$EXIT" == "0" ]]; then
    if grep -q '^===JSON===$' "$LOG"; then
        sed -n '/^===JSON===$/,$p' "$LOG" | tail -n +2 > "${OUT_DIR}/${BASE}.json"
        echo "  json: ${OUT_DIR}/${BASE}.json"
    fi
else
    # 137 = SIGKILL, i.e. the kubelet OOM-killed the container. The fix is more
    # container headroom over DRIVER_MEMORY, not a bigger DRIVER_MEMORY: the heap
    # is already below the limit precisely so Java raises a catchable OOM first.
    echo "  container exit: ${EXIT}$([[ "$EXIT" == "137" ]] && echo '  (SIGKILL — OOM-killed by kubelet)')" >&2
    echo "  NOTE: rewrite_data_files commits atomically — a failure leaves the" >&2
    echo "  table at its previous snapshot, not half-rewritten." >&2
    exit 1
fi
