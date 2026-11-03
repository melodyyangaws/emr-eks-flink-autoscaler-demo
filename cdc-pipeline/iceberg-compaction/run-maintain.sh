#!/usr/bin/env bash
# Continuous Iceberg maintenance (maintain.py / maintain.yaml): ONE FlinkDeployment
# that keeps compacting the four CDC Iceberg tables. See maintain.py's module
# docstring and README.md for the design (why continuous, not hourly).
#
#   ./run-maintain.sh render           # write maintain-deployed.yaml only; no cluster/AWS call
#   ./run-maintain.sh up                # upload maintain.py to S3, apply the FlinkDeployment
#   ./run-maintain.sh status            # the FlinkDeployment's state
#   ./run-maintain.sh logs              # follow the JobManager
#   ./run-maintain.sh suspend | resume   # spec.job.state, same as the writers (qb-watch.sh /
#                                        # querybench/run.sh can call these exactly like for
#                                        # flink-cdc-iceberg: no separate cron suspend/resume)
#   ./run-maintain.sh down              # delete the FlinkDeployment
#   ./run-maintain.sh plan [tables]     # read-only: today's file/delete counts, no job,
#                                        # via a short-lived pod on the writer image (no
#                                        # FlinkDeployment: plan mode never calls env.execute())
#
# Settings (.bench-env / .bench-env.local): AWS_ACCOUNT_ID, BUCKET_NAME,
# EMR_EXECUTION_ROLE_ARN, EMR_VERSION (7.14.0), IMAGE_TAG (default EMR_VERSION),
# CDC_DB_SUFFIX (the live table set). MAINT_TABLES (default: all four),
# SCHEDULE_COMMITS (10: rewrite+expire every N writer checkpoints, ~N minutes at
# the writers' 60s interval), PARTIAL_PROGRESS_MAX_COMMITS (10), EXPIRE_HOURS
# (12), EXPIRE_RETAIN_LAST (5), JM_NODEPOOL (driver-nodepool), TM_NODEPOOL
# (executor-memorynodepool), JM_CPU/JM_MEMORY (2 / 4Gi), TM_CPU/TM_MEMORY (8 /
# 32Gi: a Rewrite task holds the whole equality-delete set in heap; 8Gi OOMed at ~35M deletes),
# TM_SLOTS (4).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${HERE}/../lib/common.sh"

NAME="${MAINT_NAME:-iceberg-maintain}"
ICEBERG_DB="${ICEBERG_DB:-flink_iceberg_db${CDC_DB_SUFFIX}}"
export EMR_VERSION="${EMR_VERSION:-7.14.0}"
export IMAGE_TAG="${IMAGE_TAG:-$EMR_VERSION}"
export JM_NODEPOOL="${JM_NODEPOOL:-driver-nodepool}"
export TM_NODEPOOL="${TM_NODEPOOL:-executor-memorynodepool}"
export JM_CPU="${JM_CPU:-2}" JM_MEMORY="${JM_MEMORY:-4Gi}"
export TM_CPU="${TM_CPU:-8}" TM_MEMORY="${TM_MEMORY:-32Gi}" TM_SLOTS="${TM_SLOTS:-4}"
export MAINT_TABLES="${MAINT_TABLES:-customers,products,orders,order_items}"
export SCHEDULE_COMMITS="${SCHEDULE_COMMITS:-10}"
export PARTIAL_PROGRESS_MAX_COMMITS="${PARTIAL_PROGRESS_MAX_COMMITS:-10}"
export EXPIRE_HOURS="${EXPIRE_HOURS:-12}"
export EXPIRE_RETAIN_LAST="${EXPIRE_RETAIN_LAST:-5}"
export NAME ICEBERG_DB
OUT="${HERE}/maintain-deployed.yaml"
SCRIPT_S3="s3://${BUCKET_NAME}/flink/scripts/iceberg-maintain.py"

cmd_render() {   # <job state: running|suspended>, default running
    export JOB_STATE="${1:-running}"
    render "${HERE}/maintain.yaml" "$OUT" \
        NAMESPACE NAME EMR_EXECUTION_ROLE_ARN AWS_ACCOUNT_ID AWS_REGION IMAGE_TAG EMR_VERSION \
        JM_NODEPOOL TM_NODEPOOL JM_CPU JM_MEMORY TM_CPU TM_MEMORY TM_SLOTS \
        BUCKET_NAME CDC_DB_SUFFIX ICEBERG_DB MAINT_TABLES SCHEDULE_COMMITS PARTIAL_PROGRESS_MAX_COMMITS \
        EXPIRE_HOURS EXPIRE_RETAIN_LAST JOB_STATE \
        || die "maintain.yaml did not render (see above)"
    log "rendered ${OUT##*/} (db ${ICEBERG_DB}, tables ${MAINT_TABLES}, every ${SCHEDULE_COMMITS} commits, state ${JOB_STATE})"
}

cmd_up() {
    : "${EMR_EXECUTION_ROLE_ARN:?not set in .bench-env(.local)}"
    aws s3 cp --only-show-errors "${HERE}/maintain.py" "$SCRIPT_S3" \
        || die "could not upload maintain.py to ${SCRIPT_S3}"
    log "uploaded ${SCRIPT_S3}"
    cmd_render running
    kubectl apply -n "$NAMESPACE" -f "$OUT" || die "kubectl apply failed"
    log "FlinkDeployment/${NAME} applied; ./run-maintain.sh status to watch it reach RUNNING"
}

cmd_status() {
    kubectl get flinkdeployment "$NAME" -n "$NAMESPACE" \
        -o custom-columns='NAME:.metadata.name,JOB:.status.jobStatus.state,SPEC:.spec.job.state,LC:.status.lifecycleState' \
        2>/dev/null || echo "not deployed"
}

cmd_suspend_resume() {   # running|suspended
    kubectl get flinkdeployment "$NAME" -n "$NAMESPACE" >/dev/null 2>&1 || { log "no FlinkDeployment/${NAME}"; return 0; }
    kubectl patch flinkdeployment "$NAME" -n "$NAMESPACE" --type merge -p "{\"spec\":{\"job\":{\"state\":\"$1\"}}}" \
        || die "patch failed"
    log "FlinkDeployment/${NAME} spec.job.state=$1"
}

cmd_down() {
    kubectl delete flinkdeployment "$NAME" -n "$NAMESPACE" --ignore-not-found
}

# Read-only table state, no FlinkDeployment: a short-lived pod on the writer
# image (which already carries python3 + pyflink + the Iceberg runtime jar,
# verified 2026-11-02), since `plan` mode never calls env.execute() -- there is
# no Flink job to submit for it, only py4j calls against the catalog.
cmd_plan() {
    local tables="${1:-$MAINT_TABLES}" pod="iceberg-maint-plan-$$"
    local image="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/emr-flink-cdc-paimon:${IMAGE_TAG}"
    kubectl run "$pod" -n "$NAMESPACE" --restart=Never --image="$image" \
        --overrides="{\"spec\":{\"serviceAccountName\":\"starrocks-sa\"}}" \
        --command -- sleep 300 >/dev/null
    trap "kubectl delete pod ${pod} -n ${NAMESPACE} --ignore-not-found --wait=false >/dev/null" RETURN
    kubectl wait --for=condition=Ready "pod/${pod}" -n "$NAMESPACE" --timeout=120s >/dev/null \
        || die "plan pod did not become Ready"
    kubectl cp "${HERE}/maintain.py" "${NAMESPACE}/${pod}:/tmp/maintain.py" >/dev/null
    kubectl exec "$pod" -n "$NAMESPACE" -- bash -lc "
        export CLASSPATH=\"\$FLINK_HOME/lib/*\" AWS_REGION=${AWS_REGION} AWS_DEFAULT_REGION=${AWS_REGION}
        python3 /tmp/maintain.py plan --db ${ICEBERG_DB} \
            --warehouse s3://${BUCKET_NAME}/iceberg-warehouse --region ${AWS_REGION} --tables ${tables}
    "
}

case "${1:-}" in
    render)          cmd_render "${2:-running}" ;;
    up)               cmd_up ;;
    status)           cmd_status ;;
    logs)             kubectl logs -f -n "$NAMESPACE" "deploy/${NAME}" -c flink-main-container ;;
    suspend)          cmd_suspend_resume suspended ;;
    resume)           cmd_suspend_resume running ;;
    down)             cmd_down ;;
    plan)             cmd_plan "${2:-}" ;;
    *) die "usage: $0 render|up|status|logs|suspend|resume|down|plan [tables]" ;;
esac
