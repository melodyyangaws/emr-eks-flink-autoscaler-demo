#!/usr/bin/env bash
# The CDC query benchmark: DuckDB vs Trino on the tables Flink writes in both
# formats (four arms, ENGINES x FORMATS: Iceberg natively, Paimon through its
# Iceberg export), run from INSIDE the cluster with a watcher that shuts the
# run down when the test ends. Run from cdc-pipeline/.
#
#   SUITE=hot      ./querybench/run.sh start   hot test (hot-interleave.sh), writers + generator LIVE
#   SUITE=snapshot ./querybench/run.sh start   snapshot test (snapshot.sh): writers stopped, Iceberg
#                                              compacted, both engines from a cold start
#   ENGINES=duckdb|trino SUITE=... start       one engine only (default: "duckdb trino")
#   FORMATS=iceberg|paimon SUITE=... start     one storage format only (default: both)
#   ./querybench/run.sh logs [watch]           follow the runner (watch: the watcher)
#   ./querybench/run.sh status                 writers, generator, engines, Jobs
#   SUITE=hot ./querybench/run.sh stop         tear the run down by hand (generator, writers,
#                                              monitor, DuckDB, Trino): the watcher only does
#                                              it itself after a COMPLETED test
#   SUITE=hot ./querybench/run.sh render       DRY RUN: render the Job manifests, the step and
#                                              every query for all four arms with sample
#                                              values; no cluster or AWS call
#   ./querybench/run.sh summary                newest hot + snapshot run of the session ->
#                                              bench-results/QUERYBENCH-SUMMARY-<session>.md
#
# Results: the runner uploads its bench-results/ to
# s3://$BUCKET_NAME/querybench-results/cdc-session<set tag><SESSION_SUFFIX>/<STAMP>-<suite>/
# (set tag: CDC_DB_SUFFIX with - for _, -v3 for the Iceberg v3 set; .bench-env).
#
# `start`, in order (any failure stops it before a Job is created):
#   0. waits while cdc-qb-runner still runs (FORCE=1: replace it); refuses while
#      throughput-benchmark's qb-runner runs (same cluster and bucket);
#   1. waits for the old cdc-qb-watch to finish its shutdown, then deletes it;
#   2. snapshot: scales the generator to 0, waits WRITER_DRAIN_S (180) for the
#      writers to commit the binlog tail, suspends both writers, resolves
#      SNAPSHOT_DT (newest closed date in both fact tables of every format);
#   2b. snapshot with iceberg in FORMATS, COMPACT_BEFORE_SNAPSHOT=true: SUBMITS one Iceberg compaction
#      Job per COMPACT_TABLES table (../iceberg-compaction/run-compaction.sh, in
#      the background); the runner waits for them. Otherwise it checks the
#      compaction gate now and refuses if a Job is missing, running or failed
#      (ICEBERG_REQUIRE_COMPACTION=0 skips the gate);
#   3. engines up: duckdb-warm-cdc (engine-deploy/deploy-duckdb-cdc.sh up if it
#      does not answer /health) and Trino (engine-deploy/deploy-trino.sh up if
#      fewer than TRINO_WORKERS workers are active);
#   4. hot: empties both caches (clear-caches.sh; skipped when every engine was
#      just started fresh). Snapshot: the runner does it, after its gate, pins
#      and slice, right before the first warm-up pass;
#   5. starts the runner Job (cdc-qb-runner); 6. starts the watcher (cdc-qb-watch).
# Steps 0-4 run on the laptop (helm, envsubst, the writers stopped before the
# windows). Every PASS_VARS variable set in this shell reaches the pods.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
PASS_VARS="STAMP SESSION_SUFFIX RUNS WARMUP ALLOW_CONCURRENT ENGINES FORMATS PROFILE_SCAN QB_SETUP_ENGINE SKIP_PUFFIN_VERIFICATION_ARMS
HOT_TO HOT_FROM HOT_DT HOT_WINDOW_SECONDS HOT_LAG_SECONDS HOT_COMMIT_MARGIN_S HOT_COMMIT_TABLES HOT_REPIN
STABLE_CHECKS WAIT_COMMIT_S LOOKUP_ORDER_ID LOOKUP_ORDER_DT QB_QUERY_WRITERS
SNAPSHOT_DT S_FROM S_TO SNAPSHOT_SLICE_MINUTES SNAPSHOT_SLICE_MARGIN_S COMPACT_BEFORE_SNAPSHOT COMPACT_TABLES
WATCH_INTERVAL_S SUSPEND_WRITERS STOP_GENERATOR STOP_TRINO STOP_DUCKDB
HOT_QUERY_TIMEOUT_S SNAPSHOT_QUERY_TIMEOUT_S MAX_RESULT_ROWS
COMPACT_NOT_BEFORE ICEBERG_REQUIRE_COMPACTION COMPACT_GATE_TIMEOUT_S ENGINE_GATE_TIMEOUT_S
QB_RESULTS_PREFIX_RUN TRINO_VERSION TRINO_WORKERS TRINO_USER TRINO_CATALOG CLEAR_COORDINATOR"
# Passed even when set to an empty value: the pod's .bench-env has non-empty
# defaults for them (the v3 set), so `CDC_DB_SUFFIX= ...` must reach the pod as such.
PASS_VARS_EMPTY_OK="ICEBERG_FORMAT_VERSION CDC_DB_SUFFIX DUCKDB_IMAGE"
RUNNER=cdc-qb-runner
WATCHER=cdc-qb-watch
DUCK="$DUCKDB_NAME"
DEPLOY_TRINO="${CDC_DIR}/engine-deploy/deploy-trino.sh"
CMD_FILE="${CDC_DIR}/.qb-runner-cmd.sh"
ENV_LOCAL="${CDC_DIR}/.bench-env.local"
[[ -f "$ENV_LOCAL" ]] || ENV_LOCAL="${TB_DIR}/.bench-env.local"

die() { echo "$*; no query Job left running" >&2; exit 1; }
kubectl_version() {
    echo "${KUBECTL_VERSION:-$(kubectl version -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"].split("-")[0])' 2>/dev/null || echo v1.35.6)}"
}

# The step script the pod runs: .bench-env, the PASS_VARS set here, the step.
write_cmd() {   # <step>
    local v
    { echo '#!/bin/bash'; echo 'set -uo pipefail'; echo 'cd /work && source .bench-env'
      for v in $PASS_VARS; do [[ -n "${!v:-}" ]] && printf 'export %s=%q\n' "$v" "${!v}"; done
      for v in $PASS_VARS_EMPTY_OK; do [[ -n "${!v+set}" ]] && printf 'export %s=%q\n' "$v" "${!v}"; done
      printf '# step: %s\n' "${1%%$'\n'*}"
      echo "$1"; } > "$CMD_FILE"
}

render_job() {   # <name> -> its -deployed.yaml path
    local out="${HERE}/${1}-deployed.yaml" NAME="$1" ROLE_ARN="${EMR_EXECUTION_ROLE_ARN:?not set in .bench-env(.local)}" KUBECTL_VERSION
    KUBECTL_VERSION=$(kubectl_version)
    render "${HERE}/qb-runner.yaml" "$out" NAME NAMESPACE KUBECTL_VERSION ROLE_ARN || die "could not render ${out##*/}"
    echo "$out"
}

bundle() {   # <tgz>
    COPYFILE_DISABLE=1 tar --no-xattrs --no-mac-metadata --exclude='*-deployed.yaml' --exclude='__pycache__' \
        -C "$CDC_DIR" -czf "$1" querybench lib engine-deploy .bench-env .qb-runner-cmd.sh 2>/dev/null \
    || COPYFILE_DISABLE=1 tar --exclude='*-deployed.yaml' --exclude='__pycache__' \
        -C "$CDC_DIR" -czf "$1" querybench lib engine-deploy .bench-env .qb-runner-cmd.sh
}

start_job() {   # <name> <step>
    local name="$1" d out
    d=$(mktemp -d)
    write_cmd "$2"
    bundle "${d}/bundle.tgz"
    rm -f "$CMD_FILE"
    delete_job "$name"
    kubectl create configmap "$name" -n "$NAMESPACE" --from-file="${d}/bundle.tgz" >/dev/null || die "could not create ConfigMap ${name}"
    kubectl create secret generic "$name" -n "$NAMESPACE" --from-file=bench-env-local="$ENV_LOCAL" >/dev/null || die "could not create Secret ${name}"
    rm -rf "$d"
    out=$(render_job "$name")
    kubectl apply -f "$out" >/dev/null || die "could not apply ${out##*/}"
    echo "job/${name} started: ${2%%$'\n'*} (${out##*/})"
}

delete_job() {
    kubectl delete job "$1" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    kubectl delete configmap "$1" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl delete secret "$1" -n "$NAMESPACE" --ignore-not-found >/dev/null
}

job_active() { [[ "$(kubectl get job "$1" -n "$NAMESPACE" -o jsonpath='{.status.active}' 2>/dev/null || true)" =~ ^[1-9] ]]; }

wait_for_runner() {
    if job_active qb-runner || job_active qb-watch; then
        [[ "${FORCE:-0}" == 1 ]] || die "throughput-benchmark's qb-runner/qb-watch is active: it shares the cluster, the bucket and the nodepools. FORCE=1 to go on anyway"
        warn "FORCE=1: throughput-benchmark's qb-runner/qb-watch is active"
    fi
    while job_active "$RUNNER"; do
        [[ "${FORCE:-0}" == 1 ]] && { warn "FORCE=1: replacing the ${RUNNER} that is still running"; return 0; }
        log "${RUNNER} is still running a test; waiting (every 60 s; FORCE=1 to replace it)"
        sleep 60
    done
}

wait_for_watcher() {
    local deadline=$(( $(date +%s) + ${WATCH_INTERVAL_S:-600} + 900 ))
    while job_active "$WATCHER"; do
        [[ "${FORCE:-0}" == 1 ]] && { warn "FORCE=1: deleting ${WATCHER} before its shutdown"; return 0; }
        (( $(date +%s) < deadline )) || { warn "${WATCHER} still running; deleting it"; return 0; }
        log "${WATCHER} is still shutting the last test down; waiting (every 30 s)"
        sleep 30
    done
}

duckdb_health() {
    kubectl rollout status -n "$NAMESPACE" "deploy/${DUCK}" --timeout=5s >/dev/null 2>&1 || return 0
    qb_duck_health
}

STARTED_FRESH=""
duckdb_up() {
    local h; h=$(duckdb_health)
    # A server deployed for the other table set or from another image is redeployed.
    if [[ -n "$h" ]] && ! python3 -c '
import json, sys
h, fv, img = json.loads(sys.argv[1]), sys.argv[2], sys.argv[3]
sys.exit(0 if str(h.get("iceberg_format_version", 3)) == fv and (h.get("image") or "pip") == (img or "pip") else 1)' "$h" "${ICEBERG_FORMAT_VERSION}" "${DUCKDB_IMAGE:-}" 2>/dev/null; then
        echo "${SUITE}: ${DUCK} is up for another set or image (/health: ${h:0:200}); redeploying"
        h=""
    fi
    if [[ -n "$h" ]]; then echo "${SUITE}: ${DUCK} already up"
    else
        echo "${SUITE}: starting ${DUCK}"; STARTED_FRESH+=" duckdb"
        "${CDC_DIR}/engine-deploy/deploy-duckdb-cdc.sh" up || die "${DUCK} did not come up"
        h=$(duckdb_health)
    fi
    [[ "$h" == *'"busy": false'* ]] || die "${DUCK} is not ready and idle (/health: ${h:-no answer})"
}

# Trino: up when TRINO_WORKERS workers are active; else deploy-trino.sh up
# (helm upgrade --install, waits for the workers). The watcher scales a
# finished run's Trino to 0 (the runner pod has no helm): those replicas are
# restored first, so `up` does not depend on helm resetting a scaled field.
trino_up() {
    local n v
    [[ -x "$DEPLOY_TRINO" ]] || die "no ${DEPLOY_TRINO#${CDC_DIR}/}"
    n=$(qb_trino_workers)
    if [[ -n "$n" ]] && (( n >= TRINO_WORKERS )); then
        v=$(qb_trino "SELECT node_version FROM system.runtime.nodes WHERE coordinator = true" 60 | head -1 | tr -d '"')
        echo "${SUITE}: Trino already up (${n} workers, ${v:-version ?})"
        [[ -z "${TRINO_VERSION:-}" || "$v" == "$TRINO_VERSION" || "${TRINO_VERSION_CHECK:-1}" == 0 ]] \
            || die "Trino runs ${v:-an unknown version}, not TRINO_VERSION=${TRINO_VERSION} (.bench-env): ${DEPLOY_TRINO#${CDC_DIR}/} up, or TRINO_VERSION_CHECK=0 to measure it as is"
        return 0
    fi
    echo "${SUITE}: starting Trino ${TRINO_VERSION:-} (${n:-0}/${TRINO_WORKERS} workers active): ${DEPLOY_TRINO#${CDC_DIR}/} up"
    STARTED_FRESH+=" trino"
    if [[ "$(kubectl get deploy trino-coordinator -n "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)" == 0 ]]; then
        kubectl scale deploy/trino-coordinator -n "$NAMESPACE" --replicas=1 >/dev/null || true
        kubectl scale "deploy/${TRINO_WORKER_DEPLOY}" -n "$NAMESPACE" --replicas="$TRINO_WORKERS" >/dev/null || true
    fi
    "$DEPLOY_TRINO" up || die "Trino did not come up"
    qb_trino_ready || die "Trino: fewer than ${TRINO_WORKERS} active workers after up"
}

suite_step() {
    case "$SUITE" in
        hot)      echo './querybench/hot-interleave.sh' ;;
        snapshot) echo './querybench/snapshot.sh' ;;
        *)        die "set SUITE=hot or SUITE=snapshot" ;;
    esac
}

# Submit one Iceberg compaction Job per COMPACT_TABLES table, all at once, in the
# background (run-compaction.sh creates the Job and then only follows its log),
# and wait just until each Job object exists: from then on the Jobs run in the
# cluster whether or not this laptop stays up. COMPACT_NOT_BEFORE (now) makes
# the gate ignore any older iceberg-compact-* Job.
submit_compaction() {
    local t job i n ts
    export COMPACT_NOT_BEFORE; COMPACT_NOT_BEFORE=$(date -u +%s)
    mkdir -p "${CDC_DIR}/bench-results"
    for t in ${COMPACT_TABLES:-customers products orders order_items}; do
        job="iceberg-compact-${t//_/-}"
        echo "  submitting Iceberg compaction ${ICEBERG_DB}.${t} as Job ${job} (log: bench-results/${job}-${STAMP}.submit.log)"
        ICEBERG_DB="$ICEBERG_DB" ICEBERG_TABLE="$t" JOB="$job" \
            nohup "${CDC_DIR}/iceberg-compaction/run-compaction.sh" > "${CDC_DIR}/bench-results/${job}-${STAMP}.submit.log" 2>&1 &
    done
    for (( i = 0; i < 60; i++ )); do
        n=0
        for t in ${COMPACT_TABLES:-customers products orders order_items}; do
            job="iceberg-compact-${t//_/-}"
            ts=$(kubectl get job "$job" -n "$NAMESPACE" -o jsonpath='{.metadata.creationTimestamp}' 2>/dev/null || true)
            [[ -n "$ts" ]] && (( $(python3 -c 'import sys, datetime as d; print(int(d.datetime.strptime(sys.argv[1], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=d.timezone.utc).timestamp()))' "$ts") >= COMPACT_NOT_BEFORE )) && n=$((n + 1))
        done
        (( n == $(wc -w <<<"${COMPACT_TABLES:-customers products orders order_items}") )) && { echo "  compaction Jobs submitted; the runner waits for them"; return 0; }
        sleep 5
    done
    die "the compaction Jobs were not all created within 5 min (see bench-results/iceberg-compact-*-${STAMP}.submit.log)"
}

cmd_start() {
    local step results e
    : "${EMR_EXECUTION_ROLE_ARN:?not set in .bench-env(.local)}"
    [[ -f "$ENV_LOCAL" ]] || die "no .bench-env.local (cdc-pipeline/ or throughput-benchmark/)"
    [[ -z "${SNAPSHOT_FORMAT:-}" ]] || die "SNAPSHOT_FORMAT is gone: the snapshot test is one phase for both formats; FORMATS=iceberg|paimon|both picks the arms"
    export ENGINES FORMATS; ENGINES=$(qb_engines); FORMATS=$(qb_formats)
    step=$(suite_step)
    echo "table set: Iceberg v${ICEBERG_FORMAT_VERSION}, ${ICEBERG_DB} / ${PAIMON_DB}; engines: ${ENGINES}; formats: ${FORMATS}"
    export STAMP
    results="${QB_RESULTS_PREFIX}/${STAMP}-${SUITE}/"
    export QB_RESULTS_PREFIX_RUN="$results"
    step+=$'\n''rc=$?; aws s3 sync bench-results "s3://${BUCKET_NAME}/'"${results}"'" --only-show-errors \
  && echo "status: results-uploaded '"${results}"'" || echo "status: results-upload-failed"; exit $rc'
    wait_for_runner                                                          # 0
    wait_for_watcher; delete_job "$WATCHER"                                  # 1
    if [[ "$SUITE" == snapshot ]]; then                                      # 2
        qb_stop_generator; qb_stop_writers
        qb_resolve_snapshot_dt; export SNAPSHOT_DT="$QB_SNAPSHOT_DT"
        echo "  snapshot partition: ${SNAPSHOT_DT}"
        if ! qb_has_format iceberg; then                                     # 2b
            echo "  FORMATS=paimon: no Iceberg compaction (Paimon compacts in its writer)"
        elif [[ "${COMPACT_BEFORE_SNAPSHOT:-false}" == true ]]; then
            submit_compaction
            # TODO(paimon-compaction): no one-off FULL compaction of the Paimon
            # tables exists. Paimon's Iceberg export lists only top-LSM-level
            # files, so the paimon arms may read fewer rows than native Paimon
            # (both paimon arms read the same export, so they still agree with
            # each other). A batch Flink job running CALL sys.compact(`table` =>
            # '<db>.<t>', compact_strategy => 'full') per table would close it.
        else
            ( qb_compaction_gate 0 ) || die "the snapshot test cannot start"
        fi
    fi
    for e in $ENGINES; do                                                    # 3
        case "$e" in duckdb) duckdb_up ;; trino) trino_up ;; esac
    done
    if [[ "$SUITE" == snapshot ]]; then                                      # 4
        echo "snapshot: the runner empties the caches itself, after its gate, pins and slice"
    elif [[ "$(for e in $ENGINES; do [[ "$STARTED_FRESH" == *"$e"* ]] || echo warm; done)" != *warm* ]]; then
        echo "hot: every engine started fresh: caches already empty"
    else
        ENGINES="$ENGINES" "${HERE}/clear-caches.sh" || die "could not clear the caches"
    fi
    start_job "$RUNNER" "$step"                                              # 5
    ( start_job "$WATCHER" "SUITE=${SUITE} ./querybench/qb-watch.sh" ) \
        || { delete_job "$RUNNER"; die "the watcher did not start, so the runner was deleted"; }   # 6
    echo "  logs: $0 logs (runner), $0 logs watch (watcher)"
    echo "  results, once the runner logs \"status: results-uploaded\": s3://\$BUCKET_NAME/${results}"
}

# Dry run: everything start would send, rendered with sample values; no kubectl, no AWS.
cmd_render() {
    local d step t T e s v n f arm
    d="${RENDER_DIR:-$(mktemp -d)}"; mkdir -p "$d"
    export ENGINES FORMATS; ENGINES=$(qb_engines); FORMATS=$(qb_formats)
    step=$(suite_step)
    KUBECTL_VERSION="${KUBECTL_VERSION:-v1.35.6}"
    write_cmd "$step"; cp "$CMD_FILE" "${d}/qb-runner-cmd.sh"; bundle "${d}/bundle.tgz"; rm -f "$CMD_FILE"
    ( export KUBECTL_VERSION; render_job "$RUNNER" >/dev/null && render_job "$WATCHER" >/dev/null ) || die "manifest render failed"
    mv "${HERE}/${RUNNER}-deployed.yaml" "${HERE}/${WATCHER}-deployed.yaml" "$d/"
    { qb_run_meta; echo "engines: ${ENGINES}  formats: ${FORMATS}  setup engine: $(qb_setup_engine)"
      echo "results: s3://\$BUCKET_NAME/${QB_RESULTS_PREFIX}/  pins: s3://\$BUCKET_NAME/${QB_PIN_PREFIX}/"; } > "${d}/run-meta.txt"
    "${CDC_DIR}/engine-deploy/deploy-duckdb-cdc.sh" render >/dev/null && mv "${CDC_DIR}/engine-deploy/duckdb-cdc-deployed.yaml" "$d/"
    # Trino's Helm values, as `deploy-trino.sh up` passes them to helm (envsubst only).
    "$DEPLOY_TRINO" render >/dev/null 2>&1 && mv "${CDC_DIR}/engine-deploy/trino-values-deployed.yaml" "$d/" \
        || die "${DEPLOY_TRINO#${CDC_DIR}/} render failed"
    # Sample pins and windows, or the ones set in this shell.
    for t in $QB_TABLES; do
        T=$(qb_upper "$t")
        for v in "QB_PIN_SNAP_ICEBERG_${T}=8000000000000000101" "QB_PIN_SNAP_PAIMON_${T}=101" \
                 "QB_PIN_META_ICEBERG_${T}=s3://${BUCKET_NAME}/${QB_PIN_PREFIX}/SAMPLE/iceberg/${t}/00001-x.metadata.json" \
                 "QB_PIN_META_PAIMON_${T}=s3://${BUCKET_NAME}/${QB_PIN_PREFIX}/SAMPLE/paimon/${t}/v101.metadata.json"; do
            n="${v%%=*}"; [[ -n "${!n:-}" ]] || export "$v"
        done
    done
    qb_resolve_hot
    QB_SNAPSHOT_DT="${SNAPSHOT_DT:-2026-09-27}" QB_S_FROM="${S_FROM:-2026-09-27 12:00:00}" QB_S_TO="${S_TO:-2026-09-27 12:10:00}"
    QB_LOOKUP_ORDER_ID="${LOOKUP_ORDER_ID:-12345}" QB_LOOKUP_ORDER_DT="${LOOKUP_ORDER_DT:-2026-09-27}"
    for s in hot snapshot; do
        [[ -d "${QB_DIR}/${s}" ]] || { warn "no ${s}/ queries yet"; continue; }
        for e in $QB_ENGINES_ALL; do
            for arm in iceberg paimon; do
                for f in $(qb_query_files "$s"); do
                    { echo "-- ${e}/${arm}/$(qb_query_name "$f")"; qb_render "$f" "$e" "$arm"; } >> "${d}/sql-${s}-${e}-${arm}.sql" \
                        || die "render failed: ${s}/${e}/${arm}/$(basename "$f")"
                done
            done
        done
    done
    echo "rendered into ${d}:"; ls -1 "$d" | sed 's/^/  /'
}

cmd_status() {
    echo "== session: SESSION_SUFFIX='${SESSION_SUFFIX}' results: s3://\$BUCKET_NAME/${QB_RESULTS_PREFIX}/"
    echo "== table set: Iceberg v${ICEBERG_FORMAT_VERSION} CDC_DB_SUFFIX='${CDC_DB_SUFFIX}' (${ICEBERG_DB}, ${PAIMON_DB}); engines $(qb_engines); formats $(qb_formats)"
    echo "== writers:";   kubectl get flinkdeployment $FLINK_JOBS -n "$NAMESPACE" -o custom-columns='  NAME:.metadata.name,SPEC:.spec.job.state,JOB:.status.jobStatus.state' --no-headers 2>/dev/null || echo "  none"
    echo "== generator: $(qb_generator_state)"
    echo "== ${DUCK}: $(duckdb_health | cut -c1-160)"
    echo "== Trino: $(qb_trino_workers || true) of ${TRINO_WORKERS} workers active$([[ -x "$DEPLOY_TRINO" ]] && echo " (details: ${DEPLOY_TRINO#${CDC_DIR}/} status)")"
    echo "== jobs:";      kubectl get jobs "$RUNNER" "$WATCHER" qb-runner qb-watch -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print "  " $1, $2}' || true
}

# Newest hot and snapshot run of the session from S3 (HOT_STAMP= / SNAPSHOT_STAMP=
# pick others; SUMMARY_DIR=<local dir with hot/ and/or snapshot/> skips S3) -> the Markdown summary.
cmd_summary() {
    local S T hot snap session out
    S="s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}"
    T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT
    latest() { aws s3 ls "${S}/" 2>/dev/null | sed -n "s|.* PRE \([0-9]\{8\}T[0-9]\{6\}Z\)-${1}/\$|\1|p" | sort | tail -1; }
    if [[ -n "${SUMMARY_DIR:-}" ]]; then
        cp -R "${SUMMARY_DIR}/." "$T/"; hot="${HOT_STAMP:-}"; snap="${SNAPSHOT_STAMP:-}"
    else
        hot="${HOT_STAMP:-$(latest hot)}"
        [[ -z "$hot" ]] || aws s3 sync --only-show-errors "${S}/${hot}-hot/" "$T/hot/"
        snap="${SNAPSHOT_STAMP:-$(latest snapshot)}"
        [[ -z "$snap" ]] || aws s3 sync --only-show-errors "${S}/${snap}-snapshot/" "$T/snapshot/"
        [[ -n "$hot$snap" ]] || die "no runs under ${QB_RESULTS_PREFIX}/"
    fi
    session="${SESSION:-$(echo "${hot:-${snap:-$(date -u +%Y%m%d)}}" | cut -c1-8)${CDC_SET_TAG}${SESSION_SUFFIX}}"
    out="${CDC_DIR}/bench-results/QUERYBENCH-SUMMARY-${session}.md"
    mkdir -p "${CDC_DIR}/bench-results"
    python3 "${HERE}/summarize.py" --work "$T" --session "$session" ${hot:+--hot "$hot"} ${snap:+--snapshot "$snap"} \
        --iceberg-format-version "$ICEBERG_FORMAT_VERSION" --warmup "${WARMUP:-1}" --out "$out"
    [[ -z "${AWS_ACCOUNT_ID:-}" ]] || sed -i.bak -e "s/${BUCKET_NAME}/<bucket>/g" -e "s/${AWS_ACCOUNT_ID}/<id>/g" "$out"
    rm -f "$out.bak"
    log "summary: ${out#${REPO_DIR}/} (bench-results/ is git-ignored: git add -f to commit it)"
}

case "${1:-}" in
    start)   SUITE="${SUITE:-}"; [[ "$SUITE" == all ]] && die "set SUITE=hot or SUITE=snapshot"; cmd_start ;;
    render)  [[ "$SUITE" == all ]] && SUITE=hot; cmd_render ;;
    logs)    kubectl logs -f -n "$NAMESPACE" "job/$([[ "${2:-}" == watch ]] && echo "$WATCHER" || echo "$RUNNER")" ;;
    status)  cmd_status ;;
    # Stop the generator, suspend the writers, stop the job monitor, DuckDB and
    # Trino (the watcher's teardown). The watcher only does this itself after a
    # completed test; after a failure, run this once you have looked.
    stop)    SUITE="${SUITE:-hot}"; [[ "$SUITE" == all ]] && SUITE=hot; SUITE="$SUITE" "${HERE}/qb-watch.sh" teardown ;;
    summary) cmd_summary ;;
    *)       sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
