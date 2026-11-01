#!/usr/bin/env bash
# The CDC query benchmark, from INSIDE the cluster (closing the laptop does not
# stop it), with a watcher that shuts the run down when the test ends. Adapted
# from throughput-benchmark/querybench/run.sh. Run from cdc-pipeline/.
#
#   SUITE=hot      ./querybench/run.sh start   hot test (hot-interleave.sh), writers + generator LIVE
#   SUITE=snapshot ./querybench/run.sh start   snapshot test (snapshot.sh), writers stopped first, in
#                                              two phases, each from a cold start:
#       SNAPSHOT_FORMAT=both     (default) Paimon phase now, Iceberg phase once its compaction gate passes
#       SNAPSHOT_FORMAT=paimon   Paimon phase only (never waits for compaction)
#       SNAPSHOT_FORMAT=iceberg  Iceberg phase only; refuses unless its compaction has completed
#   ENGINES=duckdb SUITE=snapshot ...         one engine only (snapshot; starrocks|duckdb)
#   ./querybench/run.sh logs [watch]           follow the runner (watch: the watcher)
#   ./querybench/run.sh status                 writers, generator, engines, Jobs
#   SUITE=hot ./querybench/run.sh render       DRY RUN: render the Job manifest, the step and
#                                              every query for every engine/arm with sample
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
#      throughput-benchmark's qb-runner runs (same engines);
#   1. waits for the old cdc-qb-watch to finish its shutdown, then deletes it;
#   2. snapshot: scales the generator to 0, waits WRITER_DRAIN_S (180) for the
#      writers to commit the binlog tail, suspends both writers, resolves
#      SNAPSHOT_DT (newest closed date in both formats' fact tables);
#   2b. snapshot, COMPACT_BEFORE_SNAPSHOT=true (not with SNAPSHOT_FORMAT=iceberg):
#      SUBMITS one Iceberg compaction Job per COMPACT_TABLES table
#      (../iceberg-compaction/run-compaction.sh, in the background) and goes on:
#      the Paimon phase does not wait for it, the Iceberg phase does (in the runner);
#   2c. SNAPSHOT_FORMAT=iceberg (or both without COMPACT_BEFORE_SNAPSHOT=true):
#      checks the compaction gate now and refuses if a compaction Job is
#      missing, running or failed (ICEBERG_REQUIRE_COMPACTION=0 skips the gate);
#   3. StarRocks up (throughput-benchmark/engine-deploy/deploy-starrocks.sh
#      install if the FE or a BE is not Ready) and duckdb-warm-cdc up
#      (engine-deploy/deploy-duckdb-cdc.sh up if it does not answer /health);
#   3b. snapshot: resolves S_FROM/S_TO on StarRocks (MIN/MAX(order_date), datacache off);
#   4. hot: empties both caches (clear-caches.sh; skipped when both engines were
#      just started fresh). Snapshot: each phase does it in the runner, after
#      its gate and pins, right before its passes;
#   5. starts the runner Job (cdc-qb-runner); 6. starts the watcher (cdc-qb-watch).
# Steps 0-4 run on the laptop (helm, envsubst, the writers stopped before the
# windows). Every PASS_VARS variable set in this shell reaches the pods.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
PASS_VARS="STAMP SESSION_SUFFIX RUNS WARMUP ARMS FORMAT ALLOW_CONCURRENT ENGINES PROFILE_SCAN
HOT_TO HOT_FROM HOT_DT HOT_WINDOW_SECONDS HOT_LAG_SECONDS HOT_COMMIT_MARGIN_S HOT_COMMIT_TABLES HOT_REPIN
STABLE_CHECKS WAIT_COMMIT_S LOOKUP_ORDER_ID LOOKUP_ORDER_DT
SNAPSHOT_DT S_FROM S_TO COMPACT_BEFORE_SNAPSHOT COMPACT_TABLES
WATCH_INTERVAL_S SUSPEND_WRITERS STOP_GENERATOR STOP_STARROCKS STOP_DUCKDB
SR_PIN_FORM SR_SNAPSHOT_IO_TASKS HOT_QUERY_TIMEOUT_S SNAPSHOT_QUERY_TIMEOUT_S
SNAPSHOT_FORMAT COMPACT_NOT_BEFORE ICEBERG_REQUIRE_COMPACTION COMPACT_GATE_TIMEOUT_S ENGINE_GATE_TIMEOUT_S
QB_RESULTS_PREFIX_RUN SR_V3_ARMS SR_PAIMON_JNI SR_IMAGE_TAG SR_CHART_VERSION SKIP_PUFFIN_VERIFICATION_ARMS"
# Passed even when set to an empty value: the pod's .bench-env has non-empty
# defaults for them (the v3 set), so `CDC_DB_SUFFIX= ...` must reach the pod as such.
PASS_VARS_EMPTY_OK="ICEBERG_FORMAT_VERSION CDC_DB_SUFFIX DUCKDB_IMAGE"
RUNNER=cdc-qb-runner
WATCHER=cdc-qb-watch
DUCK="${DUCKDB_NAME:-duckdb-warm-cdc}"
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
        [[ "${FORCE:-0}" == 1 ]] || die "throughput-benchmark's qb-runner/qb-watch is active: it shares StarRocks (and its watcher takes it down). FORCE=1 to go on anyway"
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
    kubectl exec -n "$NAMESPACE" "deploy/${DUCK}" -c duckdb -- python3 -c \
        'import json, urllib.request; print(json.dumps(json.load(urllib.request.urlopen("http://localhost:8080/health", timeout=10))))' 2>/dev/null || true
}

STARTED_FRESH=""
ready_count() {
    kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/component=$1" -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null \
        | { grep -o True || true; } | wc -l | tr -d ' '
}
# The FE/BE image tags of the running StarRocks (sorted, unique).
starrocks_tags() {
    kubectl get pods -n "$NAMESPACE" -l 'app.kubernetes.io/component in (fe,be)' \
        -o jsonpath='{range .items[*]}{range .spec.containers[*]}{.image}{"\n"}{end}{end}' 2>/dev/null \
        | grep -E 'starrocks/(fe|be)' | sed 's/.*://' | sort -u | tr '\n' ' ' | sed 's/ $//'
}
# StarRocks is ONE cluster for this and the throughput benchmark, installed by
# throughput-benchmark/engine-deploy/deploy-starrocks.sh; its defaults are not
# touched: the version comes from .bench-env (SR_IMAGE_TAG / SR_CHART_VERSION)
# and is passed as that script's SR_IMAGE_TAG / CHART_VERSION. A cluster that
# is already up on another tag is refused (SR_VERSION_CHECK=0 runs on it), as
# re-installing it would also change the throughput benchmark's engine.
starrocks_up() {
    local fe be i tags
    fe=$(ready_count fe); be=$(ready_count be)
    if (( fe >= 1 && be >= 3 )); then
        tags=$(starrocks_tags)
        echo "${SUITE}: StarRocks already up (FE ${fe}, BE ${be}; image ${tags:-?})"
        if [[ -n "${SR_IMAGE_TAG:-}" && "$tags" != "$SR_IMAGE_TAG" && "${SR_VERSION_CHECK:-1}" != 0 ]]; then
            die "StarRocks runs ${tags:-an unknown tag}, not SR_IMAGE_TAG=${SR_IMAGE_TAG} (.bench-env). It is shared with the throughput benchmark: move it with SR_IMAGE_TAG=${SR_IMAGE_TAG} CHART_VERSION=${SR_CHART_VERSION:-<chart>} ${TB_DIR#${REPO_DIR}/}/engine-deploy/deploy-starrocks.sh install once no test runs on it, or SR_VERSION_CHECK=0 to measure the running one"
        fi
        return 0
    fi
    echo "${SUITE}: starting StarRocks ${SR_IMAGE_TAG:-<script default>} (chart ${SR_CHART_VERSION:-<script default>}; FE ${fe}, BE ${be} ready): throughput-benchmark/engine-deploy/deploy-starrocks.sh install (shared with the throughput benchmark)"
    STARTED_FRESH+=" starrocks"
    local -a ov=()
    [[ -n "${SR_IMAGE_TAG+set}" ]] && ov+=("SR_IMAGE_TAG=${SR_IMAGE_TAG}")     # set but empty = the chart's floating tag
    [[ -n "${SR_CHART_VERSION:-}" ]] && ov+=("CHART_VERSION=${SR_CHART_VERSION}")
    env ${ov[@]+"${ov[@]}"} "${TB_DIR}/engine-deploy/deploy-starrocks.sh" install >/dev/null || die "StarRocks did not come up"
    for (( i = 0; i < 80; i++ )); do be=$(ready_count be); (( be >= 3 )) && break; sleep 15; done
    (( be >= 3 )) || die "StarRocks BEs not Ready 20 min after install"
}
duckdb_up() {
    local h; h=$(duckdb_health)
    # A server deployed for the other table set (its ICEBERG_FORMAT_VERSION
    # decides the Paimon arm's puffin setting) or from another image is redeployed.
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
        "${HERE}/../engine-deploy/deploy-duckdb-cdc.sh" up || die "${DUCK} did not come up"
        h=$(duckdb_health)
    fi
    [[ "$h" == *'"busy": false'* ]] || die "${DUCK} is not ready and idle (/health: ${h:-no answer})"
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
# the Iceberg gate ignore any older iceberg-compact-* Job.
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
        (( n == $(wc -w <<<"${COMPACT_TABLES:-customers products orders order_items}") )) && { echo "  compaction Jobs submitted; the Iceberg phase waits for them"; return 0; }
        sleep 5
    done
    die "the compaction Jobs were not all created within 5 min (see bench-results/iceberg-compact-*-${STAMP}.submit.log)"
}

cmd_start() {
    local step results
    : "${EMR_EXECUTION_ROLE_ARN:?not set in .bench-env(.local)}"
    [[ -f "$ENV_LOCAL" ]] || die "no .bench-env.local (cdc-pipeline/ or throughput-benchmark/)"
    export ENGINES="${ENGINES:-starrocks duckdb}"
    [[ "$ENGINES" =~ ^(starrocks|duckdb)([ ,](starrocks|duckdb))?$ ]] || die "ENGINES must name starrocks and/or duckdb, got '${ENGINES}'"
    [[ "$SUITE" == hot && ( "$ENGINES" != *starrocks* || "$ENGINES" != *duckdb* ) ]] && die "the hot test interleaves both engines: ENGINES must be both"
    step=$(suite_step)
    echo "table set: Iceberg v${ICEBERG_FORMAT_VERSION}, ${PAIMON_DB} / ${ICEBERG_DB}; StarRocks arms ${SR_ARMS_ALL}${QB_SR_SKIPPED:+ (skipped: ${QB_SR_SKIPPED})}"
    export STAMP SNAPSHOT_FORMAT="${SNAPSHOT_FORMAT:-both}"
    case "$SNAPSHOT_FORMAT" in paimon|iceberg|both) ;; *) die "SNAPSHOT_FORMAT must be paimon|iceberg|both" ;; esac
    [[ "$SUITE" == snapshot && "$SNAPSHOT_FORMAT" == iceberg && "${COMPACT_BEFORE_SNAPSHOT:-false}" == true ]] \
        && die "SNAPSHOT_FORMAT=iceberg measures a compaction that has COMPLETED: submit it with SNAPSHOT_FORMAT=paimon|both (or run-compaction.sh by hand), then COMPACT_BEFORE_SNAPSHOT=false SNAPSHOT_FORMAT=iceberg"
    results="${QB_RESULTS_PREFIX}/${STAMP}-${SUITE}$([[ "$SUITE" == snapshot && "$SNAPSHOT_FORMAT" != both ]] && echo "-${SNAPSHOT_FORMAT}")/"
    export QB_RESULTS_PREFIX_RUN="$results"
    step+=$'\n''rc=$?; aws s3 sync bench-results "s3://${BUCKET_NAME}/'"${results}"'" --only-show-errors \
  && echo "status: results-uploaded '"${results}"'" || echo "status: results-upload-failed"; exit $rc'
    wait_for_runner                                                          # 0
    wait_for_watcher; delete_job "$WATCHER"                                  # 1
    if [[ "$SUITE" == snapshot ]]; then                                      # 2
        qb_stop_generator; qb_stop_writers
        qb_resolve_snapshot_dt; export SNAPSHOT_DT="$QB_SNAPSHOT_DT"
        echo "  snapshot partition: ${SNAPSHOT_DT}"
        if [[ "${COMPACT_BEFORE_SNAPSHOT:-false}" == true ]]; then           # 2b
            submit_compaction
            # TODO(paimon-compaction): no one-off FULL compaction of the Paimon
            # tables exists yet. Paimon's Iceberg export lists only top-LSM-level
            # files, so until one runs, pmn_as_iceberg / DuckDB paimon may read
            # fewer rows than native Paimon (summary: INVALID on S1/S7). A batch
            # Flink job running CALL sys.compact(`table` => 'flink_paimon_db.<t>',
            # compact_strategy => 'full') per table, on the writers' image, would
            # close it (see throughput-benchmark/compaction/ for the FlinkDeployment shape).
        fi
        if [[ "$SNAPSHOT_FORMAT" == iceberg || ( "$SNAPSHOT_FORMAT" == both && "${COMPACT_BEFORE_SNAPSHOT:-false}" != true ) ]]; then   # 2c
            ( qb_compaction_gate 0 ) || die "the Iceberg phase cannot start"
        fi
    fi
    [[ "$ENGINES" == *starrocks* ]] && starrocks_up                          # 3
    [[ "$ENGINES" == *duckdb* ]] && duckdb_up
    if [[ "$SUITE" == snapshot ]]; then                                      # 3b
        # The session's catalogs (suffixed per table set) may not exist yet on this FE.
        "${HERE}/starrocks.sh" catalogs >/dev/null || die "cannot create or read the StarRocks catalogs"
        qb_resolve_snapshot_slice
        export S_FROM="$QB_S_FROM" S_TO="$QB_S_TO"
        echo "  snapshot slice (S4, orders.order_date): [${S_FROM}, ${S_TO}) (${QB_S_HOW})"
    fi
    if [[ "$SUITE" == snapshot ]]; then                                      # 4
        echo "snapshot: each phase empties the caches itself, after its gate"
    elif [[ "$STARTED_FRESH" == *starrocks* && "$STARTED_FRESH" == *duckdb* ]]; then
        echo "hot: both engines started fresh: caches already empty"
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
    local d step arm t T e s v n f
    d="${RENDER_DIR:-$(mktemp -d)}"; mkdir -p "$d"
    step=$(suite_step)
    KUBECTL_VERSION="${KUBECTL_VERSION:-v1.35.6}"
    write_cmd "$step"; cp "$CMD_FILE" "${d}/qb-runner-cmd.sh"; bundle "${d}/bundle.tgz"; rm -f "$CMD_FILE"
    ( export KUBECTL_VERSION; render_job "$RUNNER" >/dev/null && render_job "$WATCHER" >/dev/null ) || die "manifest render failed"
    mv "${HERE}/${RUNNER}-deployed.yaml" "${HERE}/${WATCHER}-deployed.yaml" "$d/"
    qb_run_meta > "${d}/run-meta.txt"
    { echo "StarRocks arms: ${SR_ARMS_ALL}"; echo "StarRocks skipped: ${QB_SR_SKIPPED:-none}"; echo "DuckDB arms: ${DUCK_ARMS_ALL}"
      echo "results: s3://\$BUCKET_NAME/${QB_RESULTS_PREFIX}/  pins: s3://\$BUCKET_NAME/${QB_PIN_PREFIX}/"; } >> "${d}/run-meta.txt"
    "${HERE}/../engine-deploy/deploy-duckdb-cdc.sh" render >/dev/null && mv "${HERE}/../engine-deploy/duckdb-cdc-deployed.yaml" "$d/"
    # Sample pins and windows, or the ones set in this shell.
    for t in $QB_TABLES; do
        T=$(qb_upper "$t")
        for v in "QB_PIN_SNAP_PAIMON_${T}=101" "QB_PIN_SNAP_PMNICE_${T}=101" "QB_PIN_SNAP_ICEBERG_${T}=8000000000000000101" \
                 "QB_PIN_META_PAIMON_${T}=s3://${BUCKET_NAME}/${QB_PIN_PREFIX}/SAMPLE/paimon/${t}/v101.metadata.json" \
                 "QB_PIN_META_ICEBERG_${T}=s3://${BUCKET_NAME}/${QB_PIN_PREFIX}/SAMPLE/iceberg/${t}/00001-x.metadata.json"; do
            n="${v%%=*}"; [[ -n "${!n:-}" ]] || export "$v"
        done
    done
    qb_resolve_hot
    QB_SNAPSHOT_DT="${SNAPSHOT_DT:-2026-09-27}" QB_S_FROM="${S_FROM:-2026-09-27 12:00:00}" QB_S_TO="${S_TO:-2026-09-27 12:10:00}"
    QB_LOOKUP_ORDER_ID="${LOOKUP_ORDER_ID:-12345}" QB_LOOKUP_ORDER_DT="${LOOKUP_ORDER_DT:-2026-09-27}"
    for s in hot snapshot; do
        [[ -d "${QB_DIR}/${s}" ]] || { warn "no ${s}/ queries yet"; continue; }
        for e in starrocks duckdb; do
            for arm in $([[ "$e" == starrocks ]] && echo "$SR_ARMS_ALL" || echo "$DUCK_ARMS_ALL"); do
                for f in $(qb_query_files "$s"); do
                    { echo "-- ${e}/${arm}/$(qb_query_name "$f")"
                      if [[ "$e" == starrocks ]]; then qb_render_oneline "$f" "$e" "$arm"; else qb_render "$f" "$e" "$arm"; fi
                    } >> "${d}/sql-${s}-${e}-${arm}.sql" || die "render failed: ${s}/${e}/${arm}/$(basename "$f")"
                done
            done
        done
    done
    echo "rendered into ${d}:"; ls -1 "$d" | sed 's/^/  /'
}

cmd_status() {
    echo "== session: SESSION_SUFFIX='${SESSION_SUFFIX}' results: s3://\$BUCKET_NAME/${QB_RESULTS_PREFIX}/"
    echo "== table set: Iceberg v${ICEBERG_FORMAT_VERSION} CDC_DB_SUFFIX='${CDC_DB_SUFFIX}' (${PAIMON_DB}, ${ICEBERG_DB}); StarRocks arms ${SR_ARMS_ALL}${QB_SR_SKIPPED:+ (skipped: ${QB_SR_SKIPPED})}; image $(starrocks_tags)"
    echo "== writers:";   kubectl get flinkdeployment $FLINK_JOBS -n "$NAMESPACE" -o custom-columns='  NAME:.metadata.name,SPEC:.spec.job.state,JOB:.status.jobStatus.state' --no-headers 2>/dev/null || echo "  none"
    echo "== generator: $(qb_generator_state)"
    echo "== StarRocks: FE $(ready_count fe) / BE $(ready_count be) ready"
    echo "== ${DUCK}: $(duckdb_health | cut -c1-160)"
    echo "== jobs:";      kubectl get jobs "$RUNNER" "$WATCHER" qb-runner qb-watch -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print "  " $1, $2}' || true
}

# Newest hot and snapshot run of the session from S3 (HOT_STAMP= / SNAPSHOT_STAMP=
# pick others; SUMMARY_DIR=<local dir> skips S3) -> the Markdown summary.
cmd_summary() {
    local S T hot snap session out
    S="s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}"
    T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT
    latest() { aws s3 ls "${S}/" 2>/dev/null | sed -n "s|.* PRE \([0-9]\{8\}T[0-9]\{6\}Z\)-${1}/\$|\1|p" | sort | tail -1; }
    if [[ -n "${SUMMARY_DIR:-}" ]]; then   # a local copy: <dir>/hot/ and/or <dir>/snapshot/
        cp -R "${SUMMARY_DIR}/." "$T/"; hot="${HOT_STAMP:-}"; snap="${SNAPSHOT_STAMP:-}"
    else
        hot="${HOT_STAMP:-$(latest hot)}"
        [[ -z "$hot" ]] || aws s3 sync --only-show-errors "${S}/${hot}-hot/" "$T/hot/"
        # The snapshot suite: one run of both phases (<STAMP>-snapshot/), or the
        # newest Paimon phase and the newest Iceberg phase run on their own
        # (<STAMP>-snapshot-paimon/, <STAMP>-snapshot-iceberg/), whichever is newer.
        local sb sp si x
        sb="${SNAPSHOT_STAMP:-$(latest snapshot)}"; sp="${SNAPSHOT_PAIMON_STAMP:-$(latest snapshot-paimon)}"; si="${SNAPSHOT_ICEBERG_STAMP:-$(latest snapshot-iceberg)}"
        if [[ -n "$sb" && ( -n "${SNAPSHOT_STAMP:-}" || ( ! "$sp" > "$sb" && ! "$si" > "$sb" ) ) ]]; then
            aws s3 sync --only-show-errors "${S}/${sb}-snapshot/" "$T/snapshot/"; snap="$sb"
        else
            for x in ${sp:+"${sp}-snapshot-paimon"} ${si:+"${si}-snapshot-iceberg"}; do
                aws s3 sync --only-show-errors "${S}/${x}/" "$T/snapshot/"
            done
            snap="${sp}${sp:+${si:+,}}${si}"
        fi
        [[ -n "$hot$snap" ]] || die "no runs under ${QB_RESULTS_PREFIX}/"
    fi
    session="${SESSION:-$(echo "${hot:-${snap:-$(date -u +%Y%m%d)}}" | cut -c1-8)${CDC_SET_TAG}${SESSION_SUFFIX}}"
    out="${CDC_DIR}/bench-results/QUERYBENCH-SUMMARY-${session}.md"
    mkdir -p "${CDC_DIR}/bench-results"
    python3 "${HERE}/summarize.py" --work "$T" --session "$session" ${hot:+--hot "$hot"} ${snap:+--snapshot "$snap"} \
        --must-match "${MUST_MATCH:-S1_order_items_count S7_dedup_keys}" \
        --iceberg-format-version "$ICEBERG_FORMAT_VERSION" --out "$out"
    [[ -z "${AWS_ACCOUNT_ID:-}" ]] || sed -i.bak -e "s/${BUCKET_NAME}/<bucket>/g" -e "s/${AWS_ACCOUNT_ID}/<id>/g" "$out"
    rm -f "$out.bak"
    log "summary: ${out#${REPO_DIR}/} (bench-results/ is git-ignored: git add -f to commit it)"
}

case "${1:-}" in
    start)   SUITE="${SUITE:-}"; [[ "$SUITE" == all ]] && die "set SUITE=hot or SUITE=snapshot"; cmd_start ;;
    render)  [[ "$SUITE" == all ]] && SUITE=hot; cmd_render ;;
    logs)    kubectl logs -f -n "$NAMESPACE" "job/$([[ "${2:-}" == watch ]] && echo "$WATCHER" || echo "$RUNNER")" ;;
    status)  cmd_status ;;
    summary) cmd_summary ;;
    *)       sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
