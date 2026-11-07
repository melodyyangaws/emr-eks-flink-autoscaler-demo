#!/usr/bin/env bash
# The CDC query benchmark: DuckDB vs Trino on the tables Flink writes in both
# formats (four arms, ENGINES x FORMATS: Iceberg natively, Paimon through its
# Iceberg v3 export, or natively with PAIMON_READER=native), run from INSIDE the cluster with a watcher that shuts the
# run down when the test ends. Run from cdc-pipeline/.
#
#   SUITE=hot      ./querybench/run.sh start   hot test (hot-interleave.sh), writers + generator LIVE
#   SUITE=snapshot ./querybench/run.sh start   snapshot test (snapshot.sh): writers stopped, both
#                                              engines from a cold start
#   ENGINES=duckdb|trino SUITE=... start       one engine only (default: "duckdb trino")
#   FORMATS=iceberg|paimon SUITE=... start     one storage format only (default: both)
#   PAIMON_READER=native SUITE=... start       paimon arms on the native readers (default: iceberg, the export)
#   ./querybench/run.sh logs [watch]           follow the runner (watch: the watcher)
#   ./querybench/run.sh status                 writers, generator, engines, Jobs
#   SUITE=hot ./querybench/run.sh stop         tear the run down by hand (generator, writers,
#                                              monitor, DuckDB, Trino): the watcher only does
#                                              it itself after a COMPLETED test
#   SUITE=hot ./querybench/run.sh render       DRY RUN: render the Job manifests, the step and
#                                              every query for all four arms with sample
#                                              values; no cluster or AWS call
#   ./querybench/run.sh summary                newest hot + snapshot run of the session, plus the
#                                              Flink ingest over its window (ingest_stats.py) ->
#                                              bench-results/QUERYBENCH-SUMMARY-<session>.md
#
# Results: the runner uploads its bench-results/ to
# s3://$BUCKET_NAME/querybench-results/cdc-session<set tag>/<STAMP>-<suite>/
# (set tag: .bench-env's SESSION_SUFFIX with - for _, e.g. _v3r1 -> -v3r1).
#
# `start`, in order (any failure stops it before a Job is created):
#   0. waits while cdc-qb-runner still runs (FORCE=1: replace it); refuses while
#      throughput-benchmark's qb-runner runs (same cluster and bucket);
#   1. waits for the old cdc-qb-watch to finish its shutdown, then deletes it;
#   2. snapshot: refuses unless the hot watcher's compaction is over and every
#      format/table has an OK result newer than the latest hot run
#      (check_compaction_done; REQUIRE_COMPACTION=0 skips it), scales the generator to 0, waits WRITER_DRAIN_S (180) for the
#      writers to read the binlog tail and until each completed a checkpoint >= 60 s
#      after the generator stopped (qb_wait_writers_committed; refuses otherwise),
#      suspends both writers, resolves
#      SNAPSHOT_DT (newest closed date in both fact tables of every format);
#      (No compaction here: the hot test's watcher runs the one-off compaction
#      of both formats right after its teardown, and step 1 waits for that
#      watcher, as long as a cdc-compact-* job runs.)
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
PASS_VARS="STAMP RUNS WARMUP SNAPSHOT_RUNS ALLOW_CONCURRENT ENGINES FORMATS PROFILE_SCAN QB_SETUP_ENGINE SKIP_PUFFIN_VERIFICATION_ARMS PAIMON_READER PMNICE_ROOT
HOT_TO HOT_FROM HOT_DT HOT_WINDOW_SECONDS HOT_LAG_SECONDS HOT_COMMIT_MARGIN_S HOT_COMMIT_TABLES HOT_REPIN
STABLE_CHECKS WAIT_COMMIT_S HOT_DATA_WAIT_S LOOKUP_ORDER_ID LOOKUP_ORDER_DT QB_QUERY_WRITERS
SNAPSHOT_DT S_FROM S_TO SNAPSHOT_SLICE_MINUTES SNAPSHOT_SLICE_MARGIN_S
WATCH_INTERVAL_S SUSPEND_WRITERS STOP_GENERATOR STOP_TRINO STOP_DUCKDB COMPACT_AFTER_HOT COMPACT_TABLES COMPACT_PARALLELISM COMPACT_TIMEOUT_S ICEBERG_BIG_TM_TABLES ICEBERG_BIG_TM_MEMORY ICEBERG_MAX_GROUP_DELETE_KEYS
HOT_QUERY_TIMEOUT_S SNAPSHOT_QUERY_TIMEOUT_S MAX_RESULT_ROWS
ENGINE_GATE_TIMEOUT_S
QB_RESULTS_PREFIX_RUN TRINO_VERSION TRINO_WORKERS TRINO_USER TRINO_CATALOG CLEAR_COORDINATOR"
# Passed even when set to an empty value: the pod's .bench-env has non-empty
# defaults for them (the v3 set), so `SESSION_SUFFIX= ...` must reach the pod as such.
PASS_VARS_EMPTY_OK="ICEBERG_FORMAT_VERSION SESSION_SUFFIX DUCKDB_IMAGE"
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
        -C "$CDC_DIR" -czf "$1" querybench lib engine-deploy compaction .bench-env .qb-runner-cmd.sh 2>/dev/null \
    || COPYFILE_DISABLE=1 tar --exclude='*-deployed.yaml' --exclude='__pycache__' \
        -C "$CDC_DIR" -czf "$1" querybench lib engine-deploy compaction .bench-env .qb-runner-cmd.sh
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
    local deadline=$(( $(date +%s) + ${WATCH_INTERVAL_S:-300} + 1200 ))
    while job_active "$WATCHER"; do
        [[ "${FORCE:-0}" == 1 ]] && { warn "FORCE=1: deleting ${WATCHER} before its shutdown"; return 0; }
        # The hot watcher's one-off compaction (compaction/run.sh, cdc-compact-*)
        # can take hours: wait for it, the deadline only bounds the teardown.
        if kubectl get flinkdeployment -n "$NAMESPACE" -o name 2>/dev/null | grep -q '/cdc-compact-'; then
            deadline=$(( $(date +%s) + 1200 ))
            log "${WATCHER} is compacting before the snapshot test ($("${CDC_DIR}/compaction/run.sh" status 2>/dev/null | tr -s ' ' | paste -sd ';' -)); waiting (every 60 s)"
            sleep 60; continue
        fi
        (( $(date +%s) < deadline )) || { warn "${WATCHER} still running; deleting it"; return 0; }
        log "${WATCHER} is still shutting the last test down; waiting (every 30 s)"
        sleep 30
    done
}

# The snapshot test reads what the hot watcher's one-off compaction left, so it
# starts only once that compaction is over and every table compacted OK: on
# _readonlytest (2026-11-05) the start went ahead after the watcher finished
# although Iceberg orders had FAILED, order_items was never run, and a
# restarted cdc-compact-iceberg-orders was still rewriting. Waits while a
# cdc-compact-* FlinkDeployment is not FAILED/FINISHED (COMPACT_WAIT_TIMEOUT_S,
# 14400); refuses if one FAILED (fix it, ./compaction/run.sh down, re-run the
# table) or if any format/table of COMPACT_TABLES has no OK result newer than
# the latest hot run. REQUIRE_COMPACTION=0 skips the check.
check_compaction_done() {
    [[ "${REQUIRE_COMPACTION:-1}" == 0 ]] && { warn "REQUIRE_COMPACTION=0: not checking the pre-snapshot compaction"; return 0; }
    local deadline=$(( $(date +%s) + ${COMPACT_WAIT_TIMEOUT_S:-14400} )) busy failed hot res missing="" f t
    while :; do
        busy=""; failed=""
        while read -r n st; do
            [[ -n "$n" ]] || continue
            case "$st" in FAILED) failed+=" ${n}" ;; FINISHED) ;; *) busy+=" ${n}(${st:-starting})" ;; esac
        done < <(kubectl get flinkdeployment -n "$NAMESPACE" --no-headers \
                    -o custom-columns='N:.metadata.name,S:.status.jobStatus.state' 2>/dev/null | grep '^cdc-compact-')
        [[ -n "$failed" ]] && die "compaction FAILED:${failed} (./compaction/run.sh logs <fmt> <table>; fix, ./compaction/run.sh down, re-run the table; REQUIRE_COMPACTION=0 skips this check)"
        [[ -z "$busy" ]] && break
        (( $(date +%s) < deadline )) || die "compaction still running after ${COMPACT_WAIT_TIMEOUT_S:-14400}s:${busy}"
        log "compaction still running:${busy}; waiting (every 60 s)"
        sleep 60
    done
    res="s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}/compaction/"
    hot=$(aws s3 ls "s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}/" 2>/dev/null | sed -n 's|.* PRE \([0-9]\{8\}T[0-9]\{6\}Z\)-hot/$|\1|p' | sort | tail -1)
    for f in paimon iceberg; do
        for t in ${COMPACT_TABLES:-customers products orders order_items}; do
            # newest result of this format/table (stamps sort as text), newer than the hot run
            local key ok
            key=$(aws s3 ls "$res" 2>/dev/null | awk '{print $4}' | grep -E "^[0-9]{8}T[0-9]{6}Z-${f}-${t}\.json$" | sort | tail -1)
            if [[ -z "$key" || ( -n "$hot" && "${key%%-*}" < "$hot" ) ]]; then missing+=" ${f}/${t}(no run after hot ${hot:-?})"; continue; fi
            ok=$(aws s3 cp --only-show-errors "${res}${key}" - 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status",""))' 2>/dev/null)
            [[ "$ok" == OK ]] || missing+=" ${f}/${t}(${key%%-*} ${ok:-unreadable})"
        done
    done
    [[ -z "$missing" ]] || die "pre-snapshot compaction incomplete:${missing}; re-run those tables (./compaction/run.sh run <fmt> with COMPACT_TABLES=...) or REQUIRE_COMPACTION=0"
    log "compaction OK: every table of both formats compacted after the hot run ${hot:-?}"
}

duckdb_health() {
    kubectl rollout status -n "$NAMESPACE" "deploy/${DUCK}" --timeout=5s >/dev/null 2>&1 || return 0
    qb_duck_health
}

STARTED_FRESH=""
duckdb_up() {
    local h; h=$(duckdb_health)
    # A server deployed for the other table set, from another image or with other
    # skip-puffin arms (PAIMON_READER=iceberg needs `paimon`) is redeployed.
    if [[ -n "$h" ]] && ! python3 -c '
import json, sys
h, fv, img, sp = json.loads(sys.argv[1]), sys.argv[2], sys.argv[3], sorted(sys.argv[4].split())
sys.exit(0 if str(h.get("iceberg_format_version", 3)) == fv and (h.get("image") or "pip") == (img or "pip")
         and sorted(h.get("skip_puffin_verification_arms") or []) == sp else 1)' \
        "$h" "${ICEBERG_FORMAT_VERSION}" "${DUCKDB_IMAGE:-}" "${SKIP_PUFFIN_VERIFICATION_ARMS-$(qb_skip_puffin_default)}" 2>/dev/null; then
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

# The CDC job monitor (monitoring/, Deployment flink-cdc-monitor) during a hot
# test: its reports (CDC events/s in and out, checkpoints, backpressure, source
# lag) feed the summary's ingest section. The watcher's teardown scales it to
# 0, so each hot start brings it back: deploy-monitor.sh when it does not exist,
# else scale to 1. A monitor configured for another table set, or without the
# S3 report copy (REPORT_S3_URI, from before it existed), is redeployed.
# MONITOR_UP=0 leaves it alone. Never fails the start.
monitor_up() {
    local m="${CDC_MONITOR:-flink-cdc-monitor}" db uri
    [[ "${MONITOR_UP:-1}" == 0 ]] && { echo "  MONITOR_UP=0: job monitor left as it is"; return 0; }
    db=$(kubectl get configmap flink-cdc-monitor-config -n "$NAMESPACE" -o jsonpath='{.data.ICEBERG_GLUE_DB}' 2>/dev/null || true)
    uri=$(kubectl get configmap flink-cdc-monitor-config -n "$NAMESPACE" -o jsonpath='{.data.REPORT_S3_URI}' 2>/dev/null || true)
    if ! kubectl get deployment "$m" -n "$NAMESPACE" >/dev/null 2>&1 || [[ "$db" != "$ICEBERG_DB" || -z "$uri" ]]; then
        echo "  job monitor: (re)deploying (db '${db:-none}', report copy '${uri:-none}'; want ${ICEBERG_DB} + S3)"
        SESSION_SUFFIX="$SESSION_SUFFIX" "${CDC_DIR}/monitoring/deploy-monitor.sh" >/dev/null \
            || warn "job monitor deploy failed: the summary will have no checkpoint / backpressure / events-consumed rows"
    elif kubectl scale "deployment/${m}" -n "$NAMESPACE" --replicas=1 >/dev/null; then
        echo "  job monitor: ${m} scaled to 1 (reports to ${uri})"
    else
        warn "could not scale ${m} to 1: the summary will have no job-monitor rows"
    fi
}

suite_step() {
    case "$SUITE" in
        hot)      echo './querybench/hot-interleave.sh' ;;
        snapshot) echo './querybench/snapshot.sh' ;;
        *)        die "set SUITE=hot or SUITE=snapshot" ;;
    esac
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
        check_compaction_done                                                # the hot watcher's compaction is over and OK
        gen_stopped_at=$(date +%s)
        qb_stop_generator
        qb_wait_writers_committed "$gen_stopped_at" || die "a writer did not commit after the generator stopped (see above): its tables would not match MySQL"
        qb_stop_writers
        qb_resolve_snapshot_dt; export SNAPSHOT_DT="$QB_SNAPSHOT_DT"
        echo "  snapshot partition: ${SNAPSHOT_DT}"
    fi
    if [[ "$SUITE" == hot ]]; then                                           # 2c
        wait_writers_up "${WRITERS_UP_S:-1800}"                              # writers 30 min up before the engines
        monitor_up                                                           # job-monitor reports for the summary
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
                 "QB_PIN_META_PAIMON_${T}=$([[ "$PAIMON_READER" == native ]] && echo "${PAIMON_WAREHOUSE}/${PAIMON_DB}.db/${t}" \
                     || echo "s3://${BUCKET_NAME}/${QB_PIN_PREFIX}/SAMPLE/paimon/${t}/v101.metadata.json")"; do
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
    echo "== session: results s3://\$BUCKET_NAME/${QB_RESULTS_PREFIX}/"
    echo "== table set: Iceberg v${ICEBERG_FORMAT_VERSION} SESSION_SUFFIX='${SESSION_SUFFIX}' (${ICEBERG_DB}, ${PAIMON_DB}); engines $(qb_engines); formats $(qb_formats)"
    echo "== writers:";   kubectl get flinkdeployment $FLINK_JOBS -n "$NAMESPACE" -o custom-columns='  NAME:.metadata.name,SPEC:.spec.job.state,JOB:.status.jobStatus.state' --no-headers 2>/dev/null || echo "  none"
    echo "== generator: $(qb_generator_state)"
    echo "== ${DUCK}: $(duckdb_health | cut -c1-160)"
    echo "== Trino: $(qb_trino_workers || true) of ${TRINO_WORKERS} workers active$([[ -x "$DEPLOY_TRINO" ]] && echo " (details: ${DEPLOY_TRINO#${CDC_DIR}/} status)")"
    echo "== jobs:";      kubectl get jobs "$RUNNER" "$WATCHER" qb-runner qb-watch -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print "  " $1, $2}' || true
}

# Flink ingest over the run's window, from the tables' commit history
# (ingest_stats.py, read-only boto3; needs the snapshots still retained, 12 h on
# Paimon): the hot suite's pins -5 min .. last pin, else the writers' last
# 30 min before the snapshot run. Sets INGEST_JSON, or INGEST_ERR (the summary
# then says why). Never fails the summary. Through `uv` when present, which
# brings fastavro + zstd for Paimon's Avro manifests (file counts); plain
# python3 otherwise (rows and commits only). INGEST=0 skips it.
summary_ingest() {   # <work dir> <hot stamp> <snapshot stamp> <json out>
    local w="$1" hot="$2" snap="$3" out="$4" pins="" win py err host
    INGEST_JSON="" INGEST_ERR=""
    [[ "${INGEST:-1}" == 0 ]] && { INGEST_ERR="skipped (INGEST=0)"; return 0; }
    pins=$(ls "$w"/hot/hot-interleave-*.pins 2>/dev/null | tail -1 || true)
    if [[ -n "$hot" && -n "$pins" ]]; then win=(--pins "$pins")
    elif [[ -n "$snap" ]]; then win=(--before "$snap")
    else INGEST_ERR="no hot .pins file and no snapshot run to take a window from"; return 0; fi
    if command -v uv >/dev/null 2>&1; then
        py=(uv run --quiet --no-project --python 3.12 --with boto3 --with fastavro --with zstandard --with backports.zstd python)
    else py=(python3); fi
    host="${MYSQL_HOST:-}"   # .bench-env.local
    err="$w/ingest.err"
    if "${py[@]}" "${HERE}/ingest_stats.py" --bucket "$BUCKET_NAME" --iceberg-db "$ICEBERG_DB" --paimon-db "$PAIMON_DB" \
           "${win[@]}" --monitor-prefix "${QB_RESULTS_PREFIX}/monitor/" ${host:+--rds-host "$host"} \
           --flink-manifests "${CDC_DIR}/ingest-deploy/flink-cdc-paimon-deployed.yaml" "${CDC_DIR}/ingest-deploy/flink-cdc-iceberg-deployed.yaml" \
           --generator-manifest "${CDC_DIR}/mysql-data-generator/mysql-upsert-loadgen.yaml" \
           --out "$out" 2> "$err" && [[ -s "$out" ]]; then
        INGEST_JSON="$out"; log "ingest: $(tail -1 "$err" | sed "s|${REPO_DIR}/||g" | cut -c1-300)"
    else
        INGEST_ERR="ingest_stats.py failed: $(grep -v '^\s*$' "$err" | tail -1 | cut -c1-200)"
        warn "$INGEST_ERR (the summary goes on without it)"
    fi
}

# Newest hot and snapshot run of the session from S3 (HOT_STAMP= / SNAPSHOT_STAMP=
# pick others; SUMMARY_DIR=<local dir with hot/ and/or snapshot/> skips S3) -> the Markdown summary.
cmd_summary() {
    local S T hot snap session out
    S="s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}"
    T=$(mktemp -d); trap "rm -rf '${T}'" EXIT   # T is local: expand it now, not at exit
    latest() { aws s3 ls "${S}/" 2>/dev/null | sed -n "s|.* PRE \([0-9]\{8\}T[0-9]\{6\}Z\)-${1}/\$|\1|p" | sort | tail -1; }
    if [[ -n "${SUMMARY_DIR:-}" ]]; then
        cp -R "${SUMMARY_DIR}/." "$T/"; hot="${HOT_STAMP:-}"; snap="${SNAPSHOT_STAMP:-}"
    else
        hot="${HOT_STAMP:-$(latest hot)}"
        [[ -z "$hot" ]] || aws s3 sync --only-show-errors "${S}/${hot}-hot/" "$T/hot/"
        snap="${SNAPSHOT_STAMP:-$(latest snapshot)}"
        [[ -z "$snap" ]] || aws s3 sync --only-show-errors "${S}/${snap}-snapshot/" "$T/snapshot/"
        aws s3 sync --only-show-errors "${S}/compaction/" "$T/compaction/" 2>/dev/null || true   # compaction/run.sh results
        [[ -n "$hot$snap" ]] || die "no runs under ${QB_RESULTS_PREFIX}/"
    fi
    session="${SESSION:-$(echo "${hot:-${snap:-$(date -u +%Y%m%d)}}" | cut -c1-8)${CDC_SET_TAG}}"
    out="${CDC_DIR}/bench-results/QUERYBENCH-SUMMARY-${session}.md"
    mkdir -p "${CDC_DIR}/bench-results"
    # Raw ingest numbers go to S3 next to the run's other results (throughput's
    # pattern: bench-results/ keeps only the final .md; everything the summary
    # was computed from is in S3, so another laptop can reproduce or re-read it).
    ingest_json="$T/QUERYBENCH-INGEST-${session}.json"
    summary_ingest "$T" "$hot" "$snap" "$ingest_json"
    # Engine shape at summary time: fills only what a run's own metadata lacks
    # (runs before 2026-11-07 did not record it).
    local shape; shape=$(qb_engine_shape 2>/dev/null || true)
    python3 "${HERE}/summarize.py" --work "$T" --session "$session" ${hot:+--hot "$hot"} ${snap:+--snapshot "$snap"} \
        ${shape:+--engine-shape "$shape"} \
        --iceberg-format-version "$ICEBERG_FORMAT_VERSION" --warmup "${WARMUP:-1}" \
        ${INGEST_JSON:+--ingest "$INGEST_JSON"} ${INGEST_ERR:+--ingest-error "$INGEST_ERR"} --out "$out"
    [[ -z "${AWS_ACCOUNT_ID:-}" ]] || sed -i.bak -e "s/${BUCKET_NAME}/<bucket>/g" -e "s/${AWS_ACCOUNT_ID}/<id>/g" "$out"
    rm -f "$out.bak"
    if [[ -s "$ingest_json" ]]; then
        aws s3 cp --only-show-errors "$ingest_json" "${S}/${session}-summary/QUERYBENCH-INGEST-${session}.json" \
            && log "ingest data: s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}/${session}-summary/QUERYBENCH-INGEST-${session}.json" \
            || warn "could not upload the ingest JSON to S3 (kept only in $T, which is removed on exit)"
    fi
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
