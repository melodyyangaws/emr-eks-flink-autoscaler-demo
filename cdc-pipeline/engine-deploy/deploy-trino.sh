#!/usr/bin/env bash
# The CDC query benchmark's Trino (chart trinodb/trino, release "trino",
# Service trino:8080 in NAMESPACE), reading the Iceberg tables through Glue.
#
#   ./engine-deploy/deploy-trino.sh render          write trino-values-deployed.yaml only (no cluster change)
#   ./engine-deploy/deploy-trino.sh up              wait until the writers have run 30 min (TRINO_AFTER_WRITERS_S),
#                                                   helm upgrade --install, wait for all workers, print version()
#   ./engine-deploy/deploy-trino.sh status          pods, version, active nodes, SHOW SCHEMAS FROM iceberg, cache use
#   ./engine-deploy/deploy-trino.sh clear-cache     empty /cache on every node and restart them (cold start)
#   ./engine-deploy/deploy-trino.sh down            helm uninstall + delete the cache PVCs
#   ./engine-deploy/deploy-trino.sh sql '<SQL>'     run one statement on the coordinator (TSV, no header)
#   ./engine-deploy/deploy-trino.sh qstats <id>     server-side timing of one query (TSV with header)
#
# Versions: Trino 484 (2026-10-15) = chart 1.45.0 (`helm search repo
# trino/trino --versions`: 1.45.0 -> appVersion 484; 1.44.0 -> 483). The
# image tag is pinned as well. Iceberg format v3: the 484 Iceberg connector
# reads v3 (deletion vectors, equality deletes) with no extra property; its
# docs call v3 support experimental. See README.md (Trino).
#
# Sizing (resource parity with duckdb-cdc.yaml: 27 cpu / 114 Gi on one
# m5.8xlarge): TRINO_WORKERS x TRINO_WORKER_CPU cpu / TRINO_WORKER_MEMORY_GI Gi
# (3 x 9 / 38 = 27 cpu / 114 Gi; requests = limits), one worker per
# m5.4xlarge, plus a 4 cpu / 16 Gi coordinator on an m5.2xlarge (an m5.xlarge
# cannot fit 4 cpu) that runs no splits. Per node:
#   heap               = TRINO_HEAP_PCT (80) % of the container memory  (-Xmx)
#   max-memory-per-node = TRINO_QMEM_PCT (65) % of the heap
#   heap-headroom      = TRINO_HEADROOM_PCT (30) % of the heap   (65 + 30 <= 100, as Trino requires)
#   query.max-memory   = TRINO_WORKERS x worker max-memory-per-node
# Cache: Trino's file system cache (alluxio cache manager, node-level since
# 484) on a generic ephemeral gp3-fast volume per pod at /cache, capped at
# TRINO_CACHE_DISK_PCT % of it: 3 x 167 Gi x 90 % ~ 450 GiB = DuckDB's
# on-disk cache. The volume lives and dies with its pod, so a restart starts
# cold; clear-cache also empties it first.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${HERE}/../lib/common.sh"

RELEASE=trino                      # = the chart name, so the Service is "trino"
TRINO_VERSION="${TRINO_VERSION:-484}"
TRINO_CHART_VERSION="${TRINO_CHART_VERSION:-1.45.0}"
# The chart archive by URL (index: https://trinodb.github.io/charts), not by
# repo name: no `helm repo add` needed, and no dependence on the local repo cache.
TRINO_CHART="${TRINO_CHART:-https://github.com/trinodb/charts/releases/download/trino-${TRINO_CHART_VERSION}/trino-${TRINO_CHART_VERSION}.tgz}"
# Image: trinodb/trino:${TRINO_VERSION} + the Paimon connector, built by
# trino-image/build-trino-image.sh (see trino-image/Dockerfile). The harness no
# longer configures a paimon catalog (Paimon is read as Paimon-as-iceberg through
# the iceberg catalog), so the bundled connector goes unused. TRINO_IMAGE=
# (empty) = the stock trinodb/trino:${TRINO_VERSION}.
TRINO_IMAGE="${TRINO_IMAGE-${AWS_ACCOUNT_ID:+${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/trino-cdc:${TRINO_VERSION}-paimon-2.1.0}}"
if [[ -n "$TRINO_IMAGE" ]]; then
    TRINO_IMAGE_REPOSITORY="${TRINO_IMAGE%:*}" TRINO_IMAGE_TAG="${TRINO_IMAGE##*:}"
else
    TRINO_IMAGE_REPOSITORY=trinodb/trino TRINO_IMAGE_TAG="$TRINO_VERSION"
fi
[[ "$TRINO_IMAGE_TAG" == "$TRINO_VERSION" || "$TRINO_IMAGE_TAG" == "${TRINO_VERSION}-"* ]] \
    || die "TRINO_IMAGE tag ${TRINO_IMAGE_TAG} is not Trino ${TRINO_VERSION}"
TRINO_SERVICE_ACCOUNT="${TRINO_SERVICE_ACCOUNT:-starrocks-sa}"   # = duckdb-cdc.yaml: IRSA role reading Glue + both warehouses
TRINO_NODEPOOL="${TRINO_NODEPOOL:-driver-nodepool}"              # = duckdb-cdc.yaml (on-demand, m5 allowed)

TRINO_WORKERS="${TRINO_WORKERS:-3}"
TRINO_WORKER_CPU="${TRINO_WORKER_CPU:-9}"
TRINO_WORKER_MEMORY_GI="${TRINO_WORKER_MEMORY_GI:-38}"
TRINO_WORKER_INSTANCE_SIZE="${TRINO_WORKER_INSTANCE_SIZE:-4xlarge}"
TRINO_COORD_CPU="${TRINO_COORD_CPU:-4}"
TRINO_COORD_MEMORY_GI="${TRINO_COORD_MEMORY_GI:-16}"
TRINO_COORD_INSTANCE_SIZE="${TRINO_COORD_INSTANCE_SIZE:-2xlarge}"
TRINO_HEAP_PCT="${TRINO_HEAP_PCT:-80}"
TRINO_QMEM_PCT="${TRINO_QMEM_PCT:-65}"
TRINO_HEADROOM_PCT="${TRINO_HEADROOM_PCT:-30}"

CACHE_STORAGE_CLASS="${CACHE_STORAGE_CLASS:-gp3-fast}"
TRINO_CACHE_VOLUME="${TRINO_CACHE_VOLUME:-167Gi}"
TRINO_COORD_CACHE_VOLUME="${TRINO_COORD_CACHE_VOLUME:-20Gi}"
TRINO_CACHE_DISK_PCT="${TRINO_CACHE_DISK_PCT:-90}"
TRINO_CACHE_PAGE_SIZE="${TRINO_CACHE_PAGE_SIZE:-1MB}"           # Trino's default (64kB..15MB)
READY_GRACE="${READY_GRACE:-1200}"
# up waits until the writers have been RUNNING this long (wait_writers_up in
# lib/common.sh): their start-up and first snapshot get the nodes to themselves.
TRINO_AFTER_WRITERS_S="${TRINO_AFTER_WRITERS_S:-${WRITERS_UP_S:-1800}}"
CLEAR_COORDINATOR="${CLEAR_COORDINATOR:-1}"                     # clear-cache restarts the coordinator too (its metadata cache)
TRINO_USER="${TRINO_USER:-cdcbench}"

for v in TRINO_WORKERS TRINO_WORKER_MEMORY_GI TRINO_COORD_MEMORY_GI TRINO_HEAP_PCT TRINO_QMEM_PCT TRINO_HEADROOM_PCT TRINO_CACHE_DISK_PCT; do
    [[ "${!v}" =~ ^[1-9][0-9]*$ ]] || die "${v} must be a positive integer, got '${!v}'"
done
(( TRINO_QMEM_PCT + TRINO_HEADROOM_PCT <= 100 )) || die "TRINO_QMEM_PCT + TRINO_HEADROOM_PCT must be <= 100"
(( TRINO_CACHE_DISK_PCT <= 100 )) || die "TRINO_CACHE_DISK_PCT must be <= 100"

# Memory (MiB; Trino's MB is 1024^2, like -Xm<n>M).
heap_mib()     { echo $(( $1 * 1024 * TRINO_HEAP_PCT / 100 )); }
TRINO_WORKER_HEAP_MIB=$(heap_mib "$TRINO_WORKER_MEMORY_GI")
TRINO_COORD_HEAP_MIB=$(heap_mib "$TRINO_COORD_MEMORY_GI")
TRINO_WORKER_HEAP="${TRINO_WORKER_HEAP_MIB}M"
TRINO_COORD_HEAP="${TRINO_COORD_HEAP_MIB}M"
TRINO_WORKER_MAX_MEM_PER_NODE="$(( TRINO_WORKER_HEAP_MIB * TRINO_QMEM_PCT / 100 ))MB"
TRINO_WORKER_HEADROOM="$(( TRINO_WORKER_HEAP_MIB * TRINO_HEADROOM_PCT / 100 ))MB"
TRINO_COORD_MAX_MEM_PER_NODE="$(( TRINO_COORD_HEAP_MIB * TRINO_QMEM_PCT / 100 ))MB"
TRINO_COORD_HEADROOM="$(( TRINO_COORD_HEAP_MIB * TRINO_HEADROOM_PCT / 100 ))MB"
TRINO_QUERY_MAX_MEMORY="$(( TRINO_WORKERS * (TRINO_WORKER_HEAP_MIB * TRINO_QMEM_PCT / 100) ))MB"

TPL_VARS="TRINO_VERSION TRINO_CHART_VERSION TRINO_IMAGE_REPOSITORY TRINO_IMAGE_TAG AWS_REGION TRINO_SERVICE_ACCOUNT TRINO_NODEPOOL
TRINO_WORKERS TRINO_WORKER_CPU TRINO_WORKER_MEMORY_GI TRINO_WORKER_INSTANCE_SIZE TRINO_WORKER_HEAP
TRINO_WORKER_MAX_MEM_PER_NODE TRINO_WORKER_HEADROOM TRINO_COORD_CPU TRINO_COORD_MEMORY_GI
TRINO_COORD_INSTANCE_SIZE TRINO_COORD_HEAP TRINO_COORD_MAX_MEM_PER_NODE TRINO_COORD_HEADROOM
TRINO_QUERY_MAX_MEMORY CACHE_STORAGE_CLASS TRINO_CACHE_VOLUME TRINO_COORD_CACHE_VOLUME
TRINO_CACHE_DISK_PCT TRINO_CACHE_PAGE_SIZE"
VALUES_OUT="${HERE}/trino-values-deployed.yaml"
SEL_COORD="app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=coordinator"
SEL_WORKER="app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=worker"
CHART_ARGS=("$TRINO_CHART" -n "$NAMESPACE")

# helm with an empty, private repo config: the chart comes by URL, and helm 4
# otherwise fails on any stale entry in the user's repo cache ("no cached repo
# found ... prometheus-community-index.yaml") even for a URL chart.
_helm_home="$(mktemp -d)"; trap 'rm -rf "$_helm_home"' EXIT
helm_() { HELM_REPOSITORY_CONFIG="${_helm_home}/repositories.yaml" HELM_REPOSITORY_CACHE="${_helm_home}/cache" helm "$@"; }

# sql '<SQL>' [output format]: one statement through the coordinator pod's
# CLI; TSV (no header) unless a format is given (TSV_HEADER, CSV, JSON...).
# TRINO_SOURCE tags the query (system.runtime.queries.source), so its
# query_id can be looked up afterwards (README.md, Trino: timing).
sql() {
    kubectl exec -n "$NAMESPACE" "deploy/${RELEASE}-coordinator" -c trino-coordinator -- \
        trino --server http://localhost:8080 --user "$TRINO_USER" --source "${TRINO_SOURCE:-cdcbench}" --output-format "${2:-TSV}" --execute "$1"
}

worker_pods() { kubectl get pods -n "$NAMESPACE" -l "$SEL_WORKER" -o name; }

active_workers() {
    sql "SELECT count(*) FROM system.runtime.nodes WHERE NOT coordinator AND state = 'active'" 2>/dev/null | tr -d '"' || echo 0
}

wait_ready() {   # coordinator + deployments rolled out, then TRINO_WORKERS active workers
    local deadline=$(( $(date +%s) + READY_GRACE )) n
    for d in coordinator worker; do
        until kubectl rollout status -n "$NAMESPACE" "deploy/${RELEASE}-${d}" --timeout=30s >/dev/null 2>&1; do
            (( $(date +%s) < deadline )) || die "deploy/${RELEASE}-${d} not ready after ${READY_GRACE}s (kubectl describe pod -n ${NAMESPACE} -l app.kubernetes.io/instance=${RELEASE})"
            log "waiting for deploy/${RELEASE}-${d}..."
        done
    done
    until n=$(active_workers); [[ "$n" == "$TRINO_WORKERS" ]]; do
        (( $(date +%s) < deadline )) || die "${n:-0}/${TRINO_WORKERS} workers active after ${READY_GRACE}s"
        log "${n:-0}/${TRINO_WORKERS} workers active..."
        sleep 5
    done
}

cmd_render() {
    # shellcheck disable=SC2086
    render "${HERE}/trino-values.yaml" "$VALUES_OUT" $TPL_VARS || die "could not render trino-values.yaml"
    log "rendered ${VALUES_OUT##*/} (chart ${TRINO_CHART_VERSION}, image ${TRINO_IMAGE_REPOSITORY}:${TRINO_IMAGE_TAG}; workers ${TRINO_WORKERS} x ${TRINO_WORKER_CPU} cpu / ${TRINO_WORKER_MEMORY_GI}Gi, heap ${TRINO_WORKER_HEAP}, max-memory-per-node ${TRINO_WORKER_MAX_MEM_PER_NODE}, query.max-memory ${TRINO_QUERY_MAX_MEMORY})"
}

cmd_up() {
    wait_writers_up "$TRINO_AFTER_WRITERS_S"
    if ! kubectl get storageclass "$CACHE_STORAGE_CLASS" >/dev/null 2>&1; then
        [[ "$CACHE_STORAGE_CLASS" == gp3-fast ]] || die "storageclass ${CACHE_STORAGE_CLASS} not found"
        kubectl apply -f "${TB_DIR}/engine-deploy/gp3-fast-sc.yaml" >/dev/null
    fi
    kubectl get sa "$TRINO_SERVICE_ACCOUNT" -n "$NAMESPACE" >/dev/null 2>&1 \
        || die "serviceaccount ${TRINO_SERVICE_ACCOUNT} not found in ${NAMESPACE} (the IRSA ServiceAccount shared with the DuckDB server; set TRINO_SERVICE_ACCOUNT to use another)"
    cmd_render
    helm_ upgrade --install "$RELEASE" "${CHART_ARGS[@]}" -f "$VALUES_OUT" >/dev/null
    log "helm release ${RELEASE} applied; waiting for ${TRINO_WORKERS} workers"
    wait_ready
    log "$(sql 'SELECT version()' | tr -d '"') up: ${TRINO_WORKERS} workers, Service ${RELEASE}.${NAMESPACE}:8080"
}

cmd_status() {
    if ! kubectl get deploy "${RELEASE}-coordinator" -n "$NAMESPACE" >/dev/null 2>&1; then
        echo "${RELEASE}: not running"; return 0
    fi
    kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=${RELEASE}" -o wide
    echo "version: $(sql 'SELECT version()' 2>/dev/null | tr -d '"' || echo 'no answer (starting?)')"
    echo "nodes (node_id, coordinator, state):"
    sql "SELECT node_id, coordinator, state FROM system.runtime.nodes ORDER BY coordinator DESC, node_id" 2>/dev/null | tr -d '"' | sed 's/^/  /' || true
    echo "iceberg schemas (flink_*):"
    sql "SHOW SCHEMAS FROM iceberg" 2>/dev/null | tr -d '"' | grep '^flink_' | sed 's/^/  /' || echo "  (none / catalog error)"
    echo "cache (/cache) per pod:"
    for p in $(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=${RELEASE}" -o name); do
        echo "  ${p#pod/}: $(kubectl exec -n "$NAMESPACE" "$p" -- du -sh /cache 2>/dev/null | cut -f1)"
    done
}

cmd_clear_cache() {
    local p deps=("deploy/${RELEASE}-worker")
    kubectl get deploy "${RELEASE}-coordinator" -n "$NAMESPACE" >/dev/null 2>&1 || die "${RELEASE} is not up"
    [[ "$CLEAR_COORDINATOR" == 1 ]] && deps+=("deploy/${RELEASE}-coordinator")
    # Empty the cache dirs first (belt and braces: the restart below gives
    # every pod a new ephemeral volume anyway, and a cold JVM / Glue cache).
    for p in $(worker_pods) $([[ "$CLEAR_COORDINATOR" == 1 ]] && kubectl get pods -n "$NAMESPACE" -l "$SEL_COORD" -o name); do
        kubectl exec -n "$NAMESPACE" "$p" -- bash -c 'rm -rf /cache/* /cache/.[!.]* 2>/dev/null; true' || warn "could not empty /cache on ${p#pod/}"
    done
    kubectl rollout restart -n "$NAMESPACE" "${deps[@]}" >/dev/null
    log "restarted ${deps[*]#deploy/} (fresh cache volumes)"
    wait_ready
    log "cache cleared: ${TRINO_WORKERS} workers active, $(sql 'SELECT version()' | tr -d '"')"
}

cmd_down() {
    helm_ uninstall "$RELEASE" -n "$NAMESPACE" --ignore-not-found --wait || true
    # Ephemeral PVCs go with their pods (owner reference); delete stragglers.
    kubectl delete pvc -n "$NAMESPACE" -l "app.kubernetes.io/instance=${RELEASE},trino.cdcbench/cache=true" --ignore-not-found --wait=false
}

# qstats <query_id>: what the harness records. Wall times from
# system.runtime.queries (no cpu column there), cpu and bytes from the
# coordinator's /v1/query/<id> (queryStats).
cmd_qstats() {
    local id="${1:?usage: qstats <query_id>}"
    [[ "$id" =~ ^[0-9a-z_]+$ ]] || die "not a query id: ${id}"
    sql "SELECT query_id, state, queued_time_ms, analysis_time_ms, planning_time_ms,
                to_milliseconds(\"end\" - created) AS elapsed_ms,
                to_milliseconds(\"end\" - started) AS execution_ms,
                error_code
         FROM system.runtime.queries WHERE query_id = '${id}'" TSV_HEADER
    kubectl exec -n "$NAMESPACE" "deploy/${RELEASE}-coordinator" -c trino-coordinator -- \
        curl -sf -H "X-Trino-User: ${TRINO_USER}" "http://localhost:8080/v1/query/${id}?pretty" \
        | python3 -c '
import json, sys
s = json.load(sys.stdin)["queryStats"]
keys = ["elapsedTime", "queuedTime", "executionTime", "totalCpuTime", "physicalInputDataSize",
        "physicalInputPositions", "processedInputDataSize", "peakUserMemoryReservation", "totalDrivers"]
print("\t".join(keys)); print("\t".join(str(s.get(k, "")) for k in keys))'
}

case "${1:-}" in
    render)      cmd_render ;;
    up)          cmd_up ;;
    status)      cmd_status ;;
    clear-cache) cmd_clear_cache ;;
    down)        cmd_down ;;
    sql)         shift; [[ $# -ge 1 ]] || die "usage: sql '<SQL>' [output format]"; sql "$@" ;;
    qstats)      shift; cmd_qstats "${1:-}" ;;
    *)           sed -n '5,11p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
