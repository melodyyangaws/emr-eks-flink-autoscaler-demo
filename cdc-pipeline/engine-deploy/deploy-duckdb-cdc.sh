#!/usr/bin/env bash
# The CDC query benchmark's warm DuckDB server (Deployment duckdb-warm-cdc,
# duckdb_cdc_server.py), queried by querybench/duckdb-warm.sh; Trino is the
# other engine (deploy-trino.sh).
#
#   ./engine-deploy/deploy-duckdb-cdc.sh up            start (or restart) the server
#   ./engine-deploy/deploy-duckdb-cdc.sh status        health, busy flag, cache size
#   ./engine-deploy/deploy-duckdb-cdc.sh clear-cache   empty the block cache
#   ./engine-deploy/deploy-duckdb-cdc.sh down          delete the server and its cache
#   ./engine-deploy/deploy-duckdb-cdc.sh render        write duckdb-cdc-deployed.yaml only (no cluster change)
#
# A copy of throughput-benchmark/engine-deploy/deploy-duckdb-warm.sh with the
# same sizing and cache parity defaults (27 cpu / 114 Gi on an m5.8xlarge, 450
# GiB gp3-fast disk cache + 22.03 GB memory cache, 256 KiB blocks: see that
# script for why each number is what it is). Its own Deployment name, so a
# throughput test's duckdb-warm (other env, other server) is never replaced.
# The gp3-fast StorageClass is the throughput benchmark's
# (throughput-benchmark/engine-deploy/gp3-fast-sc.yaml), applied if missing.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"

NAME="${DUCKDB_NAME:-duckdb-warm-cdc}"
CM="${CM:-${NAME}}"
# The engine: DUCKDB_IMAGE (.bench-env; default the source-built 2.0.0 image in
# ECR, duckdb-cdc-server:2.0.0) as is, or, with DUCKDB_IMAGE empty,
# python:3.12-slim + pip duckdb==DUCKDB_VERSION at every start (2.0.0: the PyPI
# wheel, clang + ThinLTO; the image is GCC 14 (Debian trixie) without LTO, so
# the two are not the same build and their timings are separate baselines).
# The server needs Python >= 3.11.
DUCKDB_IMAGE="${DUCKDB_IMAGE-}"
DUCKDB_VERSION="${DUCKDB_VERSION:-2.0.0}"
DUCKDB_FALLBACK_IMAGE="${DUCKDB_FALLBACK_IMAGE:-python:3.12-slim}"
if [[ -n "$DUCKDB_IMAGE" ]]; then DUCKDB_POD_IMAGE="$DUCKDB_IMAGE" DUCKDB_FROM_IMAGE=1
else DUCKDB_POD_IMAGE="$DUCKDB_FALLBACK_IMAGE" DUCKDB_FROM_IMAGE=0; fi
ICEBERG_FORMAT_VERSION="${ICEBERG_FORMAT_VERSION:-3}"
# Default none: the paimon arm reads native Paimon (paimon_scan), not the export
# whose bare DV blobs needed iceberg_unsafe_skip_puffin_verification.
SKIP_PUFFIN_VERIFICATION_ARMS="${SKIP_PUFFIN_VERIFICATION_ARMS-}"
DUCKDB_CPU="${DUCKDB_CPU:-27}"
DUCKDB_THREADS="${DUCKDB_THREADS:-27}"
DUCKDB_MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-84049MiB}"
DUCKDB_MAX_TEMP_SIZE="${DUCKDB_MAX_TEMP_SIZE:-180GB}"
CACHE_DISK_BYTES="${CACHE_DISK_BYTES:-483183820800}"
CACHE_MEM_BYTES="${CACHE_MEM_BYTES:-22033182228}"
DUCKDB_POD_MEMORY="${DUCKDB_POD_MEMORY:-114Gi}"
DUCKDB_NODEPOOL="${DUCKDB_NODEPOOL:-driver-nodepool}"
DUCKDB_INSTANCE_SIZE="${DUCKDB_INSTANCE_SIZE:-8xlarge}"
CACHE_BLOCK_BYTES="${CACHE_BLOCK_BYTES:-262144}"
CACHE_MODE="${CACHE_MODE:-on_disk}"
CACHE_STORAGE_CLASS="${CACHE_STORAGE_CLASS:-gp3-fast}"
MAX_RESULT_ROWS="${MAX_RESULT_ROWS:-10000}"
case "$CACHE_MODE" in
    in_mem)  CACHE_VOLUME="${CACHE_VOLUME:-1Gi}" ;;
    on_disk) CACHE_VOLUME="${CACHE_VOLUME:-500Gi}" ;;
    *) qb_die "CACHE_MODE must be in_mem|on_disk, got '${CACHE_MODE}'" ;;
esac
READY_GRACE="${READY_GRACE:-1200}"
TPL_VARS="NAME NAMESPACE CM DUCKDB_VERSION DUCKDB_IMAGE DUCKDB_POD_IMAGE DUCKDB_FROM_IMAGE ICEBERG_FORMAT_VERSION SKIP_PUFFIN_VERIFICATION_ARMS AWS_REGION DUCKDB_MEMORY_LIMIT DUCKDB_THREADS DUCKDB_MAX_TEMP_SIZE
CACHE_MODE CACHE_STORAGE_CLASS CACHE_DISK_BYTES CACHE_MEM_BYTES CACHE_VOLUME DUCKDB_POD_MEMORY DUCKDB_CPU
DUCKDB_NODEPOOL DUCKDB_INSTANCE_SIZE CACHE_BLOCK_BYTES MAX_RESULT_ROWS"
OUT="${HERE}/duckdb-cdc-deployed.yaml"

server() {   # GET|POST <path>, inside the pod
    kubectl exec -n "$NAMESPACE" "deploy/${NAME}" -c duckdb -- python3 -c '
import sys, urllib.request
req = urllib.request.Request("http://localhost:8080" + sys.argv[2], method=sys.argv[1],
                             data=b"{}" if sys.argv[1] == "POST" else None,
                             headers={"Content-Type": "application/json"})
print(urllib.request.urlopen(req, timeout=60).read().decode())' "$1" "$2"
}

cmd_render() {
    # shellcheck disable=SC2086
    render "${HERE}/duckdb-cdc.yaml" "$OUT" $TPL_VARS || qb_die "could not render duckdb-cdc.yaml"
    qb_log "rendered ${OUT##*/}"
}

cmd_up() {
    local deadline
    if [[ "$CACHE_MODE" == on_disk ]] && ! kubectl get storageclass "$CACHE_STORAGE_CLASS" >/dev/null 2>&1; then
        [[ "$CACHE_STORAGE_CLASS" == gp3-fast ]] || qb_die "storageclass ${CACHE_STORAGE_CLASS} not found"
        kubectl apply -f "${TB_DIR}/engine-deploy/gp3-fast-sc.yaml" >/dev/null
    fi
    cmd_render
    kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl create configmap "$CM" -n "$NAMESPACE" --from-file="${HERE}/duckdb_cdc_server.py" >/dev/null
    kubectl apply -f "$OUT" >/dev/null
    kubectl rollout restart -n "$NAMESPACE" "deploy/${NAME}" >/dev/null   # a changed server.py reaches the pod only so
    qb_log "deploy/${NAME} applied ($([[ "$DUCKDB_FROM_IMAGE" == 1 ]] && echo "image ${DUCKDB_IMAGE}" || echo "pip duckdb ${DUCKDB_VERSION} on ${DUCKDB_FALLBACK_IMAGE}"), cache ${CACHE_MODE}, iceberg v${ICEBERG_FORMAT_VERSION}, skip-puffin arms '${SKIP_PUFFIN_VERIFICATION_ARMS}')"
    deadline=$(( $(date +%s) + READY_GRACE ))
    until kubectl rollout status -n "$NAMESPACE" "deploy/${NAME}" --timeout=30s >/dev/null 2>&1; do
        (( $(date +%s) < deadline )) || qb_die "deploy/${NAME} not ready after ${READY_GRACE}s (kubectl describe pod -l app=${NAME})"
        qb_log "waiting for deploy/${NAME}..."
    done
    qb_log "$(server GET /health 2>/dev/null || qb_die "no /health from ${NAME}")"
}

cmd_status() {
    if kubectl get deploy "$NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
        echo "${NAME}: $(server GET '/health?cache=1' 2>/dev/null || echo 'no answer (starting?)')"
    else
        echo "${NAME}: not running"
    fi
}

cmd_down() {
    kubectl delete deploy "$NAME" -n "$NAMESPACE" --ignore-not-found --wait=false
    kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found
}

case "${1:-}" in
    up)          cmd_up ;;
    status)      cmd_status ;;
    clear-cache) kubectl get deploy "$NAME" -n "$NAMESPACE" >/dev/null 2>&1 || qb_die "${NAME} is not up"
                 qb_log "cache cleared: $(server POST /clear_cache)" ;;
    down)        cmd_down ;;
    render)      cmd_render ;;
    *)           sed -n '5,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
