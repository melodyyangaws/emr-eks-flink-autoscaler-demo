#!/usr/bin/env bash
# Warm DuckDB, the like-for-like counterpart of the always-up StarRocks cluster
# (deploy-starrocks.sh): one long-lived server (duckdb_warm_server.py,
# Deployment duckdb-warm) keeps one DuckDB process, its S3 secret and a block
# cache sized like StarRocks' datacache, for both suites.
#
#   ./engine-deploy/deploy-duckdb-warm.sh up            start (or restart) the server
#   ./engine-deploy/deploy-duckdb-warm.sh status        health, busy flag, cache size
#   ./engine-deploy/deploy-duckdb-warm.sh clear-cache   empty the block cache (cold start)
#   ./engine-deploy/deploy-duckdb-warm.sh down          delete the server and its cache
#
# The queries run through querybench/duckdb-warm.sh (run, pin); querybench/run.sh
# calls `up` before a test, and its watcher `down` after it.
#
# Parity with StarRocks (checked on the cluster 2026-10-13, StarRocks 4.1.4):
#   CPU       3 BEs x 9 cpu = one 27 cpu pod, 27 threads, m5.8xlarge
#   RAM       3 BEs x 38 Gi = one 114 Gi pod. The same shares as a BE: 90% of the
#             pod (102.6 GiB) for DuckDB, of which 20% (20.52 GiB = 3 x 7.34 GB)
#             is the cache memory and the rest the memory_limit (82.08 GiB). A
#             BE's mem_limit includes its datacache; DuckDB's memory_limit does
#             not, so the cache is taken out of it here (deploy-starrocks.sh).
#   cache     3 x 150 GiB disk + 3 x 7.34 GB memory        -> CACHE_DISK_BYTES on disk + CACHE_MEM_BYTES
#             (both engines' default: cache on). DuckDB's disk is gp3-fast (850 MB/s,
#             gp3-fast-sc.yaml; an m5.8xlarge passes ~850 MB/s to EBS): on default gp3
#             (125 MB/s) cache_httpfs served hits from disk slower than S3 and, unlike
#             StarRocks' io adaptor, cannot fall back to S3. The BEs' gp3-starrocks-be (also
#             in gp3-fast-sc.yaml) gives the 3 of them the same ~850 MB/s together.
#             CACHE_MODE=in_mem keeps only the memory cache.
#
# Knobs: DUCKDB_VERSION, DUCKDB_CPU, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT, DUCKDB_POD_MEMORY,
# DUCKDB_NODEPOOL, DUCKDB_INSTANCE_SIZE, DUCKDB_MAX_TEMP_SIZE,
# CACHE_DISK_BYTES, CACHE_MEM_BYTES, CACHE_MODE, CACHE_STORAGE_CLASS, CACHE_VOLUME,
# PIN_SNAPSHOT, READY_GRACE.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# .bench-env, NAMESPACE, the table names the template needs, qb_log/qb_die, render.
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"

CM="${CM:-duckdb-warm}"
DUCKDB_VERSION="${DUCKDB_VERSION:-1.5.6}"           # latest stable; 2.0 is dev builds only (2026-10)
DUCKDB_CPU="${DUCKDB_CPU:-27}"                       # = 3 BEs x 9 cpu
DUCKDB_THREADS="${DUCKDB_THREADS:-27}"               # = the cpu limit
# 3 x a BE's mem_limit (34.2 GiB) - the cache memory (20.52 GiB) = 82.08 GiB:
# DuckDB counts its cache outside memory_limit, a BE inside mem_limit.
DUCKDB_MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-84049MiB}"
DUCKDB_MAX_TEMP_SIZE="${DUCKDB_MAX_TEMP_SIZE:-180GB}"
CACHE_DISK_BYTES="${CACHE_DISK_BYTES:-483183820800}" # 3 x 161061273600 = BE block_cache_disk_quota_bytes
CACHE_MEM_BYTES="${CACHE_MEM_BYTES:-22033182228}"    # 22.03 GB = the BEs' 3 x 7344394076 B datacache_mem_size (deploy-starrocks.sh)
DUCKDB_POD_MEMORY="${DUCKDB_POD_MEMORY:-114Gi}"      # = 3 BEs x 38Gi; about the most Karpenter places on an m5.8xlarge
DUCKDB_NODEPOOL="${DUCKDB_NODEPOOL:-driver-nodepool}"   # = the BEs'; consolidates only empty nodes
# 8xlarge (32 vcpu / 128 GiB): the smallest m5 that holds 27 cpu / 114Gi + daemonsets.
# Above ~170 GiB, Karpenter's VM-overhead estimate rules out an m5.12xlarge: use 16xlarge.
DUCKDB_INSTANCE_SIZE="${DUCKDB_INSTANCE_SIZE:-8xlarge}"
CACHE_MODE="${CACHE_MODE:-on_disk}"                  # on_disk | in_mem, see duckdb_warm_server.py
CACHE_STORAGE_CLASS="${CACHE_STORAGE_CLASS:-gp3-fast}"  # gp3-fast-sc.yaml; plain gp3 caps the cache at 125 MB/s
case "$CACHE_MODE" in
    in_mem)  CACHE_VOLUME="${CACHE_VOLUME:-1Gi}" ;;     # unused, the template still mounts it
    on_disk) CACHE_VOLUME="${CACHE_VOLUME:-500Gi}" ;;   # 450 GiB cap + filesystem overhead
    *) qb_die "CACHE_MODE must be in_mem|on_disk, got '${CACHE_MODE}'" ;;
esac
PIN_SNAPSHOT="${PIN_SNAPSHOT:-1}"
READY_GRACE="${READY_GRACE:-1200}"                   # Karpenter launch + pip + extensions
NAME=duckdb-warm
# Every placeholder in duckdb-warm.yaml (lib/common.sh's render rejects one left out).
TPL_VARS="NAME NAMESPACE CM DUCKDB_VERSION AWS_REGION BUCKET_NAME ICEBERG_DB TABLE_NAME PMNICE_TABLE_ROOT
PIN_SNAPSHOT DUCKDB_MEMORY_LIMIT DUCKDB_THREADS DUCKDB_MAX_TEMP_SIZE CACHE_MODE CACHE_STORAGE_CLASS
CACHE_DISK_BYTES CACHE_MEM_BYTES CACHE_VOLUME DUCKDB_POD_MEMORY DUCKDB_CPU DUCKDB_NODEPOOL DUCKDB_INSTANCE_SIZE"

# The server's HTTP API, called inside the pod (no port-forward): $1 GET|POST, $2 path.
server() {
    kubectl exec -n "$NAMESPACE" "deploy/${NAME}" -c duckdb -- python3 -c '
import sys, urllib.request
req = urllib.request.Request("http://localhost:8080" + sys.argv[2], method=sys.argv[1],
                             data=b"{}" if sys.argv[1] == "POST" else None,
                             headers={"Content-Type": "application/json"})
print(urllib.request.urlopen(req, timeout=60).read().decode())' "$1" "$2"
}

cmd_up() {
    local deadline
    if [[ "$CACHE_MODE" == on_disk ]] && ! kubectl get storageclass "$CACHE_STORAGE_CLASS" >/dev/null 2>&1; then
        [[ "$CACHE_STORAGE_CLASS" == gp3-fast ]] || qb_die "storageclass ${CACHE_STORAGE_CLASS} not found"
        kubectl apply -f "${HERE}/gp3-fast-sc.yaml" >/dev/null
    fi
    kubectl delete configmap "$CM" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl create configmap "$CM" -n "$NAMESPACE" --from-file="${HERE}/duckdb_warm_server.py" >/dev/null
    # shellcheck disable=SC2086
    render "${HERE}/duckdb-warm.yaml" "${HERE}/duckdb-warm-deployed.yaml" $TPL_VARS || qb_die "could not render duckdb-warm.yaml"
    kubectl apply -f "${HERE}/duckdb-warm-deployed.yaml" >/dev/null
    # A changed server.py only reaches a running pod through a restart.
    kubectl rollout restart -n "$NAMESPACE" "deploy/${NAME}" >/dev/null
    qb_log "deploy/${NAME} applied (duckdb ${DUCKDB_VERSION}, cache ${CACHE_MODE}: $((CACHE_MEM_BYTES / 1000000000)) GB mem$([[ "$CACHE_MODE" == on_disk ]] && echo " + $((CACHE_DISK_BYTES >> 30)) GiB disk"))"
    deadline=$(( $(date +%s) + READY_GRACE ))
    until kubectl rollout status -n "$NAMESPACE" "deploy/${NAME}" --timeout=30s >/dev/null 2>&1; do
        (( $(date +%s) < deadline )) || qb_die "deploy/${NAME} not ready after ${READY_GRACE}s (kubectl describe pod -l app=duckdb-warm)"
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

cmd_clear() {
    kubectl get deploy "$NAME" -n "$NAMESPACE" >/dev/null 2>&1 || qb_die "${NAME} is not up"
    qb_log "cache cleared: $(server POST /clear_cache)"
}

case "${1:-}" in
    up)          cmd_up ;;
    status)      cmd_status ;;
    clear-cache) cmd_clear ;;
    down)        cmd_down ;;
    *)           sed -n '7,10p' "${HERE}/deploy-duckdb-warm.sh" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
