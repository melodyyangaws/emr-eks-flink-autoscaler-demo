#!/usr/bin/env bash
# Empty both engines' caches, so a CDC query test starts cold on both:
# run.sh start runs it before a hot test, snapshot.sh right before the
# snapshot suite's warm-up pass. Run it by hand only for a test started some
# other way.
#
#   ./querybench/clear-caches.sh
#
# 1. Refuses while another query runs on Trino or a DuckDB server is busy:
#    emptying a cache under a running test would change its numbers.
# 2. DuckDB: restart duckdb-warm-cdc, if it is up: empty disk and memory caches,
#    Parquet-footer and HTTP-metadata caches included (/clear_cache misses those).
#    A new pod also gets a new, empty cache volume (an ephemeral PVC per pod).
# 3. Trino: engine-deploy/deploy-trino.sh clear-cache (whatever caches that
#    deployment keeps: file-system cache, metadata caches; see its header),
#    then waits until TRINO_WORKERS workers are active again.
# ENGINES (default both) without duckdb skips step 2, without trino step 3.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
DUCK="$DUCKDB_NAME"
DEPLOY_TRINO="${CDC_DIR}/engine-deploy/deploy-trino.sh"

qb_assert_alone duckdb-warm   # no other query on either engine (ALLOW_CONCURRENT=1 skips)

if qb_has_engine duckdb && kubectl get deploy "$DUCK" -n "$NAMESPACE" >/dev/null 2>&1; then
    # Restart rather than ask the server to clear: /clear_cache empties
    # cache_httpfs only; the Parquet-footer and HTTP-metadata caches (and
    # anything else in the process) survive it.
    qb_log "${DUCK}: restarting (empty caches)"
    kubectl rollout restart -n "$NAMESPACE" "deploy/${DUCK}" >/dev/null
    kubectl rollout status -n "$NAMESPACE" "deploy/${DUCK}" --timeout=1200s >/dev/null \
        || qb_die "${DUCK} not ready 20 min after its restart"
    h=$(kubectl exec -n "$NAMESPACE" "deploy/${DUCK}" -c duckdb -- python3 -c \
        'import json, urllib.request; print(json.dumps(json.load(urllib.request.urlopen("http://localhost:8080/health?cache=1", timeout=30))))' 2>/dev/null || true)
    [[ "$h" == *'"busy": false'* ]] || qb_die "${DUCK} is not ready and idle after its restart (/health: ${h:-no answer})"
    qb_log "${DUCK}: restarted, caches empty (${h:0:120})"
elif qb_has_engine duckdb; then
    qb_log "${DUCK} is not up: nothing to clear"
fi

if qb_has_engine trino; then
    [[ -x "$DEPLOY_TRINO" ]] || qb_die "no ${DEPLOY_TRINO#${CDC_DIR}/}: cannot clear Trino's caches"
    qb_log "Trino: ${DEPLOY_TRINO#${CDC_DIR}/} clear-cache"
    "$DEPLOY_TRINO" clear-cache >&2 || qb_die "Trino: clear-cache failed"
    for (( i = 0; i < 60; i++ )); do
        qb_trino_ready && break
        sleep 10
    done
    qb_trino_ready || qb_die "Trino: fewer than ${TRINO_WORKERS} active workers 10 min after clear-cache"
    qb_log "Trino: caches cleared, $(qb_trino_workers) workers active"
fi
echo "status: caches-cleared $(date -u)"
