#!/usr/bin/env bash
# Empty both engines' caches, so a query test (hot or snapshot) starts cold on
# both. querybench/run.sh start runs it before every test; run it by hand only
# for a test started some other way.
#
#   ./querybench/clear-caches.sh
#
# 1. Refuses while a cid_events query runs on the StarRocks FE: emptying a
#    cache under a running test would change its numbers.
# 2. StarRocks: for each BE, delete its datacache files and restart the pod
#    (the memory part goes with the restart; the disk part would survive it),
#    then wait until it is Ready. One BE at a time, so the cluster stays up.
# 3. DuckDB: restart duckdb-warm, if it is up: empty disk and memory caches,
#    Parquet-footer and HTTP-metadata caches included (/clear_cache misses those).
# 4. Checks every BE reports DiskUsage 0B.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
BE_PODS="${BE_PODS:-kube-starrocks-be-0 kube-starrocks-be-1 kube-starrocks-be-2}"
DATACACHE_DIR="${DATACACHE_DIR:-/opt/starrocks/be/storage/datacache}"

qb_assert_fe_idle

for p in $BE_PODS; do
    qb_log "StarRocks ${p}: emptying ${DATACACHE_DIR} and restarting"
    kubectl exec -n "$NAMESPACE" "$p" -c be -- sh -c "rm -rf ${DATACACHE_DIR:?}/*" \
        || qb_die "could not empty the datacache on ${p}"
    kubectl delete pod -n "$NAMESPACE" "$p" --wait=true >/dev/null
    # The StatefulSet recreates the pod under the same name; `kubectl wait` on a
    # pod that does not exist yet fails at once, mid-restart, so wait for it first.
    for (( i = 0; i < 60; i++ )); do
        kubectl get pod -n "$NAMESPACE" "$p" >/dev/null 2>&1 && break
        sleep 5
    done
    kubectl wait -n "$NAMESPACE" --for=condition=Ready "pod/${p}" --timeout=600s >/dev/null \
        || qb_die "${p} not Ready 10 min after its restart"
done

# DuckDB: restart the server rather than ask it to clear. /clear_cache empties
# cache_httpfs only; the Parquet-footer and HTTP-metadata caches (and anything
# else in the process) survive it, while StarRocks' BEs are restarted above. A
# new pod also gets a new, empty cache volume (an ephemeral PVC per pod).
if kubectl get deploy duckdb-warm -n "$NAMESPACE" >/dev/null 2>&1; then
    qb_log "duckdb-warm: restarting (empty caches)"
    kubectl rollout restart -n "$NAMESPACE" deploy/duckdb-warm >/dev/null
    kubectl rollout status -n "$NAMESPACE" deploy/duckdb-warm --timeout=1200s >/dev/null \
        || qb_die "duckdb-warm not ready 20 min after its restart"
    h=$(kubectl exec -n "$NAMESPACE" deploy/duckdb-warm -c duckdb -- python3 -c \
        'import json, urllib.request; print(json.dumps(json.load(urllib.request.urlopen("http://localhost:8080/health?cache=1", timeout=30))))' 2>/dev/null || true)
    [[ "$h" == *'"busy": false'* ]] || qb_die "duckdb-warm is not ready and idle after its restart (/health: ${h:-no answer})"
    qb_log "duckdb-warm: restarted, caches empty (${h:0:120})"
else
    qb_log "duckdb-warm is not up: nothing to clear"
fi

# Every BE back, with an empty datacache. The FE can take a moment to see the
# restarted BEs, so a few tries.
for try in 1 2 3 4 5 6; do
    m=$(qb_sr "SHOW BACKENDS" | grep -o 'DiskUsage: [^,]*' || true)
    n=$(grep -c . <<<"$m" || true)
    nonzero=$(grep -v -c 'DiskUsage: 0B' <<<"$m" || true)
    (( n == $(wc -w <<<"$BE_PODS") && nonzero == 0 )) && break
    (( try < 6 )) || qb_die "StarRocks datacache not empty or a BE missing: $(tr '\n' ' ' <<<"$m")"
    sleep 20
done
qb_log "caches cleared: StarRocks $(tr '\n' ' ' <<<"$m")"
echo "status: caches-cleared $(date -u)"
