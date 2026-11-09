#!/usr/bin/env bash
# Snapshot suite over the STOPPED CDC tables, warm DuckDB vs Trino on the
# Iceberg tables and on Paimon-as-iceberg (both table sets compacted once by
# the hot test's watcher, compaction/run.sh run both) (ENGINES x FORMATS, up to four arms),
# from a cold start, every arm of a format
# on the same pinned snapshot per table (run.sh's runner step).
#
#   1. gate: the CDC writers stopped (suspended here if not already); every engine in ENGINES up
#      (qb_engines_gate: duckdb-warm-cdc busy=false, Trino with TRINO_WORKERS
#      active workers);
#   2. pin: the four tables of every format, after the gate; Trino checked to read every pin (a paimon
#      table it cannot read is a WARNING, its queries record the error);
#   3. the S4 slice [S_FROM, S_TO) on the pinned tables, unless set (untimed,
#      on qb_setup_engine, before the cold start): the busiest bucket with rows in every format;
#   4. cold start: clear-caches.sh (DuckDB server restarted,
#      deploy-trino.sh clear-cache);
#   5. each engine in ENGINES order (default duckdb, then trino): SNAPSHOT_RUNS (3)
#      passes, no warm-up; the first is timed as cold (cold_ms), median_ms is
#      the median of the rest;
#   6. uploads bench-results/ (QB_RESULTS_PREFIX_RUN, set by run.sh).
# SNAPSHOT_DT comes from run.sh (exported). By hand, set it, or it is resolved.
#
# Output: as duckdb-warm.sh / trino.sh, tables-<STAMP>.tsv, and
# snapshot-<STAMP>.pins (gate times, cache-clear time, pinned snapshot ids,
# windows, writer state).
#
# Exit: non-zero when a gate, the pins, the cache clear or an engine's driver
# fails (duckdb-warm.sh / trino.sh "Exit": engine not up, render/pin failure,
# no rows). A query that is not OK (duckdb:paimon on v3, say) is a result: in
# the CSVs, the summary and the driver's "N of M queries not OK" line, with
# its engine still `status: <engine>-snapshot-exit=0`.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SUITE=snapshot
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
export STAMP
ENGINE_LIST=$(qb_engines)
export ENGINES="$ENGINE_LIST"
mkdir -p "$OUT_DIR"
now() { date -u '+%Y-%m-%d %H:%M:%SZ'; }
PINS="${OUT_DIR}/snapshot-${STAMP}.pins"

upload() {
    [[ -n "${QB_RESULTS_PREFIX_RUN:-}" ]] || return 0
    aws s3 sync "$OUT_DIR" "s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX_RUN}" --only-show-errors \
        && echo "status: results-uploaded-snapshot ${QB_RESULTS_PREFIX_RUN}" || echo "status: results-upload-failed-snapshot"
}

rc=0
qb_resolve_snapshot_dt
qb_log "=== snapshot: dt=${QB_SNAPSHOT_DT}, engines ${ENGINE_LIST}, formats $(qb_formats) ==="
{ echo "stamp=${STAMP} suite=snapshot dt=${QB_SNAPSHOT_DT} engines=${ENGINE_LIST// /,} formats=$(qb_formats | tr ' ' ',')"
  qb_run_meta
  echo "started_at=$(now)"; } > "$PINS"
# 1. gate
qb_stop_writers || qb_die "the writers could not be stopped"
echo "gate_writers_stopped_at=$(now) writers=$(qb_writer_state "$FLINK_JOBS") generator=$(qb_generator_state)" >> "$PINS"
g=$(qb_engines_gate) || exit 1
echo "$g" >> "$PINS"
# 2. pin, after the gate
p=$("${HERE}/pin.sh" "${STAMP}") || qb_die "pinning failed"
eval "$p"
# shellcheck disable=SC2046
export $(compgen -v | grep -E '^QB_PIN_') 2>/dev/null || true
{ qb_pins_dump; echo "pinned_at=$(now)"; } >> "$PINS"
# 3. the slice, on the pinned tables
qb_resolve_snapshot_slice
export SNAPSHOT_DT="$QB_SNAPSHOT_DT" S_FROM="$QB_S_FROM" S_TO="$QB_S_TO"
echo "slice=[${QB_S_FROM}, ${QB_S_TO}) (${QB_S_HOW})" >> "$PINS"
qb_log "snapshot slice (S4, orders.order_date): [${S_FROM}, ${S_TO}) (${QB_S_HOW})"
# 4. cold start, right before the first warm-up pass
"${HERE}/clear-caches.sh" || qb_die "could not clear the caches"
echo "caches_cleared_at=$(now)" >> "$PINS"
# 5. the passes. The second engine starts cold too: neither shares a cache
# with the other, and clear-caches.sh emptied both. SNAPSHOT_RUNS (3) passes,
# no warm-up: pass 0 is timed as cold (cold_ms), the rest give median_ms.
export WARMUP=0 RUNS="${SNAPSHOT_RUNS:-3}" COLD_PASS=1
echo "passes=${RUNS} (pass 0 cold, timed; median of the other $((RUNS - 1)))" >> "$PINS"
for e in $ENGINE_LIST; do
    case "$e" in
        duckdb) "${HERE}/duckdb-warm.sh" run; r=$? ;;
        trino)  "${HERE}/trino.sh" run; r=$? ;;
    esac
    echo "status: ${e}-snapshot-exit=${r}"; echo "${e}_finished_at=$(now) rc=${r}" >> "$PINS"
    (( r == 0 )) || rc=$r
done
echo "finished_at=$(now) rc=${rc}" >> "$PINS"
upload
exit $rc
