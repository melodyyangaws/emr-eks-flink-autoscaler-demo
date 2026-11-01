#!/usr/bin/env bash
# Snapshot suite over the STOPPED CDC tables, in two independent phases, one
# per format, each from a cold start (run.sh's runner step):
#
#   SNAPSHOT_FORMAT=paimon   the Paimon phase: StarRocks paimon + pmn_as_iceberg, DuckDB paimon
#   SNAPSHOT_FORMAT=iceberg  the Iceberg phase: StarRocks iceberg, DuckDB iceberg
#   (Iceberg v3 set, ICEBERG_FORMAT_VERSION=3: StarRocks pmn_as_iceberg and
#   iceberg are skipped, see lib/querybench.sh, so the Paimon phase runs
#   StarRocks paimon (JNI reader) + DuckDB paimon and the Iceberg phase DuckDB
#   iceberg alone; SR_V3_ARMS=1 runs the skipped arms to record their errors)
#   SNAPSHOT_FORMAT=both     (default) the Paimon phase now, then the Iceberg
#                            phase as soon as its gate passes (this Job waits)
#
# Each phase:
#   1. gate. Both phases: the writers stopped (suspended here if not already)
#      and the engines up (qb_engines_gate: FE + all BEs Ready, duckdb-warm-cdc
#      busy=false). The Iceberg phase also needs its compaction finished
#      (qb_compaction_gate: every COMPACT_TABLES Job iceberg-compact-<t>
#      SUCCEEDED and the table's current snapshot is its replace commit). The
#      Paimon phase never waits for compaction: Paimon compacts in its writer.
#   2. pin: this format's four tables only, AFTER the gate (so the Iceberg
#      phase pins the compacted snapshot);
#   3. cold start: clear-caches.sh (StarRocks datacache wiped + BEs restarted,
#      DuckDB server restarted);
#   4. StarRocks (the phase's arms), then DuckDB (the phase's arm): warm-up +
#      timed passes as usual;
#   5. uploads bench-results/ so far (QB_RESULTS_PREFIX_RUN, set by run.sh).
# SNAPSHOT_DT / S_FROM / S_TO come from run.sh (exported), the same for both
# phases. By hand, set them, or qb_start_suite resolves them.
#
# Output: as starrocks.sh / duckdb-warm.sh (both phases append to the same
# CSVs), tables-<STAMP>.tsv, and snapshot-<STAMP>-<format>.pins per phase
# (gate times, cache-clear time, pinned snapshot ids, windows, writer state).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SUITE=snapshot
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
export STAMP
export ENGINES="${ENGINES:-starrocks duckdb}"
SNAPSHOT_FORMAT="${SNAPSHOT_FORMAT:-both}"
case "$SNAPSHOT_FORMAT" in paimon|iceberg|both) ;; *) qb_die "SNAPSHOT_FORMAT must be paimon|iceberg|both, got '${SNAPSHOT_FORMAT}'" ;; esac
mkdir -p "$OUT_DIR"
now() { date -u '+%Y-%m-%d %H:%M:%SZ'; }

upload() {
    [[ -n "${QB_RESULTS_PREFIX_RUN:-}" ]] || return 0
    aws s3 sync "$OUT_DIR" "s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX_RUN}" --only-show-errors \
        && echo "status: results-uploaded-${1} ${QB_RESULTS_PREFIX_RUN}" || echo "status: results-upload-failed-${1}"
}

phase() {   # <paimon|iceberg>
    local fmt="$1" pins="${OUT_DIR}/snapshot-${STAMP}-$1.pins" g p rc=0 r sr_arms duck_fmt
    local engines="$ENGINES"
    case "$fmt" in
        paimon)  sr_arms="paimon pmn_as_iceberg"; duck_fmt=paimon ;;
        iceberg) sr_arms="iceberg";               duck_fmt=iceberg ;;
    esac
    sr_arms=$(qb_sr_arms_filter "$sr_arms")
    # No StarRocks arm left in this phase (v3 Iceberg phase): StarRocks is not
    # cleared or run here; it stays up for the gate only.
    [[ -z "$sr_arms" ]] && engines="${engines//starrocks/}"
    qb_log "=== ${fmt} phase: StarRocks ${sr_arms:-none}, DuckDB ${duck_fmt} ==="
    { echo "stamp=${STAMP} suite=snapshot phase=${fmt} dt=${QB_SNAPSHOT_DT} slice=[${QB_S_FROM}, ${QB_S_TO}) engines=${ENGINES} sr_arms=${sr_arms:-none}"
      qb_run_meta
      echo "phase_started_at=$(now)"; } > "$pins"
    # 1. gate
    qb_stop_writers || qb_die "the writers could not be stopped"
    echo "gate_writers_stopped_at=$(now) writers=$(qb_writer_state) generator=$(qb_generator_state)" >> "$pins"
    if [[ "$fmt" == iceberg ]]; then
        g=$(qb_compaction_gate "$([[ "$SNAPSHOT_FORMAT" == both ]] && echo 1 || echo 0)") || exit 1
        echo "$g" >> "$pins"
    fi
    g=$(qb_engines_gate) || exit 1
    echo "$g" >> "$pins"
    # 2. pin, after the gate
    p=$(QB_PIN_FORMATS="$fmt" ARMS="$sr_arms" "${HERE}/pin.sh" "${STAMP}-${fmt}") || qb_die "pinning (${fmt}) failed"
    eval "$p"
    # shellcheck disable=SC2046
    export $(compgen -v | grep -E '^(QB_PIN_|SR_PIN_FORM$)') 2>/dev/null || true
    { qb_pins_dump | grep -E "^QB_PIN_(SNAP|META|COMMIT_MS|FORMAT_VERSION)_($( [[ $fmt == paimon ]] && echo 'PAIMON|PMNICE' || echo ICEBERG ))_"
      echo "SR_PIN_FORM=${SR_PIN_FORM:-clause}"; echo "pinned_at=$(now)"; } >> "$pins"
    # 3. cold start, right before the warm-up pass
    ENGINES="$engines" "${HERE}/clear-caches.sh" || qb_die "${fmt} phase: could not clear the caches"
    echo "caches_cleared_at=$(now)" >> "$pins"
    # 4. the passes
    if [[ "$engines" == *starrocks* ]]; then
        ARMS="$sr_arms" "${HERE}/starrocks.sh"; r=$?; echo "status: starrocks-${fmt}-exit=${r}"; (( r == 0 )) || rc=$r
    fi
    if [[ "$engines" == *duckdb* ]]; then
        FORMAT="$duck_fmt" "${HERE}/duckdb-warm.sh" run; r=$?; echo "status: duckdb-${fmt}-exit=${r}"; (( r == 0 )) || rc=$r
    fi
    echo "phase_finished_at=$(now) rc=${rc}" >> "$pins"
    upload "$fmt"
    return $rc
}

qb_start_suite snapshot
export SNAPSHOT_DT="$QB_SNAPSHOT_DT" S_FROM="$QB_S_FROM" S_TO="$QB_S_TO"
rc=0
if [[ "$SNAPSHOT_FORMAT" != iceberg ]]; then phase paimon || rc=$?; fi
if [[ "$SNAPSHOT_FORMAT" != paimon ]];  then phase iceberg || rc=$?; fi
exit $rc
