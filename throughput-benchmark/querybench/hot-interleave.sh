#!/usr/bin/env bash
# Hot suite, StarRocks vs warm DuckDB, on ONE table version and with the engines
# INTERLEAVED pass by pass, so neither engine runs on a later table or under a
# different write load than the other.
#
# Pinning a window (eventTimeDT) fixes the ROWS. 
# The test is only valid if both tables have the same ROWS in that window, and the same ROWS in the pinned versions. 
#   HOT_TO='YYYY-MM-DD HH:MM:SS' ./querybench/hot-interleave.sh   # or omit: now - 5 min
# Knobs: RUNS (3), WAIT_COMMIT_S (5400: give-up for the committed-window check),
# STABLE_CHECKS (10: one-minute checks for both writers to be RUNNING before
# giving up), TEST_SUFFIX.
#
# Output: bench-results/starrocks-<STAMP>.csv and duckdb_warm-<STAMP>.csv (the
# medians, $QB_CSV_HEADER), the per-pass CSVs <...>-<STAMP>-p<N>.csv, and
# hot-interleave-<STAMP>.pins (the pinned versions).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SUITE=hot
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
RUNS="${RUNS:-3}"
WAIT_COMMIT_S="${WAIT_COMMIT_S:-5400}"
BASE="$STAMP"
ICE_T="cid_iceberg_catalog.${ICEBERG_DB}.cid_events"
PMN_T="cid_pmn_as_ice${QB_DB_SUFFIX}.${PAIMON_DB}.cid_events"
PAI_T="cid_paimon_catalog${QB_DB_SUFFIX}.${PAIMON_DB}.cid_events"

# ── 0. Writers RUNNING ───────────────────────────────────────────────────────
# Goes on as soon as both are RUNNING; otherwise checks again a minute later,
# up to STABLE_CHECKS times, then gives up. No fixed wait: querybench/run.sh starts
# this after bringing up duckdb-warm (~5-10 min on a fresh node), which is the
# writers' settling time. (Until 2026-10-20 it always waited 10 minutes.)
STABLE_CHECKS="${STABLE_CHECKS:-10}"
for (( c = 1; ; c++ )); do
    s=$(kubectl get flinkdeployment $FLINK_JOBS -n "$NAMESPACE" -o jsonpath='{.items[*].status.jobStatus.state}' 2>/dev/null || true)
    [[ "$s" == "RUNNING RUNNING" ]] && { qb_log "writers RUNNING (check ${c})"; break; }
    (( c < STABLE_CHECKS )) || qb_die "the writers are not both RUNNING after ${STABLE_CHECKS} checks: ${s:-none}"
    qb_log "writers not both RUNNING yet (${s:-none}), check ${c}/${STABLE_CHECKS}; again in 60s"
    sleep 60
done

# StarRocks' catalogs for this TEST_SUFFIX: starrocks.sh creates them on its
# first run, but the window counts below need them first (2026-10-19, -r3: the
# Paimon-export count failed until the catalog existed).
"${HERE}/starrocks.sh" catalogs || qb_die "cannot create or read the StarRocks catalogs"

# ── 1. A window both tables have committed ──────────────────────────────────
export HOT_TO="${HOT_TO:-$(qb_utc_minus_sec "$(date -u '+%Y-%m-%d %H:%M:%S')" 300)}"
qb_resolve_hot
export HOT_FROM="$QB_HOT_FROM" HOT_DT="$QB_HOT_DT"
qb_log "hot-interleave ${BASE}: window [${HOT_FROM}, ${HOT_TO}) on eventTimeDT, ${RUNS} timed passes"
win_count() {   # <table> [version]
    local v=""; [[ -n "${2:-}" ]] && v=" FOR VERSION AS OF CAST($2 AS BIGINT)"   # CAST: see arm_table in starrocks.sh
    qb_sr "SET enable_scan_datacache=false; SET populate_datacache_mode='never'; SET query_timeout=600;
           SELECT COUNT(*) FROM $1${v} WHERE dt='${HOT_DT}' AND eventTimeDT >= '${HOT_FROM}' AND eventTimeDT < '${HOT_TO}'" | tail -1
}
prev="" t0=$(date +%s)
while :; do
    i=$(win_count "$ICE_T" || true); p=$(win_count "$PMN_T" || true)
    qb_log "window rows: iceberg=${i:-?} paimon=${p:-?}"
    if [[ -n "$i" && "$i" != 0 && "$i" == "$p" && "$i" == "$prev" ]]; then break; fi
    [[ -n "$i" && "$i" == "$p" ]] && prev="$i" || prev=""
    (( $(date +%s) - t0 < WAIT_COMMIT_S )) || qb_die "window not committed in both tables after ${WAIT_COMMIT_S}s (iceberg=${i:-?} paimon=${p:-?})"
    sleep 60
done
ROWS="$i"
qb_log "window committed in both tables: ${ROWS} rows"

# ── 2. One table version for the whole run ─────────────────────────────────
pins=$("${HERE}/duckdb-warm.sh" pin) || qb_die "pin failed (is duckdb-warm up?)"
eval "$pins"
export DUCK_PIN_ICEBERG DUCK_PIN_PAIMON SR_PIN_ICEBERG SR_PIN_PMNICE SR_PIN_PAIMON
# StarRocks must read each pinned version, and see the same window rows there.
# The pin is the table's newest snapshot, which the FE's cached table metadata
# may not hold yet ("Iceberg snapshot ID does not exists", 2026-10-19): refresh
# the table and retry for up to 5 min before giving up.
for spec in "iceberg ${ICE_T} ${SR_PIN_ICEBERG}" "pmn_as_iceberg ${PMN_T} ${SR_PIN_PMNICE}" "paimon ${PAI_T} ${SR_PIN_PAIMON}"; do
    set -- $spec
    for (( try = 1; try <= 10; try++ )); do
        [[ "$1" == paimon ]] || qb_sr "REFRESH EXTERNAL TABLE $2" >/dev/null || true
        n=$(win_count "$2" "${3:-}" 2>&1 || true)
        [[ "$n" == "$ROWS" ]] && break
        qb_log "pinned ${1}: version ${3:-?} reads '${n:0:60}' window rows (try ${try}/10), retrying in 30s"
        sleep 30
    done
    if [[ "$n" == "$ROWS" ]]; then
        qb_log "pinned ${1}: version ${3:-current} reads ${n} window rows"
    elif [[ "$1" == paimon ]]; then
        # The native Paimon reader may not support FOR VERSION AS OF: run it on
        # the current table, and say so in the results.
        qb_log "WARNING: StarRocks native Paimon cannot read version ${3:-?} (${n:0:120}); that arm runs UNPINNED"
        SR_PIN_PAIMON=""
    else
        qb_die "StarRocks ${1} at version ${3:-?} reads '${n:0:120}' window rows, expected ${ROWS}"
    fi
done
# "live" while any generator pod is Running. The run is only a writers-live
# result if it is live at both ends (2026-10-19: a 1-hour generator stopped
# during the second timed pass).
gen_live() {
    local n; n=$(kubectl get pods -n "$NAMESPACE" -l job-name=cid-jsongz-gen \
                 --field-selector=status.phase=Running -o name 2>/dev/null | wc -l | tr -d ' ')
    [[ "$n" -gt 0 ]] && echo "live (${n} pods)" || echo "STOPPED"
}
GEN_START=$(gen_live)
qb_log "generator before the first pass: ${GEN_START}"
[[ "$GEN_START" == STOPPED ]] && qb_log "WARNING: the generator is not writing: this is not a writers-live hot run"
mkdir -p "$OUT_DIR"
{ echo "stamp=${BASE} window=[${HOT_FROM}, ${HOT_TO}) rows=${ROWS}"
  echo "SR_PIN_ICEBERG=${SR_PIN_ICEBERG} SR_PIN_PMNICE=${SR_PIN_PMNICE} SR_PIN_PAIMON=${SR_PIN_PAIMON:-<unpinned>}"
  echo "DUCK_PIN_ICEBERG=${DUCK_PIN_ICEBERG##*/} DUCK_PIN_PAIMON=${DUCK_PIN_PAIMON##*/}"; } > "${OUT_DIR}/hot-interleave-${BASE}.pins"

# ── 3. Interleaved passes: p0 = warm-up (discarded), p1..RUNS timed ─────────
for (( pass = 0; pass <= RUNS; pass++ )); do
    qb_log "pass ${pass}/${RUNS} ($([[ $pass == 0 ]] && echo warm-up || echo timed)): StarRocks"
    STAMP="${BASE}-p${pass}" RUNS=1 WARMUP=0 "${HERE}/starrocks.sh" >> "${OUT_DIR}/hot-interleave-${BASE}.log" 2>&1 \
        || qb_log "WARNING: StarRocks pass ${pass} exited non-zero (see the log)"
    qb_log "pass ${pass}/${RUNS}: DuckDB"
    # The scan profile (untimed, after the pass) on the last pass only: the
    # medians take their scan_rows / scan_bytes from it.
    PROFILE_SCAN=$(( pass == RUNS && ${PROFILE_SCAN:-1} != 0 ? 1 : 0 )) \
    STAMP="${BASE}-p${pass}" RUNS=1 WARMUP=0 "${HERE}/duckdb-warm.sh" run >> "${OUT_DIR}/hot-interleave-${BASE}.log" 2>&1 \
        || qb_log "WARNING: DuckDB pass ${pass} exited non-zero (see the log)"
done

GEN_END=$(gen_live)
echo "generator: before the first pass ${GEN_START}, after the last pass ${GEN_END}" >> "${OUT_DIR}/hot-interleave-${BASE}.pins"
qb_log "generator after the last pass: ${GEN_END}"
[[ "$GEN_END" == STOPPED ]] && qb_log "WARNING: the generator stopped before the last pass ended: the later passes ran without live writes (start it with DURATION_SECONDS=0 and qb-watch.sh)"

# ── 4. Medians of the timed passes ──────────────────────────────────────────
python3 - "$OUT_DIR" "$BASE" "$RUNS" "$QB_CSV_HEADER" <<'EOF'
import csv, os, statistics, sys
out, base, runs, header = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
for eng in ("starrocks", "duckdb_warm"):
    rows = {}
    for p in range(1, runs + 1):
        f = os.path.join(out, "%s-%s-p%d.csv" % (eng, base, p))
        if not os.path.exists(f):
            continue
        for r in csv.DictReader(l for l in open(f) if not l.startswith("#")):
            rows.setdefault((r["arm"], r["query"]), []).append(r)
    with open(os.path.join(out, "%s-%s.csv" % (eng, base)), "w") as w:
        w.write(header + "\n")
        for (arm, q), rs in sorted(rows.items()):
            bad = [r["status"] for r in rs if r["status"] != "OK"]
            ms = [int(r["median_ms"]) for r in rs if r["status"] == "OK" and r["median_ms"]]
            med = int(statistics.median(ms)) if ms and not bad and len(ms) == runs else ""
            status = bad[0] if bad else ("OK" if med != "" else "MISSING")
            last = rs[-1]
            w.write(",".join([base, last["engine"], arm, "hot", q, status, str(med), last["result_rows"],
                              last.get("scan_rows", ""), last.get("scan_bytes", "")]) + "\n")
    print("%s: %s" % (eng, os.path.join(out, "%s-%s.csv" % (eng, base))))
EOF
qb_log "done. Pins: ${OUT_DIR}/hot-interleave-${BASE}.pins  Log: ${OUT_DIR}/hot-interleave-${BASE}.log"
