#!/usr/bin/env bash
# Hot suite over the LIVE CDC tables, StarRocks vs warm DuckDB, engines
# INTERLEAVED pass by pass on the same pinned table versions. Adapted from
# throughput-benchmark/querybench/hot-interleave.sh.
#
#   ./querybench/hot-interleave.sh        (run.sh's runner step; by hand: the laptop must stay up)
#
# 0. Both CDC writers RUNNING (once a minute, STABLE_CHECKS=10 times); the
#    generator's state is logged (a stopped generator = not a writers-live run).
# 1. The window: HOT_WINDOW_SECONDS (300) ending HOT_LAG_SECONDS (300) before now.
# 2. Waits until BOTH formats' fact tables (HOT_COMMIT_TABLES: orders
#    order_items) have committed a snapshot at least HOT_COMMIT_MARGIN_S (60)
#    after HOT_TO, up to WAIT_COMMIT_S (1800). Not throughput's "same row count
#    in both tables": under live upserts the rows in an updated_at window keep
#    moving (a re-updated key leaves it), the two writers checkpoint at
#    different moments, and Paimon's Iceberg export only lists top-LSM-level
#    files, so equal counts need not ever happen. H1's row per arm records what
#    each arm saw; the summary shows the differences, it does not fail on them.
# 3. Pins every table in both formats (pin.sh) and picks H8's key
#    (qb_resolve_lookup), once.
# 4. Warm-up pass p0 (discarded), then RUNS timed passes; each pass runs
#    StarRocks (all arms; on an Iceberg v3 set paimon alone, lib/querybench.sh)
#    then DuckDB (both arms). HOT_REPIN=pass (default)
#    pins the tables again before every pass: Paimon keeps 5-10 snapshots
#    (snapshot.num-retained.max=10, 60 s checkpoints, so ~10 min) and expiry
#    deletes files an older pin still lists. HOT_REPIN=run keeps the first pin
#    for every pass (then passes must stay well under ~10 min).
# 5. Medians of the timed passes -> starrocks-<STAMP>.csv / duckdb_warm-<STAMP>.csv;
#    every pass's CSVs (-p<N>), the last pass's rows; hot-interleave-<STAMP>.pins.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SUITE=hot
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
RUNS="${RUNS:-3}"
WAIT_COMMIT_S="${WAIT_COMMIT_S:-1800}"
STABLE_CHECKS="${STABLE_CHECKS:-10}"
HOT_REPIN="${HOT_REPIN:-pass}"
case "$HOT_REPIN" in pass|run) ;; *) qb_die "HOT_REPIN must be pass|run" ;; esac
BASE="$STAMP"; export STAMP
PINS="${OUT_DIR}/hot-interleave-${BASE}.pins"
mkdir -p "$OUT_DIR"

# ── 0. Writers RUNNING ───────────────────────────────────────────────────────
want=$(for j in $FLINK_JOBS; do printf 'RUNNING '; done); want="${want% }"
for (( c = 1; ; c++ )); do
    # shellcheck disable=SC2086
    s=$(kubectl get flinkdeployment $FLINK_JOBS -n "$NAMESPACE" -o jsonpath='{.items[*].status.jobStatus.state}' 2>/dev/null || true)
    [[ "$s" == "$want" ]] && { qb_log "writers RUNNING (check ${c})"; break; }
    (( c < STABLE_CHECKS )) || qb_die "the writers are not all RUNNING after ${STABLE_CHECKS} checks: ${s:-none}"
    qb_log "writers not all RUNNING yet (${s:-none}), check ${c}/${STABLE_CHECKS}; again in 60s"
    sleep 60
done
"${HERE}/starrocks.sh" catalogs || qb_die "cannot create or read the StarRocks catalogs"

# ── 1-2. The window, committed past its end in both formats ─────────────────
qb_resolve_hot
export HOT_TO="$QB_HOT_TO" HOT_FROM="$QB_HOT_FROM" HOT_DT="$QB_HOT_DT"
qb_log "hot-interleave ${BASE}: [${HOT_FROM}, ${HOT_TO}) dt=${HOT_DT}, ${RUNS} timed passes, repin per ${HOT_REPIN}"
to_s=$(python3 -c 'import sys, datetime as d; print(int(d.datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S").replace(tzinfo=d.timezone.utc).timestamp()))' "$HOT_TO")
need_ms=$(( (to_s + ${HOT_COMMIT_MARGIN_S:-60}) * 1000 ))
t0=$(date +%s)
while :; do
    late=""
    for fmt in iceberg paimon; do
        for t in ${HOT_COMMIT_TABLES:-orders order_items}; do
            ms=$(qb_commit_ms "$fmt" "$t" || true)
            [[ "$ms" =~ ^[0-9]+$ && "$ms" -ge "$need_ms" ]] || late+=" ${fmt}.${t}@${ms:-?}"
        done
    done
    [[ -z "$late" ]] && break
    (( $(date +%s) - t0 < WAIT_COMMIT_S )) || qb_die "not committed past HOT_TO+${HOT_COMMIT_MARGIN_S:-60}s after ${WAIT_COMMIT_S}s:${late}"
    qb_log "waiting for commits past the window:${late}"
    sleep 30
done
qb_log "both formats committed past the window"

gen_start=$(qb_generator_state)
qb_log "generator before the first pass: ${gen_start:-none}"
qb_generator_live || qb_log "WARNING: the generator is not running: this is not a writers-live hot run"

# ── 3-4. Pin, then the interleaved passes ───────────────────────────────────
pin() {   # <tag>
    local p
    p=$("${HERE}/pin.sh" "$1") || qb_die "pin ${1} failed"
    eval "$p"
    { echo "## pins ${1}"; qb_pins_dump; echo "SR_PIN_FORM=${SR_PIN_FORM:-clause}"; } >> "$PINS"
}
{ echo "stamp=${BASE} window=[${HOT_FROM}, ${HOT_TO}) dt=${HOT_DT} repin=${HOT_REPIN}"
  qb_run_meta
  echo "generator_before=${gen_start:-none}"; } > "$PINS"
pin "${BASE}-p0"
qb_resolve_lookup
export QB_LOOKUP_ORDER_ID QB_LOOKUP_ORDER_DT
echo "h8_key order_id=${QB_LOOKUP_ORDER_ID} dt=${QB_LOOKUP_ORDER_DT} (${QB_LOOKUP_NOTE})" >> "$PINS"

for (( pass = 0; pass <= RUNS; pass++ )); do
    (( pass > 0 )) && [[ "$HOT_REPIN" == pass ]] && pin "${BASE}-p${pass}"
    # shellcheck disable=SC2046
    export $(compgen -v | grep -E '^(QB_PIN_|SR_PIN_FORM$)') 2>/dev/null || true
    qb_log "pass ${pass}/${RUNS} ($([[ $pass == 0 ]] && echo warm-up || echo timed)): StarRocks"
    STAMP="${BASE}-p${pass}" RUNS=1 WARMUP=0 "${HERE}/starrocks.sh" >> "${OUT_DIR}/hot-interleave-${BASE}.log" 2>&1 \
        || qb_log "WARNING: StarRocks pass ${pass} exited non-zero (see the log)"
    qb_log "pass ${pass}/${RUNS}: DuckDB"
    PROFILE_SCAN=$(( pass == RUNS && ${PROFILE_SCAN:-1} != 0 ? 1 : 0 )) \
    STAMP="${BASE}-p${pass}" RUNS=1 WARMUP=0 "${HERE}/duckdb-warm.sh" run >> "${OUT_DIR}/hot-interleave-${BASE}.log" 2>&1 \
        || qb_log "WARNING: DuckDB pass ${pass} exited non-zero (see the log)"
done

gen_end=$(qb_generator_state)
echo "generator_after=${gen_end:-none}" >> "$PINS"
qb_log "generator after the last pass: ${gen_end:-none}"
qb_generator_live || qb_log "WARNING: the generator stopped before the last pass ended"

# ── 5. Medians of the timed passes; the last pass's rows ────────────────────
python3 - "$OUT_DIR" "$BASE" "$RUNS" "$QB_CSV_HEADER" <<'EOF'
import csv, os, shutil, statistics, sys
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
    for suffix in ("-rows.tsv", "-firstrows.csv"):
        src = os.path.join(out, "%s-%s-p%d%s" % (eng, base, runs, suffix))
        if os.path.exists(src):
            shutil.copy(src, os.path.join(out, "%s-%s%s" % (eng, base, suffix)))
    ev = [os.path.join(out, "%s-%s-p%d-events.csv" % (eng, base, p)) for p in range(0, runs + 1)]
    lines = []
    for p, f in enumerate(ev):
        if os.path.exists(f):
            lines += ["%d,%s" % (p, l.rstrip("\n")) for l in open(f) if l.strip() and not l.startswith("arm,")]
    if lines:
        open(os.path.join(out, "%s-%s-events.csv" % (eng, base)), "w").write("hot_pass,arm,suite,pass,query,event\n" + "\n".join(lines) + "\n")
    print("%s: %s" % (eng, os.path.join(out, "%s-%s.csv" % (eng, base))))
EOF
qb_log "done. Pins: ${PINS}  Log: ${OUT_DIR}/hot-interleave-${BASE}.log"
