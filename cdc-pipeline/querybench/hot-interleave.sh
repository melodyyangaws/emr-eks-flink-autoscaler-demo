#!/usr/bin/env bash
# Hot suite over the LIVE CDC tables, warm DuckDB vs Trino on Iceberg and on
# Paimon (default: its Iceberg v3 export; PAIMON_READER=native: the native
# readers) (ENGINES x FORMATS: up to four arms), engines
# INTERLEAVED pass by pass on the same pinned table versions. Adapted from
# throughput-benchmark/querybench/hot-interleave.sh.
#
#   ./querybench/hot-interleave.sh        (run.sh's runner step; by hand: the laptop must stay up)
#
# 0. The queried writers RUNNING (QB_QUERY_WRITERS: flink-cdc-<format> per
#    format in FORMATS; once a minute, STABLE_CHECKS=10 times); the generator's
#    state is logged (a stopped generator = not a writers-live run).
# 1. The window: HOT_WINDOW_SECONDS (30) ending HOT_LAG_SECONDS (300) before now.
# 2. Waits until every format's fact tables (HOT_COMMIT_TABLES: orders
#    order_items) have committed a snapshot at least HOT_COMMIT_MARGIN_S (60)
#    after HOT_TO, up to WAIT_COMMIT_S (1800). Not "same row count in both
#    formats": under live upserts the rows in an updated_at window keep moving,
#    the writers checkpoint at different moments and the export lags, so H1's
#    row per arm records what each arm saw (the summary shows the gap).
# 3. Pins every table in every format (pin.sh: one metadata.json copy per
#    table and format, whose snapshot id Trino reads), and re-pins every 60 s
#    until every format's PINNED fact tables hold a row at or past HOT_TO
#    (orders.updated_at, order_items.created_at in HOT_DT), up to
#    HOT_DATA_WAIT_S (3600; 0 skips): a commit past the window can still carry
#    older binlog events. Then picks H8's key (qb_resolve_lookup), once.
# 4. Warm-up pass p0 (discarded), then RUNS timed passes; each pass runs every
#    engine in ENGINES (default "duckdb trino", in that order; ENGINES=trino or
#    duckdb runs one alone), each on its arms (FORMATS), on the SAME pins.
#    HOT_REPIN=pass (default) pins the tables again before every pass, so
#    each pass reads the live tables as they are when it starts, not a version
#    one more pass older each time. HOT_REPIN=run keeps the first pin for every
#    pass. Either is safe for expiry: the Paimon sinks keep snapshots 12 h
#    (snapshot.time-retained 12h, num-retained min 5 / max 1000,
#    ingest-deploy/sql-scripts/paimon/03-paimon-sinks.sql), as the Iceberg tables do, so the
#    files a pin lists outlive a run.
# 5. Medians of the timed passes -> duckdb_warm-<STAMP>.csv / trino-<STAMP>.csv;
#    every pass's CSVs (-p<N>), the last pass's rows; hot-interleave-<STAMP>.pins.
#
# Exit: a query that is not OK is a result (in the CSVs and the summary, and
# listed in the "N of M queries not OK" line at the end); it never changes the
# exit, and the drivers do not exit non-zero for one. A pass whose driver exits
# non-zero (engine not up, render/pin failure, no rows: duckdb-warm.sh /
# trino.sh "Exit") does not stop the other passes, but makes this script exit 1
# once the medians are written, so the watcher leaves the run up.
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
ENGINE_LIST=$(qb_engines)
export ENGINES="$ENGINE_LIST"
BASE="$STAMP"; export STAMP
PINS="${OUT_DIR}/hot-interleave-${BASE}.pins"
mkdir -p "$OUT_DIR"

# ── 0. Writers RUNNING ───────────────────────────────────────────────────────
want=$(for j in $QB_QUERY_WRITERS; do printf 'RUNNING '; done); want="${want% }"
for (( c = 1; ; c++ )); do
    s=$(for j in $QB_QUERY_WRITERS; do kubectl get flinkdeployment "$j" -n "$NAMESPACE" -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true; printf ' '; done)
    s="${s% }"
    [[ "$s" == "$want" ]] && { qb_log "writers RUNNING: ${QB_QUERY_WRITERS} (check ${c}); all: $(qb_writer_state "$FLINK_JOBS")"; break; }
    (( c < STABLE_CHECKS )) || qb_die "the queried writers (${QB_QUERY_WRITERS}) are not all RUNNING after ${STABLE_CHECKS} checks: ${s:-none}"
    qb_log "writers not all RUNNING yet (${s:-none}), check ${c}/${STABLE_CHECKS}; again in 60s"
    sleep 60
done

# ── 1-2. The window, committed past its end ─────────────────────────────────
qb_resolve_hot
export HOT_TO="$QB_HOT_TO" HOT_FROM="$QB_HOT_FROM" HOT_DT="$QB_HOT_DT"
qb_log "hot-interleave ${BASE}: [${HOT_FROM}, ${HOT_TO}) dt=${HOT_DT}, ${RUNS} timed passes, engines ${ENGINE_LIST}, formats $(qb_formats), repin per ${HOT_REPIN}"
to_s=$(python3 -c 'import sys, datetime as d; print(int(d.datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S").replace(tzinfo=d.timezone.utc).timestamp()))' "$HOT_TO")
need_ms=$(( (to_s + ${HOT_COMMIT_MARGIN_S:-60}) * 1000 ))
t0=$(date +%s)
while :; do
    late=""
    for fmt in $(qb_formats); do
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
qb_log "every format committed past the window ($(qb_formats))"

gen_start=$(qb_generator_state)
qb_log "generator before the first pass: ${gen_start:-none}"
qb_generator_live || qb_log "WARNING: the generator is not running: this is not a writers-live hot run"

# ── 3-4. Pin, then the interleaved passes ───────────────────────────────────
pin() {   # <tag>
    local p
    p=$("${HERE}/pin.sh" "$1") || qb_die "pin ${1} failed"
    eval "$p"
    { echo "## pins ${1}"; qb_pins_dump; } >> "$PINS"
}
{ echo "stamp=${BASE} window=[${HOT_FROM}, ${HOT_TO}) dt=${HOT_DT} repin=${HOT_REPIN} engines=${ENGINE_LIST// /,} formats=$(qb_formats | tr ' ' ',')"
  qb_run_meta
  echo "generator_before=${gen_start:-none}"; } > "$PINS"
# 3a. Data past the window, not just a commit past it: in a binlog backlog a writer
# commits every checkpoint but with events minutes old, so the commit wait above
# passes while the window's rows are not in the table yet (_v3final2, 2026-11-06:
# Paimon ~17 min behind, 5 of 9 Paimon hot queries ZERO_SCAN). Pin, then on the
# PINNED tables of every format require a fact row at or past HOT_TO (orders by
# updated_at, order_items by created_at, in partition HOT_DT); otherwise wait 60 s
# and pin again, up to HOT_DATA_WAIT_S (3600). HOT_DATA_WAIT_S=0 skips the check.
data_behind() {   # prints the formats/tables with no row at or past HOT_TO on the current pins
    local e fmt t col r out=""
    e=$(qb_setup_engine)
    for fmt in $(qb_formats); do
        for t in orders order_items; do
            [[ "$t" == orders ]] && col=updated_at || col=created_at
            r=$(qb_setup_query "$e" "$fmt" "SELECT COUNT(*) FROM (SELECT 1 FROM \${$(qb_upper "$t")} AS x
                     WHERE x.$(qb_part_col "$fmt" "$t") = '${HOT_DT}' AND x.${col} >= TIMESTAMP '${HOT_TO}' LIMIT 1) AS y" 600 | tail -1) || r=""
            [[ "$r" == 1 ]] || out+=" ${fmt}.${t}(${r:-error})"
        done
    done
    echo "$out"
}
data_t0=$(date +%s)
while :; do
    p=$("${HERE}/pin.sh" "${BASE}-p0") || qb_die "pin ${BASE}-p0 failed"
    eval "$p"
    [[ "${HOT_DATA_WAIT_S:-3600}" == 0 ]] && { qb_log "HOT_DATA_WAIT_S=0: not checking the window's data"; break; }
    behind=$(data_behind)
    [[ -z "$behind" ]] && { qb_log "every format has data past the window on its pins (waited $(( $(date +%s) - data_t0 ))s)"; break; }
    (( $(date +%s) - data_t0 < ${HOT_DATA_WAIT_S:-3600} )) \
        || qb_die "no data at or past HOT_TO=${HOT_TO} after ${HOT_DATA_WAIT_S:-3600}s:${behind} (a writer is still behind; HOT_DATA_WAIT_S=0 skips the check)"
    qb_log "waiting for data past the window (re-pinning in 60s):${behind}"
    sleep 60
done
echo "data_wait_s=$(( $(date +%s) - data_t0 ))" >> "$PINS"
{ echo "## pins ${BASE}-p0"; qb_pins_dump; } >> "$PINS"
qb_resolve_lookup
export QB_LOOKUP_ORDER_ID QB_LOOKUP_ORDER_DT
echo "h8_key order_id=${QB_LOOKUP_ORDER_ID} dt=${QB_LOOKUP_ORDER_DT} (${QB_LOOKUP_NOTE})" >> "$PINS"

failed=""
for (( pass = 0; pass <= RUNS; pass++ )); do
    (( pass > 0 )) && [[ "$HOT_REPIN" == pass ]] && pin "${BASE}-p${pass}"
    # shellcheck disable=SC2046
    export $(compgen -v | grep -E '^QB_PIN_') 2>/dev/null || true
    for e in $ENGINE_LIST; do
        qb_log "pass ${pass}/${RUNS} ($([[ $pass == 0 ]] && echo warm-up || echo timed)): ${e}"
        case "$e" in
            duckdb) PROFILE_SCAN=$(( pass == RUNS && ${PROFILE_SCAN:-1} != 0 ? 1 : 0 )) \
                    STAMP="${BASE}-p${pass}" RUNS=1 WARMUP=0 "${HERE}/duckdb-warm.sh" run >> "${OUT_DIR}/hot-interleave-${BASE}.log" 2>&1 \
                        || { failed+=" duckdb:p${pass}"; qb_log "ERROR: the DuckDB driver failed in pass ${pass} (see the log)"; } ;;
            trino)  STAMP="${BASE}-p${pass}" RUNS=1 WARMUP=0 "${HERE}/trino.sh" run >> "${OUT_DIR}/hot-interleave-${BASE}.log" 2>&1 \
                        || { failed+=" trino:p${pass}"; qb_log "ERROR: the Trino driver failed in pass ${pass} (see the log)"; } ;;
        esac
    done
done

gen_end=$(qb_generator_state)
echo "generator_after=${gen_end:-none}" >> "$PINS"
qb_log "generator after the last pass: ${gen_end:-none}"
qb_generator_live || qb_log "WARNING: the generator stopped before the last pass ended"

# ── 5. Medians of the timed passes; the last pass's rows ────────────────────
python3 - "$OUT_DIR" "$BASE" "$RUNS" "$QB_CSV_HEADER" <<'EOF'
import csv, os, shutil, statistics, sys
out, base, runs, header = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
total, notok = 0, []
for eng in ("duckdb_warm", "trino"):
    rows = {}
    for p in range(1, runs + 1):
        f = os.path.join(out, "%s-%s-p%d.csv" % (eng, base, p))
        if not os.path.exists(f):
            continue
        for r in csv.DictReader(l for l in open(f) if not l.startswith("#")):
            rows.setdefault((r["arm"], r["query"]), []).append(r)
    if not rows:
        continue
    with open(os.path.join(out, "%s-%s.csv" % (eng, base)), "w") as w:
        w.write(header + "\n")
        for (arm, q), rs in sorted(rows.items()):
            bad = [r["status"] for r in rs if r["status"] != "OK"]
            ms = [int(r["median_ms"]) for r in rs if r["status"] == "OK" and r["median_ms"]]
            med = int(statistics.median(ms)) if ms and not bad and len(ms) == runs else ""
            status = bad[0] if bad else ("OK" if med != "" else "MISSING")
            last = rs[-1]
            w.write(",".join([base, last["engine"], arm, "hot", q, status, str(med), last["result_rows"],
                              last.get("scan_rows", ""), last.get("scan_bytes", ""), ""]) + "\n")
            total += 1
            if status != "OK":
                notok.append("%s:%s:%s=%s" % ("duckdb" if eng == "duckdb_warm" else eng, arm, q, status))
    for suffix in ("-rows.tsv", "-firstrows.csv"):
        src = os.path.join(out, "%s-%s-p%d%s" % (eng, base, runs, suffix))
        if os.path.exists(src):
            shutil.copy(src, os.path.join(out, "%s-%s%s" % (eng, base, suffix)))
    # Per-pass detail: DuckDB's restart events, Trino's server-side breakdown.
    for suffix, head in (("-events.csv", "hot_pass,arm,suite,pass,query,event"), ("-queries.csv", None)):
        lines = []
        for p in range(0, runs + 1):
            f = os.path.join(out, "%s-%s-p%d%s" % (eng, base, p, suffix))
            if not os.path.exists(f):
                continue
            ls = [l.rstrip("\n") for l in open(f) if l.strip()]
            if ls and head is None:
                head = "hot_pass," + ls[0]
            lines += ["%d,%s" % (p, l) for l in ls[1:]]
        if lines:
            open(os.path.join(out, "%s-%s%s" % (eng, base, suffix)), "w").write(head + "\n" + "\n".join(lines) + "\n")
    print("%s: %s" % (eng, os.path.join(out, "%s-%s.csv" % (eng, base))))
# Not-OK queries are results, not a failure: listed here, exit unchanged.
print("hot: %d of %d queries not OK: %s" % (len(notok), total, " ".join(notok)) if notok else "hot: %d queries, all OK" % total)
EOF
qb_log "done. Pins: ${PINS}  Log: ${OUT_DIR}/hot-interleave-${BASE}.log"
if [[ -n "$failed" ]]; then
    echo "driver_failed=${failed# }" >> "$PINS"
    qb_die "driver failures in passes:${failed} (results written; see the log)"
fi
