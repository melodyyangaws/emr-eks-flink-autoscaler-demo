#!/usr/bin/env bash
# The Trino side of the CDC query tests, the counterpart of duckdb-warm.sh:
# runs a suite on Trino (engine-deploy/deploy-trino.sh: coordinator + workers,
# catalog `iceberg` = the Glue Iceberg catalog), on the pinned snapshot of each
# table, ids from pin.sh (the same pins DuckDB reads as copied metadata.json):
#   arm iceberg   iceberg.<ICEBERG_DB>.<t> FOR VERSION AS OF <snapshot id>
#   arm paimon    iceberg.<PAIMON_DB>.<t> FOR VERSION AS OF <export snapshot id>
#                 (Paimon's hive-catalog export registers the tables in Glue
#                 as table_type=ICEBERG; Trino 484 has no Paimon connector)
# FORMATS=iceberg|paimon|both (default both). An arm that cannot read its
# tables records ERROR per query; the other arm still runs.
#
#   SUITE=hot|snapshot ./querybench/trino.sh run
#   ./querybench/trino.sh render <suite> [arm]   print the rendered SQL (no cluster; dry run)
#
# How a query runs: the trino CLI INSIDE the coordinator container, through
# `kubectl exec` (lib qb_trino_cli). The coordinator image ships the CLI of the
# server's own version and the JDK it needs, so the runner pod (python:3.12-slim)
# installs nothing, and the CLI can never drift from the server. kubectl exec
# costs seconds per call, which is why no time here is taken on the client:
#
# Timing (server side), per query and pass: the query carries a unique
# `--source qb-<run>-<suite>-p<pass>-q<n>` (and `--client-tags`), and its row
# in system.runtime.queries gives elapsed (end - created) and queued_time_ms;
# the recorded time is execution = elapsed - queued (planning + execution +
# delivering the result, as DuckDB's execute + fetch). CPU and physical input
# bytes/rows come from the coordinator's /v1/query/<id> (QueryInfo.queryStats;
# best effort, blank when unreachable). The per-query cap is the session's
# query_max_run_time (HOT_QUERY_TIMEOUT_S / SNAPSHOT_QUERY_TIMEOUT_S, the same
# cap as DuckDB), plus a client deadline 300 s later that kills the query
# (system.runtime.kill_query) and records TIMEOUT.
#
# Same RUNS/WARMUP, pass-major order, median and ZERO_SCAN guard
# (allow-zero-rows, leading-col-is-label) as duckdb-warm.sh and the DuckDB
# server. Results come back as the CLI's JSON lines and are rendered like the
# DuckDB server's rows (fields |-joined, NULL as NULL, Python str() of each
# value), so the summary can compare every row across the engines. Status:
# OK, ERROR, TIMEOUT, OOM (an EXCEEDED_*MEMORY* / CLUSTER_OUT_OF_MEMORY
# failure), ZERO_SCAN, MISSING.
#
# Output: $OUT_DIR/trino-<STAMP>.csv ($QB_CSV_HEADER; median_ms = median
# execution ms; scan_rows / scan_bytes = physical input of the last timed
# pass), -queries.csv (every pass: query id, elapsed, queued, execution, CPU,
# bytes), -rows.tsv (every row of the last timed pass), -firstrows.csv, .log.
#
# Exit: non-zero only when the driver itself fails: Trino is not up with
# TRINO_WORKERS workers (at the start or the end of a suite), a render fails (a
# pin missing), the writers changed under the suite, or no result row was
# written. A query that is not OK (ERROR, TIMEOUT, OOM, ZERO_SCAN) stays in the
# CSV and the summary, and in the suite's last log line ("N of M queries not
# OK: trino:<arm>:<query>=<status> ..."); it does not change the exit.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"

CSV="${OUT_DIR}/trino-${STAMP}.csv"
LOG="${OUT_DIR}/trino-${STAMP}.log"
ROWS_TSV="${CSV%.csv}-rows.tsv"
QUERIES_CSV="${CSV%.csv}-queries.csv"
RUN_ID="${STAMP//[^0-9A-Za-z]/}$$"
MAX_RESULT_ROWS="${MAX_RESULT_ROWS:-10000}"
ARMS="$(qb_formats)"
PY=""
cleanup() { [[ -z "$PY" ]] || rm -f "$PY"; }
trap cleanup EXIT

# One query, one pass: the CLI, then its system.runtime.queries row, then its
# QueryInfo. One JSON object on stdout.
write_py() {
    PY=$(mktemp)
    cat > "$PY" <<'EOF'
import json, os, re, subprocess, sys, time, urllib.request
E = os.environ
sqlfile, tag, suite, name, timeout_s, allow_zero, label_col = sys.argv[1:8]
timeout_s, allow_zero, label_col = int(timeout_s), allow_zero == "1", label_col == "1"
MAXR = int(E.get("MAX_RESULT_ROWS") or 10000)

def cli(sql, extra, timeout):
    cmd = ["kubectl", "exec", "-i", "-n", E["NAMESPACE"], E["TRINO_COORDINATOR"], "-c", E["TRINO_CONTAINER"], "--",
           "env", "JAVA_TOOL_OPTIONS=" + E["TRINO_CLI_JAVA_OPTS"],
           "trino", "--server", "http://localhost:" + E["TRINO_PORT"], "--user", E["TRINO_USER"],
           "--catalog", E["TRINO_CATALOG"], "--schema", E["ICEBERG_DB"]] + extra + ["--execute", sql]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

def stderr_tail(s):
    lines = [l for l in (s or "").splitlines() if l.strip() and not l.startswith("Picked up JAVA_TOOL_OPTIONS")]
    return " ".join(lines[-3:])[:400]

def lookup():
    """The query's row in system.runtime.queries, once it is FINISHED or FAILED."""
    q = ("SELECT query_id, state, CAST(queued_time_ms AS bigint), "
         "to_milliseconds(\"end\" - created), to_milliseconds(\"end\" - started), "
         "coalesce(CAST(error_type AS varchar), ''), coalesce(CAST(error_code AS varchar), '') "
         "FROM system.runtime.queries WHERE source = '%s' ORDER BY created DESC LIMIT 1" % tag)
    last = None
    for _ in range(20):   # the row's "end" is set a moment after the CLI returns
        try:
            r = cli(q, ["--output-format", "TSV", "--source", "querybench-lookup"], 120)
        except subprocess.TimeoutExpired:
            continue
        # not strip(): empty trailing fields (error_type, error_code) are tabs
        f = ([l for l in r.stdout.splitlines() if l.strip()] or [""])[-1].split("\t")
        f = [x.strip('"') for x in f]
        if r.returncode == 0 and len(f) == 7:
            last = f
            if f[1] in ("FINISHED", "FAILED") and f[3] not in ("", "NULL"):
                return f
        time.sleep(2)
    return last

DUR = {"ns": 1e-6, "us": 1e-3, "ms": 1, "s": 1e3, "m": 6e4, "h": 3.6e6, "d": 8.64e7}
SIZE = {"B": 1, "kB": 2**10, "MB": 2**20, "GB": 2**30, "TB": 2**40, "PB": 2**50}
def parse(v, units):
    if isinstance(v, (int, float)):
        return int(v)
    m = re.fullmatch(r"\s*([0-9.]+)\s*([A-Za-z]+)\s*", str(v or ""))
    return int(float(m.group(1)) * units[m.group(2)]) if m and m.group(2) in units else None

def query_stats(qid):
    """CPU and physical input from the coordinator's QueryInfo (best effort)."""
    body = None
    try:
        req = urllib.request.Request(E["TRINO_URL"].rstrip("/") + "/v1/query/" + qid, headers={"X-Trino-User": E["TRINO_USER"]})
        body = urllib.request.urlopen(req, timeout=30).read()
    except Exception:
        try:
            body = subprocess.run(["kubectl", "exec", "-n", E["NAMESPACE"], E["TRINO_COORDINATOR"], "-c", E["TRINO_CONTAINER"],
                                   "--", "curl", "-sf", "-H", "X-Trino-User: " + E["TRINO_USER"],
                                   "http://localhost:%s/v1/query/%s" % (E["TRINO_PORT"], qid)],
                                  capture_output=True, timeout=60, check=True).stdout
        except Exception:
            return {}
    try:
        s = json.loads(body).get("queryStats", {})
    except ValueError:
        return {}
    return {"cpu_ms": parse(s.get("totalCpuTime"), DUR),
            "physical_input_bytes": parse(s.get("physicalInputDataSize"), SIZE),
            "physical_input_rows": s.get("physicalInputPositions"),
            "info_elapsed_ms": parse(s.get("elapsedTime"), DUR),
            "info_queued_ms": parse(s.get("queuedTime"), DUR),
            "info_execution_ms": parse(s.get("executionTime"), DUR)}

def text(v):
    if v is None:
        return "NULL"
    if isinstance(v, (list, dict)):
        return json.dumps(v, separators=(",", ":"))
    return str(v)          # bool -> True/False, float -> repr: as the DuckDB server's str()

sql = open(sqlfile).read()
res = {"status": "OK", "tag": tag}
try:
    r = cli(sql, ["--output-format", "JSON", "--source", tag,
                  "--client-tags", "querybench,cdc,%s,%s" % (suite, name),
                  "--session", "query_max_run_time=%ds" % timeout_s], timeout_s + 300)
    rc, out, err = r.returncode, r.stdout, r.stderr
except subprocess.TimeoutExpired:
    rc, out, err = None, "", ""
row = lookup()
if row:
    res.update(query_id=row[0], state=row[1])
    try:
        res["queued_ms"], res["elapsed_ms"] = int(row[2]), int(row[3])
        res["exec_ms"] = res["elapsed_ms"] - res["queued_ms"]       # the recorded time
        res["end_minus_started_ms"] = int(row[4])
    except ValueError:
        pass
if rc is None:
    res.update(status="TIMEOUT", error="client deadline (%ds)" % (timeout_s + 300))
    if row and row[1] not in ("FINISHED", "FAILED"):
        try:
            cli("CALL system.runtime.kill_query(query_id => '%s', message => 'querybench client deadline')" % row[0],
                ["--source", "querybench-kill"], 120)
        except subprocess.TimeoutExpired:
            pass
elif not row:
    res.update(status="ERROR", error="no row in system.runtime.queries for source %s (cli rc %s: %s)" % (tag, rc, stderr_tail(err)))
elif row[1] != "FINISHED" or rc != 0:
    code = row[6] if len(row) > 6 else ""
    st = ("TIMEOUT" if code == "EXCEEDED_TIME_LIMIT" else
          "OOM" if "MEMORY" in code or code == "CLUSTER_OUT_OF_MEMORY" else "ERROR")
    res.update(status=st, error="%s %s %s" % (row[1], code, stderr_tail(err)))
if res.get("query_id"):
    res.update(query_stats(res["query_id"]))
rows = []
if res["status"] == "OK":
    n = 0
    for line in out.splitlines():
        if not line.strip():
            continue
        try:
            vals = json.loads(line, object_pairs_hook=lambda p: [v for _, v in p])   # duplicate column names kept
        except ValueError:
            continue
        n += 1
        if len(rows) < MAXR:
            rows.append("|".join(text(v) for v in vals))
    res["result_rows"] = n
    first = rows[0] if rows else ""
    if not allow_zero:
        v = first.split("|", 1)[0]
        if not rows or (not label_col and v in ("0", "NULL", "")):
            res["status"] = "ZERO_SCAN"
    if res.get("exec_ms") is None and res["status"] == "OK":
        res.update(status="ERROR", error="no server-side time for %s" % tag)
    res["first_row"] = first[:200]
res["rows"] = rows
print(json.dumps(res))
EOF
}

jget() { python3 -c 'import json,sys; d=json.load(sys.stdin); v=d.get(sys.argv[1]); print("" if v is None else v)' "$1"; }

run_pass() {   # suite query-file rendered-sql-file tag
    local f="$2"
    NAMESPACE="$NAMESPACE" TRINO_COORDINATOR="$TRINO_COORDINATOR" TRINO_CONTAINER="$TRINO_CONTAINER" \
    TRINO_CLI_JAVA_OPTS="$TRINO_CLI_JAVA_OPTS" TRINO_PORT="$TRINO_PORT" TRINO_USER="$TRINO_USER" \
    TRINO_CATALOG="$TRINO_CATALOG" ICEBERG_DB="$ICEBERG_DB" TRINO_URL="$TRINO_URL" MAX_RESULT_ROWS="$MAX_RESULT_ROWS" \
    python3 "$PY" "$3" "$4" "$1" "$(qb_query_name "$f")" "$(qb_query_timeout "$1")" \
        "$(qb_query_flag "$f" allow-zero-rows && echo 1 || echo 0)" \
        "$(qb_query_flag "$f" leading-col-is-label && echo 1 || echo 0)" \
      || echo '{"status": "ERROR", "error": "trino.sh query helper failed"}'
}

# All passes of all queries, pass-major (as duckdb-warm.sh). A query that fails
# on any pass is not run again; its CSV status is the first non-OK.
run_suite_passes() {   # suite arm rendered-dir rows-file
    local s="$1" arm="$2" dir="$3" rows="$4" files f pass n name j st q
    files=$(qb_query_files "$s")
    q="${dir}/${arm}"; mkdir -p "$q"
    for (( pass = 0; pass < WARMUP + RUNS; pass++ )); do
        n=0
        for f in $files; do
            n=$((n + 1)); name=$(qb_query_name "$f")
            [[ -f "${q}/${n}.bad" ]] && continue
            j=$(run_pass "$s" "$f" "${dir}/${arm}.$(printf '%02d' "$n").sql" "qb-${RUN_ID}-${s}-${arm}-p${pass}-q${n}")
            st=$(jget status <<<"$j")
            python3 - "$j" "$name" "$pass" "$WARMUP" "$q" "$n" "$s" "$QUERIES_CSV" "$arm" 2>>"$LOG" <<'EOF'
import csv, json, os, sys
j, name, p, w, q, n, suite, qcsv, arm = json.loads(sys.argv[1]), sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9]
tag = "warm-up" if p < w else "run %d" % (p - w + 1)
print("  %-7s %-30s %-8s %9s ms exec (%s elapsed, %s queued, %s cpu)  %s  first_row=%s%s" % (
      arm, name, tag, j.get("exec_ms", "-"), j.get("elapsed_ms", "-"), j.get("queued_ms", "-"), j.get("cpu_ms", "-"),
      j["status"], (j.get("first_row") or "")[:80], ("  error=" + j["error"]) if j.get("error") else ""), file=sys.stderr)
cols = ["arm", "suite", "pass", "query", "status", "query_id", "elapsed_ms", "queued_ms", "exec_ms", "cpu_ms",
        "physical_input_bytes", "physical_input_rows", "result_rows", "error"]
new = not os.path.exists(qcsv)
with open(qcsv, "a", newline="") as fh:
    wr = csv.writer(fh)
    if new:
        wr.writerow(cols)
    wr.writerow([arm, suite, p, name] + ["" if j.get(k) is None else j.get(k) for k in cols[4:]])
if j["status"] == "OK" and p >= w:
    open("%s/%s.times" % (q, n), "a").write("%d\n" % j["exec_ms"])
    open("%s/%s.last" % (q, n), "w").write(json.dumps(j))
EOF
            if [[ "$st" != OK ]]; then
                echo "$st" > "${q}/${n}.bad"
                qb_log "[trino:${arm}/${s}] ${name} pass ${pass}: ${st} ($(jget error <<<"$j" | cut -c1-160)); not run again"
            else
                qb_log "[trino:${arm}/${s}] ${name} pass ${pass}: OK $(jget exec_ms <<<"$j") ms"
            fi
        done
    done
    n=0
    for f in $files; do
        n=$((n + 1)); name=$(qb_query_name "$f")
        local med="" st="OK" rr=0 sb="" srr=""
        [[ -f "${q}/${n}.times" ]] && med=$(qb_median < "${q}/${n}.times")
        [[ -f "${q}/${n}.bad" ]] && st=$(<"${q}/${n}.bad")
        [[ -z "$med" && "$st" == OK ]] && st=ERROR
        if [[ -f "${q}/${n}.last" ]]; then
            rr=$(jget result_rows < "${q}/${n}.last"); sb=$(jget physical_input_bytes < "${q}/${n}.last"); srr=$(jget physical_input_rows < "${q}/${n}.last")
        fi
        echo "${STAMP},trino,${arm},${s},${name},${st},${med},${rr},${srr},${sb}" >> "$rows"
        if [[ -f "${q}/${n}.last" ]]; then
            echo "${arm},${s},${name},$(jget first_row < "${q}/${n}.last")" >> "${CSV%.csv}-firstrows.csv"
            python3 - "${q}/${n}.last" "$arm" "$s" "$name" >> "$ROWS_TSV" <<'EOF'
import json, sys
j = json.load(open(sys.argv[1]))
for i, r in enumerate(j.get("rows") or [], 1):
    print("\t".join([sys.argv[2], sys.argv[3], sys.argv[4], str(i), r.replace("\t", " ").replace("\n", " ")]))
EOF
        fi
        printf '  %-7s %-30s MEDIAN %9s ms  %s\n' "$arm" "$name" "${med:--}" "$st" >> "$LOG"
    done
}

render_suite_dir() {   # suite dir
    local f n arm
    for arm in $ARMS; do
        n=0
        for f in $(qb_query_files "$1"); do
            n=$((n + 1)); qb_render "$f" trino "$arm" > "${2}/${arm}.$(printf '%02d' "$n").sql" || qb_die "render failed: ${f} (trino/${arm})"
        done
    done
}

run_suite() {   # suite
    local s="$1" dir writers rows bad arm n
    dir=$(mktemp -d); rows="${dir}/rows.csv"; : > "$rows"
    qb_start_suite "$s"
    # H8's key: exported by hot-interleave.sh; resolved here when run by hand.
    if [[ "$s" == hot && -z "${QB_LOOKUP_ORDER_ID:-}" ]] && grep -q 'LOOKUP_ORDER' "${QB_DIR}"/hot/*.sql; then
        qb_resolve_lookup
    fi
    render_suite_dir "$s" "$dir"
    qb_trino_ready || qb_die "Trino is not up with ${TRINO_WORKERS} active workers: ./engine-deploy/deploy-trino.sh up"
    writers=$(qb_writer_state)
    { echo "# ---- suite=${s} arms=${ARMS} runs=${RUNS} warmup=${WARMUP} run_id=${RUN_ID} writers: ${writers}"
      echo "# trino: $(qb_trino 'SELECT node_version FROM system.runtime.nodes WHERE coordinator = true' 60 | head -1) workers=$(qb_trino_workers) catalog=${TRINO_CATALOG} dbs=${ICEBERG_DB},${PAIMON_DB}"
      echo "# pins: $(for a in $ARMS; do for t in $QB_TABLES; do v="QB_PIN_SNAP_$(qb_upper "$a")_$(qb_upper "$t")"; printf '%s.%s@%s ' "$a" "$t" "${!v:-}"; done; done)"; } >> "$LOG"
    for arm in $ARMS; do
        qb_log "${s}: trino arm ${arm}..."
        echo "# arm=${arm}" >> "$LOG"
        run_suite_passes "$s" "$arm" "$dir" "$rows"
    done
    qb_writers_unchanged "$writers" "$s" || { rm -rf "$dir"; return 1; }
    n=$(grep -c . "$rows" || true)
    (( n > 0 )) || { qb_log "ERROR: ${s}: no result rows written"; rm -rf "$dir"; return 1; }
    { echo "# suite=${s} writers: ${writers}"; echo "# suite=${s} runs=${RUNS} warmup=${WARMUP} run_id=${RUN_ID} time=execution (elapsed - queued), system.runtime.queries"; } >> "$CSV"
    cat "$rows" >> "$CSV"
    # Not-OK queries are results, not a driver failure: logged, exit unchanged.
    bad=$(awk -F, '$6 != "OK" { printf " trino:%s:%s=%s", $3, $5, $6 }' "$rows")
    if [[ -n "$bad" ]]; then
        qb_log "${s}: $(wc -w <<<"$bad" | tr -d ' ') of ${n} queries not OK:${bad}"
    else
        qb_log "${s}: ${n} queries, all OK"
    fi
    echo "# suite=${s} queries=${n} not OK:${bad:- none}" >> "$LOG"
    rm -rf "$dir"
    qb_trino_ready || { qb_log "ERROR: Trino lost its ${TRINO_WORKERS} active workers during the ${s} suite"; return 1; }
}

cmd_run() {
    local s rc=0
    mkdir -p "$OUT_DIR"
    write_py
    qb_assert_alone trino
    qb_csv_init "$CSV"
    qb_log "trino: suite=${SUITE} runs=${RUNS}+${WARMUP} stamp=${STAMP}"
    for s in $(qb_suites); do run_suite "$s" || rc=1; done
    qb_log "results: ${CSV}  log: ${LOG}"
    return $rc
}

case "${1:-run}" in
    run)    cmd_run ;;
    render) s="${2:?render <hot|snapshot> [arm]}"
            for arm in ${3:-$ARMS}; do
                for f in $(qb_query_files "$s"); do echo "-- trino/${arm}: $(qb_query_name "$f")"; qb_render "$f" trino "$arm"; done
            done ;;
    *)      qb_die "usage: $0 run | render <suite> [arm]" ;;
esac
