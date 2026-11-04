#!/usr/bin/env python3
"""The CDC query benchmark's findings as one Markdown file, from one hot and/or
one snapshot run (querybench/run.sh summary gathers them from S3).

  summarize.py --work <dir with hot/ and/or snapshot/> --session <name>
               [--hot <STAMP>] [--snapshot <STAMP>] [--iceberg-format-version 3] [--warmup 1]
               [--ingest <ingest_stats.py JSON>] [--ingest-error <why it is missing>] --out <md>

Layout (modelled on bench-results/SUMMARY-20260927.md): Run configuration;
1. Headline (computed bullets); 2. Query latency (median per query and arm,
geometric means); 3. Ingest (Paimon vs Iceberg over the run's window, from
ingest_stats.py); 4. Table state at the window end; Conclusions / Open items;
then <details> blocks with everything else, so no number is dropped:

Up to four arms: warm DuckDB (`duckdb_warm`) and Trino (`trino`), each on the
Iceberg tables (`iceberg`) and on Paimon (`paimon`: the native readers since
2026-11-04, `paimon_reader=native` in the pins; older runs read Paimon's Iceberg
export, and keep that label and its caveats). Both
engines of a format read the same pinned snapshot per table. Per suite:
  - median seconds per query and arm, and Trino / DuckDB per format;
  - geometric means: Trino vs DuckDB per format, Paimon vs Iceberg
    per engine, over the queries both sides ran OK;
  - (details) Trino's server-side breakdown (elapsed, queued, execution, CPU,
    input bytes; trino-<STAMP>-queries.csv) and the data each arm scanned;
  - (details) result equality, every row of the last timed pass compared as
    text (the queries print engine-neutral text: DECIMAL(38,2) / BIGINT sums,
    timestamps as their first 19 characters; numbers and timestamps are
    normalised once more here). DuckDB vs Trino on the SAME format must match:
    a difference on a query both ran OK makes the suite INVALID (a wrong
    answer on one side). Iceberg vs Paimon differences are a pipeline gap
    (the two jobs are pinned at the same moment, not the same binlog point),
    recorded, not a failure. An arm that did not run OK shows its status and
    is never counted as a mismatch;
  - (details) the pinned tables' state per format and table (snapshot id,
    records, data files, delete files, equality / position deletes, bytes)
    from tables-<STAMP>.tsv; DuckDB server restarts, and every .pins file.
"""
import argparse
import csv
import datetime as dt
import glob
import json
import math
import os
import re
import statistics
from decimal import Decimal, InvalidOperation

FORMATS = ["iceberg", "paimon"]
TABLES = ["customers", "products", "orders", "order_items"]
ARMS = [("duckdb_warm", "iceberg"), ("trino", "iceberg"), ("duckdb_warm", "paimon"), ("trino", "paimon")]
HEAD = {("duckdb_warm", "iceberg"): "DuckDB iceberg", ("trino", "iceberg"): "Trino iceberg",
        ("duckdb_warm", "paimon"): "DuckDB paimon", ("trino", "paimon"): "Trino paimon"}
# True when the runs read Paimon natively (pins: paimon_reader=native); False for
# runs before 2026-11-04, which read Paimon's Iceberg export (set in main()).
NATIVE = [True]


def set_paimon_reader(native):
    NATIVE[0] = native
    tag = "" if native else " (export)"
    HEAD[("duckdb_warm", "paimon")] = "DuckDB paimon" + tag
    HEAD[("trino", "paimon")] = "Trino paimon" + tag


def paimon_name():
    return "native Paimon" if NATIVE[0] else "Paimon's Iceberg export"
ENGINE = {"duckdb_warm": "DuckDB", "trino": "Trino"}
WARMUP = [1]   # --warmup


def read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(l for l in f if l.strip() and not l.startswith("#")))


def all_files(d, pattern):
    """Every run file matching <pattern % STAMP> that is not a per-pass (-pN) file."""
    rx = re.compile("^" + re.escape(pattern).replace("%s", r"\d{8}T\d{6}Z") + "$")
    return sorted(p for p in glob.glob(os.path.join(d, pattern % "*")) if rx.match(os.path.basename(p)))


def stamp_of(path):
    m = re.search(r"-(\d{8}T\d{6}Z)", os.path.basename(path or ""))
    return m.group(1) if m else None


def norm_field(v):
    v = v.strip()
    if re.fullmatch(r"-?\d+(\.\d+)?([eE][-+]?\d+)?", v):
        try:
            d = Decimal(v).normalize()
            return format(d, "f") if d == d.to_integral() else str(d)
        except InvalidOperation:
            return v
    m = re.fullmatch(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})(\.\d+)?", v)
    if m:
        frac = (m.group(2) or "").rstrip("0").rstrip(".")
        return m.group(1).replace("T", " ") + frac
    if v in ("true", "false"):
        return v.capitalize()
    return v


def load_rows(d):
    """{(engine, arm, query): [normalised row, ...]} from every -rows.tsv file."""
    out = {}
    for eng in ("duckdb_warm", "trino"):
        for p in all_files(d, eng + "-%s-rows.tsv"):
            for line in open(p):
                parts = line.rstrip("\n").split("\t", 4)
                if len(parts) < 5:
                    continue
                arm, _suite, q, n, row = parts
                out.setdefault((eng, arm, q), []).append((int(n), "|".join(norm_field(x) for x in row.split("|"))))
    return {k: [r for _, r in sorted(v)] for k, v in out.items()}


def ms(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def num(v, nd=0):
    """1234567 -> '1,234,567'; None -> '-'."""
    if v is None:
        return "-"
    return ("{:,.%df}" % nd).format(v)


def mill(v):
    """Rows as millions from 1 M up ('232.2 M'), else with separators."""
    if v is None:
        return "-"
    return "%.1f M" % (v / 1e6) if abs(v) >= 1_000_000 else num(v)


def hhmm(ms_or_iso):
    if ms_or_iso is None:
        return "?"
    if isinstance(ms_or_iso, (int, float)):
        return dt.datetime.fromtimestamp(ms_or_iso / 1000, dt.timezone.utc).strftime("%H:%M:%SZ")
    return ms_or_iso[11:19] + "Z"


def suite_section(suite, d, stamp):
    """(main lines, details lines, stats) for one suite."""
    main, det = [], []
    st = {"suite": suite, "stamp": stamp, "geo": {}, "bad": {}, "invalid": [], "gaps": [], "queries": 0, "ok": False}
    csvs = all_files(d, "duckdb_warm-%s.csv") + all_files(d, "trino-%s.csv")
    data = {}
    for p in csvs:
        for r in read_csv(p):
            if r.get("suite") == suite:
                data[(r["engine"], r["arm"], r["query"])] = r
    stamps = sorted({stamp_of(p) for p in csvs} - {None})
    st["stamp"] = stamp or ", ".join(stamps) or "?"
    main.append("### %s suite (run %s)\n" % (suite, st["stamp"]))
    if not data:
        main.append("No results.\n")
        return main, det, st
    st["ok"] = True
    queries = sorted({k[2] for k in data})
    st["queries"] = len(queries)
    arms = [ea for ea in ARMS if any((ea[0], ea[1], q) in data for q in queries)]

    def secs(ea, q, key="median_ms"):
        r = data.get((ea[0], ea[1], q))
        if not r or r["status"] != "OK" or ms(r.get(key)) is None:
            return None
        return ms(r[key]) / 1000.0

    def cell(ea, q):
        r, v = data.get((ea[0], ea[1], q)), secs(ea, q)
        return "%.2f" % v if v is not None else (r["status"] if r else "-")

    main.append("Median of the timed passes, seconds (DuckDB: execute + fetch in the server; Trino: elapsed - queued). "
                "Ratio > 1 = Trino slower.\n")
    ratio_cols = [(fmt, ("trino", fmt), ("duckdb_warm", fmt)) for fmt in FORMATS
                  if ("trino", fmt) in arms and ("duckdb_warm", fmt) in arms
                  and any(secs(("trino", fmt), q) and secs(("duckdb_warm", fmt), q) for q in queries)]
    main.append("| query | " + " | ".join(HEAD[ea] for ea in arms) + "".join(" | Trino / DuckDB, %s" % f for f, _, _ in ratio_cols) + " |")
    main.append("|---" + "|---:" * (len(arms) + len(ratio_cols)) + "|")
    for q in queries:
        cells = [cell(ea, q) for ea in arms]
        for ea in arms:
            r = data.get((ea[0], ea[1], q))
            if r and r["status"] != "OK":
                st["bad"].setdefault(ea, []).append((q, r["status"]))
        for _, a, b in ratio_cols:
            x, y = secs(a, q), secs(b, q)
            cells.append("**%.2f**" % (x / y) if x is not None and y else "-")
        main.append("| %s | %s |" % (q, " | ".join(cells)))
    main.append("")
    if any(secs(ea, q, "cold_ms") is not None for ea in arms for q in queries):
        main.append("Cold pass, seconds: the first pass after both engines' caches were emptied "
                    "(the table above is the median of the passes after it). Ratio > 1 = Trino slower.\n")
        cold_ratio = [(f, a, b) for f, a, b in ratio_cols
                      if any(secs(a, q, "cold_ms") and secs(b, q, "cold_ms") for q in queries)]
        main.append("| query | " + " | ".join(HEAD[ea] for ea in arms) + "".join(" | Trino / DuckDB, %s" % f for f, _, _ in cold_ratio) + " |")
        main.append("|---" + "|---:" * (len(arms) + len(cold_ratio)) + "|")
        for q in queries:
            cells = []
            for ea in arms:
                v = secs(ea, q, "cold_ms")
                cells.append("%.2f" % v if v is not None else "-")
            for _, a, b in cold_ratio:
                x, y = secs(a, q, "cold_ms"), secs(b, q, "cold_ms")
                cells.append("**%.2f**" % (x / y) if x is not None and y else "-")
            main.append("| %s | %s |" % (q, " | ".join(cells)))
        main.append("")
    if st["bad"]:
        main.append("Not OK (recorded, never compared): " + "; ".join(
            "%s: %s on %d of %d queries" % (HEAD[ea], "/".join(sorted({s for _, s in v})), len(v), len(queries))
            for ea, v in st["bad"].items()) + ".\n")

    # (what is compared, engine column, format column, arm A, arm B)
    pfmt = "paimon" if NATIVE[0] else "paimon (export)"
    pairs = [("engine", "Trino vs DuckDB", f if f != "paimon" else pfmt, ("trino", f), ("duckdb_warm", f)) for f in FORMATS]
    pairs += [("format", ENGINE[e], "%s vs iceberg" % pfmt, (e, "paimon"), (e, "iceberg")) for e in ("duckdb_warm", "trino")]
    geo_lines = []
    for what, eng, fmt, a, b in pairs:
        if a not in arms or b not in arms:
            continue
        rs = [secs(a, q) / secs(b, q) for q in queries if secs(a, q) and secs(b, q)]
        g = math.exp(sum(math.log(x) for x in rs) / len(rs)) if rs else None
        st["geo"][(a, b)] = (g, len(rs))
        if rs:
            geo_lines.append("| %s | %s | **%.2f** | %d |" % (eng, fmt, g, len(rs)))
    if geo_lines:
        main.append("Geometric mean of time A / time B over the queries both ran OK "
                    "(A is the first one named in the \"vs\" column; > 1 = A slower):\n")
        main += ["| engine | format | A / B | queries |", "|---|---|---:|---:|"] + geo_lines + [""]

    # ── details: Trino's server-side breakdown (timed passes)
    det.append("<details><summary>%s: Trino server-side breakdown, data scanned, result equality</summary>\n" % suite)
    qcs = all_files(d, "trino-%s-queries.csv")
    if qcs:
        per = {}
        for p in qcs:
            for r in read_csv(p):
                if r.get("suite") != suite or r.get("status") != "OK":
                    continue
                # hot: one run per pass, hot_pass 0 = the warm-up; snapshot: pass < WARMUP
                timed = int(r["hot_pass"]) >= 1 if r.get("hot_pass") else int(r.get("pass") or 0) >= WARMUP[0]
                if timed:
                    per.setdefault((r.get("arm") or "iceberg", r["query"]), []).append(r)
        if per:
            det.append("**Trino server-side breakdown** (median over the timed passes)\n")
            det.append("| arm | query | elapsed s | queued s | execution s | CPU s | physical input GB |")
            det.append("|---|---|---|---|---|---|---|")
            for arm in FORMATS:
                for q in queries:
                    rs = per.get((arm, q))
                    if not rs:
                        continue

                    def med(k, div=1000.0):
                        vs = [ms(r.get(k)) for r in rs if ms(r.get(k)) is not None]
                        return "%.2f" % (statistics.median(vs) / div) if vs else "-"
                    det.append("| %s | %s | %s | %s | %s | %s | %s |" % (arm, q, med("elapsed_ms"), med("queued_ms"),
                               med("exec_ms"), med("cpu_ms"), med("physical_input_bytes", 1e9)))
            det.append("")

    det.append("**Data scanned, GB** (DuckDB: HTTPFS bytes in an untimed pass with its cache bypassed; "
               "Trino: physical input of the last timed pass, cache hits included)\n")
    det.append("| query | " + " | ".join(HEAD[ea] for ea in arms) + " |")
    det.append("|---" * (1 + len(arms)) + "|")
    for q in queries:
        c = []
        for ea in arms:
            v = (data.get((ea[0], ea[1], q)) or {}).get("scan_bytes") or ""
            c.append("%.2f" % (int(v) / 1e9) if v.isdigit() else "-")
        det.append("| %s | %s |" % (q, " | ".join(c)))
    det.append("")

    # Result equality
    rows = load_rows(d)
    det.append("**Same rows across arms?** (every row of the last timed pass, compared as text). Letters name distinct "
               "result sets per query; equal letters = identical rows. An arm that did not run OK shows its status "
               "and is not compared.\n")
    det.append("| query | " + " | ".join(HEAD[ea] for ea in arms) + " | rows |")
    det.append("|---" * (2 + len(arms)) + "|")
    for q in queries:
        sets, cells = [], []
        got = {}
        for ea in arms:
            r = rows.get((ea[0], ea[1], q))
            if secs(ea, q) is None:
                cells.append((data.get((ea[0], ea[1], q)) or {}).get("status") or "-")
                continue
            if r is None:
                r = []
            got[ea] = r
            if r not in sets:
                sets.append(r)
            cells.append(chr(ord("A") + sets.index(r)))
        n = "/".join(sorted({str(len(x)) for x in sets})) or "-"
        det.append("| %s | %s | %s |" % (q, " | ".join(cells), n))
        for fmt in FORMATS:   # the two engines on one format read the same pinned snapshot
            a, b = got.get(("duckdb_warm", fmt)), got.get(("trino", fmt))
            if a is None or b is None or a == b:
                continue
            first = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
            st["invalid"].append("%s, %s: DuckDB and Trino differ (%s; first at row %d: `%s` vs `%s`)" % (
                q, fmt, "same rows, other order" if sorted(a) == sorted(b) else "different rows", first + 1,
                (a[first] if first < len(a) else "<none>")[:100], (b[first] if first < len(b) else "<none>")[:100]))
        for e in ("duckdb_warm", "trino"):
            a, b = got.get((e, "iceberg")), got.get((e, "paimon"))
            if a is not None and b is not None and a != b:
                st["gaps"].append((q, ENGINE[e]))
    det.append("")
    if st["invalid"]:
        det.append("**INVALID:** both engines of a format read the same pinned snapshot, so these are wrong answers on one side:")
        det += ["- " + x for x in st["invalid"]]
    else:
        det.append("Validity: on each format, every query both engines ran OK returned the same rows.")
    if st["gaps"]:
        det.append("\nIceberg vs Paimon rows differ on: %s. A pipeline gap, not a failure: the two CDC jobs are pinned "
                   "at the same moment, not at the same binlog point%s." %
                   (", ".join("%s (%s)" % g for g in st["gaps"]),
                    "" if NATIVE[0] else ", and Paimon's export lags native Paimon by about one checkpoint (and lists "
                    "only top-LSM-level files until a full compaction)"))
    det.append("")
    det.append("First row per arm:\n")
    det.append("| query | " + " | ".join(HEAD[ea] for ea in arms) + " |")
    det.append("|---" * (1 + len(arms)) + "|")
    for q in queries:
        det.append("| %s | %s |" % (q, " | ".join("`%s`" % (rows.get((ea[0], ea[1], q)) or ["-"])[0][:60].replace("|", "\\|")
                                                  for ea in arms)))
    det.append("\n</details>\n")

    # Pinned table state: the delete-file picture per table
    tps = all_files(d, "tables-%s.tsv")
    if tps:
        t = []
        for tp in tps:
            ls = [l.rstrip("\n").split("\t") for l in open(tp) if l.strip()]
            t += ls if not t else ls[1:]
        hdr, body = t[0], t[1:]
        ix = {k: i for i, k in enumerate(hdr)}
        det.append("<details><summary>%s: pinned tables, data and delete files (%s)</summary>\n" % (
            suite, ", ".join(os.path.basename(x) for x in tps)))
        det.append("Per format and table, its pins in this suite (hot: one per pass, ranges over them; snapshot: one). "
                   + ("`paimon` = the native Paimon snapshot the arms read (records and commit kind only)."
                      if NATIVE[0] else "`paimon` = Paimon's Iceberg export metadata; `paimon_native` = the native "
                      "Paimon snapshot it was exported from (records only).") + "\n")
        det.append("| format | table | pins | snapshot id (last) | records | data files | delete files | equality deletes | position deletes (incl. DVs) | GB |")
        det.append("|---|---|---|---|---|---|---|---|---|---|")
        for fmt, tbl in [(f, t) for f in ("iceberg", "paimon", "paimon_native") for t in TABLES]:
            rs = [r for r in body if len(r) > ix["table"] and r[ix["table"]] == tbl and r[ix["format"]] == fmt]
            if not rs:
                continue
            last = rs[-1]
            g_ = lambda k: last[ix[k]] if k in ix and ix[k] < len(last) else ""
            rng = lambda k: (lambda vs: "-" if not vs else (str(min(vs)) if min(vs) == max(vs) else "%d..%d" % (min(vs), max(vs))))(
                [int(r[ix[k]]) for r in rs if k in ix and ix[k] < len(r) and r[ix[k]].isdigit()])
            gb = g_("total-files-size")
            det.append("| %s | %s | %d | %s | %s | %s | %s | %s | %s | %s |" % (
                fmt, tbl, len(rs), g_("snapshot_id"), rng("total-records"), rng("total-data-files"), rng("total-delete-files"),
                rng("total-equality-deletes"), rng("total-position-deletes"), "%.2f" % (int(gb) / 1e9) if gb.isdigit() else "-"))
        det.append("")
        det.append("Every pin (tables TSV):\n")
        det.append("| " + " | ".join(hdr) + " |")
        det.append("|---" * len(hdr) + "|")
        for r in body:
            det.append("| " + " | ".join(x.replace("|", "\\|") for x in r + [""] * (len(hdr) - len(r))) + " |")
        det.append("\n</details>\n")
    e = []
    for ev in all_files(d, "duckdb_warm-%s-events.csv"):
        e += [l.strip() for l in open(ev) if l.strip()][1:]
    if e:
        det.append("<details><summary>%s: DuckDB server restarts (`restart`) and the queries after them (`cold`)</summary>\n" % suite)
        det += ["- " + x for x in e]
        det.append("\n</details>\n")
    for pf in sorted(glob.glob(os.path.join(d, "*.pins"))):
        txt = open(pf).read()
        c = [int(x) for x in re.findall(r"QB_PIN_COMMIT_MS_\w+=(\d+)", txt)]
        if c:
            st["pin_span"] = (min(c), max(c))
            st["pins"] = len(re.findall(r"^## pins ", txt, re.M))
        det.append("<details><summary>%s: pins, gates and windows (%s)</summary>\n\n```" % (suite, os.path.basename(pf)))
        det += txt.rstrip("\n").split("\n")
        det.append("```\n</details>\n")
    return main, det, st


def run_meta(dirs):
    """key=value tokens of the harness's run-meta lines in every .pins file
    (lib/querybench.sh qb_run_meta), the first value seen per key."""
    meta = {}
    for d in dirs:
        for pf in sorted(glob.glob(os.path.join(d, "*.pins"))):
            for line in open(pf):
                for tok in line.split():
                    k, eq, v = tok.partition("=")
                    if eq and re.fullmatch(r"[a-z_0-9]+", k) and k.startswith(("iceberg_", "paimon_db", "paimon_reader", "cdc_", "duckdb_", "trino_", "engines", "formats")):
                        meta.setdefault(k, v)
    return meta


# ── Ingest (ingest_stats.py JSON) ──────────────────────────────────────────
def ingest_totals(ing):
    """{fmt: summed window figures} over the tables that were read."""
    out = {}
    for fmt in FORMATS:
        ts = [v for v in (ing.get(fmt) or {}).values() if v]
        if not ts:
            continue
        add = lambda k: None if any(t.get(k) is None for t in ts) else sum(t[k] for t in ts)
        o = {k: add(k) for k in ("rows", "bytes", "write_commits", "data_files", "delete_files", "equality_deletes",
                                 "position_deletes", "dvs", "compaction_commits", "compaction_dv_index_files")}
        iv = [t["median_commit_interval_s"] for t in ts if t.get("median_commit_interval_s")]
        o["interval"] = statistics.median(iv) if iv else None
        o["tables"] = len(ts)
        secs = (ing.get("window") or {}).get("seconds")
        o["rate"] = o["rows"] / secs if secs and o["rows"] is not None else None
        o["byte_rate"] = o["bytes"] / secs if secs and o["bytes"] is not None else None
        sts = [t["state"] for t in ts if t.get("state")]
        o["state_records"] = sum(s["records"] for s in sts) if sts else None
        out[fmt] = o
    return out


def ingest_section(ing, err):
    """(main lines, details lines, totals) for the Ingest + Table state sections."""
    main3, main4, det = ["## 3. Ingest\n"], ["## 4. Table state\n"], []
    if not ing or not ing.get("window"):
        why = err or (ing or {}).get("errors") or "no ingest data"
        why = "; ".join(why) if isinstance(why, list) else why
        main3.append("Not available: %s.\n" % why)
        main4.append("Not available (see 3).\n")
        return main3 + main4, det, {}
    w = ing["window"]
    tot = ingest_totals(ing)
    p, i = tot.get("paimon", {}), tot.get("iceberg", {})
    main3.append("Window %s to %s (%s s; %s), from each table's own commit history: Iceberg snapshot summaries in the "
                 "current metadata.json, Paimon `snapshot-N` files (`querybench/ingest_stats.py`). Write commits only; "
                 "compaction commits are counted apart.\n" % (w["from"], w["to"], num(w["seconds"]), w["how"]))
    mon = {f: m for f, m in (ing.get("monitor") or {}).items() if m and m.get("reports")}
    eqf = lambda o: None if o.get("delete_files") is None or o.get("dvs") is None else o["delete_files"] - o["dvs"]
    pos_share = (i["position_deletes"] / (i["position_deletes"] + i["equality_deletes"])
                 if i and i.get("position_deletes") is not None and (i["position_deletes"] + i["equality_deletes"]) else None)

    # Throughput: the job monitor's rows/s and data size/s, median (max) over its reports in the window.
    main3.append("**Throughput** (job-monitor reports overlapping the window: median, max in brackets)\n")
    g = lambda f, k: (mon.get(f) or {}).get(k)
    if mon:
        mb = lambda v: "-" if v is None else "%.1f" % (v / 1e6)
        trows = [("Rows/s", lambda f: "**%s** (%s)" % (num(g(f, "rows_per_s_median")), num(g(f, "rows_per_s_max")))),
                 ("Data size/s, MB/s", lambda f: "**%s** (%s)" % (mb(g(f, "bytes_per_s_median")), mb(g(f, "bytes_per_s_max"))))]
        if any(g(f, "checkpoint_ms_median") for f in FORMATS):
            trows.append(("Checkpoint duration, median", lambda f: "%s ms" % num(g(f, "checkpoint_ms_median"))))
        if any(g(f, "backpressured_ratio_max") is not None for f in FORMATS):
            trows.append(("Backpressure HIGH, max share of samples",
                          lambda f: "-" if g(f, "backpressured_ratio_max") is None else "%.0f%%" % (100 * g(f, "backpressured_ratio_max"))))
        main3 += ["| | Paimon | Iceberg |", "|---|---:|---:|"]
        main3 += ["| %s | %s | %s |" % (n, fn("paimon") if g("paimon", "reports") else "no reports",
                                         fn("iceberg") if g("iceberg", "reports") else "no reports") for n, fn in trows]
        main3.append("| Reports (sampled s) | %s | %s |" % tuple(
            "%s (%s s)" % (num(g(f, "reports")), num(g(f, "sampled_s"))) if g(f, "reports") else "0" for f in ("paimon", "iceberg")))
        main3.append("")
    else:
        main3.append("Job-monitor reports not available for this window (reports were pod-local before the S3 upload "
                     "was added); next run. The table-metadata numbers below cover the same window.\n")
    main3.append("Paimon merges re-written keys in its write buffer before committing, so its committed rows/s is "
                 "lower than Iceberg's for the same input.\n")

    # From the table metadata: what each format committed.
    main3.append("**From table metadata** (each table's commit history over the window)\n")

    def pi(fp, fi):
        return "| %s | %s |" % (fp(p) if p else "-", fi(i) if i else "-")
    mbs = lambda o: "-" if o.get("byte_rate") is None else "%.1f MB/s" % (o["byte_rate"] / 1e6)
    rows = [
        ("Rows/s (window average)", lambda o: "**%s**" % num(o["rate"]), lambda o: "**%s**" % num(o["rate"])),
        ("Data size/s (new files)", mbs, mbs),
        ("Rows committed", lambda o: mill(o["rows"]), lambda o: mill(o["rows"])),
        ("Write commits (median interval)", lambda o: "%s (%s s)" % (num(o["write_commits"]), num(o["interval"])),
         lambda o: "%s (%s s)" % (num(o["write_commits"]), num(o["interval"]))),
        ("New data files", lambda o: num(o["data_files"]), lambda o: num(o["data_files"])),
        ("**New delete files**", lambda o: "%s DV index files, by compaction" % num(o["delete_files"]) if o["delete_files"] is not None else "-",
         lambda o: "**%s** (%s DVs + %s equality-delete files)" % (num(o["delete_files"]), num(o["dvs"]), num(eqf(o)))),
        ("**Equality deletes**", lambda o: "**0**", lambda o: "**%s**" % mill(o["equality_deletes"])),
        ("Position deletes", lambda o: "none written; compaction marks rows in DVs",
         lambda o: "%s (%s of all deletes, as DVs)" % (mill(o["position_deletes"]), "%.0f%%" % (100 * pos_share) if pos_share is not None else "-")),
        ("Compaction commits", lambda o: "%s (inline)" % num(o["compaction_commits"]), lambda o: "%s (compaction job)" % num(o["compaction_commits"])),
    ]
    main3 += ["| | Paimon | Iceberg |", "|---|---:|---:|"]
    for name, fp, fi in rows:
        try:
            main3.append("| %s %s" % (name, pi(fp, fi)))
        except (TypeError, KeyError):
            main3.append("| %s | - | - |" % name)
    main3.append("")
    notes = []
    if p.get("rows") and i.get("rows"):
        notes.append("Paimon committed **%.0f%%** of Iceberg's rows: the other %.0f%% were re-writes of a key it "
                     "merged before committing. Iceberg commits each of them and a delete for the version it "
                     "replaces." % (100 * p["rows"] / i["rows"], 100 - 100 * p["rows"] / i["rows"]))
    if pos_share is not None and i.get("rows"):
        dl = i["position_deletes"] + i["equality_deletes"]
        notes.append("Iceberg wrote %s deletes for %s committed rows (%s); **%.0f%%** were position deletes (DVs: the "
                     "old row was in the same checkpoint's files), the rest equality deletes, which every read must "
                     "join by key until a compaction." % (
                         mill(dl), mill(i["rows"]), "one per row" if abs(dl - i["rows"]) <= 0.005 * i["rows"]
                         else "%.0f%% of rows" % (100.0 * dl / i["rows"]), 100 * pos_share))
    if p.get("compaction_commits") and p.get("compaction_dv_index_files") is not None:
        notes.append("Paimon's DV index files are rewritten per bucket by each inline compaction (%s commits); "
                     "the live ones at the window end are in 4." % num(p["compaction_commits"]))
    rds = (ing.get("config") or {}).get("rds") or {}
    if rds.get("cpu_avg") is not None:
        notes.append("Source RDS CPU over the window: %s%% average, %s%% max." % (rds["cpu_avg"], rds["cpu_max"]))
    main3 += ["- " + n for n in notes] + [""]

    # Table state
    main4.append("At the window end (the last commit at or before %s). Both record counts are before deletes are "
                 "applied: Iceberg `total-records`, Paimon `totalRecordCount` of the live files.\n" % w["to"])
    main4.append("| table | Paimon records | Paimon files / DV-deleted rows | Iceberg records | Iceberg data / delete files | Iceberg eq / pos deletes | snapshots P / I |")
    main4.append("|---|---:|---:|---:|---:|---:|---:|")
    for t in TABLES:
        ps = ((ing.get("paimon") or {}).get(t) or {}).get("state") or {}
        ist = ((ing.get("iceberg") or {}).get(t) or {}).get("state") or {}
        main4.append("| %s | %s | %s / %s | %s | %s / %s | %s / %s | %s / %s |" % (
            t, mill(ps.get("records")), num(ps.get("data_files")), mill(ps.get("dv_deleted_rows")),
            mill(ist.get("records")), num(ist.get("data_files")), num(ist.get("delete_files")),
            mill(ist.get("equality_deletes")), mill(ist.get("position_deletes")),
            num(ps.get("snapshots_retained")), num(ist.get("snapshots_retained"))))
    main4.append("")
    if p.get("state_records") and i.get("state_records"):
        main4.append("Iceberg holds %.1f× Paimon's records: its superseded rows stay in the data files until a "
                     "compaction rewrites them; Paimon's compaction drops them as it goes.\n" % (i["state_records"] / p["state_records"]))

    # Details: per table
    det.append("<details><summary>Ingest per table (%s to %s)</summary>\n" % (w["from"], w["to"]))
    det.append("| format | table | rows/s | rows | write commits | median interval s | data files | delete files | eq deletes | pos deletes | DVs | compaction commits | first / last commit |")
    det.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for fmt in FORMATS:
        for t in TABLES:
            v = (ing.get(fmt) or {}).get(t)
            if not v:
                det.append("| %s | %s | not read |||||||||||" % (fmt, t))
                continue
            det.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s / %s |" % (
                fmt, t, num(v["rows"] / w["seconds"]) if w["seconds"] else "-", num(v["rows"]), num(v["write_commits"]),
                num(v["median_commit_interval_s"], 1), num(v["data_files"]), num(v["delete_files"]),
                num(v["equality_deletes"]), num(v["position_deletes"]), num(v["dvs"]), num(v["compaction_commits"]),
                hhmm(v["first_commit"]), hhmm(v["last_commit"])))
    det.append("")
    if mon:
        keys = sorted({k for m in mon.values() for k in m})
        det.append("Job-monitor aggregate (ingest_stats.py, reports overlapping the window):\n")
        det += ["| field | Paimon | Iceberg |", "|---|---:|---:|"]
        det += ["| %s | %s | %s |" % (k, (mon.get("paimon") or {}).get(k, "-"), (mon.get("iceberg") or {}).get(k, "-")) for k in keys]
        det.append("")
    if ing.get("errors"):
        det.append("ingest_stats.py errors: " + "; ".join(ing["errors"]) + "\n")
    det.append("</details>\n")
    return main3 + main4, det, tot


def config_table(meta, ing, sts, fv):
    cfg = (ing or {}).get("config") or {}
    rows = []
    fl = cfg.get("flink") or {}
    if fl:
        desc = lambda c: "parallelism %s, %s TMs × %s slots (%s CPU / %s each), checkpoint every %s" % (
            c.get("parallelism"), c.get("tms"), c.get("slots_per_tm"), c.get("tm_cpu"), c.get("tm_memory"),
            c.get("checkpoint_interval"))
        cs = {f: desc(c) for f, c in fl.items() if c}
        rows.append(("Flink writers", ("Paimon and Iceberg jobs alike: " + next(iter(cs.values()))) if len(set(cs.values())) == 1
                     else "; ".join("%s: %s" % (f.capitalize(), v) for f, v in sorted(cs.items(), reverse=True))))
    rds = cfg.get("rds") or {}
    if rds:
        rows.append(("Source", "RDS %s, `%s`, %s" % (rds.get("engine"), rds.get("class"), rds.get("storage"))))
    g = cfg.get("generator") or {}
    if g:
        tgt = (int(g["replicas"]) * int(g["RATE_PER_POD"])) if g.get("replicas") and (g.get("RATE_PER_POD") or "").isdigit() else None
        rows.append(("Generator", "`mysql-upsert-loadgen`, %s pods × `RATE_PER_POD=%s`%s, `BATCH_ROWS=%s`, `UPSERT_RATIO=%s`, "
                     "`RECENT_KEY_RATIO=%s`" % (g.get("replicas") or "?", g.get("RATE_PER_POD"),
                                                " (target %s rows/s)" % num(tgt) if tgt else "", g.get("BATCH_ROWS"),
                                                g.get("UPSERT_RATIO"), g.get("RECENT_KEY_RATIO"))))
    rows.append(("Engines", "DuckDB `%s` (%s CPU / %s), warm; Trino %s (%s workers)" % (
        (meta.get("duckdb_image") or "pip").split("/")[-1], meta.get("duckdb_cpu", "?"), meta.get("duckdb_pod_memory", "?"),
        meta.get("trino_version") or "?", meta.get("trino_workers") or "?")))
    rows.append(("Table set", "Iceberg v%d%s: `%s` / `%s`; Paimon read %s" % (
        fv, " (CDC_DB_SUFFIX `%s`)" % meta["cdc_db_suffix"] if meta.get("cdc_db_suffix") else "",
        meta.get("iceberg_db", "?"), meta.get("paimon_db", "?"),
        "natively (DuckDB paimon_scan, Trino paimon connector)" if NATIVE[0] else "through its Iceberg export")))
    for s in sts:
        if s["ok"]:
            span = s.get("pin_span")
            rows.append(("%s run" % s["suite"].capitalize(), "`%s`, %d queries%s" % (
                s["stamp"], s["queries"], ", %d pins %s to %s" % (s.get("pins", 0), hhmm(span[0]), hhmm(span[1])) if span else "")))
    w = (ing or {}).get("window")
    if w:
        rows.append(("Ingest window", "%s to %s (%s)" % (w["from"], w["to"], w["how"])))
    out = ["**Run configuration**\n", "| | |", "|---|---|"]
    out += ["| %s | %s |" % r for r in rows]
    return out + [""]


def headline(sts, tot, mon=None):
    """Computed bullets: (headline bullets, conclusions, open items)."""
    hb, con, opn = [], [], []
    for s in sts:
        if not s["ok"]:
            opn.append("%s suite: no results in this summary." % s["suite"])
            continue
        geo = s["geo"]
        for fmt in FORMATS:
            g, n = geo.get((("trino", fmt), ("duckdb_warm", fmt)), (None, 0))
            if g:
                txt = "On %s, Trino took **%.2f×** warm DuckDB's time (%s suite, geometric mean of %d queries)." % (
                    "the Iceberg tables" if fmt == "iceberg" else paimon_name(), g, s["suite"], n)
                hb.append(txt)
                con.append("Engine (%s, %s): Trino / DuckDB = %.2f over %d queries." % (fmt, s["suite"], g, n))
        for e in ("trino", "duckdb_warm"):
            g, n = geo.get(((e, "paimon"), (e, "iceberg")), (None, 0))
            if g:
                ng = len({q for q, en in s["gaps"] if en == ENGINE[e]})
                if NATIVE[0]:
                    hb.append("On %s, native Paimon answered in **%.2f×** Iceberg's time (%d queries); its rows "
                              "differed from Iceberg's on %d of %d queries (pinned at the same moment, not the same "
                              "binlog point)." % (ENGINE[e], g, n, ng, s["queries"]))
                    con.append("Format (%s, %s): Paimon / Iceberg = %.2f over %d queries; rows differ on %d." % (
                        ENGINE[e], s["suite"], g, n, ng))
                else:
                    hb.append("On %s, Paimon's Iceberg export answered in **%.2f×** Iceberg's time (%d queries), but it is "
                              "not like-for-like: the export lists only top-LSM-level files until a full compaction, and its "
                              "rows differed from Iceberg's on %d of %d queries." % (ENGINE[e], g, n, ng, s["queries"]))
                    con.append("Format (%s, %s): Paimon export / Iceberg = %.2f over %d queries; rows differ on %d (export "
                               "coverage, not a like-for-like read)." % (ENGINE[e], s["suite"], g, n, ng))
        for ea, v in s["bad"].items():
            if len(v) == s["queries"]:
                hb.append("%s did not run: %s on all %d queries." % (HEAD[ea], "/".join(sorted({x for _, x in v})), s["queries"]))
                opn.append("%s: %s on every %s query (see its runner log)." % (HEAD[ea], "/".join(sorted({x for _, x in v})), s["suite"]))
            else:
                opn.append("%s: not OK on %d of %d %s queries." % (HEAD[ea], len(v), s["queries"], s["suite"]))
        if s["invalid"]:
            con.append("**%s suite INVALID**: DuckDB and Trino disagree on %d query/format pairs (details)." % (s["suite"], len(s["invalid"])))
        else:
            con.append("Valid (%s): on each format, both engines returned identical rows for every query both ran." % s["suite"])
        if s["gaps"]:
            opn.append(("Iceberg vs Paimon rows differ on %d %s queries: the two jobs are pinned at the same moment, "
                        "not the same binlog point." if NATIVE[0] else
                        "Iceberg vs Paimon-export rows differ on %d %s queries: the export covers top-LSM-level files "
                        "only until a full compaction, and the two jobs are pinned at the same moment, not the same "
                        "binlog point.") % (len({q for q, _ in s["gaps"]}), s["suite"]))
    p, i = tot.get("paimon") or {}, tot.get("iceberg") or {}
    mp, mi = (mon or {}).get("paimon") or {}, (mon or {}).get("iceberg") or {}
    mbs = lambda v: "-" if v is None else "%.1f MB/s" % (v / 1e6)
    if mp.get("rows_per_s_median") and mi.get("rows_per_s_median"):
        hb.append("Ingest throughput (job monitor, median of %d / %d reports): Paimon **%s rows/s, %s**; Iceberg "
                  "**%s rows/s, %s**. Paimon merges re-written keys before committing, so its rows/s is lower for "
                  "the same input." % (mp["reports"], mi["reports"], num(mp["rows_per_s_median"]), mbs(mp.get("bytes_per_s_median")),
                                       num(mi["rows_per_s_median"]), mbs(mi.get("bytes_per_s_median"))))
        con.append("Ingest (job monitor): Paimon %s rows/s, %s; Iceberg %s rows/s, %s." % (
            num(mp["rows_per_s_median"]), mbs(mp.get("bytes_per_s_median")), num(mi["rows_per_s_median"]), mbs(mi.get("bytes_per_s_median"))))
    elif p.get("rate") and i.get("rate"):
        hb.append("Ingest throughput, from table metadata (job-monitor reports not available for this window; next "
                  "run): Iceberg **%s rows/s, %s**; Paimon **%s rows/s, %s**. Paimon merges re-written keys before "
                  "committing (%.0f%% fewer rows here), so its rows/s is lower for the same input." % (
                      num(i["rate"]), mbs(i.get("byte_rate")), num(p["rate"]), mbs(p.get("byte_rate")),
                      100 - 100 * p["rows"] / i["rows"]))
    if p.get("rate") and i.get("rate"):
        dl = (i["position_deletes"] or 0) + (i["equality_deletes"] or 0)
        share = (100.0 * i["position_deletes"] / dl) if dl and i.get("position_deletes") is not None else None
        hb.append("Delete mix: Iceberg wrote %s deletes for %s committed rows, %s of them position deletes (DVs) and "
                  "%s equality deletes; Paimon wrote no equality deletes." % (
                      mill(dl), mill(i["rows"]), "%.0f%%" % share if share is not None else "?", mill(i["equality_deletes"])))
        con.append("Table metadata: Iceberg %s rows/s, Paimon %s rows/s committed; Iceberg added %s equality deletes "
                   "and %s position deletes (DVs), Paimon none (it merges by key; compaction writes DVs)." % (
                       num(i["rate"]), num(p["rate"]), mill(i["equality_deletes"]), mill(i["position_deletes"])))
    return hb, con, opn


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--work", required=True)
    ap.add_argument("--session", required=True)
    ap.add_argument("--hot")
    ap.add_argument("--snapshot")
    ap.add_argument("--iceberg-format-version", help="2|3; default: the runs' .pins files, else 3")
    ap.add_argument("--warmup", type=int, default=1, help="the snapshot run's WARMUP (default 1)")
    ap.add_argument("--ingest", help="ingest_stats.py JSON")
    ap.add_argument("--ingest-error", help="why there is no ingest JSON (shown in section 3)")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    WARMUP[0] = a.warmup
    dirs = [os.path.join(a.work, x) for x in ("hot", "snapshot") if os.path.isdir(os.path.join(a.work, x))]
    meta = run_meta(dirs)
    set_paimon_reader(meta.get("paimon_reader") == "native")
    fv = int(meta.get("iceberg_format_version") or a.iceberg_format_version or 3)
    if a.iceberg_format_version and meta.get("iceberg_format_version") and int(a.iceberg_format_version) != fv:
        print("note: the runs' pins say iceberg_format_version=%d, not %s; using the pins" % (fv, a.iceberg_format_version))
    ing, ing_err = None, a.ingest_error
    if a.ingest:
        try:
            ing = json.load(open(a.ingest))
        except Exception as e:
            ing_err = "could not read %s: %s" % (a.ingest, e)
    lat, det, sts = [], [], []
    for suite, stamp in (("hot", a.hot), ("snapshot", a.snapshot)):
        d = os.path.join(a.work, suite)
        if os.path.isdir(d):
            m, dd, s = suite_section(suite, d, stamp)
            lat += m
            det += dd
            sts.append(s)
    ing_main, ing_det, tot = ingest_section(ing, ing_err)
    mon = {f: m for f, m in ((ing or {}).get("monitor") or {}).items() if m and m.get("reports")}
    hb, con, opn = headline(sts, tot, mon)
    if ing and ing.get("window") and not mon:
        opn.append("Job-monitor throughput (rows/s, MB/s), checkpoints and backpressure: reports not available for "
                   "this window (reports were pod-local before the S3 upload was added); next run. `SUITE=hot run.sh "
                   "start` now brings the monitor up, and it copies each report to `<results prefix>/monitor/`.")
    elif ing and ing.get("window"):
        for f in FORMATS:
            if not mon.get(f):
                opn.append("No %s job-monitor report overlaps the ingest window." % f)
    if not any(s["suite"] == "snapshot" for s in sts):
        opn.append("No snapshot suite in this summary (writers stopped, Iceberg compacted).")

    L = ["# CDC query benchmark: %s\n" % a.session,
         "Warm DuckDB vs Trino on the four CDC tables Flink writes, on native Iceberg and on %s "
         "(cdc-pipeline/querybench/); both engines of a format read the same pinned snapshot per table.\n" % paimon_name()]
    L += config_table(meta, ing, sts, fv)
    L += ["Known write-side differences (table settings are otherwise paired; querybench/README.md): Paimon writes "
          "orders/order_items with one writer per bucket (32 fixed buckets up to _v3r3/_v3r1, dynamic buckets after), Iceberg's hash distribution sends each date partition "
          "to ONE writer; Paimon compacts and expires snapshots inline, Iceberg only in its compaction job.\n", "---\n"]
    L += ["## 1. Headline\n"] + ["- " + b for b in (hb or ["No suite results."])] + [""]
    L += ["## 2. Query latency\n"] + lat
    L += ing_main
    L += ["## Conclusions\n"] + ["%d. %s" % (n + 1, c) for n, c in enumerate(con)] + [""]
    if opn:
        L += ["### Open items\n"] + ["- " + o for o in opn] + [""]
    L += ["---\n", "## Details\n"] + det + ing_det
    with open(a.out, "w") as f:
        f.write("\n".join(L) + "\n")
    print(a.out)


if __name__ == "__main__":
    main()
