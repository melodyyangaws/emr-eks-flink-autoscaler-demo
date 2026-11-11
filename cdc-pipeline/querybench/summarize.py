#!/usr/bin/env python3
"""The CDC query benchmark's findings as one Markdown file, from one hot and/or
one snapshot run (querybench/run.sh summary gathers them from S3).

  summarize.py --work <dir with hot/ and/or snapshot/> --session <name>
               [--hot <STAMP>] [--snapshot <STAMP>] [--iceberg-format-version 3] [--warmup 1]
               [--ingest <ingest_stats.py JSON>] [--ingest-error <why it is missing>] --out <md>

Layout: Run configuration;
1. Headline (computed bullets); 2. Query latency (median per query and arm,
geometric means); 3. Ingest (Paimon vs Iceberg over the run's window, from
ingest_stats.py); 4. Table state at the window end; Conclusions / Open items;
then <details> blocks with everything else, so no number is dropped:

Four arms: warm DuckDB (`duckdb_warm`) and Trino (`trino`), each on the
Iceberg tables (`iceberg`) and on Paimon (`paimon`: Paimon-as-iceberg, its Iceberg
metadata). Both engines of a format read the same pinned snapshot per table. Per suite:
  - median seconds per query and arm, and Trino / DuckDB per format;
  - geometric means: Trino vs DuckDB per format; per engine Paimon-as-iceberg vs
    Iceberg, over the queries both sides ran OK;
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
ING_FORMATS = ["iceberg", "paimon"]   # ingest_stats.py: the two writers' table sets
TABLES = ["customers", "products", "orders", "order_items"]
ARMS = [("duckdb_warm", "iceberg"), ("trino", "iceberg"), ("duckdb_warm", "paimon"), ("trino", "paimon")]
HEAD = {("duckdb_warm", "iceberg"): "DuckDB iceberg", ("trino", "iceberg"): "Trino iceberg",
        ("duckdb_warm", "paimon"): "DuckDB paimon-as-iceberg", ("trino", "paimon"): "Trino paimon-as-iceberg"}
# Old runs on S3: session _v3 (2026-11-08) also read a third arm, `paimon_native`;
# its rows, tables-TSV rows and pins lines are dropped wherever they are loaded.
# Runs before 2026-11-07 with PAIMON_READER=native read the paimon arm natively;
# their pins say paimon_reader=native and they render as `paimon` like any other.
DROPPED_ARM = "paimon_native"


def drop_arm(txt):
    """A .pins file's text without the dropped arm's QB_PIN_* lines and list entries."""
    keep = [l for l in txt.split("\n") if not re.match(r"QB_PIN_\w*_%s_" % DROPPED_ARM.upper(), l)]
    return re.sub(r"[ ,]+%s(?: \d+)?" % DROPPED_ARM, "", "\n".join(keep))


def fmt_text(fmt):
    """A format in prose: the Iceberg tables / Paimon-as-iceberg."""
    return {"iceberg": "the Iceberg tables", "paimon": "Paimon-as-iceberg"}[fmt]


def read_tables(d):
    """(tables-<STAMP>.tsv files, header, rows), the files concatenated."""
    tps, t = all_files(d, "tables-%s.tsv"), []
    for tp in tps:
        ls = [l.rstrip("\n").split("\t") for l in open(tp) if l.strip()]
        t += ls if not t else ls[1:]
    hdr = t[0] if t else []
    f = hdr.index("format") if "format" in hdr else None
    return tps, hdr, [r for r in t[1:] if f is None or f >= len(r) or r[f] != DROPPED_ARM]
ENGINE = {"duckdb_warm": "DuckDB", "trino": "Trino"}
WARMUP = [1]   # --warmup

# ── Cost. On-demand Linux EC2 prices, us-west-2, US$/h (AWS Pricing API, 2026-11-11).
# The engines run one pod per node, so they are priced per node: query time ×
# the hourly price of every node the engine holds (DuckDB: its one node; Trino:
# workers + coordinator). Idle time between queries is not counted.
EC2_PRICE_H = {"m5.2xlarge": 0.384, "m5.4xlarge": 0.768, "m5.8xlarge": 1.536, "m5.12xlarge": 2.304,
               "m5.16xlarge": 3.072, "m5.24xlarge": 4.608}
# The compaction pods share Karpenter nodes, so they are priced by their requests:
# a linear fit to m6g.xlarge (US$0.154/h, 4 vCPU / 16 GiB) and r6g.xlarge
# (US$0.2016/h, 4 vCPU / 32 GiB), i.e. US$0.02662 per vCPU-hour + US$0.002975 per GiB-hour.
POD_VCPU_H, POD_GIB_H = 0.02662, 0.002975
# EMR on EKS uplift on top of EC2, for the compaction jobs (EMR on EKS Flink jobs;
# the query engines are plain pods and pay EC2 only). AWS Pricing API, us-west-2,
# 2026-11-11: USW2-EMR-EKS-EC2-vCPUHours / -GBHours.
EMR_VCPU_H, EMR_GB_H = 0.007084, 0.000777875
# compaction/run.sh defaults, for result JSONs written before cdc-compact.py recorded
# its own shape (2026-11-11): JM 4 vCPU / 8 GiB; TM 8 vCPU; 4 slots on Paimon, 1 on
# Iceberg; 96 GiB TMs on Iceberg orders / order_items, else 32 GiB.
COMPACT_DEFAULTS = {"jm_cpu": 4, "jm_memory_gi": 8, "tm_cpu": 8, "slots": {"paimon": 4, "iceberg": 1},
                    "tm_memory_gi": 32, "big_tm_memory_gi": 96, "big_tm_tables": ("orders", "order_items")}


def engine_rates(meta):
    """{engine: (US$ per second, description)} for the engines whose nodes have a known price."""
    out = {}
    d = EC2_PRICE_H.get(meta.get("duckdb_ec2") or "")
    if d:
        out["duckdb_warm"] = (d / 3600, "DuckDB 1 × %s = US$%.3f/h" % (meta["duckdb_ec2"], d))
    nw = int(meta["trino_workers"]) if str(meta.get("trino_workers", "")).isdigit() else None
    w, c = EC2_PRICE_H.get(meta.get("trino_worker_ec2") or ""), EC2_PRICE_H.get(meta.get("trino_coord_ec2") or "")
    if nw and w and c:
        h = nw * w + c
        out["trino"] = (h / 3600, "Trino %d × %s + 1 × %s (coordinator) = US$%.3f/h" % (
            nw, meta["trino_worker_ec2"], meta["trino_coord_ec2"], h))
    return out


def usd(x):
    return "%.5f" % x if x < 0.01 else "%.4f" % x


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
                if arm == DROPPED_ARM:
                    continue
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


def mb(v):
    """Bytes as MB with one decimal; None -> '-'."""
    return "-" if v is None else "%.1f" % (v / 1e6)


def mbs(v):
    """Bytes/s as 'N.N MB/s'; None -> '-'."""
    return "-" if v is None else mb(v) + " MB/s"


def hhmm(ms_or_iso):
    if ms_or_iso is None:
        return "?"
    if isinstance(ms_or_iso, (int, float)):
        return dt.datetime.fromtimestamp(ms_or_iso / 1000, dt.timezone.utc).strftime("%H:%M:%SZ")
    return ms_or_iso[11:19] + "Z"



def recover_finished_hot(d, data):
    """Hot suite, Trino: a query whose combined median row is not OK only because a
    pass lost its CLI result stream (Trino FINISHED it; the error text starts with
    "FINISHED"), as H8 on _v3final (2026-11-07). Re-derive that query's median over
    all timed passes from the per-pass server-side times (trino-<STAMP>-p<N>-queries.csv,
    exec_ms; pass p0 is the warm-up) and mark it OK. Runs recorded after trino.sh
    learned the same rule never need this. Returns [(arm, query, [lost passes])]."""
    per = {}
    for f in sorted(glob.glob(os.path.join(d, "trino-*-p[0-9]*-queries.csv"))):
        m = re.search(r"-p(\d+)-queries\.csv$", f)
        if not m or m.group(1) == "0":
            continue
        for r in read_csv(f):
            per.setdefault((r.get("arm"), r.get("query")), []).append((m.group(1), r))
    out = []
    for (eng, arm, q), row in list(data.items()):
        if eng != "trino" or row.get("status") == "OK":
            continue
        ps = per.get((arm, q)) or []
        if not ps:
            continue
        lost = [p for p, r in ps if r.get("status") != "OK"]
        if not lost or any(not (r.get("error") or "").startswith("FINISHED") for p, r in ps if r.get("status") != "OK"):
            continue
        ms_ = [ms(r.get("exec_ms")) for _, r in ps]
        if any(x is None for x in ms_):
            continue
        row = dict(row, status="OK", median_ms=str(int(statistics.median(ms_))))
        data[(eng, arm, q)] = row
        out.append((arm, q, lost))
    return out

def suite_section(suite, d, stamp, rates=None):
    """(main lines, details lines, stats) for one suite. rates: engine_rates(meta)."""
    main, det = [], []
    st = {"suite": suite, "stamp": stamp, "geo": {}, "bad": {}, "invalid": [], "gaps": [], "queries": 0,
          "ok": False}
    csvs = all_files(d, "duckdb_warm-%s.csv") + all_files(d, "trino-%s.csv")
    data = {}
    for p in csvs:
        for r in read_csv(p):
            if r.get("suite") == suite and r.get("arm") != DROPPED_ARM:
                data[(r["engine"], r["arm"], r["query"])] = r
    st["recovered"] = recover_finished_hot(d, data) if suite == "hot" else []
    stamps = sorted({stamp_of(p) for p in csvs} - {None})
    st["stamp"] = stamp or ", ".join(stamps) or "?"
    main.append("### %s suite (run %s)\n" % (suite, st["stamp"]))
    if st["recovered"]:
        main.append("Recovered as OK (Trino FINISHED the query; only the CLI's result stream back through `kubectl exec` "
                    "broke, so that pass's server-side time is used and its rows are not compared): %s.\n" % ", ".join(
                        "%s %s (pass %s)" % (HEAD.get(("trino", a), "Trino " + a), q, ", ".join(ps)) for a, q, ps in st["recovered"]))
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

    # Cost: query seconds × the engine's node price, one "estimated cost (US$)" column after each arm
    # and a total row at the bottom (the queries every arm ran OK), as the compaction table.
    rates_ = rates or {}
    priced = bool(arms) and all(ea[0] in rates_ for ea in arms)
    cost = lambda ea, q, key: secs(ea, q, key) * rates_[ea[0]][0] if priced and secs(ea, q, key) is not None else None
    def arm_heads():
        return " | ".join(HEAD[ea] + (" | estimated cost (US$)" if priced else "") for ea in arms)
    def arm_cells(q, key, fmt=lambda v: "%.2f" % v, missing=lambda ea, q: "-"):
        out = []
        for ea in arms:
            v = secs(ea, q, key)
            out.append(fmt(v) if v is not None else missing(ea, q))
            if priced:
                c = cost(ea, q, key)
                out.append(usd(c) if c is not None else "-")
        return out
    def total_row(key, n_extra):
        both = [q for q in queries if all(secs(ea, q, key) is not None for ea in arms)]
        if not priced or not both:
            return []
        cells = []
        st.setdefault("cost_total", {})[key] = {ea: sum(cost(ea, q, key) for q in both) for ea in arms}
        st["rates"] = rates_
        for ea in arms:
            cells += ["**%.2f**" % sum(secs(ea, q, key) for q in both), "**$%s**" % usd(sum(cost(ea, q, key) for q in both))]
        return ["| **total, %d queries** | %s |" % (len(both), " | ".join(cells + [""] * n_extra))]
    def cost_note():
        return (" Cost (US$) = seconds × the engine's EC2 on-demand price (%s); query time only, idle time between "
                "queries is not counted." % "; ".join(rates_[e][1] for e in ("duckdb_warm", "trino") if e in rates_)) if priced else ""

    main.append("Median of the timed passes, seconds (DuckDB: execute + fetch in the server; Trino: elapsed - queued). "
                "Ratio > 1 = Trino slower.%s\n" % cost_note())
    ratio_cols = [(fmt, ("trino", fmt), ("duckdb_warm", fmt)) for fmt in FORMATS
                  if ("trino", fmt) in arms and ("duckdb_warm", fmt) in arms
                  and any(secs(("trino", fmt), q) and secs(("duckdb_warm", fmt), q) for q in queries)]
    main.append("| query | " + arm_heads() + "".join(" | Trino / DuckDB, %s" % f for f, _, _ in ratio_cols) + " |")
    main.append("|---" + "|---:" * (len(arms) * (2 if priced else 1) + len(ratio_cols)) + "|")
    for q in queries:
        cells = arm_cells(q, "median_ms", missing=lambda ea, q: cell(ea, q))
        for ea in arms:
            r = data.get((ea[0], ea[1], q))
            if r and r["status"] != "OK":
                st["bad"].setdefault(ea, []).append((q, r["status"]))
        for _, a, b in ratio_cols:
            x, y = secs(a, q), secs(b, q)
            cells.append("**%.2f**" % (x / y) if x is not None and y else "-")
        main.append("| %s | %s |" % (q, " | ".join(cells)))
    main += total_row("median_ms", len(ratio_cols))
    main.append("")
    if st["bad"]:
        main.append("Not OK (recorded, never compared): " + "; ".join(
            "%s: %s on %d of %d queries" % (HEAD[ea], "/".join(sorted({s for _, s in v})), len(v), len(queries))
            for ea, v in st["bad"].items()) + ".\n")

    # (engine column, format column, arm A, arm B)
    flabel = {"iceberg": "iceberg", "paimon": "paimon-as-iceberg"}
    pairs = [("Trino vs DuckDB", flabel[f], ("trino", f), ("duckdb_warm", f)) for f in FORMATS]
    pairs += [(ENGINE[e], "paimon-as-iceberg vs iceberg", (e, "paimon"), (e, "iceberg")) for e in ("duckdb_warm", "trino")]
    def geo_table(key, out):
        lines = []
        for eng, fmt, a, b in pairs:
            if a not in arms or b not in arms:
                continue
            rs = [secs(a, q, key) / secs(b, q, key) for q in queries if secs(a, q, key) and secs(b, q, key)]
            g = math.exp(sum(math.log(x) for x in rs) / len(rs)) if rs else None
            out[(a, b)] = (g, len(rs))
            if rs:
                lines.append("| %s | %s | **%.2f** | %d |" % (eng, fmt, g, len(rs)))
        return lines
    geo_table("median_ms", st["geo"])        # fills st["geo"]; shown in geo_summary()

    # ── cold start: its own category (snapshot suite: pass 0 right after both
    # engines' caches were emptied, one sample per query, timed as cold_ms).
    st["geo_cold"] = {}
    if any(secs(ea, q, "cold_ms") is not None for ea in arms for q in queries):
        main.append("#### %s suite, cold start (first pass after both engines' caches were emptied)\n" % suite)
        main.append("One timed sample per query and arm (not a median), so noisier than the warm table above. "
                    "Seconds; ratios > 1 = the first named slower.%s\n" % cost_note())
        fmt_cols = [("Paimon / Iceberg, %s" % ENGINE[e], (e, "paimon"), (e, "iceberg"))
                    for e in ("duckdb_warm", "trino")
                    if (e, "paimon") in arms and (e, "iceberg") in arms]
        eng_cols = [(f, ("trino", f), ("duckdb_warm", f)) for f in FORMATS if ("trino", f) in arms and ("duckdb_warm", f) in arms]
        main.append("| query | " + arm_heads()
                    + "".join(" | Trino / DuckDB, %s" % f for f, _, _ in eng_cols)
                    + "".join(" | %s" % h for h, _, _ in fmt_cols) + " |")
        main.append("|---" + "|---:" * (len(arms) * (2 if priced else 1) + len(eng_cols) + len(fmt_cols)) + "|")
        for q in queries:
            cells = arm_cells(q, "cold_ms")
            for _, a, b in eng_cols + fmt_cols:
                x, y = secs(a, q, "cold_ms"), secs(b, q, "cold_ms")
                cells.append("**%.2f**" % (x / y) if x is not None and y else "-")
            main.append("| %s | %s |" % (q, " | ".join(cells)))
        main += total_row("cold_ms", len(eng_cols) + len(fmt_cols))
        main.append("")
        geo_table("cold_ms", st["geo_cold"])  # fills st["geo_cold"]; shown in geo_summary()

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
        det.append("\nIceberg vs %s rows differ on: %s. A pipeline gap, not a failure: the two CDC jobs are pinned "
                   "at the same moment, not at the same binlog point%s." %
                   ("Paimon", ", ".join("%s (%s)" % g for g in st["gaps"]),
                    ", and Paimon-as-iceberg (its v3 Iceberg metadata: files above level 0 plus deletion vectors) lags native "
                    "Paimon by about one checkpoint"))
    det.append("")
    det.append("First row per arm:\n")
    det.append("| query | " + " | ".join(HEAD[ea] for ea in arms) + " |")
    det.append("|---" * (1 + len(arms)) + "|")
    for q in queries:
        det.append("| %s | %s |" % (q, " | ".join("`%s`" % (rows.get((ea[0], ea[1], q)) or ["-"])[0][:60].replace("|", "\\|")
                                                  for ea in arms)))
    det.append("\n</details>\n")

    # Pinned table state: the delete-file picture per table
    tps, hdr, body = read_tables(d)
    if tps:
        ix = {k: i for i, k in enumerate(hdr)}
        det.append("<details><summary>%s: pinned tables, data and delete files (%s)</summary>\n" % (
            suite, ", ".join(os.path.basename(x) for x in tps)))
        det.append("Per format and table, its pins in this suite (hot: one per pass, ranges over them; snapshot: one). "
                   + "`paimon` = the Paimon-as-iceberg metadata.\n")
        det.append("| format | table | pins | snapshot id (last) | records (rows) | data files | delete files | equality deletes (rows) | position deletes (rows, incl. DVs) | GB |")
        det.append("|---|---|---|---|---|---|---|---|---|---|")
        for fmt, tbl in [(f, t) for f in FORMATS for t in TABLES]:
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
        txt = drop_arm(open(pf).read())
        c = [int(x) for x in re.findall(r"QB_PIN_COMMIT_MS_\w+=(\d+)", txt)]
        if c:
            st["pin_span"] = (min(c), max(c))
            # hot-interleave writes one "## pins" block per pass; snapshot.sh pins once, with no header
            st["pins"] = len(re.findall(r"^## pins ", txt, re.M)) or 1
            st["query_s"] = query_seconds(d, txt)
            w = re.search(r"window=\[\d{4}-\d\d-\d\d (\S+), \d{4}-\d\d-\d\d (\S+)\)", txt)   # the hot data window
            if w:
                st["window"] = w.groups()
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
                    if eq and re.fullmatch(r"[a-z_0-9]+", k) and k.startswith(("iceberg_", "paimon_db", "cdc_", "duckdb_", "trino_", "engines", "formats")):
                        meta.setdefault(k, v)
    return meta


# ── Ingest (ingest_stats.py JSON) ──────────────────────────────────────────
def ingest_totals(ing):
    """{fmt: summed window figures} over the tables that were read."""
    out = {}
    for fmt in ING_FORMATS:
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


def ingest_section(ing, err, work=None):
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
    main3.append("**Throughput** (job-monitor reports overlapping the window: median; in brackets, total rows ingested for rows/s, max for MB/s)\n")
    g = lambda f, k: (mon.get(f) or {}).get(k)
    if mon:
        trows = [("Rows/s (total rows)", lambda f: "**%s** (%s)" % (num(g(f, "rows_per_s_median")), num(g(f, "rows_total")))),
                 ("Data size/s, MB/s", lambda f: "**%s** (%s)" % (mb(g(f, "bytes_per_s_median")), mb(g(f, "bytes_per_s_max"))))]
        if any(g(f, "checkpoint_ms_median") for f in ING_FORMATS):
            trows.append(("Checkpoint duration, median", lambda f: "%s ms" % num(g(f, "checkpoint_ms_median"))))
        if any(g(f, "backpressured_ratio_max") is not None for f in ING_FORMATS):
            trows.append(("Backpressure HIGH, max share of samples",
                          lambda f: "-" if g(f, "backpressured_ratio_max") is None else "%.0f%%" % (100 * g(f, "backpressured_ratio_max"))))
        main3 += ["| | Paimon | Iceberg |", "|---|---:|---:|"]
        main3 += ["| %s | %s | %s |" % (n, fn("paimon") if g("paimon", "reports") else "no reports",
                                         fn("iceberg") if g("iceberg", "reports") else "no reports") for n, fn in trows]
        main3.append("")
    else:
        main3.append("Job-monitor reports not available for this window (reports were pod-local before the S3 upload "
                     "was added); next run. The table-metadata numbers below cover the same window.\n")
    mon_ = {f: (ing.get("monitor") or {}).get(f) or {} for f in ("paimon", "iceberg")}
    if all(m.get("rows_per_s_median") for m in mon_.values()):
        bp = [m.get("backpressured_ratio_max") for m in mon_.values()]
        main3.append("**Outcome:** both writers %s: each read the same MySQL change stream at about %s change events a "
                     "second (an UPDATE counts as 2 events, so this is above the rows MySQL wrote)%s. Checkpoints, the "
                     "moment each writer makes its new data visible, took %s for Paimon and %s for Iceberg (median).\n" % (
                         "kept up" if bp and all(x == 0 for x in bp) else "read",
                         num(round(sum(m["rows_per_s_median"] for m in mon_.values()) / 2, -3)),
                         ", with no backlog" if bp and all(x == 0 for x in bp) else "",
                         "%.1f s" % (mon_["paimon"]["checkpoint_ms_median"] / 1000) if mon_["paimon"].get("checkpoint_ms_median") else "-",
                         "%.1f s" % (mon_["iceberg"]["checkpoint_ms_median"] / 1000) if mon_["iceberg"].get("checkpoint_ms_median") else "-"))

    # From the table metadata: what each format committed.
    main3.append("**From table metadata** (each table's commit history over the window)\n")

    def pi(fp, fi):
        return "| %s | %s |" % (fp(p) if p else "-", fi(i) if i else "-")
    rows = [
        ("Rows/s (window average)", lambda o: "**%s**" % num(o["rate"]), lambda o: "**%s**" % num(o["rate"])),
        ("Data size/s (new files)", lambda o: mbs(o.get("byte_rate")), lambda o: mbs(o.get("byte_rate"))),
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
    # What each format stored, in plain words (the per-table numbers are in the details).
    main3.append("**What each format stored**\n")
    notes = []
    if p.get("rows") and i.get("rows"):
        notes.append("**Rows saved:** Iceberg saved every version of every changed row, %s rows (about %s a second). "
                     "Paimon saved %s (%.0f%% of Iceberg's): when the same row changed more than once within a checkpoint "
                     "(60 s), it kept only the latest version." % (
                         mill(i["rows"]), num(i["rate"]), mill(p["rows"]), 100 * p["rows"] / i["rows"]))
    if i.get("rows") and i.get("equality_deletes") is not None:
        dl = (i.get("position_deletes") or 0) + i["equality_deletes"]
        notes.append("**Old versions:** for each changed row Iceberg also wrote a delete marker for the version it "
                     "replaces, %s markers in this window, and every query has to check them until a compaction "
                     "removes them. Paimon wrote none: it merges old versions away while loading (%s small merges "
                     "in the window)." % (mill(dl), num(p.get("compaction_commits")) if p.get("compaction_commits") is not None else "its inline"))
    sz = compacted_gib(work) if work else {}
    if p.get("byte_rate") and i.get("byte_rate"):
        txt = ("**Data size:** while loading, Paimon wrote about %.0f%% %s bytes than Iceberg (%s vs %s of new files)"
               % (abs(100 * p["byte_rate"] / i["byte_rate"] - 100), "more" if p["byte_rate"] > i["byte_rate"] else "fewer",
                  mbs(p["byte_rate"]), mbs(i["byte_rate"])))
        if sz.get("paimon") and sz.get("iceberg"):
            txt += ("; after both were fully compacted, Paimon's tables were %.0f%% %s (%.1f vs %.1f GiB) for the same rows"
                    % (abs(100 * sz["paimon"] / sz["iceberg"] - 100), "bigger" if sz["paimon"] > sz["iceberg"] else "smaller",
                       sz["paimon"], sz["iceberg"]))
        txt += (". Each Paimon row also stores a copy of its key, a version number and a change type, which it needs to "
                "merge changes (`data-file.thin-mode`, off by default, would drop the key copy); its newest files are also "
                "lz4- rather than zstd-compressed (`file.compression.per.level = 0:lz4`).")
        notes.append(txt)
    rds = (ing.get("config") or {}).get("rds") or {}
    if rds.get("cpu_avg") is not None:
        notes.append("**Source database:** RDS CPU averaged %s%% over the window (%s%% max), so MySQL was not the limit."
                     % (rds["cpu_avg"], rds["cpu_max"]))
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
    det.append("| format | table | rows/s | rows | write commits | median interval s | data files | delete files | eq deletes (rows) | pos deletes (rows) | DVs | compaction commits | first / last commit |")
    det.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for fmt in ING_FORMATS:
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


def query_seconds(d, txt):
    """Wall-clock seconds a run spent querying, from its pins file: queries_started_at
    (hot-interleave.sh) or caches_cleared_at (snapshot.sh) to finished_at. Hot runs from
    before 2026-11-08 record neither: then from the Trino query ids (YYYYMMDD_HHMMSS =
    the query's creation, UTC), the last end minus the first start, plus pass 0's DuckDB
    time (DuckDB runs before Trino in every pass). None if neither is there."""
    ts = lambda k: (lambda m: dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%SZ") if m else None)(
        re.search(r"^%s=(\d{4}-\d\d-\d\d \d\d:\d\d:\d\dZ)" % k, txt, re.M))
    a, b = ts("queries_started_at") or ts("caches_cleared_at"), ts("finished_at")
    if a and b:
        return (b - a).total_seconds()
    q = []
    for f in glob.glob(os.path.join(d, "trino-*-p*-queries.csv")):
        for r in csv.DictReader(open(f)):
            if re.match(r"\d{8}_\d{6}", r.get("query_id") or "") and (r.get("elapsed_ms") or "").isdigit():
                t = dt.datetime.strptime(r["query_id"][:15], "%Y%m%d_%H%M%S")
                q.append((t, t + dt.timedelta(milliseconds=int(r["elapsed_ms"]))))
    if not q:
        return None
    duck = 0
    for f in glob.glob(os.path.join(d, "duckdb_warm-*-p0.csv")):
        duck += sum(int(r["median_ms"]) for r in csv.DictReader(l for l in open(f) if not l.startswith("#"))
                    if (r.get("median_ms") or "").isdigit())
    return (max(x[1] for x in q) - min(x[0] for x in q)).total_seconds() + duck / 1000


def iceberg_specs(work):
    """{table: Iceberg partition spec} from the pins' tables-<STAMP>.tsv ('spec=...' notes)."""
    specs = {}
    for f in glob.glob(os.path.join(work, "**", "tables-*.tsv"), recursive=True):
        for r in read_csv_tsv(f):
            m = re.search(r"\bspec=(\S+)", r.get("note") or "")
            if r.get("format") == "iceberg" and m:
                specs[r["table"]] = m.group(1)
    return specs


def buckets_row(work):
    """One short line: N and key per table, from the Iceberg specs (Paimon's DDL pairs them)."""
    specs = iceberg_specs(work)
    per = []
    for t in TABLES:
        m = re.search(r"bucket\[(\d+)\]\((\w+)\)", specs.get(t, ""))
        if not m:
            continue
        rest = [x for x in specs[t].split(",") if not x.startswith("bucket[")]
        part = ("day" if rest[0].endswith("_dt") else rest[0]) if rest else ""
        per.append("%s %s%s" % (t.replace("_", " "), m.group(1), " per " + part if part else ""))
    if not per:
        return None
    return ("Both OTF formats split each table into the same fixed number of buckets by its ID: %s. "
            "Each bucket is written by one Flink writer." % ", ".join(per))


def read_csv_tsv(path):
    with open(path) as f:
        return list(csv.DictReader((l for l in f if l.strip()), delimiter="\t"))


GAIN_RE = re.compile(r"(?<![*\w.])(\d+(?:\.\d+)?(?:×| times|%) (?:faster|slower|as long|cheaper|more expensive))")


def bold_gains(text):
    """Bold the numbers that state an advantage or a slowdown ('7 times faster', '41% faster',
    '1.7× as long'), leaving the bullet's bold label alone."""
    return GAIN_RE.sub(r"**\1**", text)


def geo_summary(sts):
    """One table of every geometric mean (hot warm, snapshot warm, snapshot cold): per
    pair, the geometric mean of the per-query time ratios over the queries both arms
    ran OK. Replaces the per-suite tables."""
    cols = [("Trino / DuckDB, Iceberg", ("trino", "iceberg"), ("duckdb_warm", "iceberg")),
            ("Trino / DuckDB, Paimon", ("trino", "paimon"), ("duckdb_warm", "paimon")),
            ("Paimon / Iceberg, DuckDB", ("duckdb_warm", "paimon"), ("duckdb_warm", "iceberg")),
            ("Paimon / Iceberg, Trino", ("trino", "paimon"), ("trino", "iceberg"))]
    rows = []
    for st in sts:
        if not st.get("ok"):
            continue
        for key, label in (("geo", "warm"), ("geo_cold", "cold start")):
            geo = st.get(key) or {}
            vals = [geo.get((a, b)) or (None, 0) for _, a, b in cols]
            if any(g for g, _ in vals):
                ns = sorted({n for g, n in vals if g})
                rows.append("| %s, %s | %s | %s |" % (st["suite"].capitalize(), label, " | ".join(
                    "**%.2f**" % g if g else "-" for g, _ in vals), "/".join(str(n) for n in ns)))
    if not rows:
        return []
    # Estimated cost: each suite's total cost (the latency tables' total rows), A vs B in %.
    crow = []
    for st in sts:
        for key, label in (("median_ms", "warm"), ("cold_ms", "cold start")):
            tot = (st.get("cost_total") or {}).get(key)
            if not tot:
                continue
            cells = []
            for _, a, b in cols:
                if tot.get(a) and tot.get(b):
                    d = 100 * (tot[a] / tot[b] - 1)
                    cells.append("about the same" if abs(d) < 5 else "%.0f%% %s" % (abs(d), "cheaper" if d < 0 else "more"))
                else:
                    cells.append("-")
            crow.append("| %s, %s | %s |" % (st["suite"].capitalize(), label, " | ".join(cells)))
    rt = next((st["rates"] for st in sts if st.get("rates")), {})
    pr = (rt["trino"][0] / rt["duckdb_warm"][0]) if rt.get("trino") and rt.get("duckdb_warm") else None
    cost_tbl = (["**Estimated cost**: A's total query cost against B's for the same suite (the total rows of the latency "
                 "tables: query time × the engines' EC2 on-demand price).%s\n" % (
                     " Comparing formats on one engine, cost follows time; comparing engines, Trino's cluster costs %.0f%% "
                     "more per second than DuckDB's node." % (100 * (pr - 1)) if pr else ""),
                 "| suite | " + " | ".join(c for c, _, _ in cols) + " |",
                 "|---" + "|---:" * len(cols) + "|"] + crow + [""]) if crow else []
    return (["**Speed: geometric mean of the time ratios**\n",
             "Per pair: the geometric mean of time A / time B over the queries both ran OK (A is named first; "
             "> 1 = A slower, < 1 = A faster).\n",
             "| suite | " + " | ".join(c for c, _, _ in cols) + " | queries |",
             "|---" + "|---:" * (len(cols) + 1) + "|"] + rows + [""] + cost_tbl)


def compacted_gib(work):
    """{format: GiB of data after the newest full compaction of each table} from compaction/*.json."""
    newest = {}
    for f in glob.glob(os.path.join(work, "compaction", "*.json")):
        try:
            r = json.load(open(f))
        except Exception:
            continue
        k = (r.get("format"), (r.get("table") or "").split(".")[-1])
        stamp = os.path.basename(f).split("-", 1)[0]
        if r.get("status") == "OK" and (r.get("after") or {}).get("bytes") and stamp >= newest.get(k, ("", None))[0]:
            newest[k] = (stamp, r["after"]["bytes"])
    out = {}
    for (fmt, _), (_, b) in newest.items():
        out[fmt] = out.get(fmt, 0) + b / 2**30
    return out if len({t for _, t in newest}) and all(sum(1 for f, _ in newest if f == x) == 4 for x in out) else {}


def compaction_section(work):
    """compaction/run.sh's per-table JSONs (<STAMP>-<fmt>-<table>.json): per format and
    table the newest run."""
    runs = {}
    for f in sorted(glob.glob(os.path.join(work, "compaction", "*.json"))):
        try:
            r = json.load(open(f))
        except Exception:
            continue
        r["_stamp"] = os.path.basename(f).split("-", 1)[0]
        runs.setdefault((r.get("format", ""), (r.get("table") or "").split(".")[-1]), []).append(r)
    if not runs:
        return []
    gib = lambda b: "%.2f" % ((b or 0) / 2**30)
    stamps = sorted({max(r["_stamp"] for r in rs) for rs in runs.values()})   # the runs the table shows
    L = ["### Compaction before the snapshot test (run%s %s)\n" % ("s" if len(stamps) > 1 else "", ", ".join(stamps)),
         "`compaction/run.sh`, writers stopped: Paimon `sys.compact` (full); Iceberg data files with deletes rewritten with them "
         "applied and the delete files removed, then snapshots expired (12 h, keep 5). One row per table: its newest run.\n",
         "Cost: the job's duration × its pods' requests (JobManager + TaskManagers): "
         "EC2 US$%.5f per vCPU-hour and US$%.6f per GiB-hour (a linear fit to on-demand m6g / r6g, us-west-2), plus "
         "EMR on EKS US$%.6f per vCPU-hour and US$%.9f per GB-hour (us-west-2).\n"
         % (POD_VCPU_H, POD_GIB_H, EMR_VCPU_H, EMR_GB_H),
         "| format | table | duration (s) | TaskManagers | estimated cost (US$) | data files (files) | GiB | rows in files (rows) | delete files (files) | eq deletes (rows) | pos deletes (rows) |",
         "|---|---|---:|---|---:|---|---|---|---|---:|---:|"]
    totals = {}
    for (fmt, table), rs in sorted(runs.items()):
        rs.sort(key=lambda r: r["_stamp"])
        r = rs[-1]
        b, a = r.get("before", {}), r.get("after", {})
        arrow = lambda k, fmt=lambda x: "{:,}".format(x or 0): "%s -> %s" % (fmt(b.get(k)), fmt(a.get(k))) if a else fmt(b.get(k))
        tms, shape, cost = compaction_cost(fmt, table, r)
        if cost is not None:
            totals.setdefault(fmt, [0.0, 0.0])
            totals[fmt][0] += cost
            totals[fmt][1] += r.get("duration_s") or 0
        # no status column: a run that did not finish OK says so next to its table
        L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            fmt, table if r.get("status") == "OK" else "%s (**%s**)" % (table, r.get("status")), r.get("duration_s"), "%d × %s" % (tms, shape) if tms else "-",
            usd(cost) if cost is not None else "-",
            arrow("data_files"), arrow("bytes", gib), arrow("records_in_files"), arrow("delete_files"),
            "{:,}".format(b.get("equality_deletes") or 0), "{:,}".format(b.get("position_deletes") or 0)))
    for fmt, (c, sec) in sorted(totals.items()):
        L.append("| **%s total** | | **%.1f** | | **$%s** | | | | | | |" % (fmt, sec, usd(c)))
    return L + [""]


def compaction_cost(fmt, table, r):
    """(TaskManagers, 'vCPU / GiB' per TM, US$) of one compaction run, from the shape the
    result JSON records (cdc-compact.py, since 2026-11-11) or compaction/run.sh's defaults."""
    res, dfl = r.get("resources") or {}, COMPACT_DEFAULTS
    tm_cpu = float(res.get("tm_cpu") or dfl["tm_cpu"])
    big = fmt == "iceberg" and table in dfl["big_tm_tables"]
    tm_mem = float(res.get("tm_memory_gi") or (dfl["big_tm_memory_gi"] if big else dfl["tm_memory_gi"]))
    slots = int(res.get("tm_slots") or dfl["slots"].get(fmt, 1))
    par = int(r.get("parallelism") or r.get("parallelism_cap") or 0)
    dur = r.get("duration_s")
    if not par or dur is None:
        return None, None, None
    tms = -(-par // slots)
    cpu_h, gib_h = POD_VCPU_H + EMR_VCPU_H, POD_GIB_H + EMR_GB_H     # EC2 + EMR on EKS uplift
    jm = float(res.get("jm_cpu") or dfl["jm_cpu"]) * cpu_h + float(res.get("jm_memory_gi") or dfl["jm_memory_gi"]) * gib_h
    tm = tm_cpu * cpu_h + tm_mem * gib_h
    return tms, "%g vCPU / %g GiB" % (tm_cpu, tm_mem), dur / 3600 * (jm + tms * tm)


def actual_rate(ing):
    """What the CDC sources read from the binlog, all 4 tables (the job monitor's rate
    meter, median over the ingest window). These are change EVENTS, not MySQL rows: an
    UPDATE arrives as two (-U and +U), an INSERT or DELETE as one, so this is well above
    the rows/s the generator wrote (on _v3, 55.7K events/s against 38.4K rows/s from the
    generator's own counters). The generator's rate is not read here: its counters are
    in its pod logs (2-DATA-GEN-GUIDE.md, deploy-data-generator.sh status)."""
    mon = {f: m for f, m in ((ing or {}).get("monitor") or {}).items() if m and m.get("rows_per_s_median")}
    if not mon:
        return ""
    v = [mon[f]["rows_per_s_median"] for f in ("paimon", "iceberg") if f in mon]
    bp = [mon[f].get("backpressured_ratio_max") for f in mon]
    return ("; the CDC sources read about **%s change events/s** across the 4 tables (median over the ingest window: %s%s; "
            "an UPDATE is 2 events, -U and +U, so MySQL wrote fewer rows/s than this)") % (
        num(round(sum(v) / len(v))), ", ".join("%s %s" % (f.capitalize(), num(mon[f]["rows_per_s_median"])) for f in ("paimon", "iceberg") if f in mon),
        ", no backpressure" if bp and all(x == 0 for x in bp) else "")


def config_table(meta, ing, sts, fv, work=None):
    cfg = (ing or {}).get("config") or {}
    rows = []
    fl = cfg.get("flink") or {}
    if fl:
        desc = lambda c: "parallelism %s -> %s TMs × %s slots (%s CPU / %s each TM), checkpoint every %s" % (
            c.get("parallelism"), c.get("tms"), c.get("slots_per_tm"), c.get("tm_cpu"), c.get("tm_memory"),
            c.get("checkpoint_interval"))
        cs = {f: desc(c) for f, c in fl.items() if c}
        rows.append(("Flink writers", ("Paimon and Iceberg v3 jobs alike: " + next(iter(cs.values()))) if len(set(cs.values())) == 1
                     else "; ".join("%s: %s" % (f.capitalize(), v) for f, v in sorted(cs.items(), reverse=True))))
    rds = cfg.get("rds") or {}
    if rds:
        rows.append(("Source", "RDS %s, `%s`, %s" % (rds.get("engine"), rds.get("class"), rds.get("storage"))))
    g = cfg.get("generator") or {}
    if g:
        def mix(u, r):   # "0.9", "0.3" -> the share of rows per kind, or None
            try:
                u, r = float(u), float(r)
            except (TypeError, ValueError):
                return None
            return r, (1 - r) * u, (1 - r) * (1 - u)
        m = mix(g.get("UPSERT_RATIO"), g.get("RECENT_KEY_RATIO"))
        tgt = (int(g["replicas"]) * int(g["RATE_PER_POD"])) if g.get("replicas") and (g.get("RATE_PER_POD") or "").isdigit() else None
        rows.append(("Generator", "`mysql-upsert-loadgen`, %s pods × `RATE_PER_POD=%s`%s; `RECENT_KEY_RATIO=%s` & `UPSERT_RATIO=%s`%s"
                     % (g.get("replicas") or "?", g.get("RATE_PER_POD"), " (target %s rows/s)" % num(tgt) if tgt else "",
                        g.get("RECENT_KEY_RATIO"), g.get("UPSERT_RATIO"),
                        ": settings to produce records with %.0f%% of recent re-writes, %.0f%% older-key updates, %.0f%% inserts"
                        % tuple(100 * x for x in m) if m else "")
                     + actual_rate(ing)))
    def gib(v):   # "342Gi" / "38Gi" -> 342.0 (Gi); None if unknown
        m = re.fullmatch(r"([0-9.]+)\s*(Gi|G|Mi|M)?", str(v or ""))
        if not m:
            return None
        x = float(m.group(1))
        return x / 1024 if (m.group(2) or "").startswith("M") else x
    nw = int(meta["trino_workers"]) if str(meta.get("trino_workers", "")).isdigit() else None
    wcpu, wmem = meta.get("trino_worker_cpu"), gib(meta.get("trino_worker_memory"))
    trino = "Trino %s: %s workers" % (meta.get("trino_version") or "?", nw if nw is not None else "?")
    if wcpu and wmem is not None:
        trino += " × %s CPU / %.0f Gi" % (wcpu, wmem)
        if nw and str(wcpu).replace(".", "").isdigit():
            trino += " (total **%g CPU / %.0f Gi**)" % (nw * float(wcpu), nw * wmem)
    if meta.get("trino_worker_ec2"):
        trino += ", one `%s` each" % meta["trino_worker_ec2"]
    if meta.get("trino_coord_cpu"):
        trino += "; coordinator %s CPU / %s on `%s` (schedules no splits)" % (
            meta["trino_coord_cpu"], meta.get("trino_coord_memory", "?"), meta.get("trino_coord_ec2", "?"))
    duck = "DuckDB %s (%s CPU / %s%s), warm" % (   # the image tag only: "duckdb-cdc-server:2.0.0" -> 2.0.0
        (meta.get("duckdb_image") or "pip").split("/")[-1].split(":")[-1], meta.get("duckdb_cpu", "?"), meta.get("duckdb_pod_memory", "?"),
        ", on one `%s`" % meta["duckdb_ec2"] if meta.get("duckdb_ec2") else "")
    rows.append(("Query Engines", "%s; %s" % (duck, trino)))
    sfx = (meta.get("cdc_db_suffix") or "").lstrip("_")   # the session, e.g. _v3final -> v3final
    rows.append(("Table set", "%s: `%s` / `%s`; %s" % (
        "2 databases with a session suffix %s (4 tables in each DB)" % sfx if sfx else "2 databases (4 tables in each DB)",
        meta.get("iceberg_db", "?"), meta.get("paimon_db", "?"),
        "Paimon has iceberg integration turned on by default, so downstream tools can either use Iceberg connector to query "
        "Paimon's Iceberg metadata or use Paimon connector to query Paimon's native metadata")))
    b = buckets_row(work) if work else None
    if b:
        rows.append(("Buckets", b))
    # The Iceberg library the Flink writer runs: the writer image's ARG ICEBERG_VERSION
    # (it replaces EMR's bundled Iceberg, which cannot commit to v3 tables).
    dfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ingest-deploy", "docker", "Dockerfile.cdc-paimon")
    try:
        dtxt = open(dfile).read()
    except OSError:
        dtxt = ""
    grab = lambda pat: (re.search(pat, dtxt, re.M) or [None, None])[1]
    ice, pai = grab(r"^ARG ICEBERG_VERSION=(\S+)"), grab(r"PAIMON_VERSION=(\S+?)\s")
    emr = grab(r"^ARG EMR_VERSION=(\S+)")
    rows.insert(0, ("Open Table Format(OTF)",   # first row of the table
                 "Iceberg v%d, Flink writer Apache Iceberg %s (`iceberg-flink-runtime-1.20`, replacing EMR's bundled 1.10.1); "
                 "Apache Paimon %s (`paimon-flink-1.20`, patched with the apache/paimon PR #10329 backport, + `paimon-s3`). "
                 "Both are added as jars to `/usr/lib/flink/lib` of the EMR on EKS %s Flink base image "
                 "(`public.ecr.aws/emr-on-eks/flink/emr-%s-flink`) by `ingest-deploy/docker/Dockerfile.cdc-paimon`"
                 % (fv, ice or "?", pai or "?", emr or "?", emr or "?")))
    for s in sts:
        if s["ok"]:
            span = s.get("pin_span")
            qs = s.get("query_s")
            rows.append(("%s run" % s["suite"].capitalize(), "%d queries%s%s" % (
                s["queries"], ", about %d min of querying" % max(1, round(qs / 60)) if qs else "",
                ("; data window %s to %s, queried %d times (warm-up + %d timed), each on tables re-pinned at the start of the "
                 "pass (commits %s to %s)" % (s["window"][0], s["window"][1], s["pins"], s["pins"] - 1, hhmm(span[0]), hhmm(span[1]))
                 if s.get("window") and s.get("pins", 1) > 1 else
                 "; %s, the pinned snapshots committed %s to %s" % (
                    "%d pin sets (one per pass: warm-up + timed)" % s["pins"] if s.get("pins", 1) > 1 else "1 pin set (after the writers stopped)",
                    hhmm(span[0]), hhmm(span[1]))) if span else "")))
    w = (ing or {}).get("window")
    if w:
        # right under "Flink writers" (or last if there is no writer row)
        at = next((k + 1 for k, r in enumerate(rows) if r[0] == "Flink writers"), len(rows))
        rows.insert(at, ("Ingest window", "%s to %s" % (w["from"], w["to"])))
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
                    fmt_text(fmt), g, s["suite"], n)
                hb.append(txt)
                con.append("Engine (%s, %s): Trino / DuckDB = %.2f over %d queries." % (fmt, s["suite"], g, n))
        for e in ("trino", "duckdb_warm"):
            g, n = geo.get(((e, "paimon"), (e, "iceberg")), (None, 0))
            if g:
                ng = len({q for q, en in s["gaps"] if en == ENGINE[e]})
                # v3 export (2026-11-07): files above level 0 plus deletion vectors, about one
                # checkpoint behind the Paimon table; the old v2 wording ("top-LSM-level files
                # only until a full compaction") no longer applies.
                hb.append("On %s, Paimon-as-iceberg answered in **%.2f×** Iceberg's time (%d queries); its rows "
                          "differed from Iceberg's on %d of %d queries (pinned at the same moment, not the same "
                          "binlog point; Paimon-as-iceberg trails native Paimon by about a checkpoint)." % (
                              ENGINE[e], g, n, ng, s["queries"]))
                con.append("Format (%s, %s): Paimon-as-iceberg / Iceberg = %.2f over %d queries; rows differ on %d." % (
                    ENGINE[e], s["suite"], g, n, ng))
        # Cold start: its own category (snapshot suite only has cold_ms).
        gc = s.get("geo_cold") or {}
        for fmt in FORMATS:
            g, n = gc.get((("trino", fmt), ("duckdb_warm", fmt)), (None, 0))
            if g:
                hb.append("Cold start (%s suite, first pass after the caches were emptied): on %s, Trino took **%.2f×** "
                          "DuckDB's time (%d queries, one sample each)." % (
                              s["suite"], fmt_text(fmt), g, n))
                con.append("Cold start, engine (%s, %s): Trino / DuckDB = %.2f over %d queries." % (fmt, s["suite"], g, n))
        for e in ("trino", "duckdb_warm"):
            g, n = gc.get(((e, "paimon"), (e, "iceberg")), (None, 0))
            if g:
                hb.append("Cold start (%s suite): on %s, Paimon-as-iceberg answered in **%.2f×** Iceberg's time (%d queries, one sample each)." % (
                    s["suite"], ENGINE[e], g, n))
                con.append("Cold start, format (%s, %s): Paimon / Iceberg = %.2f over %d queries." % (
                    ENGINE[e], s["suite"], g, n))
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
            opn.append("Iceberg vs Paimon-as-iceberg rows differ on %d %s queries: the two jobs are pinned at the same moment, "
                       "not the same binlog point, and Paimon-as-iceberg trails native Paimon by about a checkpoint." % (
                           len({q for q, _ in s["gaps"]}), s["suite"]))
    p, i = tot.get("paimon") or {}, tot.get("iceberg") or {}
    mp, mi = (mon or {}).get("paimon") or {}, (mon or {}).get("iceberg") or {}
    if mp.get("rows_per_s_median") and mi.get("rows_per_s_median"):
        hb.append("Ingest throughput (job monitor, median of %d / %d reports): Paimon **%s rows/s, %s**; Iceberg "
                  "**%s rows/s, %s** (rows/s = the CDC sources' rate meter, change events with an UPDATE counted twice; bytes/s = committed)." % (mp["reports"], mi["reports"], num(mp["rows_per_s_median"]), mbs(mp.get("bytes_per_s_median")),
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




def geo_of(st, key, a, b):
    """(A / B geometric mean, queries) of suite st's key ("geo" / "geo_cold"); (None, 0) if not compared."""
    return ((st or {}).get(key) or {}).get((a, b), (None, 0))


def plain_headline(sts, tot, mon=None, bucketed=False):
    """The Headline in plain English, for readers who do not know the formats or
    engines: what was compared, who was faster and by how much, from this run's
    geometric means (so it follows the data). The technical bullets stay under it."""
    by = {x["suite"]: x for x in sts if x.get("ok")}
    g = lambda st, key, a, b: geo_of(st, key, a, b)[0]

    def faster(r, a, b):
        """r = time(a) / time(b) -> plain words."""
        if r is None:
            return None
        if 0.9 <= r <= 1.1:
            return "%s and %s were about equally fast" % (a, b)
        win, x = (a, 1 / r) if r < 1 else (b, r)
        lose = b if win == a else a
        return ("%s was about %.0f times faster than %s" % (win, x, lose) if x >= 1.95
                else "%s was about %d%% faster than %s" % (win, round((x - 1) * 100), lose))

    out = ["**What was tested:** the same stream of online-shop orders was copied, as it happened, into two storage "
           "formats (Iceberg and Paimon), and the same questions were then asked of each with two query tools "
           "(DuckDB on one large server, Trino on a cluster of the same total size)."]
    hot, snap = by.get("hot"), by.get("snapshot")

    def both(d, t):
        fd, ft = faster(d, "Paimon", "Iceberg"), faster(t, "Paimon", "Iceberg")
        if fd and fd == ft:
            return ["with both query tools, " + fd]
        return [x for x in (fd and "with DuckDB, " + fd, ft and "with Trino, " + ft) if x]
    if hot:
        d = g(hot, "geo", ("duckdb_warm", "paimon"), ("duckdb_warm", "iceberg"))
        t = g(hot, "geo", ("trino", "paimon"), ("trino", "iceberg"))
        parts = both(d, t)
        if parts:
            out.append("**Questions about live, still-changing data (hot query):** %s. Paimon merges each change into its files while loading (ongoing compaction); "
                       "Iceberg leaves that merging for later, so every question has to sort through the latest changes first." % "; ".join(parts))
    if snap:
        d = g(snap, "geo", ("duckdb_warm", "paimon"), ("duckdb_warm", "iceberg"))
        t = g(snap, "geo", ("trino", "paimon"), ("trino", "iceberg"))
        parts = both(d, t)
        if parts:
            out.append("**Questions about settled data (snapshot test: ingestion stopped and both formats were fully compacted, i.e. every pending change merged into clean files):** %s. "
                       "After full compaction, the two formats are close." % "; ".join(parts))
    # The query tools: who was faster per format, warm (live / settled) and cold.
    def who(r):
        if r < 0.9:
            return "Trino was about %.1f× faster" % (1 / r)
        if r > 1.1:
            return "DuckDB was about %.1f× faster" % r
        return "the two were about equal"
    per = []
    for f, name in (("iceberg", "the bucketed Iceberg tables" if bucketed else "Iceberg"), ("paimon", "Paimon")):
        rs = [(g(st, "geo", ("trino", f), ("duckdb_warm", f)), label)
              for st, label in ((hot, "live data"), (snap, "settled data")) if st]
        rs = [(r, label) for r, label in rs if r]
        if rs and (all(r < 0.9 for r, _ in rs) or all(r > 1.1 for r, _ in rs)):   # one winner: name it once
            win = "Trino" if rs[0][0] < 1 else "DuckDB"
            per.append("on %s, %s was about %s" % (name, win, " and ".join(
                "%.1f× faster on %s" % (1 / r if r < 1 else r, label) for r, label in rs)))
        elif rs:
            per.append("on %s, %s" % (name, " and ".join("%s on %s" % (who(r), label) for r, label in rs)))
    cold = [(g(snap, "geo_cold", ("trino", f), ("duckdb_warm", f)), n) for f, n in (("iceberg", "Iceberg"), ("paimon", "Paimon"))] if snap else []
    cold = [(r, n) for r, n in cold if r]
    if cold and all(r > 1.1 for r, _ in cold):
        per.append("on the very first question after a cold start DuckDB was faster on both (Trino took %s as long)" % (
            (lambda lo, hi: "about %s×" % lo if lo == hi else "%s–%s×" % (lo, hi))(
                "%.1f" % min(r for r, _ in cold), "%.1f" % max(r for r, _ in cold))))
    elif cold:
        per.append("after a cold start, " + ", ".join("%s on %s" % (who(r), n) for r, n in cold))
    if per:
        out.append("**The query tool matters too:** " + "; ".join(per) + ".")
    mp, mi = (mon or {}).get("paimon") or {}, (mon or {}).get("iceberg") or {}
    i = (tot or {}).get("iceberg") or {}
    if mp.get("rows_per_s_median") and mi.get("rows_per_s_median"):
        lo, hi = sorted([mp["rows_per_s_median"], mi["rows_per_s_median"]])
        out.append("**Loading the data:** both formats kept up with the incoming orders, at about %s change events a second "
                   "each%s." % (num(round((lo + hi) / 2, -3)),
                                "" if hi / lo < 1.1 else " (Paimon %s, Iceberg %s)" % (num(mp["rows_per_s_median"]), num(mi["rows_per_s_median"]))))
    if i.get("equality_deletes"):
        out.append("**Why live queries are slower on Iceberg:** for every changed order, Iceberg writes the new version "
                   "plus a note saying \"ignore the old one\" (%s such notes in this test). Until a compaction merges them away, every "
                   "question has to read those notes. Paimon replaces old versions as it goes, so it has none." % (
                       "%.0f million" % (i["equality_deletes"] / 1e6) if i["equality_deletes"] >= 1e6 else num(i["equality_deletes"])))
    return out

def bottom_line(sts):
    """The verdict in plain words, computed from this run's geometric means: which
    format suits live data (hot) and settled data (snapshot, after a full compaction),
    and which query engine suits which format. 'N% faster' = the other one's time /
    this one's time - 1; 'N× faster' from 2× up; within 10% = about the same."""
    by = {s["suite"]: s for s in sts if s.get("ok")}
    fmt_r = lambda st, key, e: geo_of(st, key, (e, "paimon"), (e, "iceberg"))[0] if st else None
    def gain(x):   # x = slower time / faster time (> 1)
        return "%.1f×" % x if x >= 2 else "%.0f%%" % (100 * (x - 1))
    def cmp(r, a, b):   # r = a's time / b's time
        if r is None:
            return None
        if 0.9 <= r <= 1.1:
            return "%s and %s were about the same speed" % (a, b)
        return "%s was %s faster than %s" % ((a, gain(1 / r), b) if r < 1 else (b, gain(r), a))
    out = []
    hot, snap = by.get("hot"), by.get("snapshot")
    if hot:
        d, t = fmt_r(hot, "geo", "duckdb_warm"), fmt_r(hot, "geo", "trino")
        lead = ("**Live, changing data (hot queries): Paimon is the better fit.** " if d and t and d < 0.9 and t < 0.9
                else "**Live, changing data (hot queries):** ")
        bits = [x for x in (d and "with DuckDB, " + cmp(d, "Paimon", "Iceberg"), t and "with Trino, " + cmp(t, "Paimon", "Iceberg")) if x]
        if bits:
            txt = "; ".join(bits)
            out.append(lead + txt[:1].upper() + txt[1:] + ". Paimon tidies up changed rows while it loads them; "
                       "Iceberg leaves that work to every query until a separate clean-up (compaction) runs.")
    if snap:
        bits = []
        for e, name in (("duckdb_warm", "DuckDB"), ("trino", "Trino")):
            w, c = fmt_r(snap, "geo", e), fmt_r(snap, "geo_cold", e)
            if w:
                bits.append("with %s, %s%s" % (name, cmp(w, "Paimon", "Iceberg"),
                                               " (first query after a restart: %s)" % cmp(c, "Paimon", "Iceberg").split(" was ")[-1].replace(" than Iceberg", "")
                                               if c and not (0.9 <= c <= 1.1) and c < 1 else ""))
        if bits:
            out.append("**Settled data (after a full clean-up of both tables):** the gap mostly closes; " + "; ".join(bits) + ".")
    # Which engine suits which format: Trino / DuckDB per format, live data first.
    bits = []
    for f, name in (("iceberg", "Iceberg"), ("paimon", "Paimon")):
        r = None
        for st in (hot, snap):
            if st and r is None:
                r = geo_of(st, "geo", ("trino", f), ("duckdb_warm", f))[0]
        if r and not (0.9 <= r <= 1.1):
            bits.append("%s for %s (%s faster than %s%s)" % (
                "Trino" if r < 1 else "DuckDB", name, gain(1 / r if r < 1 else r), "DuckDB" if r < 1 else "Trino",
                " on live data" if hot else ""))
    if bits:
        out.append("**Choice of query engine:** " + "; ".join(bits) + ".")
    return out

def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--engine-shape", default="", help="key=value engine shape (qb_engine_shape), for runs that did not record it")
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
    for kv in (a.engine_shape or "").split():
        k, _, v = kv.partition("=")
        if k and v and "?" not in v and not meta.get(k):
            meta[k] = v
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
            m, dd, s = suite_section(suite, d, stamp, engine_rates(meta))
            lat += m
            det += dd
            sts.append(s)
    ing_main, ing_det, tot = ingest_section(ing, ing_err, a.work)
    mon = {f: m for f, m in ((ing or {}).get("monitor") or {}).items() if m and m.get("reports")}
    hb, con, opn = headline(sts, tot, mon)
    if ing and ing.get("window") and not mon:
        opn.append("Job-monitor throughput (rows/s, MB/s), checkpoints and backpressure: reports not available for "
                   "this window (reports were pod-local before the S3 upload was added); next run. `SUITE=hot run.sh "
                   "start` now brings the monitor up, and it copies each report to `<results prefix>/monitor/`.")
    elif ing and ing.get("window"):
        for f in ING_FORMATS:
            if not mon.get(f):
                opn.append("No %s job-monitor report overlaps the ingest window." % f)
    if not any(s["suite"] == "snapshot" for s in sts):
        opn.append("No snapshot suite in this summary (writers stopped, Iceberg compacted).")

    L = ["# CDC query benchmark: %s\n" % a.session,
         "Warm DuckDB vs Trino on the four CDC tables Flink writes, on native Iceberg and on Paimon-as-iceberg "
         "(cdc-pipeline/querybench/); both engines of a format read the same pinned snapshot per table.\n"]
    L += config_table(meta, ing, sts, fv, a.work)
    L += ["---\n"]
    ph = [bold_gains(x) for x in plain_headline(sts, tot, mon, bool(buckets_row(a.work)))]
    L += ["## 1. Headline\n"] + ["- " + b for b in (ph or ["No suite results."])] + [""] + geo_summary(sts)
    if hb:
        L += ["<details><summary>The same results, technical version</summary>\n"] + ["- " + b for b in hb] + ["", "</details>\n"]
    L += ["## 2. Query latency\n"] + compaction_section(a.work) + lat
    L += ing_main
    bl = bottom_line(sts)
    if bl:
        L += ["## Conclusions\n", "**Bottom line**\n"] + ["- " + b for b in bl] + ["", "**Details**\n"]
        L += ["%d. %s" % (n + 1, c) for n, c in enumerate(con)] + [""]
    else:
        L += ["## Conclusions\n"] + ["%d. %s" % (n + 1, c) for n, c in enumerate(con)] + [""]
    if opn:
        L += ["### Open items\n"] + ["- " + o for o in opn] + [""]
    L += ["---\n", "## Details\n"] + det + ing_det
    with open(a.out, "w") as f:
        f.write("\n".join(L) + "\n")
    print(a.out)


if __name__ == "__main__":
    main()
