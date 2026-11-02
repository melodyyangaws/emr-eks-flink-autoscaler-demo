#!/usr/bin/env python3
"""The CDC query benchmark's findings as one Markdown file, from one hot and/or
one snapshot run (querybench/run.sh summary gathers them from S3).

  summarize.py --work <dir with hot/ and/or snapshot/> --session <name>
               [--hot <STAMP>] [--snapshot <STAMP>] [--iceberg-format-version 3] [--warmup 1] --out <md>

Up to four arms: warm DuckDB (`duckdb_warm`) and Trino (`trino`), each on the
Iceberg tables (`iceberg`) and on Paimon's Iceberg export (`paimon`). Both
engines of a format read the same pinned snapshot per table. Per suite:
  - median seconds per query and arm, and Trino / DuckDB per format;
  - geometric means: Trino vs DuckDB per format, Paimon's export vs Iceberg
    per engine, over the queries both sides ran OK;
  - Trino's server-side breakdown (elapsed, queued, execution, CPU, input
    bytes; trino-<STAMP>-queries.csv) and the data each arm scanned;
  - result equality, every row of the last timed pass compared as text (the
    queries print engine-neutral text: DECIMAL(38,2) / BIGINT sums, timestamps
    as their first 19 characters; numbers and timestamps are normalised once
    more here). DuckDB vs Trino on the SAME format must match: a difference on
    a query both ran OK makes the suite INVALID (a wrong answer on one side).
    Iceberg vs Paimon differences are a pipeline gap (the export lags native
    Paimon by about a checkpoint), recorded, not a failure. An arm that did
    not run OK (duckdb:paimon on v3: a Paimon 2.1 export bug) shows its
    status and is never counted as a mismatch;
  - the pinned tables' state per format and table (snapshot id, records, data
    files, delete files, equality / position deletes, bytes) from
    tables-<STAMP>.tsv;
  - DuckDB server restarts, and every .pins file.
"""
import argparse
import csv
import glob
import math
import os
import re
import statistics
from decimal import Decimal, InvalidOperation

FORMATS = ["iceberg", "paimon"]
ARMS = [("duckdb_warm", "iceberg"), ("trino", "iceberg"), ("duckdb_warm", "paimon"), ("trino", "paimon")]
HEAD = {("duckdb_warm", "iceberg"): "DuckDB iceberg", ("trino", "iceberg"): "Trino iceberg",
        ("duckdb_warm", "paimon"): "DuckDB paimon (export)", ("trino", "paimon"): "Trino paimon (export)"}
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


def suite_section(suite, d, stamp, lines, verdicts):
    csvs = all_files(d, "duckdb_warm-%s.csv") + all_files(d, "trino-%s.csv")
    data = {}
    for p in csvs:
        for r in read_csv(p):
            if r.get("suite") == suite:
                data[(r["engine"], r["arm"], r["query"])] = r
    stamps = sorted({stamp_of(p) for p in csvs} - {None})
    lines.append("## %s suite (run %s)\n" % (suite, stamp or ", ".join(stamps) or "?"))
    if not data:
        lines.append("No results.\n")
        verdicts.append("%s: no results" % suite)
        return
    queries = sorted({k[2] for k in data})
    arms = [ea for ea in ARMS if any((ea[0], ea[1], q) in data for q in queries)]

    def secs(ea, q):
        r = data.get((ea[0], ea[1], q))
        if not r or r["status"] != "OK" or ms(r.get("median_ms")) is None:
            return None
        return ms(r["median_ms"]) / 1000.0

    def cell(ea, q):
        r, v = data.get((ea[0], ea[1], q)), secs(ea, q)
        return "%.2f" % v if v is not None else (r["status"] if r else "-")

    lines.append("Median of the timed passes, seconds. DuckDB: execute + fetch in the server; "
                 "Trino: execution = elapsed - queued (system.runtime.queries). Ratios > 1 = the first one slower.\n")
    ratio_cols = [(fmt, ("trino", fmt), ("duckdb_warm", fmt)) for fmt in FORMATS
                  if ("trino", fmt) in arms and ("duckdb_warm", fmt) in arms]
    lines.append("| query | " + " | ".join(HEAD[ea] for ea in arms) + "".join(" | Trino / DuckDB, %s" % f for f, _, _ in ratio_cols) + " |")
    lines.append("|---" * (1 + len(arms) + len(ratio_cols)) + "|")
    bad = []
    for q in queries:
        cells = [cell(ea, q) for ea in arms]
        for ea in arms:
            r = data.get((ea[0], ea[1], q))
            if r and r["status"] != "OK":
                bad.append("%s, %s: %s" % (q, HEAD[ea], r["status"]))
        for _, a, b in ratio_cols:
            x, y = secs(a, q), secs(b, q)
            cells.append("%.2f" % (x / y) if x is not None and y else "-")
        lines.append("| %s | %s |" % (q, " | ".join(cells)))
    lines.append("")
    lines.append("Not OK (an arm that errors is recorded, never compared): " + ("none" if not bad else ""))
    lines += ["- " + b for b in bad]
    lines.append("")

    lines.append("### Geometric mean of time A / time B over the queries both ran OK\n")
    lines.append("| comparison | A / B | queries |")
    lines.append("|---|---|---|")
    pairs = [("Engine on %s: Trino vs DuckDB" % f, a, b) for f, a, b in ratio_cols]
    pairs += [("Format on %s: Paimon's export vs Iceberg" % ("DuckDB" if e == "duckdb_warm" else "Trino"), (e, "paimon"), (e, "iceberg"))
              for e in ("duckdb_warm", "trino") if (e, "paimon") in arms and (e, "iceberg") in arms]
    for name, a, b in pairs:
        rs = [secs(a, q) / secs(b, q) for q in queries if secs(a, q) and secs(b, q)]
        g = math.exp(sum(math.log(x) for x in rs) / len(rs)) if rs else None
        lines.append("| %s | %s | %d |" % (name, "%.2f" % g if g else "-", len(rs)))
        if name.startswith("Engine"):
            verdicts.append("%s %s: Trino/DuckDB %s over %d" % (suite, a[1], "%.2f" % g if g else "-", len(rs)))
    lines.append("")

    # Trino's server-side breakdown (timed passes)
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
            lines.append("### Trino server-side breakdown (median over the timed passes)\n")
            lines.append("| arm | query | elapsed s | queued s | execution s | CPU s | physical input GB |")
            lines.append("|---|---|---|---|---|---|---|")
            for arm in FORMATS:
                for q in queries:
                    rs = per.get((arm, q))
                    if not rs:
                        continue
                    def med(k, div=1000.0):
                        vs = [ms(r.get(k)) for r in rs if ms(r.get(k)) is not None]
                        return "%.2f" % (statistics.median(vs) / div) if vs else "-"
                    lines.append("| %s | %s | %s | %s | %s | %s | %s |" % (arm, q, med("elapsed_ms"), med("queued_ms"),
                                 med("exec_ms"), med("cpu_ms"), med("physical_input_bytes", 1e9)))
            lines.append("")

    lines.append("### Data scanned, GB (DuckDB: HTTPFS bytes in an untimed pass with its cache bypassed; "
                 "Trino: physical input of the last timed pass, cache hits included)\n")
    lines.append("| query | " + " | ".join(HEAD[ea] for ea in arms) + " |")
    lines.append("|---" * (1 + len(arms)) + "|")
    for q in queries:
        c = []
        for ea in arms:
            v = (data.get((ea[0], ea[1], q)) or {}).get("scan_bytes") or ""
            c.append("%.2f" % (int(v) / 1e9) if v.isdigit() else "-")
        lines.append("| %s | %s |" % (q, " | ".join(c)))
    lines.append("")

    # Result equality
    rows = load_rows(d)
    lines.append("### Same rows across arms? (every row of the last timed pass, compared as text)\n")
    lines.append("Letters name distinct result sets per query; equal letters = identical rows. An arm that did not "
                 "run OK shows its status and is not compared.\n")
    lines.append("| query | " + " | ".join(HEAD[ea] for ea in arms) + " | rows |")
    lines.append("|---" * (2 + len(arms)) + "|")
    invalid, gaps = [], []
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
        lines.append("| %s | %s | %s |" % (q, " | ".join(cells), n))
        for fmt in FORMATS:   # the two engines on one format read the same pinned snapshot
            a, b = got.get(("duckdb_warm", fmt)), got.get(("trino", fmt))
            if a is None or b is None or a == b:
                continue
            first = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
            invalid.append("%s, %s: DuckDB and Trino differ (%s; first at row %d: `%s` vs `%s`)" % (
                q, fmt, "same rows, other order" if sorted(a) == sorted(b) else "different rows", first + 1,
                (a[first] if first < len(a) else "<none>")[:100], (b[first] if first < len(b) else "<none>")[:100]))
        for e in ("duckdb_warm", "trino"):
            a, b = got.get((e, "iceberg")), got.get((e, "paimon"))
            if a is not None and b is not None and a != b:
                gaps.append("%s (%s)" % (q, "DuckDB" if e == "duckdb_warm" else "Trino"))
    lines.append("")
    if invalid:
        verdicts.append("%s: INVALID (engines disagree on a format)" % suite)
        lines.append("**INVALID:** both engines of a format read the same pinned snapshot, so these are wrong answers on one side:")
        lines += ["- " + x for x in invalid]
    else:
        lines.append("Validity: on each format, every query both engines ran OK returned the same rows.")
        verdicts.append("%s: engines agree per format" % suite)
    if gaps:
        lines.append("\nIceberg vs Paimon rows differ on: %s. A pipeline gap, not a failure: the two CDC jobs are pinned "
                     "at the same moment, not at the same binlog point, and Paimon's export lags native Paimon by about "
                     "one checkpoint (and lists only top-LSM-level files until a full compaction)." % ", ".join(gaps))
    lines.append("")
    lines.append("<details><summary>First row per arm</summary>\n")
    lines.append("| query | " + " | ".join(HEAD[ea] for ea in arms) + " |")
    lines.append("|---" * (1 + len(arms)) + "|")
    for q in queries:
        lines.append("| %s | %s |" % (q, " | ".join("`%s`" % (rows.get((ea[0], ea[1], q)) or ["-"])[0][:60].replace("|", "\\|")
                                                    for ea in arms)))
    lines.append("\n</details>\n")

    # Pinned table state: the delete-file picture per table
    tps = all_files(d, "tables-%s.tsv")
    if tps:
        t = []
        for tp in tps:
            ls = [l.rstrip("\n").split("\t") for l in open(tp) if l.strip()]
            t += ls if not t else ls[1:]
        hdr, body = t[0], t[1:]
        ix = {k: i for i, k in enumerate(hdr)}
        lines.append("### Pinned tables: data and delete files (%s)\n" % ", ".join(os.path.basename(x) for x in tps))
        lines.append("Per format and table, its pins in this suite (hot: one per pass, ranges over them; snapshot: one, after compaction).\n")
        lines.append("`paimon` = Paimon's Iceberg export metadata; `paimon_native` = the native Paimon snapshot it was "
                     "exported from (records only).\n")
        lines.append("| format | table | pins | snapshot id (last) | records | data files | delete files | equality deletes | position deletes (incl. DVs) | GB |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for fmt, tbl in [(f, t) for f in ("iceberg", "paimon", "paimon_native") for t in ("customers", "products", "orders", "order_items")]:
            rs = [r for r in body if len(r) > ix["table"] and r[ix["table"]] == tbl and r[ix["format"]] == fmt]
            if not rs:
                continue
            last = rs[-1]
            g_ = lambda k: last[ix[k]] if k in ix and ix[k] < len(last) else ""
            rng = lambda k: (lambda vs: "-" if not vs else (str(min(vs)) if min(vs) == max(vs) else "%d..%d" % (min(vs), max(vs))))(
                [int(r[ix[k]]) for r in rs if k in ix and ix[k] < len(r) and r[ix[k]].isdigit()])
            gb = g_("total-files-size")
            lines.append("| %s | %s | %d | %s | %s | %s | %s | %s | %s | %s |" % (
                fmt, tbl, len(rs), g_("snapshot_id"), rng("total-records"), rng("total-data-files"), rng("total-delete-files"),
                rng("total-equality-deletes"), rng("total-position-deletes"), "%.2f" % (int(gb) / 1e9) if gb.isdigit() else "-"))
        lines.append("")
        lines.append("<details><summary>Every pin (tables TSV)</summary>\n")
        lines.append("| " + " | ".join(hdr) + " |")
        lines.append("|---" * len(hdr) + "|")
        for r in body:
            lines.append("| " + " | ".join(x.replace("|", "\\|") for x in r + [""] * (len(hdr) - len(r))) + " |")
        lines.append("\n</details>\n")
    e = []
    for ev in all_files(d, "duckdb_warm-%s-events.csv"):
        e += [l.strip() for l in open(ev) if l.strip()][1:]
    if e:
        lines.append("### DuckDB server restarts (`restart`) and the queries after them in the same pass (`cold`)\n")
        lines += ["- " + x for x in e]
        lines.append("")
    for pf in sorted(glob.glob(os.path.join(d, "*.pins"))):
        lines.append("<details><summary>Pins, gates and windows (%s)</summary>\n\n```" % os.path.basename(pf))
        lines += [l.rstrip("\n") for l in open(pf)]
        lines.append("```\n</details>\n")


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


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--work", required=True)
    ap.add_argument("--session", required=True)
    ap.add_argument("--hot")
    ap.add_argument("--snapshot")
    ap.add_argument("--iceberg-format-version", help="2|3; default: the runs' .pins files, else 3")
    ap.add_argument("--warmup", type=int, default=1, help="the snapshot run's WARMUP (default 1)")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    WARMUP[0] = a.warmup
    lines, verdicts, body = [], [], []
    dirs = [os.path.join(a.work, x) for x in ("hot", "snapshot") if os.path.isdir(os.path.join(a.work, x))]
    meta = run_meta(dirs)
    fv = int(meta.get("iceberg_format_version") or a.iceberg_format_version or 3)
    if a.iceberg_format_version and meta.get("iceberg_format_version") and int(a.iceberg_format_version) != fv:
        print("note: the runs' pins say iceberg_format_version=%d, not %s; using the pins" % (fv, a.iceberg_format_version))
    for suite, st in (("hot", a.hot), ("snapshot", a.snapshot)):
        d = os.path.join(a.work, suite)
        if os.path.isdir(d):
            suite_section(suite, d, st, body, verdicts)
    lines.append("# CDC query benchmark: %s\n" % a.session)
    lines.append("Warm DuckDB vs Trino over the four CDC tables (cdc-pipeline/querybench/), on the Iceberg tables and "
                 "on Paimon's Iceberg export; both engines of a format on the same pinned snapshot per table (DuckDB "
                 "`iceberg_scan` of a copied metadata.json, Trino `FOR VERSION AS OF` its snapshot id).\n")
    lines.append("Table set: Iceberg format version **%d**%s; databases %s / %s; engines %s; formats %s; DuckDB %s "
                 "(%s cpu / %s; puffin verification skipped on: %s); Trino %s (%s workers).\n" % (
                     fv, " (CDC_DB_SUFFIX `%s`)" % meta["cdc_db_suffix"] if meta.get("cdc_db_suffix") else "",
                     meta.get("iceberg_db", "?"), meta.get("paimon_db", "?"), meta.get("engines", "?"),
                     meta.get("formats", "?"), meta.get("duckdb_image") or "?",
                     meta.get("duckdb_cpu", "?"), meta.get("duckdb_pod_memory", "?"),
                     meta.get("duckdb_skip_puffin_verification_arms") or "none",
                     meta.get("trino_version") or "?", meta.get("trino_workers") or "?"))
    lines.append("Known write-side differences (table settings are otherwise paired; querybench/README.md): "
                 "Paimon writes orders/order_items with 32 writers (one per bucket), Iceberg's hash distribution "
                 "sends each date partition to ONE writer; Paimon compacts and expires snapshots inline, Iceberg "
                 "only in the compaction job before the snapshot test.\n")
    lines.append("Verdicts: " + ("; ".join(verdicts) if verdicts else "no suites found") + "\n")
    lines += body
    with open(a.out, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(a.out)


if __name__ == "__main__":
    main()
