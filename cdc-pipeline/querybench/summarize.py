#!/usr/bin/env python3
"""The CDC query benchmark's findings as one Markdown file, from one hot and/or
one snapshot run (querybench/run.sh summary gathers them from S3).

  summarize.py --work <dir with hot/ and/or snapshot/> --session <name>
               [--hot <STAMP>] [--snapshot <STAMP>] [--must-match "S1_... S7_..."] --out <md>

Per suite: median seconds per arm (StarRocks paimon / pmn_as_iceberg / iceberg,
DuckDB paimon / iceberg) with the like-for-like ratios, the geometric-mean
comparison per pair of arms, whether the arms returned the same rows (ALL rows,
not only the first), the pinned tables' state (snapshot ids, data / delete
files, equality / position deletes) and DuckDB server restarts.

Validity (snapshot only): for the --must-match queries, StarRocks `paimon` and
`pmn_as_iceberg` read the same Paimon files at the same snapshot, so their rows
must be equal, or the run is INVALID (Paimon's Iceberg export lists only the
top LSM level until Paimon is fully compacted). Paimon vs Iceberg rows differ
when the two CDC jobs stopped at different binlog points: recorded as a gap,
not a failure. In the hot suite every difference is informational.

Iceberg v3 (iceberg_format_version=3 in the run's .pins files, or
--iceberg-format-version 3): StarRocks' Iceberg-reader arms (pmn_as_iceberg,
iceberg) are skipped by the harness (4.1.x cannot read v3 deletion vectors) or,
with SR_V3_ARMS=1, run only to record their error, and StarRocks `paimon` runs
on its JNI reader (64-bit DVs). The validity check then compares StarRocks
`paimon` with DuckDB `paimon` (iceberg_scan over Paimon's v3 export, puffin
verification skipped): the two decode the deletion vectors independently, so
this is also the guard against a wrong DV decode on either side.
"""
import argparse
import csv
import glob
import math
import os
import re
from collections import OrderedDict
from decimal import Decimal, InvalidOperation

CTX = {"fv": 3, "skipped": set(), "jni": False}   # set by main() from the runs' pins
ARMS = [("starrocks", "paimon"), ("starrocks", "pmn_as_iceberg"), ("starrocks", "iceberg"),
        ("duckdb_warm", "paimon"), ("duckdb_warm", "iceberg")]
HEAD = ["SR paimon", "SR pmn_as_iceberg", "SR iceberg", "DuckDB paimon (export)", "DuckDB iceberg"]
RATIOS = [("DuckDB/SR, Paimon export", ("duckdb_warm", "paimon"), ("starrocks", "pmn_as_iceberg")),
          ("DuckDB/SR, Iceberg", ("duckdb_warm", "iceberg"), ("starrocks", "iceberg")),
          ("SR iceberg/paimon", ("starrocks", "iceberg"), ("starrocks", "paimon")),
          ("SR pmn/paimon", ("starrocks", "pmn_as_iceberg"), ("starrocks", "paimon"))]
PAIRS = [("Reader (SR, same Paimon files): Iceberg reader vs Paimon reader", ("starrocks", "pmn_as_iceberg"), ("starrocks", "paimon")),
         ("Format (SR, each format's own reader): Iceberg vs Paimon", ("starrocks", "iceberg"), ("starrocks", "paimon")),
         ("Writer (SR, Iceberg reader): Iceberg table vs Paimon's export", ("starrocks", "iceberg"), ("starrocks", "pmn_as_iceberg")),
         ("Writer (DuckDB): Iceberg table vs Paimon's export", ("duckdb_warm", "iceberg"), ("duckdb_warm", "paimon")),
         ("Engine on Paimon's export: DuckDB vs StarRocks", ("duckdb_warm", "paimon"), ("starrocks", "pmn_as_iceberg")),
         ("Engine on the Iceberg table: DuckDB vs StarRocks", ("duckdb_warm", "iceberg"), ("starrocks", "iceberg"))]


def label(ea):
    return ("SR" if ea[0] == "starrocks" else "DuckDB") + " `%s`" % ea[1]


def read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(l for l in f if l.strip() and not l.startswith("#")))


def all_files(d, pattern):
    """Every run file matching <pattern % '*'> that is not a per-pass (-pN) file:
    a suite directory holds one run, or, for the snapshot suite run as two
    separate phases (SNAPSHOT_FORMAT=paimon / iceberg), one run per phase."""
    rx = re.compile("^" + re.escape(pattern).replace("%s", r"\d{8}T\d{6}Z") + "$")
    return sorted(p for p in glob.glob(os.path.join(d, pattern % "*")) if rx.match(os.path.basename(p)))


def pick(d, pattern, stamp):
    """The run's file: <pattern % stamp>, or, without a stamp, the one file
    matching the pattern that is not a per-pass (-pN) file."""
    if stamp:
        p = os.path.join(d, pattern % stamp)
        return p if os.path.exists(p) else None
    c = [p for p in glob.glob(os.path.join(d, pattern % "*")) if not re.search(r"-p\d+[.-]", os.path.basename(p))]
    return sorted(c)[-1] if c else None


def stamp_of(path, prefix):
    m = re.match(re.escape(prefix) + r"-(\d{8}T\d{6}Z)", os.path.basename(path or ""))
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
    return v


def load_rows(d):
    """{(engine, arm, query): [normalised row, ...]} from every -rows.tsv file."""
    out = {}
    for eng in ("starrocks", "duckdb_warm"):
      for p in all_files(d, eng + "-%s-rows.tsv"):
        for line in open(p):
            parts = line.rstrip("\n").split("\t", 4)
            if len(parts) < 5:
                continue
            arm, _suite, q, n, row = parts
            out.setdefault((eng, arm, q), []).append((int(n), "|".join(norm_field(x) for x in row.split("|"))))
    return {k: [r for _, r in sorted(v)] for k, v in out.items()}


def suite_section(suite, d, stamp, must_match, lines, verdicts):
    csvs = all_files(d, "starrocks-%s.csv") + all_files(d, "duckdb_warm-%s.csv")
    data = {}
    for p in csvs:
        for r in read_csv(p):
            if r.get("suite") == suite:
                data[(r["engine"], r["arm"], r["query"])] = r
    stamps = sorted({stamp_of(p, os.path.basename(p).split("-")[0]) for p in csvs} - {None})
    lines.append("## %s suite (run %s)\n" % (suite, stamp or ", ".join(stamps) or "?"))
    if not data:
        lines.append("No results.\n")
        return
    queries = sorted({k[2] for k in data})

    def secs(ea, q):
        r = data.get((ea[0], ea[1], q))
        if not r or r["status"] != "OK" or not (r.get("median_ms") or "").strip():
            return None
        return int(float(r["median_ms"])) / 1000.0

    lines.append("Median of the timed passes, seconds (ratios a/b; > 1 = a slower).\n")
    lines.append("| query | " + " | ".join(HEAD + [x[0] for x in RATIOS]) + " |")
    lines.append("|---" * (1 + len(HEAD) + len(RATIOS)) + "|")
    bad = []
    for q in queries:
        cells = []
        for ea, h in zip(ARMS, HEAD):
            r = data.get((ea[0], ea[1], q))
            v = secs(ea, q)
            cells.append("%.2f" % v if v is not None else (r["status"] if r else
                         ("skip" if ea[0] == "starrocks" and ea[1] in CTX["skipped"] else "-")))
            if r and r["status"] != "OK":
                bad.append("%s, %s: %s" % (q, h, r["status"]))
        for _, a, b in RATIOS:
            x, y = secs(a, q), secs(b, q)
            cells.append("%.2f" % (x / y) if x is not None and y else "-")
        lines.append("| %s | %s |" % (q, " | ".join(cells)))
    lines.append("")
    lines.append("Not OK: " + ("none" if not bad else ""))
    lines += ["- " + b for b in bad]
    lines.append("")

    lines.append("### Like-for-like (geometric mean of time A / time B over the queries both ran OK)\n")
    lines.append("| comparison | A / B | queries |")
    lines.append("|---|---|---|")
    for name, a, b in PAIRS:
        rs = [secs(a, q) / secs(b, q) for q in queries if secs(a, q) and secs(b, q)]
        g = math.exp(sum(math.log(x) for x in rs) / len(rs)) if rs else None
        lines.append("| %s | %s | %d |" % (name, "%.2f" % g if g else "-", len(rs)))
    lines.append("")

    # Scan bytes
    lines.append("### Data scanned, GB (StarRocks ScanBytes; DuckDB HTTPFS bytes with its cache bypassed)\n")
    lines.append("| query | " + " | ".join(HEAD) + " |")
    lines.append("|---" * (1 + len(HEAD)) + "|")
    for q in queries:
        c = []
        for ea in ARMS:
            r = data.get((ea[0], ea[1], q))
            v = (r or {}).get("scan_bytes") or ""
            c.append("%.2f" % (int(v) / 1e9) if v.isdigit() else "-")
        lines.append("| %s | %s |" % (q, " | ".join(c)))
    lines.append("")

    # Row agreement
    rows = load_rows(d)
    lines.append("### Same rows across arms? (all rows of the last timed pass, numbers and timestamps normalised)\n")
    lines.append("Letters name distinct result sets per query; equal letters = identical rows.\n")
    lines.append("| query | " + " | ".join(HEAD) + " | rows |")
    lines.append("|---" * (2 + len(HEAD)) + "|")
    invalid, gaps = [], []
    for q in queries:
        sets, cells = [], []
        for ea in ARMS:
            r = rows.get((ea[0], ea[1], q))
            if r is None:
                cells.append("-")
                continue
            if r not in sets:
                sets.append(r)
            cells.append(chr(ord("A") + sets.index(r)))
        n = "/".join(sorted({str(len(s)) for s in sets})) or "-"
        lines.append("| %s | %s | %s |" % (q, " | ".join(cells), n))
        p, pm, ice = rows.get(("starrocks", "paimon", q)), rows.get(("starrocks", "pmn_as_iceberg", q)), rows.get(("starrocks", "iceberg", q))
        if suite == "snapshot" and q in must_match:
            if CTX["fv"] >= 3:
                # v3: SR pmn_as_iceberg is skipped (or only records its DV
                # error); DuckDB reads the same Paimon files through the export.
                dp = rows.get(("duckdb_warm", "paimon", q))
                if p is None or dp is None:
                    invalid.append("%s: no rows from SR paimon and/or DuckDB paimon to compare (Iceberg v3)" % q)
                elif p != dp:
                    invalid.append("%s: SR paimon and DuckDB paimon differ (Iceberg v3: Paimon's export incomplete, "
                                   "or a deletion vector decoded wrongly on one side)" % q)
            elif p is None or pm is None:
                invalid.append("%s: no rows from SR paimon and/or pmn_as_iceberg to compare" % q)
            elif p != pm:
                invalid.append("%s: SR paimon and pmn_as_iceberg differ (Paimon's export incomplete: fully compact Paimon first)" % q)
        # Paimon vs Iceberg: SR's own readers, or on v3 (no SR Iceberg reader) DuckDB's two arms.
        ip, ii = (p, ice) if CTX["fv"] < 3 else (rows.get(("duckdb_warm", "paimon", q)), rows.get(("duckdb_warm", "iceberg", q)))
        if ip is not None and ii is not None and ip != ii:
            gaps.append(q)
    lines.append("")
    if suite == "snapshot":
        invalid += ["%s: a must-match query that did not run" % q for q in sorted(must_match - set(queries))]
        if invalid:
            verdicts.append("snapshot: INVALID")
            lines.append("**INVALID:**")
            lines += ["- " + x for x in invalid]
        else:
            ref = "DuckDB `paimon`" if CTX["fv"] >= 3 else "`pmn_as_iceberg`"
            verdicts.append("snapshot: valid (%s match on %s)" % (", ".join(sorted(must_match)),
                            "SR paimon vs DuckDB paimon" if CTX["fv"] >= 3 else "paimon vs pmn_as_iceberg")
                            if must_match else "snapshot: no must-match queries")
            lines.append("Validity: the must-match queries agree on SR `paimon` and %s." % ref)
    else:
        lines.append("Hot: each format is pinned at its own snapshot and Paimon's export lags (top LSM level only), so differences are expected; H1's row per arm is what each arm saw.")
    if gaps:
        lines.append("\nPaimon vs Iceberg rows differ on: %s. With the writers stopped this is the binlog point each CDC job reached (pipeline state), not a reader result; the arms read different data there." % ", ".join(gaps))
    lines.append("")
    # First rows (compact view)
    lines.append("<details><summary>First row per arm</summary>\n")
    lines.append("| query | " + " | ".join(HEAD) + " |")
    lines.append("|---" * (1 + len(HEAD)) + "|")
    for q in queries:
        lines.append("| %s | %s |" % (q, " | ".join("`%s`" % (rows.get((ea[0], ea[1], q)) or ["-"])[0][:60].replace("|", "\\|") for ea in ARMS)))
    lines.append("\n</details>\n")

    # Tables, pins, events
    tps = all_files(d, "tables-%s.tsv")
    if tps:
        lines.append("### Pinned table state (%s)\n" % ", ".join(os.path.basename(x) for x in tps))
        t = []
        for tp in tps:
            ls = [l.rstrip("\n").split("\t") for l in open(tp) if l.strip()]
            t += ls if not t else ls[1:]
        lines.append("| " + " | ".join(t[0]) + " |")
        lines.append("|---" * len(t[0]) + "|")
        for r in t[1:]:
            lines.append("| " + " | ".join(x.replace("|", "\\|") for x in r + [""] * (len(t[0]) - len(r))) + " |")
        lines.append("")
    e = []
    for ev in all_files(d, "duckdb_warm-%s-events.csv"):
        e += [l.strip() for l in open(ev) if l.strip()][1:]
    if True:
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
                    if eq and re.fullmatch(r"[a-z_0-9]+", k) and (k.startswith(("iceberg_", "cdc_", "sr_", "duckdb_", "paimon_db"))):
                        meta.setdefault(k, v)
    return meta


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--work", required=True)
    ap.add_argument("--session", required=True)
    ap.add_argument("--hot")
    ap.add_argument("--snapshot")
    ap.add_argument("--must-match", default="S1_order_items_count S7_dedup_keys")
    ap.add_argument("--iceberg-format-version", help="2|3; default: the runs' .pins files, else 3")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    must = set(a.must_match.split())
    lines, verdicts = [], []
    body = []
    dirs = [os.path.join(a.work, x) for x in ("hot", "snapshot") if os.path.isdir(os.path.join(a.work, x))]
    meta = run_meta(dirs)
    fv = int(meta.get("iceberg_format_version") or a.iceberg_format_version or 3)
    if a.iceberg_format_version and meta.get("iceberg_format_version") and int(a.iceberg_format_version) != fv:
        print("note: the runs' pins say iceberg_format_version=%d, not %s; using the pins" % (fv, a.iceberg_format_version))
    CTX.update(fv=fv, skipped={x for x in (meta.get("sr_skipped_arms") or "").split(",") if x},
               jni=meta.get("sr_paimon_force_jni_reader") == "true")
    if fv >= 3:
        PAIRS.append(("Engine on Paimon's data: DuckDB on the v3 export vs SR native Paimon (JNI reader)",
                      ("duckdb_warm", "paimon"), ("starrocks", "paimon")))
    for suite, st in (("hot", a.hot), ("snapshot", a.snapshot)):
        d = os.path.join(a.work, suite)
        if os.path.isdir(d):
            suite_section(suite, d, st, must, body, verdicts)
    lines.append("# CDC query benchmark: %s\n" % a.session)
    lines.append("StarRocks (3 arms) and warm DuckDB (2 arms) over the four CDC tables "
                 "(cdc-pipeline/querybench/). DuckDB `paimon` = iceberg_scan over Paimon's Iceberg export, "
                 "paired with SR `pmn_as_iceberg`.\n")
    lines.append("Table set: Iceberg format version **%d**%s; databases %s / %s; StarRocks %s (chart %s); DuckDB %s.\n" % (
        fv, " (CDC_DB_SUFFIX `%s`)" % meta["cdc_db_suffix"] if meta.get("cdc_db_suffix") else "",
        meta.get("paimon_db", "?"), meta.get("iceberg_db", "?"), meta.get("sr_image_tag") or "?",
        meta.get("sr_chart_version") or "?", meta.get("duckdb_image") or "?"))
    if fv >= 3:
        lines.append("**Iceberg v3 caveats.** StarRocks arms skipped: %s (StarRocks 4.1.x refuses Iceberg v3 deletion "
                     "vectors; SR_V3_ARMS=1 runs them only to record the error). DuckDB arms with "
                     "`iceberg_unsafe_skip_puffin_verification = true`: %s (Paimon's DV index files are bare blobs, "
                     "not Puffin). %s Validity compares SR `paimon` with DuckDB `paimon`.\n" % (
                         ", ".join("`%s`" % x for x in sorted(CTX["skipped"])) or "none",
                         ", ".join("`%s`" % x for x in (meta.get("duckdb_skip_puffin_verification_arms") or "").split(",") if x) or "none",
                         "SR `paimon` runs on the **JNI reader** (`paimon_force_jni_reader = true`: the native reader "
                         "cannot decode Paimon's 64-bit DVs), which is slower: its times are not like-for-like with "
                         "a v2 run's native-reader SR `paimon`." if CTX["jni"] else
                         "SR `paimon` ran on the native reader (paimon_force_jni_reader not set)."))
    lines.append("Verdicts: " + ("; ".join(verdicts) if verdicts else "no suites found") + "\n")
    lines += body
    with open(a.out, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(a.out)


if __name__ == "__main__":
    main()
