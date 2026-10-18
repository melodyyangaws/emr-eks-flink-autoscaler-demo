#!/usr/bin/env python3
"""StarRocks vs DuckDB, one markdown table per suite, from the two engines' CSVs
(both carry lib.sh's QB_CSV_HEADER).

  ./querybench/compare.py --sr bench-results/starrocks-<STAMP>.csv \\
                          --duck bench-results/duckdb_warm-<STAMP>.csv [--suite snapshot]

Arms. DuckDB has no Paimon reader, so its `paimon` arm is iceberg_scan over
Paimon's Iceberg export: the same files and the same reader kind as StarRocks'
`pmn_as_iceberg`, which is what it is ratioed against. StarRocks' native
`paimon` arm (Paimon reader) has no DuckDB counterpart and is shown alone.
Ratios are a / b on median seconds; > 1 means a is slower.
"""
import argparse
import csv
import sys
from collections import OrderedDict

COLS = [("sr", "paimon"), ("sr", "pmn_as_iceberg"), ("sr", "iceberg"),
        ("duck", "paimon"), ("duck", "iceberg")]
HEAD = ["SR paimon", "SR pmn_as_iceberg", "SR iceberg", "DuckDB paimon (export)", "DuckDB iceberg"]
RATIOS = [  # (label, numerator, denominator)
    ("DuckDB/SR, Paimon export", ("duck", "paimon"), ("sr", "pmn_as_iceberg")),
    ("DuckDB/SR, Iceberg", ("duck", "iceberg"), ("sr", "iceberg")),
    ("SR iceberg/pmn", ("sr", "iceberg"), ("sr", "pmn_as_iceberg")),
    ("DuckDB iceberg/paimon", ("duck", "iceberg"), ("duck", "paimon")),
]


def norm_status(s):
    s = (s or "").strip()
    return {"ok": "OK", "ERR": "ERROR"}.get(s, s.upper() if s.lower() == "ok" else s)


def load(path, side):
    """{(suite, query, arm): row}, plus the engine labels seen."""
    rows, engines = {}, set()
    with open(path) as f:
        lines = [l for l in f if l.strip() and not l.startswith("#")]
    for r in csv.DictReader(lines):
        eng = r.get("engine", "")
        if side == "sr" and eng != "starrocks":
            continue
        if side == "duck" and not eng.startswith("duckdb"):
            continue
        engines.add(eng)
        r["status"] = norm_status(r.get("status"))
        rows[(r["suite"], r["query"], r["arm"])] = r
    return rows, engines


def secs(r):
    if not r or r["status"] != "OK" or not (r.get("median_ms") or "").strip():
        return None
    return int(float(r["median_ms"])) / 1000.0


def cell(r):
    if r is None:
        return "-"
    v = secs(r)
    return "%.1f" % v if v is not None else r["status"] or "?"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--sr", required=True, help="StarRocks CSV (starrocks.sh)")
    ap.add_argument("--duck", required=True, help="DuckDB CSV (duckdb-warm.sh; older cold duckdb.sh CSVs work too)")
    ap.add_argument("--suite", choices=["hot", "snapshot"], help="one suite only (default: every suite found)")
    a = ap.parse_args()
    data = {}
    sr, sr_eng = load(a.sr, "sr")
    duck, duck_eng = load(a.duck, "duck")
    for k, v in sr.items():
        data[("sr",) + k] = v
    for k, v in duck.items():
        data[("duck",) + k] = v
    suites = OrderedDict()
    for side, suite, query, arm in data:
        if a.suite and suite != a.suite:
            continue
        suites.setdefault(suite, OrderedDict())[query] = True
    if not suites:
        sys.exit("no rows%s in %s / %s" % (" for suite " + a.suite if a.suite else "", a.sr, a.duck))

    print("StarRocks: `%s` (engine %s); DuckDB: `%s` (engine %s)\n"
          % (a.sr, ", ".join(sorted(sr_eng)) or "none", a.duck, ", ".join(sorted(duck_eng)) or "none"))
    bad = []
    for suite, queries in suites.items():
        print("### %s suite, median seconds\n" % suite)
        print("| query | " + " | ".join(HEAD + [r[0] for r in RATIOS]) + " |")
        print("|---" * (1 + len(HEAD) + len(RATIOS)) + "|")
        for q in sorted(queries):
            get = lambda c: data.get((c[0], suite, q, c[1]))
            cells = [cell(get(c)) for c in COLS]
            for label, n, d in RATIOS:
                x, y = secs(get(n)), secs(get(d))
                cells.append("%.2f" % (x / y) if x is not None and y else "-")
            for c, h in zip(COLS, HEAD):
                r = get(c)
                if r is not None and r["status"] != "OK":
                    bad.append("%s %s, %s: %s" % (suite, q, h, r["status"]))
            print("| %s | %s |" % (q, " | ".join(cells)))
        print()
    print("Arms: DuckDB `paimon` = iceberg_scan over Paimon's Iceberg export, so it pairs with "
          "StarRocks `pmn_as_iceberg` (same files, Iceberg reader). StarRocks `paimon` (native "
          "Paimon reader) has no DuckDB counterpart. Ratios are a/b of medians; > 1 = a slower; "
          "`-` = a side is missing or not OK.\n")
    print("Not OK:" if bad else "Not OK: none")
    for b in bad:
        print("- " + b)


if __name__ == "__main__":
    main()
