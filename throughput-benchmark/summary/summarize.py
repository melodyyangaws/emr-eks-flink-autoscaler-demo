#!/usr/bin/env python3
"""Write a session's test outcome as Markdown: Flink write throughput from the
tables' commit history (+ the REST samples), and the hot / snapshot query
medians. summary/run.sh gathers the inputs and calls this; the layout follows
the hand-written bench-results/BENCH-SUMMARY-*.md files, minus the
interpretation, which is left as a section to fill in.

Throughput method (as in every BENCH-SUMMARY): rows each commit added / gap
since the previous commit; "steady" = a 50-70 s gap (60 s checkpoints); a gap
over 70 s is a restart or an idle period.
"""
import argparse
import csv
import os
import statistics as st
from datetime import datetime, timezone

STEADY = (50, 70)


def iso(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def med(xs, fmt="{:,.0f}"):
    return (fmt.format(st.median(xs)) + f" (n={len(xs)})") if xs else "-"


def commits(path, out_csv):
    """'<ts_ms> <rows>' lines -> per-commit rows, written to out_csv.

    Commits that added no rows (Paimon COMPACT snapshots, Iceberg's idle empty
    commits) are dropped BEFORE the gaps are taken: kept, a COMPACT snapshot
    between two APPENDs shortened the next APPEND's gap, which raised Paimon's
    rate. `steady` keeps each steady commit's time, so main() can clip both
    formats to the span they share."""
    rows = sorted((int(a) / 1000, int(b)) for a, b in (l.split() for l in open(path) if l.strip()))
    rows = [(t, n) for t, n in rows if n > 0]
    steady, gaps, prev, cum = [], [], None, 0
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["commit_ts", "rows_added", "gap_s", "rows_per_s", "cumulative_rows"])
        for t, n in rows:
            cum += n
            g = t - prev if prev else None
            rps = n / g if g else None
            w.writerow([iso(t), n, f"{g:.1f}" if g else "", f"{rps:.0f}" if rps else "", cum])
            if g and STEADY[0] <= g <= STEADY[1]:
                steady.append((t, rps))
            if g and g > STEADY[1]:
                gaps.append((iso(prev), round(g), n))
            prev = t
    return {"n": len(rows), "t0": rows[0][0] if rows else None, "t1": rows[-1][0] if rows else None,
            "first": iso(rows[0][0]) if rows else "-", "last": iso(rows[-1][0]) if rows else "-",
            "steady": steady, "gaps": gaps, "total": cum}


def rest(path):
    """The REST collector's CSV (monitoring/collect-metrics.sh)."""
    if not path or not os.path.exists(path):
        return None
    r = [x for x in csv.DictReader(open(path)) if x.get("job_state") == "RUNNING" and x.get("source_rec_per_s")]
    num = lambda k: [float(x[k]) for x in r if x.get(k) not in (None, "")]
    src = [v for v in num("source_rec_per_s") if v > 0]
    return {"rows": med(src), "busy": med(num("busy_ms"), "{:.1f} ms/s"), "bp": max(num("source_backpressure_ms") or [0]),
            "ckpt": med(num("last_ckpt_duration_ms"), "{:,.0f} ms"), "state": med([v / 1e6 for v in num("last_ckpt_size_bytes")], "{:.2f} MB"),
            "failed": r[-1].get("ckpt_failed", "-") if r else "-"}


def queries(results, stamp):
    """Medians per engine/arm/query from <engine>-<stamp>.csv, plus any non-OK pass."""
    out = {}
    for eng in ("starrocks", "duckdb_warm"):
        f = os.path.join(results, f"{eng}-{stamp}.csv")
        if not os.path.exists(f):
            continue
        for r in csv.DictReader(l for l in open(f) if not l.startswith("#")):
            out[(eng, r["arm"], r["query"])] = (r["status"], r["median_ms"])
    bad = []
    for p in range(0, 10):
        for eng in ("starrocks", "duckdb_warm"):
            f = os.path.join(results, f"{eng}-{stamp}-p{p}.csv")
            if os.path.exists(f):
                bad += [f"{eng} `{r['arm']}` {r['query']} pass {p}: {r['status']}"
                        for r in csv.DictReader(l for l in open(f) if not l.startswith("#")) if r["status"] != "OK"]
    pins = os.path.join(results, f"hot-interleave-{stamp}.pins")
    return out, bad, (open(pins).read().strip() if os.path.exists(pins) else "")


# Snapshot queries whose first column is a row count every arm must agree on:
# S1 counts the partition, S4 its time slice. The hot suite checks its window
# count before it runs (hot-interleave.sh); the snapshot suite is checked here.
COUNT_QUERIES = ("S1_partition_count", "S4_wide_json")


def same_rows(results, stamp):
    """[] if every arm read the same S1/S4 counts, else a line per query that differs."""
    seen = {}
    for eng in ("starrocks", "duckdb_warm"):
        f = os.path.join(results, f"{eng}-{stamp}-firstrows.csv")
        if not os.path.exists(f):
            continue
        for line in open(f):
            parts = line.rstrip("\n").split(",", 3)
            if len(parts) == 4 and parts[2] in COUNT_QUERIES:
                seen.setdefault(parts[2], {})[f"{eng} `{parts[0]}`"] = parts[3].split("|", 1)[0]
    if not seen:
        return ["no first-row files (`*-firstrows.csv`): the row counts could not be checked"]
    return [f"{q}: " + ", ".join(f"{k}={v}" for k, v in sorted(arms.items()))
            for q, arms in sorted(seen.items()) if len(set(arms.values())) > 1]


def query_section(title, stamp, results):
    out, bad, pins = queries(results, stamp)
    if not out:
        return [f"## {title}", "", f"No results for `{stamp}` in `{os.path.basename(results)}/`.", ""]
    arms = [("starrocks", a) for a in ("paimon", "pmn_as_iceberg", "iceberg")] + [("duckdb_warm", a) for a in ("paimon", "iceberg")]
    arms = [x for x in arms if any(k[:2] == x for k in out)]
    qs = sorted({k[2] for k in out})
    label = lambda e, a: ("SR" if e == "starrocks" else "DuckDB") + f" `{a}`"
    cell = lambda v: (f"{int(v[1]) / 1000:.1f}" if v[0] == "OK" and v[1] else f"**{v[0]}**") if v else "-"
    lines = [f"## {title}", "", f"Run `{stamp}`. Median seconds over the timed passes.", ""]
    if pins:
        lines += ["Pins and window (`.pins`):", "", "```", pins, "```", ""]
    lines += ["| Query | " + " | ".join(label(*x) for x in arms) + " |", "|---" * (len(arms) + 1) + "|"]
    lines += [f"| {q} | " + " | ".join(cell(out.get((e, a, q))) for e, a in arms) + " |" for q in qs]
    lines += [""]
    if bad:
        lines += ["**Passes not OK** (a query not OK on any pass has no median):", ""] + [f"- {b}" for b in bad] + [""]
    if "Snapshot" in title:
        diff = same_rows(results, stamp)
        if diff and diff[0].startswith("no first-row"):
            lines += [f"Row counts across arms: not checked ({diff[0]}).", ""]
        elif diff:
            lines += ["**INVALID: the arms did not read the same rows.** Their S1/S4 counts differ, "
                      "so their times are not comparable:", ""] + [f"- {d}" for d in diff] + [""]
        else:
            lines += ["Row counts across arms: S1 and S4 match on every arm of both engines.", ""]
    return lines


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--results", required=True, help="bench-results/")
    ap.add_argument("--session", required=True, help="label, e.g. 20261020-test4")
    ap.add_argument("--suffix", default="", help="TEST_SUFFIX")
    ap.add_argument("--commits", nargs=2, metavar=("PAIMON", "ICEBERG"), help="'<ts_ms> <rows>' files")
    ap.add_argument("--rest", nargs=2, metavar=("PAIMON", "ICEBERG"), default=(None, None))
    ap.add_argument("--hot", help="hot run STAMP")
    ap.add_argument("--snapshot", help="snapshot run STAMP")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    suffix = a.suffix or '""'
    L = [f"# Benchmark summary: session `{a.session}`", "",
         f"- **Table set:** `TEST_SUFFIX={suffix}`",
         f"- **Written by:** `summary/run.sh`, {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%MZ')}. "
         "Interpretation is left for §4.", ""]

    if a.commits:
        c = {f: commits(p, os.path.join(a.results, f"cid-{f}-commits-{a.session}.csv")) for f, p in zip(("paimon", "iceberg"), a.commits)}
        r = {f: rest(p) for f, p in zip(("paimon", "iceberg"), a.rest)}
        # Compare like for like: only the span BOTH tables have commits for.
        # Paimon keeps only snapshot.time-retained (3 h) of history, Iceberg
        # every snapshot, so an older session's spans differ otherwise.
        ts = [c[f][k] for f in ("paimon", "iceberg") for k in ("t0", "t1")]
        lo, hi = (max(ts[0], ts[2]), min(ts[1], ts[3])) if None not in ts else (None, None)
        sp = {f: [r for t, r in c[f]["steady"] if lo is not None and lo <= t <= hi] for f in ("paimon", "iceberg")}
        mp, mi = (st.median(sp[f]) if sp[f] else None for f in ("paimon", "iceberg"))
        rel = f"{(mi / mp - 1) * 100:+.1f}%" if mp and mi else "-"
        span = f"{iso(lo)} .. {iso(hi)}" if lo is not None and lo < hi else "none (the two histories do not overlap)"
        L += ["## 1. Flink write throughput (from the commit history)", "",
              f"Steady state = commits {STEADY[0]}-{STEADY[1]} s after the previous one, commits that added "
              f"no rows left out. Both medians cover only the span both tables have commits for: **{span}**. "
              f"Every commit: `cid-{{paimon,iceberg}}-commits-{a.session}.csv`.", "",
              "| | Paimon | Iceberg | Iceberg vs Paimon |", "|---|---|---|---|",
              f"| **per-commit rows/s, median (steady, shared span)** | **{med(sp['paimon'])}** | **{med(sp['iceberg'])}** | **{rel}** |",
              f"| commits (first .. last) | {c['paimon']['n']} ({c['paimon']['first']} .. {c['paimon']['last']}) | {c['iceberg']['n']} ({c['iceberg']['first']} .. {c['iceberg']['last']}) | |",
              f"| commit gaps > {STEADY[1]} s | {len(c['paimon']['gaps'])} | {len(c['iceberg']['gaps'])} | |",
              f"| rows at the last commit | {c['paimon']['total']:,} | {c['iceberg']['total']:,} | |"]
        if r["paimon"] and r["iceberg"]:
            L += [f"| REST rows/s, median | {r['paimon']['rows']} | {r['iceberg']['rows']} | |",
                  f"| writer busy, median | {r['paimon']['busy']} | {r['iceberg']['busy']} | |",
                  f"| source backpressure, max | {r['paimon']['bp']:.0f} | {r['iceberg']['bp']:.0f} | |",
                  f"| checkpoint duration / state, median | {r['paimon']['ckpt']} / {r['paimon']['state']} | {r['iceberg']['ckpt']} / {r['iceberg']['state']} | |",
                  f"| failed checkpoints (collector's count) | {r['paimon']['failed']} | {r['iceberg']['failed']} | |"]
        L += [""]
        for f in ("paimon", "iceberg"):
            if c[f]["gaps"]:
                L += [f"{f.capitalize()} gaps > {STEADY[1]} s (from, seconds, rows): " + ", ".join(f"{t} {g} s {n:,}" for t, g, n in c[f]["gaps"]), ""]
    if a.hot:
        L += query_section("2. Hot query suite (writers live)", a.hot, a.results)
    if a.snapshot:
        L += query_section("3. Snapshot query suite (writers stopped)", a.snapshot, a.results)
    L += ["## 4. Findings", "", "_To write: what the numbers show, what went wrong, caveats._", ""]
    open(a.out, "w").write("\n".join(L))
    print(a.out)


if __name__ == "__main__":
    main()
