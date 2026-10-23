#!/usr/bin/env python3
"""Write a session's benchmark findings as one Markdown file: Flink write
throughput from the tables' commit history (+ the REST samples), the hot and
snapshot query medians, the like-for-like comparisons, and findings derived
from those numbers. summary/run.sh gathers the inputs and calls this.

Throughput method: rows each commit added / gap since the previous commit;
"steady" = a 50-70 s gap (60 s checkpoints); a gap over 70 s is a restart or
an idle period. Both formats are compared over the span they both committed.

Comparisons: for each pair of arms, the geometric mean over the queries both
ran OK of (time of A / time of B). One number per pair and suite, so no single
long query dominates.
"""
import argparse
import csv
import glob
import json
import math
import os
import statistics as st
from datetime import datetime, timezone

STEADY = (50, 70)
EARLY_STOP_S = 180          # a format whose commits end this much earlier stopped early
NOISE_PCT = 3.0             # throughput differences under this are noise at one run each
ARMS = [("starrocks", "paimon"), ("starrocks", "pmn_as_iceberg"), ("starrocks", "iceberg"),
        ("duckdb_warm", "paimon"), ("duckdb_warm", "iceberg")]
# (table label, finding lead, A, B): one thing differs between A and B.
PAIRS = [
    ("Reader (SR, same Paimon files): Iceberg reader vs Paimon reader",
     "**Reader** — StarRocks' Iceberg reader vs its Paimon reader, same Paimon files", ("starrocks", "pmn_as_iceberg"), ("starrocks", "paimon")),
    ("Writer (SR, Iceberg reader): Iceberg table vs Paimon's export",
     "**Writer, StarRocks** — the Iceberg table vs Paimon's export, same Iceberg reader", ("starrocks", "iceberg"), ("starrocks", "pmn_as_iceberg")),
    ("Writer (DuckDB): Iceberg table vs Paimon's export",
     "**Writer, DuckDB** — the Iceberg table vs Paimon's export", ("duckdb_warm", "iceberg"), ("duckdb_warm", "paimon")),
    ("Engine on Paimon's export: DuckDB vs StarRocks",
     "**Engine, Paimon's export** — DuckDB vs StarRocks", ("duckdb_warm", "paimon"), ("starrocks", "pmn_as_iceberg")),
    ("Engine on the Iceberg table: DuckDB vs StarRocks",
     "**Engine, Iceberg table** — DuckDB vs StarRocks", ("duckdb_warm", "iceberg"), ("starrocks", "iceberg")),
]
COUNT_QUERIES = ("S1_partition_count", "S4_wide_json")   # first column = rows every arm must agree on


def iso(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hm(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S")


def med(xs, fmt="{:,.0f}"):
    return (fmt.format(st.median(xs)) + f" (n={len(xs)})") if xs else "-"


def label(e, a):
    return ("SR" if e == "starrocks" else "DuckDB") + f" `{a}`"


def times(r):
    """'1.8× slower' / '1.8× faster' / 'about the same' for a time ratio A/B."""
    if r is None:
        return "-"
    if 0.95 <= r <= 1.05:
        return "about the same"
    return f"{r:.2f}× slower" if r > 1 else f"{1 / r:.2f}× faster"


# ── Throughput ───────────────────────────────────────────────────────────────
def commits(path, out_csv):
    """'<ts_ms> <rows>' lines -> per-commit rows (also written to out_csv).
    Commits that added no rows (Paimon COMPACT, Iceberg idle commits) are
    dropped BEFORE the gaps are taken: kept, a COMPACT between two APPENDs
    shortened the next APPEND's gap and raised Paimon's rate."""
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
    busy = num("busy_ms")
    return {"rows": med(src), "busy": med(busy, "{:.1f} ms/s"), "busy_v": st.median(busy) if busy else None,
            "bp": max(num("source_backpressure_ms") or [0]),
            "ckpt": med(num("last_ckpt_duration_ms"), "{:,.0f} ms"),
            "state": med([v / 1e6 for v in num("last_ckpt_size_bytes")], "{:.2f} MB"),
            "failed": r[-1].get("ckpt_failed", "-") if r else "-",
            "until": r[-1]["ts"] if r else "-"}


def throughput(a, F):
    c = {f: commits(p, os.path.join(a.work, f"cid-{f}-commits-{a.session}.csv")) for f, p in zip(("paimon", "iceberg"), a.commits)}
    r = {f: rest(p) for f, p in zip(("paimon", "iceberg"), a.rest)}
    ts = [c[f][k] for f in ("paimon", "iceberg") for k in ("t0", "t1")]
    lo, hi = (max(ts[0], ts[2]), min(ts[1], ts[3])) if None not in ts else (None, None)
    sp = {f: [x for t, x in c[f]["steady"] if lo is not None and lo <= t <= hi] for f in ("paimon", "iceberg")}
    mp, mi = (st.median(sp[f]) if sp[f] else None for f in ("paimon", "iceberg"))
    rel = (mi / mp - 1) * 100 if mp and mi else None
    span = f"{iso(lo)} .. {iso(hi)}" if lo is not None and lo < hi else "none (the two histories do not overlap)"
    L = ["## 1. Write throughput", "",
         f"From each table's commit history. Steady = commits {STEADY[0]}–{STEADY[1]} s after the previous one; "
         f"commits that added no rows are left out. Both medians cover only the span both tables committed: **{span}**.", "",
         "| | Paimon | Iceberg | Iceberg vs Paimon |", "|---|---|---|---|",
         f"| **rows/s, median (steady, shared span)** | **{med(sp['paimon'])}** | **{med(sp['iceberg'])}** | **{f'{rel:+.1f}%' if rel is not None else '-'}** |",
         f"| commits (first .. last) | {c['paimon']['n']} ({c['paimon']['first']} .. {c['paimon']['last']}) | {c['iceberg']['n']} ({c['iceberg']['first']} .. {c['iceberg']['last']}) | |",
         f"| commit gaps > {STEADY[1]} s | {len(c['paimon']['gaps'])} | {len(c['iceberg']['gaps'])} | |",
         f"| rows at the last commit | {c['paimon']['total']:,} | {c['iceberg']['total']:,} | |"]
    if r["paimon"] and r["iceberg"]:
        L += [f"| REST rows/s, median | {r['paimon']['rows']} | {r['iceberg']['rows']} | |",
              f"| writer busy, median | {r['paimon']['busy']} | {r['iceberg']['busy']} | |",
              f"| source backpressure, max | {r['paimon']['bp']:.0f} | {r['iceberg']['bp']:.0f} | |",
              f"| checkpoint duration / state, median | {r['paimon']['ckpt']} / {r['paimon']['state']} | {r['iceberg']['ckpt']} / {r['iceberg']['state']} | |",
              f"| failed checkpoints (collector's count) | {r['paimon']['failed']} | {r['iceberg']['failed']} | |",
              f"| REST samples until | {r['paimon']['until']} | {r['iceberg']['until']} | |"]
    L += [""]
    for f in ("paimon", "iceberg"):
        if c[f]["gaps"]:
            L += [f"{f.capitalize()} gaps > {STEADY[1]} s (from, seconds, rows): " + ", ".join(f"{t} {g} s {n:,}" for t, g, n in c[f]["gaps"]), ""]

    # Findings
    if rel is not None:
        verdict = "equal within run-to-run noise" if abs(rel) < NOISE_PCT else ("Iceberg faster" if rel > 0 else "Paimon faster")
        F.append(f"**Write throughput:** Paimon {mp:,.0f} vs Iceberg {mi:,.0f} rows/s ({rel:+.1f}%): {verdict}.")
    if r["paimon"] and r["iceberg"]:
        if r["paimon"]["bp"] == 0 and r["iceberg"]["bp"] == 0:
            F.append("**Both writers kept up** with the generator (no source backpressure), so the rates are the "
                     "offered rate, not either format's ceiling.")
        else:
            bp = [f.capitalize() for f in ("paimon", "iceberg") if r[f]["bp"] > 0]
            F.append(f"**Backpressure on {' and '.join(bp)}:** that writer could not keep up; its rate is its ceiling.")
        bpv, biv = r["paimon"]["busy_v"], r["iceberg"]["busy_v"]
        if bpv and biv and abs(bpv - biv) > 20:
            near = "Paimon" if bpv > biv else "Iceberg"
            F.append(f"**{near} is closer to its limit:** writer busy {bpv:.0f} vs {biv:.0f} ms/s (Paimon vs Iceberg).")
    if None not in ts:
        for f, o in (("paimon", "iceberg"), ("iceberg", "paimon")):
            d = c[o]["t1"] - c[f]["t1"]
            if d > EARLY_STOP_S:
                F.append(f"**Incident: {f.capitalize()} stopped committing {d / 60:.0f} min before {o.capitalize()}** "
                         f"(last commit {hm(c[f]['t1'])} vs {hm(c[o]['t1'])} UTC). Check its JobManager log for a "
                         "checkpoint that never completed. The medians use the shared span, so they exclude it; "
                         f"it is why the row totals differ ({c['paimon']['total']:,} vs {c['iceberg']['total']:,}).")
    return L


# ── Queries ──────────────────────────────────────────────────────────────────
def load(results, stamp):
    """{(engine, arm, query): row} from <engine>-<stamp>.csv, the non-OK passes, the pins."""
    out = {}
    for eng in ("starrocks", "duckdb_warm"):
        f = os.path.join(results, f"{eng}-{stamp}.csv")
        if os.path.exists(f):
            for r in csv.DictReader(l for l in open(f) if not l.startswith("#")):
                out[(eng, r["arm"], r["query"])] = r
    bad = []
    for p in range(10):
        for eng in ("starrocks", "duckdb_warm"):
            f = os.path.join(results, f"{eng}-{stamp}-p{p}.csv")
            if os.path.exists(f):
                bad += [f"{eng} `{r['arm']}` {r['query']} pass {p}: {r['status']}"
                        for r in csv.DictReader(l for l in open(f) if not l.startswith("#")) if r["status"] != "OK"]
    pins = os.path.join(results, f"hot-interleave-{stamp}.pins")
    notes = []
    sr = os.path.join(results, f"starrocks-{stamp}.csv")
    if os.path.exists(sr):
        notes = [l[2:].strip() for l in open(sr) if l.startswith("# suite=")]
    return out, bad, (open(pins).read().strip() if os.path.exists(pins) else ""), notes


def secs(row):
    return int(row["median_ms"]) / 1000 if row and row["status"] == "OK" and row["median_ms"] else None


def same_rows(results, stamp):
    """None if not checkable, [] if every arm read the same S1/S4 counts, else the differences."""
    seen = {}
    for eng in ("starrocks", "duckdb_warm"):
        f = os.path.join(results, f"{eng}-{stamp}-firstrows.csv")
        if os.path.exists(f):
            for line in open(f):
                parts = line.rstrip("\n").split(",", 3)
                if len(parts) == 4 and parts[2] in COUNT_QUERIES:
                    seen.setdefault(parts[2], {})[label(eng, parts[0])] = parts[3].split("|", 1)[0]
    if not seen:
        return None, {}
    diff = [f"{q}: " + ", ".join(f"{k}={v}" for k, v in sorted(arms.items()))
            for q, arms in sorted(seen.items()) if len(set(arms.values())) > 1]
    return diff, {q: next(iter(arms.values())) for q, arms in seen.items()}


def ratio(out, A, B):
    """Geometric mean over shared OK queries of time(A) / time(B), and how many."""
    rs = [secs(out.get((*A, q))) / secs(out.get((*B, q))) for q in {k[2] for k in out}
          if secs(out.get((*A, q))) and secs(out.get((*B, q)))]
    return (math.exp(sum(map(math.log, rs)) / len(rs)), len(rs)) if rs else (None, 0)


def query_section(num, title, suite, stamp, results, F):
    out, bad, pins, notes = load(results, stamp)
    if not out:
        return [f"## {num}. {title}", "", f"No results for `{stamp}`.", ""], {}
    arms = [x for x in ARMS if any(k[:2] == x for k in out)]
    qs = sorted({k[2] for k in out})
    cell = lambda r: f"{secs(r):,.1f}" if secs(r) is not None else (f"**{r['status']}**" if r else "-")
    L = [f"## {num}. {title}", "", f"Run `{stamp}`. Median seconds over the timed passes; fastest arm in bold.", ""]
    if pins:
        L += ["```", pins, "```", ""]
    for n in notes:
        L += [f"- `{n}`"]
    L += [""] if notes else []
    L += ["| Query | " + " | ".join(label(*x) for x in arms) + " |", "|---" * (len(arms) + 1) + "|"]
    wins = {}
    for q in qs:
        vals = {x: secs(out.get((*x, q))) for x in arms}
        ok = {x: v for x, v in vals.items() if v is not None}
        best = min(ok, key=ok.get) if ok else None
        if best:
            wins[best] = wins.get(best, 0) + 1
        L += [f"| {q} | " + " | ".join((f"**{cell(out.get((*x, q)))}**" if x == best else cell(out.get((*x, q)))) for x in arms) + " |"]
    L += [""]
    sr = [x for x in arms if x[0] == "starrocks"]
    if any(out.get((*x, q), {}).get("scan_bytes") for x in sr for q in qs):
        L += ["StarRocks scanned GB (rows):", "", "| Query | " + " | ".join(label(*x) for x in sr) + " |", "|---" * (len(sr) + 1) + "|"]
        gb = lambda r: f"{int(r['scan_bytes']) / 1e9:,.1f} ({int(r['scan_rows']) / 1e6:,.0f}M)" if r and r.get("scan_bytes") else "-"
        L += [f"| {q} | " + " | ".join(gb(out.get((*x, q))) for x in sr) + " |" for q in qs] + [""]
    if bad:
        L += ["**Passes not OK** (a query not OK on every pass has no median):", ""] + [f"- {b}" for b in bad] + [""]

    total = len(qs) * len(arms)
    ok_n = sum(1 for x in arms for q in qs if secs(out.get((*x, q))) is not None)
    F.append(f"**{title.split(' (')[0]}:** {ok_n} of {total} cells OK"
             + ("." if not bad else f", {len(bad)} pass(es) not OK (listed in §{num}).")
             + (" Fastest: " + ", ".join(f"{label(*x)} on {n} of {len(qs)} queries" for x, n in sorted(wins.items(), key=lambda kv: -kv[1])) + "." if wins else ""))
    if suite == "snapshot":
        diff, counts = same_rows(results, stamp)
        if diff is None:
            L += ["Row counts across arms: not checked (no `*-firstrows.csv`).", ""]
        elif diff:
            L += ["**INVALID: the arms did not read the same rows.** Their S1/S4 counts differ, so their times are not comparable:", ""] + [f"- {d}" for d in diff] + [""]
            F.append("**Snapshot INVALID:** the arms' S1/S4 row counts differ (§3).")
        else:
            c = ", ".join(f"{q.split('_')[0]} = {int(v):,}" for q, v in sorted(counts.items()) if v.isdigit())
            L += [f"Row counts match on every arm of both engines ({c}).", ""]
            F.append(f"**Snapshot rows match on every arm** ({c}).")
    rs = {}
    for name, _, A, B in PAIRS:
        if A in arms and B in arms:
            rs[name] = ratio(out, A, B)
    sb = [int(out[k]["scan_bytes"]) / int(out[(k[0], "pmn_as_iceberg", k[2])]["scan_bytes"])
          for k in out if k[:2] == ("starrocks", "iceberg") and out[k].get("scan_bytes")
          and out.get((k[0], "pmn_as_iceberg", k[2]), {}).get("scan_bytes")]
    if sb:
        g = math.exp(sum(map(math.log, sb)) / len(sb))
        if g > 1.2:
            F.append(f"**{title.split(' (')[0]}: StarRocks scans {g:.1f}× more bytes on the Iceberg table** than on "
                     "Paimon's export (geometric mean over the queries).")
    return L, rs


def comparisons(hot, snap, F):
    names = [n for n, _, _, _ in PAIRS if n in hot or n in snap]
    if not names:
        return []
    fmt = lambda v: f"{v[0]:.2f} ({v[1]} queries)" if v and v[0] else "-"
    L = ["## 4. Like-for-like comparisons", "",
         "Geometric mean over the queries both arms ran OK of time(A) / time(B). Below 1 = A faster.", "",
         "| A vs B | Hot | Snapshot |", "|---|---|---|"]
    L += [f"| {n} | {fmt(hot.get(n))} | {fmt(snap.get(n))} |" for n in names] + [""]
    for n, lead, _, _ in PAIRS:
        if n not in names:
            continue
        parts = [f"{times(v[0])} on {suite}" for suite, v in (("hot", hot.get(n)), ("snapshot", snap.get(n))) if v and v[0]]
        F.append(f"{lead}: " + ", ".join(parts) + ".")
    return L


def compaction(d, F):
    """compaction/run.sh's per-pass JSONs (<STAMP>-<fmt>-pass<N>.json): the newest run."""
    runs = {}
    for f in sorted(glob.glob(os.path.join(d, "*-pass*.json"))):
        stamp, fmt = os.path.basename(f).split("-")[:2]
        runs.setdefault(stamp, {}).setdefault(fmt, []).append(json.load(open(f)))
    if not runs:
        return []
    stamp = max(runs)
    c = {fmt: sorted(p, key=lambda r: r.get("started", "")) for fmt, p in runs[stamp].items()}
    fmts = [f for f in ("paimon", "iceberg") if f in c]
    th = next((p[0].get("threshold_bytes") for p in c.values() if p[0].get("threshold_bytes")), 0)
    first = lambda f, k: c[f][0].get("before", {}).get(k, 0)
    last = lambda f, k: c[f][-1].get("after", {}).get(k, 0)
    arrow = lambda f, k, conv=lambda v: f"{v:,}": f"{conv(first(f, k))} → {conv(last(f, k))}"
    tb = lambda v: f"{v / 1e12:.2f}"
    ok = lambda f: all(r.get("status") == "OK" for r in c[f]) and not c[f][-1].get("remaining_groups")
    rows = [("passes", lambda f: f"{len(c[f])}"),
            ("status", lambda f: "OK" if ok(f) else "**INCOMPLETE**"),
            ("data files", lambda f: arrow(f, "files")),
            (f"files under {th >> 20} MiB", lambda f: arrow(f, "small_files")),
            ("stored TB", lambda f: arrow(f, "bytes", tb)),
            ("job time, all passes (s)", lambda f: f"{sum(r.get('duration_s', 0) for r in c[f]):,.0f}")]
    L = ["## Compaction before the snapshot test", "",
         f"Run `{stamp}` (`compaction/run.sh`): files under 70% of the 256 MiB target bin-packed, the same rule on both "
         f"formats, at parallelism {c[fmts[0]][0].get('parallelism', '?')}. The hot test ran before it, on the "
         "uncompacted tables.", "",
         "| | " + " | ".join(f.capitalize() for f in fmts) + " |", "|---|" + "---|" * len(fmts)]
    L += [f"| {name} | " + " | ".join(fn(f) for f in fmts) + " |" for name, fn in rows] + [""]
    F.append("**Snapshot queries ran on compacted tables:** " + ", ".join(
        f"{f.capitalize()} {first(f, 'files'):,} → {last(f, 'files'):,} files" + ("" if ok(f) else " (incomplete)")
        for f in fmts) + ".")
    return L


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--work", required=True, help="the inputs, synced from S3")
    ap.add_argument("--session", required=True)
    ap.add_argument("--suffix", default="")
    ap.add_argument("--setting", action="append", default=[], help="KEY=VALUE for the setup table")
    ap.add_argument("--commits", nargs=2, metavar=("PAIMON", "ICEBERG"))
    ap.add_argument("--rest", nargs=2, metavar=("PAIMON", "ICEBERG"), default=(None, None))
    ap.add_argument("--hot")
    ap.add_argument("--snapshot")
    ap.add_argument("--compaction", help="compaction/run.sh's result JSONs, if it ran")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    F, body, hot, snap = [], [], {}, {}
    if a.commits:
        body += throughput(a, F)
    if a.hot:
        L, hot = query_section(2, "Hot queries (writers live)", "hot", a.hot, a.work, F)
        body += L
    if a.compaction:
        body += compaction(a.compaction, F)
    if a.snapshot:
        L, snap = query_section(3, "Snapshot queries (writers stopped)", "snapshot", a.snapshot, a.work, F)
        body += L
    body += comparisons(hot, snap, F)

    setup = [("TEST_SUFFIX", a.suffix or '""')] + [tuple(s.split("=", 1)) for s in a.setting if "=" in s]
    head = [f"# Benchmark findings: session `{a.session}`", "",
            f"Generated by `summary/run.sh` on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%MZ')} from the session's "
            "results in S3. Re-running it rewrites this file.", "",
            "| Setting | Value |", "|---|---|"] + [f"| `{k}` | {v} |" for k, v in setup] + [""]
    head += ["## Key findings", ""] + [f"- {x}" for x in F] + [""]
    tail = ["## Caveats", "",
            "- One run per suite: differences of a few percent are within run-to-run noise.",
            "- Both writers ran side by side on the same nodepool and bucket, so each rate is under the same shared load.",
            *(["- Paimon's Iceberg export is on (`ICEBERG_COMPAT=true`): every Paimon commit also writes Iceberg metadata, "
               "work the Iceberg writer does not do."] if "hive-catalog" in " ".join(a.setting) else []),
            *(["- The snapshot test ran on compacted tables (section above), the hot test on uncompacted ones; "
               "snapshot numbers are not comparable with sessions that did not compact."] if a.compaction else []),
            "- Query times compare engines on equal CPU and memory (StarRocks 3 BEs × 9 cpu / 38 Gi vs DuckDB 27 cpu / 114 Gi; memory limit 90% of the pod and cache memory 20% of it, on both), "
            "with both caches emptied before each test.", ""]
    open(a.out, "w").write("\n".join(head + body + tail))
    print(a.out)


if __name__ == "__main__":
    main()
