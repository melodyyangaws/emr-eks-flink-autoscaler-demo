# Hot query benchmark: DuckDB vs StarRocks, Paimon vs Iceberg (`writeonly-spilloff`)

**Configuration markers:** the tables written by the
[writeonly-spilloff run](FLINK-THROUGHPUT-writeonly-spilloff-20261010.md)
(`JOB_PARALLELISM=320`, `PAIMON_WRITE_ONLY=true`,
`PAIMON_WRITE_BUFFER_SPILLABLE=false`), with **both writers RUNNING** and the
generator still writing (14:48:42Z → ~15:18:42Z). Suite `hot`, queries H1–H6
(`querybench/hot/`).

**Measured** 2026-10-10, run `20261010T150541Z`, both engines launched at
15:05:41Z.

**Window:** one 30 s window shared by every query and arm, dt=2026-10-10
`[15:02:11, 15:02:41)`. `HOT_TO` was pinned 3 minutes before launch, so that the
window would be committed in both tables (see "Rows in the window").

**Source data:**
- `bench-results/duckdb-20261010T150541Z-writeonly-spilloff.csv`
- `bench-results/starrocks-20261010T150541Z-writeonly-spilloff.csv`
- The per-arm DuckDB logs and both harness logs
  (`duckdb-{iceberg,paimon}-20261010T150541Z.log`,
  `qb-{duckdb,starrocks}-hot-wo-nospill-20261010T150541Z.log`) are in the macOS
  Trash, not in `bench-results/`.

**Harnesses as they were on that day:**

| | StarRocks (`starrocks.sh`) | DuckDB (`duckdb.sh`, **cold**) |
|---|---|---|
| version | 4.1.4 | 1.5.6 |
| compute | 3 BEs × 6 cpu / 24 Gi (two BEs then shared one m5.4xlarge) | one pod, 18 cpu / 72 Gi, m5.8xlarge |
| arms | `paimon` (Paimon reader), `pmn_as_iceberg` (Iceberg reader → Paimon's Iceberg export), `iceberg` | `paimon` (= Paimon's Iceberg export via `iceberg_scan`), `iceberg` |
| process | always up | a **fresh `duckdb` process per attempt** |
| data cache | **off** in the benchmark sessions | none |
| timing | server-side, FE audit log | wall clock around the whole process |
| passes | 1 warm-up + 3 timed, median | 1 warm-up + 3 timed, median |

The two engines ran **at the same time**, on separate nodes, reading the same S3
bucket.

## Headline

Median seconds, timed passes.

| Query | SR `paimon` | SR `pmn_as_iceberg` | SR `iceberg` | DuckDB `paimon` (export) | DuckDB `iceberg` |
|---|---|---|---|---|---|
| H1 freshness | 2.2 | 2.7 | 4.1 | 25.7 | 32.9 |
| H2 count | 2.2 | 3.7 | 3.9 | 21.7 | 32.2 |
| H3 needle uuid | 2.8 | 3.2 | 4.3 | 28.3 | 39.3 |
| H4 narrow agg | 2.1 | 2.7 | 5.5 | 27.4 | 36.2 |
| H5 wide JSON | 92.4 | 92.5 | 88.0 | 81.2 | 96.4 |
| H6 customer agg | 4.0 | 4.1 | 6.0 | 28.7 | 35.2 |

All 30 cells are OK. H3 matched no rows on every arm, as it is designed to (a
needle that is not present, `allow-zero-rows`).

**Same storage, Iceberg ÷ Paimon via the Iceberg reader** (only the writer
differs):

| Query | StarRocks `iceberg` ÷ `pmn_as_iceberg` | DuckDB `iceberg` ÷ `paimon` |
|---|---|---|
| H1 | 1.52 | 1.28 |
| H2 | 1.03 | 1.49 |
| H3 | 1.33 | 1.39 |
| H4 | 2.03 | 1.32 |
| H5 | 0.95 | 1.19 |
| H6 | 1.48 | 1.22 |

- **On both engines, Paimon-written data was faster to read than
  Iceberg-written data on five of the six queries.** The exception is H5 on
  StarRocks, where Iceberg was 5% faster.
- **StarRocks' native Paimon reader was faster again** than reading the same
  files through the Iceberg export: by 1.2–1.7× on H1–H4, and level on H5–H6.

## Why Iceberg was slower on the narrow queries

StarRocks records the bytes each scan read. For the same 30 s window, its
Iceberg arm read **3–7× more bytes** than its Paimon arms on the narrow queries:

| Query | SR `paimon` | SR `pmn_as_iceberg` | SR `iceberg` |
|---|---|---|---|
| H1 | 45 MB | 48 MB | 274 MB |
| H2 | 45 MB | 160 MB | 262 MB |
| H4 | 45 MB | 48 MB | 332 MB |
| H6 | 46 MB | 48 MB | 301 MB |
| H5 (reads `event_json`) | 54.2 GB | 54.2 GB | 56.4 GB |

- **The likely cause is the file layout, not the reader.** The two
  Iceberg-reader arms read different amounts for the same query, so the reader
  isn't the difference. Each Iceberg writer subtask writes its own files into
  the partition (`write.distribution-mode=none`), so pruning to a 30 s window
  leaves more, or larger, column chunks to read.
- **This is inferred from the bytes read, not checked against the files.**
- **H5 is dominated by decoding about 54 GB of JSON,** which is similar on all
  three arms, so H5 ends up close on both engines and both formats.

## DuckDB vs StarRocks: not an engine comparison

On H1–H4 and H6, DuckDB was 6–10× slower than StarRocks on the same storage. On
H5 the two were within ±12%: DuckDB `paimon` 81.2 s against 92.5 s, and DuckDB
`iceberg` 96.4 s against 88.0 s.

- **That pattern is a fixed cost in the cold harness**, not scan speed. The same
  SQL ran on both engines (`lib.sh` renders both from the same file; checked).
- `duckdb.sh` timed a **new `duckdb` process per attempt**. Each attempt paid
  for extension loading, the S3 secret, and reading `metadata.json`, the
  manifest list and every manifest from S3 with no cache: about **20–30 s**
  before any data was read.
- StarRocks reports **server-side** time on a cluster that is already up.
- When there is real data to read (H5), the two engines are level.
- **Do not quote the H1–H4/H6 cross-engine ratios.** The within-engine format
  ratios above are not affected, because both arms of an engine paid the same
  overhead.
- This is why the **warm** harness, `querybench/duckdb-warm.sh`, now exists: one
  long-lived DuckDB process per suite, timed in-process like StarRocks. A warm
  test of the same window on 2026-10-17 (DuckDB `paimon` arm, cache on) gave
  0.7–2.5 s on H1–H4/H6 and 75.7 s on H5. That test is not a comparison run and
  is not recorded here.

## Rows in the window

| | Iceberg | Paimon (export) |
|---|---|---|
| DuckDB `COUNT(*)` (H2), 15:05–15:15Z | 93,360,230 | 90,074,336 |
| StarRocks `scan_rows` (rows read after pruning, not the count), ~15:10–15:30Z | 96,852,949 | 97,061,719 (both Paimon arms) |

- **The two tables did not hold the same rows in the window.** DuckDB's counts
  are 3.6% apart.
- The window's bounds were right: both arms' newest row is 15:02:40.999.
- **The tables were still catching up.** The jobs were ~18k SQS messages behind
  at the time, and SQS delivers landing files out of order, so rows stamped
  inside the window kept being committed after it closed. Pinning `HOT_TO`
  3 minutes back was not enough.
- **So the per-query times on the two formats cover slightly different row
  counts.** At ~0.3–1 µs per row, a 3.6% gap is small next to the 1.2–2× format
  differences above, but it is not zero.
- StarRocks' `scan_rows` are rows read, not rows matched, so they cannot settle
  the count. The StarRocks CSV does not record result values.

## Caveats

- **One run, one window.** Each figure is the median of 3 timed passes.
- **Writers live, generator writing.** That is what the hot suite measures. But
  each table was being written while it was read, and the two writers were not
  at the same point in their backlog (see "Rows in the window").
- **Both engines ran at the same time,** sharing the S3 bucket. Their compute
  was separate, but each one's S3 reads were not isolated from the other's.
- **StarRocks ran with its data cache off** (the harness default then).
  - Every pass read S3, as DuckDB's cold harness did.
  - Since 2026-10-17 `starrocks.sh` runs with the cache on (`SR_DATACACHE=1`),
    paired with warm DuckDB. Results from the two modes are not comparable.
- **StarRocks placement was not controlled:**
  - two of the three BEs shared one m5.4xlarge;
  - the FE and BEs had no node selector.

  Both are fixed since 2026-10-17 by `querybench/starrocks-placement.sh`: all
  on `driver-nodepool`, one BE per node.
- **DuckDB has no Paimon reader.** Its `paimon` arm reads Paimon's Iceberg
  export with the same `iceberg_scan` as the `iceberg` arm. It is the
  counterpart of StarRocks' `pmn_as_iceberg`, not of StarRocks' native `paimon`
  arm.
