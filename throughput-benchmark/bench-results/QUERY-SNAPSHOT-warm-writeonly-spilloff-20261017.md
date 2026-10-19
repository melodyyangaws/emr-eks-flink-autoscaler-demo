# Snapshot query benchmark: warm DuckDB vs warm StarRocks, Paimon vs Iceberg

**Configuration markers:** suite `snapshot` (S1–S4, `querybench/snapshot/`)
over the closed partition dt=2026-10-10 (4,642,903,015 rows). The tables were
written by the
[writeonly-spilloff run](FLINK-THROUGHPUT-writeonly-spilloff-20261010.md), with
the writers stopped. Both engines were **warm, with their data caches on**
(their defaults), and both caches started empty.

**Measured:**
- Run `20261017T140929Z`.
- StarRocks: 2026-10-17 14:11Z → ~17:46Z.
- Warm DuckDB: 2026-10-18 01:05Z → 03:39Z. It ran from an in-cluster Job (see
  Caveats for why it started later).

**Source data:**
- `bench-results/starrocks-20261017T140929Z.csv`: timed passes, with scan rows
  and bytes.
- `bench-results/duckdb_warm-20261017T140929Z.csv`: timed passes.
- Warm-up passes come from the FE audit log (StarRocks) and the DuckDB pass log.
  The pass log isn't committed, because it names the bucket.
- Compare with `./querybench/compare.py --sr <starrocks csv> --duck <duckdb_warm csv> --suite snapshot`.

## Setup: like for like

| | StarRocks 4.1.4 | warm DuckDB 1.5.6 |
|---|---|---|
| nodes | `driver-nodepool`; 3 BEs on m5.2xlarge, one per node; FE on its own node | `driver-nodepool`, one m5.8xlarge |
| CPU / memory | 3 × 6 cpu / 24 Gi = 18 / 72 Gi | 18 cpu / 72 Gi (DuckDB memory_limit 44 GB) |
| process | always up | one long-lived DuckDB process |
| data cache (on, both defaults) | datacache: 3 × 150 GiB disk (gp3) + ~3.8 GB memory per BE | cache_httpfs: 450 GiB disk (gp3-fast) + 12.5 GB memory |
| cache at start | emptied, BEs restarted | emptied |
| timing | server-side (FE audit log) | in-process, execute + fetch |
| pass order | S1–S4 warm-up, then S1–S4 timed | the same |
| tracing agent | none (opted out, verified per pod) | none |
| engines at once | one at a time (StarRocks, then DuckDB) | |

**Arms:**
- **StarRocks:**
  - `paimon` uses the native Paimon reader.
  - `pmn_as_iceberg` uses the Iceberg reader on Paimon's Iceberg export.
  - `iceberg` uses the Iceberg reader on the natively written Iceberg table.
- **DuckDB** has no Paimon reader:
  - `paimon` = `iceberg_scan` over Paimon's Iceberg export. Its StarRocks
    counterpart is `pmn_as_iceberg`.
  - `iceberg` = `iceberg_scan` over the Iceberg table.

**Queries:**
- **S1–S3** scan the whole partition.
- **S4** decodes `event_json` over a 3-minute `eventTimeDT` window,
  `[15:06:00, 15:09:00)`.
  - `eventTimeDT` is set by the generator, so the window holds the same
    **462,833,934 rows** in every table. Every arm returned exactly that count.
  - On StarRocks only, S4 runs with `connector_io_tasks_per_scan_operator = 1`
    (2 elsewhere). With the cache on and 2 tasks, S4 filled the 17.5 GiB query
    pool and failed. With 1 task it peaked at ~0.6 GB.

## Headline (timed pass, seconds)

| Query | SR `paimon` | SR `pmn_as_iceberg` | SR `iceberg` | DuckDB `paimon` (export) | DuckDB `iceberg` |
|---|---|---|---|---|---|
| S1 partition count | 27.3 | **0.4** | **1.7** | 82.4 | 122.5 |
| S2 narrow aggregate | 59.1 | 72.2 | **55.0** | 161.2 | 291.5 |
| S3 distinct by risk level | 190.2 | 316.4 | 768.5 | **160.2** | 171.2 |
| S4 wide JSON (462.8M rows) | 799.8 | 1,055.3 | 1,184.5 | **462.2** | 629.3 |

All 20 cells are OK.

**DuckDB ÷ StarRocks on the same storage** (the engine comparison; below 1 means
DuckDB was faster):

| Query | Paimon export: DuckDB `paimon` ÷ SR `pmn_as_iceberg` | Iceberg: DuckDB ÷ SR |
|---|---|---|
| S1 | 215 | 73 |
| S2 | 2.23 | 5.30 |
| S3 | **0.51** | **0.22** |
| S4 | **0.44** | **0.53** |

**Iceberg ÷ Paimon, same reader** (the format comparison; only the writer
differs):

| Query | StarRocks `iceberg` ÷ `pmn_as_iceberg` | DuckDB `iceberg` ÷ `paimon` |
|---|---|---|
| S1 | 4.3 (both from metadata) | 1.49 |
| S2 | 0.76 | 1.81 |
| S3 | 2.43 | 1.07 |
| S4 | 1.12 | 1.36 |

## Findings

1. **No engine wins every query.** Each wins a different kind of query.
   - **S1:** StarRocks' Iceberg reader answers the full-partition `COUNT(*)`
     from Iceberg metadata, in 0.4 s and 1.7 s with 0 bytes scanned. DuckDB
     scans for it (82–123 s), and so does StarRocks' native Paimon reader
     (27 s). That's a planner feature, not scan speed. Don't read S1 as an
     engine speed result.
   - **S2, the narrow aggregate over one column:** StarRocks is faster, by
     2.2× on the Paimon export and 5.3× on Iceberg.
   - **S3 and S4, a heavy distinct and a wide decode:** DuckDB is faster.
     - It's about 2× faster on S4 (462 s against 1,055 s on the Paimon export;
       629 s against 1,185 s on Iceberg).
     - It's 2–4.5× faster on S3 (160 s against 316 s; 171 s against 768 s).
     - StarRocks' S4 ran with half its usual scan tasks, to stay inside its
       query memory. That is part of the S4 gap.
2. **Paimon-written data was faster to read than Iceberg-written data on most
   queries.**
   - On DuckDB, Iceberg was slower on all four: 1.07–1.81×.
   - On StarRocks it was slower on S1, S3 (2.4×) and S4 (1.12×), but faster on
     S2 (0.76×).
   - **StarRocks' Iceberg arm read more bytes than its Paimon-export arm**, for
     the same rows, on every query that scans:

     | Query | SR `iceberg` | SR `pmn_as_iceberg` |
     |---|---|---|
     | S2 | 12.1 GB | 0.19 GB |
     | S3 | 5.3 GB | 0.05 GB |

     The Paimon-export figures are bytes after its cache hits; see finding 3.
3. **The warm cache helped the narrow queries a lot, and the heavy ones little.**

   | | S2 warm-up → timed | S3 warm-up → timed | S4 warm-up → timed |
   |---|---|---|---|
   | SR `paimon` | 721 → 59 s | 202 → 190 s | 1,131 → 800 s |
   | SR `pmn_as_iceberg` | 484 → 72 s | 502 → 316 s | 1,044 → 1,055 s |
   | SR `iceberg` | 1,048 → 55 s | 792 → 768 s | 1,279 → 1,185 s |
   | DuckDB `paimon` | 919 → 161 s | 160 → 160 s | 1,014 → 462 s |
   | DuckDB `iceberg` | 1,158 → 292 s | 642 → 171 s | 1,134 → 629 s |

   - **S2 got 4–19× faster** once warm on both engines, because its one column
     fits in the cache.
   - **StarRocks' S4 barely improved** (0–29%). Its ~286 GB of `event_json`
     reads are more than its gp3 cache disks serve faster than S3.
   - **DuckDB's S4 roughly halved**, read from its faster gp3-fast cache disk.
     That difference in cache disks is part of the S4 result (see Caveats).

## Caveats

- **One timed pass per query**, not a median of 3. The run used `RUNS=1
  WARMUP=1` to fit the suite in hours rather than a day.
- **The cache disks differ.**
  - StarRocks' BEs sit on default gp3 (125 MB/s), and its io adaptor sends
    reads back to S3 when the disk saturates.
  - DuckDB's cache_httpfs can't fall back to S3, and on default gp3 it was
    slower than S3, so DuckDB's cache disk is gp3-fast (1,000 MB/s).
  - That matters for the timed passes of the large scans (S3, S4), not for the
    warm-up passes.
- **StarRocks' S4 used 1 scan task** (`io-tasks=1`), the others 2. Without it
  S4 can't finish with the cache on (see Setup).
- **The engines ran about 9 hours apart,** on the same frozen tables with the
  writers stopped, so the data was identical.
  - The in-cluster runner that should have started DuckDB straight after
    StarRocks was stuck by a bug in its idle check (fixed in `qb-runner.sh`).
  - Each engine still ran alone, and its cache started empty.
- **S1 measures planning, not scanning**, on StarRocks' Iceberg-reader arms (see
  finding 1).
- **DuckDB's `paimon` arm is Paimon's Iceberg export**, read by the same
  `iceberg_scan` as its `iceberg` arm. StarRocks' native Paimon reader has no
  DuckDB counterpart.
