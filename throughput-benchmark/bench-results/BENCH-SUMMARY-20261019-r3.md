# Benchmark summary, 2026-10-19 `-r3`: Flink write throughput + hot queries (writeonly-spilloff)

**Configuration:**
- **Tables:** a new table set, `CID_SUFFIX=-r3`:
  `flink_cid_{paimon,iceberg}_db_r3.cid_events`, empty at the start.
- **Write path:** `JOB_PARALLELISM=320`, `PAIMON_WRITE_ONLY=true`,
  `PAIMON_WRITE_BUFFER_SPILLABLE=false`, 80 TaskManagers per job, all
  `karpenter.sh/do-not-disrupt`.
- **Source:** S3 → SNS → SQS from `cid-landing-jsongz/`. The generator ran
  7 replicas until stopped (`DURATION_SECONDS=0`) and was started 2 min before
  the jobs, so they began with a small backlog: 1,573 (Paimon) and 5,882
  (Iceberg) files.
- **Contention:** both writers ran at once, on one nodepool and one bucket.
- **Query side:**
  - **StarRocks 4.1.4:** 3 BEs on `driver-nodepool`, datacache on.
  - **Warm DuckDB 1.5.6:** one server, `duckdb-warm`, started fresh by the
    runner, so its cache was empty.
- **Hot test:** `SUITE=hot ./querybench/qb-runner.sh start`, which ran
  `hot-interleave.sh` in the cluster, plus the `qb-watch` watcher.
  - **Order:** one warm-up pass, then 3 timed passes. Each pass ran StarRocks
    (all arms), then DuckDB (all arms), on one pinned version of each table.
  - **Writers live throughout:** the generator was writing before the first
    pass and after the last (`.pins` file).

**Timeline (UTC):**

| Time | Event |
|---|---|
| 19:05 | Generator started (7 replicas, until stopped). |
| 19:08 | Both jobs deployed. |
| 19:15 | Both jobs `RUNNING`, 160/160 TaskManagers. |
| 19:23–19:27 | `duckdb-warm` started and validated; runner and watcher started. |
| 19:27 | Writers `RUNNING` (first check); window `[19:21:06, 19:21:36)` set. |
| after 19:32 | Window committed in both tables. The first counts failed until the missing `-r3` StarRocks catalogs were created (~19:29; fixed in the code since). |
| 20:40:46 | Hot test done; results uploaded to `s3://<bucket>/querybench-results/20261019T192327Z-hot/`. |
| 20:47:32 | Watcher: generator deleted, both writers suspended, monitor at 0, `duckdb-warm` down. |

## 1. Flink write throughput (from the commit history)

**Method:**
- **Rate per commit:** the rows each commit added, divided by the gap since the
  previous commit.
- **Steady state:** a gap of 50–70 s (the checkpoints run every 60 s).
- **Coverage:** every commit from 19:13Z to 20:47Z is in the history: Paimon
  94 snapshots, Iceberg 93.
- **Saturated throughout:** the jobs had a backlog the whole time, so this run
  measures **capacity**.

| | Paimon | Iceberg | Iceberg vs Paimon |
|---|---|---|---|
| **per-commit rows/s, median (all steady commits)** | **3,017,956** (n=93) | **3,089,154** (n=92) | **+2.4%** |
| … before the hot test (to 19:27) | 2,997,475 (n=13) | 3,071,859 (n=11) | +2.5% |
| … during the hot test (19:27–20:40) | 3,026,622 (n=74) | 3,098,743 (n=74) | +2.4% |
| REST rows/s, median (19:17–20:47) | 3,017,475 | 3,096,661 | +2.6% |
| writer busy, median | 965.4 ms/s | 965.7 ms/s | |
| source backpressure, max | 0 | 0 | |
| checkpoint duration / state, median | 2,102 ms / 1.63 MB | 1,159 ms / 0.24 MB | |
| commit gaps > 70 s (restarts) | none | none | |
| rows at the last commit | 17,008,776,985 | 17,121,998,576 | |

- **Both formats were CPU-bound:** ~965 ms/s busy, no backpressure.
- **Iceberg writes ~2.4% more rows/s than Paimon** (3.09M against 3.02M). That
  matches the earlier capacity runs:
  - 10-10: Paimon 3.01M, Iceberg 3.09M.
  - 10-18 `-r2`: Paimon 3.02M, Iceberg 3.06–3.09M.
  - 10-19 morning: Paimon 3.00M, Iceberg 3.07M.
- **The hot test did not slow the writers.** Their rates during the test equal
  their rates before it (within ~1%).
- **Paimon's checkpoints take ~2× as long** as Iceberg's, with ~7× the state.
- **No restarts.** The only failed checkpoints (Paimon 4, Iceberg 3) were
  during startup, before 19:16:47Z.
- **Totals:** the two differ by 113M rows (0.7%). The jobs were suspended
  without draining the queues, so each was at a different point in its
  backlog. Delivery was not checked against the generator.

Data: `cid-{paimon,iceberg}-commits-20261019T1915Z-r3.csv` (every commit),
`cid-paimon-20261019T191638Z.csv` and `cid-iceberg-20261019T191642Z.csv` (REST,
30 s samples).

## 2. Hot query suite (writers live)

**Window:** `[19:21:06, 19:21:36)` on `eventTimeDT`, dt=2026-10-19.
- **Rows:** 89,883,088, the same in both tables, checked before any query ran.
- **Pins:** Iceberg at one snapshot, Paimon at snapshot 21. Every arm, on both
  engines, read those versions.

**Median seconds over the timed passes:**

| Query | SR `paimon` (native) | SR `pmn_as_iceberg` | SR `iceberg` | DuckDB `paimon` (export)* | DuckDB `iceberg` |
|---|---|---|---|---|---|
| H1 freshness | **3.1** | 10.0 | 14.2 | 9.6 | 9.4 |
| H2 count | **1.2** | 2.3 | 3.1 | 1.5 | 2.1 |
| H3 needle uuid | **3.2** | 5.3 | 5.9 | 3.5 | 4.0 |
| H4 narrow agg | **1.7** | 2.9 | 4.1 | 3.4 / 5.0 | 3.9 |
| H5 wide JSON | **83.6** | 87.9 | 84.1 | 173.2 | 165.2 |
| H6 customer agg | **2.3** | 3.4 | 5.3 | 13.2 | 12.9 |

\* **DuckDB `paimon`: timed passes 1 and 2 only** (H4 differed: 5.0 and
3.4 s, both shown). Pass 3 failed on all 6 queries, so the merged CSV marks
the arm `ERROR`; see §3. Every other arm passed all 3 timed passes.

**StarRocks scanned, per query:**

| | `paimon` | `pmn_as_iceberg` | `iceberg` |
|---|---|---|---|
| rows | 185.8M | 185.8M | 176.2M |
| bytes, H1 | 1.9 GB | 2.4 GB | 10.1 GB |
| bytes, H2 / H4 / H6 | 0.86 / 1.08 / 0.90 GB | 0.87 / 1.03 / 0.89 GB | 0.80 / 1.03 / 0.82 GB |
| bytes, H5 | 56.0 GB | 56.1 GB | 56.1 GB |

**Findings:**
- **StarRocks' native Paimon reader was the fastest arm on every query.**
- **The same reader on Paimon-written vs Iceberg-written data:**
  - **StarRocks' Iceberg reader** was faster on Paimon's export on 5 of 6
    queries. The biggest gap was H1 (10.0 s against 14.2 s), where Iceberg read
    4× the bytes. H5 was even.
  - **DuckDB** read the two formats at about the same speed.
- **Engine against engine, on the same Iceberg reader and table:**
  - **DuckDB was ~1.5× faster** on H1–H3 (e.g. H1: 9.4 s against 14.2 s).
  - **H4 was about even.**
  - **StarRocks was ~2× faster** on H5 (84 s against 165 s) and 2.4× on H6.
- **Compared with the 10-19 13:40Z run**, StarRocks' Iceberg arms were several
  times faster here (e.g. `iceberg` H1 14.2 s against 88.9 s), and read far
  less (10 GB against 53 GB on H1). The likely reason is that this table was an
  hour old with no backlog history. That was not checked against the files.

Data: `starrocks-20261019T192327Z.csv` and `duckdb_warm-20261019T192327Z.csv`
(medians), the per-pass `…-p0..p3.csv` files (p0 = warm-up), and
`hot-interleave-20261019T192327Z.{pins,log}`.

## 3. Issues found, and fixes

- **DuckDB `paimon` failed in pass 3: the pinned version had expired.**
  - **What happened:** all six queries failed to read the pinned snapshot's
    file list (`snap-21…`, HTTP GET error). Pass 3 began more than an hour
    after the pin.
  - **Cause:** the Paimon table keeps snapshots for one hour
    (`'snapshot.time-retained' = '1h'`, `sql-scripts/paimon/03-sink.sql`).
    Pinning a version does not keep its files: Paimon expires the snapshot and
    its Iceberg export's files with it.
  - **StarRocks** read the same version without errors in pass 3. Why was not
    checked.
  - **Fix to choose:** raise `snapshot.time-retained` (e.g. to 3h) so a whole
    hot test fits. It applies from the next deploy.
- **The window counts needed the `-r3` StarRocks catalogs, which didn't exist
  yet.**
  - **Effect:** the Paimon count failed (`?`) until the catalogs were created
    by hand (~19:29Z). The run lost no time to it: Iceberg had not committed
    the window yet either. On its own, the run would have waited out
    `WAIT_COMMIT_S` (90 min) and failed.
  - **Fixed in the code:** `hot-interleave.sh` now runs
    `starrocks.sh catalogs` before counting.
- **The runner's first log line showed the upload step, with the bucket name.**
  Fixed: it now shows the test (`== step: ...`), and the bucket stays
  `$BUCKET_NAME` in the pod.

## Caveats

- **Contention:** both writers shared a nodepool and a bucket, and the queries
  ran on other nodes while they wrote.
- **DuckDB `paimon` has 2 timed passes, not 3.** Its medians are less certain
  than the other arms'.
- **One window, one run.** The hot numbers are for a single 30 s window on a
  one-hour-old table.
