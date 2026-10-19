# Benchmark summary, 2026-10-19: Flink write throughput + hot queries (writeonly-spilloff)

**Configuration markers:**
- **Tables:** the original table set (`CID_SUFFIX` empty), `flink_cid_{paimon,iceberg}_db.cid_events`,
  recreated empty before the run.
- **Write path:** `JOB_PARALLELISM=320`, `PAIMON_WRITE_ONLY=true`,
  `PAIMON_WRITE_BUFFER_SPILLABLE=false`.
- **Source:** S3 → SNS → SQS, reading only `OUTPUT_URI`'s prefix
  (`cid-landing-jsongz/`).
- **TaskManagers:** `karpenter.sh/do-not-disrupt` on every TM (160/160).
- **Contention:** both writers ran at once on one nodepool and one bucket.
- **Query side:**
  - **StarRocks 4.1.4:** 3 BEs on `driver-nodepool`, datacache on and empty
    at the start, no tracing agent.
  - **Warm DuckDB 1.5.6:** `duckdb-warm-hot`, disk + memory cache on gp3-fast,
    empty at the start.

**Timeline (UTC):**

| Time | Event |
|---|---|
| 04:15 | Both jobs RUNNING. They started with ~315k queued files (~1 h of data) left by an earlier, unread generator run. |
| 04:16–05:17 | Generator: 7 replicas × 1 h on top of that backlog. |
| 04:31–04:34 | Paimon job suspended and resumed, to let Iceberg catch up. |
| 04:57:54 | Hot runner: writers stable for 5 min; window pinned. |
| 05:58 | Hot window committed in both tables. |
| 06:00–06:34 | StarRocks hot suite. |
| 06:34–06:49 | Warm DuckDB hot suite. |
| ~06:11 | Backlog fully drained (last commits with rows). |

## 1. Flink write throughput (from the commit history)

**Primary measure: commits.**
- **What it measures:** the rows each commit added, divided by the gap since the
  previous commit.
- **Steady state:** a gap of 50–70 s (60 s checkpoints).
- **Commits:** Paimon 121 snapshots, all `APPEND`, confirming write-only;
  Iceberg 120 snapshots, all `append`.
- **The jobs were saturated the whole time.** They had a backlog throughout, so
  this run measures **capacity**, unlike the 10-18 run, which was limited by
  the generator.

| | Paimon | Iceberg | Iceberg vs Paimon |
|---|---|---|---|
| **per-commit rows/s, median (all steady commits)** | **3,004,902** (n=118) | **3,065,401** (n=116) | **+2.0%** |
| … backlog + generator, 04:16–05:17 | 3,007,681 (n=58) | 3,073,679 (n=61) | +2.2% |
| … backlog drain, 05:17–06:11 | 2,999,734 (n=55) | 3,062,371 (n=55) | +2.1% |
| REST rows/s, median | 2,998,641 (04:17–04:31 only*) | 3,065,613 (04:17–06:11) | +2.2% |
| writer busy, median | 967.6 ms/s | 967.7 ms/s | |
| source backpressure, max | 0 | 0 | |
| checkpoint duration / state, median | 3,956 ms / 1.71 MB | 976 ms / 0.24 MB | |
| rows at the last commit | 21,341,501,005 | 21,278,812,807 | |

\* Paimon's laptop collector stopped when the job was suspended at 04:31Z (its
port-forward broke). The commit figures cover the whole run.

- **Both formats were CPU-bound,** at ~968 ms/s busy with no backpressure.
  **Iceberg writes ~2% more rows/s than Paimon** (3.07M against 3.00M).
- **This matches the earlier capacity runs:**
  - 10-10: Paimon 3.01M, Iceberg 3.09M, +2.8%.
  - 10-18 `-r2`: Paimon 3.02M, Iceberg 3.06–3.09M.
- **Paimon's checkpoints take ~4× longer** than Iceberg's (4.0 s against
  1.0 s), with ~7× the state.
- **The only restart was the planned Paimon suspend/resume:** a 167 s commit
  gap at 04:34Z.
  - No Karpenter eviction this time: the generator finished at 05:17Z, and the
    `do-not-disrupt` annotation held.
  - Iceberg's 600 s gaps after 06:11Z are its empty commits once idle.
- **Delivery:** the two totals differ by 62.7M rows (0.3%). Both queues drained
  to 0, so both read every file.
  - **The likely cause is Paimon's restart.** Messages for files it had taken but
    not committed became visible again after the 15 min visibility timeout. A
    file read twice would add its rows twice.
  - **Not verified.** The generator's per-pod totals were not captured.

Data: `cid-{paimon,iceberg}-commits-20261019T0415Z.csv` (every commit),
`cid-paimon-20261019T041732Z.csv` and `cid-iceberg-20261019T041736Z.csv` (REST).
The raw snapshot history is in `~/r5commits-backup-20261019/`.

## 2. Hot query suite (writers running)

**Window:** dt=2026-10-19, **`[04:52:24, 04:52:54)` on `eventTimeDT`**.
- **Same rows on every arm:** the window holds **89,691,393 rows** in both
  tables, checked equal and stable before any query ran.
- **The wait for that took about an hour:** Iceberg's count went from 0 at
  05:45Z to the full number at 05:56Z, while the backlog drained.
- **Passes:** 1 warm-up + 3 timed per query, median reported.
- **Order:** pass-major on both engines, StarRocks first, then DuckDB.

### StarRocks (median seconds; all 18 OK)

| Query | `paimon` (native reader) | `pmn_as_iceberg` | `iceberg` | `iceberg` ÷ `pmn_as_iceberg` |
|---|---|---|---|---|
| H1 freshness | 2.1 | 5.3 | 22.2 | 4.2 |
| H2 count | 1.8 | 4.9 | 21.4 | 4.4 |
| H3 needle uuid | 3.5 | 6.4 | 22.2 | 3.5 |
| H4 narrow agg | 2.4 | 5.8 | 24.7 | 4.3 |
| H5 wide JSON | 82.8 | 87.6 | 105.7 | 1.2 |
| H6 customer agg | 3.0 | 6.4 | 30.2 | 4.7 |
| arm total | 423 s | 544 s | 1,177 s | |

- **Paimon-written data was 3.5–4.7× faster to read** than Iceberg-written data
  on the narrow queries (H1–H4, H6), on the same Iceberg reader. On H5 it was
  1.2× faster.
- **StarRocks' native Paimon reader** was a further 1.1–2.7× faster than reading
  the same files through the Iceberg export.
- **Iceberg read far more data for the same 30 s window.**
  - **Bytes:** ~21–23 GB on the narrow queries, against ~1.6–1.8 GB on both
    Paimon arms (12–15×).
  - **Rows:** Iceberg scanned 257.5M rows, Paimon 286.4M.
  - **Likely cause (inferred):** Iceberg's files are much wider in
    `eventTimeDT`. The backlog was delivered out of order, and each Iceberg
    writer subtask writes its own files, so pruning to 30 s leaves far more
    column data to read. This was not checked against the files.
- **H5** decodes ~57–78 GB of `event_json` and is close on all arms.

### Warm DuckDB: not usable this run

| Query | `paimon` (export) | `iceberg` |
|---|---|---|
| H1 | 19.6 s | 18.0 s |
| H2 | 19.3 s | 18.0 s |
| H3 | 23.5 s | **ERROR** |
| H4 | **ERROR** | **ERROR** |
| H5 | **ERROR** | **ERROR** |
| H6 | **ERROR** | 22.5 s |

- **6 of 12 queries never ran** (`server refused the query`).
- **Cause, a bug in the fix for the 10-18 cursor bug:**
  - `/health` now takes the query lock while it measures the on-disk cache.
  - That measurement walks the cache directory, so it gets slow as the cache
    fills.
  - The harness calls `/health` before every request. A query submitted while
    `/health` held the lock got "busy", which the harness recorded as refused.
- **The 6 that ran are not representative either.** They ran alongside those
  cache walks: H1 took 19.6 s, against ~3.3 s on the fixed server on 10-18.
- **Fix needed:**
  - `/health` should not walk the cache. It should report only `busy`, and
    measure the cache only when asked.
  - Submission should wait for the lock briefly instead of failing at once.

  Then rerun DuckDB's hot suite on this window.
- **No DuckDB vs StarRocks comparison is drawn from this run.**

## Caveats

- **Contention:** both writers shared a nodepool and a bucket.
- **Hot runs under different write load.** StarRocks ran its hot suite during
  the backlog drain (06:00–06:34Z). DuckDB ran just after the drain, against
  writers that were running but idle.
- **Throughput per format still holds.** The Paimon suspend/resume (requested,
  04:31–04:34Z) briefly gave Iceberg the bucket and nodes to itself, but
  steady-state commits exclude the gap.
- **Rows twice:** Paimon's total is 0.3% above Iceberg's, probably files read
  twice after its restart (see §1).
