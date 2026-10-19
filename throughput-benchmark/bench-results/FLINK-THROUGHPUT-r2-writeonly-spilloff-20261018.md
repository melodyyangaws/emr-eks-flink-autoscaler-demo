# Flink write-path throughput, second table set (`-r2`, writeonly-spilloff), from the commit history

**Configuration markers:**
- `CID_SUFFIX=-r2`: new warehouses, Glue databases and checkpoint paths. The
  tables started empty.
- `JOB_PARALLELISM=320`.
- `PAIMON_WRITE_ONLY=true`, `PAIMON_WRITE_BUFFER_SPILLABLE=false`.
- The same jobs, TaskManager shape, SQS source and bucket as the
  [writeonly-spilloff run](FLINK-THROUGHPUT-writeonly-spilloff-20261010.md).
- Both writers ran at once, sharing `executor-memorynodepool` and one bucket, so
  this measures each format **under contention**, as before.

**Measured** 2026-10-18 from 10:31Z (first commits) to 13:17Z (both jobs
suspended).

**Primary measure: commits.** Rows added and the gap for every table commit, the
same method on both formats.
- **Paimon:** 134 snapshots, all `APPEND`. That confirms write-only: no
  `COMPACT` snapshots.
- **Iceberg:** 127 snapshots, all `append`.
- **Steady state:** a commit whose gap from the previous one is 50–70 s
  (60 s checkpoints), so no restart in between.

The job monitor's per-iteration rows/s reads low, and is not used. It divides a
100 s counter delta, sampled every 20 s, over counters that move in steps, so
it reads 2.4–2.5M rows/s while the commits show ~3.0M.

**Source data:**
- `bench-results/cid-paimon-r2-commits-20261018.csv` and
  `bench-results/cid-iceberg-r2-commits-20261018.csv`: every commit, with
  `rows_per_s`.
- A raw copy is in `~/r2commits-backup/`. The tables themselves were deleted
  after the run.

## Headline

**At capacity** (the jobs had an SQS backlog, so the write path was the limit):

| Phase | Paimon rows/s (n) | Iceberg rows/s (n) | Iceberg vs Paimon |
|---|---|---|---|
| C1: 7 generator replicas, after the 12:08 redeploy | 3,023,497 (17) | 3,090,868 (13) | +2.2% |
| C2: 8 replicas | 3,023,989 (32) | 3,075,833 (30) | +1.7% |
| D: generator finished, draining the backlog | 3,022,791 (10) | 3,056,442 (10) | +1.1% |

- **Paimon's capacity is ~3.02M rows/s, Iceberg's ~3.06–3.09M (+1–2%).**
- That matches the 2026-10-10 writeonly-spilloff run: 3,008,048 against
  3,092,250 per commit (+2.8%).
- So the second table set reproduces the first within 1–2%.

**Supply-limited** (the jobs were ahead of the generator: queues near 0, writer
busy ~720–750 ms/s):

| Phase | Paimon rows/s (n) | Iceberg rows/s (n) |
|---|---|---|
| A: 6 replicas, 10:30–11:00Z | 2,614,227 (24) | 2,579,001 (24) |
| B: 6 replicas, 11:29–11:59Z | 2,580,260 (27) | 2,575,471 (28) |

- **These are the generator's rate (~2.58M rows/s), not the formats'.**
- The generator was started only after both jobs were running, so there was no
  backlog for the jobs to work through.
- They show the formats keep up with 6 replicas, not how fast either can go.

## Restarts and gaps (commit gaps > 70 s)

| When (Z) | Paimon | Iceberg | Cause |
|---|---|---|---|
| 10:33–10:36 | 2 gaps (88–91 s) | 3 gaps (71–108 s) | start-up |
| 11:01–11:30 | 1 gap (1,640 s) | 3 gaps (~600 s each) | no data between generator runs (Iceberg commits an empty snapshot every ~10 min; Paimon does not) |
| 12:06–12:20 | 3 gaps (116–191 s) | 2 gaps (179 s, 602 s) | the 12:08 redeploy of both jobs; Iceberg took ~10 min to commit again |
| 12:50 | — | 1 gap (128 s) | Iceberg writer failed on S3 **503 SlowDown** (6 SDK attempts), restart from the last checkpoint |

- **The S3 503 restart is the one unplanned failure.**
  - It came at the highest load: 8 generator pods writing landing files, both
    writers at ~3M rows/s each, and the hot-query runner counting rows in
    both tables.
  - It hit only Iceberg, and only once.
  - Paimon was not throttled in this run.

## Totals

| | Paimon | Iceberg |
|---|---|---|
| rows at the last commit | 21,069,429,447 | 20,386,501,855 |
| queue when suspended (13:17Z) | 0 visible, 2,923 in flight | 19,677 visible, 4,711 in flight |

- **Delivery was not exact this time, and could not be checked.**
  - Iceberg was still ~0.7B rows behind when both jobs were suspended. The
    queues were then purged.
  - The generator's per-pod totals for the first 30-minute run were lost (the
    pods' logs were gone before they were saved).
  - The rows/s figures above are unaffected: they are per commit.

## Caveats

- **Contention:** both writers share a nodepool and a bucket, and S3 throttled
  Iceberg once at peak.
- **Short steady windows at capacity:** 59 Paimon and 53 Iceberg commits in
  C1–D.
- **Phases A and B are generator-limited.** Do not quote them as format
  throughput.
- **The hot query runs during this window were stopped,** so they recorded no
  results.
  - **First attempt:** the in-cluster runner's kubectl crashed.
  - **Second:** the DuckDB half was disturbed by an early redeploy.
  - **Third:** stopped while waiting for the window to be committed in both
    tables.

  So no query load was timed against these tables. StarRocks' completed hot
  suite at 11:40Z is in the FE audit log only.
- **Monitor and collectors:**
  - The monitor read the original tables until it was redeployed with
    `CID_SUFFIX=-r2` at 12:02Z.
  - The laptop collectors' REST samples stopped at 12:11Z, when the redeploy
    broke their port-forwards. The samples they took agree with the commits:
    Paimon 3.02–3.03M, Iceberg 3.01–3.02M (12:08–12:11Z).
2