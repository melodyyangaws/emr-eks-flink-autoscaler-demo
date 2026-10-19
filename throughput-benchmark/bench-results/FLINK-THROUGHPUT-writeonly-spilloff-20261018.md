# Flink write-path throughput, 2026-10-18 run (writeonly-spilloff, from the commit history)

**Configuration markers:**
- The original table set (`CID_SUFFIX` empty): `flink_cid_{paimon,iceberg}_db`.
- `JOB_PARALLELISM=320`.
- `PAIMON_WRITE_ONLY=true`, `PAIMON_WRITE_BUFFER_SPILLABLE=false`.
- The same jobs, TaskManager shape, SQS source and bucket as the
  [10-10 writeonly-spilloff run](FLINK-THROUGHPUT-writeonly-spilloff-20261010.md)
  and the [10-18 `-r2` run](FLINK-THROUGHPUT-r2-writeonly-spilloff-20261018.md).
- The source now reads only `OUTPUT_URI`'s prefix (`'key-prefix'` =
  `cid-landing-jsongz/`).
- Both writers ran at once, sharing `executor-memorynodepool` and one bucket, so
  this measures each format **under contention**.

**Measured:**
- **Jobs deployed:** 15:59Z.
- **Generator:** 7 replicas × 1 h, 16:00:05Z → ~17:00Z, into
  `s3://<bucket>/cid-landing-jsongz/`.
- **Jobs suspended:** 17:27Z.
- **Hot query suite:** ran against both tables from 16:19Z (StarRocks
  16:22–16:49Z, then DuckDB to 17:25Z).

**Primary measure: commits.** Rows added and the gap for every table commit.
- **Paimon:** 63 snapshots, all `APPEND`, confirming write-only.
- **Iceberg:** 66 snapshots, all `append`.
- **Steady state:** a commit 50–70 s after the previous one (60 s checkpoints).

**Secondary measure:** laptop REST collectors, 30 s samples, 16:01–17:26Z.

**Source data:**
- `bench-results/cid-{paimon,iceberg}-commits-20261018T1600Z.csv`: every commit,
  with `rows_per_s`.
- `bench-results/cid-paimon-20261018T160052Z.csv` and
  `bench-results/cid-iceberg-20261018T160057Z.csv`: the REST samples.

## Headline

| While the generator ran (16:01–17:00Z) | Paimon | Iceberg | Iceberg vs Paimon |
|---|---|---|---|
| per-commit rows/s, median (steady) | **2,980,426** (n=57) | **2,979,254** (n=56) | −0.04% |
| REST rows/s, median | 2,975,308 | 2,975,263 | 0.0% |
| writer busy, median | 926.3 ms/s | 892.8 ms/s | |
| source backpressure, max | 0 | 0 | |
| checkpoint duration, median | 1,776 ms | 1,039 ms | |
| rows committed in the phase | 10,496,831,175 | 10,420,584,917 | |

- **Both formats kept up with the generator.** Its rate (7 replicas,
  ~2.98M rows/s) was the limit, not either write path. The evidence:
  - The two per-commit medians are equal to within 0.04%.
  - Writer busy was 893–926 ms/s, below the ~965 ms/s of a saturated job.
  - There was no backpressure.
  - SQS held no waiting messages, only in-flight ones.
- **So this run does not separate the formats.** Their capacity, measured with a
  backlog on 10-10 and in the `-r2` run, is about **3.02M (Paimon) and
  3.06–3.09M (Iceberg) rows/s**, with Iceberg ahead by 1–3%.
  - 7 generator replicas sit just under both, which is why the two matched.
  - To measure capacity, start with a backlog (generator before the jobs) or run
    8+ replicas. 8 replicas triggered S3 503 throttling once on 10-18.

## Restarts (commit gaps > 70 s)

| When (Z) | Paimon | Iceberg | Cause |
|---|---|---|---|
| 15:10–16:01 | — | idle commits (600 s, 2,453 s) | no data yet: an empty commit every ~10 min before the generator started |
| 16:25 | — | 1 gap (72 s) | one slow checkpoint during the hot suite; no restart |
| 17:00–17:21 | 1 gap (534 s, to 17:09:46) | 1 gap (1,176 s, to 17:21:01) | **Karpenter consolidation** |

**About the Karpenter restarts:**
- **What happened:** the generator's pods finished at ~17:00Z, and the
  executor nodes looked underutilized. Karpenter evicted TaskManagers
  (`Evicted pod: Underutilized`), and each loss restarted the whole job.
- **Recovery time:** Iceberg took ~20 min to recover (65 TaskManagers pending
  for nodes), Paimon ~9 min.
- **Fixed for the next deploy:** `karpenter.sh/do-not-disrupt` is now on both
  TaskManager pod templates.
- **No effect on the headline:** all of this came after the generator finished,
  and steady-state commits exclude restart gaps.

## Totals and what was left unread

| | Paimon | Iceberg |
|---|---|---|
| rows at the last commit | 10,936,879,999 (17:25:50Z) | 11,005,201,338 (17:26:06Z) |

- **Delivery was not verified.** The generator's per-pod totals were not
  captured (its Job is gone).
- **The two totals differ by 68M rows (0.6%).** Each job was at a different
  point in its post-restart backlog when both were suspended.
- **A second generator run (17:23Z → ~18:23Z) was never read.** It started just
  before the jobs were suspended, so both queues hold ~315k messages (about an
  hour of data). Purge them before the next run, or the next deploy reads them
  first.

## Caveats

- **Contention:** both writers share a nodepool and a bucket.
- **Query load in the same window:** the hot query suite ran against both tables
  from 16:19Z. Its row counts read with the cache off, and its scans read
  through the cache. The effect on the writers was not isolated.
- **Generator-limited:** as above, this run measures the generator's rate, not
  either format's ceiling.
- **Hot-query results from the same run:**
  - **StarRocks** (`starrocks-20261018T161902Z.csv`): valid.
  - **Warm DuckDB** (`duckdb_warm-20261018T161902Z.csv`): **not usable.** A bug
    in `duckdb_warm_server.py`, now fixed, shared one DuckDB connection between
    the query and `/health` threads. At least 3 passes returned another
    thread's result, and the times may be wrong too. It needs a rerun.
