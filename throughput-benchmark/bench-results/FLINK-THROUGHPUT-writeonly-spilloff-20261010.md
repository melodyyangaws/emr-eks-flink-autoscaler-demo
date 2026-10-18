# Flink write-path throughput: Paimon write-only, write buffer not spillable (`writeonly-spilloff`)

**Configuration markers: `JOB_PARALLELISM=320`, `PAIMON_WRITE_ONLY=true`,
`PAIMON_WRITE_BUFFER_SPILLABLE=false`.**
This is the parallel320 run (write-only on) with one change: Paimon's write buffer
is not spillable. When the buffer fills, the writer flushes an L0 file in the
record path, instead of spilling a sorted run for the checkpoint barrier to merge.
Iceberg is unchanged. Same source (S3 → SNS → SQS), TM shape, bucket and generator.

**Measured** 2026-10-10, from the first commit at 14:55Z to the last at 15:20Z.
- **Primary measure: commits.** Rows added and the time gap for every commit,
  both jobs, using the same method on each side.
- **Secondary measure: Flink REST samples** (`collect-metrics.sh`, 30 s), both
  jobs at once on separate ports, 15:08:13Z→15:18:55Z. Both were at full rate for
  all 20 samples.

**Source data:**
- `bench-results/cid-paimon-writeonly-spilloff-commits-20261010.csv`: Paimon
  snapshots 1–26.
- `bench-results/cid-iceberg-writeonly-spilloff-commits-20261010.csv`: Iceberg
  snapshots 1–26 (`commit_identifier` is the Iceberg sequence number).
- `bench-results/cid-paimon-20261010T150803Z-writeonly-spilloff.csv` and
  `cid-iceberg-20261010T150803Z-writeonly-spilloff.csv`: the REST samples.

**Run:**
- **Wiped first.** Both Glue tables dropped, both warehouses and both
  checkpoint/HA prefixes deleted, both queues purged, both queues empty at deploy.
  The tables were recreated empty.
- **Deploy:** `PAIMON_WRITE_ONLY=true PAIMON_WRITE_BUFFER_SPILLABLE=false
  ./build-deploy-cid.sh deploy both`, about 14:48Z. The deploy printed
  `Paimon write path: write-only=true spillable=false`, and the live
  FlinkDeployment carried both settings.
- **Generator:** 6 replicas, 14:48:42Z → about 15:18:42Z (1800 s).
  - 4,642,903,015 rows, about 2.58M rows/s.
  - 139,427 landing files, about 18.6 TB raw / 0.97 TB gzipped, mean row 4,005 B.
- **Startup:** the jobs reached RUNNING at about 14:54Z (Iceberg) and 14:56Z
  (Paimon), on 80 TMs each. By then the generator was about 6 minutes ahead,
  so both jobs read a backlog for the whole run. This is the write-path limit,
  not the generator's rate.
- **End:** both tables drained the backlog by 15:20Z.

## Headline

Steady state means commits whose gap from the previous APPEND is 50–70 s, so
there was no restart in that interval. The checkpoint interval is 60 s. Each
table's final commit is excluded: it only half-filled its interval as the
backlog ran out.

| | Paimon (write-only, not spillable) | Iceberg | Iceberg vs Paimon |
|---|---|---|---|
| per-commit rows/s, median (steady) | **3,008,048** (n=24) | **3,092,250** (n=24) | **+2.8%** |
| … while the generator was writing (to 15:18:42) | 2,999,588 (n=23) | 3,092,757 (n=23) | +3.1% |
| REST rows/s, median (15:08–15:19, both running) | 3,023,858 (CV 0.14%) | 3,095,152 (CV 0.23%) | +2.4% |
| writer-chain busy, median | 966.2 ms/s | 965.0 ms/s | — |
| source backpressure, max | 0 | 0 | — |
| CPU per row (320 × busy ÷ rows/s) | 102.3 µs | 99.8 µs | — |

**Compared with the earlier Paimon runs (same parallelism, same TM shape):**

| | write-only on, spillable (parallel320, 10-09) | write-only off (`writeonlyoff`, 10-10) | write-only on, not spillable (this run) |
|---|---|---|---|
| Paimon rows/s | 3,015,743 (REST) | 2,785,746 (commits) / 2,812,501 (REST) | 3,008,048 (commits) / 3,023,858 (REST) |
| Paimon CPU per row | 102.1 µs | 109.6 µs | 102.3 µs |
| Iceberg rows/s, control | 3,068,989 (REST) | 3,044,930 (commits) | 3,092,250 (commits) / 3,095,152 (REST) |

- **A non-spillable write buffer neither helps nor hurts Paimon.** It's within
  0.3% of parallel320 on either measure (−0.25% commits, +0.27% REST), and CPU
  per row is the same 102 µs. Run-to-run noise here is about 1%, so the setting
  is not a lever at this parallelism.
- **It is about 8% faster than write-only off** (+8.0% commits, +7.5% REST). That
  is the inline-compaction cost measured in `writeonlyoff`, not anything the
  buffer setting adds.
- **Iceberg, the control, moved by under 2%** (+0.8% vs parallel320 REST, +1.6%
  vs writeonlyoff commits), so the comparison between runs holds.
- **The gap between the formats is back to about 2–3%,** as in parallel320.
- **Both writers are CPU-bound, not blocked.** Busy is 96.5% on both, and
  backpressure is 0.

## Restarts and checkpoints

| | Paimon | Iceberg |
|---|---|---|
| restarts while data flowed (commit gaps > 70 s) | 0 | 0 |
| commits from first to last | 26, every gap 57–62 s | 26, every gap 57–62 s |
| checkpoints completed / failed (REST, 15:08→15:19) | 13→24 / 3→3 | 14→24 / 3→3 |
| checkpoint duration, median (max) | 2,304 ms (5,197) | 1,022 ms (2,020) |
| checkpoint state, median | 1.6 MB | 232 KB |

- **No restart from first commit to last.** Unlike the `writeonlyoff` run (8 and
  7 restarts), both jobs ran clean for the whole 25 minutes.
  - The 3 failed checkpoints on each side were all before 15:08, during start-up.
  - None failed in the measured window.
- **After the backlog ran out, the jobs restarted while idle.**
  - Both tables were already complete when that started.
  - Paimon's REST state shows `RESTARTING` from 15:22:52Z and Iceberg's from
    15:27:20Z. Iceberg's failed-checkpoint count climbed 3→20 by 15:31.
  - The timing, a few minutes after the generator's pods finished at about
    15:19Z, matches the Karpenter consolidation seen in `writeonlyoff`. The
    cause was not investigated, and no throughput was lost.
- **Paimon's checkpoints take about twice Iceberg's time and 7× the state.**
  Both are small next to the 60 s interval.

## Totals

| | Paimon | Iceberg |
|---|---|---|
| total rows at the last commit | 4,642,903,015 (15:20:28Z) | 4,642,903,015 (15:20:02Z) |
| generator total | 4,642,903,015 | 4,642,903,015 |
| queue at the end | 0 | 0 |

**Delivery was exact.** Both tables hold exactly the generator's 4,642,903,015
rows, from tables that started empty.

## Caveats

- **This is one short run.** Each table has 24 steady commits over about
  25 minutes. Both formats' steady stretches cover the same minutes and are
  measured the same way.
- **The hot query suites ran during the run.**
  - DuckDB and StarRocks both started at 15:05:41Z against both tables. DuckDB
    finished by about 15:29Z, and StarRocks ran later.
  - That overlaps the REST window and the last 15 minutes of commits.
  - They run on separate nodes and read S3; their effect on the writers was not
    isolated.
- **Both writers shared the executor nodepool and one S3 bucket.** The deploy
  warns that this measures contention. As in parallel320, each rate here is
  under that contention, the same for both.
- **The idle-time restarts after 15:20 were not investigated.** They fall
  outside every measured interval.
