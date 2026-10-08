# Flink write-path throughput: Paimon vs Iceberg

**Measured** 2026-10-05 03:36→04:36 UTC, 107 samples at ~34 s intervals per job,
from the Flink REST API via `collect-metrics.sh`.
**Source data:** `bench-results/cid-paimon-20261005T033554Z.csv`,
`bench-results/cid-iceberg-20261005T033549Z.csv`.
**Cluster:** `loadtest-mcp` EKS, us-west-2, ns `emr-flink`. Both jobs at
parallelism 160, identical TaskManager shape (`limits.cpu: 4`), same datagen
source, same ~4 KiB rows, same S3 bucket.

> **Configuration note (added 2026-10-06).** These numbers were measured with
> Paimon's `bucket` pinned 1:1 to `JOB_PARALLELISM`, i.e. `bucket=160`. The
> project now derives `PAIMON_BUCKETS = JOB_PARALLELISM / 2` (80 at parallelism
> 160) — which is the first half of the "obvious next experiment" proposed in
> the Conclusion below. **Every `bucket=160` in this document is the
> configuration as measured, not the configuration the code produces today**,
> and the write-path gap reported here has not been re-measured at 80.

## Headline

| | Paimon | Iceberg | Iceberg advantage |
|---|---|---|---|
| source rows/s (median) | **554,844** | **717,397** | **+29.3%** |
| sink rows/s (median) | 554,825 | 717,398 | +29.3% |
| throughput @4 KiB/row | 2.12 GiB/s | 2.74 GiB/s | +0.62 GiB/s |
| coefficient of variation | 0.34% | 0.17% | — |
| job restarts / failed ckpts in window | 0 / 0 | 0 / 0 | — |

**Iceberg sustains 1.293× Paimon's write throughput.** Paimon's deficit is
**22.7%** at equal uptime, parallelism, and hardware.

Source rate and sink rate are equal to within 0.004% on both jobs, so nothing is
buffering: whatever the sink can absorb is exactly what the source is allowed to
produce. The sink is the ceiling on both arms.

## The measurement is trustworthy

Four reasons to believe the 1.293× rather than treat it as noise:

1. **Both jobs were in steady state.** CV is 0.34% (Paimon) and 0.17%
   (Iceberg) across the full hour. OLS slope on source rows/s is −0.198 and
   −0.845 rows/s² respectively — i.e. flat, with a slight downward drift far too
   small to matter. There is no warmup artifact in this window.
2. **No restarts, no failed checkpoints.** `job_state` is `RUNNING` in 107/107
   samples on both. Paimon completed 59 checkpoints (failed 4→4, no new
   failures); Iceberg completed 60 (failed 3→3). The pre-existing failure counts
   are from job startup, before this window.
3. **An independent corroboration from table metadata.** Row counts in
   `dt=2026-10-04` at the quiesce were Iceberg 4,180,412,213 vs Paimon
   3,217,389,927 — a ratio of **1.2993**, against the live-metric ratio of
   **1.2930**. Two unrelated instruments, 0.5% apart.
4. **It is not a row-size artifact.** On-disk bytes per row are 3,367 B
   (Iceberg) vs 3,452 B (Paimon) — Paimon's rows are 2.5% *larger*, so Iceberg
   is not winning by writing less data per row. It is writing more rows.

## This is not a checkpoint-commit problem

The obvious hypothesis — Paimon's commit is heavier — is wrong, and the
checkpoint metrics say so plainly:

| | Paimon | Iceberg |
|---|---|---|
| checkpoint duration, median | 4,459 ms | 2,785 ms |
| checkpoint duration, max | 5,531 ms | 5,798 ms |
| checkpoint state size | 867 KiB | 3 KiB |

Paimon's checkpoints *are* 1.6× longer and its state is 289× larger (it carries
per-bucket writer state; Iceberg's committer carries almost nothing). But:

- Checkpoint interval is ~60 s and the duration is 4.5 s, so commit occupies
  **~7% of wall-clock** — it cannot explain a 22.7% deficit.
- **Max durations are the same** (5.5 vs 5.8 s). If commit were the constraint,
  Paimon's tail would be worse, not equal.
- Previously measured: the committers are **100% idle at parallelism 1** on both
  jobs. Commit is categorically not the bottleneck.
- **Zero compactions have ever run** on the Paimon table
  (`compactionTotalCount=0` across all 160 writer subtasks;
  `compaction.min.file-num=5` was never reached). Compaction is not the cost
  either — an explanation I gave earlier and which the metrics refute.

## The cost is the bucketed hash shuffle, and the cluster is CPU-bound

Both jobs are pinned against their CPU limit, not against S3:

| | Paimon | Iceberg |
|---|---|---|
| TaskManager CPU (vs `limits.cpu: 4`) | 3,910m | 3,995m |
| writer `busyTimeMsPerSecond` | 731 | 644 |
| source `backPressuredTimeMsPerSecond` | 847 | 851 |
| **CPU-ms per row** (derived) | **1.1275** | **0.8910** |

Paimon burns **1.27× more CPU per row**. Since both jobs have the same CPU
budget and that budget is saturated, CPU per row *is* the throughput ratio —
1.27 derived vs 1.29 measured. The deficit is fully accounted for by CPU cost,
with no residual needing an I/O explanation.

Where the extra CPU goes: Paimon is configured `bucket=160` with
`bucket-key=uuid`, which forces a **hash shuffle** before the writer, plus an
extra bucket-assigner Map vertex (previously measured at 16.5% of its chain
CPU). Iceberg runs `write.distribution-mode=none` — no shuffle at all. The
network counters show it unambiguously:

| writer input | Paimon | Iceberg |
|---|---|---|
| remote | **2.42 GB/s** | 0.00 |
| local | 0.06 GB/s | **3.21 GB/s** |

Paimon ships essentially every row across the network to reach its bucket owner;
Iceberg hands every row to a writer in the same slot. Note that `busy_ms` is
*lower* for Paimon (731 vs 644 is only 1.14×) — because the shuffle's cost is
partly serialization and network wait attributed outside the writer operator.
The CPU-per-row figure captures it; the per-operator busy time does not.

The 847–851 ms/s source backpressure on **both** arms is the same number, which
is the signature of a saturated sink in both cases rather than a Paimon-specific
pathology. Both jobs are running as fast as their write path permits.

## Caveat on an earlier claim

An apparent "throughput increase after 40 minutes of running" reported earlier
was a **cumulative-averaging artifact**, not a real effect. There is a 203.4 s
dead gap between job `RUNNING` and first vertex deploy (Karpenter node
provisioning). The model `cum_avg(T) = S × (1 − 203.4/T)` with S = 547k predicts
all four observations to within 0.3%. Recomputing against *vertex* uptime
removes the growth entirely (548,415 → 547,525 rows/s). The jobs never sped up.

## This is a write-path result, and the read path is nearly at parity

The 1.293× above is a **write** number. It is worth stating plainly what the
read side looks like, because the two are easy to conflate and the gap is an
order of magnitude different.

Read cost was re-measured on the same `dt=2026-10-04` partition with the
snapshot suite's `uuid` modulo sample — `CONV(SUBSTRING(uuid,1,8),16,10) % N = 0`,
an unbiased 1/N sample that **cannot prune**, so every arm reads its full
partition and filters after the read. Serial, on an idle cluster, at N=1000:

| arm | B/row | ns/row |
|---|---|---|
| paimon | 38.02 | 443.4 |
| pmn-as-ice | 37.37 | 434.6 |
| iceberg | 42.19 | 470.4 |

Iceberg costs **1.11× Paimon's bytes/row** and **1.06× its CPU/row** — against
**1.293×** on the write path. The format gap in this benchmark is overwhelmingly
a write-path story.

Two cautions on those read figures. Earlier *concurrent* runs suggested a 6.5×
bytes gap; **that figure is retracted** — contention and differing pruning made
the arms non-comparable, and only serial full scans are trustworthy here. And
three concurrent full-partition scans exhaust the BE memory pool outright
(`Memory of default_wg exceed limit … Used: 18786279880, Limit: 18786186952`),
so the arms must run serially, which the harness enforces.

Note also that the row counts are **not** equal across arms, by construction: a
1/N sample of a larger table returns proportionally more rows, and Iceberg holds
1.2993× Paimon's rows here *because of* the write-path deficit documented above.
Raw elapsed is therefore not comparable across arms — divide by each arm's
`COUNT(*)` for cost per row, which is what the table does.

## Conclusion

At parallelism 160 on 4-CPU TaskManagers writing ~4 KiB rows to S3, Iceberg
delivers **1.29× Paimon's write throughput** (717k vs 555k rows/s; 2.74 vs
2.12 GiB/s). The entire gap is explained by CPU cost per row (1.27×), and that
cost is Paimon's `bucket-key` hash shuffle — not commit, not compaction, and not
S3.

The actionable implication is that **Paimon's deficit here is a configuration
choice, not a format property.** `bucket=160` + `bucket-key=uuid` buys
bucket-level upsert semantics that this append-only benchmark never uses, and
charges a full network shuffle for it. A Paimon table configured for append-only
writes (bucket=-1 / dynamic bucket, or a bucket key aligned with the existing
partitioning) should close most of the gap. That hypothesis is untested here and
is the obvious next experiment.
