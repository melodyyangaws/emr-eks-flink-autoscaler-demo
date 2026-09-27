# Flink Job Monitor Benchmark — 2026-09-27

Ingest-side benchmark for the Paimon and Iceberg CDC pipelines, from the
in-cluster monitor sidecar. Companion to `SUMMARY-20260927.md` (which covers the
StarRocks query side).

**Source data:** `monitor-20260927b/` — 20 JSON reports (10 per pipeline), one per
~5 min interval, 11:26–12:09 UTC.
**Earlier partial copy:** `monitor-20260927/` (12 files) — superseded.

**Run configuration:** parallelism 64 (Paimon 1608 tasks / Iceberg 1032 tasks,
16 TMs each) · RDS `db.r6i.16xlarge`, io2 256,000 IOPS · generator 10 pods ×
`RATE_PER_POD=50000`.

---

## Per-interval measurements

### Paimon

| Interval (UTC) | rec/s | records | cp_avg | cp_n | data files | delete files | pos del | eq del |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 11:26:14 | 0 | 0 | — | 0 | 0 | 0 | 0 | 0 |
| 11:30:39 | 11,089,319 | 887,950,953 | 50,112 ms | 1 | 2,816 | 0 | 0 | 0 |
| 11:35:12 | 10,998,766 | 1,102,084,675 | 41,459 ms | 1 | 3,943 | 0 | 0 | 0 |
| 11:39:47 | 576,069 | 57,728,122 | 28,145 ms | 2 | 72 | 0 | 0 | 0 |
| 11:44:22 | 29,772 | 2,983,230 | 951 ms | 0 | 128 | 0 | 0 | 0 |
| 11:49:01 | 1,088,229 | 109,168,068 | 14,754 ms | 1 | 435 | 0 | 0 | 0 |
| 11:53:46 | 1,035,626 | 103,600,271 | 12,323 ms | 1 | 644 | 0 | 0 | 0 |
| 11:58:33 | 1,002,036 | 100,110,023 | 12,330 ms | 1 | 1,417 | 0 | 0 | 0 |
| 12:03:20 | 1,032,057 | 103,227,526 | 13,360 ms | 1 | 4,050 | 0 | 0 | 0 |
| 12:08:08 | 983,600 | 98,396,194 | 14,425 ms | 1 | 2,288 | 0 | 0 | 0 |

### Iceberg

| Interval (UTC) | rec/s | records | cp_avg | cp_n | data files | delete files | pos del | eq del |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 11:27:56 | 4,890,155 | 488,205,323 | — | 0 | 0 | 0 | 0 | 0 |
| 11:32:21 | 1,307,380 | 130,889,822 | 17,038 ms | 2 | 34 | 24 | 0 | 62,043,415 |
| 11:36:54 | 1,708,307 | 171,058,198 | 15,921 ms | 2 | 39 | 23 | 0 | 81,406,684 |
| 11:41:29 | 1,556,852 | 155,918,864 | 14,506 ms | 2 | 36 | 21 | 0 | 74,594,306 |
| 11:46:04 | 1,550,512 | 155,276,308 | 9,292 ms | 2 | 179 | 310 | 50,836 | 78,011,308 |
| 11:50:43 | 1,785,598 | 178,813,940 | 10,167 ms | 2 | 176 | 307 | 26,151 | 77,422,061 |
| 11:55:28 | 1,327,402 | 132,934,547 | 31,096 ms | 1 | 87 | 152 | 14,050 | 38,779,102 |
| 12:00:15 | 1,031,341 | 103,243,661 | 14,847 ms | 2 | 167 | 302 | 23,032 | 57,266,575 |
| 12:05:02 | 92,235 | 9,234,928 | 535 ms | 1 | 78 | 151 | 9,442 | 1,563,271 |
| 12:09:50 | 89,178 | 8,929,557 | 512 ms | 1 | 80 | 153 | 11,775 | 1,477,762 |

## Aggregates

| Metric | Paimon | Iceberg |
|---|---:|---:|
| Median rec/s | 1,032,057 | 1,438,957 |
| Peak rec/s | 11,089,319 | 4,890,155 |
| Peak interval records | 1,102,084,675 | 488,205,323 |
| Median checkpoint duration | 14,425 ms | 14,506 ms |
| Total new data files | 15,793 | 876 |
| **Total new delete files** | **0** | **1,443** |
| Total position deletes | 0 | 135,286 |
| **Total equality deletes** | **0** | **472,564,484** |

---

## Findings

### 1. Iceberg wrote 472.6 M equality deletes; Paimon wrote zero

The single most important number here. Every equality delete must be merged at
read time by Iceberg's merge-on-read, which is the direct cause of the query gap
measured in `SUMMARY-20260927.md` (up to 48× slower). Paimon's LSM compaction
collapses updates in the background instead, so readers see already-merged state.

Paimon's delete-file columns are **zero in all 10 intervals** — not small, zero.

### 2. Paimon's compaction is directly observable

Some intervals report **negative** `data_files_delta` — files being collapsed
rather than added (e.g. at 12:08:08: `products -575`, `bytes_delta
-302,184,329`; earlier `products -1152`, `orders -49`). This is LSM compaction
reclaiming space mid-run, and it is why Paimon writes 18× more data files
(15,793 vs 876) without the file count running away.

### 3. Iceberg's higher median rec/s is not an advantage

Iceberg's median (1.44 M/s) exceeds Paimon's (1.03 M/s), but it processed **less
than half** the peak records (488 M vs 1.10 B). The higher median reflects
burstier windows against a smaller total, plus commit stalls that concentrate
work — not faster ingest. The peak tells the opposite story: Paimon 11.1 M/s vs
Iceberg 4.9 M/s, 2.3×.

### 4. Snapshot → tail transition is visible at 11:39

Both pipelines drop by an order of magnitude mid-run: Paimon 10,998,766 → 576,069
→ 29,772, Iceberg later 1,031,341 → 92,235. This is **not degradation** — it is
the end of the initial snapshot read and the start of binlog-tail following,
where a CDC job's throughput *is* the source DB's write rate. The multi-million
rec/s figures are bulk snapshot reads of the ~500 M-row backlog and must never be
quoted as steady-state CDC throughput.

Confirming: Paimon's `order_items` source emitted 543,995,553 records against a
table of 510,830,608 rows — it passed the full table.

### 5. Committer asymmetry is the Iceberg ingest bottleneck

Measured in the same deployment: `accumulated-backpressured-time` for
`order_items` was **110,772,534 ms (Iceberg)** vs **16,647,810 ms (Paimon)** —
6.7×; for `orders`, 97,167,754 vs 18,693,855. Sources sat at `busy=0.0` while
`IcebergStreamWriter` burned ~1.6 M ms, and **every `IcebergFilesCommitter` runs
at parallelism 1** behind 64 parallel writers. The result is a persistent data
lag: Iceberg trailed Paimon by 53.7 M rows on `orders` and 166.4 M on
`order_items`.

### 6. Checkpoint duration is not the differentiator

Medians are within 1% (14,425 vs 14,506 ms). Iceberg's cost is in the commit
path's serialization, not checkpoint wall-time. Early 50,112 / 41,459 ms values
are snapshot-phase artifacts.

---

## Caveats

- **`cp_n` is per-interval, not cumulative**, and low counts (0–2) mean interval
  aggregates are coarse. The separate cumulative `cp_fail` counter carries stale
  coordinator-level counts and is **not** authoritative — only the checkpoint
  `history` array is. A frozen `cp_fail` alongside a climbing `cp_ok` means the
  failures are historical.
- The `"Checkpoint Coordinator is suspending"` failure on `id=1` appears on both
  pipelines and is a **startup artifact** from TaskManager churn while Karpenter
  is still adding nodes — not a fault.
- **Absolute Iceberg file counts are distorted.** An earlier
  `iceberg-warehouse/` wipe was declined, leaving ~67,240 orphaned objects. The
  run did start clean (Iceberg's current-metadata pointer lives in Glue table
  properties, and those tables were dropped), so the **per-interval deltas above
  are unaffected** — but any total storage-size or file-count comparison against
  that prefix is not trustworthy.
- These 20 intervals span a period that included the 40-pod lock-contention
  window; the generator was scaled 40 → 20 → 10 during the run, which is part of
  why interval throughput varies.
- Iceberg here is **un-maintained** — no `rewrite_data_files` or
  `expire_snapshots` was run. That is realistic for streaming CDC but should be
  separated from Iceberg's maintained performance.
