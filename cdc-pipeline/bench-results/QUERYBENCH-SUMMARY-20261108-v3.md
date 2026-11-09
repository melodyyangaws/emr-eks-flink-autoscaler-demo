# CDC query benchmark: 20261108-v3

Warm DuckDB vs Trino on the four CDC tables Flink writes, on native Iceberg and on Paimon-as-iceberg and on native Paimon (cdc-pipeline/querybench/); both engines of a format read the same pinned snapshot per table.

**Run configuration**

| | |
|---|---|
| Flink writers | Paimon and Iceberg v3 jobs alike: parallelism 12 -> 6 TMs × 2 slots (8 CPU / 32Gi each TM), checkpoint every 60s |
| Source | RDS mysql 8.0.45, `db.r6i.4xlarge`, gp3 64000 IOPS / 500 GB |
| Generator | `mysql-upsert-loadgen`, 24 pods × `RATE_PER_POD=80000` (target 1,920,000 rows/s); `RECENT_KEY_RATIO=0.3` & `UPSERT_RATIO=0.9`: settings to produce records with 30% of recent re-writes, 63% older-key updates, 7% inserts; actual about **52,397 rows/s** across the 4 tables (the CDC sources' read rate, median over the ingest window: Paimon 52,754, Iceberg 52,040, no backpressure) |
| Engines | DuckDB 2.0.0 (81 CPU / 342Gi, on one `m5.24xlarge`), warm; Trino 484: 9 workers × 9 CPU / 38 Gi (total **81 CPU / 342 Gi**), one `m5.4xlarge` each; coordinator 4 CPU / 16Gi on `m5.2xlarge` (schedules no splits) |
| Table set | 2 databases with a session suffix v3 (4 tables in each DB): `flink_iceberg_db_v3` / `flink_paimon_db_v3`; Paimon has iceberg integration turned on by default, so downstream tools can either use Iceberg connector to query Paimon's Iceberg metadata or use Paimon connector to query Paimon's native metadata; and natively as `paimon_native` (DuckDB paimon_scan, Trino paimon connector) at the same Paimon snapshot |
| Iceberg | table format v3; Flink writer library Apache Iceberg 1.12.0 (`iceberg-flink-runtime-1.20`, replacing EMR's bundled 1.10.1) |
| Hot run | 9 queries, about 35 min of querying; data window 12:26:30 to 12:27:00, queried 4 times (warm-up + 3 timed), each on tables re-pinned at the start of the pass (commits 12:32:45Z to 12:56:02Z) |
| Snapshot run | 7 queries, about 24 min of querying; 1 pin set (after the writers stopped), the pinned snapshots committed 13:23:53Z to 13:47:06Z |
| Ingest window | 2026-11-08T12:27:45Z to 2026-11-08T12:56:02Z |

Known write-side differences (table settings are otherwise paired; querybench/README.md): Paimon writes orders/order_items with one writer per bucket (16 per date partition), Iceberg's hash distribution sends each date partition to ONE writer; Paimon compacts and expires snapshots inline; Iceberg is not compacted during ingest; both get one full compaction (compaction/run.sh, run by the hot test's watcher) before the snapshot test.

---

## 1. Headline

- **What was tested:** the same stream of online-shop orders was copied, as it happened, into two storage formats (Iceberg and Paimon), and the same questions were then asked of each with two query tools (DuckDB on one large server, Trino on a cluster of the same total size).
- **Questions about live, still-changing data (hot query):** with DuckDB, Paimon was about 5 times faster than Iceberg; with Trino, Paimon was about 7 times faster than Iceberg. Paimon merges each change into its files while loading (ongoing compaction); Iceberg leaves that merging for later, so every question has to sort through the latest changes first.
- **Questions about settled data (snapshot test: loading stopped and both formats were fully compacted, i.e. every pending change merged into clean files):** with DuckDB, Iceberg was about 13% faster than Paimon; with Trino, Paimon and Iceberg were about equally fast. After full compaction, the two formats are close.
- **The query tool matters too:** DuckDB answered faster than Trino almost everywhere; the biggest gap was on Iceberg settled data on the very first question after a restart, where Trino took about 2.1 times as long.
- **Loading the data:** both formats kept up with the incoming orders, at about 52,000 changes a second each.
- **Why live questions are slower on Iceberg:** for every changed order, Iceberg writes the new version plus a note saying "ignore the old one" (42 million such notes in this test). Until a compaction merges them away, every question has to read those notes. Paimon replaces old versions as it goes, so it has none.

<details><summary>The same results, technical version</summary>

- On the Iceberg tables, Trino took **1.74×** warm DuckDB's time (hot suite, geometric mean of 8 queries).
- On Paimon-as-iceberg, Trino took **1.31×** warm DuckDB's time (hot suite, geometric mean of 8 queries).
- On native Paimon, Trino took **0.26×** warm DuckDB's time (hot suite, geometric mean of 8 queries).
- On Trino, Paimon-as-iceberg answered in **0.14×** Iceberg's time (8 queries); its rows differed from Iceberg's on 7 of 9 queries (pinned at the same moment, not the same binlog point).
- On Trino, native Paimon answered in **0.82×** Iceberg's time (8 queries); its rows differed from Iceberg's on 7 of 9 queries (pinned at the same moment, not the same binlog point).
- On Trino, native Paimon answered in **5.73×** Paimon-as-iceberg's time (8 queries, the same Paimon snapshots); rows differed on 0 of 9 queries.
- On DuckDB, Paimon-as-iceberg answered in **0.19×** Iceberg's time (8 queries); its rows differed from Iceberg's on 7 of 9 queries (pinned at the same moment, not the same binlog point).
- On DuckDB, native Paimon answered in **5.60×** Iceberg's time (8 queries); its rows differed from Iceberg's on 7 of 9 queries (pinned at the same moment, not the same binlog point).
- On DuckDB, native Paimon answered in **29.41×** Paimon-as-iceberg's time (8 queries, the same Paimon snapshots); rows differed on 0 of 9 queries.
- On the Iceberg tables, Trino took **1.24×** warm DuckDB's time (snapshot suite, geometric mean of 7 queries).
- On Paimon-as-iceberg, Trino took **1.13×** warm DuckDB's time (snapshot suite, geometric mean of 7 queries).
- On native Paimon, Trino took **0.26×** warm DuckDB's time (snapshot suite, geometric mean of 7 queries).
- On Trino, Paimon-as-iceberg answered in **1.04×** Iceberg's time (7 queries); its rows differed from Iceberg's on 7 of 7 queries (pinned at the same moment, not the same binlog point).
- On Trino, native Paimon answered in **4.46×** Iceberg's time (7 queries); its rows differed from Iceberg's on 7 of 7 queries (pinned at the same moment, not the same binlog point).
- On Trino, native Paimon answered in **4.31×** Paimon-as-iceberg's time (7 queries, the same Paimon snapshots); rows differed on 0 of 7 queries.
- On DuckDB, Paimon-as-iceberg answered in **1.13×** Iceberg's time (7 queries); its rows differed from Iceberg's on 7 of 7 queries (pinned at the same moment, not the same binlog point).
- On DuckDB, native Paimon answered in **21.27×** Iceberg's time (7 queries); its rows differed from Iceberg's on 7 of 7 queries (pinned at the same moment, not the same binlog point).
- On DuckDB, native Paimon answered in **18.80×** Paimon-as-iceberg's time (7 queries, the same Paimon snapshots); rows differed on 0 of 7 queries.
- Cold start (snapshot suite, first pass after the caches were emptied): on the Iceberg tables, Trino took **2.09×** DuckDB's time (7 queries, one sample each).
- Cold start (snapshot suite, first pass after the caches were emptied): on Paimon-as-iceberg, Trino took **1.18×** DuckDB's time (7 queries, one sample each).
- Cold start (snapshot suite, first pass after the caches were emptied): on native Paimon, Trino took **0.35×** DuckDB's time (7 queries, one sample each).
- Cold start (snapshot suite): on Trino, Paimon-as-iceberg answered in **0.64×** Iceberg's time (7 queries, one sample each).
- Cold start (snapshot suite): on Trino, native Paimon answered in **2.02×** Iceberg's time (7 queries, one sample each).
- Cold start (snapshot suite): on Trino, native Paimon answered in **3.18×** Paimon-as-iceberg's time (7 queries, one sample each).
- Cold start (snapshot suite): on DuckDB, Paimon-as-iceberg answered in **1.12×** Iceberg's time (7 queries, one sample each).
- Cold start (snapshot suite): on DuckDB, native Paimon answered in **12.16×** Iceberg's time (7 queries, one sample each).
- Cold start (snapshot suite): on DuckDB, native Paimon answered in **10.82×** Paimon-as-iceberg's time (7 queries, one sample each).
- Ingest throughput (job monitor, median of 15 / 15 reports): Paimon **52,754 rows/s, 0.8 MB/s**; Iceberg **52,040 rows/s, 0.7 MB/s** (rows/s = the CDC sources' rate meter, the ingest rate; bytes/s = committed).
- Delete mix: Iceberg wrote 54.4 M deletes for 53.0 M committed rows, 22% of them position deletes (DVs) and 42.4 M equality deletes; Paimon wrote no equality deletes.

</details>

## 2. Query latency

### Compaction before the snapshot test (run 20261108T131643Z)

`compaction/run.sh`, writers stopped: Paimon `sys.compact` (full); Iceberg data files with deletes rewritten with them applied and the delete files removed, then snapshots expired (12 h, keep 5). One row per table: its newest run.

| format | table | status | duration (s) | data files (files) | GiB | rows in files (rows) | delete files (files) | eq deletes (rows) | pos deletes (rows) |
|---|---|---|---:|---|---|---|---|---:|---:|
| iceberg | customers | OK | 810.9 | 948 -> 60 | 1.50 -> 1.43 | 43,726,105 -> 41,877,746 | 1,788 -> 0 | 35,961,782 | 403,937 |
| iceberg | order_items | OK | 268.7 | 394 -> 107 | 4.96 -> 4.08 | 341,345,950 -> 287,708,611 | 544 -> 0 | 100,513,208 | 12,651,269 |
| iceberg | orders | OK | 387.6 | 391 -> 94 | 4.50 -> 3.91 | 269,008,012 -> 234,603,935 | 546 -> 0 | 109,452,813 | 4,700,578 |
| iceberg | products | OK | 145.3 | 752 -> 135 | 2.97 -> 2.68 | 121,462,752 -> 109,791,802 | 1,228 -> 0 | 111,842,131 | 3,996,923 |
| paimon | customers | OK | 281.9 | 101 -> 56 | 1.65 -> 1.62 | 42,916,710 -> 41,877,746 | 0 -> 0 | 0 | 0 |
| paimon | order_items | OK | 153.3 | 558 -> 128 | 5.35 -> 5.00 | 308,177,026 -> 287,708,525 | 0 -> 0 | 0 | 0 |
| paimon | orders | OK | 146.1 | 534 -> 128 | 4.93 -> 4.68 | 248,384,295 -> 234,603,878 | 0 -> 0 | 0 | 0 |
| paimon | products | OK | 255.5 | 469 -> 144 | 3.44 -> 3.34 | 114,532,440 -> 109,791,852 | 0 -> 0 | 0 | 0 |

### hot suite (run 20261108T115501Z)

Median of the timed passes, seconds (DuckDB: execute + fetch in the server; Trino: elapsed - queued). Ratio > 1 = Trino slower.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | DuckDB paimon native | Trino paimon native | Trino / DuckDB, iceberg | Trino / DuckDB, paimon-as-iceberg | Trino / DuckDB, paimon native |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| H1_freshness | 3.11 | 4.48 | 0.93 | 0.54 | 10.82 | 3.45 | **1.44** | **0.58** | **0.32** |
| H2_hot_count | 1.92 | 4.16 | 0.17 | 0.34 | 6.11 | 0.83 | **2.17** | **1.96** | **0.14** |
| H3_hot_needle | 1.88 | 4.37 | 0.32 | 0.30 | 14.29 | 1.28 | **2.33** | **0.94** | **0.09** |
| H4_hot_narrow_agg | 1.25 | 2.33 | 0.38 | 0.47 | 5.88 | 5.08 | **1.86** | **1.22** | **0.86** |
| H5_hot_wide_row | 1.17 | 2.25 | 0.51 | 0.52 | 26.79 | 11.35 | **1.93** | **1.02** | **0.42** |
| H6_hot_product_agg | 2.19 | 4.50 | 0.38 | 0.47 | 16.43 | 1.81 | **2.05** | **1.25** | **0.11** |
| H7_hot_status_category_rank | 9.10 | 10.11 | 1.21 | 2.93 | 30.90 | 9.41 | **1.11** | **2.41** | **0.30** |
| H8_hot_pk_lookup | ZERO_SCAN | ZERO_SCAN | ZERO_SCAN | ZERO_SCAN | ZERO_SCAN | ZERO_SCAN | - | - | - |
| H9_hot_join | 3.12 | 4.45 | 0.39 | 0.85 | 13.17 | 5.02 | **1.42** | **2.20** | **0.38** |

Not OK (recorded, never compared): DuckDB iceberg: ZERO_SCAN on 1 of 9 queries; Trino iceberg: ZERO_SCAN on 1 of 9 queries; DuckDB paimon-as-iceberg: ZERO_SCAN on 1 of 9 queries; Trino paimon-as-iceberg: ZERO_SCAN on 1 of 9 queries; DuckDB paimon native: ZERO_SCAN on 1 of 9 queries; Trino paimon native: ZERO_SCAN on 1 of 9 queries.

Geometric mean of time A / time B over the queries both ran OK (A is the first one named in the "vs" column; > 1 = A slower):

| engine | format | A / B | queries |
|---|---|---:|---:|
| Trino vs DuckDB | iceberg | **1.74** | 8 |
| Trino vs DuckDB | paimon-as-iceberg | **1.31** | 8 |
| Trino vs DuckDB | paimon native | **0.26** | 8 |
| DuckDB | paimon-as-iceberg vs iceberg | **0.19** | 8 |
| DuckDB | paimon native vs iceberg | **5.60** | 8 |
| DuckDB | paimon native vs paimon-as-iceberg | **29.41** | 8 |
| Trino | paimon-as-iceberg vs iceberg | **0.14** | 8 |
| Trino | paimon native vs iceberg | **0.82** | 8 |
| Trino | paimon native vs paimon-as-iceberg | **5.73** | 8 |

### snapshot suite (run 20261108T134858Z)

Median of the timed passes, seconds (DuckDB: execute + fetch in the server; Trino: elapsed - queued). Ratio > 1 = Trino slower.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | DuckDB paimon native | Trino paimon native | Trino / DuckDB, iceberg | Trino / DuckDB, paimon-as-iceberg | Trino / DuckDB, paimon native |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| S1_order_items_count | 0.20 | 0.23 | 0.28 | 0.22 | 6.92 | 1.57 | **1.20** | **0.80** | **0.23** |
| S2_items_narrow_agg | 0.32 | 0.38 | 0.39 | 0.41 | 7.89 | 3.21 | **1.19** | **1.04** | **0.41** |
| S3_orders_distinct_customers | 0.61 | 0.98 | 0.61 | 1.16 | 21.33 | 4.01 | **1.61** | **1.90** | **0.19** |
| S4_orders_wide_slice | 0.69 | 0.63 | 0.77 | 0.65 | 12.13 | 7.04 | **0.91** | **0.85** | **0.58** |
| S5_star_join_revenue | 4.61 | 5.70 | 4.99 | 5.32 | 37.77 | 11.17 | **1.24** | **1.07** | **0.30** |
| S6_topn_customers | 0.84 | 0.58 | 0.94 | 0.68 | 16.42 | 3.37 | **0.69** | **0.72** | **0.21** |
| S7_dedup_keys | 2.56 | 6.34 | 2.51 | 5.97 | 58.35 | 7.66 | **2.47** | **2.38** | **0.13** |

Geometric mean of time A / time B over the queries both ran OK (A is the first one named in the "vs" column; > 1 = A slower):

| engine | format | A / B | queries |
|---|---|---:|---:|
| Trino vs DuckDB | iceberg | **1.24** | 7 |
| Trino vs DuckDB | paimon-as-iceberg | **1.13** | 7 |
| Trino vs DuckDB | paimon native | **0.26** | 7 |
| DuckDB | paimon-as-iceberg vs iceberg | **1.13** | 7 |
| DuckDB | paimon native vs iceberg | **21.27** | 7 |
| DuckDB | paimon native vs paimon-as-iceberg | **18.80** | 7 |
| Trino | paimon-as-iceberg vs iceberg | **1.04** | 7 |
| Trino | paimon native vs iceberg | **4.46** | 7 |
| Trino | paimon native vs paimon-as-iceberg | **4.31** | 7 |

#### snapshot suite, cold start (first pass after both engines' caches were emptied)

One timed sample per query and arm (not a median), so noisier than the warm table above. Seconds; ratios > 1 = the first named slower.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | DuckDB paimon native | Trino paimon native | Trino / DuckDB, iceberg | Trino / DuckDB, paimon-as-iceberg | Trino / DuckDB, paimon native | Paimon-as-iceberg / Iceberg, DuckDB | Paimon-as-iceberg / Iceberg, Trino | Paimon native / Iceberg, DuckDB | Paimon native / Iceberg, Trino | Paimon native / Paimon-as-iceberg, DuckDB | Paimon native / Paimon-as-iceberg, Trino |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| S1_order_items_count | 0.94 | 3.72 | 0.97 | 0.89 | 6.87 | 7.66 | **3.95** | **0.92** | **1.11** | **1.03** | **0.24** | **7.30** | **2.06** | **7.10** | **8.64** |
| S2_items_narrow_agg | 0.57 | 1.13 | 0.68 | 0.81 | 8.25 | 3.14 | **1.98** | **1.19** | **0.38** | **1.19** | **0.72** | **14.51** | **2.79** | **12.17** | **3.88** |
| S3_orders_distinct_customers | 1.00 | 2.53 | 1.17 | 2.02 | 21.08 | 4.78 | **2.53** | **1.72** | **0.23** | **1.17** | **0.80** | **21.06** | **1.89** | **17.96** | **2.37** |
| S4_orders_wide_slice | 1.38 | 1.82 | 1.43 | 1.85 | 11.86 | 7.50 | **1.32** | **1.29** | **0.63** | **1.04** | **1.02** | **8.58** | **4.12** | **8.28** | **4.05** |
| S5_star_join_revenue | 6.16 | 10.84 | 6.76 | 6.24 | 37.34 | 13.05 | **1.76** | **0.92** | **0.35** | **1.10** | **0.58** | **6.06** | **1.20** | **5.52** | **2.09** |
| S6_topn_customers | 0.95 | 1.17 | 1.17 | 0.86 | 15.84 | 3.46 | **1.24** | **0.73** | **0.22** | **1.23** | **0.73** | **16.73** | **2.95** | **13.57** | **4.04** |
| S7_dedup_keys | 2.91 | 8.86 | 3.27 | 6.31 | 58.99 | 7.58 | **3.04** | **1.93** | **0.13** | **1.12** | **0.71** | **20.25** | **0.86** | **18.02** | **1.20** |

Cold start, geometric mean of time A / time B over the queries both ran OK:

| engine | format | A / B | queries |
|---|---|---:|---:|
| Trino vs DuckDB | iceberg | **2.09** | 7 |
| Trino vs DuckDB | paimon-as-iceberg | **1.18** | 7 |
| Trino vs DuckDB | paimon native | **0.35** | 7 |
| DuckDB | paimon-as-iceberg vs iceberg | **1.12** | 7 |
| DuckDB | paimon native vs iceberg | **12.16** | 7 |
| DuckDB | paimon native vs paimon-as-iceberg | **10.82** | 7 |
| Trino | paimon-as-iceberg vs iceberg | **0.64** | 7 |
| Trino | paimon native vs iceberg | **2.02** | 7 |
| Trino | paimon native vs paimon-as-iceberg | **3.18** | 7 |

## 3. Ingest

Window 2026-11-08T12:27:45Z to 2026-11-08T12:56:02Z (1,697 s; hot: first pin commit - 300 s .. last pin commit), from each table's own commit history: Iceberg snapshot summaries in the current metadata.json, Paimon `snapshot-N` files (`querybench/ingest_stats.py`). Write commits only; compaction commits are counted apart.

**Throughput** (job-monitor reports overlapping the window: median; in brackets, total rows ingested for rows/s, max for MB/s)

| | Paimon | Iceberg |
|---|---:|---:|
| Rows/s (total rows) | **52,754** (84,527,993) | **52,040** (79,771,085) |
| Data size/s, MB/s | **0.8** (2.6) | **0.7** (1.7) |
| Checkpoint duration, median | 9,483 ms | 472 ms |
| Backpressure HIGH, max share of samples | 0% | 0% |

Rows/s is the ingest rate: the CDC sources' rate meter (`numRecordsOutPerSecond`). Both jobs read the same MySQL change stream, so equal rows/s means both kept up; a lower one means backpressure from its writers. What each format committed (Paimon merges re-written keys first, Iceberg writes every version plus a delete) is in the table-metadata section below.

**From table metadata** (each table's commit history over the window)

| | Paimon | Iceberg |
|---|---:|---:|
| Rows/s (window average) | **27,974** | **31,224** |
| Data size/s (new files) | 0.7 MB/s | 0.7 MB/s |
| Rows committed | 47.5 M | 53.0 M |
| Write commits (median interval) | 116 (60 s) | 116 (60 s) |
| New data files | 6,960 | 899 |
| **New delete files** | 6,960 DV index files, by compaction | **1,798** (899 DVs + 899 equality-delete files) |
| **Equality deletes** | **0** | **42.4 M** |
| Position deletes | none written; compaction marks rows in DVs | 12.1 M (22% of all deletes, as DVs) |
| Compaction commits | 116 (inline) | 0 (compaction job) |

- Paimon committed **90%** of Iceberg's rows: the other 10% were re-writes of a key it merged before committing. Iceberg commits each of them and a delete for the version it replaces.
- Iceberg wrote 54.4 M deletes for 53.0 M committed rows (103% of rows); **22%** were position deletes (DVs: the old row was in the same checkpoint's files), the rest equality deletes, which every read must join by key until a compaction.
- Paimon's DV index files are rewritten per bucket by each inline compaction (116 commits); the live ones at the window end are in 4.
- Source RDS CPU over the window: 21.9% average, 39.4% max.

## 4. Table state

At the window end (the last commit at or before 2026-11-08T12:56:02Z). Both record counts are before deletes are applied: Iceberg `total-records`, Paimon `totalRecordCount` of the live files.

| table | Paimon records | Paimon files / DV-deleted rows | Iceberg records | Iceberg data / delete files | Iceberg eq / pos deletes | snapshots P / I |
|---|---:|---:|---:|---:|---:|---:|
| customers | 42.7 M | 105 / 860,804 | 43.3 M | 720 / 1,344 | 43.0 M / 340,293 | 154 / 78 |
| products | 112.0 M | 438 / 3.8 M | 117.6 M | 581 / 963 | 114.0 M / 3.6 M | 154 / 78 |
| orders | 242.8 M | 461 / 11.1 M | 258.2 M | 296 / 447 | 250.6 M / 8.7 M | 154 / 78 |
| order_items | 300.0 M | 449 / 15.6 M | 326.4 M | 299 / 448 | 312.7 M / 15.4 M | 155 / 78 |

Iceberg holds 1.1× Paimon's records: its superseded rows stay in the data files until a compaction rewrites them; Paimon's compaction drops them as it goes.

## Conclusions

**Bottom line**

- **Hot queries (live tables, merge-on-read): Paimon-as-iceberg fits this use case.** It answered in 0.19× Iceberg's time on DuckDB and 0.14× on Trino (5.2× / 7.0× faster): Paimon's inline compaction keeps the files it exposes merged, while Iceberg applies every equality delete written since its last compaction at read time.
- **Snapshot queries (compacted tables): Iceberg fits this use case better with DuckDB as the engine.** Paimon-as-iceberg took 1.13× Iceberg's time warm and 1.12× cold on DuckDB.
- On Trino the snapshot comparison agrees: Paimon-as-iceberg took 1.04× Iceberg's time warm and 0.64× cold.
- **Native Paimon** (`paimon_native`, the same Paimon snapshots read by its own readers) / Iceberg: hot 5.60× on DuckDB, 0.82× on Trino; snapshot 21.27× warm (12.16× cold) on DuckDB, 4.46× warm (2.02× cold) on Trino.
- **The engine matters most in the cold snapshot (first query after the caches were emptied) pass:** there Trino took **2.1×** DuckDB's time on Iceberg (geometric mean of 7 queries), the largest engine gap in this run.

**Details**

1. Engine (iceberg, hot): Trino / DuckDB = 1.74 over 8 queries.
2. Engine (paimon, hot): Trino / DuckDB = 1.31 over 8 queries.
3. Engine (paimon_native, hot): Trino / DuckDB = 0.26 over 8 queries.
4. Format (Trino, hot): Paimon-as-iceberg / Iceberg = 0.14 over 8 queries; rows differ on 7.
5. Format (Trino, hot): native Paimon / Iceberg = 0.82 over 8 queries; rows differ on 7.
6. Reader (Trino, hot): native Paimon / Paimon-as-iceberg = 5.73 over 8 queries; rows differ on 0.
7. Format (DuckDB, hot): Paimon-as-iceberg / Iceberg = 0.19 over 8 queries; rows differ on 7.
8. Format (DuckDB, hot): native Paimon / Iceberg = 5.60 over 8 queries; rows differ on 7.
9. Reader (DuckDB, hot): native Paimon / Paimon-as-iceberg = 29.41 over 8 queries; rows differ on 0.
10. Valid (hot): on each format, both engines returned identical rows for every query both ran.
11. Engine (iceberg, snapshot): Trino / DuckDB = 1.24 over 7 queries.
12. Engine (paimon, snapshot): Trino / DuckDB = 1.13 over 7 queries.
13. Engine (paimon_native, snapshot): Trino / DuckDB = 0.26 over 7 queries.
14. Format (Trino, snapshot): Paimon-as-iceberg / Iceberg = 1.04 over 7 queries; rows differ on 7.
15. Format (Trino, snapshot): native Paimon / Iceberg = 4.46 over 7 queries; rows differ on 7.
16. Reader (Trino, snapshot): native Paimon / Paimon-as-iceberg = 4.31 over 7 queries; rows differ on 0.
17. Format (DuckDB, snapshot): Paimon-as-iceberg / Iceberg = 1.13 over 7 queries; rows differ on 7.
18. Format (DuckDB, snapshot): native Paimon / Iceberg = 21.27 over 7 queries; rows differ on 7.
19. Reader (DuckDB, snapshot): native Paimon / Paimon-as-iceberg = 18.80 over 7 queries; rows differ on 0.
20. Cold start, engine (iceberg, snapshot): Trino / DuckDB = 2.09 over 7 queries.
21. Cold start, engine (paimon, snapshot): Trino / DuckDB = 1.18 over 7 queries.
22. Cold start, engine (paimon_native, snapshot): Trino / DuckDB = 0.35 over 7 queries.
23. Cold start, format (Trino, snapshot): Paimon-as-iceberg / Iceberg = 0.64 over 7 queries.
24. Cold start, format (Trino, snapshot): native Paimon / Iceberg = 2.02 over 7 queries.
25. Cold start, format (Trino, snapshot): native Paimon / Paimon-as-iceberg = 3.18 over 7 queries.
26. Cold start, format (DuckDB, snapshot): Paimon-as-iceberg / Iceberg = 1.12 over 7 queries.
27. Cold start, format (DuckDB, snapshot): native Paimon / Iceberg = 12.16 over 7 queries.
28. Cold start, format (DuckDB, snapshot): native Paimon / Paimon-as-iceberg = 10.82 over 7 queries.
29. Valid (snapshot): on each format, both engines returned identical rows for every query both ran.
30. Ingest (job monitor): Paimon 52,754 rows/s, 0.8 MB/s; Iceberg 52,040 rows/s, 0.7 MB/s.
31. Table metadata: Iceberg 31,224 rows/s, Paimon 27,974 rows/s committed; Iceberg added 42.4 M equality deletes and 12.1 M position deletes (DVs), Paimon none (it merges by key; compaction writes DVs).

### Open items

- DuckDB iceberg: not OK on 1 of 9 hot queries.
- Trino iceberg: not OK on 1 of 9 hot queries.
- DuckDB paimon-as-iceberg: not OK on 1 of 9 hot queries.
- Trino paimon-as-iceberg: not OK on 1 of 9 hot queries.
- DuckDB paimon native: not OK on 1 of 9 hot queries.
- Trino paimon native: not OK on 1 of 9 hot queries.
- Iceberg vs Paimon-as-iceberg rows differ on 7 hot queries: the two jobs are pinned at the same moment, not the same binlog point.
- Iceberg vs native Paimon rows differ on 7 hot queries: the two jobs are pinned at the same moment, not the same binlog point.
- Iceberg vs Paimon-as-iceberg rows differ on 7 snapshot queries: the two jobs are pinned at the same moment, not the same binlog point.
- Iceberg vs native Paimon rows differ on 7 snapshot queries: the two jobs are pinned at the same moment, not the same binlog point.

---

## Details

<details><summary>hot: Trino server-side breakdown, data scanned, result equality</summary>

**Trino server-side breakdown** (median over the timed passes)

| arm | query | elapsed s | queued s | execution s | CPU s | physical input GB |
|---|---|---|---|---|---|---|
| iceberg | H1_freshness | 4.48 | 0.00 | 4.48 | 35.38 | 0.06 |
| iceberg | H2_hot_count | 4.16 | 0.00 | 4.16 | 27.17 | 0.05 |
| iceberg | H3_hot_needle | 4.37 | 0.00 | 4.37 | 27.20 | 0.08 |
| iceberg | H4_hot_narrow_agg | 2.33 | 0.00 | 2.33 | 6.50 | 0.01 |
| iceberg | H5_hot_wide_row | 2.25 | 0.00 | 2.25 | 6.59 | 0.03 |
| iceberg | H6_hot_product_agg | 4.50 | 0.00 | 4.50 | 30.08 | 0.16 |
| iceberg | H7_hot_status_category_rank | 10.11 | 0.00 | 10.11 | 275.40 | 0.82 |
| iceberg | H9_hot_join | 4.45 | 0.00 | 4.45 | 37.21 | 0.13 |
| paimon | H1_freshness | 0.54 | 0.00 | 0.54 | 3.74 | 0.11 |
| paimon | H2_hot_count | 0.34 | 0.00 | 0.34 | 0.92 | 0.05 |
| paimon | H3_hot_needle | 0.30 | 0.00 | 0.30 | 1.21 | 0.12 |
| paimon | H4_hot_narrow_agg | 0.47 | 0.00 | 0.47 | 2.07 | 0.12 |
| paimon | H5_hot_wide_row | 0.52 | 0.00 | 0.52 | 4.22 | 0.30 |
| paimon | H6_hot_product_agg | 0.47 | 0.00 | 0.47 | 3.20 | 0.20 |
| paimon | H7_hot_status_category_rank | 2.93 | 0.00 | 2.93 | 40.15 | 0.97 |
| paimon | H8_hot_pk_lookup | 0.43 | 0.00 | 0.43 | 0.21 | 0.00 |
| paimon | H9_hot_join | 0.85 | 0.00 | 0.85 | 7.64 | 0.26 |
| paimon_native | H1_freshness | 3.45 | 0.00 | 3.45 | 76.80 | 0.00 |
| paimon_native | H2_hot_count | 0.83 | 0.00 | 0.83 | 5.71 | 0.00 |
| paimon_native | H3_hot_needle | 1.28 | 0.00 | 1.28 | 9.98 | 0.00 |
| paimon_native | H4_hot_narrow_agg | 5.08 | 0.00 | 5.08 | 123.60 | 0.00 |
| paimon_native | H5_hot_wide_row | 11.35 | 0.00 | 11.35 | 327.00 | 0.00 |
| paimon_native | H6_hot_product_agg | 1.81 | 0.00 | 1.81 | 16.29 | 0.00 |
| paimon_native | H7_hot_status_category_rank | 9.41 | 0.00 | 9.41 | 422.40 | 0.00 |
| paimon_native | H8_hot_pk_lookup | 0.66 | 0.00 | 0.66 | 0.28 | 0.00 |
| paimon_native | H9_hot_join | 5.02 | 0.00 | 5.02 | 138.60 | 0.00 |

**Data scanned, GB** (DuckDB: HTTPFS bytes in an untimed pass with its cache bypassed; Trino: physical input of the last timed pass, cache hits included)

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | DuckDB paimon native | Trino paimon native |
|---|---|---|---|---|---|---|
| H1_freshness | 0.17 | 0.07 | 0.05 | 0.11 | 0.00 | 0.00 |
| H2_hot_count | 0.11 | 0.07 | 0.02 | 0.05 | 0.00 | 0.00 |
| H3_hot_needle | 0.15 | 0.11 | 0.09 | 0.12 | 0.00 | 0.00 |
| H4_hot_narrow_agg | 0.07 | 0.01 | 0.10 | 0.13 | 0.00 | 0.00 |
| H5_hot_wide_row | 0.09 | 0.03 | 0.34 | 0.35 | 0.00 | 0.00 |
| H6_hot_product_agg | 0.27 | 0.22 | 0.18 | 0.21 | 0.00 | 0.00 |
| H7_hot_status_category_rank | 1.08 | 0.93 | 0.34 | 0.98 | 0.00 | 0.00 |
| H8_hot_pk_lookup | - | - | - | - | - | - |
| H9_hot_join | 0.28 | 0.18 | 0.19 | 0.27 | 0.00 | 0.00 |

**Same rows across arms?** (every row of the last timed pass, compared as text). Letters name distinct result sets per query; equal letters = identical rows. An arm that did not run OK shows its status and is not compared.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | DuckDB paimon native | Trino paimon native | rows |
|---|---|---|---|---|---|---|---|
| H1_freshness | A | A | B | B | B | B | 1 |
| H2_hot_count | A | A | B | B | B | B | 1 |
| H3_hot_needle | A | A | A | A | A | A | 0 |
| H4_hot_narrow_agg | A | A | B | B | B | B | 5 |
| H5_hot_wide_row | A | A | B | B | B | B | 1 |
| H6_hot_product_agg | A | A | B | B | B | B | 20 |
| H7_hot_status_category_rank | A | A | B | B | B | B | 15 |
| H8_hot_pk_lookup | ZERO_SCAN | ZERO_SCAN | ZERO_SCAN | ZERO_SCAN | ZERO_SCAN | ZERO_SCAN | - |
| H9_hot_join | A | A | B | B | B | B | 5 |

Validity: on each format, every query both engines ran OK returned the same rows.

Iceberg vs Paimon-as-iceberg rows differ on: H1_freshness (DuckDB), H1_freshness (Trino), H2_hot_count (DuckDB), H2_hot_count (Trino), H4_hot_narrow_agg (DuckDB), H4_hot_narrow_agg (Trino), H5_hot_wide_row (DuckDB), H5_hot_wide_row (Trino), H6_hot_product_agg (DuckDB), H6_hot_product_agg (Trino), H7_hot_status_category_rank (DuckDB), H7_hot_status_category_rank (Trino), H9_hot_join (DuckDB), H9_hot_join (Trino). A pipeline gap, not a failure: the two CDC jobs are pinned at the same moment, not at the same binlog point.

Iceberg vs native Paimon rows differ on: H1_freshness (DuckDB), H1_freshness (Trino), H2_hot_count (DuckDB), H2_hot_count (Trino), H4_hot_narrow_agg (DuckDB), H4_hot_narrow_agg (Trino), H5_hot_wide_row (DuckDB), H5_hot_wide_row (Trino), H6_hot_product_agg (DuckDB), H6_hot_product_agg (Trino), H7_hot_status_category_rank (DuckDB), H7_hot_status_category_rank (Trino), H9_hot_join (DuckDB), H9_hot_join (Trino). A pipeline gap, not a failure (pinned at the same moment, not at the same binlog point).

First row per arm:

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | DuckDB paimon native | Trino paimon native |
|---|---|---|---|---|---|---|
| H1_freshness | `289917\|289917\|346129\|2026-11-08 12:26:59\|2026-11-08 12:26:59` | `289917\|289917\|346129\|2026-11-08 12:26:59\|2026-11-08 12:26:59` | `290299\|290299\|346167\|2026-11-08 12:26:59\|2026-11-08 12:26:59` | `290299\|290299\|346167\|2026-11-08 12:26:59\|2026-11-08 12:26:59` | `290299\|290299\|346167\|2026-11-08 12:26:59\|2026-11-08 12:26:59` | `290299\|290299\|346167\|2026-11-08 12:26:59\|2026-11-08 12:26:59` |
| H2_hot_count | `346129` | `346129` | `346167` | `346167` | `346167` | `346167` |
| H3_hot_needle | `-` | `-` | `-` | `-` | `-` | `-` |
| H4_hot_narrow_agg | `CANCELLED\|58296\|146078168.3` | `CANCELLED\|58296\|146078168.3` | `CANCELLED\|58367\|146237170.91` | `CANCELLED\|58367\|146237170.91` | `CANCELLED\|58367\|146237170.91` | `CANCELLED\|58367\|146237170.91` |
| H5_hot_wide_row | `289917\|5\|557906729437434\|290135453464817\|727168296.2\|2026-11` | `289917\|5\|557906729437434\|290135453464817\|727168296.2\|2026-11` | `290299\|5\|558669136446418\|290520865856745\|728124411.94\|2026-1` | `290299\|5\|558669136446418\|290520865856745\|728124411.94\|2026-1` | `290299\|5\|558669136446418\|290520865856745\|728124411.94\|2026-1` | `290299\|5\|558669136446418\|290520865856745\|728124411.94\|2026-1` |
| H6_hot_product_agg | `741367813\|27\|164\|86960.71\|27` | `741367813\|27\|164\|86960.71\|27` | `741367813\|27\|164\|86960.71\|27` | `741367813\|27\|164\|86960.71\|27` | `741367813\|27\|164\|86960.71\|27` | `741367813\|27\|164\|86960.71\|27` |
| H7_hot_status_category_rank | `CANCELLED\|1\|Health & Beauty\|49\|22\|5\|255\|141989.18\|918936.24\|` | `CANCELLED\|1\|Health & Beauty\|49\|22\|5\|255\|141989.18\|918936.24\|` | `CANCELLED\|1\|Health & Beauty\|49\|22\|5\|255\|141989.18\|919088.12\|` | `CANCELLED\|1\|Health & Beauty\|49\|22\|5\|255\|141989.18\|919088.12\|` | `CANCELLED\|1\|Health & Beauty\|49\|22\|5\|255\|141989.18\|919088.12\|` | `CANCELLED\|1\|Health & Beauty\|49\|22\|5\|255\|141989.18\|919088.12\|` |
| H8_hot_pk_lookup | `-` | `-` | `-` | `-` | `-` | `-` |
| H9_hot_join | `CANCELLED\|362\|34\|1974\|918936.24` | `CANCELLED\|362\|34\|1974\|918936.24` | `CANCELLED\|362\|34\|1974\|919088.12` | `CANCELLED\|362\|34\|1974\|919088.12` | `CANCELLED\|362\|34\|1974\|919088.12` | `CANCELLED\|362\|34\|1974\|919088.12` |

</details>

<details><summary>hot: pinned tables, data and delete files (tables-20261108T115501Z.tsv)</summary>

Per format and table, its pins in this suite (hot: one per pass, ranges over them; snapshot: one). `paimon` = the Paimon-as-iceberg metadata; `paimon_native` = the native Paimon snapshot the paimon native arms read, pinned at the same N (records and commit kind only).

| format | table | pins | snapshot id (last) | records (rows) | data files | delete files | equality deletes (rows) | position deletes (rows, incl. DVs) | GB |
|---|---|---|---|---|---|---|---|---|---|
| iceberg | customers | 4 | 7795996607825704243 | 42825874..43294243 | 444..720 | 792..1344 | 42568194..42953950 | 257680..340293 | 1.74 |
| iceberg | products | 4 | 3830670287563400127 | 112507936..117620955 | 374..581 | 549..963 | 110034431..114015544 | 2473505..3605411 | 3.46 |
| iceberg | orders | 4 | 302279612608792021 | 244065629..258195962 | 181..296 | 217..447 | 238762160..250550250 | 5919974..8682323 | 5.05 |
| iceberg | order_items | 4 | 1906976086847815608 | 305215087..326395899 | 184..299 | 218..448 | 296065353..312684626 | 10168876..15422117 | 5.59 |
| paimon | customers | 4 | 114 | 4670233..4711600 | 0 | 92..97 | 0 | 585804..860804 | 0.17 |
| paimon | products | 4 | 114 | 47172489..48235211 | 0..28 | 345..435 | 0 | 2757020..3784929 | 1.48 |
| paimon | orders | 4 | 114 | 85282346..89903179 | 0 | 376..392 | 0 | 7595101..11111295 | 1.65 |
| paimon | order_items | 4 | 114 | 104283675..110594366 | 0 | 346..389 | 0 | 9751052..15618756 | 1.75 |
| paimon_native | customers | 4 | 114 | 42424651..42718912 | - | - | - | - | - |
| paimon_native | products | 4 | 114 | 109062424..112031115 | - | - | - | - | - |
| paimon_native | orders | 4 | 114 | 235733615..242805069 | - | - | - | - | - |
| paimon_native | order_items | 4 | 114 | 289694212..299982293 | - | - | - | - | - |

Every pin (tables TSV):

| pin_tag | format | table | snapshot_id | commit_ms | total-records | total-data-files | total-delete-files | total-equality-deletes | total-position-deletes | total-files-size | note |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20261108T115501Z-p0 | iceberg | customers | 8046533037514960121 | 1794141165748 | 42825874 | 444 | 792 | 42568194 | 257680 | 1717579511 | format-version=3 operation=overwrite |
| 20261108T115501Z-p0 | paimon | customers | 68 | 1794141165289 | 4677027 | 0 | 92 | 0 | 585804 | 171577438 | format-version=3 operation=replace |
| 20261108T115501Z-p0 | paimon_native | customers | 68 | 1794141165289 | 42424651 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p0 | iceberg | products | 1482721797816639417 | 1794141165791 | 112507936 | 374 | 549 | 110034431 | 2473505 | 3317116223 | format-version=3 operation=overwrite |
| 20261108T115501Z-p0 | paimon | products | 68 | 1794141168305 | 47172489 | 15 | 435 | 0 | 2757020 | 1440503417 | format-version=3 operation=replace |
| 20261108T115501Z-p0 | paimon_native | products | 68 | 1794141168305 | 109062424 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p0 | iceberg | orders | 2501278756765316036 | 1794141165715 | 244065629 | 181 | 217 | 238762160 | 5919974 | 4756477937 | format-version=3 operation=overwrite |
| 20261108T115501Z-p0 | paimon | orders | 68 | 1794141167275 | 89903179 | 0 | 392 | 0 | 7595101 | 1793251510 | format-version=3 operation=replace |
| 20261108T115501Z-p0 | paimon_native | orders | 68 | 1794141167275 | 235733615 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p0 | iceberg | order_items | 2092048481077050982 | 1794141165704 | 305215087 | 184 | 218 | 296065353 | 10168876 | 5176922751 | format-version=3 operation=overwrite |
| 20261108T115501Z-p0 | paimon | order_items | 68 | 1794141166312 | 110594366 | 0 | 346 | 0 | 9751052 | 1944646853 | format-version=3 operation=replace |
| 20261108T115501Z-p0 | paimon_native | order_items | 68 | 1794141166312 | 289694212 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p1 | iceberg | customers | 8307141058876148890 | 1794141585984 | 42969778 | 528 | 960 | 42684786 | 284992 | 1723787949 | format-version=3 operation=overwrite |
| 20261108T115501Z-p1 | paimon | customers | 82 | 1794141582678 | 4670233 | 0 | 97 | 0 | 656256 | 170740177 | format-version=3 operation=replace |
| 20261108T115501Z-p1 | paimon_native | customers | 82 | 1794141582678 | 42500963 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p1 | iceberg | products | 1972784596535228203 | 1794141585999 | 114048657 | 437 | 675 | 111235633 | 2813024 | 3360780759 | format-version=3 operation=overwrite |
| 20261108T115501Z-p1 | paimon | products | 83 | 1794141638567 | 47267628 | 0 | 345 | 0 | 2909424 | 1445493602 | format-version=3 operation=append |
| 20261108T115501Z-p1 | paimon_native | products | 83 | 1794141638567 | 109905813 |  |  |  |  |  | commitKind=APPEND |
| 20261108T115501Z-p1 | iceberg | orders | 894958992190981331 | 1794141586041 | 248307377 | 216 | 287 | 242284593 | 6764139 | 4845964803 | format-version=3 operation=overwrite |
| 20261108T115501Z-p1 | paimon | orders | 83 | 1794141638536 | 87908125 | 0 | 379 | 0 | 8023976 | 1738461899 | format-version=3 operation=append |
| 20261108T115501Z-p1 | paimon_native | orders | 83 | 1794141638536 | 237709190 |  |  |  |  |  | commitKind=APPEND |
| 20261108T115501Z-p1 | iceberg | order_items | 5253230877050312499 | 1794141645952 | 311763712 | 224 | 298 | 301281635 | 11712660 | 5306298198 | format-version=3 operation=overwrite |
| 20261108T115501Z-p1 | paimon | order_items | 84 | 1794141644790 | 108665916 | 0 | 355 | 0 | 11657963 | 1882824975 | format-version=3 operation=replace |
| 20261108T115501Z-p1 | paimon_native | order_items | 84 | 1794141644790 | 292910618 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p2 | iceberg | customers | 251815772955824689 | 1794142066395 | 43144758 | 624 | 1152 | 42829145 | 315613 | 1731365906 | format-version=3 operation=overwrite |
| 20261108T115501Z-p2 | paimon | customers | 98 | 1794142065686 | 4692423 | 0 | 96 | 0 | 761083 | 171478906 | format-version=3 operation=replace |
| 20261108T115501Z-p2 | paimon_native | customers | 98 | 1794142065686 | 42613145 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p2 | iceberg | products | 4408685769335292093 | 1794142066453 | 115633775 | 509 | 819 | 112471502 | 3162273 | 3405884639 | format-version=3 operation=overwrite |
| 20261108T115501Z-p2 | paimon | products | 98 | 1794142063687 | 47774024 | 0 | 423 | 0 | 3379774 | 1462510087 | format-version=3 operation=replace |
| 20261108T115501Z-p2 | paimon_native | products | 98 | 1794142063687 | 110875964 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p2 | iceberg | orders | 627071508494178131 | 1794142066441 | 253112700 | 256 | 367 | 246256215 | 7741426 | 4947152203 | format-version=3 operation=overwrite |
| 20261108T115501Z-p2 | paimon | orders | 98 | 1794142064702 | 86865546 | 0 | 376 | 0 | 9702424 | 1699228998 | format-version=3 operation=replace |
| 20261108T115501Z-p2 | paimon_native | orders | 98 | 1794142064702 | 240101305 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p2 | iceberg | order_items | 8829242873650061143 | 1794142066385 | 318802000 | 259 | 368 | 306810183 | 13452830 | 5444421688 | format-version=3 operation=overwrite |
| 20261108T115501Z-p2 | paimon | order_items | 98 | 1794142062699 | 106949340 | 0 | 389 | 0 | 14034513 | 1830833337 | format-version=3 operation=replace |
| 20261108T115501Z-p2 | paimon_native | order_items | 98 | 1794142062699 | 296768768 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p3 | iceberg | customers | 7795996607825704243 | 1794142547400 | 43294243 | 720 | 1344 | 42953950 | 340293 | 1737935785 | format-version=3 operation=overwrite |
| 20261108T115501Z-p3 | paimon | customers | 114 | 1794142562286 | 4711600 | 0 | 97 | 0 | 860804 | 172172622 | format-version=3 operation=replace |
| 20261108T115501Z-p3 | paimon_native | customers | 114 | 1794142562286 | 42718912 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p3 | iceberg | products | 3830670287563400127 | 1794142547548 | 117620955 | 581 | 963 | 114015544 | 3605411 | 3462364879 | format-version=3 operation=overwrite |
| 20261108T115501Z-p3 | paimon | products | 114 | 1794142560309 | 48235211 | 28 | 366 | 0 | 3784929 | 1478016845 | format-version=3 operation=replace |
| 20261108T115501Z-p3 | paimon_native | products | 114 | 1794142560309 | 112031115 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p3 | iceberg | orders | 302279612608792021 | 1794142547420 | 258195962 | 296 | 447 | 250550250 | 8682323 | 5054227916 | format-version=3 operation=overwrite |
| 20261108T115501Z-p3 | paimon | orders | 114 | 1794142559303 | 85282346 | 0 | 380 | 0 | 11111295 | 1647155810 | format-version=3 operation=replace |
| 20261108T115501Z-p3 | paimon_native | orders | 114 | 1794142559303 | 242805069 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T115501Z-p3 | iceberg | order_items | 1906976086847815608 | 1794142547393 | 326395899 | 299 | 448 | 312684626 | 15422117 | 5593267999 | format-version=3 operation=overwrite |
| 20261108T115501Z-p3 | paimon | order_items | 114 | 1794142561350 | 104283675 | 0 | 368 | 0 | 15618756 | 1749930596 | format-version=3 operation=replace |
| 20261108T115501Z-p3 | paimon_native | order_items | 114 | 1794142561350 | 299982293 |  |  |  |  |  | commitKind=COMPACT |

</details>

<details><summary>hot: pins, gates and windows (hot-interleave-20261108T115501Z.pins)</summary>

```
stamp=20261108T115501Z window=[2026-11-08 12:26:30, 2026-11-08 12:27:00) dt=2026-11-08 repin=pass engines=duckdb,trino formats=iceberg,paimon,paimon_native
iceberg_format_version=3
cdc_db_suffix=_v3 iceberg_db=flink_iceberg_db_v3 paimon_db=flink_paimon_db_v3 engines=duckdb,trino formats=iceberg,paimon,paimon_native
duckdb_image=<id>.dkr.ecr.us-west-2.amazonaws.com/duckdb-cdc-server:2.0.0 duckdb_cpu=81 duckdb_pod_memory=342Gi duckdb_skip_puffin_verification_arms=paimon
trino_version=484 trino_catalog=iceberg trino_paimon_catalog=paimon trino_workers=9 paimon_reader=iceberg
duckdb_cpu=81 duckdb_memory=342Gi duckdb_ec2=m5.24xlarge trino_worker_cpu=9 trino_worker_memory=38Gi trino_worker_ec2=m5.4xlarge trino_coord_cpu=4 trino_coord_memory=16Gi trino_coord_ec2=m5.2xlarge
generator_before=mysql-upsert-loadgen=24/24
data_wait_s=84
## pins 20261108T115501Z-p0
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794141165748
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794141165715
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794141165704
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794141165791
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794141165289
QB_PIN_COMMIT_MS_PAIMON_NATIVE_CUSTOMERS=1794141165289
QB_PIN_COMMIT_MS_PAIMON_NATIVE_ORDERS=1794141167275
QB_PIN_COMMIT_MS_PAIMON_NATIVE_ORDER_ITEMS=1794141166312
QB_PIN_COMMIT_MS_PAIMON_NATIVE_PRODUCTS=1794141168305
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794141167275
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794141166312
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794141168305
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p0/iceberg/customers/00035-ac861ac5-8f73-4b3d-9dd0-4b71c3d399e9.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p0/iceberg/orders/00035-c0783278-39d5-4cc5-b7b2-0787560cbf8f.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p0/iceberg/order_items/00035-11e263db-298b-4b62-a9aa-9c8caf2450d9.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p0/iceberg/products/00035-d05da1b3-e430-463e-a1da-0c7d13f3e58f.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p0/paimon/customers/v68.metadata.json
QB_PIN_META_PAIMON_NATIVE_CUSTOMERS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/customers
QB_PIN_META_PAIMON_NATIVE_ORDERS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/orders
QB_PIN_META_PAIMON_NATIVE_ORDER_ITEMS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/order_items
QB_PIN_META_PAIMON_NATIVE_PRODUCTS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/products
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p0/paimon/orders/v68.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p0/paimon/order_items/v68.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p0/paimon/products/v68.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3
QB_PIN_SNAP_ICEBERG_CUSTOMERS=8046533037514960121
QB_PIN_SNAP_ICEBERG_ORDERS=2501278756765316036
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=2092048481077050982
QB_PIN_SNAP_ICEBERG_PRODUCTS=1482721797816639417
QB_PIN_SNAP_PAIMON_CUSTOMERS=68
QB_PIN_SNAP_PAIMON_NATIVE_CUSTOMERS=68
QB_PIN_SNAP_PAIMON_NATIVE_ORDERS=68
QB_PIN_SNAP_PAIMON_NATIVE_ORDER_ITEMS=68
QB_PIN_SNAP_PAIMON_NATIVE_PRODUCTS=68
QB_PIN_SNAP_PAIMON_ORDERS=68
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=68
QB_PIN_SNAP_PAIMON_PRODUCTS=68
h8_key order_id=1294388552 dt=2026-11-07 (picked on trino:iceberg (pinned orders), not confirmed on the other arms (the summary compares their rows); partition older than HOT_DT (more equality deletes))
queries_started_at=2026-11-08 12:34:23Z
## pins 20261108T115501Z-p1
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794141585984
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794141586041
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794141645952
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794141585999
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794141582678
QB_PIN_COMMIT_MS_PAIMON_NATIVE_CUSTOMERS=1794141582678
QB_PIN_COMMIT_MS_PAIMON_NATIVE_ORDERS=1794141638536
QB_PIN_COMMIT_MS_PAIMON_NATIVE_ORDER_ITEMS=1794141644790
QB_PIN_COMMIT_MS_PAIMON_NATIVE_PRODUCTS=1794141638567
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794141638536
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794141644790
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794141638567
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p1/iceberg/customers/00042-527537b3-db74-438e-bedb-818156227b3e.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p1/iceberg/orders/00042-b20af5bc-8e7b-4195-93d8-9a44390699df.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p1/iceberg/order_items/00043-4407da1a-f4aa-4211-a80b-1ef8b1d343b6.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p1/iceberg/products/00042-86df6487-a586-4c8b-910b-fd94c8d6fb58.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p1/paimon/customers/v82.metadata.json
QB_PIN_META_PAIMON_NATIVE_CUSTOMERS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/customers
QB_PIN_META_PAIMON_NATIVE_ORDERS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/orders
QB_PIN_META_PAIMON_NATIVE_ORDER_ITEMS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/order_items
QB_PIN_META_PAIMON_NATIVE_PRODUCTS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/products
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p1/paimon/orders/v83.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p1/paimon/order_items/v84.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p1/paimon/products/v83.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3
QB_PIN_SNAP_ICEBERG_CUSTOMERS=8307141058876148890
QB_PIN_SNAP_ICEBERG_ORDERS=894958992190981331
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=5253230877050312499
QB_PIN_SNAP_ICEBERG_PRODUCTS=1972784596535228203
QB_PIN_SNAP_PAIMON_CUSTOMERS=82
QB_PIN_SNAP_PAIMON_NATIVE_CUSTOMERS=82
QB_PIN_SNAP_PAIMON_NATIVE_ORDERS=83
QB_PIN_SNAP_PAIMON_NATIVE_ORDER_ITEMS=84
QB_PIN_SNAP_PAIMON_NATIVE_PRODUCTS=83
QB_PIN_SNAP_PAIMON_ORDERS=83
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=84
QB_PIN_SNAP_PAIMON_PRODUCTS=83
## pins 20261108T115501Z-p2
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794142066395
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794142066441
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794142066385
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794142066453
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794142065686
QB_PIN_COMMIT_MS_PAIMON_NATIVE_CUSTOMERS=1794142065686
QB_PIN_COMMIT_MS_PAIMON_NATIVE_ORDERS=1794142064702
QB_PIN_COMMIT_MS_PAIMON_NATIVE_ORDER_ITEMS=1794142062699
QB_PIN_COMMIT_MS_PAIMON_NATIVE_PRODUCTS=1794142063687
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794142064702
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794142062699
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794142063687
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p2/iceberg/customers/00050-ad286f9e-33db-422a-abef-e9244da67dcb.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p2/iceberg/orders/00050-2e2062ee-5d6a-43cf-8395-9046e5152e07.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p2/iceberg/order_items/00050-daa73c04-1de1-4f3a-ab27-10498ffe7126.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p2/iceberg/products/00050-55e8ee5e-bb36-4aea-a899-32fa53d95080.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p2/paimon/customers/v98.metadata.json
QB_PIN_META_PAIMON_NATIVE_CUSTOMERS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/customers
QB_PIN_META_PAIMON_NATIVE_ORDERS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/orders
QB_PIN_META_PAIMON_NATIVE_ORDER_ITEMS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/order_items
QB_PIN_META_PAIMON_NATIVE_PRODUCTS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/products
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p2/paimon/orders/v98.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p2/paimon/order_items/v98.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p2/paimon/products/v98.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3
QB_PIN_SNAP_ICEBERG_CUSTOMERS=251815772955824689
QB_PIN_SNAP_ICEBERG_ORDERS=627071508494178131
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=8829242873650061143
QB_PIN_SNAP_ICEBERG_PRODUCTS=4408685769335292093
QB_PIN_SNAP_PAIMON_CUSTOMERS=98
QB_PIN_SNAP_PAIMON_NATIVE_CUSTOMERS=98
QB_PIN_SNAP_PAIMON_NATIVE_ORDERS=98
QB_PIN_SNAP_PAIMON_NATIVE_ORDER_ITEMS=98
QB_PIN_SNAP_PAIMON_NATIVE_PRODUCTS=98
QB_PIN_SNAP_PAIMON_ORDERS=98
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=98
QB_PIN_SNAP_PAIMON_PRODUCTS=98
## pins 20261108T115501Z-p3
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794142547400
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794142547420
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794142547393
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794142547548
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794142562286
QB_PIN_COMMIT_MS_PAIMON_NATIVE_CUSTOMERS=1794142562286
QB_PIN_COMMIT_MS_PAIMON_NATIVE_ORDERS=1794142559303
QB_PIN_COMMIT_MS_PAIMON_NATIVE_ORDER_ITEMS=1794142561350
QB_PIN_COMMIT_MS_PAIMON_NATIVE_PRODUCTS=1794142560309
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794142559303
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794142561350
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794142560309
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p3/iceberg/customers/00058-89ec4457-6edf-4a96-886a-a9d20c93671d.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p3/iceberg/orders/00058-d37f9d57-49ed-4bbd-9fee-5446b814557d.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p3/iceberg/order_items/00058-8fbdb698-73d3-4100-89aa-fdf773dc2648.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p3/iceberg/products/00058-9e761271-5d00-4f02-ad0f-5d07ff9cd592.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p3/paimon/customers/v114.metadata.json
QB_PIN_META_PAIMON_NATIVE_CUSTOMERS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/customers
QB_PIN_META_PAIMON_NATIVE_ORDERS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/orders
QB_PIN_META_PAIMON_NATIVE_ORDER_ITEMS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/order_items
QB_PIN_META_PAIMON_NATIVE_PRODUCTS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/products
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p3/paimon/orders/v114.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p3/paimon/order_items/v114.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T115501Z-p3/paimon/products/v114.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3
QB_PIN_SNAP_ICEBERG_CUSTOMERS=7795996607825704243
QB_PIN_SNAP_ICEBERG_ORDERS=302279612608792021
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=1906976086847815608
QB_PIN_SNAP_ICEBERG_PRODUCTS=3830670287563400127
QB_PIN_SNAP_PAIMON_CUSTOMERS=114
QB_PIN_SNAP_PAIMON_NATIVE_CUSTOMERS=114
QB_PIN_SNAP_PAIMON_NATIVE_ORDERS=114
QB_PIN_SNAP_PAIMON_NATIVE_ORDER_ITEMS=114
QB_PIN_SNAP_PAIMON_NATIVE_PRODUCTS=114
QB_PIN_SNAP_PAIMON_ORDERS=114
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=114
QB_PIN_SNAP_PAIMON_PRODUCTS=114
finished_at=2026-11-08 13:09:02Z
generator_after=mysql-upsert-loadgen=24/24
```
</details>

<details><summary>snapshot: Trino server-side breakdown, data scanned, result equality</summary>

**Trino server-side breakdown** (median over the timed passes)

| arm | query | elapsed s | queued s | execution s | CPU s | physical input GB |
|---|---|---|---|---|---|---|
| iceberg | S1_order_items_count | 0.23 | 0.00 | 0.23 | 2.50 | 0.04 |
| iceberg | S2_items_narrow_agg | 0.38 | 0.00 | 0.38 | 9.34 | 0.35 |
| iceberg | S3_orders_distinct_customers | 0.98 | 0.00 | 0.98 | 37.80 | 0.25 |
| iceberg | S4_orders_wide_slice | 0.63 | 0.00 | 0.63 | 17.77 | 1.18 |
| iceberg | S5_star_join_revenue | 5.70 | 0.00 | 5.70 | 238.20 | 2.22 |
| iceberg | S6_topn_customers | 0.58 | 0.00 | 0.58 | 11.60 | 0.48 |
| iceberg | S7_dedup_keys | 6.34 | 0.00 | 6.34 | 338.10 | 0.82 |
| paimon | S1_order_items_count | 0.22 | 0.00 | 0.22 | 1.26 | 0.04 |
| paimon | S2_items_narrow_agg | 0.41 | 0.00 | 0.41 | 5.95 | 0.35 |
| paimon | S3_orders_distinct_customers | 1.16 | 0.00 | 1.16 | 20.95 | 0.25 |
| paimon | S4_orders_wide_slice | 0.65 | 0.00 | 0.65 | 9.92 | 1.23 |
| paimon | S5_star_join_revenue | 5.32 | 0.00 | 5.32 | 248.10 | 2.58 |
| paimon | S6_topn_customers | 0.68 | 0.00 | 0.68 | 7.05 | 0.47 |
| paimon | S7_dedup_keys | 5.97 | 0.00 | 5.97 | 356.10 | 0.77 |
| paimon_native | S1_order_items_count | 1.57 | 0.00 | 1.57 | 12.29 | 0.00 |
| paimon_native | S2_items_narrow_agg | 3.21 | 0.00 | 3.21 | 29.50 | 0.00 |
| paimon_native | S3_orders_distinct_customers | 4.01 | 0.00 | 4.01 | 44.42 | 0.00 |
| paimon_native | S4_orders_wide_slice | 7.04 | 0.00 | 7.04 | 87.60 | 0.00 |
| paimon_native | S5_star_join_revenue | 11.17 | 0.00 | 11.17 | 443.40 | 0.00 |
| paimon_native | S6_topn_customers | 3.37 | 0.00 | 3.37 | 47.16 | 0.00 |
| paimon_native | S7_dedup_keys | 7.66 | 0.00 | 7.66 | 377.10 | 0.00 |

**Data scanned, GB** (DuckDB: HTTPFS bytes in an untimed pass with its cache bypassed; Trino: physical input of the last timed pass, cache hits included)

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | DuckDB paimon native | Trino paimon native |
|---|---|---|---|---|---|---|
| S1_order_items_count | 0.04 | 0.04 | 0.04 | 0.04 | 0.00 | 0.00 |
| S2_items_narrow_agg | 0.35 | 0.35 | 0.35 | 0.35 | 0.00 | 0.00 |
| S3_orders_distinct_customers | 0.24 | 0.25 | 0.24 | 0.25 | 0.00 | 0.00 |
| S4_orders_wide_slice | 1.19 | 1.18 | 1.22 | 1.23 | 0.00 | 0.00 |
| S5_star_join_revenue | 2.27 | 2.19 | 2.33 | 2.59 | 0.00 | 0.00 |
| S6_topn_customers | 0.46 | 0.48 | 0.45 | 0.47 | 0.00 | 0.00 |
| S7_dedup_keys | 0.79 | 0.82 | 0.77 | 0.77 | 0.00 | 0.00 |

**Same rows across arms?** (every row of the last timed pass, compared as text). Letters name distinct result sets per query; equal letters = identical rows. An arm that did not run OK shows its status and is not compared.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | DuckDB paimon native | Trino paimon native | rows |
|---|---|---|---|---|---|---|---|
| S1_order_items_count | A | A | B | B | B | B | 1 |
| S2_items_narrow_agg | A | A | B | B | B | B | 10 |
| S3_orders_distinct_customers | A | A | B | B | B | B | 5 |
| S4_orders_wide_slice | A | A | B | B | B | B | 1 |
| S5_star_join_revenue | A | A | B | B | B | B | 180 |
| S6_topn_customers | A | A | B | B | B | B | 20 |
| S7_dedup_keys | A | A | B | B | B | B | 4 |

Validity: on each format, every query both engines ran OK returned the same rows.

Iceberg vs Paimon-as-iceberg rows differ on: S1_order_items_count (DuckDB), S1_order_items_count (Trino), S2_items_narrow_agg (DuckDB), S2_items_narrow_agg (Trino), S3_orders_distinct_customers (DuckDB), S3_orders_distinct_customers (Trino), S4_orders_wide_slice (DuckDB), S4_orders_wide_slice (Trino), S5_star_join_revenue (DuckDB), S5_star_join_revenue (Trino), S6_topn_customers (DuckDB), S6_topn_customers (Trino), S7_dedup_keys (DuckDB), S7_dedup_keys (Trino). A pipeline gap, not a failure: the two CDC jobs are pinned at the same moment, not at the same binlog point.

Iceberg vs native Paimon rows differ on: S1_order_items_count (DuckDB), S1_order_items_count (Trino), S2_items_narrow_agg (DuckDB), S2_items_narrow_agg (Trino), S3_orders_distinct_customers (DuckDB), S3_orders_distinct_customers (Trino), S4_orders_wide_slice (DuckDB), S4_orders_wide_slice (Trino), S5_star_join_revenue (DuckDB), S5_star_join_revenue (Trino), S6_topn_customers (DuckDB), S6_topn_customers (Trino), S7_dedup_keys (DuckDB), S7_dedup_keys (Trino). A pipeline gap, not a failure (pinned at the same moment, not at the same binlog point).

First row per arm:

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | DuckDB paimon native | Trino paimon native |
|---|---|---|---|---|---|---|
| S1_order_items_count | `92064441\|506434941` | `92064441\|506434941` | `92064444\|506435035` | `92064444\|506435035` | `92064444\|506435035` | `92064444\|506435035` |
| S2_items_narrow_agg | `1\|9206681\|4165642240.48` | `1\|9206681\|4165642240.48` | `1\|9206676\|4165641183.66` | `1\|9206676\|4165641183.66` | `1\|9206676\|4165641183.66` | `1\|9206676\|4165641183.66` |
| S3_orders_distinct_customers | `APPLE_PAY\|13333691\|47537` | `APPLE_PAY\|13333691\|47537` | `APPLE_PAY\|13333686\|47537` | `APPLE_PAY\|13333686\|47537` | `APPLE_PAY\|13333686\|47537` | `APPLE_PAY\|13333686\|47537` |
| S4_orders_wide_slice | `3940275\|216297702\|9869265402.31\|4070201921722185\|79191561307` | `3940275\|216297702\|9869265402.31\|4070201921722185\|79191561307` | `3940275\|216297702\|9869272355.83\|4070201921722185\|79191561307` | `3940275\|216297702\|9869272355.83\|4070201921722185\|79191561307` | `3940275\|216297702\|9869272355.83\|4070201921722185\|79191561307` | `3940275\|216297702\|9869272355.83\|4070201921722185\|79191561307` |
| S5_star_join_revenue | `2026-11-06\|Automotive\|AZ\|455413\|2503765\|1133141281.11` | `2026-11-06\|Automotive\|AZ\|455413\|2503765\|1133141281.11` | `2026-11-06\|Automotive\|AZ\|455413\|2503765\|1133141281.11` | `2026-11-06\|Automotive\|AZ\|455413\|2503765\|1133141281.11` | `2026-11-06\|Automotive\|AZ\|455413\|2503765\|1133141281.11` | `2026-11-06\|Automotive\|AZ\|455413\|2503765\|1133141281.11` |
| S6_topn_customers | `1031491700\|Lisa Garcia\|MA\|1911\|4957798.53` | `1031491700\|Lisa Garcia\|MA\|1911\|4957798.53` | `1031491700\|Lisa Garcia\|MA\|1911\|4957798.53` | `1031491700\|Lisa Garcia\|MA\|1911\|4957798.53` | `1031491700\|Lisa Garcia\|MA\|1911\|4957798.53` | `1031491700\|Lisa Garcia\|MA\|1911\|4957798.53` |
| S7_dedup_keys | `customers\|41877746\|41877746\|0` | `customers\|41877746\|41877746\|0` | `customers\|41877746\|41877746\|0` | `customers\|41877746\|41877746\|0` | `customers\|41877746\|41877746\|0` | `customers\|41877746\|41877746\|0` |

</details>

<details><summary>snapshot: pinned tables, data and delete files (tables-20261108T134858Z.tsv)</summary>

Per format and table, its pins in this suite (hot: one per pass, ranges over them; snapshot: one). `paimon` = the Paimon-as-iceberg metadata; `paimon_native` = the native Paimon snapshot the paimon native arms read, pinned at the same N (records and commit kind only).

| format | table | pins | snapshot id (last) | records (rows) | data files | delete files | equality deletes (rows) | position deletes (rows, incl. DVs) | GB |
|---|---|---|---|---|---|---|---|---|---|
| iceberg | customers | 1 | 2512467091802224015 | 41877746 | 60 | 0 | 0 | 0 | 1.53 |
| iceberg | products | 1 | 7174109491063639053 | 109791802 | 135 | 0 | 0 | 0 | 2.88 |
| iceberg | orders | 1 | 6448864148251745869 | 234603935 | 94 | 0 | 0 | 0 | 4.20 |
| iceberg | order_items | 1 | 1013404128522299693 | 287708611 | 107 | 0 | 0 | 0 | 4.38 |
| paimon | customers | 1 | 154 | 3620660 | 0 | 0 | 0 | 0 | 0.13 |
| paimon | products | 1 | 154 | 44317494 | 0 | 0 | 0 | 0 | 1.39 |
| paimon | orders | 1 | 154 | 67894050 | 0 | 0 | 0 | 0 | 1.26 |
| paimon | order_items | 1 | 155 | 80061932 | 0 | 0 | 0 | 0 | 1.25 |
| paimon_native | customers | 1 | 154 | 41877746 | - | - | - | - | - |
| paimon_native | products | 1 | 154 | 109791852 | - | - | - | - | - |
| paimon_native | orders | 1 | 154 | 234603878 | - | - | - | - | - |
| paimon_native | order_items | 1 | 155 | 287708525 | - | - | - | - | - |

Every pin (tables TSV):

| pin_tag | format | table | snapshot_id | commit_ms | total-records | total-data-files | total-delete-files | total-equality-deletes | total-position-deletes | total-files-size | note |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20261108T134858Z | iceberg | customers | 2512467091802224015 | 1794144768482 | 41877746 | 60 | 0 | 0 | 0 | 1534251408 | format-version=3 operation=replace |
| 20261108T134858Z | paimon | customers | 154 | 1794144233133 | 3620660 | 0 | 0 | 0 | 0 | 131143289 | format-version=3 operation=replace |
| 20261108T134858Z | paimon_native | customers | 154 | 1794144233133 | 41877746 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T134858Z | iceberg | products | 7174109491063639053 | 1794144947348 | 109791802 | 135 | 0 | 0 | 0 | 2882362964 | format-version=3 operation=replace |
| 20261108T134858Z | paimon | products | 154 | 1794144501018 | 44317494 | 0 | 0 | 0 | 0 | 1391566330 | format-version=3 operation=replace |
| 20261108T134858Z | paimon_native | products | 154 | 1794144501018 | 109791852 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T134858Z | iceberg | orders | 6448864148251745869 | 1794145344893 | 234603935 | 94 | 0 | 0 | 0 | 4195458665 | format-version=3 operation=replace |
| 20261108T134858Z | paimon | orders | 154 | 1794144669340 | 67894050 | 0 | 0 | 0 | 0 | 1260202867 | format-version=3 operation=replace |
| 20261108T134858Z | paimon_native | orders | 154 | 1794144669340 | 234603878 |  |  |  |  |  | commitKind=COMPACT |
| 20261108T134858Z | iceberg | order_items | 1013404128522299693 | 1794145626841 | 287708611 | 107 | 0 | 0 | 0 | 4381117326 | format-version=3 operation=replace |
| 20261108T134858Z | paimon | order_items | 155 | 1794144861766 | 80061932 | 0 | 0 | 0 | 0 | 1252419403 | format-version=3 operation=replace |
| 20261108T134858Z | paimon_native | order_items | 155 | 1794144861766 | 287708525 |  |  |  |  |  | commitKind=COMPACT |

</details>

<details><summary>snapshot: pins, gates and windows (snapshot-20261108T134858Z.pins)</summary>

```
stamp=20261108T134858Z suite=snapshot dt=2026-11-07 engines=duckdb,trino formats=iceberg,paimon,paimon_native
iceberg_format_version=3
cdc_db_suffix=_v3 iceberg_db=flink_iceberg_db_v3 paimon_db=flink_paimon_db_v3 engines=duckdb,trino formats=iceberg,paimon,paimon_native
duckdb_image=<id>.dkr.ecr.us-west-2.amazonaws.com/duckdb-cdc-server:2.0.0 duckdb_cpu=81 duckdb_pod_memory=342Gi duckdb_skip_puffin_verification_arms=paimon
trino_version=484 trino_catalog=iceberg trino_paimon_catalog=paimon trino_workers=9 paimon_reader=iceberg
duckdb_cpu=81 duckdb_memory=342Gi duckdb_ec2=m5.24xlarge trino_worker_cpu=9 trino_worker_memory=38Gi trino_worker_ec2=m5.4xlarge trino_coord_cpu=4 trino_coord_memory=16Gi trino_coord_ec2=m5.2xlarge
started_at=2026-11-08 13:59:01Z
gate_writers_stopped_at=2026-11-08 13:59:02Z writers=flink-cdc-paimon=suspended flink-cdc-iceberg=suspended generator=mysql-upsert-loadgen=/0
gate_engines_ready_at=2026-11-08 13:59:04Z
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794144768482
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794145344893
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794145626841
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794144947348
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794144233133
QB_PIN_COMMIT_MS_PAIMON_NATIVE_CUSTOMERS=1794144233133
QB_PIN_COMMIT_MS_PAIMON_NATIVE_ORDERS=1794144669340
QB_PIN_COMMIT_MS_PAIMON_NATIVE_ORDER_ITEMS=1794144861766
QB_PIN_COMMIT_MS_PAIMON_NATIVE_PRODUCTS=1794144501018
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794144669340
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794144861766
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794144501018
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T134858Z/iceberg/customers/00078-23ae5ea4-d1c1-412c-8d7f-9c22fe35d84b.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T134858Z/iceberg/orders/00078-198c02fe-6a74-44a0-bbc4-d87c091cfb8f.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T134858Z/iceberg/order_items/00078-0f6f210c-9fe0-4e1f-8fda-42b82eaf0e65.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T134858Z/iceberg/products/00078-6364ebe7-3cd7-45ec-8780-5e7e209853bc.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T134858Z/paimon/customers/v154.metadata.json
QB_PIN_META_PAIMON_NATIVE_CUSTOMERS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/customers
QB_PIN_META_PAIMON_NATIVE_ORDERS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/orders
QB_PIN_META_PAIMON_NATIVE_ORDER_ITEMS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/order_items
QB_PIN_META_PAIMON_NATIVE_PRODUCTS=s3://<bucket>/paimon-warehouse/flink_paimon_db_v3.db/products
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T134858Z/paimon/orders/v154.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T134858Z/paimon/order_items/v155.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261108T134858Z/paimon/products/v154.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3
QB_PIN_SNAP_ICEBERG_CUSTOMERS=2512467091802224015
QB_PIN_SNAP_ICEBERG_ORDERS=6448864148251745869
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=1013404128522299693
QB_PIN_SNAP_ICEBERG_PRODUCTS=7174109491063639053
QB_PIN_SNAP_PAIMON_CUSTOMERS=154
QB_PIN_SNAP_PAIMON_NATIVE_CUSTOMERS=154
QB_PIN_SNAP_PAIMON_NATIVE_ORDERS=154
QB_PIN_SNAP_PAIMON_NATIVE_ORDER_ITEMS=155
QB_PIN_SNAP_PAIMON_NATIVE_PRODUCTS=154
QB_PIN_SNAP_PAIMON_ORDERS=154
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=155
QB_PIN_SNAP_PAIMON_PRODUCTS=154
pinned_at=2026-11-08 13:59:54Z
slice=[2026-11-07 15:50:00, 2026-11-07 16:00:00) (10 min, the busiest bucket of the partition (rows: iceberg 3940275, paimon 3940275, paimon_native 3940275; 42 non-empty buckets); formats: iceberg paimon paimon_native)
caches_cleared_at=2026-11-08 14:02:03Z
passes=3 (pass 0 cold, timed; median of the other 2)
duckdb_finished_at=2026-11-08 14:19:39Z rc=0
trino_finished_at=2026-11-08 14:26:33Z rc=0
finished_at=2026-11-08 14:26:33Z rc=0
```
</details>

<details><summary>Ingest per table (2026-11-08T12:27:45Z to 2026-11-08T12:56:02Z)</summary>

| format | table | rows/s | rows | write commits | median interval s | data files | delete files | eq deletes (rows) | pos deletes (rows) | DVs | compaction commits | first / last commit |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| iceberg | customers | 362 | 615,015 | 29 | 60.0 | 348 | 696 | 504,690 | 110,325 | 348 | 0 | 12:27:45Z / 12:55:47Z |
| iceberg | products | 3,842 | 6,520,381 | 29 | 60.0 | 261 | 522 | 5,072,203 | 1,448,178 | 261 | 0 | 12:27:45Z / 12:55:47Z |
| iceberg | orders | 10,658 | 18,087,442 | 29 | 60.0 | 145 | 290 | 15,060,074 | 3,562,996 | 145 | 0 | 12:27:45Z / 12:55:47Z |
| iceberg | order_items | 16,361 | 27,763,773 | 29 | 60.0 | 145 | 290 | 21,717,244 | 6,949,956 | 145 | 0 | 12:27:45Z / 12:55:47Z |
| paimon | customers | 298 | 506,444 | 29 | 59.7 | 232 | 232 | 0 | - | - | 29 | 12:27:52Z / 12:56:02Z |
| paimon | products | 3,034 | 5,148,090 | 29 | 59.7 | 2,088 | 2,088 | 0 | - | - | 29 | 12:27:52Z / 12:56:00Z |
| paimon | orders | 10,088 | 17,118,885 | 29 | 59.7 | 2,320 | 2,320 | 0 | - | - | 29 | 12:27:52Z / 12:55:59Z |
| paimon | order_items | 14,554 | 24,698,831 | 29 | 59.7 | 2,320 | 2,320 | 0 | - | - | 29 | 12:27:52Z / 12:56:01Z |

Job-monitor aggregate (ingest_stats.py, reports overlapping the window):

| field | Paimon | Iceberg |
|---|---:|---:|
| backpressured_ratio_max | 0.0 | 0.0 |
| bytes_per_s_max | 2592866.6 | 1744980.8 |
| bytes_per_s_median | 780635 | 669790 |
| checkpoint_ms_median | 9483 | 472 |
| committed_rows_per_s_median | 26040 | 30328 |
| first_start | 2026-11-08T12:26:34.244150Z | 2026-11-08T12:26:34.244334Z |
| last_end | 2026-11-08T12:55:49.924642Z | 2026-11-08T12:55:49.798387Z |
| reports | 15 | 15 |
| rows_per_s_max | 207154.42 | 130373.95 |
| rows_per_s_median | 52754 | 52040 |
| rows_total | 84527993 | 79771085 |
| sampled_s | 1800 | 1800 |

</details>

