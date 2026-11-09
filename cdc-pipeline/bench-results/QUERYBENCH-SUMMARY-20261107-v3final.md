# CDC query benchmark: 20261107-v3final

Warm DuckDB vs Trino on the four CDC tables Flink writes, on native Iceberg and on Paimon-as-iceberg (cdc-pipeline/querybench/); both engines of a format read the same pinned snapshot per table.

**Run configuration**

| | |
|---|---|
| Flink writers | Paimon and Iceberg v3 jobs alike: parallelism 12 -> 6 TMs × 2 slots (8 CPU / 32Gi each TM), checkpoint every 60s |
| Source | RDS mysql 8.0.45, `db.r6i.4xlarge`, gp3 64000 IOPS / 500 GB |
| Generator | `mysql-upsert-loadgen`, 24 pods × `RATE_PER_POD=80000` (target 1,920,000 rows/s); `RECENT_KEY_RATIO=0.3` & `UPSERT_RATIO=0.9`: settings to produce records with 30% of recent re-writes, 63% older-key updates, 7% inserts; actual about **100,136 rows/s** across the 4 tables (the CDC sources' read rate, median over the ingest window: Paimon 99,135, Iceberg 101,137, no backpressure) |
| Engines | DuckDB 2.0.0 (81 CPU / 342Gi, on one `m5.24xlarge`), warm; Trino 484: 9 workers × 9 CPU / 38 Gi (total **81 CPU / 342 Gi**), one `m5.4xlarge` each; coordinator 4 CPU / 16Gi on `m5.2xlarge` (schedules no splits) |
| Table set | 2 databases with a session suffix v3final (4 tables in each DB): `flink_iceberg_db_v3final` / `flink_paimon_db_v3final`; Paimon has iceberg integration turned on by default, so downstream tools can either use Iceberg connector to query Paimon's Iceberg metadata or use Paimon connector to query Paimon's native metadata |
| Iceberg | table format v3; Flink writer library Apache Iceberg 1.12.0 (`iceberg-flink-runtime-1.20`, replacing EMR's bundled 1.10.1) |
| Hot run | 9 queries, about 21 min of querying; data window 15:46:30 to 15:47:00, queried 4 times (warm-up + 3 timed), each on tables re-pinned at the start of the pass (commits 15:52:07Z to 16:07:16Z) |
| Snapshot run | 7 queries, about 10 min of querying; 1 pin set (after the writers stopped), the pinned snapshots committed 16:23:08Z to 16:45:30Z |
| Ingest window | 2026-11-07T15:47:07Z to 2026-11-07T16:07:16Z |

Known write-side differences (table settings are otherwise paired; querybench/README.md): Paimon writes orders/order_items with one writer per bucket (16 per date partition), Iceberg's hash distribution sends each date partition to ONE writer; Paimon compacts and expires snapshots inline; Iceberg is not compacted during ingest; both get one full compaction (compaction/run.sh, run by the hot test's watcher) before the snapshot test.

---

## 1. Headline

- **What was tested:** the same stream of online-shop orders was copied, as it happened, into two storage formats (Iceberg and Paimon), and the same questions were then asked of each with two query tools (DuckDB on one large server, Trino on a cluster of the same total size).
- **Questions about live, still-changing data (hot query):** with both query tools, Paimon was about 7 times faster than Iceberg. Paimon merges each change into its files while loading (ongoing compaction); Iceberg leaves that merging for later, so every question has to sort through the latest changes first.
- **Questions about settled data (snapshot test: loading stopped and both formats were fully compacted, i.e. every pending change merged into clean files):** with DuckDB, Paimon and Iceberg were about equally fast; with Trino, Paimon was about 13% faster than Iceberg. After full compaction, the two formats are close.
- **The query tool matters too:** DuckDB answered faster than Trino almost everywhere; the biggest gap was on Iceberg live data with warmed-up tools, where Trino took about 2.4 times as long.
- **Loading the data:** both formats kept up with the incoming orders, at about 100,000 changes a second each.
- **Why live questions are slower on Iceberg:** for every changed order, Iceberg writes the new version plus a note saying "ignore the old one" (49 million such notes in this test). Until a compaction merges them away, every question has to read those notes. Paimon replaces old versions as it goes, so it has none.

<details><summary>The same results, technical version</summary>

- On the Iceberg tables, Trino took **2.36×** warm DuckDB's time (hot suite, geometric mean of 9 queries).
- On Paimon-as-iceberg, Trino took **2.28×** warm DuckDB's time (hot suite, geometric mean of 9 queries).
- On Trino, Paimon-as-iceberg answered in **0.14×** Iceberg's time (9 queries); its rows differed from Iceberg's on 6 of 9 queries (pinned at the same moment, not the same binlog point; Paimon-as-iceberg trails native Paimon by about a checkpoint).
- On DuckDB, Paimon-as-iceberg answered in **0.14×** Iceberg's time (9 queries); its rows differed from Iceberg's on 6 of 9 queries (pinned at the same moment, not the same binlog point; Paimon-as-iceberg trails native Paimon by about a checkpoint).
- On the Iceberg tables, Trino took **1.28×** warm DuckDB's time (snapshot suite, geometric mean of 7 queries).
- On Paimon-as-iceberg, Trino took **1.21×** warm DuckDB's time (snapshot suite, geometric mean of 7 queries).
- On Trino, Paimon-as-iceberg answered in **0.88×** Iceberg's time (7 queries); its rows differed from Iceberg's on 2 of 7 queries (pinned at the same moment, not the same binlog point; Paimon-as-iceberg trails native Paimon by about a checkpoint).
- On DuckDB, Paimon-as-iceberg answered in **0.93×** Iceberg's time (7 queries); its rows differed from Iceberg's on 2 of 7 queries (pinned at the same moment, not the same binlog point; Paimon-as-iceberg trails native Paimon by about a checkpoint).
- Cold start (snapshot suite, first pass after the caches were emptied): on the Iceberg tables, Trino took **2.23×** DuckDB's time (7 queries, one sample each).
- Cold start (snapshot suite, first pass after the caches were emptied): on Paimon-as-iceberg, Trino took **1.40×** DuckDB's time (7 queries, one sample each).
- Cold start (snapshot suite): on Trino, Paimon-as-iceberg answered in **0.63×** Iceberg's time (7 queries, one sample each).
- Cold start (snapshot suite): on DuckDB, Paimon-as-iceberg answered in **1.00×** Iceberg's time (7 queries, one sample each).
- Ingest throughput (job monitor, median of 11 / 11 reports): Paimon **99,135 rows/s, 1.4 MB/s**; Iceberg **101,137 rows/s, 0.6 MB/s** (rows/s = the CDC sources' rate meter, the ingest rate; bytes/s = committed).
- Delete mix: Iceberg wrote 65.3 M deletes for 63.7 M committed rows, 25% of them position deletes (DVs) and 49.0 M equality deletes; Paimon wrote no equality deletes.

</details>

## 2. Query latency

### Compaction before the snapshot test (run 20261107T162100Z)

`compaction/run.sh`, writers stopped: Paimon `sys.compact` (full); Iceberg data files with deletes rewritten with them applied and the delete files removed, then snapshots expired (12 h, keep 5). One row per table: its newest run.

| format | table | status | duration (s) | data files (files) | GiB | rows in files (rows) | delete files (files) | eq deletes (rows) | pos deletes (rows) |
|---|---|---|---:|---|---|---|---|---:|---:|
| iceberg | customers | OK | 675.0 | 768 -> 72 | 1.49 -> 1.43 | 43,422,692 -> 41,787,386 | 1,410 -> 0 | 31,144,661 | 415,614 |
| iceberg | order_items | OK | 226.4 | 259 -> 91 | 4.49 -> 3.79 | 310,224,183 -> 265,257,556 | 332 -> 0 | 132,032,954 | 1,992,505 |
| iceberg | orders | OK | 347.8 | 260 -> 84 | 4.13 -> 3.63 | 246,545,316 -> 217,242,877 | 378 -> 0 | 106,072,195 | 10,484,744 |
| iceberg | products | OK | 118.7 | 612 -> 117 | 2.73 -> 2.48 | 111,716,955 -> 101,274,561 | 981 -> 0 | 94,537,200 | 4,000,345 |
| paimon | customers | OK | 122.1 | 103 -> 56 | 1.64 -> 1.61 | 42,687,754 -> 41,787,386 | 0 -> 0 | 0 | 0 |
| paimon | order_items | OK | 136.4 | 453 -> 112 | 4.79 -> 4.65 | 277,730,713 -> 265,257,560 | 0 -> 0 | 0 | 0 |
| paimon | orders | OK | 138.8 | 423 -> 112 | 4.50 -> 4.33 | 226,104,991 -> 217,242,880 | 0 -> 0 | 0 | 0 |
| paimon | products | OK | 136.8 | 532 -> 144 | 3.16 -> 3.07 | 105,064,854 -> 101,274,560 | 0 -> 0 | 0 | 0 |

### hot suite (run 20261107T151845Z)

Recovered as OK (Trino FINISHED the query; only the CLI's result stream back through `kubectl exec` broke, so that pass's server-side time is used and its rows are not compared): Trino iceberg H8_hot_pk_lookup (pass 2).

Median of the timed passes, seconds (DuckDB: execute + fetch in the server; Trino: elapsed - queued). Ratio > 1 = Trino slower.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | Trino / DuckDB, iceberg | Trino / DuckDB, paimon |
|---|---:|---:|---:|---:|---:|---:|
| H1_freshness | 5.97 | 8.80 | 1.26 | 1.16 | **1.47** | **0.92** |
| H2_hot_count | 3.13 | 8.68 | 0.31 | 0.81 | **2.77** | **2.57** |
| H3_hot_needle | 3.23 | 8.32 | 0.48 | 1.05 | **2.57** | **2.19** |
| H4_hot_narrow_agg | 2.14 | 5.94 | 0.36 | 0.82 | **2.78** | **2.30** |
| H5_hot_wide_row | 2.38 | 5.83 | 0.43 | 1.41 | **2.45** | **3.28** |
| H6_hot_product_agg | 3.55 | 8.83 | 0.61 | 1.25 | **2.49** | **2.05** |
| H7_hot_status_category_rank | 11.01 | 17.85 | 1.48 | 3.96 | **1.62** | **2.67** |
| H8_hot_pk_lookup | 4.50 | 13.94 | 0.42 | 0.62 | **3.10** | **1.48** |
| H9_hot_join | 5.90 | 14.99 | 0.63 | 3.26 | **2.54** | **5.19** |

Geometric mean of time A / time B over the queries both ran OK (A is the first one named in the "vs" column; > 1 = A slower):

| engine | format | A / B | queries |
|---|---|---:|---:|
| Trino vs DuckDB | iceberg | **2.36** | 9 |
| Trino vs DuckDB | paimon-as-iceberg | **2.28** | 9 |
| DuckDB | paimon-as-iceberg vs iceberg | **0.14** | 9 |
| Trino | paimon-as-iceberg vs iceberg | **0.14** | 9 |

### snapshot suite (run 20261107T155322Z)

Median of the timed passes, seconds (DuckDB: execute + fetch in the server; Trino: elapsed - queued). Ratio > 1 = Trino slower.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | Trino / DuckDB, iceberg | Trino / DuckDB, paimon |
|---|---:|---:|---:|---:|---:|---:|
| S1_order_items_count | 0.23 | 0.37 | 0.19 | 0.25 | **1.58** | **1.29** |
| S2_items_narrow_agg | 0.55 | 0.44 | 0.54 | 0.49 | **0.80** | **0.92** |
| S3_orders_distinct_customers | 0.89 | 1.66 | 0.86 | 1.52 | **1.87** | **1.77** |
| S4_orders_wide_slice | 0.84 | 0.62 | 0.81 | 0.69 | **0.73** | **0.85** |
| S5_star_join_revenue | 5.87 | 8.22 | 5.28 | 7.17 | **1.40** | **1.36** |
| S6_topn_customers | 1.51 | 1.21 | 1.33 | 0.74 | **0.80** | **0.55** |
| S7_dedup_keys | 2.82 | 8.29 | 2.89 | 8.42 | **2.94** | **2.91** |

Geometric mean of time A / time B over the queries both ran OK (A is the first one named in the "vs" column; > 1 = A slower):

| engine | format | A / B | queries |
|---|---|---:|---:|
| Trino vs DuckDB | iceberg | **1.28** | 7 |
| Trino vs DuckDB | paimon-as-iceberg | **1.21** | 7 |
| DuckDB | paimon-as-iceberg vs iceberg | **0.93** | 7 |
| Trino | paimon-as-iceberg vs iceberg | **0.88** | 7 |

#### snapshot suite, cold start (first pass after both engines' caches were emptied)

One timed sample per query and arm (not a median), so noisier than the warm table above. Seconds; ratios > 1 = the first named slower.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | Trino / DuckDB, iceberg | Trino / DuckDB, paimon | Paimon / Iceberg, DuckDB | Paimon / Iceberg, Trino |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| S1_order_items_count | 0.91 | 4.18 | 0.67 | 1.13 | **4.61** | **1.69** | **0.74** | **0.27** |
| S2_items_narrow_agg | 0.88 | 1.52 | 0.96 | 1.15 | **1.72** | **1.20** | **1.09** | **0.76** |
| S3_orders_distinct_customers | 1.40 | 4.04 | 1.34 | 2.47 | **2.89** | **1.84** | **0.96** | **0.61** |
| S4_orders_wide_slice | 1.52 | 2.13 | 1.61 | 1.91 | **1.40** | **1.18** | **1.06** | **0.89** |
| S5_star_join_revenue | 7.07 | 14.57 | 6.46 | 8.30 | **2.06** | **1.28** | **0.91** | **0.57** |
| S6_topn_customers | 1.38 | 1.66 | 1.63 | 1.29 | **1.21** | **0.79** | **1.18** | **0.78** |
| S7_dedup_keys | 3.04 | 10.48 | 3.55 | 8.45 | **3.45** | **2.38** | **1.17** | **0.81** |

Cold start, geometric mean of time A / time B over the queries both ran OK:

| engine | format | A / B | queries |
|---|---|---:|---:|
| Trino vs DuckDB | iceberg | **2.23** | 7 |
| Trino vs DuckDB | paimon-as-iceberg | **1.40** | 7 |
| DuckDB | paimon-as-iceberg vs iceberg | **1.00** | 7 |
| Trino | paimon-as-iceberg vs iceberg | **0.63** | 7 |

## 3. Ingest

Window 2026-11-07T15:47:07Z to 2026-11-07T16:07:16Z (1,209 s; hot: first pin commit - 300 s .. last pin commit), from each table's own commit history: Iceberg snapshot summaries in the current metadata.json, Paimon `snapshot-N` files (`querybench/ingest_stats.py`). Write commits only; compaction commits are counted apart.

**Throughput** (job-monitor reports overlapping the window: median; in brackets, total rows ingested for rows/s, max for MB/s)

| | Paimon | Iceberg |
|---|---:|---:|
| Rows/s (total rows) | **99,135** (85,880,563) | **101,137** (89,833,758) |
| Data size/s, MB/s | **1.4** (1.5) | **0.6** (1.3) |
| Checkpoint duration, median | 12,794 ms | 652 ms |
| Backpressure HIGH, max share of samples | 0% | 0% |

Rows/s is the ingest rate: the CDC sources' rate meter (`numRecordsOutPerSecond`). Both jobs read the same MySQL change stream, so equal rows/s means both kept up; a lower one means backpressure from its writers. What each format committed (Paimon merges re-written keys first, Iceberg writes every version plus a delete) is in the table-metadata section below.

**From table metadata** (each table's commit history over the window)

| | Paimon | Iceberg |
|---|---:|---:|
| Rows/s (window average) | **38,809** | **52,657** |
| Data size/s (new files) | 1.0 MB/s | 1.1 MB/s |
| Rows committed | 46.9 M | 63.7 M |
| Write commits (median interval) | 80 (60 s) | 84 (60 s) |
| New data files | 4,160 | 609 |
| **New delete files** | 4,160 DV index files, by compaction | **1,218** (609 DVs + 609 equality-delete files) |
| **Equality deletes** | **0** | **49.0 M** |
| Position deletes | none written; compaction marks rows in DVs | 16.2 M (25% of all deletes, as DVs) |
| Compaction commits | 80 (inline) | 0 (compaction job) |

- Paimon committed **74%** of Iceberg's rows: the other 26% were re-writes of a key it merged before committing. Iceberg commits each of them and a delete for the version it replaces.
- Iceberg wrote 65.3 M deletes for 63.7 M committed rows (103% of rows); **25%** were position deletes (DVs: the old row was in the same checkpoint's files), the rest equality deletes, which every read must join by key until a compaction.
- Paimon's DV index files are rewritten per bucket by each inline compaction (80 commits); the live ones at the window end are in 4.
- Source RDS CPU over the window: 34.9% average, 44.9% max.

## 4. Table state

At the window end (the last commit at or before 2026-11-07T16:07:16Z). Both record counts are before deletes are applied: Iceberg `total-records`, Paimon `totalRecordCount` of the live files.

| table | Paimon records | Paimon files / DV-deleted rows | Iceberg records | Iceberg data / delete files | Iceberg eq / pos deletes | snapshots P / I |
|---|---:|---:|---:|---:|---:|---:|
| customers | 42.5 M | 96 / 739,482 | 43.2 M | 624 / 1,140 | 42.8 M / 373,194 | 123 / 62 |
| products | 103.3 M | 449 / 3.1 M | 109.2 M | 504 / 837 | 105.3 M / 3.9 M | 123 / 63 |
| orders | 224.7 M | 432 / 9.7 M | 239.7 M | 212 / 306 | 231.7 M / 8.9 M | 123 / 63 |
| order_items | 271.5 M | 440 / 9.1 M | 300.5 M | 211 / 308 | 286.1 M / 16.0 M | 123 / 63 |

Iceberg holds 1.1× Paimon's records: its superseded rows stay in the data files until a compaction rewrites them; Paimon's compaction drops them as it goes.

## Conclusions

**Bottom line**

- **Hot queries (live tables, merge-on-read): Paimon-as-iceberg fits this use case.** It answered in 0.14× Iceberg's time on DuckDB and 0.14× on Trino (7.1× / 7.4× faster): Paimon's inline compaction keeps the files it exposes merged, while Iceberg applies every equality delete written since its last compaction at read time.
- Snapshot queries on DuckDB: Paimon-as-iceberg took 0.93× Iceberg's time warm and 1.00× cold (Iceberg not ahead here).
- On Trino the snapshot comparison goes the other way: Paimon-as-iceberg took 0.88× Iceberg's time warm and 0.63× cold.
- **The engine matters most in the warm hot pass:** there Trino took **2.4×** DuckDB's time on Iceberg (geometric mean of 9 queries), the largest engine gap in this run.

**Details**

1. Engine (iceberg, hot): Trino / DuckDB = 2.36 over 9 queries.
2. Engine (paimon, hot): Trino / DuckDB = 2.28 over 9 queries.
3. Format (Trino, hot): Paimon-as-iceberg / Iceberg = 0.14 over 9 queries; rows differ on 6.
4. Format (DuckDB, hot): Paimon-as-iceberg / Iceberg = 0.14 over 9 queries; rows differ on 6.
5. Valid (hot): on each format, both engines returned identical rows for every query both ran.
6. Engine (iceberg, snapshot): Trino / DuckDB = 1.28 over 7 queries.
7. Engine (paimon, snapshot): Trino / DuckDB = 1.21 over 7 queries.
8. Format (Trino, snapshot): Paimon-as-iceberg / Iceberg = 0.88 over 7 queries; rows differ on 2.
9. Format (DuckDB, snapshot): Paimon-as-iceberg / Iceberg = 0.93 over 7 queries; rows differ on 2.
10. Cold start, engine (iceberg, snapshot): Trino / DuckDB = 2.23 over 7 queries.
11. Cold start, engine (paimon, snapshot): Trino / DuckDB = 1.40 over 7 queries.
12. Cold start, format (Trino, snapshot): Paimon / Iceberg = 0.63 over 7 queries.
13. Cold start, format (DuckDB, snapshot): Paimon / Iceberg = 1.00 over 7 queries.
14. Valid (snapshot): on each format, both engines returned identical rows for every query both ran.
15. Ingest (job monitor): Paimon 99,135 rows/s, 1.4 MB/s; Iceberg 101,137 rows/s, 0.6 MB/s.
16. Table metadata: Iceberg 52,657 rows/s, Paimon 38,809 rows/s committed; Iceberg added 49.0 M equality deletes and 16.2 M position deletes (DVs), Paimon none (it merges by key; compaction writes DVs).

### Open items

- Iceberg vs Paimon-as-iceberg rows differ on 6 hot queries: the two jobs are pinned at the same moment, not the same binlog point, and Paimon-as-iceberg trails native Paimon by about a checkpoint.
- Iceberg vs Paimon-as-iceberg rows differ on 2 snapshot queries: the two jobs are pinned at the same moment, not the same binlog point, and Paimon-as-iceberg trails native Paimon by about a checkpoint.

---

## Details

<details><summary>hot: Trino server-side breakdown, data scanned, result equality</summary>

**Trino server-side breakdown** (median over the timed passes)

| arm | query | elapsed s | queued s | execution s | CPU s | physical input GB |
|---|---|---|---|---|---|---|
| iceberg | H1_freshness | 8.80 | 0.00 | 8.80 | 78.00 | 0.12 |
| iceberg | H2_hot_count | 8.68 | 0.00 | 8.68 | 62.40 | 0.11 |
| iceberg | H3_hot_needle | 8.32 | 0.00 | 8.32 | 65.40 | 0.16 |
| iceberg | H4_hot_narrow_agg | 5.94 | 0.00 | 5.94 | 12.33 | 0.01 |
| iceberg | H5_hot_wide_row | 5.83 | 0.00 | 5.83 | 12.04 | 0.02 |
| iceberg | H6_hot_product_agg | 8.83 | 0.00 | 8.83 | 66.00 | 0.33 |
| iceberg | H7_hot_status_category_rank | 17.85 | 0.00 | 17.85 | 440.40 | 1.02 |
| iceberg | H8_hot_pk_lookup | 11.01 | 0.00 | 11.01 | 85.80 | 0.11 |
| iceberg | H9_hot_join | 14.99 | 0.00 | 14.99 | 76.80 | 0.26 |
| paimon | H1_freshness | 1.16 | 0.00 | 1.16 | 6.20 | 0.18 |
| paimon | H2_hot_count | 0.81 | 0.00 | 0.81 | 3.23 | 0.13 |
| paimon | H3_hot_needle | 1.05 | 0.00 | 1.05 | 4.55 | 0.31 |
| paimon | H4_hot_narrow_agg | 0.82 | 0.00 | 0.82 | 1.88 | 0.11 |
| paimon | H5_hot_wide_row | 1.41 | 0.00 | 1.41 | 3.85 | 0.27 |
| paimon | H6_hot_product_agg | 1.25 | 0.00 | 1.25 | 8.18 | 0.56 |
| paimon | H7_hot_status_category_rank | 3.96 | 0.00 | 3.96 | 42.15 | 1.28 |
| paimon | H8_hot_pk_lookup | 0.62 | 0.00 | 0.62 | 0.92 | 0.05 |
| paimon | H9_hot_join | 3.26 | 0.00 | 3.26 | 7.92 | 0.52 |

**Data scanned, GB** (DuckDB: HTTPFS bytes in an untimed pass with its cache bypassed; Trino: physical input of the last timed pass, cache hits included)

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg |
|---|---|---|---|---|
| H1_freshness | 0.27 | 0.14 | 0.11 | 0.19 |
| H2_hot_count | 0.20 | 0.13 | 0.09 | 0.13 |
| H3_hot_needle | 0.27 | 0.20 | 0.33 | 0.37 |
| H4_hot_narrow_agg | 0.07 | 0.01 | 0.08 | 0.12 |
| H5_hot_wide_row | 0.10 | 0.02 | 0.29 | 0.30 |
| H6_hot_product_agg | 0.47 | 0.40 | 0.63 | 0.67 |
| H7_hot_status_category_rank | 1.22 | 1.12 | 0.78 | 1.42 |
| H8_hot_pk_lookup | 0.31 | 0.12 | 0.25 | 0.05 |
| H9_hot_join | 0.44 | 0.32 | 0.48 | 0.60 |

**Same rows across arms?** (every row of the last timed pass, compared as text). Letters name distinct result sets per query; equal letters = identical rows. An arm that did not run OK shows its status and is not compared.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | rows |
|---|---|---|---|---|---|
| H1_freshness | A | A | B | B | 1 |
| H2_hot_count | A | A | B | B | 1 |
| H3_hot_needle | A | A | A | A | 0 |
| H4_hot_narrow_agg | A | A | B | B | 5 |
| H5_hot_wide_row | A | A | B | B | 1 |
| H6_hot_product_agg | A | A | A | A | 20 |
| H7_hot_status_category_rank | A | A | B | B | 15 |
| H8_hot_pk_lookup | A | A | A | A | 1 |
| H9_hot_join | A | A | B | B | 5 |

Validity: on each format, every query both engines ran OK returned the same rows.

Iceberg vs Paimon rows differ on: H1_freshness (DuckDB), H1_freshness (Trino), H2_hot_count (DuckDB), H2_hot_count (Trino), H4_hot_narrow_agg (DuckDB), H4_hot_narrow_agg (Trino), H5_hot_wide_row (DuckDB), H5_hot_wide_row (Trino), H7_hot_status_category_rank (DuckDB), H7_hot_status_category_rank (Trino), H9_hot_join (DuckDB), H9_hot_join (Trino). A pipeline gap, not a failure: the two CDC jobs are pinned at the same moment, not at the same binlog point, and Paimon-as-iceberg (its v3 Iceberg metadata: files above level 0 plus deletion vectors) lags native Paimon by about one checkpoint.

First row per arm:

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg |
|---|---|---|---|---|
| H1_freshness | `219308\|219308\|293817\|2026-11-07 15:46:59\|2026-11-07 15:46:59` | `219308\|219308\|293817\|2026-11-07 15:46:59\|2026-11-07 15:46:59` | `219577\|219577\|293877\|2026-11-07 15:46:59\|2026-11-07 15:46:59` | `219577\|219577\|293877\|2026-11-07 15:46:59\|2026-11-07 15:46:59` |
| H2_hot_count | `293817` | `293817` | `293877` | `293877` |
| H3_hot_needle | `-` | `-` | `-` | `-` |
| H4_hot_narrow_agg | `DELIVERED\|44058\|110329410.62` | `DELIVERED\|44058\|110329410.62` | `DELIVERED\|44109\|110461381.03` | `DELIVERED\|44109\|110461381.03` |
| H5_hot_wide_row | `219308\|4\|418631226714993\|219587320981425\|549598117.74\|2026-1` | `219308\|4\|418631226714993\|219587320981425\|549598117.74\|2026-1` | `219577\|4\|419178760206822\|219861553931628\|550250958.01\|2026-1` | `219577\|4\|419178760206822\|219861553931628\|550250958.01\|2026-1` |
| H6_hot_product_agg | `721627738\|31\|169\|73185.85\|30` | `721627738\|31\|169\|73185.85\|30` | `721627738\|31\|169\|73185.85\|30` | `721627738\|31\|169\|73185.85\|30` |
| H7_hot_status_category_rank | `CANCELLED\|1\|Electronics\|12\|4\|4\|69\|28989.55\|177784.59\|0` | `CANCELLED\|1\|Electronics\|12\|4\|4\|69\|28989.55\|177784.59\|0` | `CANCELLED\|1\|Electronics\|12\|4\|4\|69\|28989.55\|177784.59\|0` | `CANCELLED\|1\|Electronics\|12\|4\|4\|69\|28989.55\|177784.59\|0` |
| H8_hot_pk_lookup | `1294860554\|SHIPPED\|3431.65\|GOOGLE_PAY\|2026-11-07 15:46:59` | `1294860554\|SHIPPED\|3431.65\|GOOGLE_PAY\|2026-11-07 15:46:59` | `1294860554\|SHIPPED\|3431.65\|GOOGLE_PAY\|2026-11-07 15:46:59` | `1294860554\|SHIPPED\|3431.65\|GOOGLE_PAY\|2026-11-07 15:46:59` |
| H9_hot_join | `CANCELLED\|60\|5\|342\|177784.59` | `CANCELLED\|60\|5\|342\|177784.59` | `CANCELLED\|60\|5\|342\|177784.59` | `CANCELLED\|60\|5\|342\|177784.59` |

</details>

<details><summary>hot: pinned tables, data and delete files (tables-20261107T151845Z.tsv)</summary>

Per format and table, its pins in this suite (hot: one per pass, ranges over them; snapshot: one). `paimon` = the Paimon-as-iceberg metadata; `paimon_native` = the native Paimon snapshot it was written from (records only).

| format | table | pins | snapshot id (last) | records (rows) | data files | delete files | equality deletes (rows) | position deletes (rows, incl. DVs) | GB |
|---|---|---|---|---|---|---|---|---|---|
| iceberg | customers | 4 | 1935729399052521439 | 42702741..43190315 | 444..624 | 780..1140 | 42430205..42817121 | 272536..373194 | 1.74 |
| iceberg | products | 4 | 3111385128491957554 | 104122668..109201913 | 369..504 | 567..837 | 101443974..105274977 | 2678694..3926936 | 3.21 |
| iceberg | orders | 4 | 1582900841775555951 | 224950339..239659887 | 152..212 | 186..306 | 220260207..231729810 | 5279372..8933015 | 4.70 |
| iceberg | order_items | 4 | 5487422869023695912 | 278921019..300520476 | 151..211 | 188..308 | 269432541..286142120 | 10453954..16001013 | 5.14 |
| paimon | customers | 4 | 96 | 8344458..8380099 | 0..16 | 88..95 | 0 | 491771..739482 | 0.33 |
| paimon | products | 4 | 96 | 44380336..45110038 | 0..52 | 310..429 | 0 | 2343525..3124711 | 1.38 |
| paimon | orders | 4 | 96 | 76515245..80171166 | 0 | 291..364 | 0 | 6003032..9669812 | 1.50 |
| paimon | order_items | 4 | 96 | 86058396..94296046 | 0 | 327..409 | 0 | 6705668..9365807 | 1.43 |

Every pin (tables TSV):

| pin_tag | format | table | snapshot_id | commit_ms | total-records | total-data-files | total-delete-files | total-equality-deletes | total-position-deletes | total-files-size | note |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20261107T151845Z-p0 | iceberg | customers | 5751528006818421965 | 1794066735054 | 42702741 | 444 | 780 | 42430205 | 272536 | 1721631829 | format-version=3 operation=overwrite |
| 20261107T151845Z-p0 | paimon | customers | 65 | 1794066727737 | 8371197 | 0 | 95 | 0 | 491771 | 330320701 | format-version=3 operation=append |
| 20261107T151845Z-p0 | iceberg | products | 6372650357700397182 | 1794066735091 | 104122668 | 369 | 567 | 101443974 | 2678694 | 3073494685 | format-version=3 operation=overwrite |
| 20261107T151845Z-p0 | paimon | products | 66 | 1794066731339 | 44380336 | 52 | 429 | 0 | 2343525 | 1350392986 | format-version=3 operation=replace |
| 20261107T151845Z-p0 | iceberg | orders | 6516185483478200321 | 1794066735096 | 224950339 | 152 | 186 | 220260207 | 5279372 | 4398261254 | format-version=3 operation=overwrite |
| 20261107T151845Z-p0 | paimon | orders | 66 | 1794066733174 | 80171166 | 0 | 304 | 0 | 6003032 | 1606132904 | format-version=3 operation=replace |
| 20261107T151845Z-p0 | iceberg | order_items | 3350050811936924418 | 1794066735118 | 278921019 | 151 | 188 | 269432541 | 10453954 | 4726933437 | format-version=3 operation=overwrite |
| 20261107T151845Z-p0 | paimon | order_items | 66 | 1794066732184 | 94296046 | 0 | 327 | 0 | 6705668 | 1637650163 | format-version=3 operation=replace |
| 20261107T151845Z-p1 | iceberg | customers | 5856778165900679065 | 1794066975427 | 42849836 | 492 | 876 | 42540964 | 308872 | 1727652336 | format-version=3 operation=overwrite |
| 20261107T151845Z-p1 | paimon | customers | 75 | 1794067007671 | 8380099 | 0 | 95 | 0 | 603631 | 330611682 | format-version=3 operation=append |
| 20261107T151845Z-p1 | iceberg | products | 6612185160665936655 | 1794066975504 | 106207156 | 405 | 639 | 103002307 | 3204849 | 3131099390 | format-version=3 operation=overwrite |
| 20261107T151845Z-p1 | paimon | products | 75 | 1794067007681 | 44722176 | 46 | 384 | 0 | 2695538 | 1362686649 | format-version=3 operation=append |
| 20261107T151845Z-p1 | iceberg | orders | 7155996196353442746 | 1794066975456 | 230448074 | 168 | 218 | 224454905 | 6737142 | 4510989305 | format-version=3 operation=overwrite |
| 20261107T151845Z-p1 | paimon | orders | 76 | 1794067011118 | 77986332 | 0 | 291 | 0 | 7123688 | 1546239014 | format-version=3 operation=replace |
| 20261107T151845Z-p1 | iceberg | order_items | 2526360082397492470 | 1794066975757 | 286431854 | 167 | 220 | 275167266 | 12454010 | 4870946956 | format-version=3 operation=overwrite |
| 20261107T151845Z-p1 | paimon | order_items | 76 | 1794067012126 | 90770442 | 0 | 362 | 0 | 7456288 | 1547587981 | format-version=3 operation=replace |
| 20261107T151845Z-p2 | iceberg | customers | 7933211820158965335 | 1794067275430 | 43069551 | 552 | 996 | 42717149 | 352402 | 1736736846 | format-version=3 operation=overwrite |
| 20261107T151845Z-p2 | paimon | customers | 84 | 1794067251501 | 8344458 | 16 | 88 | 0 | 655020 | 326995938 | format-version=3 operation=append |
| 20261107T151845Z-p2 | iceberg | products | 8901880335951130605 | 1794067275416 | 108116320 | 450 | 729 | 104440735 | 3675585 | 3184098881 | format-version=3 operation=overwrite |
| 20261107T151845Z-p2 | paimon | products | 85 | 1794067308804 | 44821212 | 50 | 310 | 0 | 2792046 | 1368046581 | format-version=3 operation=append |
| 20261107T151845Z-p2 | iceberg | orders | 1102556567688353626 | 1794067275373 | 235916466 | 188 | 258 | 228716812 | 8097368 | 4623473433 | format-version=3 operation=overwrite |
| 20261107T151845Z-p2 | paimon | orders | 85 | 1794067308781 | 77629696 | 0 | 345 | 0 | 8943420 | 1533231474 | format-version=3 operation=append |
| 20261107T151845Z-p2 | iceberg | order_items | 3638212495164704147 | 1794067275371 | 294610191 | 187 | 260 | 281475013 | 14575563 | 5028481068 | format-version=3 operation=overwrite |
| 20261107T151845Z-p2 | paimon | order_items | 86 | 1794067312359 | 88848436 | 0 | 409 | 0 | 9365807 | 1493943072 | format-version=3 operation=replace |
| 20261107T151845Z-p3 | iceberg | customers | 1935729399052521439 | 1794067636263 | 43190315 | 624 | 1140 | 42817121 | 373194 | 1742007264 | format-version=3 operation=overwrite |
| 20261107T151845Z-p3 | paimon | customers | 96 | 1794067612251 | 8376821 | 0 | 88 | 0 | 739482 | 328509054 | format-version=3 operation=replace |
| 20261107T151845Z-p3 | iceberg | products | 3111385128491957554 | 1794067635471 | 109201913 | 504 | 837 | 105274977 | 3926936 | 3214794558 | format-version=3 operation=overwrite |
| 20261107T151845Z-p3 | paimon | products | 96 | 1794067613343 | 45110038 | 0 | 377 | 0 | 3124711 | 1376966977 | format-version=3 operation=replace |
| 20261107T151845Z-p3 | iceberg | orders | 1582900841775555951 | 1794067635671 | 239659887 | 212 | 306 | 231729810 | 8933015 | 4701299619 | format-version=3 operation=overwrite |
| 20261107T151845Z-p3 | paimon | orders | 96 | 1794067614318 | 76515245 | 0 | 364 | 0 | 9669812 | 1497504331 | format-version=3 operation=replace |
| 20261107T151845Z-p3 | iceberg | order_items | 5487422869023695912 | 1794067635515 | 300520476 | 211 | 308 | 286142120 | 16001013 | 5143131040 | format-version=3 operation=overwrite |
| 20261107T151845Z-p3 | paimon | order_items | 96 | 1794067615309 | 86058396 | 0 | 376 | 0 | 9050683 | 1430608625 | format-version=3 operation=replace |

</details>

<details><summary>hot: pins, gates and windows (hot-interleave-20261107T151845Z.pins)</summary>

```
stamp=20261107T151845Z window=[2026-11-07 15:46:30, 2026-11-07 15:47:00) dt=2026-11-07 repin=pass engines=duckdb,trino formats=iceberg,paimon
iceberg_format_version=3
cdc_db_suffix=_v3final iceberg_db=flink_iceberg_db_v3final paimon_db=flink_paimon_db_v3final engines=duckdb,trino formats=iceberg,paimon
duckdb_image=<id>.dkr.ecr.us-west-2.amazonaws.com/duckdb-cdc-server:2.0.0 duckdb_cpu=81 duckdb_pod_memory=342Gi duckdb_skip_puffin_verification_arms=paimon
trino_version=484 trino_catalog=iceberg trino_paimon_catalog=paimon trino_workers=9 paimon_reader=iceberg
duckdb_cpu=81 duckdb_memory=342Gi duckdb_ec2=m5.24xlarge trino_worker_cpu=9 trino_worker_memory=38Gi trino_worker_ec2=m5.4xlarge trino_coord_cpu=4 trino_coord_memory=16Gi trino_coord_ec2=m5.2xlarge
generator_before=mysql-upsert-loadgen=24/24
data_wait_s=57
## pins 20261107T151845Z-p0
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794066735054
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794066735096
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794066735118
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794066735091
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794066727737
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794066733174
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794066732184
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794066731339
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p0/iceberg/customers/00034-12481427-5ff5-4da2-91d4-9d8c4ffc29dc.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p0/iceberg/orders/00035-6cf3ec8c-35b3-4b2e-b4c9-4f53d5b7f89e.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p0/iceberg/order_items/00035-f1720e72-5eee-475a-b6f8-158ab1741898.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p0/iceberg/products/00035-0293ce7b-fd19-4b79-a543-2231a3807d75.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p0/paimon/customers/v65.metadata.json
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p0/paimon/orders/v66.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p0/paimon/order_items/v66.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p0/paimon/products/v66.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3final
QB_PIN_SNAP_ICEBERG_CUSTOMERS=5751528006818421965
QB_PIN_SNAP_ICEBERG_ORDERS=6516185483478200321
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=3350050811936924418
QB_PIN_SNAP_ICEBERG_PRODUCTS=6372650357700397182
QB_PIN_SNAP_PAIMON_CUSTOMERS=65
QB_PIN_SNAP_PAIMON_ORDERS=66
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=66
QB_PIN_SNAP_PAIMON_PRODUCTS=66
h8_key order_id=1294860554 dt=2026-11-07 (picked on trino:iceberg (pinned orders), not confirmed on the other arms (the summary compares their rows); partition = HOT_DT)
## pins 20261107T151845Z-p1
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794066975427
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794066975456
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794066975757
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794066975504
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794067007671
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794067011118
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794067012126
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794067007681
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p1/iceberg/customers/00038-222b3e4e-5152-4769-a231-1deedff03787.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p1/iceberg/orders/00039-8c09d8d8-f4be-42e6-83d4-f2b753bb82e3.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p1/iceberg/order_items/00039-c898f4c4-721b-4cda-a462-823564a79a87.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p1/iceberg/products/00039-dc94e940-3c74-4643-86dc-7b3c59759d6c.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p1/paimon/customers/v75.metadata.json
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p1/paimon/orders/v76.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p1/paimon/order_items/v76.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p1/paimon/products/v75.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3final
QB_PIN_SNAP_ICEBERG_CUSTOMERS=5856778165900679065
QB_PIN_SNAP_ICEBERG_ORDERS=7155996196353442746
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=2526360082397492470
QB_PIN_SNAP_ICEBERG_PRODUCTS=6612185160665936655
QB_PIN_SNAP_PAIMON_CUSTOMERS=75
QB_PIN_SNAP_PAIMON_ORDERS=76
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=76
QB_PIN_SNAP_PAIMON_PRODUCTS=75
## pins 20261107T151845Z-p2
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794067275430
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794067275373
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794067275371
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794067275416
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794067251501
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794067308781
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794067312359
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794067308804
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p2/iceberg/customers/00043-104fc15d-69a2-49a2-9bf3-0d5115f3fab3.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p2/iceberg/orders/00044-c5d4d800-f63a-46a1-ab2e-2b4b39399c08.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p2/iceberg/order_items/00044-712b8046-97ad-417e-955b-e059349f8adc.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p2/iceberg/products/00044-16f368a9-f7b2-4d5c-9216-030afb338674.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p2/paimon/customers/v84.metadata.json
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p2/paimon/orders/v85.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p2/paimon/order_items/v86.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p2/paimon/products/v85.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3final
QB_PIN_SNAP_ICEBERG_CUSTOMERS=7933211820158965335
QB_PIN_SNAP_ICEBERG_ORDERS=1102556567688353626
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=3638212495164704147
QB_PIN_SNAP_ICEBERG_PRODUCTS=8901880335951130605
QB_PIN_SNAP_PAIMON_CUSTOMERS=84
QB_PIN_SNAP_PAIMON_ORDERS=85
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=86
QB_PIN_SNAP_PAIMON_PRODUCTS=85
## pins 20261107T151845Z-p3
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794067636263
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794067635671
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794067635515
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794067635471
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794067612251
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794067614318
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794067615309
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794067613343
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p3/iceberg/customers/00049-44839b3f-494f-4f47-a115-cd7da19e40d9.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p3/iceberg/orders/00050-d87633e2-1901-4ef3-a19d-32ca8e88e578.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p3/iceberg/order_items/00050-1e8125a8-d05e-4b6c-8a16-8b49d2cb02dc.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p3/iceberg/products/00050-4cf360d5-eb44-4ee0-a473-9afe5e5af1e2.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p3/paimon/customers/v96.metadata.json
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p3/paimon/orders/v96.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p3/paimon/order_items/v96.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T151845Z-p3/paimon/products/v96.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3final
QB_PIN_SNAP_ICEBERG_CUSTOMERS=1935729399052521439
QB_PIN_SNAP_ICEBERG_ORDERS=1582900841775555951
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=5487422869023695912
QB_PIN_SNAP_ICEBERG_PRODUCTS=3111385128491957554
QB_PIN_SNAP_PAIMON_CUSTOMERS=96
QB_PIN_SNAP_PAIMON_ORDERS=96
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=96
QB_PIN_SNAP_PAIMON_PRODUCTS=96
generator_after=mysql-upsert-loadgen=24/24
```
</details>

<details><summary>snapshot: Trino server-side breakdown, data scanned, result equality</summary>

**Trino server-side breakdown** (median over the timed passes)

| arm | query | elapsed s | queued s | execution s | CPU s | physical input GB |
|---|---|---|---|---|---|---|
| iceberg | S1_order_items_count | 0.37 | 0.00 | 0.37 | 4.18 | 0.07 |
| iceberg | S2_items_narrow_agg | 0.44 | 0.00 | 0.44 | 15.65 | 0.56 |
| iceberg | S3_orders_distinct_customers | 1.66 | 0.00 | 1.66 | 84.30 | 0.47 |
| iceberg | S4_orders_wide_slice | 0.62 | 0.00 | 0.62 | 19.35 | 1.35 |
| iceberg | S5_star_join_revenue | 8.22 | 0.00 | 8.22 | 330.60 | 2.81 |
| iceberg | S6_topn_customers | 1.21 | 0.00 | 1.21 | 31.67 | 0.93 |
| iceberg | S7_dedup_keys | 8.29 | 0.00 | 8.29 | 498.90 | 0.91 |
| paimon | S1_order_items_count | 0.25 | 0.00 | 0.25 | 2.08 | 0.06 |
| paimon | S2_items_narrow_agg | 0.49 | 0.00 | 0.49 | 10.27 | 0.56 |
| paimon | S3_orders_distinct_customers | 1.52 | 0.00 | 1.52 | 54.32 | 0.47 |
| paimon | S4_orders_wide_slice | 0.69 | 0.00 | 0.69 | 11.30 | 1.37 |
| paimon | S5_star_join_revenue | 7.17 | 0.00 | 7.17 | 354.00 | 3.05 |
| paimon | S6_topn_customers | 0.74 | 0.00 | 0.74 | 15.09 | 0.85 |
| paimon | S7_dedup_keys | 8.42 | 0.00 | 8.42 | 509.40 | 0.86 |

**Data scanned, GB** (DuckDB: HTTPFS bytes in an untimed pass with its cache bypassed; Trino: physical input of the last timed pass, cache hits included)

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg |
|---|---|---|---|---|
| S1_order_items_count | 0.07 | 0.07 | 0.07 | 0.06 |
| S2_items_narrow_agg | 0.55 | 0.56 | 0.55 | 0.56 |
| S3_orders_distinct_customers | 0.46 | 0.47 | 0.47 | 0.47 |
| S4_orders_wide_slice | 1.33 | 1.35 | 1.38 | 1.37 |
| S5_star_join_revenue | 2.85 | 2.79 | 2.86 | 3.05 |
| S6_topn_customers | 0.91 | 0.93 | 0.85 | 0.85 |
| S7_dedup_keys | 0.86 | 0.91 | 0.86 | 0.86 |

**Same rows across arms?** (every row of the last timed pass, compared as text). Letters name distinct result sets per query; equal letters = identical rows. An arm that did not run OK shows its status and is not compared.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | rows |
|---|---|---|---|---|---|
| S1_order_items_count | A | A | A | A | 1 |
| S2_items_narrow_agg | A | A | A | A | 10 |
| S3_orders_distinct_customers | A | A | B | B | 5 |
| S4_orders_wide_slice | A | A | A | A | 1 |
| S5_star_join_revenue | A | A | A | A | 180 |
| S6_topn_customers | A | A | A | A | 20 |
| S7_dedup_keys | A | A | B | B | 4 |

Validity: on each format, every query both engines ran OK returned the same rows.

Iceberg vs Paimon rows differ on: S3_orders_distinct_customers (DuckDB), S3_orders_distinct_customers (Trino), S7_dedup_keys (DuckDB), S7_dedup_keys (Trino). A pipeline gap, not a failure: the two CDC jobs are pinned at the same moment, not at the same binlog point, and Paimon-as-iceberg (its v3 Iceberg metadata: files above level 0 plus deletion vectors) lags native Paimon by about one checkpoint.

First row per arm:

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg |
|---|---|---|---|---|
| S1_order_items_count | `145669862\|801222841` | `145669862\|801222841` | `145669862\|801222841` | `145669862\|801222841` |
| S2_items_narrow_agg | `1\|14569559\|6593148941.96` | `1\|14569559\|6593148941.96` | `1\|14569559\|6593148941.96` | `1\|14569559\|6593148941.96` |
| S3_orders_distinct_customers | `APPLE_PAY\|25150780\|81764` | `APPLE_PAY\|25150780\|81764` | `APPLE_PAY\|25150781\|81764` | `APPLE_PAY\|25150781\|81764` |
| S4_orders_wide_slice | `11546061\|633800923\|28928516969.59\|8142302030571960\|168273498` | `11546061\|633800923\|28928516969.59\|8142302030571960\|168273498` | `11546061\|633800923\|28928516969.59\|8142302030571960\|168273498` | `11546061\|633800923\|28928516969.59\|8142302030571960\|168273498` |
| S5_star_join_revenue | `2026-11-05\|Automotive\|AZ\|46699\|256699\|116257189.94` | `2026-11-05\|Automotive\|AZ\|46699\|256699\|116257189.94` | `2026-11-05\|Automotive\|AZ\|46699\|256699\|116257189.94` | `2026-11-05\|Automotive\|AZ\|46699\|256699\|116257189.94` |
| S6_topn_customers | `796883782\|Emily Davis\|WA\|4574\|11674519.35` | `796883782\|Emily Davis\|WA\|4574\|11674519.35` | `796883782\|Emily Davis\|WA\|4574\|11674519.35` | `796883782\|Emily Davis\|WA\|4574\|11674519.35` |
| S7_dedup_keys | `customers\|41787386\|41787386\|0` | `customers\|41787386\|41787386\|0` | `customers\|41787386\|41787386\|0` | `customers\|41787386\|41787386\|0` |

</details>

<details><summary>snapshot: pinned tables, data and delete files (tables-20261107T155322Z.tsv)</summary>

Per format and table, its pins in this suite (hot: one per pass, ranges over them; snapshot: one). `paimon` = the Paimon-as-iceberg metadata; `paimon_native` = the native Paimon snapshot it was written from (records only).

| format | table | pins | snapshot id (last) | records (rows) | data files | delete files | equality deletes (rows) | position deletes (rows, incl. DVs) | GB |
|---|---|---|---|---|---|---|---|---|---|
| iceberg | customers | 1 | 2772722997968864374 | 41787386 | 72 | 0 | 0 | 0 | 1.54 |
| iceberg | products | 1 | 9216884910701371249 | 101274561 | 117 | 0 | 0 | 0 | 2.66 |
| iceberg | orders | 1 | 524174069884234056 | 217242877 | 84 | 0 | 0 | 0 | 3.90 |
| iceberg | order_items | 1 | 2065909799771631067 | 265257556 | 91 | 0 | 0 | 0 | 4.07 |
| paimon | customers | 1 | 123 | 7514705 | 0 | 0 | 0 | 0 | 0.30 |
| paimon | products | 1 | 123 | 42205027 | 0 | 0 | 0 | 0 | 1.31 |
| paimon | orders | 1 | 123 | 63337900 | 0 | 0 | 0 | 0 | 1.21 |
| paimon | order_items | 1 | 123 | 71560701 | 0 | 0 | 0 | 0 | 1.20 |

Every pin (tables TSV):

| pin_tag | format | table | snapshot_id | commit_ms | total-records | total-data-files | total-delete-files | total-equality-deletes | total-position-deletes | total-files-size | note |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20261107T155322Z | iceberg | customers | 2772722997968864374 | 1794069146171 | 41787386 | 72 | 0 | 0 | 0 | 1538915995 | format-version=3 operation=replace |
| 20261107T155322Z | paimon | customers | 123 | 1794068588992 | 7514705 | 0 | 0 | 0 | 0 | 295792271 | format-version=3 operation=replace |
| 20261107T155322Z | iceberg | products | 9216884910701371249 | 1794069299159 | 101274561 | 117 | 0 | 0 | 0 | 2660013015 | format-version=3 operation=replace |
| 20261107T155322Z | paimon | products | 123 | 1794068761423 | 42205027 | 0 | 0 | 0 | 0 | 1314448788 | format-version=3 operation=replace |
| 20261107T155322Z | iceberg | orders | 524174069884234056 | 1794069682527 | 217242877 | 84 | 0 | 0 | 0 | 3901985222 | format-version=3 operation=replace |
| 20261107T155322Z | paimon | orders | 123 | 1794068916814 | 63337900 | 0 | 0 | 0 | 0 | 1205171347 | format-version=3 operation=replace |
| 20261107T155322Z | iceberg | order_items | 2065909799771631067 | 1794069930940 | 265257556 | 91 | 0 | 0 | 0 | 4073606464 | format-version=3 operation=replace |
| 20261107T155322Z | paimon | order_items | 123 | 1794069070007 | 71560701 | 0 | 0 | 0 | 0 | 1204921719 | format-version=3 operation=replace |

</details>

<details><summary>snapshot: pins, gates and windows (snapshot-20261107T155322Z.pins)</summary>

```
stamp=20261107T155322Z suite=snapshot dt=2026-11-06 engines=duckdb,trino formats=iceberg,paimon
iceberg_format_version=3
cdc_db_suffix=_v3final iceberg_db=flink_iceberg_db_v3final paimon_db=flink_paimon_db_v3final engines=duckdb,trino formats=iceberg,paimon
duckdb_image=<id>.dkr.ecr.us-west-2.amazonaws.com/duckdb-cdc-server:2.0.0 duckdb_cpu=81 duckdb_pod_memory=342Gi duckdb_skip_puffin_verification_arms=paimon
trino_version=484 trino_catalog=iceberg trino_paimon_catalog=paimon trino_workers=9 paimon_reader=iceberg
duckdb_cpu=81 duckdb_memory=342Gi duckdb_ec2=m5.24xlarge trino_worker_cpu=9 trino_worker_memory=38Gi trino_worker_ec2=m5.4xlarge trino_coord_cpu=4 trino_coord_memory=16Gi trino_coord_ec2=m5.2xlarge
started_at=2026-11-07 16:56:06Z
gate_writers_stopped_at=2026-11-07 16:56:06Z writers=flink-cdc-paimon=suspended flink-cdc-iceberg=suspended generator=mysql-upsert-loadgen=/0
gate_engines_ready_at=2026-11-07 16:56:08Z
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794069146171
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794069682527
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794069930940
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794069299159
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794068588992
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794068916814
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794069070007
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794068761423
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T155322Z/iceberg/customers/00062-df44a1ae-ee8b-424e-91ec-d73416e1746b.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T155322Z/iceberg/orders/00063-d7745ecb-ab01-4a78-ab49-fb536a720671.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T155322Z/iceberg/order_items/00063-b175a86b-1194-4657-8397-b8c0e36eed49.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T155322Z/iceberg/products/00063-6a692ad8-3f58-4325-b8ed-66ffbffe2cd5.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T155322Z/paimon/customers/v123.metadata.json
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T155322Z/paimon/orders/v123.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T155322Z/paimon/order_items/v123.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3final/20261107T155322Z/paimon/products/v123.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3final
QB_PIN_SNAP_ICEBERG_CUSTOMERS=2772722997968864374
QB_PIN_SNAP_ICEBERG_ORDERS=524174069884234056
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=2065909799771631067
QB_PIN_SNAP_ICEBERG_PRODUCTS=9216884910701371249
QB_PIN_SNAP_PAIMON_CUSTOMERS=123
QB_PIN_SNAP_PAIMON_ORDERS=123
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=123
QB_PIN_SNAP_PAIMON_PRODUCTS=123
pinned_at=2026-11-07 16:56:38Z
slice=[2026-11-06 04:10:00, 2026-11-06 04:20:00) (10 min, the busiest bucket of the partition (rows: iceberg 11546061, paimon 11546061; 53 non-empty buckets); formats: iceberg paimon)
caches_cleared_at=2026-11-07 16:58:22Z
passes=3 (pass 0 cold, timed; median of the other 2)
duckdb_finished_at=2026-11-07 17:04:00Z rc=0
trino_finished_at=2026-11-07 17:08:25Z rc=0
finished_at=2026-11-07 17:08:25Z rc=0
```
</details>

<details><summary>Ingest per table (2026-11-07T15:47:07Z to 2026-11-07T16:07:16Z)</summary>

| format | table | rows/s | rows | write commits | median interval s | data files | delete files | eq deletes (rows) | pos deletes (rows) | DVs | compaction commits | first / last commit |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| iceberg | customers | 601 | 726,319 | 21 | 60.0 | 252 | 504 | 564,477 | 161,842 | 252 | 0 | 15:47:15Z / 16:07:16Z |
| iceberg | products | 6,253 | 7,559,805 | 21 | 60.1 | 189 | 378 | 5,691,397 | 1,868,408 | 189 | 0 | 15:47:14Z / 16:07:15Z |
| iceberg | orders | 18,321 | 22,150,310 | 21 | 60.1 | 84 | 168 | 17,279,932 | 5,487,960 | 84 | 0 | 15:47:14Z / 16:07:15Z |
| iceberg | order_items | 27,482 | 33,226,322 | 21 | 60.1 | 84 | 168 | 25,493,481 | 8,729,807 | 84 | 0 | 15:47:14Z / 16:07:15Z |
| paimon | customers | 449 | 542,880 | 20 | 59.9 | 160 | 160 | 0 | - | - | 20 | 15:47:57Z / 16:06:52Z |
| paimon | products | 4,473 | 5,408,107 | 20 | 60.0 | 1,440 | 1,440 | 0 | - | - | 20 | 15:47:57Z / 16:06:53Z |
| paimon | orders | 13,627 | 16,475,334 | 20 | 60.0 | 1,280 | 1,280 | 0 | - | - | 20 | 15:47:57Z / 16:06:54Z |
| paimon | order_items | 20,260 | 24,494,304 | 20 | 59.9 | 1,280 | 1,280 | 0 | - | - | 20 | 15:47:57Z / 16:06:55Z |

Job-monitor aggregate (ingest_stats.py, reports overlapping the window):

| field | Paimon | Iceberg |
|---|---:|---:|
| backpressured_ratio_max | 0.0 | 0.0 |
| bytes_per_s_max | 1492090.55 | 1298141.29 |
| bytes_per_s_median | 1359650 | 641153 |
| checkpoint_ms_median | 12794 | 652 |
| committed_rows_per_s_median | 47339 | 59036 |
| first_start | 2026-11-07T15:46:34.603224Z | 2026-11-07T15:46:34.604446Z |
| last_end | 2026-11-07T16:07:55.815914Z | 2026-11-07T16:07:55.927718Z |
| reports | 11 | 11 |
| rows_per_s_max | 109270.39 | 110261.97 |
| rows_per_s_median | 99135 | 101137 |
| rows_total | 85880563 | 89833758 |
| sampled_s | 1320 | 1320 |

</details>

