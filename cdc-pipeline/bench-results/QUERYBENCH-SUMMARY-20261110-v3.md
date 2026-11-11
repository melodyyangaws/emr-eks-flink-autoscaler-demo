# CDC query benchmark: 20261110-v3

Warm DuckDB vs Trino on the four CDC tables Flink writes, on native Iceberg and on Paimon-as-iceberg (cdc-pipeline/querybench/); both engines of a format read the same pinned snapshot per table.

**Run configuration**

| | |
|---|---|
| Open Table Format(OTF) | Iceberg v3, Flink writer Apache Iceberg 1.12.0 (`iceberg-flink-runtime-1.20`, replacing EMR's bundled 1.10.1); Apache Paimon 2.1.0 (`paimon-flink-1.20`, patched with the apache/paimon PR #10329 backport, + `paimon-s3`). Both are added as jars to `/usr/lib/flink/lib` of the EMR on EKS 7.14.0 Flink base image (`public.ecr.aws/emr-on-eks/flink/emr-7.14.0-flink`) by `ingest-deploy/docker/Dockerfile.cdc-paimon` |
| Flink writers | Paimon and Iceberg v3 jobs alike: parallelism 12 -> 6 TMs × 2 slots (8 CPU / 32Gi each TM), checkpoint every 60s |
| Ingest window | 2026-11-10T16:07:48Z to 2026-11-10T16:24:01Z |
| Source | RDS mysql 8.0.46-rds.20260624, `db.r6i.4xlarge`, gp3 64000 IOPS / 500 GB |
| Generator | `mysql-upsert-loadgen`, 24 pods × `RATE_PER_POD=80000` (target 1,920,000 rows/s); `RECENT_KEY_RATIO=0.3` & `UPSERT_RATIO=0.9`: settings to produce records with 30% of recent re-writes, 63% older-key updates, 7% inserts; actual about **38,400 rows/s** across the 4 tables (the generator's own counters, summed over its 24 pods, 16:07:48-16:24:01Z: ~34,300 updates, ~2,700 inserts, ~800 rows deleted, 0 errors); the CDC sources read ~55,700 change events/s (Paimon 56,186, Iceberg 55,132, no backpressure), more than the rows because an UPDATE is 2 events, -U and +U |
| Query Engines | DuckDB 2.0.0 (81 CPU / 342Gi, on one `m5.24xlarge`), warm; Trino 484: 9 workers × 9 CPU / 38 Gi (total **81 CPU / 342 Gi**), one `m5.4xlarge` each; coordinator 4 CPU / 16Gi on `m5.2xlarge` (schedules no splits) |
| Table set | 2 databases with a session suffix v3 (4 tables in each DB): `flink_iceberg_db_v3` / `flink_paimon_db_v3`; Paimon has iceberg integration turned on by default, so downstream tools can either use Iceberg connector to query Paimon's Iceberg metadata or use Paimon connector to query Paimon's native metadata |
| Buckets | Both OTF formats split each table into the same fixed number of buckets by its ID: customers 8, products 8 per category, orders 16 per day, order items 16 per day. Each bucket is written by one Flink writer. |
| Hot run | 9 queries, about 16 min of querying; data window 16:06:30 to 16:07:00, queried 4 times (warm-up + 3 timed), each on tables re-pinned at the start of the pass (commits 16:12:48Z to 16:24:01Z) |
| Snapshot run | 7 queries, about 9 min of querying; 1 pin set (after the writers stopped), the pinned snapshots committed 16:43:19Z to 17:08:15Z |

---

## 1. Headline

- **What was tested:** the same stream of online-shop orders was copied, as it happened, into two storage formats (Iceberg and Paimon), and the same questions were then asked of each with two query tools (DuckDB on one large server, Trino on a cluster of the same total size).
- **Questions about live, still-changing data (hot query):** with DuckDB, Paimon was about **7 times faster** than Iceberg; with Trino, Paimon was about **41% faster** than Iceberg. Paimon merges each change into its files while loading (ongoing compaction); Iceberg leaves that merging for later, so every question has to sort through the latest changes first.
- **Questions about settled data (snapshot test: ingestion stopped and both formats were fully compacted, i.e. every pending change merged into clean files):** with DuckDB, Paimon was about **52% faster** than Iceberg; with Trino, Paimon and Iceberg were about equally fast. After full compaction, the two formats are close.
- **The query tool matters too:** on the bucketed Iceberg tables, Trino was about **3.5× faster** on live data and **1.2× faster** on settled data; on Paimon, DuckDB was about **1.4× faster** on live data and **1.3× faster** on settled data; on the very first question after a cold start DuckDB was faster on both (Trino took about **1.7× as long**).
- **Loading the data:** both formats kept up with the incoming orders, at about 56,000 change events a second each (the generator wrote ~38,400 rows/s; an UPDATE is 2 events).
- **Why live queries are slower on Iceberg:** for every changed order, Iceberg writes the new version plus a note saying "ignore the old one" (26 million such notes in this test). Until a compaction merges them away, every question has to read those notes. Paimon replaces old versions as it goes, so it has none.

**Speed: geometric mean of the time ratios**

Per pair: the geometric mean of time A / time B over the queries both ran OK (A is named first; > 1 = A slower, < 1 = A faster).

| suite | Trino / DuckDB, Iceberg | Trino / DuckDB, Paimon | Paimon / Iceberg, DuckDB | Paimon / Iceberg, Trino | queries |
|---|---:|---:|---:|---:|---:|
| Hot, warm | **0.29** | **1.45** | **0.14** | **0.71** | 9 |
| Snapshot, warm | **0.81** | **1.34** | **0.66** | **1.08** | 7 |
| Snapshot, cold start | **1.65** | **1.71** | **0.70** | **0.72** | 7 |

**Estimated cost**: A's total query cost against B's for the same suite (the total rows of the latency tables: query time × the engines' EC2 on-demand price). Comparing formats on one engine, cost follows time; comparing engines, Trino's cluster costs 58% more per second than DuckDB's node.

| suite | Trino / DuckDB, Iceberg | Trino / DuckDB, Paimon | Paimon / Iceberg, DuckDB | Paimon / Iceberg, Trino |
|---|---:|---:|---:|---:|
| Hot, warm | 44% cheaper | 144% more | 86% cheaper | 40% cheaper |
| Snapshot, warm | 67% more | 117% more | 22% cheaper | about the same |
| Snapshot, cold start | 194% more | 131% more | 20% cheaper | 37% cheaper |

<details><summary>The same results, technical version</summary>

- On the Iceberg tables, Trino took **0.29×** warm DuckDB's time (hot suite, geometric mean of 9 queries).
- On Paimon-as-iceberg, Trino took **1.45×** warm DuckDB's time (hot suite, geometric mean of 9 queries).
- On Trino, Paimon-as-iceberg answered in **0.71×** Iceberg's time (9 queries); its rows differed from Iceberg's on 7 of 9 queries (pinned at the same moment, not the same binlog point; Paimon-as-iceberg trails native Paimon by about a checkpoint).
- On DuckDB, Paimon-as-iceberg answered in **0.14×** Iceberg's time (9 queries); its rows differed from Iceberg's on 7 of 9 queries (pinned at the same moment, not the same binlog point; Paimon-as-iceberg trails native Paimon by about a checkpoint).
- On the Iceberg tables, Trino took **0.81×** warm DuckDB's time (snapshot suite, geometric mean of 7 queries).
- On Paimon-as-iceberg, Trino took **1.34×** warm DuckDB's time (snapshot suite, geometric mean of 7 queries).
- On Trino, Paimon-as-iceberg answered in **1.08×** Iceberg's time (7 queries); its rows differed from Iceberg's on 5 of 7 queries (pinned at the same moment, not the same binlog point; Paimon-as-iceberg trails native Paimon by about a checkpoint).
- On DuckDB, Paimon-as-iceberg answered in **0.66×** Iceberg's time (7 queries); its rows differed from Iceberg's on 5 of 7 queries (pinned at the same moment, not the same binlog point; Paimon-as-iceberg trails native Paimon by about a checkpoint).
- Cold start (snapshot suite, first pass after the caches were emptied): on the Iceberg tables, Trino took **1.65×** DuckDB's time (7 queries, one sample each).
- Cold start (snapshot suite, first pass after the caches were emptied): on Paimon-as-iceberg, Trino took **1.71×** DuckDB's time (7 queries, one sample each).
- Cold start (snapshot suite): on Trino, Paimon-as-iceberg answered in **0.72×** Iceberg's time (7 queries, one sample each).
- Cold start (snapshot suite): on DuckDB, Paimon-as-iceberg answered in **0.70×** Iceberg's time (7 queries, one sample each).
- Ingest throughput (job monitor, median of 9 / 9 reports): Paimon **56,186 rows/s, 1.2 MB/s**; Iceberg **55,132 rows/s, 0.8 MB/s** (rows/s = the CDC sources' rate meter, change events with an UPDATE counted twice; bytes/s = committed).
- Delete mix: Iceberg wrote 33.5 M deletes for 32.6 M committed rows, 24% of them position deletes (DVs) and 25.5 M equality deletes; Paimon wrote no equality deletes.

</details>

## 2. Query latency

### Compaction before the snapshot test (run 20261110T163643Z)

`compaction/run.sh`, writers stopped: Paimon `sys.compact` (full); Iceberg data files with deletes rewritten with them applied and the delete files removed, then snapshots expired (12 h, keep 5). One row per table: its newest run.

Cost: the job's duration × its pods' requests (JobManager + TaskManagers): EC2 US$0.02662 per vCPU-hour and US$0.002975 per GiB-hour (a linear fit to on-demand m6g / r6g, us-west-2), plus EMR on EKS US$0.007084 per vCPU-hour and US$0.000777875 per GB-hour (us-west-2).

| format | table | duration (s) | TaskManagers | estimated cost (US$) | data files (files) | GiB | rows in files (rows) | delete files (files) | eq deletes (rows) | pos deletes (rows) |
|---|---|---:|---|---:|---|---|---|---|---:|---:|
| iceberg | customers | 245.2 | 16 × 8 vCPU / 32 GiB | 0.4359 | 528 -> 75 | 1.50 -> 1.44 | 43,719,772 -> 42,093,243 | 883 -> 0 | 30,884,864 | 472,912 |
| iceberg | order_items | 576.8 | 16 × 8 vCPU / 96 GiB | 1.6412 | 6,304 -> 160 | 6.26 -> 5.22 | 397,545,904 -> 349,092,095 | 6,228 -> 0 | 236,801,900 | 1,779,052 |
| iceberg | orders | 555.5 | 16 × 8 vCPU / 96 GiB | 1.5806 | 6,320 -> 160 | 5.82 -> 5.13 | 320,757,952 -> 291,184,285 | 5,816 -> 0 | 133,505,481 | 822,080 |
| iceberg | products | 293.5 | 16 × 8 vCPU / 32 GiB | 0.5218 | 4,464 -> 144 | 3.76 -> 3.40 | 145,523,741 -> 134,019,842 | 5,004 -> 0 | 133,078,043 | 713,343 |
| paimon | customers | 273.1 | 4 × 8 vCPU / 32 GiB | 0.1308 | 104 -> 56 | 1.66 -> 1.63 | 42,956,081 -> 42,093,243 | 0 -> 0 | 0 | 0 |
| paimon | order_items | 158.8 | 4 × 8 vCPU / 32 GiB | 0.0760 | 622 -> 160 | 6.23 -> 5.95 | 363,627,199 -> 349,091,971 | 0 -> 0 | 0 | 0 |
| paimon | orders | 161.4 | 4 × 8 vCPU / 32 GiB | 0.0773 | 634 -> 160 | 5.91 -> 5.75 | 298,877,281 -> 291,184,288 | 0 -> 0 | 0 | 0 |
| paimon | products | 162.0 | 4 × 8 vCPU / 32 GiB | 0.0776 | 585 -> 144 | 4.19 -> 4.06 | 139,696,437 -> 134,019,891 | 0 -> 0 | 0 | 0 |
| **iceberg total** | | **1671.0** | | **$4.1796** | | | | | | |
| **paimon total** | | **755.3** | | **$0.3616** | | | | | | |

### hot suite (run 20261110T160511Z)

Median of the timed passes, seconds (DuckDB: execute + fetch in the server; Trino: elapsed - queued). Ratio > 1 = Trino slower. Cost (US$) = seconds × the engine's EC2 on-demand price (DuckDB 1 × m5.24xlarge = US$4.608/h; Trino 9 × m5.4xlarge + 1 × m5.2xlarge (coordinator) = US$7.296/h); query time only, idle time between queries is not counted.

| query | DuckDB iceberg | estimated cost (US$) | Trino iceberg | estimated cost (US$) | DuckDB paimon-as-iceberg | estimated cost (US$) | Trino paimon-as-iceberg | estimated cost (US$) | Trino / DuckDB, iceberg | Trino / DuckDB, paimon |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| H1_freshness | 4.72 | 0.00604 | 1.64 | 0.00332 | 1.08 | 0.00138 | 0.70 | 0.00142 | **0.35** | **0.65** |
| H2_hot_count | 3.15 | 0.00403 | 0.52 | 0.00105 | 0.18 | 0.00023 | 0.30 | 0.00060 | **0.16** | **1.68** |
| H3_hot_needle | 3.07 | 0.00393 | 0.56 | 0.00113 | 0.33 | 0.00042 | 0.34 | 0.00069 | **0.18** | **1.04** |
| H4_hot_narrow_agg | 2.01 | 0.00258 | 0.50 | 0.00101 | 0.39 | 0.00050 | 0.42 | 0.00086 | **0.25** | **1.10** |
| H5_hot_wide_row | 2.01 | 0.00258 | 0.58 | 0.00117 | 0.41 | 0.00052 | 0.84 | 0.00171 | **0.29** | **2.08** |
| H6_hot_product_agg | 3.08 | 0.00394 | 0.66 | 0.00134 | 0.37 | 0.00047 | 0.66 | 0.00134 | **0.22** | **1.78** |
| H7_hot_status_category_rank | 10.63 | 0.0136 | 6.04 | 0.0122 | 1.23 | 0.00158 | 2.80 | 0.00567 | **0.57** | **2.27** |
| H8_hot_pk_lookup | 1.10 | 0.00141 | 0.68 | 0.00138 | 0.37 | 0.00047 | 0.59 | 0.00120 | **0.62** | **1.60** |
| H9_hot_join | 5.40 | 0.00691 | 1.23 | 0.00250 | 0.46 | 0.00058 | 0.76 | 0.00154 | **0.23** | **1.66** |
| **total, 9 queries** | **35.17** | **$0.0450** | **12.41** | **$0.0251** | **4.80** | **$0.00615** | **7.41** | **$0.0150** |  |  |

### snapshot suite (run 20261110T171110Z)

Median of the timed passes, seconds (DuckDB: execute + fetch in the server; Trino: elapsed - queued). Ratio > 1 = Trino slower. Cost (US$) = seconds × the engine's EC2 on-demand price (DuckDB 1 × m5.24xlarge = US$4.608/h; Trino 9 × m5.4xlarge + 1 × m5.2xlarge (coordinator) = US$7.296/h); query time only, idle time between queries is not counted.

| query | DuckDB iceberg | estimated cost (US$) | Trino iceberg | estimated cost (US$) | DuckDB paimon-as-iceberg | estimated cost (US$) | Trino paimon-as-iceberg | estimated cost (US$) | Trino / DuckDB, iceberg | Trino / DuckDB, paimon |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| S1_order_items_count | 0.37 | 0.00048 | 0.22 | 0.00044 | 0.19 | 0.00024 | 0.23 | 0.00046 | **0.58** | **1.18** |
| S2_items_narrow_agg | 0.41 | 0.00052 | 0.35 | 0.00071 | 0.21 | 0.00027 | 0.41 | 0.00084 | **0.86** | **1.93** |
| S3_orders_distinct_customers | 0.74 | 0.00095 | 0.56 | 0.00114 | 0.41 | 0.00052 | 0.76 | 0.00154 | **0.76** | **1.86** |
| S4_orders_wide_slice | 0.65 | 0.00084 | 0.51 | 0.00102 | 0.46 | 0.00059 | 0.55 | 0.00111 | **0.77** | **1.19** |
| S5_star_join_revenue | 4.77 | 0.00610 | 4.19 | 0.00849 | 4.32 | 0.00553 | 3.98 | 0.00806 | **0.88** | **0.92** |
| S6_topn_customers | 1.03 | 0.00132 | 0.55 | 0.00112 | 0.77 | 0.00099 | 0.55 | 0.00112 | **0.54** | **0.71** |
| S7_dedup_keys | 3.05 | 0.00391 | 5.26 | 0.0107 | 2.28 | 0.00291 | 5.36 | 0.0109 | **1.72** | **2.36** |
| **total, 7 queries** | **11.02** | **$0.0141** | **11.63** | **$0.0236** | **8.64** | **$0.0111** | **11.84** | **$0.0240** |  |  |

#### snapshot suite, cold start (first pass after both engines' caches were emptied)

One timed sample per query and arm (not a median), so noisier than the warm table above. Seconds; ratios > 1 = the first named slower. Cost (US$) = seconds × the engine's EC2 on-demand price (DuckDB 1 × m5.24xlarge = US$4.608/h; Trino 9 × m5.4xlarge + 1 × m5.2xlarge (coordinator) = US$7.296/h); query time only, idle time between queries is not counted.

| query | DuckDB iceberg | estimated cost (US$) | Trino iceberg | estimated cost (US$) | DuckDB paimon-as-iceberg | estimated cost (US$) | Trino paimon-as-iceberg | estimated cost (US$) | Trino / DuckDB, iceberg | Trino / DuckDB, paimon | Paimon / Iceberg, DuckDB | Paimon / Iceberg, Trino |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| S1_order_items_count | 1.26 | 0.00161 | 3.59 | 0.00728 | 0.56 | 0.00072 | 1.07 | 0.00218 | **2.86** | **1.92** | **0.44** | **0.30** |
| S2_items_narrow_agg | 0.64 | 0.00081 | 0.82 | 0.00166 | 0.38 | 0.00048 | 1.00 | 0.00202 | **1.29** | **2.66** | **0.59** | **1.22** |
| S3_orders_distinct_customers | 1.22 | 0.00156 | 1.98 | 0.00401 | 0.99 | 0.00126 | 1.81 | 0.00367 | **1.62** | **1.83** | **0.81** | **0.92** |
| S4_orders_wide_slice | 1.16 | 0.00148 | 1.55 | 0.00315 | 0.93 | 0.00119 | 1.99 | 0.00402 | **1.34** | **2.14** | **0.80** | **1.28** |
| S5_star_join_revenue | 6.93 | 0.00887 | 11.54 | 0.0234 | 6.39 | 0.00818 | 5.74 | 0.0116 | **1.67** | **0.90** | **0.92** | **0.50** |
| S6_topn_customers | 1.11 | 0.00142 | 1.13 | 0.00230 | 0.76 | 0.00097 | 0.79 | 0.00159 | **1.02** | **1.03** | **0.69** | **0.69** |
| S7_dedup_keys | 3.55 | 0.00454 | 8.77 | 0.0178 | 2.76 | 0.00353 | 6.22 | 0.0126 | **2.47** | **2.25** | **0.78** | **0.71** |
| **total, 7 queries** | **15.85** | **$0.0203** | **29.39** | **$0.0596** | **12.76** | **$0.0163** | **18.61** | **$0.0377** |  |  |  |  |

## 3. Ingest

Window 2026-11-10T16:07:48Z to 2026-11-10T16:24:01Z (972 s; hot: first pin commit - 300 s .. last pin commit), from each table's own commit history: Iceberg snapshot summaries in the current metadata.json, Paimon `snapshot-N` files (`querybench/ingest_stats.py`). Write commits only; compaction commits are counted apart.

**Throughput** (job-monitor reports overlapping the window: median; in brackets, total rows ingested for rows/s, max for MB/s)

| | Paimon | Iceberg |
|---|---:|---:|
| Rows/s (total rows) | **56,186** (47,603,680) | **55,132** (49,474,393) |
| Data size/s, MB/s | **1.2** (1.3) | **0.8** (0.9) |
| Checkpoint duration, median | 2,272 ms | 1,580 ms |
| Backpressure HIGH, max share of samples | 0% | 0% |

**Outcome:** both writers kept up: each read the same MySQL change stream at about 56,000 change events a second (an UPDATE counts as 2 events, so this is above the rows MySQL wrote), with no backlog. Checkpoints, the moment each writer makes its new data visible, took 2.3 s for Paimon and 1.6 s for Iceberg (median).

**From table metadata** (each table's commit history over the window)

| | Paimon | Iceberg |
|---|---:|---:|
| Rows/s (window average) | **26,265** | **33,582** |
| Data size/s (new files) | 1.0 MB/s | 0.8 MB/s |
| Rows committed | 25.5 M | 32.6 M |
| Write commits (median interval) | 68 (60 s) | 68 (60 s) |
| New data files | 5,168 | 5,168 |
| **New delete files** | 1,592 DV index files, by compaction | **10,305** (5,137 DVs + 5,168 equality-delete files) |
| **Equality deletes** | **0** | **25.5 M** |
| Position deletes | none written; compaction marks rows in DVs | 8.0 M (24% of all deletes, as DVs) |
| Compaction commits | 63 (inline) | 0 (compaction job) |

**What each format stored**

- **Rows saved:** Iceberg saved every version of every changed row, 32.6 M rows (about 33,582 a second). Paimon saved 25.5 M (78% of Iceberg's): when the same row changed more than once within a checkpoint (60 s), it kept only the latest version.
- **Old versions:** for each changed row Iceberg also wrote a delete marker for the version it replaces, 33.5 M markers in this window, and every query has to check them until a compaction removes them. Paimon wrote none: it merges old versions away while loading (63 small merges in the window).
- **Data size:** while loading, Paimon wrote about 23% more bytes than Iceberg (1.0 MB/s vs 0.8 MB/s of new files); after both were fully compacted, Paimon's tables were 15% bigger (17.4 vs 15.2 GiB) for the same rows. Each Paimon row also stores a copy of its key, a version number and a change type, which it needs to merge changes (`data-file.thin-mode`, off by default, would drop the key copy); its newest files are also lz4- rather than zstd-compressed (`file.compression.per.level = 0:lz4`).
- **Source database:** RDS CPU averaged 21.3% over the window (23.4% max), so MySQL was not the limit.

## 4. Table state

At the window end (the last commit at or before 2026-11-10T16:24:01Z). Both record counts are before deletes are applied: Iceberg `total-records`, Paimon `totalRecordCount` of the live files.

| table | Paimon records | Paimon files / DV-deleted rows | Iceberg records | Iceberg data / delete files | Iceberg eq / pos deletes | snapshots P / I |
|---|---:|---:|---:|---:|---:|---:|
| customers | 42.8 M | 105 / 734,176 | 43.5 M | 440 / 776 | 43.0 M / 496,122 | 104 / 63 |
| products | 138.3 M | 624 / 4.7 M | 143.3 M | 3,672 / 6,552 | 139.5 M / 3.8 M | 123 / 63 |
| orders | 295.9 M | 751 / 5.2 M | 313.8 M | 5,088 / 8,520 | 304.6 M / 10.2 M | 124 / 63 |
| order_items | 360.8 M | 692 / 12.4 M | 388.4 M | 5,072 / 8,656 | 372.0 M / 17.9 M | 123 / 63 |

Iceberg holds 1.1× Paimon's records: its superseded rows stay in the data files until a compaction rewrites them; Paimon's compaction drops them as it goes.

## Conclusions

**Bottom line**

- **Live, changing data (hot queries): Paimon is the better fit.** With DuckDB, Paimon was 7.1× faster than Iceberg; with Trino, Paimon was 41% faster than Iceberg. Paimon tidies up changed rows while it loads them; Iceberg leaves that work to every query until a separate clean-up (compaction) runs.
- **Settled data (after a full clean-up of both tables):** the gap mostly closes; with DuckDB, Paimon was 52% faster than Iceberg (first query after a restart: 42% faster); with Trino, Paimon and Iceberg were about the same speed (first query after a restart: 38% faster).
- **Choice of query engine:** Trino for Iceberg (3.5× faster than DuckDB on live data); DuckDB for Paimon (45% faster than Trino on live data).

**Details**

1. Engine (iceberg, hot): Trino / DuckDB = 0.29 over 9 queries.
2. Engine (paimon, hot): Trino / DuckDB = 1.45 over 9 queries.
3. Format (Trino, hot): Paimon-as-iceberg / Iceberg = 0.71 over 9 queries; rows differ on 7.
4. Format (DuckDB, hot): Paimon-as-iceberg / Iceberg = 0.14 over 9 queries; rows differ on 7.
5. Valid (hot): on each format, both engines returned identical rows for every query both ran.
6. Engine (iceberg, snapshot): Trino / DuckDB = 0.81 over 7 queries.
7. Engine (paimon, snapshot): Trino / DuckDB = 1.34 over 7 queries.
8. Format (Trino, snapshot): Paimon-as-iceberg / Iceberg = 1.08 over 7 queries; rows differ on 5.
9. Format (DuckDB, snapshot): Paimon-as-iceberg / Iceberg = 0.66 over 7 queries; rows differ on 5.
10. Cold start, engine (iceberg, snapshot): Trino / DuckDB = 1.65 over 7 queries.
11. Cold start, engine (paimon, snapshot): Trino / DuckDB = 1.71 over 7 queries.
12. Cold start, format (Trino, snapshot): Paimon / Iceberg = 0.72 over 7 queries.
13. Cold start, format (DuckDB, snapshot): Paimon / Iceberg = 0.70 over 7 queries.
14. Valid (snapshot): on each format, both engines returned identical rows for every query both ran.
15. Ingest (job monitor): Paimon 56,186 rows/s, 1.2 MB/s; Iceberg 55,132 rows/s, 0.8 MB/s.
16. Table metadata: Iceberg 33,582 rows/s, Paimon 26,265 rows/s committed; Iceberg added 25.5 M equality deletes and 8.0 M position deletes (DVs), Paimon none (it merges by key; compaction writes DVs).

### Open items

- Iceberg vs Paimon-as-iceberg rows differ on 7 hot queries: the two jobs are pinned at the same moment, not the same binlog point, and Paimon-as-iceberg trails native Paimon by about a checkpoint.
- Iceberg vs Paimon-as-iceberg rows differ on 5 snapshot queries: the two jobs are pinned at the same moment, not the same binlog point, and Paimon-as-iceberg trails native Paimon by about a checkpoint.

---

## Details

<details><summary>hot: Trino server-side breakdown, data scanned, result equality</summary>

**Trino server-side breakdown** (median over the timed passes)

| arm | query | elapsed s | queued s | execution s | CPU s | physical input GB |
|---|---|---|---|---|---|---|
| iceberg | H1_freshness | 1.64 | 0.00 | 1.64 | 22.70 | 0.20 |
| iceberg | H2_hot_count | 0.52 | 0.00 | 0.52 | 11.84 | 0.15 |
| iceberg | H3_hot_needle | 0.56 | 0.00 | 0.56 | 11.59 | 0.15 |
| iceberg | H4_hot_narrow_agg | 0.50 | 0.00 | 0.50 | 7.49 | 0.05 |
| iceberg | H5_hot_wide_row | 0.58 | 0.00 | 0.58 | 7.80 | 0.05 |
| iceberg | H6_hot_product_agg | 0.66 | 0.00 | 0.66 | 14.74 | 0.15 |
| iceberg | H7_hot_status_category_rank | 6.04 | 0.00 | 6.04 | 198.60 | 1.08 |
| iceberg | H8_hot_pk_lookup | 0.68 | 0.00 | 0.68 | 2.86 | 0.03 |
| iceberg | H9_hot_join | 1.23 | 0.00 | 1.23 | 17.63 | 0.19 |
| paimon | H1_freshness | 0.70 | 0.00 | 0.70 | 2.71 | 0.06 |
| paimon | H2_hot_count | 0.30 | 0.00 | 0.30 | 0.64 | 0.03 |
| paimon | H3_hot_needle | 0.34 | 0.00 | 0.34 | 0.92 | 0.08 |
| paimon | H4_hot_narrow_agg | 0.42 | 0.00 | 0.42 | 1.21 | 0.06 |
| paimon | H5_hot_wide_row | 0.84 | 0.00 | 0.84 | 2.40 | 0.15 |
| paimon | H6_hot_product_agg | 0.66 | 0.00 | 0.66 | 2.50 | 0.16 |
| paimon | H7_hot_status_category_rank | 2.80 | 0.00 | 2.80 | 44.11 | 0.98 |
| paimon | H8_hot_pk_lookup | 0.59 | 0.00 | 0.59 | 0.34 | 0.02 |
| paimon | H9_hot_join | 0.76 | 0.00 | 0.76 | 1.61 | 0.14 |

**Data scanned, GB** (DuckDB: HTTPFS bytes in an untimed pass with its cache bypassed; Trino: physical input of the last timed pass, cache hits included)

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg |
|---|---|---|---|---|
| H1_freshness | 0.15 | 0.24 | 0.04 | 0.06 |
| H2_hot_count | 0.09 | 0.19 | 0.02 | 0.02 |
| H3_hot_needle | 0.13 | 0.19 | 0.08 | 0.08 |
| H4_hot_narrow_agg | 0.07 | 0.05 | 0.06 | 0.08 |
| H5_hot_wide_row | 0.10 | 0.05 | 0.21 | 0.21 |
| H6_hot_product_agg | 0.20 | 0.19 | 0.15 | 0.17 |
| H7_hot_status_category_rank | 1.31 | 1.14 | 0.23 | 1.01 |
| H8_hot_pk_lookup | 0.02 | 0.03 | 0.13 | 0.00 |
| H9_hot_join | 0.23 | 0.23 | 0.13 | 0.15 |

**Same rows across arms?** (every row of the last timed pass, compared as text). Letters name distinct result sets per query; equal letters = identical rows. An arm that did not run OK shows its status and is not compared.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | rows |
|---|---|---|---|---|---|
| H1_freshness | A | A | B | B | 1 |
| H2_hot_count | A | A | B | B | 1 |
| H3_hot_needle | A | A | A | A | 0 |
| H4_hot_narrow_agg | A | A | B | B | 5 |
| H5_hot_wide_row | A | A | B | B | 1 |
| H6_hot_product_agg | A | A | B | B | 20 |
| H7_hot_status_category_rank | A | A | B | B | 15 |
| H8_hot_pk_lookup | A | A | A | A | 1 |
| H9_hot_join | A | A | B | B | 5 |

Validity: on each format, every query both engines ran OK returned the same rows.

Iceberg vs Paimon rows differ on: H1_freshness (DuckDB), H1_freshness (Trino), H2_hot_count (DuckDB), H2_hot_count (Trino), H4_hot_narrow_agg (DuckDB), H4_hot_narrow_agg (Trino), H5_hot_wide_row (DuckDB), H5_hot_wide_row (Trino), H6_hot_product_agg (DuckDB), H6_hot_product_agg (Trino), H7_hot_status_category_rank (DuckDB), H7_hot_status_category_rank (Trino), H9_hot_join (DuckDB), H9_hot_join (Trino). A pipeline gap, not a failure: the two CDC jobs are pinned at the same moment, not at the same binlog point, and Paimon-as-iceberg (its v3 Iceberg metadata: files above level 0 plus deletion vectors) lags native Paimon by about one checkpoint.

First row per arm:

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg |
|---|---|---|---|---|
| H1_freshness | `96863\|174925\|96863\|2026-11-10 16:06:59\|2026-11-10 16:06:59` | `96863\|174925\|96863\|2026-11-10 16:06:59\|2026-11-10 16:06:59` | `96983\|179014\|96983\|2026-11-10 16:06:59\|2026-11-10 16:06:59` | `96983\|179014\|96983\|2026-11-10 16:06:59\|2026-11-10 16:06:59` |
| H2_hot_count | `96863` | `96863` | `96983` | `96983` |
| H3_hot_needle | `-` | `-` | `-` | `-` |
| H4_hot_narrow_agg | `DELIVERED\|35200\|88503920.51` | `DELIVERED\|35200\|88503920.51` | `DELIVERED\|36040\|90581389.94` | `DELIVERED\|36040\|90581389.94` |
| H5_hot_wide_row | `174925\|7\|379070130966176\|177508646308749\|438518106.68\|2026-1` | `174925\|7\|379070130966176\|177508646308749\|438518106.68\|2026-1` | `179014\|7\|387713701734413\|181646832663259\|448848153.81\|2026-1` | `179014\|7\|387713701734413\|181646832663259\|448848153.81\|2026-1` |
| H6_hot_product_agg | `824887873\|14\|89\|39637.94\|14` | `824887873\|14\|89\|39637.94\|14` | `824887873\|14\|89\|39637.94\|14` | `824887873\|14\|89\|39637.94\|14` |
| H7_hot_status_category_rank | `CANCELLED\|1\|Sports\|7\|7\|4\|38\|18139.03\|88189.19\|0` | `CANCELLED\|1\|Sports\|7\|7\|4\|38\|18139.03\|88189.19\|0` | `CANCELLED\|1\|Sports\|7\|7\|4\|38\|18139.03\|88189.19\|0` | `CANCELLED\|1\|Sports\|7\|7\|4\|38\|18139.03\|88189.19\|0` |
| H8_hot_pk_lookup | `1295614288\|SHIPPED\|2504.6\|APPLE_PAY\|2026-11-10 16:06:59` | `1295614288\|SHIPPED\|2504.6\|APPLE_PAY\|2026-11-10 16:06:59` | `1295614288\|SHIPPED\|2504.6\|APPLE_PAY\|2026-11-10 16:06:59` | `1295614288\|SHIPPED\|2504.6\|APPLE_PAY\|2026-11-10 16:06:59` |
| H9_hot_join | `CANCELLED\|36\|13\|189\|88189.19` | `CANCELLED\|36\|13\|189\|88189.19` | `CANCELLED\|36\|13\|189\|88189.19` | `CANCELLED\|36\|13\|189\|88189.19` |

</details>

<details><summary>hot: pinned tables, data and delete files (tables-20261110T160511Z.tsv)</summary>

Per format and table, its pins in this suite (hot: one per pass, ranges over them; snapshot: one). `paimon` = the Paimon-as-iceberg metadata.

| format | table | pins | snapshot id (last) | records (rows) | data files | delete files | equality deletes (rows) | position deletes (rows, incl. DVs) | GB |
|---|---|---|---|---|---|---|---|---|---|
| iceberg | customers | 4 | 6296598436421063064 | 43360608..43541467 | 352..440 | 600..776 | 42910490..43045345 | 450118..496122 | 1.75 |
| iceberg | products | 4 | 2193838078834690459 | 140973837..143303708 | 2880..3672 | 4968..6552 | 137771413..139542254 | 3202424..3761454 | 4.46 |
| iceberg | orders | 4 | 7741659502259137536 | 306198048..313841063 | 3856..5088 | 6078..8520 | 298219568..304617761 | 8735147..10201976 | 6.78 |
| iceberg | order_items | 4 | 5777480947708571155 | 377591607..388391801 | 3840..5072 | 6192..8656 | 363886102..372001147 | 14923633..17931131 | 7.35 |
| paimon | customers | 4 | 82 | 0..4391 | 0..1 | 77..83 | 0 | 664884..734176 | 0.00 |
| paimon | products | 4 | 98 | 5036602..5289362 | 0 | 320..422 | 0 | 3980851..4688391 | 0.00 |
| paimon | orders | 4 | 99 | 941335..3899956 | 0 | 313..379 | 0 | 3301622..5245383 | 0.00 |
| paimon | order_items | 4 | 98 | 7545647..10896415 | 0 | 281..346 | 0 | 9188781..12379238 | 0.00 |

Every pin (tables TSV):

| pin_tag | format | table | snapshot_id | commit_ms | total-records | total-data-files | total-delete-files | total-equality-deletes | total-position-deletes | total-files-size | note |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20261110T160511Z-p0 | iceberg | customers | 8984117918450422233 | 1794327169024 | 43360608 | 352 | 600 | 42910490 | 450118 | 1739878405 | format-version=3 operation=overwrite spec=bucket[8](customer_id) |
| 20261110T160511Z-p0 | paimon | customers | 63 | 1794327175241 | 0 | 0 | 83 | 0 | 664884 | 0 | format-version=3 operation=append spec=none |
| 20261110T160511Z-p0 | iceberg | products | 3694213077945967617 | 1794327168950 | 140973837 | 2880 | 4968 | 137771413 | 3202424 | 4390821396 | format-version=3 operation=overwrite spec=category,bucket[8](product_id) |
| 20261110T160511Z-p0 | paimon | products | 75 | 1794327175216 | 5289362 | 0 | 320 | 0 | 3980851 | 0 | format-version=3 operation=append spec=category |
| 20261110T160511Z-p0 | iceberg | orders | 2374445214854708455 | 1794327168941 | 306198048 | 3856 | 6078 | 298219568 | 8735147 | 6595341375 | format-version=3 operation=overwrite spec=order_dt,bucket[16](order_id) |
| 20261110T160511Z-p0 | paimon | orders | 77 | 1794327179556 | 3899956 | 0 | 313 | 0 | 3301622 | 0 | format-version=3 operation=replace spec=order_date_str |
| 20261110T160511Z-p0 | iceberg | order_items | 6359733766002798612 | 1794327168907 | 377591607 | 3840 | 6192 | 363886102 | 14923633 | 7091593629 | format-version=3 operation=overwrite spec=created_dt,bucket[16](order_item_id) |
| 20261110T160511Z-p0 | paimon | order_items | 76 | 1794327180473 | 10896415 | 0 | 281 | 0 | 9188781 | 0 | format-version=3 operation=replace spec=created_date_str |
| 20261110T160511Z-p1 | iceberg | customers | 4227884943004609982 | 1794327409133 | 43433359 | 384 | 664 | 42964671 | 468688 | 1742885123 | format-version=3 operation=overwrite spec=bucket[8](customer_id) |
| 20261110T160511Z-p1 | paimon | customers | 67 | 1794327356254 | 0 | 0 | 78 | 0 | 673441 | 0 | format-version=3 operation=append spec=none |
| 20261110T160511Z-p1 | iceberg | products | 5832122792087226464 | 1794327409102 | 141819127 | 3168 | 5544 | 138424654 | 3394473 | 4416904321 | format-version=3 operation=overwrite spec=category,bucket[8](product_id) |
| 20261110T160511Z-p1 | paimon | products | 83 | 1794327415387 | 5235567 | 0 | 380 | 0 | 4385625 | 0 | format-version=3 operation=append spec=category |
| 20261110T160511Z-p1 | iceberg | orders | 6788351151890891877 | 1794327409181 | 308921108 | 4304 | 6968 | 300457259 | 9298725 | 6661116226 | format-version=3 operation=overwrite spec=order_dt,bucket[16](order_id) |
| 20261110T160511Z-p1 | paimon | orders | 84 | 1794327415470 | 3015850 | 0 | 314 | 0 | 4366047 | 0 | format-version=3 operation=append spec=order_date_str |
| 20261110T160511Z-p1 | iceberg | order_items | 2050079631225767195 | 1794327409080 | 381542581 | 4288 | 7088 | 366876469 | 16000048 | 7184529950 | format-version=3 operation=overwrite spec=created_dt,bucket[16](order_item_id) |
| 20261110T160511Z-p1 | paimon | order_items | 84 | 1794327420945 | 9768573 | 0 | 339 | 0 | 11162437 | 0 | format-version=3 operation=replace spec=created_date_str |
| 20261110T160511Z-p2 | iceberg | customers | 315399061221772518 | 1794327589342 | 43480678 | 408 | 712 | 42999988 | 480690 | 1744860166 | format-version=3 operation=overwrite spec=bucket[8](customer_id) |
| 20261110T160511Z-p2 | paimon | customers | 75 | 1794327600021 | 4391 | 1 | 78 | 0 | 697551 | 124213 | format-version=3 operation=append spec=none |
| 20261110T160511Z-p2 | iceberg | products | 8178528866769744224 | 1794327589482 | 142528441 | 3384 | 5976 | 138960445 | 3567996 | 4438652128 | format-version=3 operation=overwrite spec=category,bucket[8](product_id) |
| 20261110T160511Z-p2 | paimon | products | 90 | 1794327599134 | 5189243 | 0 | 422 | 0 | 4619514 | 0 | format-version=3 operation=replace spec=category |
| 20261110T160511Z-p2 | iceberg | orders | 1773531368111930424 | 1794327589306 | 310839469 | 4640 | 7632 | 302087058 | 9643263 | 6707940425 | format-version=3 operation=overwrite spec=order_dt,bucket[16](order_id) |
| 20261110T160511Z-p2 | paimon | orders | 91 | 1794327601265 | 2669218 | 0 | 379 | 0 | 5063325 | 0 | format-version=3 operation=replace spec=order_date_str |
| 20261110T160511Z-p2 | iceberg | order_items | 5004511789828803321 | 1794327589530 | 385013122 | 4624 | 7760 | 369470660 | 16981235 | 7265242895 | format-version=3 operation=overwrite spec=created_dt,bucket[16](order_item_id) |
| 20261110T160511Z-p2 | paimon | order_items | 90 | 1794327598210 | 9288642 | 0 | 346 | 0 | 11626072 | 0 | format-version=3 operation=replace spec=created_date_str |
| 20261110T160511Z-p3 | iceberg | customers | 6296598436421063064 | 1794327829462 | 43541467 | 440 | 776 | 43045345 | 496122 | 1747400698 | format-version=3 operation=overwrite spec=bucket[8](customer_id) |
| 20261110T160511Z-p3 | paimon | customers | 82 | 1794327840506 | 0 | 0 | 77 | 0 | 734176 | 0 | format-version=3 operation=replace spec=none |
| 20261110T160511Z-p3 | iceberg | products | 2193838078834690459 | 1794327829434 | 143303708 | 3672 | 6552 | 139542254 | 3761454 | 4462600599 | format-version=3 operation=overwrite spec=category,bucket[8](product_id) |
| 20261110T160511Z-p3 | paimon | products | 98 | 1794327839469 | 5036602 | 0 | 339 | 0 | 4688391 | 0 | format-version=3 operation=replace spec=category |
| 20261110T160511Z-p3 | iceberg | orders | 7741659502259137536 | 1794327829485 | 313841063 | 5088 | 8520 | 304617761 | 10201976 | 6780254312 | format-version=3 operation=overwrite spec=order_dt,bucket[16](order_id) |
| 20261110T160511Z-p3 | paimon | orders | 99 | 1794327841128 | 941335 | 0 | 344 | 0 | 5245383 | 0 | format-version=3 operation=replace spec=order_date_str |
| 20261110T160511Z-p3 | iceberg | order_items | 5777480947708571155 | 1794327829465 | 388391801 | 5072 | 8656 | 372001147 | 17931131 | 7346533166 | format-version=3 operation=overwrite spec=created_dt,bucket[16](order_item_id) |
| 20261110T160511Z-p3 | paimon | order_items | 98 | 1794327838501 | 7545647 | 0 | 308 | 0 | 12379238 | 0 | format-version=3 operation=replace spec=created_date_str |

</details>

<details><summary>hot: pins, gates and windows (hot-interleave-20261110T160511Z.pins)</summary>

```
stamp=20261110T160511Z window=[2026-11-10 16:06:30, 2026-11-10 16:07:00) dt=2026-11-10 repin=pass engines=duckdb,trino formats=iceberg,paimon
iceberg_format_version=3
cdc_db_suffix=_v3 iceberg_db=flink_iceberg_db_v3 paimon_db=flink_paimon_db_v3 engines=duckdb,trino formats=iceberg,paimon
duckdb_image=<id>.dkr.ecr.us-west-2.amazonaws.com/duckdb-cdc-server:2.0.0 duckdb_cpu=81 duckdb_pod_memory=342Gi duckdb_skip_puffin_verification_arms=paimon
trino_version=484 trino_catalog=iceberg trino_workers=9 paimon_reader=iceberg
duckdb_cpu=81 duckdb_memory=342Gi duckdb_ec2=m5.24xlarge trino_worker_cpu=9 trino_worker_memory=38Gi trino_worker_ec2=m5.4xlarge trino_coord_cpu=4 trino_coord_memory=16Gi trino_coord_ec2=m5.2xlarge
generator_before=mysql-upsert-loadgen=24/24
data_wait_s=40
## pins 20261110T160511Z-p0
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794327169024
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794327168941
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794327168907
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794327168950
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794327175241
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794327179556
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794327180473
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794327175216
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p0/iceberg/customers/00041-3f19f2a2-86b7-49a8-a79f-4ba14efa143a.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p0/iceberg/orders/00041-d1a7f811-bc61-4259-8b8b-b3ea6d92da1a.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p0/iceberg/order_items/00041-0890ec3d-c967-42ff-aba2-34801714a4d5.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p0/iceberg/products/00041-c181f3af-b509-4fce-ac0f-0ba796d2425a.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p0/paimon/customers/v63.metadata.json
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p0/paimon/orders/v77.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p0/paimon/order_items/v76.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p0/paimon/products/v75.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3
QB_PIN_SNAP_ICEBERG_CUSTOMERS=8984117918450422233
QB_PIN_SNAP_ICEBERG_ORDERS=2374445214854708455
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=6359733766002798612
QB_PIN_SNAP_ICEBERG_PRODUCTS=3694213077945967617
QB_PIN_SNAP_PAIMON_CUSTOMERS=63
QB_PIN_SNAP_PAIMON_ORDERS=77
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=76
QB_PIN_SNAP_PAIMON_PRODUCTS=75
h8_key order_id=1295614288 dt=2026-11-10 (picked on trino:iceberg (pinned orders, order_id % 16 = 0: never deleted by the generator), not confirmed on the other arms (the summary compares their rows); partition = HOT_DT)
queries_started_at=2026-11-10 16:13:41Z
## pins 20261110T160511Z-p1
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794327409133
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794327409181
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794327409080
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794327409102
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794327356254
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794327415470
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794327420945
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794327415387
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p1/iceberg/customers/00045-7062f522-f6ba-4d8b-88ae-536ff44dfa10.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p1/iceberg/orders/00045-8a90bde6-f482-4c48-a8bd-0f2587900cc9.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p1/iceberg/order_items/00045-4ed3a150-0244-473a-a8bc-5d6b00dc0f35.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p1/iceberg/products/00045-259d2e47-2da3-4a93-a992-be3116fce30c.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p1/paimon/customers/v67.metadata.json
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p1/paimon/orders/v84.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p1/paimon/order_items/v84.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p1/paimon/products/v83.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3
QB_PIN_SNAP_ICEBERG_CUSTOMERS=4227884943004609982
QB_PIN_SNAP_ICEBERG_ORDERS=6788351151890891877
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=2050079631225767195
QB_PIN_SNAP_ICEBERG_PRODUCTS=5832122792087226464
QB_PIN_SNAP_PAIMON_CUSTOMERS=67
QB_PIN_SNAP_PAIMON_ORDERS=84
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=84
QB_PIN_SNAP_PAIMON_PRODUCTS=83
## pins 20261110T160511Z-p2
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794327589342
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794327589306
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794327589530
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794327589482
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794327600021
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794327601265
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794327598210
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794327599134
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p2/iceberg/customers/00048-bb4418e7-0963-46b0-bff0-0b390dca611c.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p2/iceberg/orders/00048-11a8e0a5-1fbe-439f-ae7b-bd9d73b582ce.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p2/iceberg/order_items/00048-48a24085-213b-499a-be3f-e045e882c2ae.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p2/iceberg/products/00048-42a1c9df-05b8-43ec-8e7f-2be15ee68c61.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p2/paimon/customers/v75.metadata.json
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p2/paimon/orders/v91.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p2/paimon/order_items/v90.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p2/paimon/products/v90.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3
QB_PIN_SNAP_ICEBERG_CUSTOMERS=315399061221772518
QB_PIN_SNAP_ICEBERG_ORDERS=1773531368111930424
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=5004511789828803321
QB_PIN_SNAP_ICEBERG_PRODUCTS=8178528866769744224
QB_PIN_SNAP_PAIMON_CUSTOMERS=75
QB_PIN_SNAP_PAIMON_ORDERS=91
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=90
QB_PIN_SNAP_PAIMON_PRODUCTS=90
## pins 20261110T160511Z-p3
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794327829462
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794327829485
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794327829465
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794327829434
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794327840506
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794327841128
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794327838501
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794327839469
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p3/iceberg/customers/00052-56f1f139-ef66-4dc8-9a4a-098cdc7cbda1.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p3/iceberg/orders/00052-a46e74eb-16e2-475e-b17e-ba2f0d54316c.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p3/iceberg/order_items/00052-b0ba1096-6fbc-40eb-9a64-ab3a3ea05bcd.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p3/iceberg/products/00052-df78f327-783a-4395-a383-26815e0dda52.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p3/paimon/customers/v82.metadata.json
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p3/paimon/orders/v99.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p3/paimon/order_items/v98.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T160511Z-p3/paimon/products/v98.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3
QB_PIN_SNAP_ICEBERG_CUSTOMERS=6296598436421063064
QB_PIN_SNAP_ICEBERG_ORDERS=7741659502259137536
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=5777480947708571155
QB_PIN_SNAP_ICEBERG_PRODUCTS=2193838078834690459
QB_PIN_SNAP_PAIMON_CUSTOMERS=82
QB_PIN_SNAP_PAIMON_ORDERS=99
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=98
QB_PIN_SNAP_PAIMON_PRODUCTS=98
finished_at=2026-11-10 16:29:47Z
generator_after=mysql-upsert-loadgen=24/24
```
</details>

<details><summary>snapshot: Trino server-side breakdown, data scanned, result equality</summary>

**Trino server-side breakdown** (median over the timed passes)

| arm | query | elapsed s | queued s | execution s | CPU s | physical input GB |
|---|---|---|---|---|---|---|
| iceberg | S1_order_items_count | 0.22 | 0.00 | 0.22 | 1.03 | 0.02 |
| iceberg | S2_items_narrow_agg | 0.35 | 0.00 | 0.35 | 3.92 | 0.17 |
| iceberg | S3_orders_distinct_customers | 0.56 | 0.00 | 0.56 | 14.25 | 0.14 |
| iceberg | S4_orders_wide_slice | 0.51 | 0.00 | 0.51 | 7.76 | 0.68 |
| iceberg | S5_star_join_revenue | 4.19 | 0.00 | 4.19 | 153.90 | 2.00 |
| iceberg | S6_topn_customers | 0.55 | 0.00 | 0.55 | 5.67 | 0.27 |
| iceberg | S7_dedup_keys | 5.26 | 0.00 | 5.26 | 266.40 | 0.83 |
| paimon | S1_order_items_count | 0.23 | 0.00 | 0.23 | 0.53 | 0.02 |
| paimon | S2_items_narrow_agg | 0.41 | 0.00 | 0.41 | 2.98 | 0.17 |
| paimon | S3_orders_distinct_customers | 0.76 | 0.00 | 0.76 | 8.44 | 0.13 |
| paimon | S4_orders_wide_slice | 0.55 | 0.00 | 0.55 | 5.06 | 0.63 |
| paimon | S5_star_join_revenue | 3.98 | 0.00 | 3.98 | 159.90 | 2.10 |
| paimon | S6_topn_customers | 0.55 | 0.00 | 0.55 | 3.46 | 0.26 |
| paimon | S7_dedup_keys | 5.36 | 0.00 | 5.36 | 288.00 | 0.71 |

**Data scanned, GB** (DuckDB: HTTPFS bytes in an untimed pass with its cache bypassed; Trino: physical input of the last timed pass, cache hits included)

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg |
|---|---|---|---|---|
| S1_order_items_count | 0.02 | 0.02 | 0.02 | 0.02 |
| S2_items_narrow_agg | 0.17 | 0.17 | 0.17 | 0.17 |
| S3_orders_distinct_customers | 0.13 | 0.14 | 0.13 | 0.13 |
| S4_orders_wide_slice | 0.69 | 0.68 | 0.65 | 0.63 |
| S5_star_join_revenue | 2.33 | 1.98 | 2.08 | 1.77 |
| S6_topn_customers | 0.26 | 0.27 | 0.25 | 0.26 |
| S7_dedup_keys | 0.78 | 0.83 | 0.71 | 0.71 |

**Same rows across arms?** (every row of the last timed pass, compared as text). Letters name distinct result sets per query; equal letters = identical rows. An arm that did not run OK shows its status and is not compared.

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg | rows |
|---|---|---|---|---|---|
| S1_order_items_count | A | A | B | B | 1 |
| S2_items_narrow_agg | A | A | B | B | 10 |
| S3_orders_distinct_customers | A | A | B | B | 5 |
| S4_orders_wide_slice | A | A | A | A | 1 |
| S5_star_join_revenue | A | A | B | B | 180 |
| S6_topn_customers | A | A | A | A | 20 |
| S7_dedup_keys | A | A | B | B | 4 |

Validity: on each format, every query both engines ran OK returned the same rows.

Iceberg vs Paimon rows differ on: S1_order_items_count (DuckDB), S1_order_items_count (Trino), S2_items_narrow_agg (DuckDB), S2_items_narrow_agg (Trino), S3_orders_distinct_customers (DuckDB), S3_orders_distinct_customers (Trino), S5_star_join_revenue (DuckDB), S5_star_join_revenue (Trino), S7_dedup_keys (DuckDB), S7_dedup_keys (Trino). A pipeline gap, not a failure: the two CDC jobs are pinned at the same moment, not at the same binlog point, and Paimon-as-iceberg (its v3 Iceberg metadata: files above level 0 plus deletion vectors) lags native Paimon by about one checkpoint.

First row per arm:

| query | DuckDB iceberg | Trino iceberg | DuckDB paimon-as-iceberg | Trino paimon-as-iceberg |
|---|---|---|---|---|
| S1_order_items_count | `43859740\|241265529` | `43859740\|241265529` | `43859744\|241265488` | `43859744\|241265488` |
| S2_items_narrow_agg | `1\|4384323\|1983557005.97` | `1\|4384323\|1983557005.97` | `1\|4384326\|1983558167.8` | `1\|4384326\|1983558167.8` |
| S3_orders_distinct_customers | `APPLE_PAY\|7365154\|20024` | `APPLE_PAY\|7365154\|20024` | `APPLE_PAY\|7365153\|20024` | `APPLE_PAY\|7365153\|20024` |
| S4_orders_wide_slice | `3836248\|210581382\|9607447794.29\|3966892382915537\|85513204049` | `3836248\|210581382\|9607447794.29\|3966892382915537\|85513204049` | `3836248\|210581382\|9607447794.29\|3966892382915537\|85513204049` | `3836248\|210581382\|9607447794.29\|3966892382915537\|85513204049` |
| S5_star_join_revenue | `2026-11-08\|Automotive\|AZ\|133267\|733553\|331737569.64` | `2026-11-08\|Automotive\|AZ\|133267\|733553\|331737569.64` | `2026-11-08\|Automotive\|AZ\|133267\|733553\|331737569.64` | `2026-11-08\|Automotive\|AZ\|133267\|733553\|331737569.64` |
| S6_topn_customers | `1031089689\|Michael Brown\|CA\|2047\|5321602.22` | `1031089689\|Michael Brown\|CA\|2047\|5321602.22` | `1031089689\|Michael Brown\|CA\|2047\|5321602.22` | `1031089689\|Michael Brown\|CA\|2047\|5321602.22` |
| S7_dedup_keys | `customers\|42093243\|42093243\|0` | `customers\|42093243\|42093243\|0` | `customers\|42093243\|42093243\|0` | `customers\|42093243\|42093243\|0` |

</details>

<details><summary>snapshot: pinned tables, data and delete files (tables-20261110T171110Z.tsv)</summary>

Per format and table, its pins in this suite (hot: one per pass, ranges over them; snapshot: one). `paimon` = the Paimon-as-iceberg metadata.

| format | table | pins | snapshot id (last) | records (rows) | data files | delete files | equality deletes (rows) | position deletes (rows, incl. DVs) | GB |
|---|---|---|---|---|---|---|---|---|---|
| iceberg | customers | 1 | 4710971306199873401 | 42093243 | 75 | 0 | 0 | 0 | 1.55 |
| iceberg | products | 1 | 7818021361323693901 | 134019842 | 144 | 0 | 0 | 0 | 3.65 |
| iceberg | orders | 1 | 2693351644727175885 | 291184285 | 160 | 0 | 0 | 0 | 5.51 |
| iceberg | order_items | 1 | 5804433427551117286 | 349092095 | 160 | 0 | 0 | 0 | 5.60 |
| paimon | customers | 1 | 104 | 0 | 0 | 0 | 0 | 0 | 0.00 |
| paimon | products | 1 | 123 | 0 | 0 | 0 | 0 | 0 | 0.00 |
| paimon | orders | 1 | 124 | 0 | 0 | 0 | 0 | 0 | 0.00 |
| paimon | order_items | 1 | 123 | 0 | 0 | 0 | 0 | 0 | 0.00 |

Every pin (tables TSV):

| pin_tag | format | table | snapshot_id | commit_ms | total-records | total-data-files | total-delete-files | total-equality-deletes | total-position-deletes | total-files-size | note |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20261110T171110Z | iceberg | customers | 4710971306199873401 | 1794328999715 | 42093243 | 75 | 0 | 0 | 0 | 1549456838 | format-version=3 operation=replace spec=bucket[8](customer_id) |
| 20261110T171110Z | paimon | customers | 104 | 1794329021261 | 0 | 0 | 0 | 0 | 0 | 0 | format-version=3 operation=replace spec=none |
| 20261110T171110Z | iceberg | products | 7818021361323693901 | 1794329314594 | 134019842 | 144 | 0 | 0 | 0 | 3649013072 | format-version=3 operation=replace spec=category,bucket[8](product_id) |
| 20261110T171110Z | paimon | products | 123 | 1794329209220 | 0 | 0 | 0 | 0 | 0 | 0 | format-version=3 operation=replace spec=category |
| 20261110T171110Z | iceberg | orders | 2693351644727175885 | 1794329888443 | 291184285 | 160 | 0 | 0 | 0 | 5507659520 | format-version=3 operation=replace spec=order_dt,bucket[16](order_id) |
| 20261110T171110Z | paimon | orders | 124 | 1794329397224 | 0 | 0 | 0 | 0 | 0 | 0 | format-version=3 operation=replace spec=order_date_str |
| 20261110T171110Z | iceberg | order_items | 5804433427551117286 | 1794330495063 | 349092095 | 160 | 0 | 0 | 0 | 5603498267 | format-version=3 operation=replace spec=created_dt,bucket[16](order_item_id) |
| 20261110T171110Z | paimon | order_items | 123 | 1794329582461 | 0 | 0 | 0 | 0 | 0 | 0 | format-version=3 operation=replace spec=created_date_str |

</details>

<details><summary>snapshot: pins, gates and windows (snapshot-20261110T171110Z.pins)</summary>

```
stamp=20261110T171110Z suite=snapshot dt=2026-11-09 engines=duckdb,trino formats=iceberg,paimon
iceberg_format_version=3
cdc_db_suffix=_v3 iceberg_db=flink_iceberg_db_v3 paimon_db=flink_paimon_db_v3 engines=duckdb,trino formats=iceberg,paimon
duckdb_image=<id>.dkr.ecr.us-west-2.amazonaws.com/duckdb-cdc-server:2.0.0 duckdb_cpu=81 duckdb_pod_memory=342Gi duckdb_skip_puffin_verification_arms=paimon
trino_version=484 trino_catalog=iceberg trino_workers=9 paimon_reader=iceberg
duckdb_cpu=81 duckdb_memory=342Gi duckdb_ec2=m5.24xlarge trino_worker_cpu=9 trino_worker_memory=38Gi trino_worker_ec2=m5.4xlarge trino_coord_cpu=4 trino_coord_memory=16Gi trino_coord_ec2=m5.2xlarge
started_at=2026-11-10 17:21:17Z
gate_writers_stopped_at=2026-11-10 17:21:18Z writers=flink-cdc-paimon=suspended flink-cdc-iceberg=suspended generator=mysql-upsert-loadgen=/0
gate_engines_ready_at=2026-11-10 17:21:20Z
QB_PIN_COMMIT_MS_ICEBERG_CUSTOMERS=1794328999715
QB_PIN_COMMIT_MS_ICEBERG_ORDERS=1794329888443
QB_PIN_COMMIT_MS_ICEBERG_ORDER_ITEMS=1794330495063
QB_PIN_COMMIT_MS_ICEBERG_PRODUCTS=1794329314594
QB_PIN_COMMIT_MS_PAIMON_CUSTOMERS=1794329021261
QB_PIN_COMMIT_MS_PAIMON_ORDERS=1794329397224
QB_PIN_COMMIT_MS_PAIMON_ORDER_ITEMS=1794329582461
QB_PIN_COMMIT_MS_PAIMON_PRODUCTS=1794329209220
QB_PIN_FORMAT_VERSION_ICEBERG_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDERS=3
QB_PIN_FORMAT_VERSION_ICEBERG_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_ICEBERG_PRODUCTS=3
QB_PIN_FORMAT_VERSION_PAIMON_CUSTOMERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDERS=3
QB_PIN_FORMAT_VERSION_PAIMON_ORDER_ITEMS=3
QB_PIN_FORMAT_VERSION_PAIMON_PRODUCTS=3
QB_PIN_META_ICEBERG_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T171110Z/iceberg/customers/00064-1edb6294-1da7-4e41-b591-97f1909caacf.metadata.json
QB_PIN_META_ICEBERG_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T171110Z/iceberg/orders/00064-8c838824-36cd-420e-bad6-3ff8622a4be0.metadata.json
QB_PIN_META_ICEBERG_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T171110Z/iceberg/order_items/00064-31900259-0a09-49e7-a718-cc8b9e2c0490.metadata.json
QB_PIN_META_ICEBERG_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T171110Z/iceberg/products/00064-460325b2-00e3-4714-b40b-2aa66cfe5c12.metadata.json
QB_PIN_META_PAIMON_CUSTOMERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T171110Z/paimon/customers/v104.metadata.json
QB_PIN_META_PAIMON_ORDERS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T171110Z/paimon/orders/v124.metadata.json
QB_PIN_META_PAIMON_ORDER_ITEMS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T171110Z/paimon/order_items/v123.metadata.json
QB_PIN_META_PAIMON_PRODUCTS=s3://<bucket>/querybench-pins/cdc-session-v3/20261110T171110Z/paimon/products/v123.metadata.json
QB_PIN_PREFIX=querybench-pins/cdc-session-v3
QB_PIN_SNAP_ICEBERG_CUSTOMERS=4710971306199873401
QB_PIN_SNAP_ICEBERG_ORDERS=2693351644727175885
QB_PIN_SNAP_ICEBERG_ORDER_ITEMS=5804433427551117286
QB_PIN_SNAP_ICEBERG_PRODUCTS=7818021361323693901
QB_PIN_SNAP_PAIMON_CUSTOMERS=104
QB_PIN_SNAP_PAIMON_ORDERS=124
QB_PIN_SNAP_PAIMON_ORDER_ITEMS=123
QB_PIN_SNAP_PAIMON_PRODUCTS=123
pinned_at=2026-11-10 17:21:52Z
slice=[2026-11-09 14:50:00, 2026-11-09 15:00:00) (10 min, the busiest bucket of the partition (rows: iceberg 3836248, paimon 3836248; 18 non-empty buckets); formats: iceberg paimon)
caches_cleared_at=2026-11-10 17:23:48Z
passes=3 (pass 0 cold, timed; median of the other 2)
duckdb_finished_at=2026-11-10 17:29:02Z rc=0
trino_finished_at=2026-11-10 17:32:43Z rc=0
finished_at=2026-11-10 17:32:43Z rc=0
```
</details>

<details><summary>Ingest per table (2026-11-10T16:07:48Z to 2026-11-10T16:24:01Z)</summary>

| format | table | rows/s | rows | write commits | median interval s | data files | delete files | eq deletes (rows) | pos deletes (rows) | DVs | compaction commits | first / last commit |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| iceberg | customers | 294 | 285,875 | 17 | 60.0 | 136 | 272 | 215,747 | 70,128 | 136 | 0 | 16:07:49Z / 16:23:49Z |
| iceberg | products | 4,052 | 3,938,389 | 17 | 59.9 | 1,224 | 2,448 | 3,004,675 | 933,714 | 1,224 | 0 | 16:07:49Z / 16:23:49Z |
| iceberg | orders | 12,181 | 11,839,721 | 17 | 60.0 | 1,904 | 3,777 | 9,855,081 | 2,325,383 | 1,873 | 0 | 16:07:49Z / 16:23:49Z |
| iceberg | order_items | 17,055 | 16,577,273 | 17 | 60.0 | 1,904 | 3,808 | 12,440,891 | 4,626,692 | 1,904 | 0 | 16:07:49Z / 16:23:49Z |
| paimon | customers | 223 | 216,614 | 17 | 60.0 | 136 | 47 | 0 | - | - | 13 | 16:07:55Z / 16:24:00Z |
| paimon | products | 3,101 | 3,014,641 | 17 | 60.0 | 1,224 | 491 | 0 | - | - | 17 | 16:07:55Z / 16:23:59Z |
| paimon | orders | 10,127 | 9,843,547 | 17 | 60.0 | 1,904 | 549 | 0 | - | - | 17 | 16:07:55Z / 16:24:01Z |
| paimon | order_items | 12,814 | 12,455,236 | 17 | 60.0 | 1,904 | 505 | 0 | - | - | 16 | 16:07:56Z / 16:23:58Z |

Job-monitor aggregate (ingest_stats.py, reports overlapping the window):

| field | Paimon | Iceberg |
|---|---:|---:|
| backpressured_ratio_max | 0.0 | 0.0 |
| bytes_per_s_max | 1287282.96 | 895454.11 |
| bytes_per_s_median | 1159883 | 823576 |
| checkpoint_ms_median | 2272 | 1580 |
| committed_rows_per_s_median | 25491 | 32091 |
| first_start | 2026-11-10T16:06:45.942316Z | 2026-11-10T16:06:45.943510Z |
| last_end | 2026-11-10T16:24:13.165066Z | 2026-11-10T16:24:13.210946Z |
| reports | 9 | 9 |
| rows_per_s_max | 64018.93 | 63946.2 |
| rows_per_s_median | 56186 | 55132 |
| rows_total | 47603680 | 49474393 |
| sampled_s | 1080 | 1080 |

</details>

