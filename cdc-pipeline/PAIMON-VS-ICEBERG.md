# Paimon vs Iceberg: Lakehouse Comparison Guide

## Executive Summary

Both **Apache Paimon** and **Apache Iceberg** are open table formats that enable building data lakehouses on S3. This guide helps you choose the right format for your Flink CDC use case.

> **Latest measured findings:** [Iceberg v3 on EMR 7.14 (2026-11-02)](#iceberg-v3-on-emr-714-findings-of-2026-11-02).
> The StarRocks-era benchmark below ([Performance Benchmarks](#performance-benchmarks)) is historical.

## Quick Comparison

| Aspect | **Apache Paimon** | **Apache Iceberg** |
|--------|-------------------|-------------------|
| **Primary Focus** | Streaming-first lakehouse | Batch-first with streaming support |
| **Best For** | Real-time data warehouses, streaming analytics | Data lakes, batch analytics, time travel |
| **Catalog** | File-based (S3) | AWS Glue, Hive, Nessie, REST |
| **Maturity** | Newer (2023) | Mature (2017, Netflix) |
| **Community** | Growing, Flink-native | Large, multi-engine |
| **Write Performance** | Faster for streaming | Optimized for batch |
| **Read Performance** | Excellent for recent data | Excellent for all data |
| **Compaction** | Built-in, automatic | Manual or scheduled |
| **Schema Evolution** | Full support | Full support |
| **Time Travel** | Snapshot-based | Snapshot + timestamp |
| **ACID Transactions** | Yes | Yes |
| **Hidden Partitioning** | Manual (string-based) | Automatic (function-based) |

---

## Architecture Comparison

### Paimon Architecture

```
MySQL CDC → Flink
            ↓
         Paimon Catalog (S3)
            ↓
     ┌──────┴──────┐
     │  Metadata   │ (snapshots, manifests)
     └──────┬──────┘
            ↓
     ┌──────┴──────┐
     │ Data  Files │ (Parquet in buckets)
     └─────────────┘
            ↓
     Spark/Flink Queries
```

**Key Features:**
- Self-contained on S3
- Built-in LSM-tree compaction
- Primary key tables with UPSERT
- Consumer-based streaming reads
- Bucket-based parallelism

### Iceberg Architecture

```
MySQL CDC → Flink
            ↓
      Iceberg Catalog (AWS Glue)
            ↓
     ┌──────┴──────┐
     │  Glue Tables│ (metadata service)
     └──────┬──────┘
            ↓
     ┌──────┴──────┐
     │ S3 Metadata │ (snapshots, manifests)
     └──────┬──────┘
            ↓
     ┌──────┴──────┐
     │ Data Files  │ (Parquet with hidden partitions)
     └─────────────┘
            ↓
     Athena/Spark/Presto/Trino/Starrocks
```

**Key Features:**
- Centralized metadata in AWS Glue
- Hidden partitioning (automatic)
- Multi-engine compatibility
- Time travel by timestamp
- Row-level deletes (v2)

---

## Detailed Comparison

### 1. Catalog Management

#### Paimon
```python
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 's3://bucket/paimon-warehouse/'
    # Glue integration
    'metastore' = 'hive',
    'metastore.client.class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient'
    # provided by EMR on EKS flink
    'hive-conf-dir' = '/glue/confs/hive/conf',
    'hadoop-conf-dir' = '/glue/confs/hadoop/conf',
    'lock.enabled' = 'false'
)
```

**Pros:**
- ✅ No external dependencies
- ✅ Simpler architecture
- ✅ Portable (just copy S3 bucket)
- ✅ No additional AWS service costs

**Cons:**
- ❌ Limited discoverability from other tools
- ❌ File-based locking can have issues

#### Iceberg
```python
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
    'warehouse' = 's3://bucket/iceberg-warehouse/'
)
```

**Pros:**
- ✅ Central metadata in AWS Glue
- ✅ Native integration with Athena
- ✅ Better discoverability
- ✅ Stronger consistency

**Cons:**
- ❌ Dependency on AWS Glue
- ❌ Additional AWS service costs
- ❌ Better tools and 3P integration

### 2. Partitioning

#### Paimon
```sql
-- No hidden partition support
-- Manual partitioning with string partition column
CREATE TABLE orders (
    order_id INT,
    order_date TIMESTAMP(6),
    order_date_str STRING,  -- Must create partition column
    ...
    PRIMARY KEY (order_date_str, order_id) NOT ENFORCED
) PARTITIONED BY (order_date_str)
```

**Characteristics:**
- Must manually create partition columns
- String-based partitions
- Partition pruning requires explicit column in WHERE clause

#### Iceberg
```sql
-- Hidden partitioning with function-based transforms
CREATE TABLE orders (
    order_id INT,
    order_date TIMESTAMP(3),
    ...
    PRIMARY KEY (order_id) NOT ENFORCED
) PARTITIONED BY (DAYS(order_date))  -- Automatic!
```

**Characteristics:**
- **Hidden partitioning** - no extra columns needed
- Function-based transforms: DAYS(), MONTHS(), YEARS(), HOURS()
- Partition pruning automatic in WHERE clauses
- Can evolve partition spec without rewriting data

### 3. UPSERT Semantics

#### Paimon
```sql
CREATE TABLE customers (
    customer_id INT,
    ...
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'deduplicate'
)
```

**Merge Engines:**
- `deduplicate` - default option - latest value wins (for dimensions)
- `aggregate` - Aggregate on updates (for metrics)
- `first-row` - Keep first value

#### Iceberg
```sql
CREATE TABLE customers (
    customer_id INT,
    ...
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'write.upsert.enabled' = 'true'
)
```

**UPSERT Mode:**
- More storage overhead
- Better for batch workloads

### 4. Read Performance

#### Paimon
```sql
-- Streaming read with consumer (exactly-once)
SELECT * FROM customers /*+ OPTIONS(
    'consumer-id' = 'my-consumer',
    'scan.timestamp' = '2026-03-01 00:00:00',
    'consumer.expiration-time' = '60000000'
) */
```

**Characteristics:**
- Optimized for recent data reads
- Excellent streaming performance
- Built-in changelog consumption

#### Iceberg
```sql
-- Streaming read with incremental scan
SELECT * FROM customers /*+ OPTIONS(
    'streaming' = 'true',
    'monitor-interval' = '1s',
    'start-snapshot-id' = '1234567890'
) */
```

**Characteristics:**
- Snapshot-based incremental reads
- Excellent for time travel queries
- Better for historical data scans
- Native Athena Query integration

### 5. Compaction

#### Paimon
**Automatic compaction, retain snapshots for 12 hours** (as in `ingest-deploy/sql-scripts/paimon/03-paimon-sinks.sql`):
```sql
CREATE TABLE orders (...) WITH (
    'num-sorted-run.compaction-trigger' = '4',
    'snapshot.time-retained' = '12h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '1000'   -- above 720 commits in 12 h, so age binds
)
```

(`compaction.max.file-num` is not a Paimon option for primary-key tables; Paimon
stores it and ignores it.)

- Built-in LSM-tree compaction
- Automatic small file merging
- Background compaction threads
- No manual intervention needed

#### Iceberg
**A separate compaction job.** Here: one continuous FlinkDeployment, `iceberg-maintain`
(`iceberg-compaction/maintain.py`, Iceberg 1.12's Flink `TableMaintenance` with
`RewriteDataFiles` + `ExpireSnapshots`), on the writers' image. It is triggered by the
tables' own commits: every 10 writer commits it rewrites the data files (partial
progress, target 64 / 128 MiB) and expires snapshots (12 h, keep 5). A rewrite reloads
the table's whole equality-delete set, so it must start with the writers: started late
on 2026-11-03 it faced ~35M deletes on `customers`, needed a 32 Gi TaskManager, and
completed passes on `products` only. With the writers suspended nothing triggers it.

**Retain snapshots for 12 hours** (as in `ingest-deploy/sql-scripts/iceberg/03-iceberg-sinks.sql`;
applied when snapshots are expired, here by the compaction job)
```sql
CREATE TABLE orders (...) WITH (
    'history.expire.max-snapshot-age-ms' = '43200000',
    'history.expire.min-snapshots-to-keep' = '5',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '3'
)
```

- Manual compaction required
- Can schedule in Glue or use S3Tables's automatic fully managed compaction
- Large default compaction target file size (512MB), suits to batch.

### 6. Schema Evolution

#### Paimon
```sql
-- Add column
ALTER TABLE customers ADD COLUMN email_verified BOOLEAN;

-- Change column type (with re-write)
ALTER TABLE customers ALTER COLUMN phone TYPE STRING;
```

**Support:**
- ✅ Support add,drop,rename columns
- ⚠️ Limited column type changes 
- ⚠️ Metadata-based changes where possible

#### Iceberg
```sql
-- Add column
ALTER TABLE customers ADD COLUMN email_verified BOOLEAN;

-- Safe type evolution
ALTER TABLE customers ALTER COLUMN nested_col.field TYPE BIGINT;  -- int to bigint
```

**Support:**
- ✅ Support add,drop,rename columns
- ✅ Metadata-only changes when type changes

### 7. Time Travel

#### Paimon
```sql
-- Get list of snapshot IDs
SELECT * FROM orders.snapshots;
-- Query by snapshot ID
df.spark.read.option("snapshot-id", "899").table("customers").df.show()
-- Query by timestamp
SELECT * FROM orders /*+ OPTIONS(
    'scan.timestamp' = '2024-01-01 00:00:00'
) */;
```

**Characteristics:**
- Snapshot-based time travel
- Fast for recent snapshots
- Retention based on count or time

#### Iceberg
```sql
-- Query by snapshot ID
SELECT * FROM "flink_iceberg_db"."orders$snapshots"

SELECT * FROM orders FOR VERSION AS OF 123456789;
-- Query by timestamp
SELECT * FROM orders FOR TIMESTAMP AS OF '2024-01-01 00:00:00';
```

**Characteristics:**
- Native SQL syntax (ANSI SQL)
- Works across all Iceberg engines
- Metadata versioning with history
- Better support in BI tools

8. Checkpoint Tradeoff

╭────────────────────────────┬──────────────────────────────────────────┬────────────────────────────────╮
│                            │ Paimon                                   │ Iceberg                        │
├────────────────────────────┼──────────────────────────────────────────┼────────────────────────────────┤
│ Checkpoint duration        │ Longer                                   │ Shorter                        │
│ Checkpoint work            │ Compact + expire + flush + dual metadata │ Append + flush + metadata      │
│ Post-checkpoint state      │ Clean (in-file bitmap DVs, no deletes)   │ Dirty (data + delete files)    │
│ Needs external compaction  │ No                                       │ Yes                            │
│ Read performance over time │ Stable                                   │ Degrades without compaction    │
╰────────────────────────────┴──────────────────────────────────────────┴────────────────────────────────╯
**Paimon pays upfront at checkpoint time → stable read performance.
**Iceberg defers work → fast checkpoints but accumulates technical debt.

The direction of this tradeoff is structural, but the magnitude is workload-specific —
measure it on your own data rather than assuming a ratio. See
[Performance Benchmarks](#performance-benchmarks) for how to capture it.

---

## Use Case Matrix

### Choose **Paimon** When:

✅ **Streaming-First Architecture**
- Real-time data warehouse requirements
- Need sub-second latency for recent data
- Heavy streaming read workloads
- Consumer-based offset management

✅ **Simplicity Preferred**
- Want fewer AWS dependencies
- Prefer file-based catalog (no Glue)
- Simpler deployment and ops
- Flink-only ecosystem

✅ **Cost Optimization**
- No AWS Glue costs
- Automatic compaction (no orchestration needed)
- Smaller operational overhead

✅ **Specific Features**
- Need built-in changelog consumption
- Require multiple merge engines (deduplicate, aggregate)
- Want automatic compaction without scheduling

**Example Use Cases:**
- Real-time dashboards
- Streaming ETL with Flink
- Change data capture (CDC) processing
- Event-driven applications

### Choose **Iceberg** When:

✅ **Multi-Engine Ecosystem**
- Need Athena/Presto/Trino access
- Mix of batch and streaming workloads
- BI tools require Glue catalog
- Multiple query engines

✅ **Batch-Heavy Workloads**
- Primarily batch analytics
- Large historical data scans
- Complex time travel requirements
- Data science workloads

✅ **Enterprise Requirements**
- Need central metadata management
- Governance and discovery (Glue Data Catalog)
- Compliance and audit requirements
- Schema registry integration

✅ **Specific Features**
- Hidden partitioning (automatic)
- Row-level deletes (Format V2)
- Partition evolution
- Better Athena integration

**Example Use Cases:**
- Data lake analytics
- BI and reporting (Athena/Redshift Spectrum)
- Data science with Spark
- Historical data analysis

---

## Iceberg v3 on EMR 7.14: findings of 2026-11-02

**Setup.** First v3 dry run on the `cdc-icebergv3` branch: EMR on EKS 7.14.0 (Flink
1.20.0-amzn-8), Paimon 2.1.0, Iceberg Flink runtime 1.12.0, `format-version = 3` on
the native Iceberg tables and on Paimon's Iceberg export
(`ICEBERG_FORMAT_VERSION=3`, `deletion-vectors.bitmap64 = true`). About one hour of
the upsert workload at ~64,000 rows/s against the shrunk source (~37.5M rows; see
[0-MYSQL-SETUP-GUIDE.md](./0-MYSQL-SETUP-GUIDE.md)). Readers: DuckDB 2.0.0 (built
from source) and StarRocks 4.1.7.

### Readers

| Reader | Native Iceberg v3 (DVs + equality deletes) | Paimon v3 |
|---|---|---|
| **DuckDB 2.0** (`iceberg` extension) | **Reads it**, including deletion vectors (Puffin) and equality deletes. `customers`: 17.07M rows in 7.5 s; `order_items`: 177M rows in 28.1 s; 0 duplicate keys | **Fails** on the Iceberg export: the exported manifests have no Avro key-value metadata field `schema`, which DuckDB requires (`Manifest ... missing required Avro key-value metadata field 'schema'`) |
| **StarRocks 4.1.7** | **Refuses** any scan that touches a DV: `Iceberg V3 Deletion Vectors are not supported` (a v3 table with only equality deletes still plans) | **Fails** in both Paimon readers. The native C++ reader cannot decode the 64-bit DV layout (`can not read fully`). The JNI reader (`SET paimon_force_jni_reader = true`) fails with an `InstantiationIOException` on StarRocks' credential provider class (`com.starrocks.credential.provider.…`) when it opens the DV index files |
| **Athena engine v3** | Not supported (Iceberg v2 only) | Not supported |

**Net result:** on v3, the only engine in this stack that read native Iceberg was
DuckDB, and none of these read Paimon. StarRocks has since been removed from the CDC
pipeline. The query benchmark now runs DuckDB and Trino
([querybench/README.md](./querybench/README.md)). Neither has a native Paimon
reader (no Paimon connector in the DuckDB image, none for Trino 484): both
"paimon" arms read **Paimon's Iceberg export** through an Iceberg reader. Trino 484
reads that v3 export (verified 2026-11-02, `_v3r2` hot test); DuckDB 2.0 still
fails on it with the `schema` error above.

### Paimon's Iceberg export: behaviours to know

- **v3 needs 64-bit deletion vectors.** Paimon writes DVs into the v3 export only when
  all three hold (`IcebergCommitCallback.needAddDvToIceberg`):
  `deletion-vectors.enabled = true`, `deletion-vectors.bitmap64 = true`,
  `metadata.iceberg.format-version = 3`. `bitmap64` takes effect at CREATE TABLE
  only (existing 32-bit DV index files are not rewritten), and it breaks StarRocks'
  native Paimon reader. That is why `ingest-deploy/build-deploy-generic.sh` derives
  `PAIMON_DV_BITMAP64` from the format version (true for v3, false for v2) instead of
  exposing it as a setting. On a v2 export Paimon writes no DVs and lists only files
  at the top LSM level, so v2 readers see a stale, read-optimized view until a full
  compaction.
- **The Glue entries are re-registered as ICEBERG.** With
  `metadata.iceberg.storage = hive-catalog`, the export's committer re-registers each
  table in the Paimon Glue database (`flink_paimon_db<suffix>`) as
  `table_type = ICEBERG`, pointing at the export's metadata. Glue-based Iceberg
  readers (Trino's `trino:paimon` arm) read Paimon through those entries. Paimon's
  committer owns them, so do not edit them by hand.
- **`metadata.json` rotation.** Paimon's default kept **0** previous metadata files:
  each commit deleted the previous `metadata.json`, so a path read from Glue returned
  404 within one checkpoint, mid-scan. The sink DDL now sets
  `metadata.iceberg.previous-versions-max = 3` with
  `metadata.iceberg.delete-after-commit.enabled = true`, the same as the native
  tables' `write.metadata.previous-versions-max = 3`. A reader still has to pin one
  metadata version for the length of a query.
- **Freshness.** With DVs on, lookup compaction moves level 0 up at every checkpoint,
  so the v3 export trails the table by about one checkpoint without forced full
  compaction.

### Write-side asymmetries kept on purpose

The sink tables are paired on every setting both formats have (retention, metadata
versions, manifest merging, stats, file and row-group sizes, compression; see
[1-BUILD-DEPLOY-FLINK-APP-GUIDE.md](./1-BUILD-DEPLOY-FLINK-APP-GUIDE.md#sink-table-settings-paimon-vs-iceberg)).
These differences remain by design. Report them next to every result:

| Difference | Paimon | Iceberg |
|---|---|---|
| Writer parallelism | 32 writers per table (`bucket = 32`) | `write.distribution-mode = hash` on a date-partitioned upsert table sends each date partition to one writer, so today's changes go through a single writer. At the dry run's source-limited rate both kept up; above it, that single writer is the expected ceiling |
| Compaction | inline, in the writer (LSM lookup compaction, deletion vectors) | none in the sink; a separate continuous FlinkDeployment `iceberg-maintain` (`iceberg-compaction/`), every 10 writer commits while the writers run |
| Snapshot expiry timing | in the writer, at commit time (`snapshot.expire.execution-mode = async`) | only when `iceberg-maintain` runs, with the same 12 h / keep-5 rule. Neither expires while its writer is suspended |
| Row-level deletes | deletion vectors in Paimon's index files | equality deletes for rows from earlier checkpoints, DVs (v3) or position deletes (v2) for rows rewritten within one checkpoint. The sink's equality-delete-to-DV converter is not enabled |
| Glue integration | EMR's Glue Hive metastore client (`aws.glue.enabled: "true"`, the one intended `flinkConfiguration` difference) | Iceberg's own `GlueCatalog` |
| Schema details | `TIMESTAMP(6)`; partition columns `order_date_str`, `created_date_str` | `TIMESTAMP(3)`; partition columns `order_dt`, `created_dt` |
| Paimon-only knobs | write buffers (64/128 MB, spillable; = Iceberg target file size; `INSERT` hints), `num-sorted-run.compaction-trigger = 4`, `sink.writer-coordinator.enabled` | — |

---

## Performance Benchmarks

> **Historical: StarRocks era, 2026-09-22.** Iceberg format v2, EMR 7.12 with its
> bundled Iceberg runtime, Paimon 1.3, the full-size source (before the 2026-11-02 shrink), operator chaining
> off, StarRocks 4.1 as the only query engine. StarRocks has since been removed from
> the CDC pipeline, and none of these settings match the current branch. The
> "Ingest side" rows came from the earlier monitor, which summed `numRecordsOut`
> over every vertex and over-counts by about 6x on Paimon and 3x on Iceberg
> ([3-MONITOR.md](./3-MONITOR.md#rows-s-from-the-tables-commit-history-per-table)), so
> do not compare them with current monitor output. The query-side ratios describe
> that configuration only. For current results, see
> [the 2026-11-02 findings](#iceberg-v3-on-emr-714-findings-of-2026-11-02) and
> [querybench/](./querybench/README.md).

**Run of 2026-09-22 — 40,000 rows/s sustained upsert load.** Both pipelines ingested the
same MySQL binlog stream concurrently; both were verified `RUNNING` with 34/34 checkpoints
completed and 0 failed before any query was timed. Query timings are medians of 3
server-side `Time=` readings from the StarRocks FE audit log, taken while the load was
still running. Raw CSV: `bench-results/starrocks-audit-20260922T160312Z.csv` (in git
history).

### Write workload

| Parameter | Value |
|-----------|-------|
| Sustained rate | **~39,960 rows/s** (8 generator pods × 4,997/s measured) |
| Operation mix | 80% upsert on existing PKs (`ON DUPLICATE KEY UPDATE`), 20% new inserts |
| Key skew | 60% of upserts into a 50,000-key hot window |
| Total applied before benchmark | ≈41M row-modifications over ~17 min |

### Ingest side — Iceberg is cheaper to write

| Metric (120 s window) | Paimon | Iceberg | Δ |
|-----------------------|-------:|--------:|---|
| Records processed | 29,334,839 | 18,215,135 | −37.9% |
| Throughput | 293,301 rec/s | 182,103 rec/s | −37.9% |
| Avg checkpoint duration | 11,745 ms | **981 ms** | **−91.6%** |
| Failed checkpoints | 0 | 0 | — |
| New data files | 352 | 29 | — |
| New delete files | **0** | **58** | — |
| New equality deletes | 0 | 1,376,345 | — |
| New position deletes | 0 | 717,026 | — |
| Warehouse size | 2.94 GB | **1.86 GB** | −37% |
| Warehouse objects | 6,708 | **3,564** | −47% |

Paimon's checkpoints are **12x longer** because its writers do LSM merge work inline.
Iceberg's committer just appends data files plus delete files and returns — which is why its
checkpoints are sub-second and its storage footprint is ~37% smaller.

### Read side — Paimon wins every query

Delete-file accumulation at benchmark time (the mechanism behind every ratio):

| Table | Iceberg rows | data files | eq-delete files | delete records |
|-------|-------------:|-----------:|----------------:|---------------:|
| customers | 3,994,246 | 544 | 544 | 10,090,377 |
| products | 4,095,398 | 306 | 306 | 8,366,034 |
| orders | 9,370,063 | 69 | 69 | 23,045,340 |
| order_items | 21,012,997 | 73 | 72 | 41,244,366 |

Roughly one delete file per data file, and on every table **more delete records than live
rows**. Merge-on-read must read and apply all of it on every query.

**Snapshot queries:**

| Query | Paimon ms | Iceberg ms | Ratio | Paimon ScanRows | Iceberg ScanRows |
|-------|----------:|-----------:|------:|----------------:|-----------------:|
| Q1_count | 212 | 457 | 2.16x | 5,299,513 | 13,289,772 |
| Q2_group_by | 246 | 555 | 2.26x | 5,299,513 | 13,289,772 |
| Q3_join | 666 | 2,804 | 4.21x | 16,815,938 | 45,746,838 |
| Q4_product_sales | 1,230 | 3,992 | 3.25x | 29,016,478 | 63,787,030 |
| Q5_time_window | 347 | 2,323 | 6.69x | 11,516,425 | 32,457,066 |
| Q6_multi_join | 1,616 | 5,094 | 3.15x | 45,832,416 | 109,533,868 |

**Hot-data queries** (recent upserts, `UTC_TIMESTAMP()` windows):

| Query | Paimon ms | Iceberg ms | Ratio | Paimon ScanRows | Iceberg ScanRows |
|-------|----------:|-----------:|------:|----------------:|-----------------:|
| Q7_freshness | 218 | 1,955 | **8.97x** | 2,752,437 | 16,249,483 |
| Q8_hot_pk_lookup | 480 | 2,880 | **6.00x** | 12,042,127 | 48,762,824 |
| Q9_recent_window | 235 | 2,378 | **10.12x** | 4,829,282 | 19,358,652 |
| Q10_hot_join | 693 | 4,422 | **6.38x** | 29,583,354 | 72,966,002 |

### Conclusions from this run (historical)

1. **The gap is far larger on hot data than on historical data** — 6.0–10.1x vs 2.2–6.7x.
   Hot rows are where the upserts are landing, so they carry the newest, densest, least
   compacted delete files. If your workload queries recent data, this is the number that
   matters, and it is the one that most favours Paimon.
2. **Scan volume explains the timings; the relationship is super-linear.** Iceberg scans
   2.5–4.0x more rows for identical results, but takes up to 10x longer, because deletes
   must be *applied* and not merely read. Q9: 4.0x the scan, 10.1x the time.
3. **This is a straight time-shift, not a free win.** Paimon spends on writes (12x
   checkpoint duration, 1.6x storage, 1.9x objects) what it saves on reads. Iceberg's
   sub-second checkpoints are genuinely attractive for write-heavy pipelines whose reads are
   infrequent or batch.
4. **Paimon needed no operator action; Iceberg would.** Every Paimon number here reflects
   automatic background compaction. The Iceberg numbers are the *uncompacted* steady state
   that a pipeline reaches on its own — which is precisely the realistic state if nobody has
   scheduled `rewrite_data_files`. Iceberg's read performance is recoverable with
   maintenance, but that maintenance is a job somebody has to run, schedule, and pay for.

### Regenerating

The 2026-09 numbers cannot be regenerated as they were: the StarRocks harness
(`ingest-deploy/sql-scripts/starrocks/bench-server-side.sh`) was removed with StarRocks and is in git
history only. The current equivalents:

```bash
# upsert load (4 pods)
./mysql-data-generator/deploy-data-generator.sh deploy

# Flink side: one bounded REST window (WINDOW=120 by default), or the monitor pod
./monitoring/capture-flink-metrics.sh
bash ./monitoring/deploy-monitor.sh

# Query side: DuckDB vs Trino, server-side times
SUITE=hot ./querybench/run.sh start
```

### Running a comparison that means something

1. **Start both pipelines from the same clean slate.** Empty both S3 warehouse prefixes
   and drop both Glue databases, then launch the two jobs together so their snapshot
   phases overlap. Otherwise you are comparing different amounts of data.
2. **Verify both are healthy before measuring.** A Flink job reports `RUNNING` with dead
   sink committers: `/jobs` shows no error while the table silently stops advancing. Check
   per-vertex task states and `/jobs/<id>/checkpoints`, and confirm freshness on each side
   (`MAX(updated_at)` vs `UTC_TIMESTAMP()`) before trusting any query timing.
3. **Take query timings server-side.** The 2026-09 numbers came from the StarRocks FE
   audit log (`Time=`, `ScanRows=`, `ScanBytes=`); `querybench/` now takes them from the
   DuckDB server's own timer and Trino's `system.runtime.queries`. Timing client-side
   around `kubectl exec` costs ~5.5 s per call on this cluster — enough that a bare
   `SELECT 1;` and a four-table join came back within a few hundred ms of each other.
4. **Report `ScanRows` beside every timing.** It distinguishes "answered faster" from
   "matched nothing", which a wall-clock figure alone cannot.
5. **Use UTC in hot-window predicates.** CDC writes UTC timestamps. On StarRocks the
   FE session timezone was +08:00, so `NOW()` silently matched zero rows; querybench
   uses explicit `TIMESTAMP` literals for the same reason.
6. **Measure Iceberg before and after `rewrite_data_files`.** Paimon's LSM compacts
   continuously with no operator action, so a single Iceberg state is not a fair
   comparison — capture both and label which is which. This asymmetry is itself a finding
   and should not be smoothed over by compacting Paimon too.

Dimensions worth capturing: records written and throughput over an identical window;
checkpoint completion rate and duration; end-to-end freshness lag; snapshot-query and
hot-window-query latency with scan volume; S3 object count and total size; and
data-file-to-delete-file ratio on the Iceberg side.

---

## Cost Comparison (Monthly, 1TB data lake)

| Cost Component | Paimon | Iceberg |
|----------------|--------|---------|
| **S3 Storage (1TB)** | $23 | $23 |
| **AWS Glue Catalog** | $0 (file-based) | $1 (100K requests) |
| **S3 API Calls** | ~$0.50 | ~$1.00 (more metadata) |
| **Compaction Compute** | Included | $5-10 (orchestration) |
| **Total Monthly Cost** | **~$24** | **~$30-35** |

**Savings:** Paimon ~30% cheaper for this workload

---

## Migration Path

### From Paimon to Iceberg
```sql
-- Export from Paimon
INSERT INTO iceberg_catalog.db.customers
SELECT * FROM paimon_catalog.db.customers;
```

### From Iceberg to Paimon
```sql
-- Import to Paimon
INSERT INTO paimon_catalog.db.customers
SELECT * FROM iceberg_catalog.db.customers;
```

**Note:** Both require full table rewrite. Plan for downtime or dual-write period.

---

## Recommendation Decision Tree

```
Start Here
    │
    ├─ Need AWS Glue integration? ──YES──> Iceberg
    │                               NO
    │                                │
    ├─ Use Athena for queries? ─────YES──> Iceberg
    │                               NO
    │                                │
    ├─ Primarily streaming workload? YES──> Paimon
    │                               NO
    │                                │
    ├─ Need automatic compaction? ──YES──> Paimon
    │                               NO
    │                                │
    ├─ Use multiple query engines? ─YES──> Iceberg
    │                               NO
    │                                │
    ├─ Want simplest architecture? ─YES──> Paimon
    │                               NO
    │                                │
    └─ Mature ecosystem important? ─YES──> Iceberg
                                    NO──> Paimon
```

---

## Hybrid Approach: Use Both!

You can run **both** Paimon and Iceberg in parallel:

```
MySQL CDC
    ├──> Paimon (for real-time dashboards)
    └──> Iceberg (for batch analytics & Athena)
```

**Benefits:**
- Best tool for each job
- Paimon for hot/recent data queries
- Iceberg for cold/historical data
- Athena access to Iceberg
- Flink streaming from Paimon

**Cost:**
- 2x storage (S3 compressed, so ~1.5x actual cost)
- Dual-write overhead in Flink

---

## Summary

| If You Value... | Choose |
|----------------|--------|
| **Streaming performance** | Paimon |
| **Batch analytics** | Iceberg |
| **Simplicity** | Paimon |
| **Ecosystem maturity** | Iceberg |
| **Automatic compaction** | Paimon |
| **Athena integration** | Iceberg |
| **Lower costs** | Paimon |
| **Central metadata** | Iceberg |
| **Flink-only workloads** | Paimon |
| **Multi-engine queries** | Iceberg |

**General Recommendation:**
- **Start with Paimon** if you're Flink-focused and streaming-heavy
- **Start with Iceberg** if you need AWS ecosystem integration
- **Use both** if you have the budget and need best-of-both-worlds

Both are excellent choices for building data lakehouses with Flink CDC! 🚀
