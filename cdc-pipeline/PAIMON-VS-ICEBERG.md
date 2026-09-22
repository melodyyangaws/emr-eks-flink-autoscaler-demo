# Paimon vs Iceberg: Lakehouse Comparison Guide

## Executive Summary

Both **Apache Paimon** and **Apache Iceberg** are open table formats that enable building data lakehouses on S3. This guide helps you choose the right format for your Flink CDC use case.

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
CREATE CATALOG paimon_catalogv3 WITH (
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
CREATE CATALOG icebergv3_catalog WITH (
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
**Automatic compaction, retain snapshots for 1 hour:**
```sql
CREATE TABLE orders (...) WITH (
    'num-sorted-run.compaction-trigger' = '4',
    'compaction.max.file-num' = '8',
    'snapshot.time-retained' = '1h',
    'snapshot.num-retained.min' = '5'
)
```

- Built-in LSM-tree compaction
- Automatic small file merging
- Background compaction threads
- No manual intervention needed

#### Iceberg
**Manual or scheduled compaction:**
```sql
-- Call compaction procedure (Spark/Flink)
CALL icebergv3_catalog.system.rewrite_data_files(
    table => 'orders',
    strategy => 'binpack',
    options => map('target-file-size-bytes','536870912') --default to 512MB
```
**Retain snapshots for 1 hour**
```sql
CREATE TABLE orders (...) WITH (
    'history.expire.max-snapshot-age-ms' = '3600000',
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
SELECT * FROM "flink_icebergv3_db"."orders$snapshots"

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
│                            │ Paimon (8,822ms)                         │ Iceberg (437ms)                │
├────────────────────────────┼──────────────────────────────────────────┼────────────────────────────────┤
│ Checkpoint work            │ Compact + expire + flush + dual metadata │ Append + flush + metadata      │
│ Post-checkpoint state      │ Clean (in-file bitmap DVs, no deletes)   │ Dirty (1:1 data:delete files)  │
│ Needs external compaction  │ No                                       │ Yes                            │
│ Read performance over time │ Stable                                   │ Degrades without compaction    │
╰────────────────────────────┴──────────────────────────────────────────┴────────────────────────────────╯
**Paimon pays upfront at checkpoint time → stable read performance.
**Iceberg defers work → fast checkpoints but accumulates technical debt.

Durations above are from the 2026-09-21 run (see [Performance Benchmarks](#performance-benchmarks)).
One caution on the Iceberg figure: in that run Iceberg's 437 ms was the duration of
checkpoint #5, the **last one that ever succeeded** before the V3 deletion-vector
incompatibility began failing every subsequent commit. The "defers work" tradeoff is real,
but on this release the deferral was total — 59 of 62 checkpoints failed and the table
stopped advancing altogether.

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

## Performance Benchmarks

> **Measured 2026-09-21, `loadtest-mcp` EKS cluster (us-west-2), EMR on EKS 7.12.0,
> Flink 1.20.0-amzn-6, Paimon 1.3.0, Iceberg V3 via bundled `iceberg-flink-runtime.jar`.**
> Job parallelism 16, 60 s checkpoint interval, MySQL 8.0 CDC source (4 tables).
>
> Both jobs were deployed from a clean slate **at the same time** (20:28:01 and 20:28:09
> UTC) after emptying all four S3 prefixes and dropping both Glue databases, so their
> snapshot phases overlap and the comparison is like-for-like. Load: 5 data generator
> replicas, `BATCH_SIZE=220`, `SLEEP_SECONDS=2`.
>
> Flink figures are **deltas over a bounded 322 s window** (a 300 s target; the
> extra 22 s is REST sampling time, and throughput is divided by the real elapsed
> value, not the target), captured with
> `monitoring/capture-flink-metrics.sh`. Lifetime counters are dominated by the initial
> snapshot replay and would hide steady-state behaviour.

### ⚠️ Iceberg V3 + the Flink sink are incompatible on this release

The single most important result of this run is a hard failure, not a ratio.

```
java.lang.IllegalArgumentException: Must use DVs for position deletes in V3: s3://.../orders/data/...parquet
  at org.apache.iceberg.MergingSnapshotProducer.validateNewDeleteFile(MergingSnapshotProducer.java:292)
  at org.apache.iceberg.flink.sink.IcebergFilesCommitter.commitDeltaTxn(IcebergFilesCommitter.java:363)
```

Iceberg format-version 3 **requires** deletion vectors (Puffin) for position deletes. The
bundled Flink sink still writes legacy positional-delete Parquet files, so Iceberg's own
commit validator rejects every delta commit. `'write.delete.vector.enabled' = 'true'` is
already set in the sink DDL and does not help — the writer path ignores it.

What this looks like in practice, and why it is dangerous:

- **59 of 62 checkpoints failed.** The last successful commit was checkpoint #5.
- **The table froze silently.** Newest `orders.updated_at` was 58 minutes stale
  (3,468 s) while Paimon's was 26 s.
- **The job still reported `RUNNING`.** Two of four `IcebergFilesCommitter` tasks were
  `FAILED`; sources, writers and the remaining committers kept running, so neither
  `/jobs` nor the operator's `jobStatus` surfaced the problem. Only
  `/jobs/<id>/checkpoints` and the per-vertex task states revealed it.
- **Writers kept producing orphaned S3 objects** that no snapshot references —
  `order_items/data/` held 6 Parquet objects while `order_items$files` reported 3.

Every Iceberg number below is therefore measured against a frozen, smaller dataset.

### Write Performance (322 s bounded window)

| Metric | Paimon | Iceberg | Winner |
|--------|-------:|--------:|--------|
| Records written in window | 1,820,387 | 645,003 | 🟢 Paimon (2.8×) |
| Throughput | **5,653 rec/s** | 2,003 rec/s | 🟢 Paimon (2.8×) |
| Bytes written | 268.5 MB | 87.6 MB | 🟢 Paimon (3.1×) |
| Lifetime records | 23,879,576 | 13,874,186 | 🟢 Paimon (1.7×) |
| Checkpoints completed in window | **6** | **0** | 🟢 Paimon |
| Checkpoints failed in window | **0** | **5** | 🟢 Paimon |
| Lifetime checkpoints failed | **0 / 61** | **59 / 62** | 🟢 Paimon |
| Last checkpoint duration | 8,822 ms | 437 ms | — (see below) |
| Last checkpoint size | 0.91 MB | 0.05 MB | — (see below) |
| Compaction | Automatic (LSM-tree) | Manual (`rewrite_data_files`) | 🟢 Paimon |

Read the last two rows carefully — they are a trap. Iceberg's checkpoints look 20× faster
and 18× smaller, which reads like a win. They are fast because they are **from
checkpoint #5, the last one that ever completed**, and small because the committer that
would carry real state is dead. A fast checkpoint on a pipeline that cannot commit is not
a performance characteristic.

Iceberg's 2,003 rec/s is likewise not sustained ingest: writers continue to produce files,
but none of it becomes queryable. Paimon's 5,653 rec/s is data actually visible to readers.

### Data freshness — the metric that decides real-time suitability

| Metric | Paimon | Iceberg |
|---|---:|---:|
| Newest row (`orders.updated_at`) | 2026-09-21 21:40:53 | 2026-09-21 20:38:30 |
| Lag behind now | **26 s** | **3,468 s (58 min)** |
| Rows in `orders` | 793,366 | 434,643 |
| Last successful commit | continuous | 20:38 UTC, then never |

### Read / Query Performance (StarRocks 4.1.4)

Server-side `Time` from the StarRocks FE audit log, median of 3 runs. Full methodology,
including why client-side timing had to be discarded, is in
[`4-STARROCKS-OLAP-ENGINE.md`](./4-STARROCKS-OLAP-ENGINE.md#performance-comparison).

| Query class | Paimon | Iceberg | Notes |
|---|---:|---:|---|
| Q1 COUNT(*) | 187 ms | 35 ms | Iceberg scans 241K rows vs Paimon 400K |
| Q6 4-table JOIN | 920 ms | 283 ms | Iceberg scans 2.2M vs Paimon 3.9M |
| **Q7 freshness (5 min)** | 351 ms | 52 ms | **Iceberg scans 61 rows — window is empty** |
| **Q9 10-min rollup** | 235 ms | 55 ms | **Iceberg scans 61 rows — window is empty** |
| **Q10 10-min join** | 562 ms | 111 ms | **Iceberg scans 10,927 vs Paimon 2.66M** |

On static scans (Q1–Q6) Iceberg is genuinely 1.3–2.5× faster **per row scanned** — the
expected benefit of a compacted Parquet layout with rich min/max statistics. On hot
queries it is not faster; it is empty. That is the distinction this test was built to
draw, and the numbers draw it sharply.

| Capability | Paimon | Iceberg | Winner |
|---|---|---|---|
| Athena SQL | ❌ Not supported | ✅ Native via Glue | 🟢 Iceberg |
| StarRocks external catalog | ✅ Native | ✅ Native | Tie |
| Static scan efficiency (per row) | Baseline | 1.3–2.5× faster | 🟢 Iceberg |
| Hot / sub-minute freshness | ✅ 26 s | ❌ frozen | 🟢 Paimon |
| Partition pruning | Manual string columns | Hidden partition transforms | 🟢 Iceberg |
| Time travel | `scan.timestamp` hint | `VERSION AS OF` / `TIMESTAMP AS OF` | 🟢 Iceberg |

### Storage & Metadata

| Metric | Paimon | Iceberg | Winner |
|---|---:|---:|---|
| S3 objects | 6,270 | 209 | — (not comparable) |
| Total size | 252 MB | 41 MB | — (not comparable) |
| Data : delete file ratio | n/a (in-file bitmap DVs) | **1 : 1** (43:43, 18:18, 2:2, 3:3) | 🟢 Paimon |
| Snapshots retained | auto-expired | 2–3 (frozen, not expired) | — |
| Metadata format | Binary Avro | JSON with rich summary | 🟢 Iceberg |

The size columns are not a fair comparison: Iceberg holds 58% of the rows and stopped
writing an hour earlier. The **1:1 data-to-delete-file ratio** is the meaningful number —
every Iceberg data file carries a matching positional-delete file. That is the
merge-on-read tax this test set out to quantify, and it is the same mechanism that tripped
the V3 deletion-vector validator.

### Verdict

| Strength | Winner | Basis in this run |
|---|---|---|
| **CDC write throughput** | 🟢 Paimon | 5,653 vs 2,003 rec/s over an identical 322 s window |
| **Write reliability on V3** | 🟢 Paimon | 61/61 checkpoints vs 3/62; Iceberg cannot commit at all |
| **Real-time freshness** | 🟢 Paimon | 26 s vs 58 min; 3 of 4 hot queries returned nothing on Iceberg |
| **Static scan efficiency** | 🟢 Iceberg | 1.3–2.5× faster per row scanned (on a frozen, compacted table) |
| **Query engine support** | 🟢 Iceberg | Native Athena SQL, Spark, Trino, Presto |
| **Operational simplicity** | 🟢 Paimon | Automatic LSM compaction; no delete-file or DV management |
| **Failure visibility** | 🟢 Paimon | Iceberg showed `RUNNING` with a dead committer for 58 minutes |

**Caveat on scope.** The Iceberg half of this comparison measures a broken pipeline. The
V3/deletion-vector incompatibility must be resolved — either drop the sinks to
`format-version = 2`, which accepts the positional deletes the sink actually writes, or
upgrade `iceberg-flink-runtime.jar` to a build that writes Puffin DVs — before Iceberg's
write throughput or hot-query latency can be fairly compared. Paimon's figures stand on
their own; it ingested continuously with zero checkpoint failures throughout.

**Still outstanding:** the post-`rewrite_data_files` compacted-Iceberg row. Compaction is
meaningless while commits fail, so that measurement is deferred rather than skipped.

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
INSERT INTO icebergv3_catalog.db.customers
SELECT * FROM paimon_catalogv3.db.customers;
```

### From Iceberg to Paimon
```sql
-- Import to Paimon
INSERT INTO paimon_catalogv3.db.customers
SELECT * FROM icebergv3_catalog.db.customers;
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
