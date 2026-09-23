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
CALL iceberg_catalog.system.rewrite_data_files(
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

## Performance Benchmarks

**Run of 2026-09-22 — 40,000 rows/s sustained upsert load.** Both pipelines ingested the
same MySQL binlog stream concurrently; both were verified `RUNNING` with 34/34 checkpoints
completed and 0 failed before any query was timed. Query timings are medians of 3
server-side `Time=` readings from the StarRocks FE audit log, taken while the load was
still running. Raw CSV: `bench-results/starrocks-audit-20260922T160312Z.csv`.

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

### Conclusions from this run

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

```bash
# 40k/s upsert load
kubectl apply -f mysql-data-generator/mysql-upsert-loadgen.yaml

# Flink-side: records written, throughput, checkpoint success/duration, state size
./monitoring/capture-flink-metrics.sh            # WINDOW=300 by default

# StarRocks-side: server-side Time/ScanRows/ScanBytes per query, both catalogs
./sql-scripts/starrocks/bench-server-side.sh
```

### Running a comparison that means something

1. **Start both pipelines from the same clean slate.** Empty both S3 warehouse prefixes
   and drop both Glue databases, then launch the two jobs together so their snapshot
   phases overlap. Otherwise you are comparing different amounts of data.
2. **Verify both are healthy before measuring.** A Flink job reports `RUNNING` with dead
   sink committers: `/jobs` shows no error while the table silently stops advancing. Check
   per-vertex task states and `/jobs/<id>/checkpoints`, and confirm freshness on each side
   (`MAX(updated_at)` vs `UTC_TIMESTAMP()`) before trusting any query timing.
3. **Take query timings server-side.** Use `bench-server-side.sh`, which reads `Time=`,
   `ScanRows=` and `ScanBytes=` from the StarRocks FE audit log.
   `bench-hot-data.sh` times client-side around `kubectl exec`, which costs ~5.5 s per
   call on this cluster — enough that a bare `SELECT 1;` and a four-table join came back
   within a few hundred ms of each other. Its absolute numbers are not quotable.
4. **Report `ScanRows` beside every timing.** It distinguishes "answered faster" from
   "matched nothing", which a wall-clock figure alone cannot.
5. **Use `UTC_TIMESTAMP()` in hot-window predicates, never `NOW()`.** The FE session
   timezone is +08:00 while CDC writes UTC; `NOW()` silently matches zero rows.
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
