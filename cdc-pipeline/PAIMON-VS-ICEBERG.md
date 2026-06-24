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
│                            │ Paimon (5,102ms)                         │ Iceberg (870ms)                │
├────────────────────────────┼──────────────────────────────────────────┼────────────────────────────────┤
│ Checkpoint work            │ Compact + expire + flush + dual metadata │ Append + flush + metadata      │
│ Post-checkpoint state      │ Clean (468 files, 0 deletes)             │ Dirty (770 files, 781 deletes) │
│ Needs external compaction  │ No                                       │ Yes                            │
│ Read performance over time │ Stable                                   │ Degrades without compaction    │
╰────────────────────────────┴──────────────────────────────────────────┴────────────────────────────────╯
**Paimon pays upfront at checkpoint time → stable read performance.
**Iceberg defers work → fast checkpoints but accumulates technical debt.

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

> **Test Environment:** EMR on EKS 7.12, Flink 1.20, Paimon 1.3.0, Iceberg 1.10.0-amzn-0,
> parallelism=4, 30s checkpoint interval, MySQL 8.0 CDC source (4 tables).

### Write Performance

| Metric | Paimon | Iceberg | Winner |
|--------|--------|---------|--------|
| Cumulative Records Written | 2,347,162 | 1,759,421 | 🟢 Paimon |
| Burst Throughput (catch-up) | ~20,400 rec/s | ~2,330 rec/s | 🟢 Paimon |
| Steady-State Throughput | ~8 rec/s | ~2.3 rec/s | 🟢 Paimon |
| Data Files (all tables) | 468 | 770 | 🟢 Paimon |
| Delete Files | 0 (deletion vectors) | 781 (position deletes) | 🟢 Paimon |
| Compaction | Automatic (LSM-tree) | Manual (`rewrite_data_files`) | 🟢 Paimon |
| Backpressure | None | None | Tie |

### Read / Query Performance

| Metric | Paimon | Iceberg | Winner |
|--------|--------|---------|--------|
| Athena SQL | ❌ Not supported | ✅ Native via Glue | 🟢 Iceberg |
| Athena Spark | ✅ Via Hadoop catalog | ✅ Native | 🟢 Iceberg |
| StarRocks | ✅ Native catalog | ✅ Native catalog | Tie |
| Query Planning Speed | Slower (binary Avro metadata) | Faster (JSON with summary stats) | 🟢 Iceberg |
| Read Amplification (MoR) | Low (in-file bitmap DVs) | High (781 delete files to merge) | 🟢 Paimon |
| Partition Pruning | Manual string columns | Hidden partition transforms | 🟢 Iceberg |
| Time Travel | `scan.timestamp` hint | `VERSION AS OF` / `TIMESTAMP AS OF` | 🟢 Iceberg |

### Storage & Metadata

| Metric | Paimon | Iceberg | Winner |
|--------|--------|---------|--------|
| Total Storage | 59.4 MB | Managed via Glue | — |
| File-to-Delete Ratio | 468 : 0 | 770 : 781 (~1:1) | 🟢 Paimon |
| Snapshot Retention | 5 (auto-expired) | 107+ (accumulating) | 🟢 Paimon |
| Metadata Format | Binary Avro | JSON with rich summary | 🟢 Iceberg |
| Iceberg Compatibility | ✅ via `IcebergHiveMetadataCommitter` | Native | 🟢 Iceberg |

### Verdict

| Strength | Winner | Why |
|----------|--------|-----|
| **CDC Write Throughput** | 🟢 Paimon | 8.7× higher burst, auto-compaction, zero delete files |
| **Query Engine Support** | 🟢 Iceberg | Native Athena SQL, Spark, Trino, Presto, StarRocks |
| **Storage Efficiency** | 🟢 Paimon | 40% fewer files, no delete file overhead, aggressive expiration |
| **Read Performance** | 🟢 Iceberg | Rich metadata → fast planning; but degrades as delete files grow |
| **Operational Simplicity** | 🟢 Paimon | No compaction scheduling, no delete file management |
| **Ecosystem Maturity** | 🟢 Iceberg | Broader adoption, better tooling, native AWS integration |

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
