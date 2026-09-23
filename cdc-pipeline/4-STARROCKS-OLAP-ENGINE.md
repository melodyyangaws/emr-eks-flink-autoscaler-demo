# OLAP Query Layer Guide — Athena & StarRocks

## Overview

Complete guide for setting up interactive query engines on top of your Paimon and Iceberg lakehouses.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  MySQL RDS (Source)                         │
└────────────────────────┬────────────────────────────────────┘
                         │ Flink CDC
                         ↓
┌─────────────────────────────────────────────────────────────┐
│              Lakehouse Storage Layer (S3)                   │
│  ┌──────────────────────┐    ┌──────────────────────────┐  │
│  │  Paimon Tables       │    │  Iceberg Tables          │  │
│  │  (Hive/Glue Catalog) │    │  (AWS Glue Catalog)      │  │
│  └──────────┬───────────┘    └───────────┬──────────────┘  │
└─────────────┼──────────────────────────────┼────────────────┘
              │                              │
              ↓                              ↓
┌─────────────────────────────────────────────────────────────┐
│              OLAP Query Layer (Choose One or Both)          │
│                                                             │
│  Option 1: AWS Athena (Serverless)                         │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  • Iceberg: Native support via Glue                  │  │
│  │  • Paimon: Via Athena Spark + Hadoop catalog         │  │
│  │  • Pay-per-query pricing                             │  │
│  │  • No infrastructure management                      │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  Option 2: StarRocks on EKS (Self-hosted)                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  • Paimon: Native catalog support                    │  │
│  │  • Iceberg: Native catalog support                   │  │
│  │  • Sub-second query performance                      │  │
│  │  • Vectorized execution engine                       │  │
│  │  • Materialized views & caching                      │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
              │
              ↓
      BI Tools (QuickSight, Tableau, Superset)
```

---

## Option 1: AWS Athena

### Iceberg with Athena (Native Support)

**✅ Iceberg has native Athena support via Glue Catalog**

#### 1.1 Verify Glue Database

```bash
aws glue get-database --name flink_iceberg_db --region $AWS_REGION
aws glue get-tables --database-name flink_iceberg_db --region $AWS_REGION
```

#### 1.2 Query Iceberg Tables (Athena SQL)

Open Athena Console → Data source: `AwsDataCatalog` → Database: `flink_iceberg_db`

```sql
-- Get totla rows
SELECT
    (SELECT COUNT(*) FROM flink_iceberg_db.customers) as customers,
    (SELECT COUNT(*) FROM flink_iceberg_db.products) as products,
    (SELECT COUNT(*) FROM flink_iceberg_db.orders) as orders,
    (SELECT COUNT(*) FROM flink_iceberg_db.order_items) as order_items;

-- Daily revenue
SELECT
    DATE(order_date) as order_day,
    COUNT(*) as order_count,
    SUM(total_amount) as daily_revenue
FROM flink_iceberg_db.orders
WHERE order_date >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY DATE(order_date)
ORDER BY order_day DESC;

-- Customer analytics
SELECT
    c.customer_name, c.city, c.state,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent
FROM flink_iceberg_db.customers c
LEFT JOIN flink_iceberg_db.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name, c.city, c.state
ORDER BY total_spent DESC
LIMIT 20;

-- Product sales
SELECT
    p.product_name, p.category,
    SUM(oi.quantity) as units_sold,
    SUM(oi.subtotal) as revenue
FROM flink_iceberg_db.products p
JOIN flink_iceberg_db.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 20;
```

#### 1.3 Iceberg Time Travel

```sql
-- Query as of specific timestamp
SELECT * FROM flink_iceberg_db.customers
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-02-20 00:00:00';

-- Query specific snapshot
SELECT * FROM flink_iceberg_db.customers
FOR SYSTEM_VERSION AS OF 1234567890;

-- View snapshot history
SELECT * FROM flink_iceberg_db."customers$snapshots";

-- View table manifests
SELECT * FROM flink_iceberg_db."customers$manifests";
```

---

### Paimon with Athena (via Iceberg Compatibility)

**⚠️ Paimon tables are registered in Glue as `table_type = PAIMON`. Athena cannot read them directly.**

Paimon's Iceberg compatibility mode (`metadata.iceberg.storage = 'hive-catalog'`) writes Iceberg-format metadata under the `iceberg/` subdirectory in S3. Use **Athena Spark** with a Hadoop catalog to read this metadata directly, bypassing Glue's `PaimonStorageHandler`.

#### 2.1 Athena Spark Notebook

```python
# Configure Iceberg Hadoop catalog — reads metadata files directly from S3
spark.conf.set("spark.sql.catalog.paimon_iceberg", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.paimon_iceberg.type", "hadoop")
spark.conf.set("spark.sql.catalog.paimon_iceberg.warehouse","${PAIMON_WAREHOUSE}/iceberg")

# Query Paimon tables via Iceberg-compatible metadata
spark.sql("SELECT * FROM paimon_iceberg.flink_paimon_db.customers LIMIT 10").show()

spark.sql("""
    SELECT order_status, COUNT(*) as cnt, SUM(total_amount) as revenue
    FROM paimon_iceberg.flink_paimon_db.orders
    GROUP BY order_status
""").show()
```

> **Why Hadoop catalog?** The GlueCatalog tries to load `PaimonStorageHandler` (not on Athena's classpath) and fails with `ClassNotFoundException`. The Hadoop catalog reads Iceberg `v*.metadata.json` files directly from S3.

> **Important:** Paimon sink tables must have `'metadata.iceberg.format-version' = '2'` (not `'3'`). Athena engine v3 only supports Iceberg format V2.

---

## Option 2: StarRocks on EKS

**🚀 StarRocks provides native support for both Paimon and Iceberg!**

### 3.1 Deploy StarRocks

```bash
export AWS_REGION=us-west-2
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME=emr-on-eks-test-${AWS_ACCOUNT_ID}-${AWS_REGION}
export EKS_CLUSTER_NAME=eks-test

# Install IRSA + operator + cluster
./starrocks/deploy-starrocks.sh install-iam
```

This creates:
- IAM policy `StarRocksS3GluePolicy` (S3 + Glue read access)
- IRSA service account `starrocks/starrocks-sa`
- StarRocks operator + FE/BE cluster via Helm

### 3.2 Connect to StarRocks

```bash
# Get FE load balancer endpoint
./deploy-starrocks.sh connection

# Or manually
STARROCKS_FE=$(kubectl get svc kube-starrocks-fe-service -n starrocks \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')

mysql -h $STARROCKS_FE -P 9030 -u root
```

### 3.3 Configure Catalogs

```sql
-- Create Paimon catalog
SOURCE sql-scripts/starrocks/01-starrocks-paimon-catalog.sql;

-- Create Iceberg catalog
SOURCE sql-scripts/starrocks/02-starrocks-iceberg-catalog.sql;
```

### 3.4 Query Both Formats

```sql
-- Query Paimon tables
SET CATALOG paimon_catalog;
USE flink_paimon_db;
SELECT * FROM customers LIMIT 10;

-- Query Iceberg tables
SET CATALOG iceberg_catalog;
USE flink_iceberg_db;
SELECT * FROM customers LIMIT 10;

-- Cross-catalog comparison
SELECT
    'paimon' as source, COUNT(*) as cnt
FROM paimon_catalog.flink_paimon_db.customers
UNION ALL
SELECT
    'iceberg' as source, COUNT(*) as cnt
FROM iceberg_catalog.flink_iceberg_db.customers;
```

### 3.5 Monitor StarRocks

```bash
./deploy-starrocks.sh monitor
kubectl get pods -n starrocks
```

**Cost:** ~$300–500/month (3 FE + 3 BE pods)

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| StarRocks SA Helm conflict | `kubectl annotate sa starrocks -n starrocks meta.helm.sh/release-name=starrocks --overwrite` |
| FE label query returns empty | Use `app.starrocks.io/component=fe` (not `app.kubernetes.io/component=fe`) |
| FE "not alive yet" on first boot | Normal — FE tries to find peers, times out ~30s, then bootstraps as leader |
| Athena `PaimonStorageHandler` error | Use Hadoop catalog in Athena Spark (see section 2.1) |
| Athena can't read Paimon's exported Iceberg metadata | Set `metadata.iceberg.format-version = '2'` in Paimon sink tables — Athena engine v3 reads Iceberg V2 only |

---

## Performance Comparison

Results below are from the 40k/s upsert run of **2026-09-22** (`bench-results/starrocks-audit-20260922T160312Z.csv`).
Every timing is the median of 3 server-side `Time=` values from the FE audit log, taken
**while the 40k/s load was still running** — so these are concurrent-ingest read latencies,
not quiet-table latencies.

### Workload

| Parameter | Value |
|-----------|-------|
| Write rate | **~39,960 rows/s** sustained (8 pods × 4,997/s measured avg) |
| Operation mix | 80% `INSERT … ON DUPLICATE KEY UPDATE` on existing PKs, 20% new inserts |
| Key skew | 60% of upserts into a 50,000-key hot window, 40% across the full key range |
| Table mix | order_items 45%, orders 30%, customers 15%, products 10% |
| Duration before benchmark | ~17 min sustained (≈41M row-modifications) |
| Generator | `mysql-data-generator/mysql-upsert-loadgen.yaml`, 8 replicas × 4 threads × 500-row batches |
| Both pipelines | `RUNNING`, 34/34 checkpoints completed, 0 failed, parallelism 16 |

### Table state at benchmark time

| Table | Paimon rows | Iceberg rows | Iceberg data files | pos-delete files | eq-delete files | delete records |
|-------|------------:|-------------:|-------------------:|-----------------:|----------------:|---------------:|
| customers | 3,978,888 | 3,994,246 | 544 | 521 | 544 | 10,090,377 |
| products | 4,084,032 | 4,095,398 | 306 | 288 | 306 | 8,366,034 |
| orders | 9,509,789 | 9,370,063 | 69 | 65 | 69 | 23,045,340 |
| order_items | 20,706,066 | 21,012,997 | 73 | 66 | 72 | 41,244,366 |

Iceberg carries roughly **one delete file per data file** and, on the large tables, *more
delete records than live rows*. That single fact drives every number below: StarRocks must
read the deletes to answer any query, so its scan volume is the row count plus the delete
volume, while Paimon's LSM has already merged them away.

### Snapshot queries (full-table, historical)

| Query | Paimon ms | Iceberg ms | Ratio | Paimon ScanRows | Iceberg ScanRows |
|-------|----------:|-----------:|------:|----------------:|-----------------:|
| Q1_count | 212 | 457 | **2.16x** | 5,299,513 | 13,289,772 |
| Q2_group_by | 246 | 555 | **2.26x** | 5,299,513 | 13,289,772 |
| Q3_join | 666 | 2,804 | **4.21x** | 16,815,938 | 45,746,838 |
| Q4_product_sales | 1,230 | 3,992 | **3.25x** | 29,016,478 | 63,787,030 |
| Q5_time_window | 347 | 2,323 | **6.69x** | 11,516,425 | 32,457,066 |
| Q6_multi_join | 1,616 | 5,094 | **3.15x** | 45,832,416 | 109,533,868 |

### Hot-data queries (recent upserts, `UTC_TIMESTAMP()` windows)

| Query | Paimon ms | Iceberg ms | Ratio | Paimon ScanRows | Iceberg ScanRows |
|-------|----------:|-----------:|------:|----------------:|-----------------:|
| Q7_freshness | 218 | 1,955 | **8.97x** | 2,752,437 | 16,249,483 |
| Q8_hot_pk_lookup | 480 | 2,880 | **6.00x** | 12,042,127 | 48,762,824 |
| Q9_recent_window | 235 | 2,378 | **10.12x** | 4,829,282 | 19,358,652 |
| Q10_hot_join | 693 | 4,422 | **6.38x** | 29,583,354 | 72,966,002 |

### What the numbers say

- **Paimon wins every query, and wins hot data far more decisively than snapshot data.**
  Snapshot ratios sit at 2.2–6.7x; hot-window ratios at 6.0–10.1x. The hot path is exactly
  where the upserts are landing, so it carries the freshest and densest delete files.
- **The cause is scan volume, not CPU.** Iceberg's ScanRows exceeds Paimon's by 2.5–4.0x on
  the same logical result. Q9 is the clearest case: 4.8M rows scanned vs 19.4M for the same
  answer — a 4.0x scan difference producing a 10.1x time difference (super-linear because
  merge-on-read must also *apply* the deletes, not just read them).
- **Ratios track delete density per table, which is the mechanism working as predicted.**
  Q7 (freshness, single-table) at 9.0x sits on tables whose delete records outnumber live
  rows ~2:1. Q1/Q2 at ~2.2x are the cheapest because a `COUNT`/`GROUP BY` can lean on
  metadata more than a join can.
- **Paimon pays for this on the write side, in storage and checkpoints.** Paimon's warehouse
  is 2.94 GB / 6,708 objects against Iceberg's 1.86 GB / 3,564 — background compaction
  rewrites data, so it costs ~1.6x the storage and ~1.9x the object count. Avg checkpoint
  duration was 11,745 ms for Paimon vs 981 ms for Iceberg (**12x**): Paimon's writers do
  merge work inside the checkpoint that Iceberg defers to read time. That is the trade being
  measured — Paimon moves cost from query time to write time, Iceberg does the reverse.

### Reproducing

```bash
# Flink-side: records written, throughput, checkpoint success/duration
./monitoring/capture-flink-metrics.sh

# StarRocks-side: server-side Time/ScanRows/ScanBytes per query
./sql-scripts/starrocks/bench-server-side.sh            # snapshot + hot suites
SUITE=hot RUNS=5 ./sql-scripts/starrocks/bench-server-side.sh

# 40k/s upsert load (8 pods x 5000/s):
kubectl apply -f mysql-data-generator/mysql-upsert-loadgen.yaml
```

Note the StarRocks catalogs must exist and point at the *current* warehouses — this cluster
still carries stale `paimon_catalogv3` / `icebergv3_catalog` entries from an earlier run,
and the first benchmark attempt returned all-zero timings purely because `paimon_catalog`
did not exist yet. All-zero `ScanRows` means the queries errored, not that they were fast.

### Measurement notes — read before trusting any number

- **Use `bench-server-side.sh`, not `bench-hot-data.sh`.** Timings must come from the
  StarRocks FE audit log (`/opt/starrocks/fe/log/fe.audit.log` — the `Time=`, `ScanRows=`
  and `ScanBytes=` fields), never from a stopwatch around a client call. A
  `kubectl exec ... mysql -e` wrapper costs ~5.5 s on this cluster, which swamped every
  query: a bare `SELECT 1;` measured 5,665 ms while a `COUNT(*)` measured 6,079 ms, and
  the audit log recorded that same `COUNT(*)` as `Time=684`. About 89% of each reading was
  harness, so the resulting ratios compared process-spawn overhead rather than StarRocks.
  `bench-hot-data.sh` still times client-side; its absolute numbers are not quotable.
- **`ScanRows` is what makes a timing interpretable.** A query that looks fast may simply
  have matched nothing. Always report scan volume next to elapsed time.
- **Hot-window predicates must use `UTC_TIMESTAMP()`, never `NOW()`.** The FE session
  timezone is +08:00 while CDC writes UTC, so `NOW()` silently matches zero rows and a
  broken query reads as a fast one.
- **Check pipeline health before believing a result.** A Flink job reports `RUNNING` with
  dead sink committers — `/jobs` looks fine while the table is frozen. Confirm freshness
  (`MAX(updated_at)` vs `UTC_TIMESTAMP()`) and checkpoint counts on both sides first;
  comparing a live table against a stalled one measures nothing.
- **Compare like with like.** Paimon compacts continuously in the background; Iceberg
  needs an explicit `rewrite_data_files`. Capture Iceberg both before and after
  compaction, and say which state each number came from.

---

## Cost Comparison (Monthly, 1TB data, 1000 queries/day)

| Component | Athena | StarRocks |
|-----------|--------|-----------|
| **Compute** | $1,500 (queries) | $350 (EC2 instances) |
| **Storage** | $23 (S3) | $23 (S3) + $50 (EBS) |
| **Metadata** | $1 (Glue) | Included |
| **Infrastructure** | $0 | $100 (management) |
| **Total** | **~$1,524** | **~$523** |

- **Athena**: Better for sporadic queries (<100/day)
- **StarRocks**: Better for frequent queries (>500/day)
- **Break-even**: ~300 queries/day

---

## Use Case Matrix

| Use Case | Recommendation |
|----------|---------------|
| **Ad-hoc Analysis** | Athena |
| **Real-time Dashboards** | StarRocks + Paimon |
| **Historical Analysis** | StarRocks + Iceberg |
| **BI Integration** | Athena (QuickSight native) |
| **Data Science** | StarRocks (faster iteration) |
| **Cost Optimization** | Athena (sporadic) or StarRocks (frequent) |
| **Operational Analytics** | StarRocks + Paimon |
| **Compliance/Audit** | Iceberg (time travel) |

---

## Architecture Decision Tree

```
Start: Need OLAP Query Layer
│
├─ Query Frequency?
│  ├─ < 100 queries/day → Athena
│  └─ > 500 queries/day → StarRocks
│
├─ Latency Requirements?
│  ├─ Sub-second needed → StarRocks
│  └─ Seconds acceptable → Athena or StarRocks
│
├─ Infrastructure Preference?
│  ├─ Serverless → Athena
│  └─ Self-managed OK → StarRocks
│
├─ Lakehouse Format?
│  ├─ Paimon → StarRocks (best support)
│  ├─ Iceberg → Athena or StarRocks (both good)
│  └─ Both → StarRocks (query both)
│
└─ BI Tool Integration?
   ├─ QuickSight → Athena (native)
   ├─ Tableau/Superset → StarRocks (JDBC)
   └─ Custom → Either
```

---

**Status**: ✅ Production Ready
**Last Updated**: 2026-03-08
**Technologies**: Flink 1.20, Paimon 1.3.0, Iceberg 1.10.0-amzn-1, StarRocks 4.1.4
