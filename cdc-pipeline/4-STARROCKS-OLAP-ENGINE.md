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
aws glue get-database --name flink_icebergv3_db --region $AWS_REGION
aws glue get-tables --database-name flink_icebergv3_db --region $AWS_REGION
```

#### 1.2 Query Iceberg Tables (Athena SQL)

Open Athena Console → Data source: `AwsDataCatalog` → Database: `flink_icebergv3_db`

```sql
-- Get totla rows
SELECT
    (SELECT COUNT(*) FROM flink_icebergv3_db.customers) as customers,
    (SELECT COUNT(*) FROM flink_icebergv3_db.products) as products,
    (SELECT COUNT(*) FROM flink_icebergv3_db.orders) as orders,
    (SELECT COUNT(*) FROM flink_icebergv3_db.order_items) as order_items;

-- Daily revenue
SELECT
    DATE(order_date) as order_day,
    COUNT(*) as order_count,
    SUM(total_amount) as daily_revenue
FROM flink_icebergv3_db.orders
WHERE order_date >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY DATE(order_date)
ORDER BY order_day DESC;

-- Customer analytics
SELECT
    c.customer_name, c.city, c.state,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent
FROM flink_icebergv3_db.customers c
LEFT JOIN flink_icebergv3_db.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name, c.city, c.state
ORDER BY total_spent DESC
LIMIT 20;

-- Product sales
SELECT
    p.product_name, p.category,
    SUM(oi.quantity) as units_sold,
    SUM(oi.subtotal) as revenue
FROM flink_icebergv3_db.products p
JOIN flink_icebergv3_db.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 20;
```

#### 1.3 Iceberg Time Travel

```sql
-- Query as of specific timestamp
SELECT * FROM flink_icebergv3_db.customers
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-02-20 00:00:00';

-- Query specific snapshot
SELECT * FROM flink_icebergv3_db.customers
FOR SYSTEM_VERSION AS OF 1234567890;

-- View snapshot history
SELECT * FROM flink_icebergv3_db."customers$snapshots";

-- View table manifests
SELECT * FROM flink_icebergv3_db."customers$manifests";
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
spark.sql("SELECT * FROM paimon_iceberg.flink_paimonv3_db.customers LIMIT 10").show()

spark.sql("""
    SELECT order_status, COUNT(*) as cnt, SUM(total_amount) as revenue
    FROM paimon_iceberg.flink_paimonv3_db.orders
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
SET CATALOG paimon_catalogv3;
USE flink_paimonv3_db;
SELECT * FROM customers LIMIT 10;

-- Query Iceberg tables
SET CATALOG icebergv3_catalog;
USE flink_icebergv3_db;
SELECT * FROM customers LIMIT 10;

-- Cross-catalog comparison
SELECT
    'paimon' as source, COUNT(*) as cnt
FROM paimon_catalogv3.flink_paimonv3_db.customers
UNION ALL
SELECT
    'iceberg' as source, COUNT(*) as cnt
FROM icebergv3_catalog.flink_icebergv3_db.customers;
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
| Athena can't read Iceberg V3 | Set `metadata.iceberg.format-version = '2'` in Paimon sink tables |

---

## Performance Comparison

> **Measured 2026-09-21 on `loadtest-mcp` EKS (us-west-2), StarRocks 4.1.4 (1 FE, 3 BE).**
> Both CDC jobs were started from a clean slate at 20:28:01–20:28:09 UTC (S3 warehouses
> emptied, Glue databases dropped) so the snapshot phases overlapped. Load: 5 data
> generator replicas against MySQL 8.0, `BATCH_SIZE=220`, `SLEEP_SECONDS=2`.
> Scripts: `sql-scripts/starrocks/bench-server-side.sh`, `monitoring/capture-flink-metrics.sh`.
>
> ### How these timings are measured — read this before trusting any number here
>
> Query times come from the **StarRocks FE audit log** (`/opt/starrocks/fe/log/fe.audit.log`,
> the `Time=` / `ScanRows=` / `ScanBytes=` fields), not from a stopwatch around a client call.
>
> That distinction is not pedantic — it is the difference between a valid and an invalid
> benchmark. The first version of this harness timed each query client-side around
> `kubectl exec ... mysql -e`. Every query came back at ~5,500 ms whether it was a
> `COUNT(*)` or a four-table join, which is the tell that something is wrong. A control
> proved it: a bare `SELECT 1;` measured **5,665 ms** while the `COUNT(*)` measured
> **6,079 ms**, and the audit log recorded that same `COUNT(*)` as **Time=684**. Roughly
> 5.6 s of every reading was `kubectl exec` plus mysql client startup — about 89% of the
> measurement was harness. Ratios computed from those numbers compared process-spawn
> overhead, not StarRocks.
>
> If you re-run this, use `bench-server-side.sh`. `bench-hot-data.sh` still times
> client-side and its absolute numbers should not be quoted.

### ⚠️ The Iceberg pipeline was NOT healthy during this run

Every Iceberg number below is measured against a table that **stopped receiving data
58 minutes before the benchmark ran**. This is not a caveat to note and move past; it
invalidates any straight reading of "Iceberg was faster."

Root cause, from the JobManager log:

```
java.lang.IllegalArgumentException: Must use DVs for position deletes in V3:
  s3://.../icebergv3-warehouse/flink_icebergv3_db.db/orders/data/order_dt=2026-09-21/00010-0-...parquet
  at org.apache.iceberg.MergingSnapshotProducer.validateNewDeleteFile(MergingSnapshotProducer.java:292)
  at org.apache.iceberg.flink.sink.IcebergFilesCommitter.commitDeltaTxn(IcebergFilesCommitter.java:363)
  at org.apache.iceberg.flink.sink.IcebergFilesCommitter.notifyCheckpointComplete(...)
```

The tables are created with `'format-version' = '3'`, and format-version 3 **requires**
deletion vectors (Puffin) for position deletes. The bundled `iceberg-flink-runtime.jar`
sink still emits legacy positional-delete Parquet files, so Iceberg's own validator
rejects the commit. Setting `'write.delete.vector.enabled' = 'true'` in the sink DDL
(it is already set — see `sql-scripts/icebergv3/03-iceberg-sinks.sql`) does not change
this: the writer path ignores it.

Consequences, all confirmed:

| Symptom | Evidence |
|---|---|
| Commit fails on every checkpoint | 59 of 62 checkpoints failed; last success was checkpoint #5 |
| Table frozen at last good commit | Newest `orders.updated_at` = 20:38:30 UTC, i.e. **3,468 s stale** vs Paimon's **26 s** |
| Job still reports `RUNNING` | 2 of 4 `IcebergFilesCommitter` tasks are `FAILED`; the other operators keep running, so `/jobs` shows RUNNING and the failure is invisible from status alone |
| Written data is orphaned | Writers keep producing Parquet to S3 that no snapshot references — `order_items/data/` holds 6 objects while `order_items$files` reports 3 |

So Iceberg's apparent query speed is mostly **a smaller, frozen, already-compacted
dataset**: 434,643 rows vs Paimon's 793,366 at the same instant. Nothing is landing to
slow it down.

### Snapshot queries (Q1–Q6) — server-side `Time`, median of 3 runs

`ScanRows` is included because it is what makes the timings interpretable.

| Query | Paimon ms | Iceberg ms | Paimon ScanRows | Iceberg ScanRows | ms per M rows scanned (P → I) |
|-------|----------:|-----------:|----------------:|-----------------:|---|
| **Q1** COUNT(*) | 187 | 35 | 399,732 | 241,464 | 468 → 145 |
| **Q2** GROUP BY state | 209 | 44 | 399,732 | 241,464 | 523 → 182 |
| **Q3** 2-table JOIN | 336 | 114 | 1,244,185 | 676,198 | 270 → 169 |
| **Q4** product sales JOIN | 668 | 150 | 2,704,458 | 1,551,294 | 247 → 97 |
| **Q5** 7-day time window | 203 | 84 | 844,453 | 434,734 | 240 → 193 |
| **Q6** 4-table JOIN | 920 | 283 | 3,948,643 | 2,227,478 | 233 → 127 |

Iceberg reads ~58% of the rows but takes ~20–40% of the time, so it is genuinely faster
**per row scanned** as well — roughly 1.3–2.5×. That part is a real Iceberg advantage and
is what you would expect from a static, fully-compacted Parquet layout with rich
min/max statistics. It is also exactly the state a frozen table is in.

### Hot-data queries (Q7–Q10) — the real-time test

All predicates use `UTC_TIMESTAMP()`, never `NOW()`. The FE session timezone is +08:00
while CDC writes UTC, so `NOW()` silently matches zero rows and a broken query looks
like a fast one.

| Query | Paimon ms | Iceberg ms | Paimon ScanRows | Iceberg ScanRows | Verdict |
|-------|----------:|-----------:|----------------:|-----------------:|---|
| **Q7** freshness (5-min window) | 351 | 52 | 147,837 | **61** | Iceberg window is empty |
| **Q8** hot PK lookup (MAX order_id) | 579 | 69 | 851,544 | 434,851 | Iceberg returns a stale row |
| **Q9** 10-min status rollup | 235 | 55 | 398,232 | **61** | Iceberg window is empty |
| **Q10** 10-min join to order_items | 562 | 111 | 2,663,463 | **10,927** | Iceberg window is near-empty |

**This is the headline result of the whole exercise.** On Q7/Q9/Q10 Iceberg scans ~61 rows
against Paimon's 148K–2.7M. It is not answering the question faster; it has no recent data
to answer it with. A dashboard on the Paimon table shows activity from 26 seconds ago;
the same dashboard on Iceberg shows nothing in the last 10 minutes and gives no error.

Paimon's cost on these queries is real work: its LSM tree is continuously absorbing and
compacting fresh writes, and the hot window genuinely contains hundreds of thousands of
rows. 351 ms to answer "what happened in the last 5 minutes" over a live 793K-row table
is the actual real-time number.

### Table state at benchmark time

| Table | Paimon rows | Iceberg rows | Iceberg data files | Iceberg delete files | Iceberg snapshots |
|-------|------------:|-------------:|-------------------:|---------------------:|------------------:|
| customers | 392,307 | 240,972 | 43 | 43 | 3 |
| products | 388,826 | 232,561 | 18 | 18 | 2 |
| orders | 748,849 | 434,643 | 2 | 2 | 2 |
| order_items | 2,235,533 | 1,307,630 | 3 | 3 | 3 |

Note the **1:1 data-file-to-delete-file ratio** — every Iceberg data file has a matching
positional-delete file. That 1:1 ratio is the merge-on-read tax this test set out to
measure, and it is also precisely what tripped the V3 deletion-vector validator.

Storage footprint: Paimon 6,270 objects / 252 MB; Iceberg 209 objects / 41 MB. Iceberg's
figure is small mainly because it holds 58% of the rows and stopped writing an hour ago.

### Verdict

| Dimension | Winner | Basis |
|---|---|---|
| **Hot / real-time queries** | 🟢 **Paimon** | 26 s freshness vs 3,468 s; Iceberg returned empty or stale results on 3 of 4 hot queries |
| **Static scan efficiency** | 🟢 **Iceberg** | 1.3–2.5× faster per row scanned on Q1–Q6 — genuine, but measured on a frozen compacted table |
| **CDC write reliability (V3)** | 🟢 **Paimon** | 61/61 checkpoints vs 3/62; Iceberg V3 + Flink sink is incompatible on this release |
| **Failure visibility** | 🟢 **Paimon** | Iceberg reported `RUNNING` with a dead committer and a frozen table for 58 minutes |

**Not yet measured:** the post-`rewrite_data_files` comparison. Compacting Iceberg is
pointless while it cannot commit — fix the deletion-vector problem first, let both
formats ingest the same live data, then re-run `SUITE=hot` for a fair compacted-vs-hot row.

### Reproducing / fixing before you re-benchmark

The Iceberg side needs one of these before its numbers mean anything:

1. **Drop to `format-version = 2`** in `sql-scripts/icebergv3/03-iceberg-sinks.sql`. V2
   accepts positional-delete files, which is what the sink actually writes. This trades
   the V3 feature set for a pipeline that commits — and it still exercises merge-on-read
   with accumulating delete files, which is the comparison this test wants.
2. **Upgrade `iceberg-flink-runtime.jar`** to a build whose sink writes Puffin deletion
   vectors, and keep V3.

Option 1 is the smaller change and keeps the delete-file read-amplification behaviour
under test. Option 2 is the right long-term fix.

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
**Technologies**: Flink 1.20, Paimon 1.3.0, Iceberg 1.10.0-amzn-0, StarRocks 4.1.4
