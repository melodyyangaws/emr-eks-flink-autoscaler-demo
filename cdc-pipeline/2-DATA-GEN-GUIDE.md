# MySQL Data Generator Guide

## Overview

The **MySQL Data Generator** is a Kubernetes-deployed Python application that continuously generates realistic e-commerce transactional data to test your CDC pipelines with **streaming incremental data**.

It simulates a real production workload with:
- ✅ **INSERT** operations (new customers, products, orders)
- ✅ **UPDATE** operations (order status changes, stock updates)
- ✅ **DELETE** operations (cleanup of cancelled orders)

---

## Two workloads — pick the right one

`deploy-data-generator.sh` drives **two** generators against the same database. Every
command below takes `mixed` or `upsert` as a trailing argument (or `WORKLOAD=`), and the
default is `mixed`. They are not interchangeable:

| | `mixed` (default) | `upsert` |
|---|---|---|
| Manifest | `mysql-data-generator.yaml` | `mysql-upsert-loadgen.yaml` |
| Kubernetes kind | Deployment | **StatefulSet** |
| Statements | one row per statement | `INSERT … ON DUPLICATE KEY UPDATE`, 500 rows/statement |
| Rate | ~400 rows/s ceiling | ~40,000 rows/s across 8 pods |
| DELETE events | **yes** | no |
| Use it for | CDC correctness, all three event types | the Paimon-vs-Iceberg storage benchmark |

`mixed` does not scale — it issues one statement per row and picks rows with
`ORDER BY RAND()`. Use it to prove the pipeline handles inserts, updates *and* deletes.

`upsert` is the benchmark workload. Repeated upserts on a skewed hot-key window are what
make Iceberg accumulate equality-delete files while Paimon's LSM compacts them away; a
uniformly random key stream over 14M keys would measure raw ingest and show neither effect.

> **The upsert generator is a StatefulSet, not a Deployment.** Each pod owns a *disjoint
> slice* of the hot-key window and derives that slice from its ordinal, and only a
> StatefulSet hands out exact `0..N-1` ordinals (`mysql-upsert-loadgen-0` … `-7`). This
> matters for two commands: `kubectl` calls must say `statefulset`, not `deployment`, and
> if you ever ran the older Deployment version you must `kubectl delete deployment
> mysql-upsert-loadgen -n emr-flink` first — `kubectl apply` cannot convert a Deployment
> into a StatefulSet of the same name.

The rest of this guide covers the `mixed` workload; the
[Upsert Workload](#upsert-workload-benchmark-load) section covers `upsert`.

---

## What It Does

### **Operations Generated**

Every iteration (default: 5 seconds), the generator performs:

| Operation | Description | Frequency |
|-----------|-------------|-----------|
| **INSERT customers** | New customer registrations | 1-10 per batch |
| **INSERT products** | New product catalog entries | 1-10 per batch |
| **INSERT orders** | New orders with line items | 2-20 per batch |
| **UPDATE orders** | Status progression (PENDING→CONFIRMED→SHIPPED→DELIVERED) | 1-10 per batch |
| **UPDATE products** | Stock quantity adjustments | 1-10 per batch |
| **DELETE orders** | Cleanup of old cancelled orders (30+ days) | Every 10 iterations |

### **Realistic Data Generation**

- **Customers**: Random names, emails, phone numbers, addresses
- **Products**: 9 categories, realistic pricing ($9.99 - $999.99)
- **Orders**: Random customer + products, various payment methods
- **Order Status Flow**: PENDING → CONFIRMED → SHIPPED → DELIVERED
- **Statistics**: Tracks total customers, products, orders, revenue

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│          Kubernetes Deployment (emr-flink namespace)        │
│  ┌────────────────────────────────────────────────┐         │
│  │  mysql-data-generator Pod                      │         │
│  │  ┌──────────────────────────────────────────┐  │         │
│  │  │  Python 3.11                             │  │         │
│  │  │  - mysql-connector-python                │  │         │
│  │  │  - generate_data.py script               │  │         │
│  │  │                                          │  │         │
│  │  │  Configuration:                          │  │         │
│  │  │  - BATCH_SIZE: 10                        │  │         │
│  │  │  - SLEEP_SECONDS: 5                      │  │         │
│  │  │  - MYSQL_HOST: from secret               │  │         │
│  │  │  - MYSQL_USER: from secret               │  │         │
│  │  │  - MYSQL_PASSWORD: from secret           │  │         │
│  │  └──────────────────────────────────────────┘  │         │
│  └────────────────────────────────────────────────┘         │
└──────────────────────────┬──────────────────────────────────┘
                           │ Continuous SQL Operations
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                    MySQL RDS (ecommerce DB)                 │
│  Tables: customers, products, orders, order_items           │
│  Binlog: Capturing all INSERT/UPDATE/DELETE                 │
└──────────────────────────┬──────────────────────────────────┘
                           │ CDC Stream
                           ↓
┌─────────────────────────────────────────────────────────────┐
│               Flink CDC Pipeline (Paimon/Iceberg)           │
│  Real-time ingestion of all changes                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### **1. Deploy Data Generator**

```bash
cd cdc-pipeline
source mysql-cdc-env.sh

# Deploy to emr-flink namespace
./mysql-data-generator/deploy-data-generator.sh deploy

# Expected output:
# [10:30:15] Deploying MySQL data generator...
# [10:30:15] ✓ MySQL credentials secret found
# configmap/mysql-data-generator-script created
# deployment.apps/mysql-data-generator created
# [10:30:16] ✓ Data generator deployed successfully
```

### **2. Watch Live Data Generation**

```bash
./mysql-data-generator/deploy-data-generator.sh logs
```

**Sample Output:**
```
============================================================
MySQL Transactional Data Generator
============================================================
Target: my-rds.us-west-2.rds.amazonaws.com/ecommerce
Batch Size: 10
Interval: 5 seconds
============================================================
✓ Connected to MySQL at my-rds.us-west-2.rds.amazonaws.com

[2024-02-23 10:30:20] Iteration #1
  → Inserted 8 customers
  → Inserted 7 products
  → Inserted 15 orders with items
  → Updated 6 order statuses
  → Updated 9 product stock levels
✓ Batch completed. Sleeping for 5s...

[2024-02-23 10:30:25] Iteration #2
  → Inserted 5 customers
  → Inserted 10 products
  → Inserted 12 orders with items
  → Updated 8 order statuses
  → Updated 7 product stock levels
✓ Batch completed. Sleeping for 5s...

[2024-02-23 10:30:35] Iteration #3
  → Inserted 9 customers
  → Inserted 6 products
  → Inserted 18 orders with items
  → Updated 10 order statuses
  → Updated 8 product stock levels

Database Statistics:
   Customers: 1,245
   Products: 823
   Orders: 5,632
   Order Items: 15,891
   Total Revenue: $345,678.90
✓ Batch completed. Sleeping for 5s...
```

### **3. Check Status**

```bash
./mysql-data-generator/deploy-data-generator.sh status
```

**Output:**
```
[10:32:15] Data generator status:

NAME                   READY   UP-TO-DATE   AVAILABLE   AGE
mysql-data-generator   1/1     1            1           2m

NAME                                    READY   STATUS    RESTARTS   AGE
mysql-data-generator-6f9c8d7b5c-x4k2p   1/1     Running   0          2m
```

---

## Advanced Usage

### **Scale for Higher Data Rate**

Generate data faster by running multiple pods:

```bash
# Scale to 3 pods (3x data generation rate)
./mysql-data-generator/deploy-data-generator.sh scale 3

# Check pods
kubectl get pods -l app=mysql-data-generator -n emr-flink
```

### **Configure Generation Rate**

Adjust batch size and interval:

```bash
# Generate 20 records every 2 seconds (faster)
  ./mysql-data-generator/deploy-data-generator.sh config 20 2

# Generate 5 records every 10 seconds (slower)
./mysql-data-generator/deploy-data-generator.sh config 5 10

# Default: 10 records every 5 seconds
./mysql-data-generator/deploy-data-generator.sh config 10 5
```

### **Stop Data Generator**

```bash
./mysql-data-generator/deploy-data-generator.sh stop
```

---

## Upsert Workload (benchmark load)

The high-rate workload used for the Paimon-vs-Iceberg comparison in
[4-STARROCKS-OLAP-ENGINE.md](4-STARROCKS-OLAP-ENGINE.md). Append `upsert` to every command.

### **Deploy**

```bash
cd cdc-pipeline
source mysql-cdc-env.sh

# One-time only, and only if the older Deployment version was ever applied.
# apply cannot convert a Deployment into a StatefulSet of the same name.
kubectl delete deployment mysql-upsert-loadgen -n emr-flink --ignore-not-found

# 8 pods x RATE_PER_POD 5000 = 40,000 rows/s aggregate
./mysql-data-generator/deploy-data-generator.sh deploy upsert
```

Pods come up as `mysql-upsert-loadgen-0` … `-7` all at once
(`podManagementPolicy: Parallel` — the default `OrderedReady` would start them one at a
time, and during that ramp the achieved rate is meaningless).

Each pod logs its ordinal on startup; this is the line to check, because a duplicate
ordinal means two pods are fighting over the same rows:

```
mysql C extension: available
pod=mysql-upsert-loadgen-3 ordinal=3 of 8
```

### **Stop**

```bash
./mysql-data-generator/deploy-data-generator.sh stop upsert
```

This deletes the StatefulSet, its headless Service and the script ConfigMap. To pause
without deleting anything, scale to zero instead:

```bash
kubectl scale statefulset mysql-upsert-loadgen -n emr-flink --replicas=0
```

### **Status and achieved rate**

```bash
./mysql-data-generator/deploy-data-generator.sh status upsert
```

Quote the **`avg=`** figure, never `inst=`. `avg` is cumulative since pod start; `inst` is a
15-second sample and the pods report on staggered clocks, so summing `inst` reads high or
low depending on when you looked. The target rate is only a token-bucket ceiling — a
shortfall shows up as a low `avg`, not as an error.

### **Scale**

```bash
# 16 pods x 5000 = 80,000 rows/s target
./mysql-data-generator/deploy-data-generator.sh scale 16 upsert
```

The script sets `POD_COUNT=16` **before** scaling. `POD_COUNT` is the divisor that sizes
each pod's hot-key slice, so it has to track the replica count: set too low, slices overlap
and the cross-pod lock convoy returns; set too high, part of the window goes unwritten. If
you scale with a bare `kubectl scale`, set `POD_COUNT` yourself.

### **Tune rate and key skew**

```bash
# rate/pod, worker threads, rows per statement
./mysql-data-generator/deploy-data-generator.sh config 5000 4 500 upsert

# upsert ratio, hot-key ratio, hot-key window
./mysql-data-generator/deploy-data-generator.sh upsert-mix 0.9 0.8 20000 upsert
```

`config` changes *how fast*; `upsert-mix` changes *what is being measured*. A tighter
`HOT_KEY_WINDOW` concentrates updates and accumulates Iceberg delete files faster —
widening it weakens the very effect the comparison is looking for.

### **Measured ceiling**

A replica ramp (`mysql-data-generator/find-rds-ceiling.sh`) against `db.m5.2xlarge` /
500 GB gp3 / 12,000 provisioned IOPS found the aggregate rate peaks near **32 replicas at
~69,000 rows/s and then declines**:

| replicas | target | achieved | % of target | RDS CPU | write IOPS |
|---------:|-------:|---------:|------------:|--------:|-----------:|
| 8 | 40,000 | 34,989 | 87.5% | 32% | 7,363 |
| 16 | 80,000 | 58,265 | 72.8% | 47% | 8,013 |
| 24 | 120,000 | 65,914 | 54.9% | 47% | 7,955 |
| **32** | 160,000 | **68,818** | 43.0% | 48% | 7,997 |
| 40 | 200,000 | 66,404 | 33.2% | 46% | 7,481 |

The limit was **InnoDB row-lock contention, not hardware**: at 40 replicas 154 of 163
server connections sat in `LOCK WAIT` with 2.33M cumulative row-lock waits averaging
135 ms, while CPU idled under 50% and write IOPS sat at 8,000 of the 12,000 provisioned.
Connections were never the constraint — 160 against `max_connections=2591`.

Hot-window sharding (`SHARD_HOT_KEYS=true`) was added in response: it gives each pod a
disjoint slice so pods stop colliding, which raised 8-replica throughput from 34,989 to
**39,964 rows/s (99.9% of target)** and pushed RDS CPU up to 54.7% — the bottleneck moving
toward the database, which is the point. Sharding makes each key *hotter*, not colder (with
32 pods a pod cycles ~1,560 keys instead of 50,000), so it strengthens rather than dilutes
the benchmark signal.

Re-run the ramp to re-establish the ceiling under the sharded pattern:

```bash
# hold_seconds, then replica steps
./mysql-data-generator/find-rds-ceiling.sh 300 8 16 24 32 40
# results land in ./bench-results/rds-ceiling-<UTC timestamp>.csv
```

> Pods are **not** CPU-bound: measured 31–70 millicores against a 2-core limit (~3%). They
> block on MySQL round trips. Add replicas rather than raising `THREADS` in one pod.

---

## Verify CDC Pipeline is Working

### **1. Check MySQL Data**

```bash
# Connect to MySQL
mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD ecommerce

# Check row counts
SELECT
    (SELECT COUNT(*) FROM customers) as customers,
    (SELECT COUNT(*) FROM products) as products,
    (SELECT COUNT(*) FROM orders) as orders,
    (SELECT COUNT(*) FROM order_items) as order_items;

# Check recent activity (Athena Spark)
SELECT
    date_format(date_trunc('minute', created_at), 'yyyy-MM-dd HH:mm') AS minute,
    COUNT(*) AS new_customers
FROM paimon_iceberg.flink_paimon_db.customers
WHERE created_at >= current_timestamp() - INTERVAL 8 HOUR
GROUP BY date_trunc('minute', created_at)
ORDER BY minute DESC
LIMIT 10;

# in Athena SQL
SELECT
    date_format(created_at, '%Y-%m-%d %H:%i') AS minute,
    COUNT(*) AS new_customers
FROM flink_iceberg_db.customers
WHERE created_at >= NOW() - INTERVAL '2' HOUR
GROUP BY date_format(created_at, '%Y-%m-%d %H:%i')
ORDER BY 1 DESC
LIMIT 10;
```

### **2. Check Flink CDC Ingestion**

```bash
# Check Flink job status
kubectl get flinkdeployment -n emr-flink

# View Flink CDC logs
kubectl logs -f -l app=flink-cdc-paimon -n emr-flink
# OR
kubectl logs -f -l app=flink-cdc-iceberg -n emr-flink

# Look for messages like:
# "Snapshot phase completed, starting binlog phase..."
# "Processed 1000 records"
```

### **3. Check Lakehouse Data (S3)**

```bash
# Paimon warehouse
aws s3 ls s3://${BUCKET_NAME}/paimon-warehouse/ecommerce.db/ --recursive | tail -20

# Iceberg warehouse
aws s3 ls s3://${BUCKET_NAME}/iceberg-warehouse/ecommerce.db/ --recursive | tail -20

# Check file timestamps - should be recent
aws s3 ls s3://${BUCKET_NAME}/paimon-warehouse/ecommerce.db/customers/bucket-0/ \
  --recursive | grep "\.parquet$" | tail -10
```

### **4. Query from Athena**

```sql
-- Check row count (should match MySQL closely)
SELECT COUNT(*) as total_customers
FROM ecommerce.customers;

-- Check latest ingestion time
SELECT
    MAX(updated_at) as latest_update,
    COUNT(*) as total_rows,
    COUNT(DISTINCT DATE(updated_at)) as days_of_data
FROM ecommerce.customers;

-- Verify recent data
SELECT
    customer_name,
    email,
    city,
    state,
    created_at
FROM ecommerce.customers
ORDER BY created_at DESC
LIMIT 10;
```

---

## Troubleshooting

### **Data Generator Not Starting**

```bash
# Check pod status
kubectl get pods -l app=mysql-data-generator -n emr-flink

# If CrashLoopBackOff, check logs
kubectl logs -l app=mysql-data-generator -n emr-flink --tail=100

# Common issues:
# 1. MySQL credentials secret not found
kubectl get secret mysql-credentials -n emr-flink

# 2. Cannot connect to RDS (security group)
# - Verify RDS security group allows traffic from EKS
# - Check RDS endpoint is correct
```

### **Slow Data Generation**

```bash
# Increase batch size and reduce sleep time
./mysql-data-generator/deploy-data-generator.sh config 50 2

# Scale to multiple pods
./mysql-data-generator/deploy-data-generator.sh scale 5
```

### **Too Much Data Generated**

```bash
# Reduce batch size and increase sleep time
./mysql-data-generator/deploy-data-generator.sh config 5 10

# Scale down to 1 pod
./mysql-data-generator/deploy-data-generator.sh scale 1

# Or stop completely
./mysql-data-generator/deploy-data-generator.sh stop
```

### **Database Connection Errors**

```bash
# Test connection from pod
kubectl run mysql-test --rm -it --restart=Never \
  --image=mysql:8.0 \
  --namespace=emr-flink \
  --command -- mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1;"
```

---

## Performance Metrics

### **Default Configuration (1 pod)**
- **Batch size**: 10 records
- **Interval**: 5 seconds
- **Rate**: ~100-150 operations/minute
- **Daily volume**: ~150K operations/day

### **Scaled Configuration (5 pods)**
- **Batch size**: 20 records
- **Interval**: 2 seconds
- **Rate**: ~1,500-2,000 operations/minute
- **Daily volume**: ~2.5M operations/day

### **Resource Usage**
- **CPU**: 100-250 millicores per pod
- **Memory**: 128-256 MB per pod
- **Network**: < 1 Mbps per pod

---

## Integration with CDC Pipeline

### **Complete Flow**

```
1. Data Generator Pod
   ↓ SQL: INSERT/UPDATE/DELETE
2. MySQL RDS
   ↓ Binlog events
3. Flink CDC Connector
   ↓ Change records
4. Paimon/Iceberg Tables
   ↓ Parquet files + metadata
5. AWS Glue Catalog
   ↓ Table registration
6. Query Engines (Athena/Spark/Presto)
```

### **Monitoring End-to-End Lag**

```bash
# 1. Check latest timestamp in MySQL
mysql -h $MYSQL_HOST -u $MYSQL_USER -p -e \
  "SELECT MAX(created_at) as mysql_latest FROM ecommerce.customers;"

# 2. Check latest timestamp in Athena
# Run in Athena console:
SELECT MAX(created_at) as athena_latest
FROM ecommerce.customers;

# 3. Calculate lag
# Lag = mysql_latest - athena_latest
# Healthy: < 30 seconds
# Warning: 30-60 seconds
# Critical: > 60 seconds
```

---

## Commands Reference

Every command takes `mixed` (default) or `upsert` as a trailing argument. `WORKLOAD=upsert`
works too if you prefer an environment variable.

### mixed workload — CDC correctness, all three event types

```bash
# Deploy
./mysql-data-generator/deploy-data-generator.sh deploy

# Stop
./mysql-data-generator/deploy-data-generator.sh stop

# Status
./mysql-data-generator/deploy-data-generator.sh status

# Live logs
./mysql-data-generator/deploy-data-generator.sh logs

# Scale (1-10 pods)
./mysql-data-generator/deploy-data-generator.sh scale <NUM_PODS>

# Configure rate
./mysql-data-generator/deploy-data-generator.sh config <BATCH_SIZE> <SLEEP_SECONDS>

# Examples:
./mysql-data-generator/deploy-data-generator.sh scale 3           # 3 pods
./mysql-data-generator/deploy-data-generator.sh config 20 2       # 20 records/2s
./mysql-data-generator/deploy-data-generator.sh config 5 10       # 5 records/10s
```

### upsert workload — 40k/s benchmark load (StatefulSet)

```bash
# One-time, only if the older Deployment version was ever applied
kubectl delete deployment mysql-upsert-loadgen -n emr-flink --ignore-not-found

# Deploy — 8 pods x 5000 = 40,000 rows/s
./mysql-data-generator/deploy-data-generator.sh deploy upsert

# Stop (removes StatefulSet + headless Service + ConfigMap)
./mysql-data-generator/deploy-data-generator.sh stop upsert

# Pause without deleting
kubectl scale statefulset mysql-upsert-loadgen -n emr-flink --replicas=0

# Status + achieved rate per pod (quote avg=, not inst=)
./mysql-data-generator/deploy-data-generator.sh status upsert

# Scale — also sets POD_COUNT so hot-key slices stay disjoint
./mysql-data-generator/deploy-data-generator.sh scale 16 upsert

# Rate: rows/s per pod, threads, rows per statement
./mysql-data-generator/deploy-data-generator.sh config 5000 4 500 upsert

# Key skew: upsert ratio, hot-key ratio, hot-key window
./mysql-data-generator/deploy-data-generator.sh upsert-mix 0.9 0.8 20000 upsert

# Find the RDS ceiling: hold_seconds, then replica steps
./mysql-data-generator/find-rds-ceiling.sh 300 8 16 24 32 40
```

---

## Summary

✅ **Two workloads**: `mixed` (Deployment, ~400 rows/s, emits DELETEs) and `upsert`
   (StatefulSet, ~40,000 rows/s, no DELETEs)
✅ **Deployed in**: `emr-flink` namespace
✅ **Purpose**: `mixed` for CDC correctness, `upsert` for the storage-format benchmark
✅ **Operations**: INSERT, UPDATE, DELETE on 4 tables (`mixed`);
   batched `ON DUPLICATE KEY UPDATE` (`upsert`)
✅ **Scalable**: 1-10 pods (`mixed`); 8-40 pods with per-pod hot-key slices (`upsert`)
✅ **Configurable**: batch size and interval (`mixed`); rate, threads, key skew (`upsert`)
✅ **Realistic**: E-commerce transactional patterns
✅ **Measured**: ~69,000 rows/s RDS ceiling on `db.m5.2xlarge`, lock-contention-bound

Use this to test and validate your Flink CDC pipelines with realistic streaming workloads! 🚀
