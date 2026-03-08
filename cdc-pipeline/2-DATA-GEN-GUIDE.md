# MySQL Data Generator Guide

## Overview

The **MySQL Data Generator** is a Kubernetes-deployed Python application that continuously generates realistic e-commerce transactional data to test your CDC pipelines with **streaming incremental data**.

It simulates a real production workload with:
- ✅ **INSERT** operations (new customers, products, orders)
- ✅ **UPDATE** operations (order status changes, stock updates)
- ✅ **DELETE** operations (cleanup of cancelled orders)

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

# Check recent activity
SELECT
    DATE_FORMAT(created_at, '%Y-%m-%d %H:%i') as minute,
    COUNT(*) as new_customers
FROM customers
WHERE created_at >= NOW() - INTERVAL 1 HOUR
GROUP BY DATE_FORMAT(created_at, '%Y-%m-%d %H:%i')
ORDER BY minute DESC
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

```bash
# Deploy
./mysql-data-generator/deploy-data-generator.sh deploy

# Stop
./mysql-data-generator/deploy-data-generator.shstop

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

---

## Summary

✅ **Deployed as**: Kubernetes Deployment in `emr-flink` namespace
✅ **Purpose**: Generate continuous streaming data for CDC testing
✅ **Operations**: INSERT, UPDATE, DELETE on 4 tables
✅ **Scalable**: 1-10 pods for different load profiles
✅ **Configurable**: Batch size and interval adjustable
✅ **Realistic**: E-commerce transactional patterns
✅ **Monitored**: Live statistics every 5 iterations

Use this to test and validate your Flink CDC pipelines with realistic streaming workloads! 🚀
