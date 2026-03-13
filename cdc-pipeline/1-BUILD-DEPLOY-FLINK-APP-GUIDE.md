# Generic Flink CDC SQL Executor - User Guide

## Overview

A **flexible, SQL-driven** approach to Flink CDC that eliminates the need to rebuild Docker images when modifying CDC pipelines. Supports both **Apache Paimon** and **Apache Iceberg** lakehouses.

## Key Features

✅ **SQL-Based Configuration** - Define CDC pipelines in SQL files
✅ **No Image Rebuilds** - Modify pipelines by updating SQL scripts on S3
✅ **Environment Variable Substitution** - Dynamic configuration with `${VAR_NAME}`
✅ **Dual Lakehouse Support** - Deploy to Paimon, Iceberg, or both
✅ **Modular SQL Scripts** - Separate files for catalog setup, sources, sinks, pipelines
✅ **Load from S3 or Local** - Execute SQL from S3 buckets or local directories

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    MySQL RDS (Source)                       │
│                  with binlog enabled                        │
└────────────────────────┬────────────────────────────────────┘
                         │ CDC Streaming
                         ↓
┌─────────────────────────────────────────────────────────────┐
│          Generic Flink CDC Executor (PyFlink)               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  1. Load SQL scripts from S3/local                   │   │
│  │  2. Substitute environment variables (${MYSQL_HOST}) │   │
│  │  3. Parse SQL into statements                        │   │
│  │  4. Execute in sequence                              │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────┬──────────────────┬───────────────────────┘
                   │                  │
        ┌──────────┴─────────┐   ┌──┴────────────────┐
        │  Paimon on S3      │   │  Iceberg on S3    │
        │  (File Catalog)    │   │  (AWS Glue)       │
        └────────┬───────────┘   └───────┬───────────┘
                 │                       │
        ┌────────┴────────┐     ┌────────┴────────┐
        │  Spark Query    │     │  Athena Query   │
        └─────────────────┘     └─────────────────┘
```

---

## File Structure

```
cdc-pipeline/
  ├── docker
    ├── Dockerfile.cdc-paimon        ← Docker image with FlinkCDC(add) + Paimon(add) + Iceberg 
  |
  ├── mysql-data-generator
    ├── deploy-data-generator.sh
    ├── mysql-data-generator.yaml
  |  
  ├── flink-cdc-executor.py            ← Generic SQL executor to produce Paimon or Iceberg tables
  │
  ├── sql-scripts/                     ← Modular SQL scripts
  │   ├── common/
  │   │   └── 02-cdc-sources.sql             ← MySQL CDC sources
  │   ├── paimon/
  │   │   ├── 01-catalog-setup.sql
  │   │   ├── 03-paimon-sinks.sql
  │   │   └── 04-cdc-pipelines.sql
  │   ├── iceberg/
  │   │   ├── 01-catalog-setup.sql
  │   │   ├── 03-iceberg-sinks.sql
  │   │   └── 04-cdc-pipelines.sql
  │   └── starrocks/
  │       ├── 01-starrocks-paimon-catalog.sql
  │       └── 02-starrocks-iceberg-catalog.sql
  │
  ├── flink-cdc-paimon-sql.yaml    ← Kubernetes manifest (Paimon)
  ├── flink-cdc-iceberg-sql.yaml   ← Kubernetes manifest (Iceberg)
  ├── build-deploy-generic.sh      ← Flink CDC image build and deployment, execute flink jobs
  ├── deploy-starrocks.sh          ← StarRocks deployment
```

---

## Quick Start
### 0. Install EMR on EKS Flink Operator if needed
```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.12.0/cert-manager.yaml

export VERSION=7.12.0
export NAMESPACE=emr-flink

helm install flink-kubernetes-operator \
oci://public.ecr.aws/emr-on-eks/flink-kubernetes-operator \
--version $VERSION \
--namespace $NAMESPACE
```

### 1. Set Environment Variables
Now that MySQL is set up, you can proceed to deploy Flink CDC.

```bash
# Source MySQL environment variables
source mysql-cdc-env.sh

# Set AWS environment variables
export AWS_REGION=us-west-2
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME=emr-on-eks-test-${AWS_ACCOUNT_ID}-${AWS_REGION}
export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::${AWS_ACCOUNT_ID}:role/emr-on-eks-test-execution-role

# Lakehouse format (paimon | iceberg | both)
export LAKEHOUSE_FORMAT=both
```

### 2. Build and Deploy
```bash
# build docker images, upload assets to s3 (one-off)
./build-deploy-generic.sh build
# deploy PyFlink(FlinkSQL) jobs to EKS
./build-deploy-generic.sh deploy
```
### 3. Monitor
```bash
./build-deploy-generic.sh monitor
```

```bash
# View all deployments
kubectl get flinkdeployment -n emr-flink

# Monitor pipelines
./build-deploy-generic.sh monitor paimon
./build-deploy-generic.sh monitor iceberg

# View logs
kubectl logs -f -l app=flink-cdc-paimon -n emr-flink -c flink-main-container
kubectl logs -f -l app=flink-cdc-iceberg -n emr-flink -c flink-main-container

# Access Flink UI
kubectl port-forward svc/flink-cdc-paimon-rest 8081:8081 -n emr-flink
# Open: http://localhost:8081
```

---

### Change CDC Configuration

**Update MySQL Connection Timeout**

Edit `sql-scripts/common/02-cdc-sources.sql`:
```sql
'connect.timeout' = '60s',  -- Changed from 30s
```

Upload and restart:
```bash
aws s3 cp sql-scripts/common/02-cdc-sources.sql \
  s3://${BUCKET_NAME}/flink/sql-scripts/common/

# Restart job
kubectl delete flinkdeployment flink-cdc-paimon -n emr-flink
kubectl apply -f flink-cdc-paimon-deployed.yaml -n emr-flink
```

---

## Generic Executor Usage

The `flink-cdc-executor.py` can be used standalone for testing:

### Execute Single SQL File
```bash
python flink-cdc-executor.py --sql-file setup.sql
```

### Execute Multiple SQL Files
```bash
python flink-cdc-executor.py --sql-file setup.sql,pipelines.sql
```

### Execute All SQL Files in Directory
```bash
python flink-cdc-executor.py --sql-dir /opt/flink/sql/
```

### Execute from S3
```bash
python flink-cdc-executor.py \
  --sql-dir s3://my-bucket/flink-sql/ \
  --mode streaming \
  --parallelism 8
```

### Custom Configuration
```bash
python flink-cdc-executor.py \
  --sql-file pipelines.sql \
  --mode batch \
  --checkpoint-interval 30s \
  --parallelism 4
```

---

## Deployment Commands

```bash
# Build Docker image and upload scripts only
./build-deploy-generic.sh build

# Deploy to Kubernetes only (image must exist)
./build-deploy-generic.sh deploy

# Build + Deploy (complete setup)
./build-deploy-generic.sh all

# Monitor deployment
./build-deploy-generic.sh monitor paimon
./build-deploy-generic.sh monitor iceberg

# Clean up
./build-deploy-generic.sh cleanup
```

---

## Troubleshooting

### SQL Parsing Errors

**Issue**: SQL statement not recognized

**Solution**: Ensure statements end with semicolon:
```sql
CREATE CATALOG my_catalog WITH (...);  -- ✅ Good
CREATE CATALOG my_catalog WITH (...)   -- ❌ Bad
```

### Variable Substitution Fails

**Issue**: `Environment variable 'VAR' is not set`

**Solution**: Set the variable or provide a default:
```sql
'hostname' = '${MYSQL_HOST:localhost}',  -- Uses 'localhost' if not set
```

### CDC Job Not Starting

**Check Flink logs:**
```bash
kubectl logs -l app=flink-cdc-paimon -n emr-flink --tail=200
```

**Common issues:**
- MySQL binlog not enabled
- CDC user permissions missing
- S3 access denied (check IAM role)

### SQL Files Not Found

**Issue**: `No such file or directory: s3://...`

**Solution**: Verify S3 path and permissions:
```bash
aws s3 ls s3://${BUCKET_NAME}/flink/sql-scripts/paimon/
```
---

## Next Steps

- **Generate sample data**: Follow the [DATA-GEN-GUIDE](./DATA-GEN-GUIDE.md) to produce more source data in mysql DB based on different rate.

---

**Status**: ✅ Production Ready
**Last Updated**: 2026-02-21
**EMR Version**: 7.12.0
**Flink Version**: 1.20
**Paimon Version**: 1.3.0
**Iceberg Version**: 1.10.0-amzn-0
**mySQL Version**: 8.0.45
