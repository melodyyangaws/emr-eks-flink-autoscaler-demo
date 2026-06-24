# Realtime Lakehouse via Flink CDC (FlinkSQL Approch)

## What We Built

A complete **real-time Change Data Capture (CDC)** pipeline with dual lakehouse formats and multiple OLAP query engines:

- **Source**: MySQL RDS with binlog-based CDC
- **Processing**: Apache Flink on EMR on EKS 7.12
- **Storage**: Apache Paimon + Apache Iceberg on S3 (AWS Glue Catalog)
- **OLAP**: AWS Athena (serverless) + StarRocks on EKS (high-performance)
- **Monitoring**: Custom Python monitor app comparing Paimon vs Iceberg CDC pipelines

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     MySQL RDS (OLTP Source)                         │
│  E-Commerce DB: customers, products, orders, order_items           │
│  Binlog: ROW format, CDC user with REPLICATION SLAVE               │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ Binlog Streaming (Full Snapshot + Incremental)
                           ↓
┌─────────────────────────────────────────────────────────────────────┐
│              Generic Flink CDC SQL Executor (PyFlink)               │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  • Loads modular SQL scripts from S3                        │   │
│  │  • Substitutes env vars (${MYSQL_HOST}, ${GLUE_DATABASE})   │   │
│  │  • Executes: catalog → sources → sinks → pipelines          │   │
│  └───────────────────┬─────────────────────┬───────────────────┘   │
│                      │                     │                       │
│            ┌─────────┴────────┐  ┌─────────┴────────┐             │
│            │  Paimon on S3    │  │  Iceberg on S3   │             │
│            │  Hive Metastore  │  │  AWS Glue        │             │
│            │  (via Glue)      │  │  Catalog         │             │
│            └─────────┬────────┘  └─────────┬────────┘             │
└──────────────────────┼─────────────────────┼──────────────────────┘
                       │                     │
              ┌────────┴─────────────────────┴────────┐
              │         OLAP Query Layer               │
              │  ┌──────────────┐  ┌───────────────┐  │
              │  │ AWS Athena   │  │ StarRocks     │  │
              │  │ (Serverless) │  │ (on EKS)      │  │
              │  └──────────────┘  └───────────────┘  │
              └────────────────────────────────────────┘
```

## File Structure

```
cdc-pipeline/
├── 0-MYSQL-SETUP-GUIDE.md              ← Step 0: MySQL RDS setup
├── 1-BUILD-DEPLOY-FLINK-APP-GUIDE.md   ← Step 1: Build & deploy Flink CDC pipelines
├── 2-DATA-GEN-GUIDE.md                 ← Step 2: Data generation for source DB with adjustable rate
├── 3-MONITOR.md                        ← Step 3: Monitoring setup guide
├── 4-STARROCKS-OLAP-ENGINE.md          ← Step 4: OLAP layer (Athena + StarRocks)
│
├── docker/
│   └── Dockerfile.cdc-paimon           ← Flink image (CDC + Paimon + Iceberg)
│
├── flink-cdc-executor.py               ← Generic SQL executor (PyFlink) runing FlinkSQL as jobs
├── build-deploy-generic.sh             ← Build image, upload SQL, deploy jobs
│
├── sql-scripts/
│   ├── common/
│   │   └── 02-cdc-sources.sql          ← MySQL CDC source table definitions
│   ├── paimonv3/
│   │   ├── 01-catalog-setup.sql        ← Paimon catalog (Hive/Glue metastore)
│   │   ├── 03-paimon-sinks.sql         ← Paimon tables (DV + Iceberg compat)
│   │   └── 04-cdc-pipelines.sql        ← INSERT INTO streaming pipelines
│   ├── icebergv3/
│   │   ├── 01-catalog-setup.sql        ← Iceberg catalog (Glue)
│   │   ├── 03-iceberg-sinks.sql        ← Iceberg V3 tables (merge-on-read)
│   │   └── 04-cdc-pipelines.sql        ← INSERT INTO streaming pipelines
│   └── starrocks/
│       ├── 01-starrocks-paimon-catalog.sql
│       └── 02-starrocks-iceberg-catalog.sql
│
├── flink-cdc-paimon-sql.yaml           ← FlinkDeployment manifest (Paimon)
├── flink-cdc-iceberg-sql.yaml          ← FlinkDeployment manifest (Iceberg)
│
├── mysql-data-generator/
│   ├── deploy-data-generator.sh        ← Deploy/scale/configure data gen
│   └── mysql-data-generator.yaml       ← ConfigMap + Deployment (Python)
│
├── monitoring/
│   ├── Dockerfile.monitor              ← Monitor image (pyiceberg + boto3)
│   ├── deploy-monitor.sh               ← Build & deploy monitor
│   ├── flink_cdc_monitor.py            ← Flink REST API + table stats
│   ├── entrypoint.py                   ← Continuous monitoring loop
│   ├── monitor-deployment.yaml         ← K8s Deployment + ConfigMap
│   └── requirements.txt
│
├── deploy-starrocks.sh                 ← StarRocks on EKS deployment
├── helm/
│   └── starrocks-eks-values.yaml
│
├── PAIMON-VS-ICEBERG.md                ← Lakehouse format comparison
├── QUICKSTART.md                       ← Quick reference
└── QUICK-REFERENCE.md
```

---

## Deployment Steps

### Step 0 — MySQL RDS Setup
> Guide: [0-MYSQL-SETUP-GUIDE.md](0-MYSQL-SETUP-GUIDE.md)

1. Create RDS MySQL 8.0 instance in the EKS VPC
2. Verify binlog is enabled (`ROW` format, auto-enabled on RDS)
3. Create CDC user with `REPLICATION SLAVE` + `REPLICATION CLIENT`
4. Create `ecommerce` database with 4 tables:
   - `customers` — dimension table, email unique constraint
   - `products` — dimension table, category-based
   - `orders` — fact table, FK to customers
   - `order_items` — fact table, `subtotal` as generated column
5. Load initial sample data
6. Test connectivity from EKS (`kubectl run mysql-test ...`)
7. Export environment variables → `mysql-cdc-env.sh`

```bash
source mysql-cdc-env.sh
echo $MYSQL_HOST  # verify
```

### Step 1 — Build & Deploy Flink CDC
> Guide: [1-BUILD-DEPLOY-FLINK-APP-GUIDE.md](1-BUILD-DEPLOY-FLINK-APP-GUIDE.md)

1. Install EMR on EKS Flink Operator (if needed)
2. Set environment variables
3. Build Docker image + upload SQL scripts to S3
4. Deploy FlinkDeployments (Paimon, Iceberg, or both)

```bash
source mysql-cdc-env.sh
export AWS_REGION=us-west-2
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME=emr-on-eks-test-${AWS_ACCOUNT_ID}-${AWS_REGION}
export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::${AWS_ACCOUNT_ID}:role/emr-on-eks-test-execution-role
export LAKEHOUSE_FORMAT=both  # paimon | iceberg | both

# upload flink sql-scripts to s3
aws s3 sync sql-scripts/ s3://emr-on-eks-test-021732063925-us-west-2/flink/sql-scripts/

# build and push docker image to ECR
./build-deploy-generic.sh build

# deploy Flink CDC pipelines: flink-cdc-iceberg-sql.yaml & flink-cdc-paimon-sql.yaml
./build-deploy-generic.sh deploy
# or deploy individual CDC pipelines
# above deployment need to run first to produced deployed yaml iles)
kubectl apply -f flink-cdc-iceberg-deployed.yaml
kubectl apply -f flink-cdc-paimon-deployed.yaml

# stop two flnk CDC pipelines
./build-deploy-generic.sh cleanup
# or individual CDC pipeline
kubectl delete -f flink-cdc-iceberg-deployed.yaml
kubectl delete -f flink-cdc-paimon-deployed.yaml

```

**Flink Job Configuration:**
| Setting | Value |
|---------|-------|
| EMR Version | 7.12.0 |
| Flink Version | 1.20 |
| Job Manager | 2 replicas (HA), 2 GB memory |
| Task Manager | 4 GB memory, 4 task slots |
| Parallelism | 4 |
| Checkpointing | 60s, EXACTLY_ONCE |
| State Backend | Hashmap + S3 checkpoints |

### Step 2 — Deploy Data Generator
> Guide: [2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md)

Deploys a K8s pod that continuously generates INSERT/UPDATE/DELETE operations against MySQL.

```bash
source mysql-cdc-env.sh
# produce source data at adjustable rate (insert/update/delete)
./mysql-data-generator/deploy-data-generator.sh deploy
./mysql-data-generator/deploy-data-generator.sh logs
# stop
./mysql-data-generator/deploy-data-generator.sh stop
```

**Operations per iteration (every 5 seconds) - default frequency:**
| Operation | Volume |
|-----------|--------|
| INSERT customers | 1–10 |
| INSERT products | 1–10 |
| INSERT orders + order_items | 2–20 |
| UPDATE order statuses | 1–10 |
| UPDATE product stock | 1–10 |
| DELETE old cancelled orders | Every 10th iteration |

**Scaling:** `./mysql-data-generator/deploy-data-generator.sh scale 3`

**Rate tuning:** `./mysql-data-generator/deploy-data-generator.sh config 20 2`

### Step 3 — Monitoring
> Guide: [3-MONITOR.md](3-MONITOR.md)

Deploy a custom monitor pod in EKS that continuously tracks both Paimon and Iceberg CDC pipelines, comparing throughput, checkpoints, and table growth.

```bash
# Build image + deploy pod ( 10 mins)
bash ./monitoring/deploy-monitor.sh

# Or deploy without rebuilding docker image ( in seconds)
kubectl apply -f monitoring/monitor-deployment-deployed.yaml

# Stop the monitoring
kubectl delete -f monitoring/monitor-deployment-deployed.yaml

# View logs
kubectl logs -f deployment/flink-cdc-monitor -n emr-flink
```

The monitor reads table stats via:
- **Iceberg tables**: `pyiceberg` (snapshot metadata from S3)
- **Paimon tables**: boto3 Glue API fallback (since `table_type = PAIMON`)

> **Note:** The monitor pod requires IRSA. See [3-MONITOR.md](3-MONITOR.md) for trust policy setup.

### Step 4 — OLAP Query Layer (Athena + StarRocks)
> Guide: [4-STARROCKS-OLAP-ENGINE.md](4-STARROCKS-OLAP-ENGINE.md)

**Athena (Icebergv2)** — query via Athena Console directly:
```sql
SELECT * FROM flink_icebergv3_db.customers LIMIT 10;
```

**Athena (Paimon)** — query via Athena Notebook :
```json
# custom spark properties
{
    "spark.sql.catalog.paimon_iceberg": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.paimon_iceberg.type": "hadoop",
    "spark.sql.catalog.paimon_iceberg.warehouse": "s3://$PAIMON_WAREHOUSE/iceberg"
}
```

```sql
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.sql("SELECT * FROM paimon_iceberg.flink_paimonv3_db.customers LIMIT 10").show()
```

**StarRocks on EKS** — native support for both Paimon and Iceberg:
```bash
./deploy-starrocks.sh install-iam
./deploy-starrocks.sh connection
mysql -h <STARROCKS_FE_LB> -P 9030 -u root
```

> See [4-STARROCKS-OLAP-ENGINE.md](4-STARROCKS-OLAP-ENGINE.md) for full catalog setup, query examples, performance/cost comparison, and decision tree.

---

## CDC Behavior

### Phase 1: Full Snapshot (Initial Load)
- Chunked reads by primary key ranges (parallel, no table locks)
- Consistent point-in-time snapshot
- Written as INSERT operations to Paimon/Iceberg

### Phase 2: Incremental CDC (Continuous)
- Streams INSERT/UPDATE/DELETE from MySQL binlog
- Exactly-once via Flink checkpointing
- UPSERT semantics with primary keys
- Sub-second latency (binlog → lakehouse)

---

## Key Design Decisions

### Paimon Tables
- **Deletion vectors** enabled — bitmap-based deletes reduce write amplification
- **Iceberg compatibility** (`metadata.iceberg.format-version = '2'`) — enables Athena/Spark queries
- **Input changelog producer** — downstream consumers see full CDC stream
- **4/8 buckets** — balances parallelism vs small file count

### Iceberg Tables
- **Format V3** with deletion vectors — efficient merge-on-read for CDC
- **UPSERT enabled** (`write.upsert.enabled = true`)
- **Explicit partition columns** — Flink SQL doesn't support hidden transforms
- **Glue catalog** — native Athena integration

### MySQL Schema Notes
- `order_items.subtotal` is a **generated column** (`quantity * unit_price`) — do not INSERT into it directly
- `order_date`, `created_at`, `updated_at` have `DEFAULT CURRENT_TIMESTAMP` — CDC sources read them automatically
- `email` has a UNIQUE constraint — data generator uses UUID-based emails to avoid collisions

---

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Initial Load Speed | ~10K rows/sec (depends on RDS instance) |
| CDC Latency | < 1 second (binlog → lakehouse) |
| Throughput | ~3K events/sec per TaskManager |
| Checkpoint Interval | 60 seconds |
| Checkpoint Duration | < 15 seconds (typical) |
| Storage Format | Parquet (zstd compression) |

---

## Troubleshooting Quick Reference

| Issue | Solution |
|-------|----------|
| FlinkDeployment won't start | Check IAM role, secrets, image pull, ECR auth |
| CDC not capturing changes | Verify binlog enabled, CDC user has REPLICATION SLAVE |
| Duplicate email crash | Data generator uses UUID-based emails |
| Calculated column error (subtotal) | Don't INSERT into `order_items.subtotal` |
| Data generator connects but no data | Check pod logs for per-operation errors; ensure ConfigMap is redeployed |
| Monitor `PaimonStorageHandler` error | Monitor uses Glue API fallback for Paimon tables |
| Athena Spark can't read Paimon tables | Use Hadoop catalog in Athena Spark; set `metadata.iceberg.format-version = '2'` |
| Athena SQL can't read Paimon tables | Manually change table_type property to `Icberg` in Glue Catalog |
| StarRocks SA conflict | Annotate existing SA with Helm release metadata |
| Docker image platform mismatch | Build with `--platform linux/amd64,linux/arm64` |
| Monitor pod `ImagePullBackOff` | Use `docker buildx build --platform` for multi-arch |
| Monitor pod can't assume role | Update IAM trust policy to allow the SA's OIDC subject |

---

## Resources Created

- ☑️ 1 RDS MySQL instance (binlog-enabled, CDC user configured)
- ☑️ 2 ECR repository (multi-arch Flink CDC & monitor images)
- ☑️ 2 FlinkDeployments (Paimon + Iceberg pipelines)
- ☑️ 1 Data generator Deployment (ConfigMap-based Python script)
- ☑️ 1 Monitor Deployment (Flink REST API + Glue/pyiceberg stats)
- ☑️ 4 Paimon tables on S3 (with Iceberg compatibility metadata)
- ☑️ 4 Iceberg tables on S3 (Glue catalog, format V2)
- ☑️ 1 StarRocks cluster on EKS (FE + BE pods)
- ☑️ S3 checkpoint and HA storage
- ☑️ Kubernetes secrets for MySQL credentials

---

**Status**: ✅ Production Ready
**Last Updated**: 2026-03-11
**EMR Version**: 7.12.0
**Flink Version**: 1.20
**Paimon Version**: 1.3.0
**Iceberg Version**: 1.10.0-amzn-0
**StarRocks Version**: 3.2.x
**MySQL Version**: 8.0.45
