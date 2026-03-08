# Flink CDC Pipeline Monitoring Guide

## Overview

A custom monitoring pod that continuously tracks both **Paimon** and **Iceberg** CDC pipelines, comparing throughput, checkpoint health, and table growth side-by-side.

```
┌─────────────────────────────────────────────────────────────┐
│              Flink CDC Monitor Pod (emr-flink)              │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  entrypoint.py — Continuous monitoring loop            │  │
│  │  flink_cdc_monitor.py — Flink REST API + table stats  │  │
│  │                                                       │  │
│  │  Data Sources:                                        │  │
│  │  • Flink REST API → job status, throughput, checkpts  │  │
│  │  • pyiceberg       → Iceberg table snapshots          │  │
│  │  • boto3 Glue API  → Paimon table stats (fallback)   │  │
│  └───────────────────────────────────────────────────────┘  │
│  Output: /tmp/reports/*.json + structured logs              │
└─────────────────────────────────────────────────────────────┘
         │                              │
         ↓                              ↓
┌──────────────────┐          ┌──────────────────┐
│  Paimon Flink    │          │  Iceberg Flink   │
│  REST API :8081  │          │  REST API :8081  │
└──────────────────┘          └──────────────────┘
```

---

## Quick Start

### Set Environment Variables

```bash
export AWS_REGION=us-west-2
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME=emr-on-eks-test-${AWS_ACCOUNT_ID}-${AWS_REGION}
export NAMESPACE=emr-flink
```

### Deploy Monitor (Build + Deploy)

```bash
cd cdc-pipeline
bash ./monitoring/deploy-monitor.sh
```

This builds the Docker image (multi-arch: amd64+arm64), pushes to ECR, and deploys the K8s Deployment.

### Deploy Monitor (Image Already Built)

```bash
kubectl apply -f monitoring/monitor-deployment.yaml
```

### View Logs

```bash
kubectl logs -f deployment/flink-cdc-monitor -n emr-flink
```

### Stop Monitoring

```bash
kubectl delete -f monitoring/monitor-deployment.yaml
```

---

## What the Monitor Tracks

### Per Pipeline (Paimon & Iceberg)

| Metric | Source | Description |
|--------|--------|-------------|
| **Job Status** | Flink REST `/jobs` | RUNNING, FAILING, RESTARTING, etc. |
| **Parallelism** | Flink REST `/jobs/:id` | Current task parallelism |
| **Throughput** | Flink REST `/jobs/:id/vertices` | Records in/out per operator |
| **Checkpoint Status** | Flink REST `/jobs/:id/checkpoints` | Latest checkpoint size, duration, alignment |
| **Table Row Count** | pyiceberg / Glue API | Total records per sink table |
| **Table File Count** | pyiceberg / Glue API | Data files per table |
| **Table Size** | pyiceberg / Glue API | Total bytes on S3 |
| **Snapshot Count** | pyiceberg / Glue API | Number of snapshots retained |

### Table Stats: Iceberg vs Paimon

| Table Type | How Stats Are Read |
|------------|-------------------|
| **Iceberg** | `pyiceberg` loads Glue catalog → reads snapshot summary from S3 metadata |
| **Paimon** | pyiceberg fails (`table_type = PAIMON`) → falls back to `boto3 glue.get_table()` → reads `numRows`, `numFiles`, `totalSize` from Glue parameters |

---

## Monitor Architecture

### File Structure

```
monitoring/
├── Dockerfile.monitor          ← Multi-stage Docker image (Python 3.11)
├── deploy-monitor.sh           ← Build (docker buildx) + push to ECR + kubectl apply
├── flink_cdc_monitor.py        ← FlinkCDCMonitor class
│   ├── Flink REST API client   → job status, throughput, checkpoints
│   ├── pyiceberg table reader  → Iceberg snapshot stats
│   └── Glue API fallback       → Paimon table stats via boto3
├── entrypoint.py               ← Main loop: build monitors → run → sleep → repeat
├── monitor-deployment.yaml     ← K8s Deployment + ConfigMap (env vars)
└── requirements.txt            ← boto3, pyiceberg[glue,s3fs], pyarrow, requests, tabulate
```

### Key Configuration (ConfigMap)

| Env Var | Default | Description |
|---------|---------|-------------|
| `PAIMON_FLINK_URL` | `http://flink-cdc-paimon-rest:8081` | Paimon Flink REST endpoint |
| `ICEBERG_FLINK_URL` | `http://flink-cdc-iceberg-rest:8081` | Iceberg Flink REST endpoint |
| `PAIMON_GLUE_DB` | `flink_paimon_db` | Paimon Glue database name |
| `ICEBERG_GLUE_DB` | `flink_iceberg_db` | Iceberg Glue database name |
| `PAIMON_WAREHOUSE` | `s3://<BUCKET>/paimon-warehouse/` | Paimon S3 warehouse path |
| `ICEBERG_WAREHOUSE` | `s3://<BUCKET>/iceberg-warehouse/` | Iceberg S3 warehouse path |
| `CDC_TABLES` | `customers,products,orders,order_items` | Tables to monitor |
| `MONITOR_INTERVAL` | `60` | Seconds between monitoring cycles |
| `REPORT_DIR` | `/tmp/reports` | JSON report output directory |

---

## IRSA Setup (Required)

The monitor pod needs S3 + Glue read access. It uses the `default` service account annotated with the EMR execution role.

### 1. Get OIDC Issuer

```bash
OIDC_ID=$(aws eks describe-cluster --name eks-test --region $AWS_REGION \
  --query "cluster.identity.oidc.issuer" --output text | cut -d'/' -f5)
```

### 2. Update IAM Trust Policy
The mornitoring pod will use default ServiceAccount in the namesapce emr-flink:

```bash
aws iam update-assume-role-policy \
  --role-name emr-on-eks-test-execution-role \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::${AWS_ACCOUNT_ID}:oidc-provider/oidc.eks.${AWS_REGION}.amazonaws.com/id/'$OIDC_ID'"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringLike": {
          "oidc.eks.${AWS_REGION}.amazonaws.com/id/'$OIDC_ID':sub": "system:serviceaccount:${NAMESPACE}:default"
        }
      }
    }]
  }'
```

### 3. Annotate Service Account

```bash
kubectl annotate serviceaccount default -n emr-flink \
  eks.amazonaws.com/role-arn=arn:aws:iam::${AWS_ACCOUNT_ID}:role/emr-on-eks-test-execution-role \
  --overwrite
```

### 4. Restart Monitor

```bash
kubectl rollout restart deployment/flink-cdc-monitor -n emr-flink
```

---

## Sample Output

```
════════════════════════════════════════════════════════════
 Flink CDC Monitor — Paimon vs Iceberg Comparison
════════════════════════════════════════════════════════════

Job: flink-cdc-paimon [3812d4307feb3a01f59c4b5fe5346a56] parallelism=4
Job: flink-cdc-iceberg [a1b2c3d4e5f67890abcdef1234567890] parallelism=4

┌──────────────┬─────────┬──────────┬───────────┬──────────┐
│ Table        │ Format  │ Records  │ Files     │ Size     │
├──────────────┼─────────┼──────────┼───────────┼──────────┤
│ customers    │ paimon  │ 12,345   │ 8         │ 2.4 MB   │
│ customers    │ iceberg │ 12,340   │ 12        │ 3.1 MB   │
│ products     │ paimon  │ 8,234    │ 6         │ 1.8 MB   │
│ products     │ iceberg │ 8,230    │ 10        │ 2.5 MB   │
│ orders       │ paimon  │ 45,678   │ 24        │ 12.3 MB  │
│ orders       │ iceberg │ 45,670   │ 32        │ 15.1 MB  │
│ order_items  │ paimon  │ 123,456  │ 48        │ 28.7 MB  │
│ order_items  │ iceberg │ 123,450  │ 64        │ 34.2 MB  │
└──────────────┴─────────┴──────────┴───────────┴──────────┘

Report saved to /tmp/reports/comparison_20260308_081200.json
```

---

## Monitoring Flink Directly (Without Monitor Pod)

```bash
# Check FlinkDeployment status
kubectl get flinkdeployment -n emr-flink

# View Flink job logs
kubectl logs -f -l app=flink-cdc-paimon -n emr-flink -c flink-main-container
kubectl logs -f -l app=flink-cdc-iceberg -n emr-flink -c flink-main-container

# Access Flink Web UI
kubectl port-forward svc/flink-cdc-paimon-rest 8081:8081 -n emr-flink
# Open: http://localhost:8081

# Check S3 data freshness
aws s3 ls s3://${BUCKET_NAME}/paimon-warehouse/ --recursive | tail -10
aws s3 ls s3://${BUCKET_NAME}/iceberg-warehouse/ --recursive | tail -10
```

---

**Status**: ✅ Production Ready
**Last Updated**: 2026-03-08
