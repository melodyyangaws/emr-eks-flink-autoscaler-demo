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
export EKS_CLUSTER_NAME=loadtest-mcp   # only needed for the IRSA step below
```

### Deploy Monitor

```bash
cd cdc-pipeline
bash ./monitoring/deploy-monitor.sh                       # the v3 set (default): flink_*_db_v3
CDC_DB_SUFFIX= bash ./monitoring/deploy-monitor.sh        # the v2 set: flink_paimon_db, flink_iceberg_db
```

`CDC_DB_SUFFIX` picks the table set the monitor reads (`PAIMON_GLUE_DB` /
`ICEBERG_GLUE_DB` = `flink_paimon_db<suffix>` / `flink_iceberg_db<suffix>`,
rendered into the ConfigMap with envsubst). Unset, it is the value in
`cdc-pipeline/.bench-env` (`_v3` on this branch, the Iceberg v3 set), else
`_v3`; set but empty, the v2 set. Match it to the Flink jobs being monitored.

There is **no image to build**. The monitor runs stock `python:3.11-slim`: the
script pushes `flink_cdc_monitor.py`, `entrypoint.py`, and `requirements.txt`
into the `flink-cdc-monitor-code` ConfigMap (~41 KB, well under the 1 MB limit),
and the pod `pip install`s the requirements at boot. That trades ~2 min of cold
pod startup for dropping the whole build-and-push path — no Dockerfile, no ECR
repository, and no `ecr:*` push grant on the execution role.

To pick up a code change, re-run the same script: it refreshes the ConfigMap and
restarts the Deployment.

### View Logs

```bash
kubectl logs -f deployment/flink-cdc-monitor -n emr-flink
```

### Stop Monitoring

```bash
# Pause (keeps the Deployment and ConfigMap, so restarting is instant)
kubectl scale deployment/flink-cdc-monitor -n emr-flink --replicas=0

# Resume
kubectl scale deployment/flink-cdc-monitor -n emr-flink --replicas=1

# Or remove entirely
kubectl delete -f monitoring/monitor-deployment-deployed.yaml
```

Prefer scaling to zero: the pod `pip install`s its requirements at boot, so a full redeploy
costs ~2 min of cold start while a scale-up is near-instant.

The monitor is **independent** of the pipelines and the load generator — stopping it does not
affect them, and `./build-deploy-generic.sh cleanup` does not stop it. The related commands:

```bash
# Flink CDC jobs (prompts for confirmation)
./build-deploy-generic.sh cleanup [paimon|iceberg]

# Load generator — `mixed` (Deployment) or `upsert` (StatefulSet)
./mysql-data-generator/deploy-data-generator.sh stop upsert
```

> `monitor-deployment-deployed.yaml` is the rendered output of the template and is
> gitignored, so `kubectl delete -f` only works on a machine that has run
> `deploy-monitor.sh`. Elsewhere use
> `kubectl delete deployment/flink-cdc-monitor -n emr-flink`.

---

## What the Monitor Tracks

### Per Pipeline (Paimon & Iceberg)

| Metric | Source | Description |
|--------|--------|-------------|
| **Job Status** | Flink REST `/jobs` | RUNNING, FAILING, RESTARTING, etc. |
| **Parallelism** | Flink REST `/jobs/:id` | Current task parallelism |
| **Throughput (rows/s)** | The tables' commit history | Rows each commit added / time between commits, per table and summed (`commits`). Falls back to the sources' `numRecordsOutPerSecond` (`rate meter`), then the source `numRecordsOut` delta (`counter`) when no commit landed in the window |
| **Records Processed (counter)** | Flink REST `/vertices/:vid/subtasks/metrics` | Operator-scoped: the sources' `numRecordsOut` and the writers' `numRecordsIn`, restart-aware |
| **Committed bytes** | Iceberg: `committedDataFilesByteCount`; Paimon: APPEND commits' delta manifests | Parquet bytes the window committed; compaction rewrites excluded |
| **Checkpoint Status** | Flink REST `/jobs/:id/checkpoints` | Latest checkpoint size, duration, alignment |
| **Checkpoint failures** | Flink REST `/jobs/:id/checkpoints` → `counts.failed` | Reported as the **delta over the window**; the raw counter is cumulative — see the caveat below |
| **Table Row Count** | Iceberg snapshot summary / Paimon snapshot | Total records per sink table |
| **Table File Count** | Iceberg snapshot summary / Paimon manifests | Live data files per table (Paimon compaction nets out) |
| **Table Size** | Iceberg snapshot summary (`total-files-size`) | Total bytes on S3 |
| **Snapshot Count** | Iceberg metadata / Paimon `snapshot/` | Number of snapshots retained |

The monitor is a port of `throughput-benchmark/monitoring/flink_cid_monitor.py`,
extended to the four CDC tables. Throughput figures from the previous version
(before 2026-11-01, including `bench-results/JOB-MONITOR-2026092*.md`) are **not
comparable**: it summed `numRecordsOut` over every vertex, and with operator
chaining off that counts each row once per hop — 6× on the Paimon graph (Source,
DropUpdateBefore, Calc, ConstraintEnforcer, Map, bucket assigner), 3× on Iceberg
(Source, Calc, ConstraintEnforcer).

### Checkpoint failure counts are cumulative, not current

`monitoring/capture-flink-metrics.sh` reads `CP_FAIL` from
`counts.failed` in `/jobs/<jobid>/checkpoints`, and reports it two ways:

```
Checkpoints failed     <delta over the sampling window>
Lifetime ckpt failed    <the raw cumulative counter>
```

`counts.failed` **never decreases for the life of a job**. A job that failed
checkpoints during its snapshot phase and has been checkpointing cleanly ever
since still reports the same non-zero `counts.failed` forever. A byte-identical
`cp_fail` across two captures therefore tells you nothing about current health —
it is the *expected* reading for a healthy job.

The monitor's "Failed Checkpoints (Δ)" row is the change in that counter over
its window, so a stale lifetime value no longer shows there. Only the `history`
array in the same response is authoritative for "is it checkpointing right now",
and neither `capture-flink-metrics.sh` nor the monitor reads it, so check it by
hand:

```bash
JM=$(kubectl get pods -n emr-flink -o name | grep flink-cdc-paimon | grep -v taskmanager | head -1)
JID=$(kubectl exec -n emr-flink "$JM" -c flink-main-container -- \
        curl -s localhost:8081/jobs | python3 -c 'import sys,json; print(json.load(sys.stdin)["jobs"][0]["id"])')
kubectl exec -n emr-flink "$JM" -c flink-main-container -- \
  curl -s "localhost:8081/jobs/$JID/checkpoints" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); [print(c["id"], c["status"], c["trigger_timestamp"]) for c in d["history"]]'
```

Recent `COMPLETED` entries with advancing ids and timestamps mean the job is
healthy regardless of what `counts.failed` says. Use the **delta** row
("Checkpoints failed") from `capture-flink-metrics.sh`, never the lifetime row.

> A related trap: checkpoints that are *aborted at trigger time* (because not
> every task is running) are **not** counted as FAILED, so neither
> `counts.failed` nor `tolerable-failed-checkpoints` reacts to them. That was the
> failure mode the manifests' `failover-strategy: full` setting exists to prevent.

### Table Stats: Iceberg vs Paimon

| Table Type | How Stats Are Read |
|------------|-------------------|
| **Iceberg** | `pyiceberg` loads Glue catalog → reads snapshot summary from S3 metadata. On an Iceberg v3 table the summaries also carry deletion-vector counts per commit (`added-dvs`, `removed-dvs`; Iceberg 1.12's SnapshotSummary has no running DV total, and `total-delete-files` / `added-delete-files` count the DVs too): the report sums them over the commits new in the window and shows a "Deletion Vectors (added / removed)" row and `total_new_dvs` / `total_removed_dvs` (per table `dvs_added_in_window`, ...) in the JSON. A v2 table has no such keys, and its report is unchanged |
| **Paimon** | Its own metadata on S3: `snapshot/LATEST` → `snapshot-N` (`totalRecordCount`, commit kind and time), the manifest lists for live files, and APPEND commits' delta manifests for added bytes. Not the Glue parameters, which a Flink-written table never fills |

---

## Monitor Architecture

### File Structure

```
monitoring/
├── deploy-monitor.sh           ← Code ConfigMap + envsubst + kubectl apply (no image build)
├── flink_cdc_monitor.py        ← FlinkCDCMonitor class
│   ├── Flink REST API client   → job status, throughput, checkpoints
│   ├── pyiceberg table reader  → Iceberg snapshot stats and commit history
│   └── Paimon metadata reader  → snapshots + Avro manifests on S3 (fastavro)
├── entrypoint.py               ← Main loop: build monitors → run → sleep → repeat
├── monitor-deployment.yaml     ← Template: Deployment (python:3.11-slim) + ConfigMap
├── monitor-deployment-deployed.yaml ← Rendered output of the template (gitignored)
└── requirements.txt            ← boto3, pyiceberg[glue,s3fs] 0.12.0 (v3 metadata + DVs; was 0.9.1), pyarrow 21.0.0, requests, tabulate, fastavro, zstandard
```

### Key Configuration (ConfigMap)

| Env Var | Default | Description |
|---------|---------|-------------|
| `PAIMON_FLINK_URL` | `http://flink-cdc-paimon-rest:8081` | Paimon Flink REST endpoint |
| `ICEBERG_FLINK_URL` | `http://flink-cdc-iceberg-rest:8081` | Iceberg Flink REST endpoint |
| `PAIMON_GLUE_DB` | `flink_paimon_db${CDC_DB_SUFFIX}` | Paimon Glue database name (`flink_paimon_db_v3` by default on this branch) |
| `ICEBERG_GLUE_DB` | `flink_iceberg_db${CDC_DB_SUFFIX}` | Iceberg Glue database name (`flink_iceberg_db_v3` by default on this branch) |
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
export EKS_CLUSTER_NAME=loadtest-mcp   # the cluster this pipeline runs on
OIDC_ID=$(aws eks describe-cluster --name $EKS_CLUSTER_NAME --region $AWS_REGION \
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

Job: flink-cdc-paimon [3812d4307feb3a01f59c4b5fe5346a56] parallelism=32
Job: flink-cdc-iceberg [a1b2c3d4e5f67890abcdef1234567890] parallelism=32

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
**Last Updated**: 2026-09-27
