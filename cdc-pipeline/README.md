# Realtime Lakehouse via Flink CDC (FlinkSQL Approch)

## Quick Start — what to ask your AI tool

- Clone the source code, go to the project directory, start your GenAI assistant. 
- Just talk to the GenAI assistant at your terminal or IDE in plain language. See the following examples:

- **Create a test on an existing EKS cluster:**
  > "follow the Deployment Steps to create the test, use my existing EKS cluster called demo"

- **Start flink jobs to produce Paimon and Iceberg data to S3**
  > "Start both jobs for Paimon and Iceberg at the same time"
  > 
- **Start data generator to create new CDC dataset in RDS as source**
  > "Start a large scale data gen to test UPSERT performance and report the achieved rate"
  >
- **Capture benchmark result**
  > "Start to monitor the flink jobs to capture test result"
  >
- **Test StarRocks queries and capture the result**
  > "Query hostdata and snapshot data using Starrocks, collect the read performance result for Paimon and Iceberg formats and compare"
  >
  
## What We Built

A complete **real-time Change Data Capture (CDC)** pipeline with dual lakehouse formats and multiple OLAP query engines:

- **Source**: MySQL RDS with binlog-based CDC
- **Processing**: Apache Flink on EMR on EKS 7.13
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
│  │  • Script itself mounted from ConfigMap flink-cdc-executor-py│  │
│  │    at /opt/flink/usrlib (NOT downloaded from S3 — see below) │   │
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
├── 4-STARROCKS-OLAP-ENGINE.md          ← Step 4: OLAP layer (Athena + StarRocks) + benchmark results
│
├── docker/
│   └── Dockerfile.cdc-paimon           ← Flink image (CDC + Paimon + Iceberg), multi-arch
│
├── flink-cdc-executor.py               ← Generic SQL executor (PyFlink) running FlinkSQL as jobs
├── build-deploy-generic.sh             ← Build image (Kaniko), upload SQL, deploy jobs
│
├── sql-scripts/
│   ├── common/
│   │   └── 02-cdc-sources.sql          ← MySQL CDC source table definitions
│   ├── paimon/
│   │   ├── 01-catalog-setup.sql        ← Paimon catalog (Hive/Glue metastore)
│   │   ├── 03-paimon-sinks.sql         ← Paimon tables (DV + Iceberg compat)
│   │   └── 04-cdc-pipelines.sql        ← INSERT INTO streaming pipelines
│   ├── iceberg/
│   │   ├── 01-catalog-setup.sql        ← Iceberg catalog (Glue)
│   │   ├── 03-iceberg-sinks.sql        ← Iceberg V2 tables (merge-on-read)
│   │   └── 04-cdc-pipelines.sql        ← INSERT INTO streaming pipelines
│   └── starrocks/
│       ├── 01-starrocks-paimon-catalog.sql
│       ├── 02-starrocks-iceberg-catalog.sql
│       ├── 03-hot-data-queries.sql     ← Hot-window query suite (UTC_TIMESTAMP)
│       ├── bench-server-side.sh        ← Benchmark harness — USE THIS ONE
│       └── bench-hot-data.sh           ← Client-side timing; absolute numbers NOT quotable
│
├── flink-cdc-paimon-sql.yaml           ← FlinkDeployment manifest (Paimon), ${VAR} template
├── flink-cdc-iceberg-sql.yaml          ← FlinkDeployment manifest (Iceberg), ${VAR} template
│
├── mysql-data-generator/
│   ├── deploy-data-generator.sh        ← Deploy/scale/configure either workload
│   ├── mysql-data-generator.yaml       ← "mixed": INSERT/UPDATE/DELETE, ~400 rows/s
│   └── mysql-upsert-loadgen.yaml       ← "upsert": StatefulSet, 8 pods, batched
│                                         upserts; rate is measured, not set
│
├── monitoring/
│   ├── deploy-monitor.sh               ← Deploy monitor (no image build)
│   ├── flink_cdc_monitor.py            ← Flink REST API + table stats
│   ├── entrypoint.py                   ← Continuous monitoring loop
│   ├── monitor-deployment.yaml         ← K8s Deployment (python:3.11-slim) + ConfigMap
│   ├── capture-flink-metrics.sh        ← Point-in-time Flink metrics → CSV
│   └── requirements.txt                ← pip-installed at pod boot
│
├── k8s/
│   ├── ebs-sc-storageclass.yaml        ← gp3 StorageClass named "ebs-sc" (EMR requires
│   │                                     that exact name for Flink local-recovery)
│   └── kyverno-flink-az-affinity.yaml  ← Pin each Flink job's pods to one AZ
│
├── starrocks/
│   └── deploy-starrocks.sh             ← StarRocks on EKS deployment
├── helm/
│   └── starrocks-eks-values.yaml       ← kube-starrocks chart values (>= 1.11)
│
├── glue-catalog-test.py                ← Standalone Glue catalog connectivity check
├── glue-catalog-test.yaml              ← Pod manifest for the above
│
├── bench-results/                      ← Benchmark CSVs (committed; metrics only)
│   └── starrocks-audit-20260922T160312Z.csv
│
├── PAIMON-VS-ICEBERG.md                ← Format comparison + benchmark results
└── README.md                           ← This file
```

**Not in git** (`.gitignore`) — generated locally, and they contain resolved account
IDs and bucket names:

```
mysql-cdc-env.sh                        ← MYSQL_HOST / MYSQL_USER / MYSQL_PASSWORD
*-deployed.yaml                         ← rendered manifests with ${VAR} substituted:
                                          flink-cdc-{paimon,iceberg}-deployed.yaml,
                                          monitoring/monitor-deployment-deployed.yaml
```

Edit the `*-sql.yaml` / `monitor-deployment.yaml` templates, never the `*-deployed.yaml`
renderings — `build-deploy-generic.sh` overwrites the latter on every run.

### How rendering works

`build-deploy-generic.sh` renders each `flink-cdc-<format>-sql.yaml` template into
`flink-cdc-<format>-deployed.yaml` with a plain `sed` pass. It substitutes exactly
these placeholders:

| Placeholder | Source / default |
|---|---|
| `${AWS_REGION}` | `$AWS_REGION`, defaulted to `us-west-2` by the script |
| `${AWS_ACCOUNT_ID}` | `$AWS_ACCOUNT_ID` (required) |
| `${BUCKET_NAME}` | `$BUCKET_NAME` (required) |
| `${EMR_EXECUTION_ROLE_ARN}` | `$EMR_EXECUTION_ROLE_ARN` (required) |
| `${EMR_VERSION}` | `7.13.0` |
| `${IMAGE_TAG}` | `$IMAGE_TAG`, else `$EMR_VERSION`, else `7.13.0` |
| `${GLUE_DATABASE}` | `flink_iceberg_db` |
| `${MYSQL_HOST}` / `${MYSQL_USER}` | `$MYSQL_HOST` / `$MYSQL_USER` (required) |
| `${MYSQL_ENV_SECRET_NAME}` | `flink-cdc-mysql-env` |
| `${JM_NODEPOOL}` | `driver-nodepool` |
| `${TM_NODEPOOL}` | `executor-memorynodepool` |

`MYSQL_PASSWORD` is **deliberately not substituted.** The manifests pull it in at
pod start with `envFrom: secretRef: <MYSQL_ENV_SECRET_NAME>`, so no credential is
ever written into a file — not even the gitignored rendering. (`envFrom` rather
than an `env[].valueFrom.secretKeyRef` because the EMR Flink operator's
pod-template env merge emits an empty `value: ""` next to `valueFrom`, which the
Kubernetes API rejects.)

After rendering, the script fails loudly on either of two conditions rather than
letting a silent misconfiguration reach the cluster:

- any surviving `${UPPER_CASE}` placeholder → *"Unsubstituted placeholders remain"*
- any `value: ""` line → *"Empty env value(s) rendered"*

The second guard exists because an empty `AWS_REGION` renders as a blank value
that Kubernetes accepts happily, and the job then fails minutes later with an
error naming a heartbeat timeout rather than the missing variable.

> One asymmetry worth knowing: `flink-cdc-iceberg-sql.yaml` templates
> `fs.s3a.endpoint.region: ${AWS_REGION}`, while `flink-cdc-paimon-sql.yaml`
> **hardcodes** `aws.region: us-west-2` and `fs.s3a.endpoint.region: us-west-2`.
> Deploying the Paimon job outside `us-west-2` requires editing that template.

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
3. Create the MySQL password Secret and the executor-script ConfigMap
4. Build Docker image + upload SQL scripts to S3
5. **Render** the manifests, then apply both in one `kubectl` call

```bash
source mysql-cdc-env.sh
export AWS_REGION=us-west-2
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME=emr-on-eks-test-${AWS_ACCOUNT_ID}-${AWS_REGION}
export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::${AWS_ACCOUNT_ID}:role/emr-on-eks-test-execution-role
export LAKEHOUSE_FORMAT=both  # paimon | iceberg | both

# 1. MySQL password Secret. The manifests pull MYSQL_PASSWORD in via
#    `envFrom: secretRef`, so the password is NEVER written into any file in
#    this repo — not even the rendered *-deployed.yaml. Create it once:
#    (see 0-MYSQL-SETUP-GUIDE.md; default name is flink-cdc-mysql-env)
kubectl get secret flink-cdc-mysql-env -n emr-flink   # verify it exists

# 2. Executor-script ConfigMap. flink-cdc-executor.py is mounted from this
#    ConfigMap at /opt/flink/usrlib — it is NOT fetched from S3, and
#    build-deploy-generic.sh does NOT create it. Re-run this after every edit
#    to flink-cdc-executor.py or the JobManager runs the old copy:
kubectl create configmap flink-cdc-executor-py -n emr-flink \
  --from-file=flink-cdc-executor.py=./flink-cdc-executor.py \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Build and push the image to ECR, and upload the SQL scripts to S3
./build-deploy-generic.sh build

# 4. Render flink-cdc-{paimon,iceberg}-deployed.yaml WITHOUT applying, then
#    apply both at once. `deploy` applies Paimon, waits for it to reconcile,
#    then applies Iceberg — the jobs start ~30s apart, which invalidates a
#    Paimon-vs-Iceberg comparison because whichever starts first snapshots a
#    smaller table and gets a head start on the binlog.
./build-deploy-generic.sh render
kubectl apply -n emr-flink \
  -f flink-cdc-paimon-deployed.yaml \
  -f flink-cdc-iceberg-deployed.yaml

# Stop both pipelines (prompts for confirmation)
./build-deploy-generic.sh cleanup
# or one at a time
kubectl delete -f flink-cdc-iceberg-deployed.yaml -n emr-flink
kubectl delete -f flink-cdc-paimon-deployed.yaml  -n emr-flink
```

> **Re-deploying is DELETE + APPLY, not `kubectl apply` alone.** `upgradeMode:
> last-state` means the operator restores a running job from its last checkpoint
> and will not pick up most spec changes (pod template, resources, parallelism)
> on a bare re-apply. Delete the FlinkDeployment, wait for the JobManager pod to
> disappear, confirm no HA ConfigMaps were left behind, then apply:
> ```bash
> kubectl delete flinkdeployment flink-cdc-paimon flink-cdc-iceberg -n emr-flink
> kubectl get pods -n emr-flink | grep flink-cdc          # wait until empty
> kubectl get configmap -n emr-flink | grep -E 'cluster-config-map|-config-map'
> #   ^ leftover HA ConfigMaps make the new JobManager recover the OLD job graph.
> #     Delete them before re-applying if any remain.
> kubectl apply -n emr-flink -f flink-cdc-paimon-deployed.yaml \
>                            -f flink-cdc-iceberg-deployed.yaml
> ```

**Flink Job Configuration** (from `flink-cdc-{paimon,iceberg}-sql.yaml`):
| Setting | Value |
|---------|-------|
| EMR Version | 7.13.0 |
| Flink Version | 1.20 |
| Job Manager | 1 replica, HA enabled, 16 Gi / 4 cpu, `driver-nodepool` (on-demand) |
| Task Manager | 32 Gi / 8 cpu, 4 task slots, `executor-memorynodepool` (on-demand) |
| Job parallelism | 32 (`job.parallelism` + `--parallelism 32`); `parallelism.default: 4`, `pipeline.max-parallelism: 128` |
| Task count | 808 tasks (Paimon) / 520 tasks (Iceberg) at parallelism 32 |
| Managed memory | `taskmanager.memory.managed.fraction: 0.2` — the rest of the 32 Gi goes to task heap (~21 Gi, ~5.3 Gi/slot) so the CDC deserializer stops OOMing |
| Checkpointing | 60s interval, 600s timeout, `tolerable-failed-checkpoints: 10` |
| State Backend | RocksDB + changelog (`state.changelog.enabled: true`), S3 checkpoints, EBS local recovery |
| Failover | `jobmanager.execution.failover-strategy: full`, `restart-strategy.type: exponential-delay`, `jobmanager.scheduler: default` (not adaptive) |
| Autoscaler | Disabled (`job.autoscaler.enabled: false`) |

Both jobs' `--parallelism 32` and checkpoint settings are **fair-comparison
variables** — change them in lockstep or the Paimon-vs-Iceberg numbers are not
comparable. The same applies to the `server-id` ranges: Paimon uses 5401–5552
and Iceberg 6401–6552 across the four CDC sources, 32 ids each, and they must
never overlap (MySQL evicts the older binlog client when an id is reused).

### Step 2 — Deploy Data Generator
> Guide: [2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md)

Two separate workloads, and they are **not** interchangeable. Every command takes
`mixed` (default) or `upsert` as a trailing argument.

| | `mixed` | `upsert` |
|---|---|---|
| Kind | Deployment | StatefulSet, 8 replicas |
| Emits DELETEs | **yes** | no |
| Rate | ~400 rows/s ceiling | measured, not configured |
| Use for | CDC correctness (all three event types) | the Paimon-vs-Iceberg benchmark |

```bash
source mysql-cdc-env.sh

# mixed — INSERT/UPDATE/DELETE, the only workload that emits DELETE events
./mysql-data-generator/deploy-data-generator.sh deploy
./mysql-data-generator/deploy-data-generator.sh logs
./mysql-data-generator/deploy-data-generator.sh stop

# upsert — benchmark load. StatefulSet, so pods get exact 0..N-1 ordinals and
# each owns a disjoint slice of the hot-key window.
./mysql-data-generator/deploy-data-generator.sh deploy upsert
./mysql-data-generator/deploy-data-generator.sh status upsert
./mysql-data-generator/deploy-data-generator.sh stop upsert

# Bound a measurement window by scaling to zero, not by DURATION_SECONDS
# (which is 0 = forever; a self-exiting pod just gets restarted).
kubectl scale statefulset mysql-upsert-loadgen -n emr-flink --replicas=0
```

**`mixed` operations per iteration (every 5 seconds), default frequency:**
| Operation | Volume |
|-----------|--------|
| INSERT customers | 1–10 |
| INSERT products | 1–10 |
| INSERT orders + order_items | 2–20 |
| UPDATE order statuses | 1–10 |
| UPDATE product stock | 1–10 |
| DELETE old cancelled orders | Every 10th iteration |

**`upsert` sizing rules** (current manifest: 8 replicas, `RATE_PER_POD 80000`,
`THREADS 8`, `BATCH_ROWS 8000`, `UPSERT_RATIO 0.5`, `HOT_KEY_RATIO 0.5`,
`HOT_KEY_WINDOW 5000000`):

- `RATE_PER_POD` is a **token-bucket ceiling set deliberately out of reach** so the
  pods run flat out. It is not a throughput claim — read `inst=` from the pod logs.
- `POD_COUNT` must equal `replicas`. It is the divisor sizing each pod's hot-key
  slice; the `scale` subcommand keeps them in step, a bare `kubectl scale` does not.
- `HOT_KEY_WINDOW` is a **buffer-pool** number: `window ≈ 0.65 x buffer_pool_bytes
  / avg_row_bytes`. At ~3.36 KB/row against a ~24 GB pool, 5,000,000 keys ≈ 16 GB
  and fits; 200,000,000 was 641 GB (27x the pool) and turned every upsert into a
  random disk read.
- `BATCH_ROWS` is **not monotonic** — a batch is one transaction holding every row
  lock it touches. Raising it to 10,000 measured *worse* (~40% batch failures).
- Judge the rate by `inst=`, never `avg=`; check `errors=` when `inst=` is low,
  because 1213/1205 are retried silently and never printed.

**Scaling:** `./mysql-data-generator/deploy-data-generator.sh scale 3` (or
`scale 16 upsert`, which also sets `POD_COUNT`)

### Step 3 — Monitoring
> Guide: [3-MONITOR.md](3-MONITOR.md)

Deploy a custom monitor pod in EKS that continuously tracks both Paimon and Iceberg CDC pipelines, comparing throughput, checkpoints, and table growth.

```bash
# Deploy (no image build — runs python:3.11-slim, code from a ConfigMap)
bash ./monitoring/deploy-monitor.sh

# Pause / resume without redeploying
kubectl scale deployment/flink-cdc-monitor -n emr-flink --replicas=0
kubectl scale deployment/flink-cdc-monitor -n emr-flink --replicas=1

# Remove entirely
kubectl delete -f monitoring/monitor-deployment-deployed.yaml

# View logs
kubectl logs -f deployment/flink-cdc-monitor -n emr-flink
```

The monitor reads table stats via:
- **Iceberg tables**: `pyiceberg` (snapshot metadata from S3)
- **Paimon tables**: boto3 Glue API fallback (since `table_type = PAIMON`)

For a point-in-time Flink-side capture (records/bytes written, checkpoint
success and duration, as a delta over a sampling window):

```bash
./monitoring/capture-flink-metrics.sh    # writes a CSV
```

> **Note:** The monitor pod requires IRSA. See [3-MONITOR.md](3-MONITOR.md) for trust policy setup.
>
> **Note:** The `cp_fail` / "Lifetime ckpt failed" figure that capture script
> reports is `counts.failed` — cumulative since job start, never decreasing. Use
> the windowed "Checkpoints failed" delta, or read the `history` array directly.
> See [3-MONITOR.md](3-MONITOR.md#checkpoint-failure-counts-are-cumulative-not-current).

### Step 4 — OLAP Query Layer (Athena + StarRocks)
> Guide: [4-STARROCKS-OLAP-ENGINE.md](4-STARROCKS-OLAP-ENGINE.md)

**Athena (Icebergv2)** — query via Athena Console directly:
```sql
SELECT * FROM flink_iceberg_db.customers LIMIT 10;
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
spark.sql("SELECT * FROM paimon_iceberg.flink_paimon_db.customers LIMIT 10").show()
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
- **Format V2** with positional delete files — merge-on-read for CDC, matching what
  the bundled `iceberg-flink-runtime.jar` sink writes
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
| CDC Latency | < 1 second (binlog → lakehouse) |
| Checkpoint Interval | 60 seconds (`execution.checkpointing.interval`) |
| Checkpoint Timeout | 600 seconds, up to 10 tolerable failures |
| Storage Format | Parquet (zstd compression) |
| Source scale (observed) | ~491M rows / 102 GiB read during the snapshot phase |

Throughput numbers are **workload- and run-specific** — measure them, do not quote
a table. Source-side write rate comes from the load generator's `inst=` line (see
[2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md)); sink-side rates and per-format
comparisons come from the monitor and the benchmark harness (see
[3-MONITOR.md](3-MONITOR.md) and [4-STARROCKS-OLAP-ENGINE.md](4-STARROCKS-OLAP-ENGINE.md)).

---

## Troubleshooting Quick Reference

| Issue | Solution |
|-------|----------|
| `NoSuchFileException: /tmp/pyflink/<uuid>/<uuid>/flink-cdc-executor.py` | The `flink-cdc-executor-py` ConfigMap is missing or stale. The operator's `jarURI` download lands in a volume the main container never mounts, so the script **must** come from the ConfigMap — re-create it (see Step 1) and re-deploy |
| Spec change (resources, parallelism, pod template) didn't take effect | `upgradeMode: last-state` means a bare `kubectl apply` won't pick it up. DELETE the FlinkDeployment, wait for the pods to go, check for leftover HA ConfigMaps, then apply |
| Job cycles RUNNING → RESTARTING → CREATED → RUNNING for no visible reason | Karpenter consolidation. `executor-memorynodepool` runs `consolidationPolicy: WhenEmptyOrUnderutilized` / `consolidateAfter: 2m` with a 25% Underutilized budget, so it evicts nodes still hosting RUNNING TaskManagers. With `failover-strategy: full` one lost TM restarts the whole graph. Look for `Disrupting NodeClaim: Underutilized` and `Evicted pod: Underutilized`; the JM only shows `NoResourceAvailableException` / an RPC `TimeoutException` |
| `AWSGlueClientFactory - No region info found, using SDK default region: us-east-1` + `EC2MetadataUtils` + `Unauthorized (Status Code: 401)` | **Noise on the JobManager, not a failure.** The job reaches RUNNING with these lines present and they do not appear in steady-state TM logs at all. Setting `AWS_DEFAULT_REGION` does NOT suppress it — that was tested with both vars confirmed in the resolved container env. The legacy EMR Glue client takes its region from the Hadoop/Hive Configuration, not the process env. Look elsewhere (usually Karpenter, above) for a restart cause |
| `cp_fail` / "Lifetime ckpt failed" is non-zero but unchanged | That counter is `counts.failed`, cumulative since job start; it never decreases. Only the `history` array from `/jobs/<id>/checkpoints` shows current health — see [3-MONITOR.md](3-MONITOR.md#checkpoint-failure-counts-are-cumulative-not-current) |
| Load generator far below target rate, no errors in the logs | 1213 (deadlock) and 1205 (lock-wait timeout) are retried silently and never printed — they only show in the `errors=` counter. Check `errors=` vs `batches=`, and judge rate by `inst=`, not `avg=`. See [2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md) |
| Both jobs are up but the benchmark isn't comparable | `deploy both` starts them ~30s apart. Use `render` + a single `kubectl apply -f a -f b` so the snapshot phases overlap |
| StarRocks BEs orphaned after a scale-down / PVCs still billing after uninstall | Scale the BEs down before the FE, and note `helm uninstall` leaves the 200 Gi BE and 50 Gi FE PVCs behind — see [4-STARROCKS-OLAP-ENGINE.md](4-STARROCKS-OLAP-ENGINE.md) |
| FlinkDeployment won't start | Check IAM role, secrets, image pull, ECR auth |
| CDC not capturing changes | Verify binlog enabled, CDC user has REPLICATION SLAVE |
| Duplicate email crash | Data generator uses UUID-based emails |
| Calculated column error (subtotal) | Don't INSERT into `order_items.subtotal` |
| Data generator connects but no data | Check pod logs for per-operation errors; ensure ConfigMap is redeployed |
| Monitor `PaimonStorageHandler` error | Monitor uses Glue API fallback for Paimon tables |
| Athena Spark can't read Paimon tables | Use Hadoop catalog in Athena Spark; set `metadata.iceberg.format-version = '2'` |
| Athena SQL can't read Paimon tables | Manually change table_type property to `Icberg` in Glue Catalog |
| StarRocks SA conflict | Annotate existing SA with Helm release metadata |
| Flink image build fails locally | Builds run in-cluster with Kaniko; no local Docker needed |
| Monitor pod stuck `ContainerCreating`/no logs | First boot pip-installs pyiceberg+pyarrow (~2 min); check `kubectl logs` again |
| Monitor pod can't assume role | Update IAM trust policy to allow the SA's OIDC subject |

---

## Resources Created

- ☑️ 1 RDS MySQL instance (binlog-enabled, CDC user configured)
- ☑️ 1 ECR repository (Flink CDC image, built in-cluster with Kaniko)
- ☑️ 2 FlinkDeployments (Paimon + Iceberg pipelines), 1 JM + 8 TMs each
- ☑️ 1 `flink-cdc-executor-py` ConfigMap (the PyFlink entrypoint — created by hand,
  **not** by `build-deploy-generic.sh`)
- ☑️ 1 Data generator (`mixed` Deployment or `upsert` StatefulSet, ConfigMap-based script)
- ☑️ 1 Monitor Deployment (Flink REST API + Glue/pyiceberg stats)
- ☑️ 4 Paimon tables on S3 (with Iceberg compatibility metadata)
- ☑️ 4 Iceberg tables on S3 (Glue catalog, format V2)
- ☑️ 1 StarRocks cluster on EKS (1 FE + 3 BE pods), optional
- ☑️ S3 checkpoint, changelog, savepoint and HA storage
- ☑️ `ebs-sc` gp3 StorageClass (exact name required by EMR for Flink local recovery)
- ☑️ Kyverno ClusterPolicy `flink-cdc-single-az` (pins each job's pods to one AZ)
- ☑️ Kubernetes Secrets for MySQL credentials (`flink-cdc-mysql-env` for the Flink
  jobs via `envFrom`, `mysql-credentials` for the data generators)

---

**Status**: ✅ Production Ready
**Last Updated**: 2026-09-27
**EMR Version**: 7.13.0
**Flink Version**: 1.20
**Paimon Version**: 1.3.2 (newest release still publishing `paimon-spark-3.5`; 1.4.x dropped it)
**Iceberg Version**: 1.10.0-amzn-1
**StarRocks Version**: 4.1.x — `SELECT CURRENT_VERSION()` reported `4.1.4-4a9848e`.
Neither the chart nor the images are pinned: `starrocks/deploy-starrocks.sh` passes no
`--version` to `helm upgrade --install`, and `helm/starrocks-eks-values.yaml` leaves
`image.tag: ""` so the chart's floating `4.1-latest` default applies. A fresh install
can therefore land on a newer build than the one benchmarked.
**MySQL Version**: 8.0.45
