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

> **Two things here are easy to get wrong, so read them before Quick Start:**
>
> 1. The **SQL scripts** come from S3. The **executor script itself
>    (`flink-cdc-executor.py`) does not** — it is mounted from the
>    `flink-cdc-executor-py` ConfigMap, and nothing in
>    `build-deploy-generic.sh` creates or refreshes that ConfigMap. See
>    [The executor script comes from a ConfigMap](#the-executor-script-comes-from-a-configmap).
> 2. Re-deploying after a spec change is **DELETE + APPLY**, not `kubectl apply`.
>    See [Change CDC Configuration](#change-cdc-configuration).

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
        ┌────────┴───-─────┐     ┌────────┴────────┐
        │Spark/Athena Query│     │  Athena Query   │
        └────────────-─────┘     └─────────────────┘
```

---

## File Structure

```
cdc-pipeline/
  ├── docker
    ├── Dockerfile.cdc-paimon        ← Docker image with FlinkCDC(add) + Paimon(add) + Iceberg 
  |
  ├── mysql-data-generator
    ├── deploy-data-generator.sh     ← Manages both workloads (pass `mixed` or `upsert`)
    ├── mysql-data-generator.yaml    ← `mixed`: Deployment, INSERT/UPDATE/DELETE, ~400 rows/s
    ├── mysql-upsert-loadgen.yaml    ← `upsert`: StatefulSet, 8 pods, batched upserts
    ├── find-rds-ceiling.sh          ← Ramps upsert replicas to find the RDS write ceiling
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

export VERSION=7.13.0
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

### 2. Create the MySQL Secret and the executor ConfigMap

Neither of these is created by `build-deploy-generic.sh`. Both are prerequisites.

```bash
# MySQL password — the manifests read it at pod start via
#   envFrom: secretRef: <MYSQL_ENV_SECRET_NAME>   (default: flink-cdc-mysql-env)
# with key MYSQL_PASSWORD. No password is ever written into a manifest, rendered
# or otherwise. Create it per 0-MYSQL-SETUP-GUIDE.md, then verify:
kubectl get secret flink-cdc-mysql-env -n emr-flink

# Executor script — mounted at /opt/flink/usrlib from this ConfigMap.
# Re-run after EVERY edit to flink-cdc-executor.py, or the JobManager silently
# runs the previous copy.
kubectl create configmap flink-cdc-executor-py -n emr-flink \
  --from-file=flink-cdc-executor.py=./flink-cdc-executor.py \
  --dry-run=client -o yaml | kubectl apply -f -
```

### 3. Build, Render, Apply

```bash
# build docker images, upload SQL scripts to s3 (one-off)
./build-deploy-generic.sh build

# Render the *-deployed.yaml manifests WITHOUT applying, then apply both at once.
./build-deploy-generic.sh render
kubectl apply -n emr-flink \
  -f flink-cdc-paimon-deployed.yaml \
  -f flink-cdc-iceberg-deployed.yaml
```

> **Why `render` and not `deploy` for a benchmark.** `deploy both` applies Paimon,
> waits for it to reconcile, *then* applies Iceberg — the two jobs start ~30s
> apart. With `scan.startup.mode = initial`, whichever starts first snapshots a
> smaller MySQL table and gets a head start on the binlog, which invalidates the
> Paimon-vs-Iceberg comparison. Applying both in one `kubectl` call makes their
> snapshot phases overlap. `deploy` is fine when you are not comparing the two.

`./build-deploy-generic.sh deploy` remains the one-command path:

```bash
./build-deploy-generic.sh deploy
```

Both `render` and `deploy` also create the `ebs-sc` StorageClass, install Kyverno
and apply the `flink-cdc-single-az` ClusterPolicy, and (for Iceberg) create the
Glue database. Note that `deploy` does **not** upload the SQL scripts — that call
is commented out of the `deploy` path, so `build` (or a manual
`aws s3 sync sql-scripts/ s3://${BUCKET_NAME}/flink/sql-scripts/`) is required
first.

### 4. Monitor
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

### The executor script comes from a ConfigMap

`flink-cdc-executor.py` is mounted into the JobManager at `/opt/flink/usrlib`
from the `flink-cdc-executor-py` ConfigMap. The `job.jarURI` in the manifests
still points at
`s3://${BUCKET_NAME}/flink/scripts/flink-cdc-executor.py`, but that path is
**not** how the file arrives.

The operator's artifact plumbing is broken for this case. It injects an `aws-cli`
initContainer that downloads `jarURI` into a volume named `flink-artifact` at
`/flink-artifact`, but the main container never mounts that volume — it mounts
`user-artifacts-volume` at `/opt/flink/artifacts`, while the job args point at a
third path, `/opt/flink/usrlib/`. The download succeeds (the initContainer exits
0) and the file is then discarded with the volume, so `PythonDriver` fails with:

```
java.nio.file.NoSuchFileException: /tmp/pyflink/<uuid>/<uuid>/flink-cdc-executor.py
```

(That `/tmp/pyflink` path is where `PythonEnvUtils` symlinks the `-py` argument,
so the message names the temp copy rather than the missing source.) Mounting the
ConfigMap directly at `/opt/flink/usrlib` puts the file exactly where the args
already expect it; the directory is empty in the image, so shadowing it costs
nothing.

**Consequences for the workflow:**

- `build-deploy-generic.sh` does not create or refresh this ConfigMap. Doing it is
  on you.
- Editing `flink-cdc-executor.py` and re-running `build` changes nothing — the S3
  copy is never read. Refresh the ConfigMap:
  ```bash
  kubectl create configmap flink-cdc-executor-py -n emr-flink \
    --from-file=flink-cdc-executor.py=./flink-cdc-executor.py \
    --dry-run=client -o yaml | kubectl apply -f -
  ```
- A ConfigMap change does not restart a running JobManager. Delete and re-apply
  the FlinkDeployment (below).

### Change CDC Configuration

**SQL-only change** (e.g. a connector option). These *are* read from S3, so
upload and restart the job:

```bash
# Update MySQL connection timeout in sql-scripts/common/02-cdc-sources.sql:
#   'connect.timeout' = '60s',   -- Changed from 30s
aws s3 cp sql-scripts/common/02-cdc-sources.sql \
  s3://${BUCKET_NAME}/flink/sql-scripts/common/
```

**Then restart — DELETE + APPLY, in that order.** A bare `kubectl apply` is not
enough: `upgradeMode: last-state` means the operator restores the running job
from its last checkpoint and will not pick up most spec changes (pod template,
resources, parallelism, env, mounted ConfigMap contents). The SQL is also parsed
once at job submission, so a re-upload alone changes nothing on a running job.

```bash
# 1. Delete
kubectl delete flinkdeployment flink-cdc-paimon flink-cdc-iceberg -n emr-flink

# 2. Wait until the JobManager and all TaskManager pods are gone
kubectl get pods -n emr-flink | grep flink-cdc
#    (repeat until it returns nothing — do NOT apply while pods are terminating)

# 3. Check for leftover HA ConfigMaps. The operator stores the job graph and
#    leader info in these; if any survive the delete, the new JobManager will
#    recover the OLD job graph and your change is silently ignored.
kubectl get configmap -n emr-flink | grep flink-cdc
#    Expected to remain: flink-cdc-executor-py (yours), flink-cdc-monitor-* .
#    Expected to be GONE: flink-cdc-<job>-cluster-config-map and
#                         flink-cdc-<job>-<hash>-config-map
#    Delete any that linger:
# kubectl delete configmap flink-cdc-paimon-cluster-config-map -n emr-flink

# 4. Apply both at once
kubectl apply -n emr-flink \
  -f flink-cdc-paimon-deployed.yaml \
  -f flink-cdc-iceberg-deployed.yaml
```

Re-run `./build-deploy-generic.sh render` first if you changed a `*-sql.yaml`
template or any of the substituted environment variables.

---

## How the manifests are rendered

`generate_flink_deployment()` turns `flink-cdc-<format>-sql.yaml` into
`flink-cdc-<format>-deployed.yaml` with a single `sed` pass. It substitutes
exactly these placeholders, and nothing else:

| Placeholder | Source / default |
|---|---|
| `${AWS_REGION}` | `$AWS_REGION`, which the script defaults to `us-west-2` |
| `${AWS_ACCOUNT_ID}` | required |
| `${BUCKET_NAME}` | required |
| `${EMR_EXECUTION_ROLE_ARN}` | required |
| `${EMR_VERSION}` | `7.13.0` |
| `${IMAGE_TAG}` | `$IMAGE_TAG`, else `$EMR_VERSION`, else `7.13.0` |
| `${GLUE_DATABASE}` | `flink_iceberg_db` |
| `${MYSQL_HOST}` / `${MYSQL_USER}` | required |
| `${MYSQL_ENV_SECRET_NAME}` | `flink-cdc-mysql-env` |
| `${JM_NODEPOOL}` | `driver-nodepool` |
| `${TM_NODEPOOL}` | `executor-memorynodepool` |

`MYSQL_PASSWORD` is **deliberately not substituted**, even though
`check_env_vars` requires it to be set in your shell (the build path needs it for
other steps). The manifests obtain it at pod start with
`envFrom: secretRef: <MYSQL_ENV_SECRET_NAME>`, so the password never appears in a
file — not even the gitignored `*-deployed.yaml`. `envFrom` is used rather than an
`env[].valueFrom.secretKeyRef` because the EMR Flink operator's pod-template env
merge emits an empty `value: ""` alongside `valueFrom`, which the Kubernetes API
rejects with *"may not be specified when `value` is not empty"*.

The script then refuses to continue on either of two conditions:

```
Unsubstituted placeholders remain in flink-cdc-<format>-deployed.yaml: ...
Empty env value(s) rendered into flink-cdc-<format>-deployed.yaml: ...
```

The second guard matters more than it looks: an empty `AWS_REGION` renders as a
blank value that Kubernetes accepts, and the job then dies minutes later with an
error naming a heartbeat timeout rather than the missing variable.

> The two templates are **not** symmetric on region. Iceberg uses
> `fs.s3a.endpoint.region: ${AWS_REGION}`; Paimon hardcodes `aws.region: us-west-2`
> and `fs.s3a.endpoint.region: us-west-2`. Deploying Paimon to another region
> requires editing `flink-cdc-paimon-sql.yaml`.

## Current job sizing

From the templates (both jobs, kept in lockstep as fair-comparison variables):

| Setting | Value |
|---|---|
| JobManager | 1 replica, `highAvailabilityEnabled: true`, 16 Gi / 4 cpu, `driver-nodepool`, on-demand |
| TaskManager | 32 Gi / 8 cpu, `taskmanager.numberOfTaskSlots: 4`, `executor-memorynodepool`, on-demand |
| Job parallelism | 32 (`job.parallelism` and `--parallelism 32`) → 8 TMs per job |
| `parallelism.default` | 4 |
| `pipeline.max-parallelism` | 128 |
| Task graph | 808 tasks (Paimon) / 520 tasks (Iceberg) |
| `taskmanager.memory.managed.fraction` | 0.2 — at 32 Gi process size this leaves ~21 Gi of task heap (~5.3 Gi/slot). At the default 0.4 the CDC deserializer OOMed inside `RowDataDebeziumDeserializeSchema`, killing TaskManagers |
| Checkpointing | 60s interval, 600s timeout, 30s min-pause, `tolerable-failed-checkpoints: 10` |
| State | RocksDB + changelog (`state.changelog.enabled: true`), S3 checkpoints, EBS local recovery (`ebs-sc`) |
| Failover | `jobmanager.execution.failover-strategy: full`, `restart-strategy.type: exponential-delay`, `jobmanager.scheduler: default` |
| Autoscaler | off (`job.autoscaler.enabled: false`, `job.autoscaler.scaling.enabled: false`) |
| `server-id` ranges | Paimon 5401–5432 / 5441–5472 / 5481–5512 / 5521–5552; Iceberg 6401–6432 / 6441–6472 / 6481–6512 / 6521–6552 |

Notes on the less obvious ones:

- **`failover-strategy: full`, not `region`.** Each of the source subtasks is its
  own pipelined region, so `region` failover had nothing to cascade to: one source
  subtask died, the rest stayed RUNNING, Flink did not consider the job failed, and
  every subsequent checkpoint aborted at trigger time with *"Not all required tasks
  are currently running"* for 65 minutes with no error surfaced. Aborted-at-trigger
  checkpoints are not counted as FAILED, so `tolerable-failed-checkpoints` never
  fired either.
- **`exponential-delay`, not `fixed-delay`.** `fixed-delay`'s attempts counter is
  global while region failover files one restart request per failed task, so a
  single TaskManager loss consumed the whole budget — observed as *"Restarting
  job."* 200 times inside 20 milliseconds, then *"Recovery is suppressed by
  FixedDelayRestartBackoffTimeStrategy"*. No attempt cap is safe, because the
  number scales with task count rather than with real faults.
- **`scheduler: default`, not `adaptive`.** Adaptive tracks desired vs available
  parallelism instead of requiring every declared task to be deployed, which is how
  a subtask sat FAILED with the job still reporting RUNNING and `failure-cause`
  `None`. Autoscaling is off, so adaptive buys nothing.
- **`server-id` ranges must not overlap.** Both pipelines read the same RDS
  instance, and MySQL evicts whichever binlog client connected first when a second
  reuses its id. Each range is 32 wide to cover parallelism 32 — a narrower range
  makes source subtasks share ids and evict each other.

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
# Build Docker image (Kaniko, in-cluster) and upload SQL scripts to S3
./build-deploy-generic.sh build

# Render *-deployed.yaml WITHOUT applying — use this for benchmark runs, then
# apply both manifests in ONE kubectl call so the snapshot phases overlap
./build-deploy-generic.sh render

# Deploy to Kubernetes (image must exist). Applies Paimon, waits, then Iceberg —
# ~30s apart, which is fine unless you are comparing the two formats.
./build-deploy-generic.sh deploy

# Build + Deploy (complete setup)
./build-deploy-generic.sh all

# Monitor deployment
./build-deploy-generic.sh monitor paimon
./build-deploy-generic.sh monitor iceberg

# Clean up
./build-deploy-generic.sh cleanup
```

Neither `render` nor `deploy` creates the `flink-cdc-executor-py` ConfigMap or the
MySQL Secret. Both are prerequisites — see step 2 of Quick Start.

`LAKEHOUSE_FORMAT` (`paimon` | `iceberg` | `both`, default `both`) selects which
manifests each action touches. Required environment variables: `AWS_REGION`
(defaulted to `us-west-2`), `AWS_ACCOUNT_ID`, `BUCKET_NAME`,
`EMR_EXECUTION_ROLE_ARN`, `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`.
`GLUE_DATABASE` is optional and defaults to `flink_iceberg_db`.

### Stopping the pipelines

`cleanup` prompts for confirmation, then deletes the rendered FlinkDeployments. It takes an
optional format so you can stop one pipeline and leave the other running — useful when only
one side of the Paimon-vs-Iceberg comparison needs restarting:

```bash
./build-deploy-generic.sh cleanup            # both (LAKEHOUSE_FORMAT default)
./build-deploy-generic.sh cleanup paimon     # Paimon only
./build-deploy-generic.sh cleanup iceberg    # Iceberg only
```

This removes the jobs, **not** the data: S3 warehouses, Glue tables and checkpoints all
survive, so a redeploy resumes against the existing tables.

To stop everything the pipeline touches, stop the load generator and the monitor too — they
are independent workloads and `cleanup` does not know about them:

```bash
./mysql-data-generator/deploy-data-generator.sh stop upsert   # or `mixed`
kubectl scale deployment/flink-cdc-monitor -n emr-flink --replicas=0
```

> Restarting a job with `kubectl delete flinkdeployment` + `kubectl apply` (as in
> [Change CDC Configuration](#change-cdc-configuration) above) is not the same as `cleanup`:
> it reuses the already-rendered `*-deployed.yaml` and skips the confirmation prompt.

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

### `NoSuchFileException` on the executor script

**Issue**:
```
java.nio.file.NoSuchFileException: /tmp/pyflink/<uuid>/<uuid>/flink-cdc-executor.py
```

**Solution**: The `flink-cdc-executor-py` ConfigMap is missing. The `jarURI`
download is discarded by the operator, so the ConfigMap is the only source of the
file. See
[The executor script comes from a ConfigMap](#the-executor-script-comes-from-a-configmap).

```bash
kubectl get configmap flink-cdc-executor-py -n emr-flink
```

### A config change had no effect

**Issue**: You edited a `*-sql.yaml` template, or `flink-cdc-executor.py`, or a SQL
script, re-applied, and the job behaves exactly as before.

**Solution**: `upgradeMode: last-state` restores the running job from its
checkpoint rather than re-submitting it. Do the full DELETE + APPLY, including the
leftover-HA-ConfigMap check — see
[Change CDC Configuration](#change-cdc-configuration). Also re-run
`./build-deploy-generic.sh render` if the change was in a template, and refresh
the `flink-cdc-executor-py` ConfigMap if it was in the Python.

### Job cycles RUNNING → RESTARTING → CREATED → RUNNING

**Issue**: The job restarts repeatedly. The JobManager log shows
`NoResourceAvailableException` or an RPC `TimeoutException` on a Writer subtask,
and TaskManager attempt indices climb (`...-taskmanager-1-23`). Nothing names a
cause.

**Cause: Karpenter consolidation, not Flink.** `executor-memorynodepool` runs
`consolidationPolicy: WhenEmptyOrUnderutilized` with `consolidateAfter: 2m` and a
25% `Underutilized` disruption budget, so it deletes nodes that are still hosting
RUNNING TaskManagers:

```
nodeclaim/...                          Disrupting NodeClaim: Underutilized
pod/flink-cdc-paimon-taskmanager-1-23  Evicted pod: Underutilized
```

Losing one TaskManager with `failover-strategy: full` restarts the entire task
graph (808 tasks for Paimon, 520 for Iceberg). Confirm with:

```bash
kubectl get nodepool executor-memorynodepool -o jsonpath='{.spec.disruption}'
kubectl get events -n emr-flink --sort-by=.lastTimestamp | grep -i underutilized
```

The second-order cause is a fragmented instance mix. `instance-size` allows
`4xlarge|8xlarge|12xlarge|16xlarge` and a TaskManager requests 8100m cpu /
32868Mi, so per node: 4xlarge fits 1 TM, 8xlarge 3, 12xlarge 5, 16xlarge 7 (six do
*not* fit on 48 vCPU: `6 x 8100 + 480m DaemonSets = 49080m > 47810m` allocatable).
Karpenter satisfies a leftover single pod with a 4xlarge holding one TM and 7.4
vCPU stranded, which keeps the pool looking "underutilized" and invites the next
consolidation pass — a self-feeding loop. Nodepool `limits.cpu` is *not* the
constraint here (16 TMs provision 160–256 vCPU against `limits.cpu: 3440`).

For a clean apple-to-apple run, pin the pool to ONE instance size (16xlarge alone
→ `ceil(16/7) = 3` nodes; 12xlarge alone → 4 nodes, landing 5+5+5+1). That is a
change to a **shared** nodepool spec, so it has deliberately not been made — it
needs explicit sign-off.

### Glue region / IMDS 401 log sequence — this is NOISE

**Issue**: The JobManager log shows:
```
AWSGlueClientFactory - No region info found, using SDK default region: us-east-1
EC2MetadataUtils - Unable to retrieve the requested metadata (/latest/dynamic/instance-identity/document)
com.amazonaws.AmazonServiceException: Unauthorized (Status Code: 401)
```

**This is not a failure and it is not the cause of a restart.** The job reaches
RUNNING with all tasks deployed while these lines are present, and they do not
appear in steady-state TaskManager logs at all (0 hits across all 8 TMs).

**Setting `AWS_DEFAULT_REGION` does not suppress it.** That was tested: with *both*
`AWS_REGION` and `AWS_DEFAULT_REGION` confirmed present in the resolved JobManager
and TaskManager container env (`env | grep ^AWS_`), the sequence still appeared.
The legacy EMR Glue client resolves its region from the Hadoop/Hive Configuration
it is handed, not from the process environment, so no env var can silence it. The
401 is IMDSv2 being unreachable from the pod (hop limit / IRSA-only networking).
Suppressing it properly would mean setting the region in Hadoop conf (a
`hive-site`/`core-site` property the Glue factory reads).

Both manifests do still set `AWS_DEFAULT_REGION` — for the v1 SDK's
`DefaultAwsRegionProviderChain`, which reads it where the v2 SDK reads
`AWS_REGION`. That is worth keeping; it just is not a fix for the above.

> Separately, `AWS_REGION` **must** be set on the TaskManager and not only the
> JobManager. Paimon's `IcebergHiveMetadataCommitter` and the EMR Glue client run
> in the TM (`CommitterOperator.initializeState`); without a region the client
> blocks on a cross-region Glue call, stalling `initializeState` until
> `heartbeat.timeout` (180s) kills the TM. The job then fails with *"The heartbeat
> of TaskManager … timed out"* and nothing in the JM log names the real cause.

---

## Next Steps

- **Generate sample data**: Follow the [Data Generator Guide](./2-DATA-GEN-GUIDE.md) to produce
  more source data in the MySQL DB at different rates. Use the `mixed` workload for CDC
  correctness (it is the only one that emits DELETE events) and the `upsert` workload
  (StatefulSet, 8 pods) for high-rate benchmark load — its rate is measured from the pods'
  `inst=` line, not configured.
- **Monitor the pipelines**: [Monitor Guide](./3-MONITOR.md)
- **Query with StarRocks**: [StarRocks OLAP Engine](./4-STARROCKS-OLAP-ENGINE.md)

---

**Status**: ✅ Production Ready
**Last Updated**: 2026-09-27
**EMR Version**: 7.13.0
**Flink Version**: 1.20
**Paimon Version**: 1.3.2 (newest release still publishing `paimon-spark-3.5`; 1.4.x dropped it)
**Iceberg Version**: 1.10.0-amzn-1
**mySQL Version**: 8.0.45
