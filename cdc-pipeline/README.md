# Realtime Lakehouse via Flink CDC (FlinkSQL approach)

MySQL RDS → Flink CDC (EMR on EKS) → **Apache Paimon** and **Apache Iceberg** on S3
(AWS Glue catalog), written side by side from the same binlog, then read by
**DuckDB** and **Trino** in a hot and a snapshot query benchmark.

This file is the end-to-end runbook (state as of **2026-11-02**). The detailed
guides it links to:
[0-MYSQL-SETUP-GUIDE.md](0-MYSQL-SETUP-GUIDE.md) ·
[1-BUILD-DEPLOY-FLINK-APP-GUIDE.md](1-BUILD-DEPLOY-FLINK-APP-GUIDE.md) ·
[2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md) ·
[3-MONITOR.md](3-MONITOR.md) ·
[engine-deploy/README.md](engine-deploy/README.md) ·
[querybench/README.md](querybench/README.md) ·
[PAIMON-VS-ICEBERG.md](PAIMON-VS-ICEBERG.md)

## Quick start: what to ask your AI assistant

Clone the repo, `cd cdc-pipeline`, start the assistant, and ask in plain language, for example:

- "Follow the README runbook on my existing EKS cluster `demo`, using the table set in `.bench-env`"
- "Start a new table set `_v3r2` and deploy both writers in one apply"
- "Start the upsert generator and report the achieved rate"
- "Run the hot test, then the snapshot test, and write the summary"
- "Tear the run down and clean up the old table set `_v3`"

## Stack (2026-11-02)

| Component | Version / setting |
|---|---|
| EMR on EKS | **7.14.0** (`emr-7.14.0-flink-latest`). This is the newest EMR release with Flink; EMR 8.x has no Flink |
| Flink | 1.20.0-amzn-8, Java 17 |
| EMR Flink operator | Helm chart **7.14.0** (`oci://public.ecr.aws/emr-on-eks/flink-kubernetes-operator`) |
| MySQL CDC connector | `flink-sql-connector-mysql-cdc` **3.7.0-1.20** |
| Paimon | **2.1.0**. The catalog uses `s3.aws.credentials.provider = WebIdentityTokenFileCredentialsProvider` |
| Iceberg (Flink sink) | OSS `iceberg-flink-runtime-1.20` **1.12.0**, plus `url-connection-client` 2.44.5 in the image. The catalog uses `http-client.type = urlconnection` |
| Flink image | `<account>.dkr.ecr.<region>.amazonaws.com/emr-flink-cdc-paimon:7.14.0`: multi-arch (amd64 + arm64), `imagePullPolicy: Always` |
| Table format | Iceberg format **v3** by default (`ICEBERG_FORMAT_VERSION=3`). Paimon exports Iceberg v3 metadata with the same version |
| Query engines | DuckDB **2.0.0**, a source-built image `duckdb-cdc-server:2.0.0` (multi-arch); Trino **484** (chart 1.45.0). StarRocks was removed on 2026-11-02 |
| Iceberg compaction | EMR 7.14.0 Spark image (Spark 3.5, Iceberg 1.10.1-amzn-1), run as a bare Kubernetes Job |
| Source | RDS MySQL 8.0 `ecommerce` DB. It was shrunk to its newest ~10% (**37.5M rows**) on 2026-11-02 |

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ MySQL RDS 8.0 (ecommerce: customers, products, orders, order_items)  │
│ binlog ROW, CDC user with REPLICATION SLAVE / CLIENT                 │
│   ▲ upsert load generator (StatefulSet mysql-upsert-loadgen, 4 pods) │
└──────────────┬─────────────────────────────────────┬─────────────────┘
               │ full snapshot, then binlog          │ (server-ids 64xx)
               │ (server-ids 54xx)                   │
┌──────────────▼──────────────┐       ┌──────────────▼──────────────┐
│ FlinkDeployment             │       │ FlinkDeployment             │
│ flink-cdc-paimon            │       │ flink-cdc-iceberg           │
│ PyFlink SQL executor        │       │ PyFlink SQL executor        │
│ (jarURI = executor .py)     │       │ (jarURI = executor .py)     │
│ parallelism 32, ckpt 60 s   │       │ parallelism 32, ckpt 60 s   │
└──────────────┬──────────────┘       └──────────────┬──────────────┘
               │                                     │
   paimon-warehouse/ (S3)                iceberg-warehouse/ (S3)
   Glue DB flink_paimon_db<suffix>       Glue DB flink_iceberg_db<suffix>
   + Iceberg export under                + external compaction Job
     paimon-warehouse/iceberg/             (rewrite + expire_snapshots)
               │                                     │
               └───────────────┬─────────────────────┘
                               ▼
        ┌──────────────────────────────────────────────┐
        │ Query benchmark (querybench/): four arms     │
        │ DuckDB 2.0.0 warm server │ Trino 484 (3 wkr) │
        │ × Iceberg native         │ × Paimon export   │
        └──────────────────────────────────────────────┘
   monitor: flink-cdc-monitor (Flink REST + table stats, both formats)
```

## Files

```
cdc-pipeline/
├── .bench-env                  session settings: table set, warehouses, engine versions (git)
├── .bench-env.local            account values (git-ignored, you create it)
├── mysql-cdc-env.sh            MYSQL_* values (git-ignored, from 0-MYSQL-SETUP-GUIDE.md)
├── lib/common.sh               loads .bench-env, keeps command-line overrides, derives table-set names
├── lib/querybench.sh           shared query-benchmark functions (arms, pins, gates, stop/suspend)
├── build-deploy-generic.sh     build | render | deploy | all | cleanup | monitor
├── docker/Dockerfile.cdc-paimon   Flink image (CDC + Paimon + Iceberg jars)
├── flink-cdc-executor.py       PyFlink executor: runs the SQL files in order
├── flink-cdc-{paimon,iceberg}-sql.yaml   FlinkDeployment templates (${VAR})
├── sql-scripts/
│   ├── common/02-cdc-sources.sql          MySQL CDC source tables
│   ├── paimon/01-catalog-setup.sql, 03-paimon-sinks.sql, 04-cdc-pipelines.sql
│   └── iceberg/01-catalog-setup.sql, 03-iceberg-sinks.sql, 04-cdc-pipelines.sql
├── k8s/                        ebs-sc StorageClass, Kyverno single-AZ policy
├── mysql-data-generator/       deploy-data-generator.sh, mysql-upsert-loadgen.yaml, find-rds-ceiling.sh
├── monitoring/                 deploy-monitor.sh, monitor code, capture-flink-metrics.sh
├── engine-deploy/              deploy-duckdb-cdc.sh, deploy-trino.sh, duckdb-image/ (README.md there)
├── iceberg-compaction/         run-compaction.sh, compact-job.yaml, compact-order-items.py
├── querybench/                 run.sh (start|render|status|logs|summary|stop), suites, README.md
└── bench-results/              local output (git-ignored)
```

`*-deployed.yaml` files are renderings with values substituted. They are
git-ignored, and every run overwrites them. Edit the templates instead.

---

# Runbook

Run every command from `cdc-pipeline/`. Each step lists what it does, how to
check it worked, how long it takes, and the main failure with its fix. Times
are marked **(measured)** when they were observed in the 2026-11-02 dry run.
They are marked **(estimate)** otherwise.

## Step 0: Prerequisites

| Item | Requirement |
|---|---|
| EKS cluster | `kubectl` points at it. It needs Karpenter NodePools `driver-nodepool` (on-demand: JobManagers, generator, DuckDB, Trino, runner) and `executor-memorynodepool` (TaskManagers). The TaskManager pool provisions both x86 and Graviton nodes, so every image must be multi-arch |
| Namespace | `emr-flink`, set up for EMR on EKS. The execution role `emr-on-eks-test-execution-role` (`EMR_EXECUTION_ROLE_ARN`) must trust `system:serviceaccount:emr-flink:*` through the cluster's OIDC provider. It also needs Glue and S3 read/write on the bucket |
| cert-manager | Needed by the operator chart ([1-BUILD guide, step 0](1-BUILD-DEPLOY-FLINK-APP-GUIDE.md)) |
| IRSA ServiceAccount `starrocks-sa` | **The name is historical.** StarRocks is gone, but this is still the IRSA account shared by the monitor, the DuckDB server, Trino and the Iceberg compaction Jobs. It is annotated with the EMR execution role |
| IRSA ServiceAccount `image-builder-sa` | Used by the Kaniko build pods (Flink image and DuckDB image). Its role must be able to push to ECR and read `s3://$BUCKET_NAME/flink/build/` |
| ECR | The repositories `emr-flink-cdc-paimon` and `duckdb-cdc-server` are created by the build scripts. Do **not** add a lifecycle rule that expires untagged images: the per-arch children of the multi-arch tag are untagged |
| S3 | `BUCKET_NAME` holds the warehouses, Flink state, logs, query pins and results. The bucket is versioned (see Step 13) |
| RDS MySQL 8.0 | Set up as in [0-MYSQL-SETUP-GUIDE.md](0-MYSQL-SETUP-GUIDE.md): in the EKS VPC, binlog `ROW`, `cdcuser` with `REPLICATION SLAVE` / `REPLICATION CLIENT`, the `ecommerce` schema. The guide also writes `mysql-cdc-env.sh` |
| Secrets | `flink-cdc-mysql-env` (key `MYSQL_PASSWORD`; the Flink jobs read it through `envFrom`) and `mysql-credentials` (`hostname` / `username` / `password`; the generator reads it). `deploy-data-generator.sh` creates `mysql-credentials` from `MYSQL_*` if it is missing |
| Laptop | `kubectl`, `helm`, AWS CLI, `envsubst` (`brew install gettext`), `python3` with PyYAML (used by `run-compaction.sh`). You also need this repo's `throughput-benchmark/` directory: the engine scripts apply its `gp3-fast` StorageClass |

```bash
# ServiceAccounts (skip any that already exist)
kubectl create serviceaccount starrocks-sa -n emr-flink
kubectl annotate serviceaccount starrocks-sa -n emr-flink \
  eks.amazonaws.com/role-arn="$EMR_EXECUTION_ROLE_ARN"
kubectl create serviceaccount image-builder-sa -n emr-flink
kubectl annotate serviceaccount image-builder-sa -n emr-flink \
  eks.amazonaws.com/role-arn=<role that can push to ECR and read the S3 build context>

# Flink secret (the generator secret is created by Step 6 if missing)
source mysql-cdc-env.sh
kubectl create secret generic flink-cdc-mysql-env -n emr-flink \
  --from-literal=MYSQL_PASSWORD="$MYSQL_PASSWORD" --dry-run=client -o yaml | kubectl apply -f -
```

- **Check:** run `kubectl get sa starrocks-sa image-builder-sa -n emr-flink -o yaml | grep role-arn` (two lines), then `kubectl get secret flink-cdc-mysql-env mysql-credentials -n emr-flink`. Finally, the MySQL connectivity test in the MySQL guide must pass.
- **Time:** RDS creation is about 15–20 min (estimate). Everything else takes minutes.
- **If it fails:** a JobManager stuck in `CreateContainerConfigError` means `flink-cdc-mysql-env` is missing (it only shows in `kubectl describe pod`). Pods that cannot assume their role need the role's trust policy to cover the SA's OIDC subject.

## Step 1: Settings (`.bench-env`, `.bench-env.local`)

| File | In git | Holds |
|---|---|---|
| `.bench-env` | yes | `NAMESPACE=emr-flink`, **`ICEBERG_FORMAT_VERSION=3`**, **`CDC_DB_SUFFIX=_v3r1`**, `SESSION_SUFFIX`, the warehouses, `FLINK_JOBS`, `TRINO_VERSION=484` / `TRINO_CHART_VERSION=1.45.0`, `DUCKDB_IMAGE`, `GENERATOR_RESOURCES`, `COMPACT_BEFORE_SNAPSHOT=true`, `COMPACT_TABLES` |
| `.bench-env.local` | no | `AWS_REGION`, `AWS_ACCOUNT_ID`, `BUCKET_NAME`, `EMR_EXECUTION_ROLE_ARN`. `.bench-env` sources it, and falls back to `../throughput-benchmark/.bench-env.local` |
| `mysql-cdc-env.sh` | no | `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD` |

```bash
cat > .bench-env.local <<'EOF'
export AWS_REGION=<region>
export AWS_ACCOUNT_ID=<account-id>
export BUCKET_NAME=emr-on-eks-test-<account-id>-<region>
export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::<account-id>:role/emr-on-eks-test-execution-role
EOF
```

Which scripts read what:

| Script | Reads |
|---|---|
| `querybench/run.sh`, `engine-deploy/*.sh`, `duckdb-image/build-duckdb-image.sh`, `iceberg-compaction/run-compaction.sh` | `.bench-env` and `.bench-env.local` through `lib/common.sh`. A value set on the command line wins, with a `[cdc-bench] X=... overrides` line |
| `build-deploy-generic.sh` | The environment. When `CDC_DB_SUFFIX` is **unset**, it reads only that one value from `.bench-env`. `ICEBERG_FORMAT_VERSION` defaults to 3 and is *not* read from `.bench-env`. So `source .bench-env` first (export any override after that) |
| `monitoring/deploy-monitor.sh` | The environment. When `CDC_DB_SUFFIX` is unset, it also reads it from `.bench-env` |
| `mysql-data-generator/deploy-data-generator.sh` | The environment only (`MYSQL_*`, `NAMESPACE`) |

- **Check** (offline): `bash -c 'source lib/common.sh; echo $ICEBERG_FORMAT_VERSION $PAIMON_DB $ICEBERG_DB $QB_RESULTS_PREFIX'` prints `3 flink_paimon_db_v3r1 flink_iceberg_db_v3r1 querybench-results/cdc-session-v3r1`.
- **Time:** about 1 min.
- **If it fails:** `CDC_DB_SUFFIX must be empty or like _v3` means the suffix has to match `^(_[a-z0-9]+)?$`. Likewise `SESSION_SUFFIX` must be empty or look like `-r2`.

## Step 2: EMR Flink operator 7.14.0

A fresh install uses the commands in [1-BUILD-DEPLOY-FLINK-APP-GUIDE.md step 0](1-BUILD-DEPLOY-FLINK-APP-GUIDE.md).
An upgrade from an older chart works differently. `helm upgrade` never updates CRDs, so the upgrade path is
uninstall → delete the CRDs → install:

```bash
helm list -n emr-flink                                   # what is installed now
kubectl delete flinkdeployment flink-cdc-paimon flink-cdc-iceberg -n emr-flink --ignore-not-found
helm uninstall flink-kubernetes-operator -n emr-flink
# Deleting these CRDs DELETES EVERY FlinkDeployment / FlinkSessionJob in the cluster.
kubectl delete crd flinkdeployments.flink.apache.org flinksessionjobs.flink.apache.org
#   If this hangs, a CR is stuck on its finalizer (its operator is gone). In another shell:
#   kubectl get flinkdeployment -A
#   kubectl patch flinkdeployment <name> -n <ns> --type=merge -p '{"metadata":{"finalizers":null}}'
helm install flink-kubernetes-operator oci://public.ecr.aws/emr-on-eks/flink-kubernetes-operator \
  --version 7.14.0 -n emr-flink --set emrContainers.awsRegion=${AWS_REGION:-us-west-2}
kubectl wait deployment flink-kubernetes-operator -n emr-flink --for condition=Available=True --timeout=120s
```

- **Check:** `helm list -n emr-flink` shows `flink-kubernetes-operator-7.14.0`, and `kubectl get crd flinkdeployments.flink.apache.org` exists.
- **Time:** about 5 min (estimate).
- **If it fails:** the CRD delete hangs on finalizers (clear them as above). After an upgrade, clear leftover HA ConfigMaps before re-applying the jobs (Step 4).

## Step 3: Images

### 3a. Flink image + executor + SQL upload

```bash
source .bench-env                # AWS_* and the table set
./build-deploy-generic.sh build  # needs no MySQL values
```

This step:

1. Packs `docker/Dockerfile.cdc-paimon` into `s3://$BUCKET_NAME/flink/build/`.
2. Builds it with Kaniko, one pod per architecture (`flink-cdc-image-build-{amd64,arm64}`, built in sequence, `image-builder-sa`).
3. Joins the two builds into **one** multi-arch tag, `emr-flink-cdc-paimon:7.14.0`, and verifies both children.
4. Uploads `flink-cdc-executor.py` to `s3://$BUCKET_NAME/flink/scripts/` (the `jarURI`) and the SQL to `flink/sql-scripts/{paimon,iceberg}/`.

There is no upload-only command, so re-run `build` after any change to the
SQL or the executor. `PLATFORMS="amd64"` builds one architecture. Don't use it
for the TaskManager pool, which mixes architectures.

- **Check:** the log ends with `✓ Verified emr-flink-cdc-paimon:7.14.0 is one tag serving amd64/arm64`. Then run `aws s3 ls s3://$BUCKET_NAME/flink/sql-scripts/ --recursive`.
- **Time:** about 10–20 min per architecture, about 20–40 min total (estimate; each architecture has a 30 min timeout).
- **If it fails:**
  - `ServiceAccount image-builder-sa not found` means Step 0 is incomplete.
  - A failed Kaniko pod prints its last 60 log lines.
  - If TaskManagers die with `exitCode=255` while the JobManager survives (`NoResourceAvailableException`), the image is single-arch.

### 3b. DuckDB image (only if `duckdb-cdc-server:2.0.0` is missing, or its Dockerfile changed)

```bash
./engine-deploy/duckdb-image/build-duckdb-image.sh     # COMPILE_JOBS=30 by default
```

This compiles DuckDB 2.0.0 from source with iceberg, httpfs, aws and avro
linked in. Kaniko builds amd64 and arm64 **in parallel** on on-demand nodes
(30 cpu / 68 GiB each), and the script joins them into one multi-arch tag. See
[engine-deploy/duckdb-image/README.md](engine-deploy/duckdb-image/README.md).

- **Check:** `aws ecr describe-images --repository-name duckdb-cdc-server --image-ids imageTag=2.0.0`.
- **Time:** about 1.5–3 h per architecture on a cold build. That is the script's own figure (estimate); the timeout is 4 h.
- **If it fails:** an OOM or eviction in the build pod calls for a lower `COMPILE_JOBS`. To skip the build, set `DUCKDB_IMAGE=` (empty) to use `python:3.12-slim` + pip `duckdb==2.0.0`. That is a different build, so it is a different timing baseline.

## Step 4: Choose the table set

A **table set** is `ICEBERG_FORMAT_VERSION` + `CDC_DB_SUFFIX`:

| | Current set (`.bench-env`) |
|---|---|
| Glue databases | `flink_paimon_db_v3r1`, `flink_iceberg_db_v3r1` |
| Flink state / HA / logs | `s3://$BUCKET_NAME/flink/cdc-{paimon,iceberg}_v3r1/`, `flink/logs/cdc-{paimon,iceberg}_v3r1` |
| Warehouse data | `paimon-warehouse/flink_paimon_db_v3r1.db/`, Paimon's Iceberg export `paimon-warehouse/iceberg/flink_paimon_db_v3r1/`, `iceberg-warehouse/flink_iceberg_db_v3r1.db/` |
| Query results / pins | `querybench-results/cdc-session-v3r1<SESSION_SUFFIX>/`, `querybench-pins/cdc-session-v3r1<SESSION_SUFFIX>/` |

**An existing table never changes its format or options.** The DDL is
`CREATE TABLE IF NOT EXISTS`, so options apply at creation only. To test new
table settings (or another format version), set a **new** `CDC_DB_SUFFIX` in
`.bench-env`, e.g. `_v3r2`. A new set starts with empty tables and empty Flink
state, and the CDC sources run the full MySQL snapshot again (37.5M rows).

Before you deploy a set, nothing from the previous one may be left. The HA ConfigMap
names do **not** include the suffix, so a leftover makes the new JobManager
recover the old job graph:

```bash
kubectl get flinkdeployment -n emr-flink                         # expect: No resources found
kubectl get configmap -n emr-flink -o name | grep -E 'flink-cdc-(paimon|iceberg)-.*config-map'   # expect: nothing
# delete any leftovers:
kubectl get configmap -n emr-flink -o name | grep -E 'flink-cdc-(paimon|iceberg)-.*config-map' \
  | xargs -r kubectl delete -n emr-flink
# a NEW set must not exist yet:
aws glue get-database --name "flink_iceberg_db${CDC_DB_SUFFIX}"   # expect EntityNotFoundException
aws s3 ls "s3://$BUCKET_NAME/flink/cdc-iceberg${CDC_DB_SUFFIX}/"  # expect nothing
```

- **Check:** all four checks come back empty or not found.
- **Time:** about 1 min.
- **If it fails:** a FlinkDeployment that is still running must be deleted first (Step 12). The old set's data can stay; Step 13 removes it.

## Step 5: Writers (both FlinkDeployments in one apply)

```bash
source mysql-cdc-env.sh            # MYSQL_HOST, MYSQL_USER (render requires them; the password comes from the Secret)
source .bench-env
./build-deploy-generic.sh render   # writes flink-cdc-{paimon,iceberg}-deployed.yaml, applies nothing
kubectl apply -n emr-flink -f flink-cdc-paimon-deployed.yaml -f flink-cdc-iceberg-deployed.yaml
```

`render` checks the variables, applies the `ebs-sc` StorageClass and Kyverno with the
`flink-cdc-single-az` policy if they are missing, creates the Iceberg Glue
database, and renders both manifests. It fails on any `${VAR}` it leaves
behind and on any `value: ""`.

**Apply both files in one `kubectl apply`** so the two initial snapshots start
together. `./build-deploy-generic.sh deploy` (and `all`) applies Paimon,
waits, and then applies Iceberg. The jobs then start about 30 s apart, which
spoils the comparison.

**Re-deploying is delete + create, never `kubectl apply` over a live CR.** EMR
stages the `jarURI` into `/opt/flink/usrlib` on CREATE only. An apply over an
existing FlinkDeployment empties that directory, and the JobManager fails with
`NoSuchFileException: /tmp/pyflink/...`. With `upgradeMode: last-state`, a
bare apply also ignores most spec changes.

```bash
kubectl delete flinkdeployment flink-cdc-paimon flink-cdc-iceberg -n emr-flink
kubectl get pods -n emr-flink | grep flink-cdc        # wait until empty
# HA ConfigMap check from Step 4, then the same single apply as above
```

Pausing and resuming (no new snapshot, they restore their last checkpoint) only patches `job.state`:

```bash
for j in flink-cdc-paimon flink-cdc-iceberg; do
  kubectl patch flinkdeployment $j -n emr-flink --type=merge -p '{"spec":{"job":{"state":"running"}}}'   # or "suspended"
done
```

- **Check:**
  - `kubectl get flinkdeployment -n emr-flink` shows both jobs `RUNNING` / `STABLE`.
  - `aws glue get-tables --database-name flink_iceberg_db${CDC_DB_SUFFIX} --query 'TableList[].Name'` lists 4 tables, and the same holds for `flink_paimon_db${CDC_DB_SUFFIX}`.
  - Flink UI: `kubectl port-forward svc/flink-cdc-iceberg-rest 8081:8081 -n emr-flink`.
- **Time:** both JobManagers and TaskManagers come up in about 3–10 min, including Karpenter nodes (estimate). The initial MySQL snapshot of 37.5M rows has not been measured on the shrunk source; expect about 15–45 min (estimate).
- **If it fails:** see [Troubleshooting](#troubleshooting). The usual causes are a missing Secret, an apply over an existing CR, a leftover HA ConfigMap, or nodepool limits.

## Step 6: Load generator (upsert)

```bash
source mysql-cdc-env.sh
./mysql-data-generator/deploy-data-generator.sh deploy   # StatefulSet mysql-upsert-loadgen, 4 pods
./mysql-data-generator/deploy-data-generator.sh status   # per-pod inst= / avg= / errors=
```

This is batched `INSERT ... ON DUPLICATE KEY UPDATE` from 4 pods. The current
manifest sets `RATE_PER_POD=80000` (a token-bucket ceiling set out of reach on
purpose), `THREADS=8`, `BATCH_ROWS=5000`, `UPSERT_RATIO=0.5`,
`HOT_KEY_RATIO=0.6`, `HOT_KEY_WINDOW=5000000` and `POD_COUNT=4`. Cold keys are
drawn from `[MIN(id), MAX(id)]`, read **once at pod start**. This is the only
generator: the row-at-a-time INSERT/UPDATE/DELETE "mixed" generator was removed on
2026-11-02, so the benchmark has no DELETE events. Details are in
[2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md).

```bash
# after any shrink of the source tables: re-read the id ranges
kubectl rollout restart statefulset/mysql-upsert-loadgen -n emr-flink
# after the snapshot test (which scales it to 0): bring it back
./mysql-data-generator/deploy-data-generator.sh scale 4   # also sets POD_COUNT
```

- **Check:** each pod logs `inst=` / `avg=` / `errors=` lines. Judge the current rate by `inst=` and the failures by `errors=` against `batches=`.
- **Rate:** source-bound at about **52–64K rows/s**, with about **34% batch errors** from lock contention, retried silently (measured on the 2026-11-02 dry run, with 8 pods; 4 pods since then, not measured yet). The `40,000 rows/s` and `5000 x 8` figures printed by the script's help are out of date.
- **Time:** pods ready in about 1–2 min (estimate).
- **If it fails:** `Secret mysql-credentials not found` means you need to export `MYSQL_HOST`, `MYSQL_USER` and `MYSQL_PASSWORD` and re-run. If the source tables regrow after a shrink, the pods were not restarted. A bare `kubectl scale` leaves `POD_COUNT` stale and the hot-key slices overlap, so use `scale N upsert`.

## Step 7: Monitor

```bash
./monitoring/deploy-monitor.sh                  # python:3.11-slim + ConfigMap code, SA starrocks-sa
kubectl logs -f deployment/flink-cdc-monitor -n emr-flink
```

The monitor prints one Paimon-vs-Iceberg table per cycle for the four tables of the
current set: rows/s from commit history, checkpoints, and table stats.
`./monitoring/capture-flink-metrics.sh` writes a point-in-time Flink metrics CSV.
Details are in [3-MONITOR.md](3-MONITOR.md).

- **Check:** the banner shows `Glue DBs: flink_paimon_db_v3r1, flink_iceberg_db_v3r1`, and `rollout status` succeeds.
- **Time:** about 2 min on first boot, because it pip-installs pyiceberg and pyarrow (estimate).
- **If it fails:** a pod that can't assume its role points to the trust policy or the SA annotation. The checkpoint "failed" counter is cumulative since job start ([3-MONITOR.md](3-MONITOR.md#checkpoint-failure-counts-are-cumulative-not-current)).

## Step 8: Query engines

`querybench/run.sh start` brings both engines up when needed, so this step is
optional. Run it to see the engines come up before a test:

```bash
./engine-deploy/deploy-duckdb-cdc.sh up       # Deployment duckdb-warm-cdc: 27 cpu / 114 GiB, 450 GiB disk cache
./engine-deploy/deploy-trino.sh up            # waits until the writers have run 15 min, then Helm release trino: 3 workers x 9 cpu / 38 GiB + 4 cpu coordinator
./engine-deploy/deploy-duckdb-cdc.sh status
./engine-deploy/deploy-trino.sh status
```

**Trino waits for the writers.** `deploy-trino.sh up` (and so `run.sh start`,
when Trino is not up yet) installs only once every RUNNING writer in
`FLINK_JOBS` has been RUNNING for `TRINO_AFTER_WRITERS_S` (900 s = 15 min) since
its current job start, so a restarted writer restarts the clock. The writers'
start-up and first snapshot get the nodes to themselves. Suspended writers are
skipped, a missing one stops it, and it gives up after
`TRINO_WRITER_WAIT_MAX_S` (7200 s). `TRINO_AFTER_WRITERS_S=0` installs at once.

The four arms (`ENGINES` × `FORMATS`) on the v3 set, as known on 2026-11-02:

| Arm | Reads | Status |
|---|---|---|
| `duckdb:iceberg` | `flink_iceberg_db<suffix>`, pinned `metadata.json` copy | works |
| `trino:iceberg` | the same, `FOR VERSION AS OF <snapshot>` | works |
| `duckdb:paimon` | Paimon's Iceberg export, pinned `vN.metadata.json` copy | **fails on v3**: `Manifest ... missing required Avro key-value metadata field 'schema'` (the export's manifests lack the key). Its queries are recorded as `ERROR` and the run continues |
| `trino:paimon` | the export, through the Paimon Glue tables | **unverified** |

Paimon's Iceberg export (`metadata.iceberg.storage = hive-catalog`)
re-registers the **Paimon Glue tables as `table_type=ICEBERG`**, and that entry
is the one `trino:paimon` reads. Athena reads only Iceberg v2, so it cannot
read this set.

- **Check:**
  - DuckDB `/health` shows `"busy": false`, `iceberg_format_version` 3 and the image.
  - Trino shows `3` active workers, version `484`, and lists the set's schemas.
- **Time:** Trino first waits until the writers have run 15 min; then about 5–20 min, including new m5 nodes; the scripts wait up to 20 min (estimate).
- **If it fails:**
  - `serviceaccount starrocks-sa not found` means Step 0 is incomplete.
  - A Trino on another version is refused by `run.sh start`. Run `deploy-trino.sh up`, or set `TRINO_VERSION_CHECK=0`.
  - A DuckDB server for another set or image is redeployed automatically.

## Step 9: Hot test (writers and generator live)

```bash
SUITE=hot ./querybench/run.sh render   # optional dry run: renders the Jobs and every query, no cluster/AWS
SUITE=hot ./querybench/run.sh start
./querybench/run.sh logs               # the runner (cdc-qb-runner)
./querybench/run.sh logs watch         # the watcher (cdc-qb-watch)
./querybench/run.sh status             # writers, generator, engines, Jobs
```

Preconditions: both writers are `RUNNING`, the generator is running, and no other
`cdc-qb-runner` or throughput `qb-runner` is active (`FORCE=1` overrides).

`start` brings up the engines and clears their caches, then starts the runner
Job and the watcher Job in the cluster. The laptop can disconnect after that.
The runner:

1. Checks the writers' stability 10 times, once a minute.
2. Takes a 5 min window ending 5 min ago and waits for commits after it.
3. Pins every table in both formats.
4. Runs a warm-up pass and then `RUNS=3` timed passes, interleaving DuckDB and Trino. It re-pins before each pass.
5. Uploads the results to `s3://$BUCKET_NAME/querybench-results/cdc-session-v3r1<SESSION_SUFFIX>/<STAMP>-hot/`.

**The watcher tears down only after a completed test.** That means
`status: runner-exit=0`. It then scales the generator to 0, suspends both
writers, scales the monitor to 0, deletes DuckDB, and scales Trino to 0. On any
failure it **leaves everything running** and logs why, the last errors, and
what is still up (`status: qb-watch-failed-left-running`). Fix the problem and
re-run `start`, or tear down with Step 12.

Before a second hot test after a completed one, resume the writers (Step 5),
the generator (`scale 8 upsert`) and the monitor
(`kubectl scale deployment/flink-cdc-monitor -n emr-flink --replicas=1`).

- **Check:** the runner logs `status: results-uploaded ...`, and the watcher logs `status: qb-watch-done`.
- **Time:** about 45–90 min end to end (estimate). The stability check alone takes 10 min, the window lags 5 min, and the watcher polls every 10 min.
- **If it fails:** read `./querybench/run.sh logs`. Any setup error in `start` stops it before a Job is created ("no query Job left running"). See [querybench/README.md](querybench/README.md) for every setting.

## Step 10: Snapshot test (compaction gate)

```bash
SUITE=snapshot ./querybench/run.sh start
./querybench/run.sh logs
kubectl get jobs -n emr-flink | grep iceberg-compact
```

In order, `start`:

1. Scales the generator to 0.
2. Waits `WRITER_DRAIN_S=180` s so the writers commit the binlog tail.
3. **Suspends both writers** and waits until no TaskManager is left.
4. Resolves `SNAPSHOT_DT`, the newest closed date in both formats' fact tables.
5. Because `COMPACT_BEFORE_SNAPSHOT=true`, **submits one Iceberg compaction Job per `COMPACT_TABLES` table** (`iceberg-compact-{customers,products,orders,order-items}`). It runs them in the background from the laptop; the submit logs go to `bench-results/iceberg-compact-<t>-<STAMP>.submit.log`, but the Jobs themselves run in the cluster.
6. Brings the engines up and starts the runner and watcher.

Each compaction Job runs `rewrite_data_files` and then
`expire_snapshots` (older than 12 h, keep 5), Paimon's retention rule.

The runner then waits on the **compaction gate**. Every Job must have
succeeded, and each table's current snapshot must be that Job's `replace`
commit. The gate waits up to `COMPACT_GATE_TIMEOUT_S=21600`. After that the
runner pins every table, does a cold start (DuckDB restarted, Trino caches
cleared), and runs the passes. `snapshot-<STAMP>.pins` records the gate times
and the pinned snapshot ids. Paimon gets no extra step, because it compacts
inside its writer.

- **Check:**
  - All four `iceberg-compact-*` Jobs show `Complete`.
  - The runner logs `gate_compaction_finished_at=` and then `status: results-uploaded ...<STAMP>-snapshot/`.
- **Time:** compaction depends on the equality-delete backlog, from minutes to hours. It has not been measured on the 37.5M-row set (estimate). The passes take about 30–60 min (estimate).
- **If it fails:**
  - `compaction Job ... FAILED`: read `kubectl logs job/iceberg-compact-<t> -n emr-flink`. For a quick classpath, Glue and IRSA check, run `DRY_RUN=true ICEBERG_TABLE=<t> ./iceberg-compaction/run-compaction.sh` (about 1 min).
  - `current snapshot is ... not the compaction's replace commit`: something wrote after the compaction, so recompact. `ICEBERG_REQUIRE_COMPACTION=0` measures the tables as they are, and the run records that.
  - Outside us-west-2, `SPARK_IMAGE` must be set.

## Step 11: Summary

```bash
./querybench/run.sh summary     # newest hot + snapshot run of the session
#   HOT_STAMP=... SNAPSHOT_STAMP=... pick other runs; SUMMARY_DIR=<dir> reads local results instead of S3
```

This writes `bench-results/QUERYBENCH-SUMMARY-<YYYYMMDD>-v3r1<SESSION_SUFFIX>.md`
with the bucket and account redacted. `bench-results/` is git-ignored, so use
`git add -f` to commit the file. A suite in which two engines on the same
format return different rows is marked **INVALID**.

- **Check:** the file exists and lists both suites.
- **Time:** about 1 min (estimate).
- **If it fails:** `no runs under querybench-results/...` means the wrong set or `SESSION_SUFFIX`, or the runner never uploaded.

## Step 12: Stop / teardown

```bash
# The same teardown the watcher does after a completed test (use it after a failed one):
SUITE=hot ./querybench/run.sh stop
#   generator -> 0, writers suspended, monitor -> 0, DuckDB deleted, Trino -> 0 (release kept)
#   STOP_GENERATOR=0 / SUSPEND_WRITERS=0 / STOP_DUCKDB=0 / STOP_TRINO=0 keep one of them
```

To remove everything (the S3 data and the Glue tables stay):

```bash
./build-deploy-generic.sh cleanup        # asks "yes"; deletes both FlinkDeployments via the *-deployed.yaml files
kubectl get configmap -n emr-flink -o name | grep -E 'flink-cdc-(paimon|iceberg)-.*config-map' \
  | xargs -r kubectl delete -n emr-flink # leftover HA ConfigMaps
./mysql-data-generator/deploy-data-generator.sh stop
kubectl delete -f monitoring/monitor-deployment-deployed.yaml
./engine-deploy/deploy-duckdb-cdc.sh down
./engine-deploy/deploy-trino.sh down     # helm uninstall + cache PVCs
for t in customers products orders order_items; do
  JOB=iceberg-compact-${t//_/-} ./iceberg-compaction/run-compaction.sh clean
done
```

- **Check:** `./querybench/run.sh status` shows no writers, generator `0/0`, DuckDB not answering, and 0 Trino workers. `kubectl get pods -n emr-flink` lists only the operator, plus anything unrelated.
- **Time:** about 2–5 min (estimate).
- **If it fails:** a suspended writer that never stops makes `run.sh stop` time out after 600 s. Delete the FlinkDeployment instead.

## Step 13: Clean up old table sets

The data of an old set stays in Glue and S3 until you remove it. Its settings
can never be reused (Step 4). Set `OLD` to its suffix, e.g. `_v3`. **Never** use
the current one. `OLD=` (empty) means the original unsuffixed v2 set.

```bash
OLD=_v3
# 1. Nothing uses it: no FlinkDeployment writes it, and no summary still needs its results
kubectl get flinkdeployment -n emr-flink -o yaml | grep -A1 GLUE_DATABASE
# 2. Keep the table definitions
for db in flink_paimon_db$OLD flink_iceberg_db$OLD; do aws glue get-tables --database-name $db > glue-backup-$db.json; done
# 3. Glue (drops the tables with the databases)
aws glue delete-database --name flink_paimon_db$OLD
aws glue delete-database --name flink_iceberg_db$OLD
# 4. S3. Keep the TRAILING SLASH: without it, ".../cdc-paimon" also matches cdc-paimon_v3r1/
for p in paimon-warehouse/flink_paimon_db$OLD.db/ paimon-warehouse/iceberg/flink_paimon_db$OLD/ \
         iceberg-warehouse/flink_iceberg_db$OLD.db/ \
         flink/cdc-paimon$OLD/ flink/cdc-iceberg$OLD/ flink/logs/cdc-paimon$OLD/ flink/logs/cdc-iceberg$OLD/; do
  aws s3 rm --recursive "s3://$BUCKET_NAME/$p"
done
#    optional: querybench-pins/cdc-session${OLD//_/-}*/ and querybench-results/cdc-session${OLD//_/-}*/
#    (never with OLD= empty: "cdc-session*" then matches every session)
```

**The bucket is versioned.** `aws s3 rm` only writes delete markers, and the
bytes stay as noncurrent versions until a lifecycle rule expires them. To
reclaim the space, add a rule for each deleted prefix (or one for the whole
bucket). Nothing in this pipeline reads noncurrent versions.

```bash
aws s3api get-bucket-versioning --bucket $BUCKET_NAME
aws s3api get-bucket-lifecycle-configuration --bucket $BUCKET_NAME > lifecycle-current.json   # merge into this:
# put-bucket-lifecycle-configuration REPLACES the whole configuration, so include the existing rules.
cat > lifecycle.json <<EOF
{"Rules": [{
  "ID": "expire-noncurrent-iceberg-warehouse-flink_iceberg_db$OLD", "Status": "Enabled",
  "Filter": {"Prefix": "iceberg-warehouse/flink_iceberg_db$OLD.db/"},
  "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
  "Expiration": {"ExpiredObjectDeleteMarker": true},
  "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}
}]}
EOF
aws s3api put-bucket-lifecycle-configuration --bucket $BUCKET_NAME --lifecycle-configuration file://lifecycle.json
```

- **Check:**
  - `aws glue get-database --name flink_iceberg_db$OLD` returns `EntityNotFoundException`.
  - `aws s3 ls s3://$BUCKET_NAME/iceberg-warehouse/flink_iceberg_db$OLD.db/` is empty.
  - `aws s3api list-object-versions --bucket $BUCKET_NAME --prefix <p> --max-items 5` eventually returns nothing.
- **Time:** the Glue deletes take seconds. `s3 rm` takes minutes to an hour, depending on object count. Lifecycle expiry is asynchronous and usually takes 1–2 days (estimate).
- **If it fails:** if the space is not reclaimed, the bucket has no rule for those prefixes. Check that the rule covers them and that `Status` is `Enabled`.

---

# Reference

## How rendering works

`build-deploy-generic.sh render` replaces, with `sed`, exactly these
placeholders: `AWS_REGION` (default `us-west-2`), `AWS_ACCOUNT_ID`,
`BUCKET_NAME`, `EMR_EXECUTION_ROLE_ARN`, `EMR_VERSION` (7.14.0), `IMAGE_TAG`
(the same as `EMR_VERSION`), `ICEBERG_FORMAT_VERSION` (2|3, default 3),
`CDC_DB_SUFFIX`, `PAIMON_DV_BITMAP64`, `MYSQL_HOST`, `MYSQL_USER`,
`MYSQL_ENV_SECRET_NAME` (`flink-cdc-mysql-env`), `JM_NODEPOOL`
(`driver-nodepool`) and `TM_NODEPOOL` (`executor-memorynodepool`).
`PAIMON_DV_BITMAP64` is derived, not a setting: it is `true` exactly when the
format is 3, because the v3 DV export needs 64-bit DVs. `MYSQL_PASSWORD` is
never substituted: the pods pull it from the Secret through `envFrom`, so no
credential lands in a file.

## Flink job configuration (both jobs, identical)

| Setting | Value |
|---|---|
| JobManager | 1 replica, HA in `s3://<bucket>/flink/cdc-<format><suffix>/ha/`, 8 Gi / 4 cpu, `driver-nodepool` (on-demand) |
| TaskManager | 32 Gi / 8 cpu, 4 slots, `executor-memorynodepool` (on-demand). The pods carry `karpenter.sh/do-not-disrupt: "true"` |
| Parallelism | 32 (`job.parallelism` and `--parallelism 32`), `pipeline.max-parallelism: 128` |
| Chaining | `pipeline.operator-chaining.enabled: true`, `pipeline.object-reuse: true` |
| State | `hashmap` on the task heap (`taskmanager.memory.managed.fraction: 0.1`), checkpoints and savepoints in `s3p://<bucket>/flink/cdc-<format><suffix>/` |
| Checkpointing | every 60 s, 600 s timeout, 30 s minimum pause, `tolerable-failed-checkpoints: 10` |
| Failover | `failover-strategy: full`, `restart-strategy: exponential-delay`, `jobmanager.scheduler: default`. The autoscaler is off |
| MySQL server-ids | Paimon 5401–5552, Iceberg 6401–6552 (32 per source). They must never overlap |

Parallelism, checkpointing, state backend and sizing are fair-comparison
variables: change them in both manifests together.

## Paired table settings (`03-paimon-sinks.sql` ↔ `03-iceberg-sinks.sql`)

| Concern | Paimon | Iceberg |
|---|---|---|
| Format version | `metadata.iceberg.format-version = ${ICEBERG_FORMAT_VERSION}` | `format-version = ${ICEBERG_FORMAT_VERSION}` |
| Deletes | deletion vectors, `deletion-vectors.bitmap64 = ${PAIMON_DV_BITMAP64}` | merge-on-read: equality deletes, plus DVs on v3 |
| Data files | Parquet zstd level 1; `target-file-size` 64 MB (dimension tables) / 128 MB (fact tables) | Parquet zstd level 1; `write.target-file-size-bytes` 64 / 128 MB |
| Row group | `parquet.block.size` 8 MB (dimension) / 16 MB (fact) | `write.parquet.row-group-size-bytes` 8 / 16 MB |
| Snapshot retention | `snapshot.time-retained 12h`, `num-retained.min 5` (`max 1000`) | `history.expire.max-snapshot-age-ms` 12 h, `min-snapshots-to-keep 5` |
| Metadata files | export `previous-versions-max 3` + `delete-after-commit` | `write.metadata.previous-versions-max 3` + `delete-after-commit` |
| Manifests | `manifest.merge-min-count 5`, `manifest.target-file-size 8mb`; export compaction min 5 / max 50 | `commit.manifest.min-count-to-merge 5`, `commit.manifest.target-size-bytes 8 MB` |
| Column stats | `metadata.stats-mode truncate(16)` | `write.metadata.metrics.default truncate(16)` |

**Asymmetries kept on purpose:**

- **Write layout:** Paimon has 32 bucket writers per table. Iceberg's `write.distribution-mode = hash` gives one writer per date partition.
- **Maintenance:** Paimon compacts and expires inline, in the writer. Iceberg relies on the external compaction Job (`iceberg-compaction/`), which also runs `expire_snapshots` (12 h, keep 5).
- **Changelog:** Paimon `changelog-producer = none`, because nothing reads a changelog and Iceberg writes none.
- **Catalog access:** Paimon uses the Hive metastore client on Glue (`aws.glue.enabled: "true"`, the one `flinkConfiguration` difference). Iceberg uses its own `GlueCatalog` with `S3FileIO`.

<a id="step-4--olap-query-layer-athena"></a>
## Athena

Athena is not part of the current pipeline. Athena (engine v3) reads Iceberg v2 only
([AWS Iceberg V3 support table](https://docs.aws.amazon.com/AmazonS3/latest/userguide/working-with-apache-iceberg-v3.html)),
so it cannot read the v3 set. A v2 set (`ICEBERG_FORMAT_VERSION=2` with its
own `CDC_DB_SUFFIX`) can be queried from Athena as
`SELECT ... FROM flink_iceberg_db<suffix>.<table>`. The earlier recipe that
registered Paimon's v2 export in a separate Glue database for Athena (verified
2026-09-28 on the v2 set) is in this file's git history.

## CDC behavior

- **Phase 1, full snapshot:** chunked primary-key-range reads, parallel, no table locks. A consistent snapshot is written as inserts. Every new table set repeats it.
- **Phase 2, binlog:** INSERT, UPDATE and DELETE are applied as upserts on the primary keys. Exactly-once comes from the 60 s checkpoints, and data is visible after a commit, so roughly one checkpoint behind.

## MySQL schema notes

- `order_items.subtotal` is a **generated column** (`quantity * unit_price`). Never INSERT into it.
- `order_date`, `created_at` and `updated_at` default to `CURRENT_TIMESTAMP`.
- `customers.email` is UNIQUE. The generators use UUID-based emails.
- The partition columns differ by format: Paimon `order_date_str` / `created_date_str`, Iceberg `order_dt` / `created_dt`. Both are `DATE_FORMAT()` strings and are part of the primary key.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `NoSuchFileException: /tmp/pyflink/.../flink-cdc-executor.py` | Either the `jarURI` object is missing (re-run `build`), or the CR was `kubectl apply`'d over an existing one (EMR stages `jarURI` on CREATE only). Delete the FlinkDeployment and create it again |
| JobManager `CreateContainerConfigError` | The `flink-cdc-mysql-env` Secret is missing (Step 0) |
| New set's JobManager recovers an old job graph | Leftover HA ConfigMaps `flink-cdc-{paimon,iceberg}-*config-map`. Delete them (Step 4) |
| Paimon: `NoAuthWithAWSException` on the warehouse | The catalog lacks `s3.aws.credentials.provider = ...WebIdentityTokenFileCredentialsProvider`. paimon-s3 ignores `hadoop-conf-dir` (fixed in `01-catalog-setup.sql`) |
| Iceberg: `NoClassDefFoundError .../hc/core5/http/ClassicHttpRequest` | Iceberg 1.12's default `apache` HTTP client isn't in EMR's SDK bundle. Use `http-client.type = urlconnection` plus the url-connection-client jar (both are in place) |
| New table options have no effect | `CREATE TABLE IF NOT EXISTS` is a no-op on an existing table. Use a new `CDC_DB_SUFFIX` |
| TaskManagers die with `exitCode=255`, `NoResourceAvailableException` | The image is single-arch on a mixed-arch pool. Rebuild with both `PLATFORMS` |
| Pods Pending, `exceed limits for nodepool` | Karpenter counts provisioned capacity against the NodePool `limits.cpu`. Raise it, e.g. `kubectl patch nodepool driver-nodepool --type=merge -p '{"spec":{"limits":{"cpu":256}}}'` (unquoted number) |
| Job cycles RUNNING → RESTARTING for no visible reason | Karpenter consolidation evicting a TaskManager (`failover-strategy: full` restarts all). Check that `karpenter.sh/do-not-disrupt` is on the running TaskManager pods |
| `AWSGlueClientFactory - No region info found ... us-east-1` + `401` on the JobManager | Noise. The job reaches RUNNING anyway. Look elsewhere for a restart cause |
| Operator CRD delete hangs | A CR is stuck on its finalizer. `kubectl patch flinkdeployment <n> -n <ns> --type=merge -p '{"metadata":{"finalizers":null}}'` |
| Generator rate low, no errors printed | MySQL 1213 / 1205 errors are retried silently. Read `errors=` against `batches=`. About 34% batch errors is the measured norm on this source |
| Source tables regrow after a shrink | The generator pods still use the old `[MIN, MAX]` id range. Restart the StatefulSet |
| `duckdb:paimon` errors `missing required Avro key-value metadata field 'schema'` | A known Paimon 2.1 v3 export issue. The arm records `ERROR` and the run continues |
| A Glue `metadata_location` 404s mid-scan | Paimon's export and Iceberg keep only 3 previous metadata files. The benchmark reads pinned copies, so don't read live Glue paths during a test |
| `run.sh start` refuses: Trino version | Run `./engine-deploy/deploy-trino.sh up` (Trino 484), or set `TRINO_VERSION_CHECK=0` |
| Snapshot test refused at the compaction gate | A Job failed, is missing, or a write followed it. See Step 10 |
| After a failed test everything is still running | By design, the watcher only tears down after `runner-exit=0`. Fix it and re-run, or `SUITE=hot ./querybench/run.sh stop` |
| Monitor pod stuck with no logs | First boot pip-installs pyiceberg and pyarrow (about 2 min) |
| Monitor or engines can't assume role | The role's trust policy must cover `system:serviceaccount:emr-flink:<sa>`, and `starrocks-sa` must carry the role-arn annotation |

## Resources created

- 1 RDS MySQL 8.0 instance (binlog, CDC user) and 2 Secrets (`flink-cdc-mysql-env`, `mysql-credentials`)
- ECR `emr-flink-cdc-paimon:7.14.0` and `duckdb-cdc-server:2.0.0`, both multi-arch
- EMR Flink operator 7.14.0 (Helm), the `ebs-sc` StorageClass, and Kyverno with the `flink-cdc-single-az` ClusterPolicy
- 2 FlinkDeployments (`flink-cdc-paimon`, `flink-cdc-iceberg`), each 1 JobManager + 8 TaskManagers
- StatefulSet `mysql-upsert-loadgen` (4 pods)
- Deployment `flink-cdc-monitor`
- Deployment `duckdb-warm-cdc`, Helm release `trino` (with per-pod gp3-fast cache volumes), Jobs `cdc-qb-runner` / `cdc-qb-watch` / `iceberg-compact-*`
- Per table set: 2 Glue DBs × 4 tables; S3 warehouse, export, Flink state and log prefixes; `querybench-pins/` and `querybench-results/` session prefixes
