# Realtime Lakehouse via Flink CDC (FlinkSQL approach)

MySQL RDS → Flink CDC (EMR on EKS) → **Apache Paimon** and **Apache Iceberg** on S3
(AWS Glue catalog), written side by side from the same binlog, then read by
**DuckDB** and **Trino** in a hot and a snapshot query benchmark.

This file is the end-to-end runbook (state as of **2026-11-04**; last full run
`_v3r3`, summary `bench-results/QUERYBENCH-SUMMARY-20261103-v3r3.md`). The
detailed guides it links to:
[0-MYSQL-SETUP-GUIDE.md](0-MYSQL-SETUP-GUIDE.md) ·
[1-BUILD-DEPLOY-FLINK-APP-GUIDE.md](1-BUILD-DEPLOY-FLINK-APP-GUIDE.md) ·
[2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md) ·
[3-MONITOR.md](3-MONITOR.md) ·
[engine-deploy/README.md](engine-deploy/README.md) ·
[engine-deploy/duckdb-image/README.md](engine-deploy/duckdb-image/README.md) ·
[iceberg-compaction/README.md](iceberg-compaction/README.md) ·
[querybench/README.md](querybench/README.md) ·
[PAIMON-VS-ICEBERG.md](PAIMON-VS-ICEBERG.md)

## Quick start: what to ask your AI assistant

Clone the repo, `cd cdc-pipeline`, start the AI assistant, and ask in plain language, for example:

- "Follow the README runbook on my existing EKS cluster `demo`, using the table set in `.bench-env`"
- "Start a new table set `_v3r4` and deploy both writers in one apply, with Iceberg compaction"
- "Start the upsert generator and report the achieved rate"
- "Run the hot test, then the snapshot test, and write the summary"
- "Tear the run down and clean up the old table set `_v3r3` (after moving to `_v3r4`)"

## Stack

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
| Engine size | Tripled on 2026-11-02, both about **81 cpu / 342 GiB**: DuckDB one pod of 81 cpu / 342 GiB on an m5.24xlarge; Trino 9 workers × 9 cpu / 38 GiB (one per m5.4xlarge) + a 4 cpu coordinator. Total cache stays about 450 GiB (DuckDB 450 GiB disk; Trino 9 × 56 Gi × 90%). Before: DuckDB 27 cpu / 114 GiB, Trino 3 workers |
| Iceberg compaction | One continuous FlinkDeployment `iceberg-maintain` ([iceberg-compaction/](iceberg-compaction/README.md): `maintain.py`, Iceberg 1.12's `TableMaintenance`): rewrite + snapshot expiry every 10 writer commits, started with the writers. The Spark job (2026-11-02) and the Java hourly/merge app (2026-11-03) were removed |
| Source | RDS MySQL **8.0.45** on **db.r6i.4xlarge** (was db.m5.2xlarge), 500 GB gp3, 64,000 IOPS, buffer pool 92 GiB. `ecommerce` DB shrunk to its newest ~10% on 2026-11-02: **491.0M → 49.36M rows** |

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ MySQL RDS 8.0.45, db.r6i.4xlarge (ecommerce: customers, products,    │
│ orders, order_items), binlog ROW, CDC user REPLICATION SLAVE/CLIENT  │
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
   + Iceberg export under                + FlinkDeployment iceberg-maintain
     paimon-warehouse/iceberg/             (rewrite + expiry every 10 commits)
               │                                     │
               └───────────────┬─────────────────────┘
                               ▼
        ┌──────────────────────────────────────────────┐
        │ Query benchmark (querybench/): four arms     │
        │ DuckDB 2.0.0 warm     │ Trino 484            │
        │ 81 cpu / 342 GiB      │ 9 wkr x 9 cpu/38 GiB │
        │ × Iceberg native      │ × Paimon native      │
        └──────────────────────────────────────────────┘
   monitor: flink-cdc-monitor (Flink REST + table stats, both formats in
            parallel; reports to s3://<bucket>/querybench-results/.../monitor/)
```

## Files

```
cdc-pipeline/
├── .bench-env                  session settings: table set, warehouses, engine versions (git)
├── .bench-env.local            account values (git-ignored, you create it)
├── lib/common.sh               loads .bench-env, keeps command-line overrides, derives table-set names
├── lib/querybench.sh           shared query-benchmark functions (arms, pins, gates, stop/suspend)
├── ingest-deploy/               the Flink CDC writers (run its script from cdc-pipeline/ or anywhere)
│   ├── build-deploy-generic.sh build | render | deploy | all | cleanup | monitor
│   ├── flink-cdc-executor.py   PyFlink executor: runs the SQL files in order (the jobs' jarURI)
│   ├── flink-cdc-{paimon,iceberg}-sql.yaml   FlinkDeployment templates (${VAR}; renders next to them)
│   ├── docker/Dockerfile.cdc-paimon          Flink image (CDC + Paimon + Iceberg jars)
│   ├── sql-scripts/
│   │   ├── common/02-cdc-sources.sql         MySQL CDC source tables
│   │   ├── paimon/01-catalog-setup.sql, 03-paimon-sinks.sql, 04-cdc-pipelines.sql
│   │   └── iceberg/01-catalog-setup.sql, 03-iceberg-sinks.sql, 04-cdc-pipelines.sql
│   └── k8s/                    ebs-sc StorageClass, Kyverno single-AZ policy
├── mysql-data-generator/       deploy-data-generator.sh, mysql-upsert-loadgen.yaml
├── monitoring/                 deploy-monitor.sh, monitor code, capture-flink-metrics.sh
├── engine-deploy/              deploy-duckdb-cdc.sh, deploy-trino.sh, duckdb-image/, trino-image/ (README.md there)
├── iceberg-compaction/         Iceberg compaction: maintain.py / maintain.yaml / run-maintain.sh (FlinkDeployment iceberg-maintain)
├── querybench/                 run.sh (start|render|status|logs|summary|stop), suites, ingest_stats.py, README.md
└── bench-results/              local output (git-ignored)
```

`*-deployed.yaml` files are renderings with values substituted. They are
git-ignored, and every run overwrites them. Edit the templates instead.

---

# Runbook

Run every command from `cdc-pipeline/`. Each step says what it does, how to
check it, how long it takes and the usual failure. **(measured)** times are
from the 2026-11-03 `_v3r3` run; **(estimate)** otherwise.

**The whole run at a glance**

| # | Step | Command (short) | Time |
|---|---|---|---|
| 0 | Prerequisites (once per cluster) | ServiceAccounts, Secret `mysql-credentials` | minutes |
| 1 | Settings | `.bench-env`, `.bench-env.local` | 1 min |
| 2 | EMR Flink operator 7.14.0 (once) | `helm install ...` | ~5 min |
| 3 | Images (after a Dockerfile/SQL/executor change) | `./ingest-deploy/build-deploy-generic.sh build` | 20–40 min |
| 4 | A fresh table set | new `CDC_DB_SUFFIX`, clear leftovers | ~1 min |
| 5 | Writers + Iceberg compaction | `render`, one `kubectl apply`, `run-maintain.sh up` | ~8 min to RUNNING |
| 6 | Load generator | `deploy-data-generator.sh deploy` | 1–2 min |
| 7 | Monitor | `monitoring/deploy-monitor.sh` (or let Step 9 do it) | 1–2 min |
| 8 | Query engines (optional, Step 9 does it) | `deploy-duckdb-cdc.sh up`, `deploy-trino.sh up` | 5–20 min |
| 9 | Hot test | `SUITE=hot ./querybench/run.sh start` | ~35 min after the 15 min writer wait |
| 10 | Snapshot test | `SUITE=snapshot ./querybench/run.sh start` | ~50 min |
| 11 | Summary | `./querybench/run.sh summary` | a few min |
| 12 | Teardown | `SUITE=hot ./querybench/run.sh stop` | 2–5 min |
| 13 | Clean up the old table set | Glue + S3 removal | minutes–1 h |

## Step 0: Prerequisites

| Item | Requirement |
|---|---|
| EKS cluster | `kubectl` points at it. Karpenter NodePools `driver-nodepool` (on-demand: JobManagers, generator, DuckDB, Trino, runner) and `executor-memorynodepool` (TaskManagers, compaction). The TaskManager pool mixes x86 and Graviton, so every image must be multi-arch |
| `driver-nodepool` sizes | Must allow `12xlarge`, `16xlarge` and `24xlarge` (the 81-cpu DuckDB pod needs an m5.24xlarge), and `limits.cpu` must cover DuckDB + Trino (512). A cluster-side patch, **not in the repo**; check below |
| Namespace | `emr-flink`, set up for EMR on EKS. The execution role (`EMR_EXECUTION_ROLE_ARN`) must trust `system:serviceaccount:emr-flink:*` through the cluster's OIDC provider, with Glue and S3 read/write on the bucket |
| cert-manager | Needed by the operator chart ([1-BUILD guide, step 0](1-BUILD-DEPLOY-FLINK-APP-GUIDE.md)) |
| IRSA ServiceAccount `starrocks-sa` | **Historical name**, still the IRSA account of the monitor, DuckDB, Trino, the Iceberg compaction and the query Jobs; annotated with the EMR execution role |
| IRSA ServiceAccount `image-builder-sa` | The Kaniko build pods (Flink and DuckDB images): push to ECR, read `s3://$BUCKET_NAME/flink/build/` |
| ECR | `emr-flink-cdc-paimon` and `duckdb-cdc-server` are created by the build scripts. No lifecycle rule that expires untagged images: the per-arch children of a multi-arch tag are untagged |
| S3 | `BUCKET_NAME`: warehouses, Flink state and logs, query pins, results, monitor reports. Versioned (Step 13) |
| RDS MySQL 8.0 | As in [0-MYSQL-SETUP-GUIDE.md](0-MYSQL-SETUP-GUIDE.md): in the EKS VPC, binlog `ROW`, `cdcuser` with `REPLICATION SLAVE` / `REPLICATION CLIENT`, schema `ecommerce`. Now **db.r6i.4xlarge**, 8.0.45, 500 GB gp3 at 64,000 IOPS, buffer pool 92 GiB |
| Secret | **One**, `mysql-credentials` (keys `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`), shared by the Flink jobs (`envFrom`, hence env-var key names) and the generator (`secretKeyRef`). You create it below. **No file in the repo or on the laptop holds the password** |
| Laptop | `kubectl`, `helm`, AWS CLI, `python3`, `uv` (Step 11's ingest stats), and this repo's `throughput-benchmark/` directory (the engine scripts apply its `gp3-fast` StorageClass) |

```bash
# ServiceAccounts (skip any that exist)
kubectl create serviceaccount starrocks-sa -n emr-flink
kubectl annotate serviceaccount starrocks-sa -n emr-flink eks.amazonaws.com/role-arn="$EMR_EXECUTION_ROLE_ARN"
kubectl create serviceaccount image-builder-sa -n emr-flink
kubectl annotate serviceaccount image-builder-sa -n emr-flink \
  eks.amazonaws.com/role-arn=<role that can push to ECR and read the S3 build context>

# The one MySQL Secret (Flink jobs + generator), from a prompt: never a file or shell history
source .bench-env                                   # MYSQL_HOST (.bench-env.local), MYSQL_USER
read -rs -p 'cdcuser password: ' MYSQL_PASSWORD; echo
kubectl create secret generic mysql-credentials -n emr-flink \
  --from-literal=MYSQL_HOST="$MYSQL_HOST" --from-literal=MYSQL_USER="$MYSQL_USER" \
  --from-literal=MYSQL_PASSWORD="$MYSQL_PASSWORD" --dry-run=client -o yaml | kubectl apply -f -
unset MYSQL_PASSWORD

# Checks
kubectl get sa starrocks-sa image-builder-sa -n emr-flink -o yaml | grep role-arn      # two lines
kubectl get secret mysql-credentials -n emr-flink   # keys MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD
kubectl get nodepool driver-nodepool \
  -o jsonpath='{range .spec.template.spec.requirements[*]}{.key}={.values}{"\n"}{end}{.spec.limits}{"\n"}' \
  | grep -E 'instance-size|cpu'                                                      # lists 24xlarge
aws rds describe-db-instances --db-instance-identifier flink-cdc-mysql-8-0 \
  --query 'DBInstances[0].[DBInstanceClass,EngineVersion,AllocatedStorage,Iops]' --output text
```

To **rotate** the CDC password: `ALTER USER 'cdcuser'@'%'` from an in-cluster
`mysql:8.0` pod that takes both passwords only through `secretKeyRef`, then
update `mysql-credentials`' `MYSQL_PASSWORD`, then delete + create the writers
and restart the generator. Never print the value; the Secrets are the only place it lives.

- **If it fails:** a JobManager in `CreateContainerConfigError` = `mysql-credentials` missing (only `kubectl describe pod` says so). `Access denied for user 'cdcuser'` in the JobManager = the Secret does not hold cdcuser's current password. A pod that cannot assume its role = the trust policy does not cover the SA. DuckDB Pending = `driver-nodepool` lacks `24xlarge` or cpu.

## Step 1: Settings (`.bench-env`, `.bench-env.local`)

| File | In git | Holds |
|---|---|---|
| `.bench-env` | yes | `NAMESPACE`, `ICEBERG_FORMAT_VERSION=3`, **`CDC_DB_SUFFIX`** (the table set), `SESSION_SUFFIX`, warehouses, `FLINK_JOBS`, Trino/DuckDB versions and sizes, `MYSQL_PORT` / `MYSQL_DATABASE` / `MYSQL_USER` / `MYSQL_ENV_SECRET_NAME`, `GENERATOR_RESOURCES`, the Paimon write and S3A upload settings, `HOT_WINDOW_SECONDS=30` |
| `.bench-env.local` | no | `AWS_REGION`, `AWS_ACCOUNT_ID`, `BUCKET_NAME`, `EMR_EXECUTION_ROLE_ARN`, **`MYSQL_HOST`** (the RDS endpoint). `.bench-env` sources it (fallback: `../throughput-benchmark/.bench-env.local`) |

```bash
cat > .bench-env.local <<'EOF'
export AWS_REGION=<region>
export AWS_ACCOUNT_ID=<account-id>
export BUCKET_NAME=emr-on-eks-test-<account-id>-<region>
export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::<account-id>:role/emr-on-eks-test-execution-role
export MYSQL_HOST=<rds-endpoint>
EOF
```

Settings worth knowing (all in `.bench-env`):

| Setting | Value | Meaning |
|---|---|---|
| `CDC_DB_SUFFIX` | e.g. `_v3r4` | the table set: Glue DBs `flink_{paimon,iceberg}_db<suffix>`, Flink state paths, result prefixes (Step 4) |
| `PAIMON_WRITE_ONLY` | `false` | Paimon `INSERT` hint; `true` turns off Paimon's inline compaction **and** snapshot expiry. Keep `false` for the comparison |
| `PAIMON_WRITE_BUFFER_SPILLABLE` / `PAIMON_WRITE_BUFFER_SIZE` | `true` / unset (64mb dimension, 128mb fact = Iceberg's target file sizes) | Paimon `INSERT` hints |
| `S3A_MULTIPART_SIZE` / `S3A_MULTIPART_THRESHOLD` / `S3A_ACTIVE_BLOCKS` | `16M` / `16M` / `6` | Paimon's S3A upload shape; Iceberg's S3FileIO gets the derived equivalent (16 MiB parts, 24 threads) |
| `TRINO_WORKERS` / `DUCKDB_CPU` … | `9` / `81` … | engine size (about 81 cpu / 342 GiB each); the script defaults are smaller, these lines make the engines big |

Who reads what: `querybench/run.sh`, `engine-deploy/*.sh`, `iceberg-compaction/run-maintain.sh`
and the DuckDB image build read `.bench-env` through `lib/common.sh` (a value on
the command line wins, with an `overrides` line). `ingest-deploy/build-deploy-generic.sh`
reads the **environment**, so `source .bench-env` first (it reads only
`CDC_DB_SUFFIX` itself when that is unset). `mysql-data-generator/deploy-data-generator.sh`
sources `.bench-env` for `MYSQL_HOST` / `MYSQL_USER`; `monitoring/deploy-monitor.sh`
reads `CDC_DB_SUFFIX` from it.

- **Check (offline):** `bash -c 'source lib/common.sh; echo $ICEBERG_FORMAT_VERSION $PAIMON_DB $ICEBERG_DB $QB_RESULTS_PREFIX'` prints e.g. `3 flink_paimon_db_v3r4 flink_iceberg_db_v3r4 querybench-results/cdc-session-v3r4`.
- **If it fails:** `CDC_DB_SUFFIX must be empty or like _v3` = the suffix must match `^(_[a-z0-9]+)?$`.

## Step 2: EMR Flink operator 7.14.0

A fresh install: [1-BUILD-DEPLOY-FLINK-APP-GUIDE.md step 0](1-BUILD-DEPLOY-FLINK-APP-GUIDE.md).
An upgrade cannot use `helm upgrade` (it never updates CRDs): uninstall, delete the CRDs, install.

```bash
helm list -n emr-flink
kubectl delete flinkdeployment --all -n emr-flink --ignore-not-found    # deleting the CRDs would anyway
helm uninstall flink-kubernetes-operator -n emr-flink
kubectl delete crd flinkdeployments.flink.apache.org flinksessionjobs.flink.apache.org
#   hangs = a CR stuck on its finalizer:
#   kubectl patch flinkdeployment <name> -n emr-flink --type=merge -p '{"metadata":{"finalizers":null}}'
helm install flink-kubernetes-operator oci://public.ecr.aws/emr-on-eks/flink-kubernetes-operator \
  --version 7.14.0 -n emr-flink --set emrContainers.awsRegion=${AWS_REGION:-us-west-2}
kubectl wait deployment flink-kubernetes-operator -n emr-flink --for condition=Available=True --timeout=120s
```

- **Check:** `helm list -n emr-flink` shows `flink-kubernetes-operator-7.14.0`.
- **Time:** about 5 min (estimate).

## Step 3: Images

### 3a. Flink image, executor and SQL upload

```bash
source .bench-env
./ingest-deploy/build-deploy-generic.sh build
```

Packs `ingest-deploy/docker/Dockerfile.cdc-paimon`, builds it with Kaniko per
architecture (`image-builder-sa`), joins one multi-arch tag
`emr-flink-cdc-paimon:7.14.0`, and uploads `ingest-deploy/flink-cdc-executor.py`
(the jobs' `jarURI`) and `ingest-deploy/sql-scripts/` to
`s3://$BUCKET_NAME/flink/{scripts,sql-scripts}/`. There is no upload-only
command: **re-run `build` after any SQL or executor change**, before Step 5.

- **Check:** the log ends `✓ Verified emr-flink-cdc-paimon:7.14.0 is one tag serving amd64/arm64`; `aws s3 ls s3://$BUCKET_NAME/flink/sql-scripts/ --recursive`.
- **Time:** 20–40 min for both architectures (estimate).
- **If it fails:** `ServiceAccount image-builder-sa not found` = Step 0. TaskManagers dying with `exitCode=255` / `NoResourceAvailableException` = a single-arch image.

### 3b. DuckDB image (only if `duckdb-cdc-server:2.0.0` is missing or its Dockerfile changed)

```bash
./engine-deploy/duckdb-image/build-duckdb-image.sh
```

Compiles DuckDB 2.0.0 with iceberg, httpfs, aws, avro (and the paimon extension)
for amd64 and arm64 in parallel. 1.5–3 h per architecture cold (estimate). See
[engine-deploy/duckdb-image/README.md](engine-deploy/duckdb-image/README.md).

## Step 4: A fresh table set

A **table set** is `ICEBERG_FORMAT_VERSION` + `CDC_DB_SUFFIX`. **Every full test
uses a new suffix** (`_v3r3` → `_v3r4`, ...): the sink DDL is `CREATE TABLE IF NOT
EXISTS`, so table options (buckets, formats, file sizes) apply at creation only,
and a new set gives empty tables, empty Flink state and a fresh MySQL snapshot.
Never reuse a suffix: its Flink HA/checkpoint state on S3 would be recovered.

| Per set | Where |
|---|---|
| Glue databases | `flink_paimon_db<suffix>`, `flink_iceberg_db<suffix>` |
| Writer state / HA / logs | `s3://$BUCKET_NAME/flink/cdc-{paimon,iceberg}<suffix>/`, `flink/logs/cdc-{paimon,iceberg}<suffix>` |
| Compaction state | `s3://$BUCKET_NAME/flink/iceberg-maintain<suffix>/` |
| Warehouse data | `paimon-warehouse/flink_paimon_db<suffix>.db/`, `paimon-warehouse/iceberg/flink_paimon_db<suffix>/` (Paimon's Iceberg export), `iceberg-warehouse/flink_iceberg_db<suffix>.db/` |
| Results / pins / monitor reports | `querybench-results/cdc-session<tag><SESSION_SUFFIX>/`, `querybench-pins/...` (`<tag>` = suffix with `-`, e.g. `-v3r4`) |

```bash
# 1. stop everything that writes the old set
kubectl delete flinkdeployment flink-cdc-paimon flink-cdc-iceberg iceberg-maintain -n emr-flink --ignore-not-found
kubectl get pods -n emr-flink | grep -E 'flink-cdc|iceberg-maintain'      # wait until empty
# 2. HA ConfigMaps (their names carry NO suffix; a leftover makes a new JobManager recover the old job)
kubectl get configmap -n emr-flink -o name | grep -E '(flink-cdc-(paimon|iceberg)|iceberg-maintain)-.*config-map' \
  | xargs -r kubectl delete -n emr-flink
# 3. the new suffix in .bench-env, then check it is really the one every script sees
sed -i '' 's/^export CDC_DB_SUFFIX=.*/export CDC_DB_SUFFIX=_v3r4/' .bench-env
bash -c 'source .bench-env; echo "CDC_DB_SUFFIX=$CDC_DB_SUFFIX"'
# 4. the new set must not exist yet
source .bench-env
aws glue get-database --name "flink_iceberg_db${CDC_DB_SUFFIX}"   # expect EntityNotFoundException
aws glue get-database --name "flink_paimon_db${CDC_DB_SUFFIX}"    # expect EntityNotFoundException
for p in flink/cdc-iceberg flink/cdc-paimon flink/iceberg-maintain; do aws s3 ls "s3://$BUCKET_NAME/${p}${CDC_DB_SUFFIX}/"; done   # expect nothing
```

- **Check:** every check is empty / not found, and `render` (Step 5) later says **"Glue database created"**, not "already exists".
- **Time:** about 1 min.
- **If it fails:** a Glue DB or S3 prefix of the new suffix already exists = pick another suffix (or remove that set, Step 13).

### Step 4a: Shrink the source to ~10% (only when it outgrew the buffer pool)

The generator inserts as well as updates, so the source grows; past the 92 GiB
buffer pool the rate drops. On 2026-11-02 the newest ~10% of each table (plus
its FK closure and the generator's hot window) was kept by **copy-and-swap stored
procedures** (`shrink_10pct_build` → `swap` → `verify` → `drop_old_and_add_fks`;
`unswap` rolls back until `*_old` is dropped): 491.0M → 49.36M rows, ~14 min.
The procedure file is **not in the repo**. Preconditions: generator at 0, both
writers suspended, no `Binlog Dump` session, a manual RDS snapshot. Afterwards:
a new `CDC_DB_SUFFIX` (Step 4) and a generator restart (it reads `MIN`/`MAX` ids
at pod start).

## Step 5: Writers and Iceberg compaction

```bash
source .bench-env                                   # MYSQL_HOST (from .bench-env.local), MYSQL_USER, the suffix
./ingest-deploy/build-deploy-generic.sh build       # only if the SQL / executor / Dockerfile changed (Step 3a)
./ingest-deploy/build-deploy-generic.sh render      # renders ingest-deploy/flink-cdc-*-deployed.yaml, applies nothing
kubectl apply -n emr-flink -f ingest-deploy/flink-cdc-paimon-deployed.yaml -f ingest-deploy/flink-cdc-iceberg-deployed.yaml
./iceberg-compaction/run-maintain.sh up             # continuous Iceberg compaction, started WITH the writers
```

`render` validates the settings, applies the `ebs-sc` StorageClass and the
Kyverno `flink-cdc-single-az` policy if missing, creates the Iceberg Glue
database, prints the Paimon write and S3A upload settings, and fails on any
`${VAR}` left or `value: ""`.

- **One `kubectl apply` for both files**, so the two initial snapshots start together (`build-deploy-generic.sh deploy` applies them ~30 s apart).
- **Re-deploying is delete + create, never `apply` over a live CR**: EMR stages `jarURI` on CREATE only, and an apply over an existing FlinkDeployment empties `/opt/flink/usrlib` (`NoSuchFileException: /tmp/pyflink/...`). Delete, wait for the pods, clear the HA ConfigMaps (Step 4), apply again.
- **Pause / resume** (no new snapshot; restores the last checkpoint): `kubectl patch flinkdeployment <job> -n emr-flink --type=merge -p '{"spec":{"job":{"state":"suspended"}}}'` (or `"running"`).

**Iceberg compaction (`iceberg-maintain`).** Iceberg's Flink writer never merges
files: every checkpoint adds data files, equality-delete files and deletion
vectors. `iceberg-compaction/run-maintain.sh up` creates one FlinkDeployment
(`maintain.py`, Iceberg's own `TableMaintenance`) that rewrites each table and
expires its snapshots (older than 12 h, keep 5) every 10 writer commits (~10 min).
**Start it right after the writers**: the first pass has to read every
equality delete written so far, and a late start meant ~35M deletes on
`customers` and an OOM at 8 Gi (now 32 Gi). It only runs while the writers
commit. `run-maintain.sh status | plan | suspend | resume | down`; details in
[iceberg-compaction/README.md](iceberg-compaction/README.md). Paimon compacts
and expires inline in its writer.

- **Check:**
  - `kubectl get flinkdeployment -n emr-flink`: `flink-cdc-paimon`, `flink-cdc-iceberg` and `iceberg-maintain` `RUNNING` / `STABLE`.
  - `aws glue get-tables --database-name flink_iceberg_db${CDC_DB_SUFFIX} --query 'TableList[].Name'` lists 4 tables, and the same for `flink_paimon_db${CDC_DB_SUFFIX}`.
  - No `Access denied` in `kubectl logs deploy/flink-cdc-iceberg -n emr-flink -c flink-main-container`.
- **Time:** both writers `RUNNING` / `STABLE` with 16 TaskManagers about **8 min** after the apply (measured). The initial snapshot (~50M rows plus what the generator added) then runs for tens of minutes (estimate).
- **If it fails:** see [Troubleshooting](#troubleshooting): a missing or stale Secret, an apply over an existing CR, a leftover HA ConfigMap, nodepool limits, or (Paimon) a JobManager `OutOfMemoryError: Metaspace` (metaspace is 512m in both manifests).

## Step 6: Load generator (upsert)

```bash
./mysql-data-generator/deploy-data-generator.sh deploy          # StatefulSet mysql-upsert-loadgen, 4 pods
./mysql-data-generator/deploy-data-generator.sh status          # per pod: inst= / avg= / recent= / deletes= / errors=
./mysql-data-generator/deploy-data-generator.sh scale 8 upsert  # change the pod count (also sets POD_COUNT)
```

Batched `INSERT ... ON DUPLICATE KEY UPDATE` plus FK-safe `DELETE`s on
`orders` / `order_items`, from the manifest `mysql-data-generator/mysql-upsert-loadgen.yaml`:

| Setting | Value | Meaning |
|---|---|---|
| `replicas` / `POD_COUNT` | 4 / 4 | `POD_COUNT` shards the hot window; always change both with `scale N upsert` |
| `RATE_PER_POD` | 80000 | token-bucket ceiling, out of reach on purpose |
| `THREADS` / `BATCH_ROWS` | 8 / 5000 | |
| `UPSERT_RATIO` | 0.9 | 90% updates of existing keys |
| `HOT_KEY_RATIO` / `HOT_KEY_WINDOW` | 0.6 / 5000000 | skew to the newest 5M ids; `[MIN, MAX]` id range read once at pod start |
| `RECENT_KEY_RATIO` / `RECENT_KEY_MAX_AGE_S` | 0.6 / 20 | re-writes of keys the thread wrote in the last 20 s → Iceberg position deletes (DVs) |
| `TABLE_RUN_BATCHES` | 8 | each thread stays on one table for 8 batches |
| `DELETE_RATIO` | 0.05 | `orders` / `order_items` only; an order is deleted with its items; ids divisible by 16 never |

Change the mix with `upsert-mix <upsert> <hot> <window>`, `recent-mix <ratio> <age_s>`,
`delete-mix <ratio>` (each a rolling restart). Details:
[2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md). It reads the same Secret
`mysql-credentials` as the writers, and refuses to deploy without it.

- **Check:** `status` prints a rate per pod and `recent=` around 50%; `errors=` against `batches=` is the failure share (MySQL 1213/1205 retries are silent).
- **Rate (measured 2026-11-03):** 4 pods ≈ **89K rows/s** in total, 8 pods ≈ **79K rows/s**: past a few pods, extra pods add lock contention, not throughput. The source, not the writers, sets the ingest rate (both writers read ~165K rows/s of CDC records with no backpressure).
- **Time:** pods ready in 1–2 min (estimate).
- **If it fails:** `Secret mysql-credentials not found` / `has no key` = Step 0. Rows below the shrink cutoff = pods not restarted after a shrink (`kubectl rollout restart statefulset/mysql-upsert-loadgen -n emr-flink`).

## Step 7: Monitor

`SUITE=hot ./querybench/run.sh start` brings it up itself (`MONITOR_UP=0`
skips that). By hand:

```bash
./monitoring/deploy-monitor.sh
kubectl logs -f deployment/flink-cdc-monitor -n emr-flink
```

Every ~2–3 min it samples Paimon and Iceberg in parallel over the same 120 s
window and prints one comparison (rows/s from commit history, source rate,
checkpoints, backpressure, table stats). Every report JSON is uploaded to
`s3://<bucket>/querybench-results/cdc-session<tag><SESSION_SUFFIX>/monitor/`;
Step 11 reads them back. Details: [3-MONITOR.md](3-MONITOR.md).

- **Check:** the banner says `Reports : ... cdc-session-v3r4/monitor/` for the current set; `aws s3 ls` of that prefix lists `paimon-cdc_*.json` and `iceberg-cdc_*.json` after the first cycle.
- **Note:** "committed rows/s" is lower on Paimon by design: it merges re-written keys in its write buffer before committing. Compare the **source rate meter** for input.

## Step 8: Query engines (optional)

`querybench/run.sh start` brings both up when needed. To bring them up first:

```bash
./engine-deploy/deploy-duckdb-cdc.sh up      # duckdb-warm-cdc: 81 cpu / 342 GiB, one m5.24xlarge, 450 GiB disk cache
./engine-deploy/deploy-trino.sh up           # waits until the writers have run 15 min, then 9 workers + coordinator
./engine-deploy/deploy-duckdb-cdc.sh status
./engine-deploy/deploy-trino.sh status
```

The four arms (`ENGINES` × `FORMATS`):

| Arm | Reads | Status (v3) |
|---|---|---|
| `duckdb:iceberg` | `flink_iceberg_db<suffix>`, pinned `metadata.json` copy | works |
| `trino:iceberg` | the same, `FOR VERSION AS OF BIGINT '<snapshot>'` | works |
| `duckdb:paimon` | **native Paimon**, `paimon_scan('<warehouse>/<PAIMON_DB>.db/<t>', snapshot_from_id=N)` (the image's `paimon` extension) | new 2026-11-04, **not yet run live**; reads S3 uncached (paimon-cpp's own S3 client) |
| `trino:paimon` | **native Paimon**, catalog `paimon` (paimon-trino 2.1.0 in `trino-cdc:484-paimon-2.1.0`), `FOR VERSION AS OF BIGINT 'N'` | new 2026-11-04, **not yet run live** |

Both Paimon arms are pinned at the native snapshot `N` of `<table>/snapshot/LATEST`.
Runs up to `_v3r3` read Paimon's Iceberg export instead (a partial view: top-LSM
files only until a full compaction; DuckDB could not read its v3 manifests).

- **Check:** DuckDB `/health` `"busy": false`, `iceberg_format_version` 3; Trino 9 active workers, version 484.
- **Time:** 5–20 min including new nodes (estimate); Trino first waits for the writers' 15 min.

## Step 9: Hot test (writers, generator and compaction live)

```bash
SUITE=hot ./querybench/run.sh render   # optional dry run: the Jobs and every query, no cluster/AWS call
SUITE=hot ./querybench/run.sh start    # leave the laptop up until it prints "job/cdc-qb-watch started"
./querybench/run.sh logs               # the runner (cdc-qb-runner)
./querybench/run.sh logs watch         # the watcher (cdc-qb-watch)
./querybench/run.sh status
```

Preconditions: both writers `RUNNING`, the generator running, no other
`cdc-qb-runner` or throughput `qb-runner` active (`FORCE=1` overrides).

`start`, on the laptop:
1. waits until every writer has been RUNNING `WRITERS_UP_S` (900 s) since its current job start (a restart restarts the clock), so run it right after Steps 5–6 and it just blocks;
2. resumes `iceberg-maintain` (or creates it with `run-maintain.sh up` if missing);
3. brings the monitor up, then DuckDB, then Trino; empties the caches unless every engine just started fresh;
4. starts the runner and watcher Jobs in the cluster. The laptop can disconnect after that.

The runner pins every table of both formats on a **30 s** window ending
`HOT_LAG_SECONDS=300` before the start, runs a warm-up pass and `RUNS=3` timed
passes, interleaving DuckDB and Trino and re-pinning before each pass, and
uploads to `s3://$BUCKET_NAME/querybench-results/cdc-session<tag><SESSION_SUFFIX>/<STAMP>-hot/`.
A query that errors on one arm is recorded as `ERROR`; the run goes on.

**The watcher tears down only after a completed test** (`status: runner-exit=0`):
generator → 0, writers suspended, `iceberg-maintain` suspended, monitor → 0,
DuckDB deleted, Trino → 0 (`STOP_GENERATOR=0`, `SUSPEND_WRITERS=0`,
`STOP_COMPACTION_CRON=0`, `STOP_DUCKDB=0`, `STOP_TRINO=0` keep one). On a failure
it leaves everything running and logs why.

- **Check:** the runner logs `status: results-uploaded ...`, the watcher `status: qb-watch-done`.
- **Time (measured):** engines up ~7 min after the writer wait, runner ~22 min.
- **If it fails:** `./querybench/run.sh logs`; [querybench/README.md](querybench/README.md). A failure inside `start` stops before any Job ("no query Job left running").

### Step 9a: Iceberg compaction during the hot test

`iceberg-maintain` (started in Step 5) keeps compacting while the writers run;
the hot `start` resumes it and the watcher suspends it. Check it with
`./iceberg-compaction/run-maintain.sh status` and `./iceberg-compaction/run-maintain.sh plan`
(read-only: data/delete files and delete counts per table). On 2026-11-03 it
started late and completed passes on `products` only, so the Iceberg arms read
uncompacted tables: the summary must say so.

## Step 10: Snapshot test (writers stopped, cold start)

Run it after the hot test (it stops the writers):

```bash
SUITE=snapshot ./querybench/run.sh start
./querybench/run.sh logs
```

`start`, on the laptop:
1. suspends `iceberg-maintain` so the pinned tables stay put;
2. scales the generator to 0, waits `WRITER_DRAIN_S` (180 s) and until each running writer completed a checkpoint ≥ 60 s after the generator stopped (refuses otherwise), then suspends both writers;
3. resolves `SNAPSHOT_DT`, the newest closed date in both formats' fact tables;
4. brings the engines up and starts the runner and watcher.

There is **no pre-test compaction**: Iceberg is measured as the writers and
`iceberg-maintain` left it; the summary reports its file and delete counts.

The runner (`querybench/snapshot.sh`): waits for the engines, pins the four
tables of every format, resolves the S4 slice, **empties both caches**, then
runs each engine (DuckDB, then Trino) for `SNAPSHOT_RUNS=3` passes with no
warm-up: **pass 0 is timed as the cold pass** (`cold_ms`), `median_ms` is the
median of passes 1–2. Results go to `.../<STAMP>-snapshot/`.

- **Check:** `status: results-uploaded ...` and `status: runner-exit=0`.
- **Time (measured):** ~48 min from `start` to the runner's exit.
- **If it fails:** `no date before <today> in the orders and order_items partitions` → `SNAPSHOT_DT=YYYY-MM-DD`. `snapshot slice ... under the 10-min slice` → lower `SNAPSHOT_SLICE_MINUTES` or set `S_FROM` / `S_TO`.

## Step 11: Summary

```bash
./querybench/run.sh summary            # newest hot + snapshot run of the current set
#   HOT_STAMP=<stamp> SNAPSHOT_STAMP=<stamp>   pick other runs
#   SUMMARY_DIR=<dir with hot/ and snapshot/>  read local results instead of S3
#   INGEST=0                                   skip the ingest part
```

`summary` downloads both runs' results, runs `querybench/ingest_stats.py`
(read-only, laptop credentials, through `uv`) over the hot run's window, and
writes **`bench-results/QUERYBENCH-SUMMARY-<YYYYMMDD><tag><SESSION_SUFFIX>.md`**
with the bucket and account redacted. **Only that `.md` stays local**: the
ingest JSON goes to `s3://.../cdc-session<tag>/<session>-summary/`, and every raw
result stays in its run's S3 prefix. Contents:

1. run configuration; 2. headline; 3. query latency per suite (snapshot: cold
pass and warm median, each with Trino / DuckDB ratios and geometric means);
4. ingest (rows/s and MB/s from the monitor reports, checkpoint time,
backpressure; equality vs position deletes and file counts from table
metadata); 5. table state; conclusions and open items. A suite where two
engines on one format return different rows is marked **INVALID**.

Before sharing it, add a short **run notes** section with anything that changes
how to read the numbers (compaction state, restarts, generator changes), as in
`bench-results/QUERYBENCH-SUMMARY-20261103-v3r3.md`. `bench-results/` is
git-ignored: `git add -f` to commit the summary.

- **Check:** both suites are listed, and the ingest section has numbers (not "ingest_stats.py failed").
- **If it fails:** `no runs under querybench-results/...` = wrong `CDC_DB_SUFFIX` / `SESSION_SUFFIX`. The ingest step needs the run's commit history, which both formats keep 12 h.

## Step 12: Stop / teardown

```bash
# What the watcher does after a completed test (use it after a failed one):
SUITE=hot ./querybench/run.sh stop
#   generator -> 0, writers and iceberg-maintain suspended, monitor -> 0, DuckDB deleted, Trino -> 0

# Remove everything (the S3 data and Glue tables stay; Step 13 removes those):
./ingest-deploy/build-deploy-generic.sh cleanup       # asks "yes"; deletes both writer FlinkDeployments
./iceberg-compaction/run-maintain.sh down
kubectl get configmap -n emr-flink -o name | grep -E '(flink-cdc-(paimon|iceberg)|iceberg-maintain)-.*config-map' \
  | xargs -r kubectl delete -n emr-flink
./mysql-data-generator/deploy-data-generator.sh stop
kubectl delete -f monitoring/monitor-deployment-deployed.yaml
./engine-deploy/deploy-duckdb-cdc.sh down
./engine-deploy/deploy-trino.sh down                  # helm uninstall + cache PVCs
```

- **Check:** `./querybench/run.sh status`: no writers, generator `0/0`, DuckDB not answering, 0 Trino workers.
- **Note:** suspended jobs do not expire snapshots, so the old files of both formats stay on S3 until the writers commit again or the set is removed.

## Step 13: Clean up old table sets

An old set stays in Glue and S3 until removed. Set `OLD` to its suffix (never
the current one; `OLD=` empty is the original unsuffixed v2 set):

```bash
OLD=_v3r3
# 1. nothing writes it
kubectl get flinkdeployment -n emr-flink -o yaml | grep -A1 -E 'GLUE_DATABASE|--db'
# 2. keep the table definitions, then drop the databases (and their tables)
for db in flink_paimon_db$OLD flink_iceberg_db$OLD; do aws glue get-tables --database-name $db > glue-backup-$db.json; done
aws glue delete-database --name flink_paimon_db$OLD
aws glue delete-database --name flink_iceberg_db$OLD
# 3. S3. Keep the TRAILING SLASH: without it OLD=_v3 also matches _v3r1, _v3r3, ...
for p in paimon-warehouse/flink_paimon_db$OLD.db/ paimon-warehouse/iceberg/flink_paimon_db$OLD/ \
         iceberg-warehouse/flink_iceberg_db$OLD.db/ \
         flink/cdc-paimon$OLD/ flink/cdc-iceberg$OLD/ flink/iceberg-maintain$OLD/ \
         flink/logs/cdc-paimon$OLD/ flink/logs/cdc-iceberg$OLD/; do
  aws s3 rm --recursive "s3://$BUCKET_NAME/$p"
done
#    optional: querybench-pins/cdc-session${OLD//_/-}*/ and querybench-results/cdc-session${OLD//_/-}*/
#    (never with OLD= empty: "cdc-session*" then matches every session)
```

**The bucket is versioned**: `aws s3 rm` writes delete markers and the bytes
stay as noncurrent versions until a lifecycle rule expires them. Add a rule
per deleted prefix (`NoncurrentVersionExpiration`, `ExpiredObjectDeleteMarker`),
merged into the existing configuration (`put-bucket-lifecycle-configuration`
replaces all rules).

- **Check:** `aws glue get-database --name flink_iceberg_db$OLD` → `EntityNotFoundException`; `aws s3 ls` of each prefix is empty.
- **Time:** Glue seconds; `s3 rm` minutes to an hour; lifecycle expiry 1–2 days (estimate).

---

# Reference

## How rendering works

`ingest-deploy/build-deploy-generic.sh render` replaces, with `sed`, exactly these
placeholders: `AWS_REGION` (default `us-west-2`), `AWS_ACCOUNT_ID`,
`BUCKET_NAME`, `EMR_EXECUTION_ROLE_ARN`, `EMR_VERSION` (7.14.0), `IMAGE_TAG`
(the same as `EMR_VERSION`), `ICEBERG_FORMAT_VERSION` (2|3, default 3),
`CDC_DB_SUFFIX`, `PAIMON_DV_BITMAP64`, `MYSQL_PORT`, `MYSQL_DATABASE`,
`MYSQL_ENV_SECRET_NAME` (`mysql-credentials`), `JM_NODEPOOL`
(`driver-nodepool`), `TM_NODEPOOL` (`executor-memorynodepool`), and the write
and upload settings below.

| Setting (`.bench-env`) | Default | Where it lands |
|---|---|---|
| `PAIMON_WRITE_ONLY` | `false` | Paimon `INSERT` hint `write-only`. `true` = no inline compaction and no snapshot expiry in the writer |
| `PAIMON_WRITE_BUFFER_SPILLABLE` | `true` | hint `write-buffer-spillable` |
| `PAIMON_WRITE_BUFFER_SIZE` | unset: 64mb dimension / 128mb fact (= Iceberg's `write.target-file-size-bytes`) | hint `write-buffer-size`, one value for every table when set |
| `S3A_MULTIPART_SIZE` / `S3A_MULTIPART_THRESHOLD` / `S3A_ACTIVE_BLOCKS` | `16M` / `16M` / `6` | Paimon catalog `s3.multipart.*`, `s3.fast.upload.active.blocks` (disk buffer in `/tmp/s3a-paimon`) |
| `ICEBERG_S3_MULTIPART_SIZE_BYTES` / `ICEBERG_S3_MULTIPART_THREADS` | derived: `S3A_MULTIPART_SIZE` in bytes / `S3A_ACTIVE_BLOCKS` × 4 | Iceberg catalog `s3.multipart.part-size-bytes` / `num-threads` |

The hints apply to existing tables, so no new `CDC_DB_SUFFIX` is needed; a
change takes `./ingest-deploy/build-deploy-generic.sh build` (uploads the SQL) and a writer
delete + create (Step 5).
`PAIMON_DV_BITMAP64` is derived, not a setting: it is `true` exactly when the
format is 3, because the v3 DV export needs 64-bit DVs. `MYSQL_PASSWORD` is
never substituted: the pods pull it from the Secret through `envFrom`, so no
credential lands in a file.

## Flink job configuration (both jobs, identical)

| Setting | Value |
|---|---|
| JobManager | 1 replica, HA in `s3://<bucket>/flink/cdc-<format><suffix>/ha/`, 8 Gi / 4 cpu, metaspace 512m, `driver-nodepool` (on-demand) |
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

- **Write layout:** Paimon has fixed buckets: 32 per date partition on `orders` / `order_items`, 8 on `customers`, 4 per category on `products` (from the next table set; `_v3r3` has 32 everywhere). Iceberg's `write.distribution-mode = hash` gives one writer per date partition.
- **Maintenance:** Paimon compacts and expires inline, in the writer. Iceberg relies on the separate FlinkDeployment `iceberg-maintain` (`iceberg-compaction/`), which rewrites and expires snapshots (12 h, keep 5) every 10 writer commits, and only while the writers commit.
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
| JobManager `CreateContainerConfigError` | The `mysql-credentials` Secret is missing (Step 0) |
| New set's JobManager recovers an old job graph | Leftover HA ConfigMaps `flink-cdc-{paimon,iceberg}-*config-map`. Delete them (Step 4) |
| Paimon: `NoAuthWithAWSException` on the warehouse | The catalog lacks `s3.aws.credentials.provider = ...WebIdentityTokenFileCredentialsProvider`. paimon-s3 ignores `hadoop-conf-dir` (fixed in `01-catalog-setup.sql`) |
| Iceberg: `NoClassDefFoundError .../hc/core5/http/ClassicHttpRequest` | Iceberg 1.12's default `apache` HTTP client isn't in EMR's SDK bundle. Use `http-client.type = urlconnection` plus the url-connection-client jar (both are in place) |
| New table options have no effect | `CREATE TABLE IF NOT EXISTS` is a no-op on an existing table. Use a new `CDC_DB_SUFFIX` |
| TaskManagers die with `exitCode=255`, `NoResourceAvailableException` | The image is single-arch on a mixed-arch pool. Rebuild with both `PLATFORMS` |
| Pods Pending, `exceed limits for nodepool` | Karpenter counts provisioned capacity against the NodePool `limits.cpu`. Raise it, e.g. `kubectl patch nodepool driver-nodepool --type=merge -p '{"spec":{"limits":{"cpu":256}}}'` (unquoted number) |
| Job cycles RUNNING → RESTARTING for no visible reason | Karpenter consolidation evicting a TaskManager (`failover-strategy: full` restarts all). Check that `karpenter.sh/do-not-disrupt` is on the running TaskManager pods |
| `AWSGlueClientFactory - No region info found ... us-east-1` + `401` on the JobManager | Noise. The job reaches RUNNING anyway. Look elsewhere for a restart cause |
| Operator CRD delete hangs | A CR is stuck on its finalizer. `kubectl patch flinkdeployment <n> -n <ns> --type=merge -p '{"metadata":{"finalizers":null}}'` |
| Generator rate low, no errors printed | MySQL 1213 / 1205 errors are retried silently. Read `errors=` against `batches=` (about 34% batch errors was measured with 8 pods on db.m5.2xlarge; not re-measured on r6i.4xlarge) |
| Generator rate falls over hours (e.g. ~85K → ~30K rows/s) | The source outgrew the buffer pool (37.5M → ~491M rows in ~4 h at 50% inserts on db.m5.2xlarge). Shrink it (Step 4a) and/or size RDS up (Step 0) |
| Source tables regrow below the cutoff after a shrink | The generator pods still use the old `[MIN, MAX]` id range. Restart the StatefulSet |
| DuckDB pod Pending, no node | `driver-nodepool` must allow `24xlarge` (and 12/16xlarge); a cluster-side patch, not in the repo (Step 0) |
| Every Trino pin fails in the runner pod (fixed 2026-11-02) | Kubernetes service links set `TRINO_PORT=tcp://...`, which overrode the port. It is now `QB_TRINO_PORT`, and the Jobs set `enableServiceLinks: false` |
| Trino `FOR VERSION AS OF` fails on Paimon's export (fixed 2026-11-02) | Paimon's snapshot ids are small integers; Trino needs `FOR VERSION AS OF BIGINT '<id>'` |
| A Trino setup error says only `command terminated with exit code 1` | Fixed 2026-11-02: `qb_trino_try` now prints the Trino CLI's own error line |
| Spark compaction (removed; history, 2026-11-02) | Parallel GC's full-GC stalls dropped S3 streams ("Premature end of Content-Length"), and a rewrite that rewrote nothing still "committed". Both were fixed before the Spark job was replaced by the Flink one |
| `duckdb:paimon` errors `missing required Avro key-value metadata field 'schema'` (runs up to `_v3r3`) | The Paimon 2.1 v3 export issue; the arm now reads native Paimon instead |
| A Glue `metadata_location` 404s mid-scan | Paimon's export and Iceberg keep only 3 previous metadata files. The benchmark reads pinned copies, so don't read live Glue paths during a test |
| `run.sh start` refuses: Trino version | Run `./engine-deploy/deploy-trino.sh up` (Trino 484), or set `TRINO_VERSION_CHECK=0` |
| `iceberg-maintain` RESTARTING, `OutOfMemoryError: Java heap space` in `Rewrite` | A large equality-delete backlog (it started late). TaskManager is 32 Gi by default (`TM_MEMORY`); start it with the writers (Step 5) |
| Writers restart-loop with `Access denied for user 'cdcuser'` | The Secret does not hold cdcuser's current password (e.g. after a rotation): fix the Secret, then delete + create the writers |
| Paimon JobManager exits 239, `OutOfMemoryError: Metaspace` | JobManager metaspace; both manifests set `jobmanager.memory.jvm-metaspace.size: 512m` |
| After a failed test everything is still running | By design, the watcher only tears down after `runner-exit=0`. Fix it and re-run, or `SUITE=hot ./querybench/run.sh stop` |
| Monitor pod stuck with no logs | First boot pip-installs pyiceberg and pyarrow (about 2 min) |
| Monitor or engines can't assume role | The role's trust policy must cover `system:serviceaccount:emr-flink:<sa>`, and `starrocks-sa` must carry the role-arn annotation |

## Resources created

- 1 RDS MySQL 8.0.45 instance, db.r6i.4xlarge, 500 GB gp3 / 64,000 IOPS (binlog, CDC user), and 1 Secret (`mysql-credentials`)
- ECR `emr-flink-cdc-paimon:7.14.0` and `duckdb-cdc-server:2.0.0`, both multi-arch; `trino-cdc:484-paimon-2.1.0` once the in-progress Paimon Trino image is pushed
- EMR Flink operator 7.14.0 (Helm), the `ebs-sc` StorageClass, and Kyverno with the `flink-cdc-single-az` ClusterPolicy
- 2 FlinkDeployments (`flink-cdc-paimon`, `flink-cdc-iceberg`), each 1 JobManager + 8 TaskManagers
- StatefulSet `mysql-upsert-loadgen` (4 pods)
- Deployment `flink-cdc-monitor`
- Deployment `duckdb-warm-cdc` (81 cpu / 342 GiB, one m5.24xlarge), Helm release `trino` (9 workers + coordinator, per-pod gp3-fast cache volumes of 56 Gi), Jobs `cdc-qb-runner` / `cdc-qb-watch`
- FlinkDeployment `iceberg-maintain` (Iceberg compaction, [iceberg-compaction/](iceberg-compaction/README.md))
- Per table set: 2 Glue DBs × 4 tables; S3 warehouse, export, Flink state and log prefixes; `querybench-pins/` and `querybench-results/` session prefixes (job-monitor reports under `monitor/`)
