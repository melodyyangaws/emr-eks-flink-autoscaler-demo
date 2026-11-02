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
- **Run the query benchmark and capture the result**
  > "Run the hot and snapshot query tests on DuckDB and Trino, collect the read performance for the Paimon and Iceberg formats and compare"
  >
- **Choose the table set (Iceberg v3 by default)**
  > "Run the query benchmark on the v2 table set" (`ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX=`; see [Step 0b](#step-0b--settings-bench-env-and-bench-envlocal))
  
## What We Built

A complete **real-time Change Data Capture (CDC)** pipeline with dual lakehouse formats and multiple OLAP query engines:

- **Source**: MySQL RDS (8.0) with binlog-based CDC (`flink-sql-connector-mysql-cdc` 3.7.0-1.20)
- **Processing**: Apache Flink 1.20.0-amzn-8 on EMR on EKS 7.14.0
- **Storage**: Apache Paimon 2.1.0 + Apache Iceberg 1.12.0 on S3 (AWS Glue Catalog); Iceberg format v3 (deletion vectors) by default, v2 on request
- **OLAP**: AWS Athena (serverless, v2 set); for the query benchmark, a DuckDB 2.0.0 warm server and Trino 484 on EKS
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
│  │  • Script is the job.jarURI (s3://…/flink-cdc-executor.py);  │  │
│  │    EMR on EKS stages it into /opt/flink/usrlib for us        │   │
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
              │  │ AWS Athena   │  │ DuckDB, Trino │  │
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
│
├── docker/
│   └── Dockerfile.cdc-paimon           ← Flink image (CDC + Paimon + Iceberg), multi-arch
│
├── flink-cdc-executor.py               ← Generic SQL executor (PyFlink) running FlinkSQL as jobs
├── build-deploy-generic.sh             ← Build image (Kaniko), upload SQL, deploy jobs
│                                         (environment only; does not read .bench-env)
├── .bench-env                          ← Session settings: table set (Iceberg v3 / _v3 by
│                                         default), warehouses, engine versions (Step 0b)
├── lib/
│   ├── common.sh                       ← Loads .bench-env (+ .bench-env.local), command-line
│   │                                     overrides, table-set names, logging, render
│   └── querybench.sh                   ← Shared query-benchmark functions (arms, pins, render, gates)
│
├── sql-scripts/
│   ├── common/
│   │   └── 02-cdc-sources.sql          ← MySQL CDC source table definitions
│   ├── paimon/
│   │   ├── 01-catalog-setup.sql        ← Paimon catalog (Hive/Glue metastore)
│   │   ├── 03-paimon-sinks.sql         ← Paimon tables (DV + Iceberg compat)
│   │   └── 04-cdc-pipelines.sql        ← INSERT INTO streaming pipelines
│   └── iceberg/
│       ├── 01-catalog-setup.sql        ← Iceberg catalog (Glue)
│       ├── 03-iceberg-sinks.sql        ← Iceberg tables (merge-on-read), format-version
│       │                                 ${ICEBERG_FORMAT_VERSION} (3 by default)
│       └── 04-cdc-pipelines.sql        ← INSERT INTO streaming pipelines
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
│   ├── ebs-sc-storageclass.yaml        ← gp3 StorageClass named "ebs-sc" (EMR's name for
│   │                                     Flink local recovery; unused since hashmap state)
│   └── kyverno-flink-az-affinity.yaml  ← Pin each Flink job's pods to one AZ
│
├── engine-deploy/                      ← The query benchmark's engines (README.md there)
│   ├── deploy-duckdb-cdc.sh            ← DuckDB warm server duckdb-warm-cdc: up | status | clear-cache | down | render
│   ├── duckdb-cdc.yaml                 ← its Deployment (27 cpu / 114 Gi, 450 GiB disk cache)
│   ├── duckdb_cdc_server.py            ← the server (one query at a time, every result row)
│   ├── duckdb-image/                   ← Dockerfile + build-duckdb-image.sh: DuckDB 2.0.0 built
│   │                                     from source, pushed to ECR as duckdb-cdc-server:2.0.0
│   ├── deploy-trino.sh                 ← Trino 484 (chart 1.45.0): render | up | status | clear-cache | down | sql | qstats
│   └── trino-values.yaml               ← its Helm values (3 x 9 cpu / 38 Gi workers + 4 cpu coordinator)
│
├── bench-results/                      ← Local run output: logs, CSVs, summaries. Git-ignored
│                                         (.gitignore: cdc-pipeline/bench-results); only a few
│                                         older files are still tracked
│
├── iceberg-compaction/                 ← Iceberg rewrite_data_files as a bare Spark Job
│   ├── run-compaction.sh               ← Submit one table's Job, follow it, save log + JSON
│   ├── compact-job.yaml                ← Job manifest (EMR 7.14.0 Spark image, local mode)
│   └── compact-order-items.py          ← The PySpark compaction (Glue catalog, S3FileIO)
│
├── querybench/                         ← Step 5: hot + snapshot query benchmark, DuckDB vs Trino
│   │                                     on Iceberg and on Paimon's Iceberg export (four arms)
│   ├── README.md                       ← Design, sizing parity, commands, settings, results
│   ├── run.sh                          ← Entry point: render | start | status | logs | summary
│   ├── qb-runner.yaml                  ← Runner / watcher Job (bundles querybench/, lib/, engine-deploy/, .bench-env)
│   ├── hot-interleave.sh, snapshot.sh  ← the two suites (runner steps)
│   ├── duckdb-warm.sh, trino.sh        ← the engine drivers (same passes, median, CSV)
│   ├── pin.sh, clear-caches.sh         ← pins (both formats, both engines), cold start
│   ├── qb-watch.sh, summarize.py       ← shutdown after a test; the Markdown summary
│   ├── dialect-duckdb.env, dialect-trino.env  ← per-engine SQL fragments the queries use
│   ├── hot/                            ← hot suite, one query per file + QUERIES.md
│   └── snapshot/                       ← snapshot suite, one query per file + QUERIES.md
├── PAIMON-VS-ICEBERG.md                ← Format comparison + benchmark results
└── README.md                           ← This file
```

**Not in git** (`.gitignore`) — generated locally, and they contain resolved account
IDs and bucket names:

```
mysql-cdc-env.sh                        ← MYSQL_HOST / MYSQL_USER / MYSQL_PASSWORD
.bench-env.local                        ← AWS_REGION / AWS_ACCOUNT_ID / BUCKET_NAME /
                                          EMR_EXECUTION_ROLE_ARN (Step 0b)
bench-results/                          ← local run output
*-deployed.yaml                         ← rendered manifests with ${VAR} substituted:
                                          flink-cdc-{paimon,iceberg}-deployed.yaml,
                                          monitoring/monitor-deployment-deployed.yaml,
                                          engine-deploy/{duckdb-cdc,trino-values}-deployed.yaml,
                                          querybench/cdc-qb-{runner,watch}-deployed.yaml
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
| `${EMR_VERSION}` | `$EMR_VERSION`, else `7.14.0` (also the base-image release and `emrReleaseLabel`) |
| `${IMAGE_TAG}` | `$IMAGE_TAG`, else `$EMR_VERSION`, else `7.14.0` |
| `${ICEBERG_FORMAT_VERSION}` | `$ICEBERG_FORMAT_VERSION`, else **`3`** on this branch; must be `2` or `3`. Env var on both JobManagers: the Iceberg sink's `format-version` and Paimon's `metadata.iceberg.format-version`. Always rendered non-empty (the executor treats `""` as set and would write `= ''` into `CREATE TABLE`) |
| `${PAIMON_DV_BITMAP64}` | Derived, not a setting: `true` when `ICEBERG_FORMAT_VERSION=3`, `false` when `2` (StarRocks' native Paimon reader cannot decode 64-bit deletion vectors). Both JobManagers |
| `${CDC_DB_SUFFIX}` | `$CDC_DB_SUFFIX`, else **`_v3`** when unset; empty or like `_v3`. Appended to both Glue DB names and to the checkpoint / savepoint / HA / log paths (`flink/cdc-<format>${CDC_DB_SUFFIX}/…`). Set it explicitly empty for the unsuffixed set |
| `${MYSQL_HOST}` / `${MYSQL_USER}` | `$MYSQL_HOST` / `$MYSQL_USER` (required) |
| `${MYSQL_ENV_SECRET_NAME}` | `flink-cdc-mysql-env` |
| `${JM_NODEPOOL}` | `driver-nodepool` |
| `${TM_NODEPOOL}` | `executor-memorynodepool` |

On this branch the defaults select the **Iceberg v3 table set** (`*_v3` databases,
format 3, `_v3` state paths). The original v2 set is
`ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX= ./build-deploy-generic.sh render` — the
empty `CDC_DB_SUFFIX=` is honoured (the script defaults it with `${CDC_DB_SUFFIX-_v3}`,
unset only). An empty suffix never trips the `value: ""` guard below, because it is
only ever substituted inside a non-empty value.

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

### Step 0b — Settings: `.bench-env` and `.bench-env.local`

Settings live in three files in `cdc-pipeline/`, each with its own job:

| File | In git | Holds | Read by |
|---|---|---|---|
| `.bench-env` | yes | session values: table set, warehouses, engine versions, what the query benchmark stops and compacts | `lib/common.sh` (below) |
| `.bench-env.local` | no (`.gitignore`: `.bench-env.local`) | account values: region, account ID, bucket, EMR execution role ARN | sourced by `.bench-env` itself; falls back to `../throughput-benchmark/.bench-env.local` when absent |
| `mysql-cdc-env.sh` | no (`.gitignore`) | the MySQL source: `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`, and the Secret names `MYSQL_SECRET_NAME`, `MYSQL_SECRET_NAMESPACE`, `MYSQL_ENV_SECRET_NAME` (Step 0) | you, `source`d by hand before Steps 1–2 |

Account values go in the git-ignored `.bench-env.local`, never in committed
files or results:

```bash
cat > .bench-env.local <<'EOF'
export AWS_REGION=<region>
export AWS_ACCOUNT_ID=<account-id>
export BUCKET_NAME=emr-on-eks-test-<account-id>-<region>
export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::<account-id>:role/emr-on-eks-test-execution-role
EOF
```

If you already have `throughput-benchmark/.bench-env.local`, you can skip this:
`.bench-env` and `querybench/run.sh` use that file when this one is missing.
Neither `.bench-env` file holds MySQL values, and nothing reads
`mysql-cdc-env.sh` for you. The MySQL password reaches the Flink jobs only
through the Kubernetes Secret `flink-cdc-mysql-env` (key `MYSQL_PASSWORD`;
another name via `MYSQL_ENV_SECRET_NAME`), and it reaches the data generators
through `mysql-credentials` (`hostname` / `username` / `password`). See
[0-MYSQL-SETUP-GUIDE.md](0-MYSQL-SETUP-GUIDE.md).

What `.bench-env` sets:

| Variable | `.bench-env` | Default when unset | Controls |
|---|---|---|---|
| `NAMESPACE` | `emr-flink` | `emr-flink` | the EMR on EKS namespace for every job and pod |
| `ICEBERG_FORMAT_VERSION` | `3` | `3` (`build-deploy-generic.sh`, `lib/common.sh`) | Iceberg `format-version` of the Iceberg sink tables and of Paimon's Iceberg export (`metadata.iceberg.format-version`). It also derives `PAIMON_DV_BITMAP64` (`true` only for 3) and whether DuckDB's `paimon` arm skips puffin verification (Paimon's v3 DV export) |
| `CDC_DB_SUFFIX` | `_v3` | `_v3` in `build-deploy-generic.sh` and `deploy-monitor.sh`; empty in `lib/common.sh` | the table set. It is appended to the Glue databases (`flink_{paimon,iceberg}_db<suffix>`), the Flink checkpoint / savepoint / HA / log paths (`flink/cdc-<format><suffix>/`), the query engines' database names and the result prefixes |
| `SESSION_SUFFIX` | empty | empty | labels one round of query results (empty or like `-r2`): `querybench-results/cdc-session<set tag><SESSION_SUFFIX>/`. It selects no tables |
| `PAIMON_WAREHOUSE` | `s3://${BUCKET_NAME}/paimon-warehouse` | the same | the Paimon warehouse the query benchmark reads. The Flink manifests write `s3://${BUCKET_NAME}/paimon-warehouse/` themselves and do not read this variable |
| `ICEBERG_WAREHOUSE` | `s3://${BUCKET_NAME}/iceberg-warehouse` | the same | the Iceberg warehouse, read the same way |
| `FLINK_JOBS` | `flink-cdc-paimon flink-cdc-iceberg` | the same | the writers the query benchmark checks, and stops for the snapshot test |
| `TRINO_VERSION` / `TRINO_CHART_VERSION` | `484` / `1.45.0` | the same (`engine-deploy/deploy-trino.sh`) | the Trino `deploy-trino.sh up` installs, and the version `querybench/run.sh start` requires on a Trino that is already up |
| `DUCKDB_IMAGE` | `${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/duckdb-cdc-server:2.0.0` | — | the DuckDB warm-server image (built from source by `engine-deploy/duckdb-image/`). Empty (`DUCKDB_IMAGE=`) means `python:3.12-slim` with a pip install of `duckdb==2.0.0`, which is a different build and so a different timing baseline |
| `GENERATOR_RESOURCES` | `statefulset/mysql-upsert-loadgen deployment/mysql-data-generator` | the same | the load generators the hot test needs running; the snapshot test scales them to 0 |
| `COMPACT_BEFORE_SNAPSHOT` | `true` | `false` | the snapshot test submits the Iceberg compaction Jobs (`iceberg-compaction/`) first |
| `COMPACT_TABLES` | `customers products orders order_items` | the same | the tables compacted, and the ones the Iceberg phase's gate requires |

`ICEBERG_FORMAT_VERSION` and `CDC_DB_SUFFIX` change together. An existing
table cannot change its format, so **each format version has its own table
set**:

| Set | Settings | Glue databases | Flink state |
|---|---|---|---|
| **v3** (default on this branch) | `.bench-env` as is (`ICEBERG_FORMAT_VERSION=3`, `CDC_DB_SUFFIX=_v3`) | `flink_paimon_db_v3`, `flink_iceberg_db_v3` | `s3://$BUCKET_NAME/flink/cdc-{paimon,iceberg}_v3/` |
| v2 (the original) | `ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX=` (set, but empty) | `flink_paimon_db`, `flink_iceberg_db` | `s3://$BUCKET_NAME/flink/cdc-{paimon,iceberg}/` |

A v3 deploy writes **new** tables and starts from empty Flink state, so it
does not restore a v2 checkpoint or touch the v2 tables. Its CDC sources run
the full MySQL snapshot again before reading the binlog. Both sets can exist
side by side, and the query benchmark reads whichever set is selected.
`lib/common.sh` warns when `ICEBERG_FORMAT_VERSION=3` is combined with an
empty suffix.

**Which scripts read `.bench-env`:**

| Reads it | How |
|---|---|
| `querybench/run.sh`, `querybench/qb-watch.sh`, `engine-deploy/deploy-duckdb-cdc.sh`, `engine-deploy/deploy-trino.sh`, `engine-deploy/duckdb-image/build-duckdb-image.sh`, `lib/querybench.sh` | through `lib/common.sh` |
| the query-benchmark runner pod (`querybench/qb-runner.yaml`) and the scripts it runs (`snapshot.sh`, `pin.sh`, `duckdb-warm.sh`, `trino.sh`, ...) | `run.sh` bundles `.bench-env` into the Job and puts `.bench-env.local` in a Secret; the pod's step script sources `.bench-env` first |
| `iceberg-compaction/run-compaction.sh` | only when `run.sh` starts it, which passes `ICEBERG_DB` and `ICEBERG_TABLE` |
| `monitoring/deploy-monitor.sh` | reads only `CDC_DB_SUFFIX` from it, when that is not already set in the environment |

| Does **not** read it | What to do |
|---|---|
| `build-deploy-generic.sh` | It takes everything from the environment. Run `source .bench-env` first, which also loads `.bench-env.local`, or export the same values by hand (Step 1) |
| `mysql-data-generator/deploy-data-generator.sh` | `source mysql-cdc-env.sh` (`MYSQL_*`); `NAMESPACE` defaults to `emr-flink` |
| `monitoring/capture-flink-metrics.sh` | environment and its own defaults only |

**Overriding a value for one run.** For scripts that go through
`lib/common.sh`, a value on the command line wins. The script restores it
after sourcing `.bench-env` and prints a `[cdc-bench] X='...' from the
environment overrides .bench-env's '...'` line:

```bash
ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX= SUITE=hot ./querybench/run.sh start   # v2 set, this run only
```

For `build-deploy-generic.sh`, plain shell rules apply. A prefix on the command
line wins over the shell, but `source .bench-env` overwrites anything you
exported earlier, so export your overrides *after* sourcing it.

### Step 1 — Build & Deploy Flink CDC
> Guide: [1-BUILD-DEPLOY-FLINK-APP-GUIDE.md](1-BUILD-DEPLOY-FLINK-APP-GUIDE.md)

1. Install EMR on EKS Flink Operator (if needed; chart 7.14.0, see the guide)
2. Set environment variables (`mysql-cdc-env.sh` + `.bench-env`, Step 0b)
3. Create the MySQL password Secret
4. Build Docker image + upload the executor script and SQL scripts to S3
5. **Render** the manifests, then apply both in one `kubectl` call

```bash
source mysql-cdc-env.sh       # MYSQL_HOST / MYSQL_USER / MYSQL_PASSWORD (all required by the script)
source .bench-env             # + .bench-env.local: AWS_REGION, AWS_ACCOUNT_ID, BUCKET_NAME,
                              #   EMR_EXECUTION_ROLE_ARN; NAMESPACE, ICEBERG_FORMAT_VERSION=3, CDC_DB_SUFFIX=_v3
export LAKEHOUSE_FORMAT=both  # paimon | iceberg | both
# v2 set instead: export ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX=   (after sourcing .bench-env)

# Without .bench-env, export the same values by hand:
#   export AWS_REGION=us-west-2
#   export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
#   export BUCKET_NAME=emr-on-eks-test-${AWS_ACCOUNT_ID}-${AWS_REGION}
#   export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::${AWS_ACCOUNT_ID}:role/emr-on-eks-test-execution-role
#   export NAMESPACE=emr-flink ICEBERG_FORMAT_VERSION=3 CDC_DB_SUFFIX=_v3   # the script's defaults anyway

# 1. MySQL password Secret. The manifests pull MYSQL_PASSWORD in via
#    `envFrom: secretRef`, so the password is NEVER written into any file in
#    this repo — not even the rendered *-deployed.yaml. Create it once:
#    (see 0-MYSQL-SETUP-GUIDE.md; default name is flink-cdc-mysql-env)
kubectl get secret flink-cdc-mysql-env -n emr-flink   # verify it exists

# 2. Build and push the image to ECR, and upload the SQL scripts to S3.
#    This also uploads flink-cdc-executor.py to the job.jarURI key, which EMR on
#    EKS stages into /opt/flink/usrlib for the JobManager — so re-run `build`
#    after editing the executor script.
./build-deploy-generic.sh build

# 3. Render flink-cdc-{paimon,iceberg}-deployed.yaml WITHOUT applying, then
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
| EMR Version | 7.14.0 (`emr-7.14.0-flink-latest`; no EMR 8.x Flink release exists) |
| Flink Version | 1.20 (1.20.0-amzn-8), Java 17 |
| Connectors | Paimon 2.1.0 (`paimon-flink-1.20`), Iceberg 1.12.0 OSS `iceberg-flink-runtime-1.20` (replaces EMR's 1.10.1-amzn-1), `flink-sql-connector-mysql-cdc` 3.7.0-1.20 |
| Table set | `ICEBERG_FORMAT_VERSION` 3 + `CDC_DB_SUFFIX` `_v3` by default → `flink_{paimon,iceberg}_db_v3`; `ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX=` for the original v2 set |
| Job Manager | 1 replica, HA enabled (`s3://<bucket>/flink/cdc-<format>${CDC_DB_SUFFIX}/ha/`), 8 Gi / 4 cpu, `driver-nodepool` (on-demand) |
| Task Manager | 32 Gi / 8 cpu, 4 task slots, `executor-memorynodepool` (on-demand) |
| Job parallelism | 32 (`job.parallelism` + `--parallelism 32`); `parallelism.default: 4`, `pipeline.max-parallelism: 128` |
| Task count | 808 tasks (Paimon) / 520 tasks (Iceberg) at parallelism 32 with chaining off (before 2026-11-02); fewer with `pipeline.operator-chaining.enabled: true` |
| Chaining | `pipeline.operator-chaining.enabled: true`, `pipeline.object-reuse: true` — Source → DropUpdateBefore → Calc → ConstraintEnforcer run as one task |
| Managed memory | `taskmanager.memory.managed.fraction: 0.1` — only RocksDB used managed memory, so with hashmap the rest of the 32 Gi goes to task heap (~24 Gi) for the CDC deserializer |
| Checkpointing | 60s interval, 600s timeout, `tolerable-failed-checkpoints: 10` |
| State Backend | `hashmap` (task heap), S3 checkpoints/savepoints under `s3p://<bucket>/flink/cdc-<format>${CDC_DB_SUFFIX}/`; no changelog, no EBS local recovery (RocksDB + changelog + EBS before 2026-11-02) |
| Region | `aws.region` / `fs.s3a.endpoint.region` from `${AWS_REGION}` in both manifests (Paimon used to hardcode `us-west-2`) |
| TaskManager pods | `karpenter.sh/do-not-disrupt: "true"`, so consolidation cannot evict a running TM |
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

Every cycle logs one side-by-side Paimon vs Iceberg table (the same monitor as
`throughput-benchmark/monitoring`, over the four CDC tables). The headline
rows/s is rows committed / time between commits, from each table's history,
with a per-table row under it. The monitor reads table stats via:
- **Iceberg tables**: `pyiceberg` (snapshot metadata through the Glue catalog)
- **Paimon tables**: Paimon's own snapshots and manifests on S3

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

### Step 4 — OLAP Query Layer (Athena)

**Athena (Iceberg v2 set only)**: query the tables from the Athena console.
```sql
SELECT * FROM flink_iceberg_db.customers LIMIT 10;
```
Athena cannot query the default v3 set (`flink_iceberg_db_v3`). The AWS
[Iceberg V3 support table](https://docs.aws.amazon.com/AmazonS3/latest/userguide/working-with-apache-iceberg-v3.html)
lists Amazon Athena (Trino) as "No" for V3. For Athena, deploy the v2 set
(`ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX=`, Step 0b). The v3 set is read by
the query benchmark's engines (Step 5).

**Athena (Paimon)** — the `flink_paimon_db` tables are `table_type = PAIMON` and
Athena cannot read them. Two ways round it; the first is the one verified here
(on the v2 set, 2026-09-28).

*Athena SQL, via a shadow Glue database.* Register Paimon's *exported* Iceberg
metadata (`paimon-warehouse/iceberg/...`) as a normal Iceberg table in a
**separate** Glue database, then query it with plain Athena SQL. Use a separate
database because the `flink_paimon_db` entries are owned by Paimon's
`IcebergHiveMetadataCommitter`.

```python
import boto3, json
glue, s3 = boto3.client("glue"), boto3.client("s3")
BUCKET, SRC_DB, DST_DB = "<bucket>", "flink_paimon_db", "paimon_athena_test"

ICE2HIVE = {"long": "bigint", "timestamptz": "timestamp"}   # rest map 1:1

for t in ["customers", "products", "orders", "order_items"]:
    base = f"paimon-warehouse/iceberg/{SRC_DB}/{t}/metadata"
    # version-hint.text names the current metadata version
    v = s3.get_object(Bucket=BUCKET, Key=f"{base}/version-hint.text")["Body"].read().decode().strip()
    mkey = f"{base}/v{v}.metadata.json"
    meta = json.load(s3.get_object(Bucket=BUCKET, Key=mkey)["Body"])
    schema = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
    cols = [{"Name": f["name"], "Type": ICE2HIVE.get(f["type"], f["type"])}
            for f in schema["fields"]]

    glue.create_table(DatabaseName=DST_DB, TableInput={
        "Name": t, "TableType": "EXTERNAL_TABLE",
        "Parameters": {"table_type": "ICEBERG",
                       "metadata_location": f"s3://{BUCKET}/{mkey}"},
        "StorageDescriptor": {
            "Location": f"s3://{BUCKET}/paimon-warehouse/{SRC_DB}.db/{t}",
            "Columns": cols,
        }})
```
```sql
SELECT COUNT(*) FROM paimon_athena_test.customers;
```

> **`Columns` is not optional.** With an empty column list, `COUNT(*)` still
> succeeds (answered from manifest statistics, 0 bytes scanned), but any
> projection fails with `COLUMN_NOT_FOUND: Relation contains no accessible
> columns`. Athena takes the column list from Glue and the data from the
> Iceberg metadata, so both have to be right.
>
> **`metadata_location` pins a snapshot.** The registered table is frozen at the
> `v<N>.metadata.json` you named. Re-read `version-hint.text` and `UpdateTable`
> to refresh. Results: [bench-results/ATHENA-PAIMON-TEST-20260928.md](bench-results/ATHENA-PAIMON-TEST-20260928.md).

*Athena Notebook.* Needs `athena:StartCalculationExecution`, which the admin role
in this account does not currently have.
```json
# custom spark properties — SparkCatalog, not SparkSessionCatalog
{
    "spark.sql.catalog.paimon_iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.paimon_iceberg.type": "hadoop",
    "spark.sql.catalog.paimon_iceberg.warehouse": "s3://$PAIMON_WAREHOUSE/iceberg"
}
```

```python
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.sql("SELECT * FROM paimon_iceberg.flink_paimon_db.customers LIMIT 10").show()
```

StarRocks was removed from this pipeline on 2026-11-02 (the v3 set is
unreadable by StarRocks 4.1.x's Iceberg reader); its 2026-09 results stay in
`bench-results/` and [PAIMON-VS-ICEBERG.md](PAIMON-VS-ICEBERG.md), the scripts
in git history.

### Step 5 — Query benchmark: DuckDB vs Trino (hot and snapshot)
> Guide: [querybench/README.md](querybench/README.md) — design, sizing parity, commands, settings and results.
> Engines: [engine-deploy/README.md](engine-deploy/README.md).
> Queries: [querybench/hot/QUERIES.md](querybench/hot/QUERIES.md),
> [querybench/snapshot/QUERIES.md](querybench/snapshot/QUERIES.md)

Flink writes both formats; two query engines read them, each on both, so a
run has up to **four arms** (`ENGINES` × `FORMATS`, default all four):

| Arm | Reads | How the pinned version is read |
|---|---|---|
| `duckdb:iceberg` | the Iceberg tables (`flink_iceberg_db<suffix>`) | `iceberg_scan()` of a copy of the pinned `metadata.json` |
| `trino:iceberg` | the same | `iceberg.<db>.<t> FOR VERSION AS OF <snapshot id>` |
| `duckdb:paimon` | Paimon's Iceberg export (`paimon-warehouse/iceberg/...`) | `iceberg_scan()` of a copy of the pinned `vN.metadata.json`, puffin verification skipped on v3 |
| `trino:paimon` | the same export, registered in the Paimon Glue DB as `table_type=ICEBERG` | `iceberg.<paimon db>.<t> FOR VERSION AS OF <export snapshot id>` |

Both engines of a format read the **same pinned snapshot** of every table, so
their rows must match (the summary marks the suite INVALID otherwise);
Iceberg vs Paimon differences are a pipeline gap, recorded. On the v3 set
`duckdb:paimon` currently errors (`Manifest ... missing required Avro key-value
metadata field 'schema'`, a Paimon 2.1 export bug): its queries are recorded
as `ERROR` and the run goes on.

- **hot**: the CDC writers and the upsert generator run; a 5-minute window,
  the tables pinned again before every pass, DuckDB and Trino interleaved pass
  by pass.
- **snapshot**: the writers are stopped, the Iceberg compaction Jobs
  (`iceberg-compaction/`) run first and must complete (Paimon compacts in its
  writer), the tables are pinned after that, and both engines start cold
  (DuckDB restarted, `deploy-trino.sh clear-cache`) before the first warm-up pass.
- **Sizing parity**: DuckDB 27 cpu / 114 GiB on one node; Trino 3 workers ×
  9 cpu / 38 GiB (= 27 cpu / 114 GiB) plus a 4 cpu / 16 GiB coordinator that
  runs no splits; both with a ~450 GiB disk cache.

```bash
# from cdc-pipeline/; session values in .bench-env, account values in .bench-env.local (Step 0b)
# v2 set for one run: prefix ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX= to any command below
SUITE=hot ./querybench/run.sh render        # dry run: renders every query for all four arms, no cluster or AWS
SUITE=hot ./querybench/run.sh start         # hot test, writers and generator running
SUITE=snapshot ./querybench/run.sh start    # snapshot test: stops the writers, submits the Iceberg
                                            # compaction, waits for it, then cold start + passes
ENGINES=trino FORMATS=iceberg SUITE=snapshot ./querybench/run.sh start   # one arm only
./querybench/run.sh status | logs [watch] | summary
```

The snapshot run records its gate times (`gate_writers_stopped_at`,
`gate_compaction_finished_at`, `gate_engines_ready_at`), `pinned_at`,
`caches_cleared_at` and the pinned snapshot ids in `snapshot-<STAMP>.pins`,
next to its results in
`s3://$BUCKET_NAME/querybench-results/cdc-session<set tag><SESSION_SUFFIX>/` (set tag
`-v3` for `CDC_DB_SUFFIX=_v3`, empty for v2).

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
- **Format v3 by default** (`ICEBERG_FORMAT_VERSION`; v2 for the unsuffixed set) —
  merge-on-read for CDC. The Iceberg 1.12.0 Flink sink writes equality deletes for
  upserts and, on v3, deletion vectors for deletes within a checkpoint
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
| Storage Format | Parquet (zstd, level 1 on both formats) |
| Table format | Iceberg format v3 by default (`ICEBERG_FORMAT_VERSION=3`, `_v3` set; row-level deletes as deletion vectors); v2 set via `ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX=`. Paimon: deletion vectors, 64-bit (`deletion-vectors.bitmap64`) on the v3 set |
| State backend | `hashmap` on the task heap, S3 checkpoints; no changelog, no EBS local recovery |
| Source scale (observed) | ~491M rows / 102 GiB read during the snapshot phase (measured on the v2 set, before the 7.14.0 upgrade) |

Throughput numbers are **workload- and run-specific** — measure them, do not quote
a table. Source-side write rate comes from the load generator's `inst=` line (see
[2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md)); sink-side rates and per-format
comparisons come from the monitor and the benchmark harness (see
[3-MONITOR.md](3-MONITOR.md) and [querybench/README.md](querybench/README.md)).

---

## Troubleshooting Quick Reference

| Issue | Solution |
|-------|----------|
| `NoSuchFileException: /tmp/pyflink/<uuid>/<uuid>/flink-cdc-executor.py` | The `job.jarURI` object is missing from S3, so the operator's staging initContainer had nothing to place in `/opt/flink/usrlib`. Re-run `./build-deploy-generic.sh build` to re-upload `flink-cdc-executor.py`, then re-deploy |
| `initContainers[0].volumeMounts[0].name: Not found: "flink-artifact"`, FlinkDeployment wedged in `UPGRADING` | A pod template supplied its own `volumes:` list that omitted `flink-artifact`, which the operator's staging initContainer needs. Don't add a `volumes:` list to the JobManager pod template; if the CR already holds the injected initContainer, strip it with `kubectl patch flinkdeployment <name> -n emr-flink --type=json -p '[{"op":"remove","path":"/spec/jobManager/podTemplate/spec/initContainers"}]'` (or delete and re-apply the CR) |
| Spec change (resources, parallelism, pod template) didn't take effect | `upgradeMode: last-state` means a bare `kubectl apply` won't pick it up. DELETE the FlinkDeployment, wait for the pods to go, check for leftover HA ConfigMaps, then apply |
| Job cycles RUNNING → RESTARTING → CREATED → RUNNING for no visible reason | Karpenter consolidation. `executor-memorynodepool` runs `consolidationPolicy: WhenEmptyOrUnderutilized` / `consolidateAfter: 2m` with a 25% Underutilized budget, so it evicts nodes still hosting RUNNING TaskManagers. With `failover-strategy: full` one lost TM restarts the whole graph. Look for `Disrupting NodeClaim: Underutilized` and `Evicted pod: Underutilized`; the JM only shows `NoResourceAvailableException` / an RPC `TimeoutException`. The TaskManager pods now carry `karpenter.sh/do-not-disrupt: "true"`; if this still happens, check the annotation is on the running pods |
| `AWSGlueClientFactory - No region info found, using SDK default region: us-east-1` + `EC2MetadataUtils` + `Unauthorized (Status Code: 401)` | **Noise on the JobManager, not a failure.** The job reaches RUNNING with these lines present and they do not appear in steady-state TM logs at all. Setting `AWS_DEFAULT_REGION` does NOT suppress it — that was tested with both vars confirmed in the resolved container env. The legacy EMR Glue client takes its region from the Hadoop/Hive Configuration, not the process env. Look elsewhere (usually Karpenter, above) for a restart cause |
| `cp_fail` / "Lifetime ckpt failed" is non-zero but unchanged | That counter is `counts.failed`, cumulative since job start; it never decreases. Only the `history` array from `/jobs/<id>/checkpoints` shows current health — see [3-MONITOR.md](3-MONITOR.md#checkpoint-failure-counts-are-cumulative-not-current) |
| Load generator far below target rate, no errors in the logs | 1213 (deadlock) and 1205 (lock-wait timeout) are retried silently and never printed — they only show in the `errors=` counter. Check `errors=` vs `batches=`, and judge rate by `inst=`, not `avg=`. See [2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md) |
| Both jobs are up but the benchmark isn't comparable | `deploy both` starts them ~30s apart. Use `render` + a single `kubectl apply -f a -f b` so the snapshot phases overlap |
| FlinkDeployment won't start | Check IAM role, secrets, image pull, ECR auth |
| CDC not capturing changes | Verify binlog enabled, CDC user has REPLICATION SLAVE |
| Duplicate email crash | Data generator uses UUID-based emails |
| Calculated column error (subtotal) | Don't INSERT into `order_items.subtotal` |
| Data generator connects but no data | Check pod logs for per-operation errors; ensure ConfigMap is redeployed |
| Monitor `PaimonStorageHandler` error | Paimon tables are read from their own S3 snapshots and manifests, not through pyiceberg; redeploy the monitor (`./monitoring/deploy-monitor.sh`) if an old version is running |
| Athena Spark can't read Paimon tables | Use Hadoop catalog in Athena Spark; set `metadata.iceberg.format-version = '2'` |
| Athena SQL can't read Paimon tables | Register the exported Iceberg metadata as `table_type = ICEBERG` in a **separate** Glue database — see [Step 4](#step-4--olap-query-layer-athena). Don't edit `flink_paimon_db` in place; Paimon's committer owns those entries |
| `COLUMN_NOT_FOUND: Relation contains no accessible columns` | The Glue table has an empty `StorageDescriptor.Columns`. Athena reads the column list from Glue and the data from Iceberg metadata; populate columns from the metadata JSON's `current-schema-id` schema |
| Flink image build fails locally | Builds run in-cluster with Kaniko; no local Docker needed |
| Monitor pod stuck `ContainerCreating`/no logs | First boot pip-installs pyiceberg+pyarrow (~2 min); check `kubectl logs` again |
| Monitor pod can't assume role | Update IAM trust policy to allow the SA's OIDC subject |

---

## Resources Created

- ☑️ 1 RDS MySQL instance (binlog-enabled, CDC user configured)
- ☑️ 1 ECR repository (Flink CDC image, built in-cluster with Kaniko)
- ☑️ 2 FlinkDeployments (Paimon + Iceberg pipelines), 1 JM + 8 TMs each (parallelism 32 / 4 slots);
  TM pods carry `karpenter.sh/do-not-disrupt: "true"`
- ☑️ 1 Data generator (`mixed` Deployment or `upsert` StatefulSet, ConfigMap-based script)
- ☑️ 1 Monitor Deployment (Flink REST API + Glue/pyiceberg stats)
- ☑️ 4 Paimon tables on S3 (with Iceberg compatibility metadata) in `flink_paimon_db${CDC_DB_SUFFIX}`
- ☑️ 4 Iceberg tables on S3 (Glue catalog) in `flink_iceberg_db${CDC_DB_SUFFIX}`: format v3
  (`flink_iceberg_db_v3`) by default, format v2 (`flink_iceberg_db`) for the v2 set. Each set
  is a separate pair of Glue databases, and both can exist at once
- ☑️ For the query benchmark (Step 5), optional: the DuckDB 2.0.0 warm server `duckdb-warm-cdc`
  (image `duckdb-cdc-server:2.0.0` in this account's ECR), Trino 484 (Helm release `trino`:
  1 coordinator + 3 workers, per-pod gp3 cache volumes), the `iceberg-compact-<table>`
  Spark Jobs (EMR 7.14.0 Spark image), and results under `s3://$BUCKET_NAME/querybench-results/`
- ☑️ S3 checkpoint, savepoint and HA storage under `s3://$BUCKET_NAME/flink/cdc-<format>${CDC_DB_SUFFIX}/`
  (`hashmap` state backend: no changelog storage, no EBS local-recovery volumes)
- ☑️ `ebs-sc` gp3 StorageClass (`k8s/ebs-sc-storageclass.yaml`). It was needed for EMR's Flink
  local recovery, which the jobs no longer use
- ☑️ Kyverno ClusterPolicy `flink-cdc-single-az` (pins each job's pods to one AZ)
- ☑️ Kubernetes Secrets for MySQL credentials (`flink-cdc-mysql-env` for the Flink
  jobs via `envFrom`, `mysql-credentials` for the data generators)

---

**Status**: ✅ Production Ready
**Last Updated**: 2026-11-02
**EMR Version**: 7.14.0 (base image `public.ecr.aws/emr-on-eks/flink/emr-7.14.0-flink`, `latest` or pinned `20260926`; Flink operator chart 7.14.0 recommended, see 1-BUILD-DEPLOY-FLINK-APP-GUIDE.md)
**Flink Version**: 1.20.0-amzn-8 (Java 17)
**Paimon Version**: 2.1.0 (the Spark 3.5 bundle was renamed `paimon-spark-3.5_2.12` from 1.4, not dropped)
**Iceberg Version**: 1.12.0 (OSS `iceberg-flink-runtime-1.20`, replacing EMR's bundled 1.10.1-amzn-1; writes deletion vectors on v3 tables). Tables: format v3 by default on this branch.
Compaction (`iceberg-compaction/`): EMR 7.14.0 Spark 3.5.8-amzn-0 image with Iceberg 1.10.1-amzn-1
**Flink CDC MySQL connector**: 3.7.0-1.20
**Trino Version** (query benchmark): 484, chart `trinodb/trino` 1.45.0 (`engine-deploy/deploy-trino.sh`; `.bench-env` `TRINO_VERSION` / `TRINO_CHART_VERSION`)
**DuckDB Version** (query benchmark): 2.0.0, built from source (`engine-deploy/duckdb-image/`)
**MySQL Version**: 8.0.45
