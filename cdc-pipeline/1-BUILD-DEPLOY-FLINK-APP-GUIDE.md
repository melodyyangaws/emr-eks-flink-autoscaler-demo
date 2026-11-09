# Build and Deploy the Flink CDC Jobs

## Overview

Two FlinkDeployments on EMR on EKS 7.14.0 read the same MySQL binlog and write the
same four tables: `flink-cdc-paimon` to Apache Paimon 2.1.0 (with a patched
`paimon-flink-1.20` jar) and `flink-cdc-iceberg` to Apache Iceberg (Flink runtime
1.12.0, replacing EMR's 1.10.1). A generic PyFlink executor
(`ingest-deploy/flink-cdc-executor.py`) runs the SQL in `ingest-deploy/sql-scripts/`. You can change a pipeline
by editing SQL on S3, with no image rebuild.

> **Three things here are easy to get wrong. Read them before Step 1:**
>
> 1. The **SQL scripts** and the **executor itself** both come from S3, uploaded by
>    `./ingest-deploy/build-deploy-generic.sh build`. A local edit changes nothing until it is
>    uploaded. See [The executor script comes from S3](#the-executor-script-comes-from-s3).
> 2. Re-deploying is **delete, clear the HA ConfigMaps, apply**. A bare
>    `kubectl apply` over a live FlinkDeployment is not enough. See
>    [Step 7](#step-7--re-deploy-after-a-change).
> 3. Table options apply **at CREATE TABLE only**. New sink settings need a fresh
>    table set, which means a new `SESSION_SUFFIX`.

**This is the reference for the writers.** The step-by-step run (settings, table
set, writers, generator, tests, summary, teardown) is [README.md](README.md);
this guide adds the details behind its Steps 2-5. Run commands from
`cdc-pipeline/`. `ingest-deploy/build-deploy-generic.sh` changes into its own
directory, so its relative paths (`docker/`, `sql-scripts/`, `k8s/`, the
templates and their renders) are inside `ingest-deploy/`.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    MySQL RDS (Source)                       │
│                  with binlog enabled                        │
└────────────────────────┬────────────────────────────────────┘
                         │ CDC streaming (one binlog reader set per job)
                         ↓
┌─────────────────────────────────────────────────────────────┐
│          Generic Flink CDC Executor (PyFlink)               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  1. Load SQL scripts from S3                         │   │
│  │  2. Substitute environment variables (${MYSQL_PORT}) │   │
│  │  3. Parse SQL into statements                        │   │
│  │  4. Execute in sequence                              │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────┬──────────────────┬───────────────────────┘
                   │                  │
        ┌──────────┴─────────┐   ┌────┴──────────────┐
        │  Paimon on S3      │   │  Iceberg on S3    │
        │  (Glue via Hive    │   │  (Glue via        │
        │   metastore client)│   │   GlueCatalog)    │
        └────────┬───────────┘   └───────┬───────────┘
                 │ + Iceberg v3 metadata │
                 │ (Paimon-as-iceberg)   │
        ┌────────┴──────────────────────┴────────┐
        │  querybench/: DuckDB 2.0, Trino 484    │
        └────────────────────────────────────────┘
```

---

## File Structure

```
cdc-pipeline/
  ├── .bench-env, .bench-env.local     ← settings + account values (README Step 1)
  ├── ingest-deploy/                   ← the Flink CDC writers
  │   ├── build-deploy-generic.sh      ← build | render | deploy | all | cleanup | monitor
  │   ├── flink-cdc-executor.py        ← generic SQL executor (the jobs' jarURI)
  │   ├── flink-cdc-paimon-sql.yaml    ← FlinkDeployment template (Paimon)
  │   ├── flink-cdc-iceberg-sql.yaml   ← FlinkDeployment template (Iceberg)
  │   ├── docker/Dockerfile.cdc-paimon ← image: EMR 7.14 Flink + MySQL CDC + Paimon 2.1 + Iceberg 1.12
  │   ├── docker/patched-jars/         ← the patched paimon-flink-1.20 jar (git-ignored; README.md there)
  │   ├── sql-scripts/
  │   │   ├── common/02-cdc-sources.sql      ← MySQL CDC sources (copied into each format dir on S3)
  │   │   ├── paimon/{01-catalog-setup,03-paimon-sinks,04-cdc-pipelines}.sql
  │   │   └── iceberg/{01-catalog-setup,03-iceberg-sinks,04-cdc-pipelines}.sql
  │   └── k8s/                         ← ebs-sc StorageClass, Kyverno single-AZ policy
  ├── compaction/                     ← one-off compaction of both formats before the snapshot test
  ├── mysql-data-generator/            ← upsert load generator (StatefulSet)
  ├── monitoring/                      ← job monitor
  ├── engine-deploy/                   ← query engines: DuckDB warm server, Trino
  └── querybench/                      ← hot + snapshot query tests, summary
```

---

## Step 1 — Upgrade (or install) the EMR Flink operator

The jobs use `emrReleaseLabel: emr-7.14.0-flink-latest`, so the cluster needs the
**7.14.0** operator chart (operator 1.13.0-amzn-1, image
`flink-k8s-operator/emr-7.14.0-flink`).

1. Check what is installed:

   ```bash
   export NAMESPACE=emr-flink
   helm list --namespace $NAMESPACE      # CHART flink-kubernetes-operator-<version>
   ```

   If it already shows `flink-kubernetes-operator-7.14.0`, skip to Step 2.

2. **Fresh install only.** Install cert-manager, then the chart, and go to step 7:

   ```bash
   kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.12.0/cert-manager.yaml
   ```

3. **Upgrade.** Stop both CDC jobs while the old operator is still running, so it
   can run its finalizers. Checkpoints and table data in S3 are not touched:

   ```bash
   kubectl delete flinkdeployment flink-cdc-paimon flink-cdc-iceberg -n $NAMESPACE --ignore-not-found
   kubectl get flinkdeployment -A        # wait until none are left
   ```

   The CRD delete in step 5 deletes every FlinkDeployment and FlinkSessionJob in
   the cluster anyway, so stop them deliberately here.

4. Uninstall the old operator:

   ```bash
   helm uninstall flink-kubernetes-operator -n $NAMESPACE
   ```

5. Delete the CRDs. `helm upgrade` never updates the CRDs a chart ships, so the AWS
   procedure ([Upgrade the Kubernetes operator](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/jobruns-flink-kubernetes-operator-getting-started.html))
   is uninstall, delete CRDs, reinstall:

   ```bash
   kubectl delete crd flinkdeployments.flink.apache.org flinksessionjobs.flink.apache.org
   ```

   `flinkstatesnapshots.flink.apache.org` is byte-identical between 7.12.0 and
   7.14.0, so leave it. `helm install` keeps the existing one.

6. **If the CRD delete hangs**, a custom resource still carries the operator's
   finalizer, and no operator is left to remove it. Find it and clear the
   finalizer. The CRD delete then completes on its own:

   ```bash
   kubectl get flinkdeployments,flinksessionjobs -A
   kubectl patch flinkdeployment <name> -n <namespace> --type=merge \
     -p '{"metadata":{"finalizers":null}}'
   # same for any flinksessionjob that is listed
   ```

   Without its finalizer, the operator's cleanup never ran for that job. Check for
   leftovers and delete them by hand:

   ```bash
   kubectl get deployment,configmap -n $NAMESPACE | grep flink-cdc
   ```

   The `flink-cdc-monitor*` objects belong to the monitor and stay.

7. Install the 7.14.0 chart and wait for it:

   ```bash
   helm install flink-kubernetes-operator \
     oci://public.ecr.aws/emr-on-eks/flink-kubernetes-operator \
     --version 7.14.0 \
     --namespace $NAMESPACE \
     --set emrContainers.awsRegion=${AWS_REGION:-us-west-2}

   kubectl wait deployment flink-kubernetes-operator --namespace $NAMESPACE \
     --for condition=Available=True --timeout=120s
   helm list --namespace $NAMESPACE      # CHART flink-kubernetes-operator-7.14.0
   ```

   `emrContainers.awsRegion` picks the ECR registry the operator image comes from.
   The chart defaults to `us-east-1`. Set it to the cluster's region to avoid a
   cross-region pull.

An in-place `helm upgrade` after `kubectl replace -f` of the chart's `crds/` keeps
existing CRs. That is the upstream Apache operator's alternative, not the
AWS-documented path for the EMR chart.

---

## Step 2 — Set the environment

Settings come from `.bench-env` and the git-ignored `.bench-env.local`
([README Step 1](README.md#step-1-settings-bench-env-bench-envlocal)).
`build-deploy-generic.sh` reads the environment, so `source .bench-env` first.
`LAKEHOUSE_FORMAT` (`paimon` | `iceberg` | `both`, default `both`) selects which
jobs `build`, `render`, `deploy` and `all` touch.

### Which table set a run writes

| Variable | Default | Effect |
|---|---|---|
| `ICEBERG_FORMAT_VERSION` | `3` | Iceberg sink `format-version` and Paimon `metadata.iceberg.format-version`. Must be `2` or `3` |
| `SESSION_SUFFIX` | If unset: the value in `.bench-env`, else empty. Free text, under 100 characters; names the round only | Appended to both Glue DBs (`flink_paimon_db<suffix>`, `flink_iceberg_db<suffix>`) and to the checkpoint, savepoint, HA and log paths (`flink/cdc-<format><suffix>/`). `render` rejects 100 characters or more; Glue database names allow only lowercase letters, digits and `_` |
| `PAIMON_DV_BITMAP64` | Derived, not a setting: `true` if and only if `ICEBERG_FORMAT_VERSION=3` | Paimon `deletion-vectors.bitmap64` |

An explicit `SESSION_SUFFIX=` (set but empty) selects the original v2 set without a session suffix:

```bash
ICEBERG_FORMAT_VERSION=2 SESSION_SUFFIX= ./ingest-deploy/build-deploy-generic.sh render
```

Table sets never share tables or Flink state, so one set never restores another's
checkpoint, as long as the HA ConfigMaps are cleared when you switch
([Step 7](#step-7--re-deploy-after-a-change)). The query benchmark and the monitor
read `SESSION_SUFFIX` from the same `.bench-env`. Change it there, not on one command
line.

---

## Step 3 — Create the MySQL secret

`build-deploy-generic.sh` does not create it. The writers and the generator share
one Secret, `mysql-credentials` (keys `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`),
created as in [README Step 0](README.md#step-0-one-time-setup).
The manifests load it with `envFrom: secretRef`; no credential is substituted into
any file, including the git-ignored `*-deployed.yaml`.

---

## Step 4 — Build the image and upload the scripts

```bash
aws s3 cp --recursive s3://$BUCKET_NAME/flink/build/patched-jars/ ingest-deploy/docker/patched-jars/   # once per checkout
./ingest-deploy/build-deploy-generic.sh build
```

`build` needs `AWS_REGION`, `AWS_ACCOUNT_ID`, `BUCKET_NAME` and
`EMR_EXECUTION_ROLE_ARN`, plus `kubectl`, the AWS CLI, `python3` and `shasum`. It
needs no MySQL values. It does the following:

1. Creates the ECR repository `emr-flink-cdc-paimon` if it does not exist.
2. Builds the image **in the cluster with Kaniko**, one pod per architecture
   (`flink-cdc-image-build-amd64`, then `-arm64`), each pinned to a node of its
   architecture, because Kaniko cannot cross-build. Each build takes several minutes
   and times out after 30 minutes. The build context is the Dockerfile plus
   `docker/patched-jars/` (the patched Paimon jar; `build` stops if the directory
   holds no jar), shipped via `s3://$BUCKET_NAME/flink/build/`. It needs the ServiceAccount
   `image-builder-sa` in `emr-flink`, with IRSA to push to ECR and read that S3 key.
3. Joins the two per-arch images into **one manifest-list tag**,
   `emr-flink-cdc-paimon:7.14.0` (`IMAGE_TAG`, default `EMR_VERSION`), and deletes
   the temporary `-buildstage-<arch>` tags.
4. Uploads `ingest-deploy/flink-cdc-executor.py` and `ingest-deploy/sql-scripts/` to S3 (see
   [Step 5](#step-5--upload-sql-or-executor-changes-without-a-rebuild)).

Both architectures are the default because `executor-memorynodepool` provisions
Graviton and x86 nodes. With a single-arch image, TaskManagers die with
`exitCode=255` on the other architecture, and the job reports a misleading
`NoResourceAvailableException`. `PLATFORMS=amd64` builds one architecture only.

**Verify the image.** The script ends with these lines, one per architecture:

```
✓ 7.14.0 -> linux/amd64 sha256:…
✓ 7.14.0 -> linux/arm64 sha256:…
✓ Verified emr-flink-cdc-paimon:7.14.0 is one tag serving amd64/arm64
```

The Dockerfile's last step is a classpath check that fails the build rather than
the job. Each Kaniko pod stays until the next build, so read its result there:

```bash
for a in amd64 arm64; do
  kubectl logs flink-cdc-image-build-$a -n emr-flink \
    | grep -A1 -E 'Java specification|Jars with Iceberg|Providers of'
done
```

Expect, for each architecture:

| Line | Expected |
|---|---|
| `Java specification version:` | `17` |
| `Jars with Iceberg core classes:` | exactly one: `<FLINK_LIB_DIR>/iceberg-flink-runtime-1.20-1.12.0.jar` |
| `Providers of software/amazon/awssdk/services/glue/GlueClient:` | at least one jar (EMR's SDK v2 bundle under `/usr/share/aws/aws-java-sdk-v2/`) |
| `Providers of software/amazon/awssdk/services/s3/S3Client:` | at least one jar |
| `Providers of software/amazon/awssdk/http/urlconnection/UrlConnectionHttpClient:` | `<FLINK_LIB_DIR>/url-connection-client-2.44.5.jar` |

A failed check prints `ERROR: ...` and fails the pod. The script then prints the last
60 log lines.

### What the image contains

`ingest-deploy/docker/Dockerfile.cdc-paimon` builds on
`public.ecr.aws/emr-on-eks/flink/emr-7.14.0-flink:latest` (Flink 1.20.0-amzn-8,
Corretto 17) and adds:

- Paimon 2.1.0: `paimon-s3` from Maven Central, and `paimon-flink-1.20` **replaced
  by the patched build** in `docker/patched-jars/` (release-2.1.0 + the apache/paimon
  PR #10329 backport, so Paimon's Iceberg v3 manifests carry the Avro metadata
  DuckDB requires; [patched-jars/README.md](ingest-deploy/docker/patched-jars/README.md)).
  `paimon-flink-action`, `paimon-hive-catalog` and `paimon-hive-connector-3.1` are
  no longer installed (2026-11-07): the bundle already carries their classes, and
  the Dockerfile's comments give the evidence
- `flink-sql-connector-mysql-cdc` 3.7.0-1.20 and `mysql-connector-j` 8.4.0
- the OSS `iceberg-flink-runtime-1.20-1.12.0.jar`, which **replaces** EMR's bundled
  Iceberg 1.10.1-amzn-1 (deleted from `/usr/share/aws/iceberg/lib`). On a v3 table,
  1.10's sink fails the commit when a key is updated twice in one checkpoint, and
  1.12 writes deletion vectors.
- `url-connection-client-2.44.5.jar`, the AWS SDK URLConnection HTTP client at the
  version of EMR 7.14's SDK bundle (see the dry-run fixes below).

The Iceberg runtime has no AWS SDK of its own. GlueCatalog and S3FileIO use EMR's SDK
v2 (2.44.5). If a job ever fails with `NoSuchMethodError` / `NoClassDefFoundError`
under `software.amazon.awssdk`, add `iceberg-aws-bundle-1.12.0.jar` to the jar
downloads. It is an unrelocated SDK 2.54.17 that would shadow EMR's, which is why it
is not the default.

The tag `7.14.0` is rebuilt in place, so both manifests set
`imagePullPolicy: Always`. Layers a node already has are not downloaded again.

---

## Step 5 — Upload SQL or executor changes without a rebuild

`build` is the normal way to upload. For a SQL-only or executor-only change, these
commands do the same upload without rebuilding the image:

```bash
aws s3 cp ingest-deploy/flink-cdc-executor.py s3://${BUCKET_NAME}/flink/scripts/
aws s3 sync ingest-deploy/sql-scripts/common/ s3://${BUCKET_NAME}/flink/sql-scripts/common/ --delete
for f in paimon iceberg; do
  aws s3 sync ingest-deploy/sql-scripts/$f/ s3://${BUCKET_NAME}/flink/sql-scripts/$f/ --delete
  # each job reads only its own dir (--sql-dir .../<format>/), so the shared
  # sources file is copied into it, after the sync (sync --delete removes it)
  aws s3 cp ingest-deploy/sql-scripts/common/02-cdc-sources.sql s3://${BUCKET_NAME}/flink/sql-scripts/$f/
done
```

An upload alone changes nothing on a running job. The SQL is parsed once at job
submission, so re-deploy ([Step 7](#step-7--re-deploy-after-a-change)).

---

## Step 6 — Render and apply both writers

1. Render the manifests without applying them:

   ```bash
   ./ingest-deploy/build-deploy-generic.sh render
   ```

   `render` needs the four AWS values. It needs no MySQL host, user or password
   (the pods read them from the Secret); `MYSQL_PORT` / `MYSQL_DATABASE` default to
   `3306` / `ecommerce`. It also:

   - creates the `ebs-sc` StorageClass (`ingest-deploy/k8s/ebs-sc-storageclass.yaml`) if missing.
     It is the class EMR uses for task-local recovery on EBS; neither writer enables
     local recovery since the switch to the `hashmap` state backend (2026-11-02), so
     nothing uses it today
   - installs Kyverno with Helm if missing, and applies the `flink-cdc-single-az`
     ClusterPolicy (`ingest-deploy/k8s/kyverno-flink-az-affinity.yaml`)
   - creates the Glue database `flink_iceberg_db<suffix>`. The Paimon database is
     created by `CREATE DATABASE IF NOT EXISTS` in `ingest-deploy/sql-scripts/paimon/01-catalog-setup.sql`.
   - writes `ingest-deploy/flink-cdc-paimon-deployed.yaml` and `ingest-deploy/flink-cdc-iceberg-deployed.yaml`
     (git-ignored)

2. Check the configuration block the script prints:

   ```
   DB/state suffix:  <suffix>
   Paimon DV 64-bit: true
   Iceberg format:    v3
   ```

   And check the rendered table-set values:

   ```bash
   grep -nA1 -E 'name: (GLUE_DATABASE|ICEBERG_FORMAT_VERSION|PAIMON_DV_BITMAP64)$' ingest-deploy/flink-cdc-*-deployed.yaml
   ```

3. Apply **both manifests in one call**:

   ```bash
   kubectl apply -n emr-flink \
     -f ingest-deploy/flink-cdc-paimon-deployed.yaml \
     -f ingest-deploy/flink-cdc-iceberg-deployed.yaml
   ```

   With `scan.startup.mode = initial`, the job that starts first snapshots a smaller
   MySQL table and gets a head start on the binlog. One `kubectl apply` makes the two
   snapshot phases overlap. `./ingest-deploy/build-deploy-generic.sh deploy` applies Paimon, waits,
   then applies Iceberg (about 30 s apart). That is fine when you are not comparing
   the two formats.

4. Wait for both jobs:

   ```bash
   kubectl get flinkdeployment -n emr-flink -w     # JOB STATUS RUNNING, LIFECYCLE STABLE
   ```

   Then check a JobManager log:

   ```bash
   kubectl logs -f -l app=flink-cdc-iceberg,component=jobmanager -n emr-flink -c flink-main-container
   ```

   The Flink UI: `kubectl port-forward svc/flink-cdc-iceberg-rest 8081:8081 -n emr-flink`,
   then open http://localhost:8081.

### What render substitutes

`generate_flink_deployment()` turns `flink-cdc-<format>-sql.yaml` into
`flink-cdc-<format>-deployed.yaml` with one `sed` pass, for exactly these
placeholders:

| Placeholder | Source / default |
|---|---|
| `${AWS_REGION}` | `$AWS_REGION`, defaulted to `us-west-2` |
| `${AWS_ACCOUNT_ID}`, `${BUCKET_NAME}`, `${EMR_EXECUTION_ROLE_ARN}` | required |
| `${EMR_VERSION}` | `$EMR_VERSION`, else `7.14.0` (base image and `emrReleaseLabel`) |
| `${IMAGE_TAG}` | `$IMAGE_TAG`, else `$EMR_VERSION` |
| `${ICEBERG_FORMAT_VERSION}` | see [Step 2](#which-table-set-a-run-writes) |
| `${SESSION_SUFFIX}` | see Step 2. Used in `GLUE_DATABASE` (hardcoded per manifest as `flink_<format>_db${SESSION_SUFFIX}`) and in every state and log path |
| `${PAIMON_DV_BITMAP64}` | derived from `ICEBERG_FORMAT_VERSION` |
| `${MYSQL_PORT}`, `${MYSQL_DATABASE}` | `3306`, `ecommerce` (`.bench-env`). Host, user and password are not rendered: the pods read them from Secret `mysql-credentials` (`envFrom`) |
| `${MYSQL_ENV_SECRET_NAME}` | `mysql-credentials` |
| `${JM_NODEPOOL}` / `${TM_NODEPOOL}` | `driver-nodepool` / `executor-memorynodepool` |
| the write and upload settings | below |

The write and upload settings (from the environment, so `source .bench-env`
first; `render` validates them and prints them in its configuration block):

| Setting | Script default (`.bench-env`) | Where it lands |
|---|---|---|
| `PAIMON_WRITE_ONLY` | `false` (`false`) | Paimon `INSERT` hint `write-only`. `true` = no inline compaction and no snapshot expiry in the writer, and new data stays in level 0, which batch reads with deletion vectors (and Paimon-as-iceberg) do not see; keep `false` |
| `PAIMON_WRITE_BUFFER_SPILLABLE` | `true` (`false`) | hint `write-buffer-spillable` |
| `PAIMON_WRITE_BUFFER_SIZE` | unset: 64mb dimension / 128mb fact | hint `write-buffer-size`, one value for every table when set |
| `S3A_MULTIPART_SIZE` / `S3A_MULTIPART_THRESHOLD` / `S3A_ACTIVE_BLOCKS` | `16M` / `16M` / `6` | Paimon catalog `s3.multipart.*`, `s3.fast.upload.active.blocks` (disk buffer in `/tmp/s3a-paimon`) |
| `ICEBERG_S3_MULTIPART_SIZE_BYTES` / `ICEBERG_S3_MULTIPART_THREADS` | derived: `S3A_MULTIPART_SIZE` in bytes / `S3A_ACTIVE_BLOCKS` × 4 | Iceberg catalog `s3.multipart.part-size-bytes` / `num-threads` |

The hints apply to existing tables, so no new `SESSION_SUFFIX` is needed; a
change takes `build` (uploads the SQL) and a writer delete + create
([Step 7](#step-7--re-deploy-after-a-change)).

The script stops if a `${...}` placeholder is left, or if any env `value: ""` is
rendered. An empty region would otherwise render as a blank value that Kubernetes
accepts, and the job would die minutes later with a heartbeat timeout that does not
name the missing variable. An empty `SESSION_SUFFIX` never trips the check, because it
only appears inside non-empty values.

`MYSQL_PASSWORD` is never substituted. `envFrom` is used instead of
`env[].valueFrom.secretKeyRef` because the EMR operator's pod-template env merge adds
an empty `value: ""` next to `valueFrom`, which the API rejects.

---

## Step 7 — Re-deploy after a change

Use this for any change: template, rendered variable, SQL, executor, or a new
`SESSION_SUFFIX`. `upgradeMode: last-state` restores a running job from its last
checkpoint and ignores most spec changes. The operator also stages `jarURI` only
when a CR is created: an `apply` over an existing CR leaves `/opt/flink/usrlib`
empty.

1. Re-render if a template or a substituted variable changed. Upload if SQL or the
   executor changed ([Step 5](#step-5--upload-sql-or-executor-changes-without-a-rebuild)).

   ```bash
   ./ingest-deploy/build-deploy-generic.sh render
   ```

2. Delete both FlinkDeployments:

   ```bash
   kubectl delete flinkdeployment flink-cdc-paimon flink-cdc-iceberg -n emr-flink
   ```

3. Wait until every JobManager and TaskManager pod is gone. Do not apply while pods
   are terminating.

   ```bash
   kubectl get pods -n emr-flink | grep -E 'flink-cdc-(paimon|iceberg)'   # repeat until empty
   ```

4. Clear the HA ConfigMaps. They hold the job graph and leader pointer. If one
   survives, the new JobManager recovers the **old** job graph, and the change is
   silently ignored. They are named by cluster id, not by table set, so a leftover
   one also points a new `SESSION_SUFFIX` at the old set's HA state.

   ```bash
   kubectl get configmap -n emr-flink | grep -E 'flink-cdc-(paimon|iceberg)'
   # expected to remain: nothing (flink-cdc-monitor-* is the monitor's, and stays)
   kubectl delete configmap -n emr-flink flink-cdc-paimon-cluster-config-map --ignore-not-found
   kubectl delete configmap -n emr-flink flink-cdc-iceberg-cluster-config-map --ignore-not-found
   # and any flink-cdc-<job>-<hash>-config-map the grep still lists
   ```

5. Apply both at once, as in [Step 6](#step-6--render-and-apply-both-writers):

   ```bash
   kubectl apply -n emr-flink \
     -f ingest-deploy/flink-cdc-paimon-deployed.yaml \
     -f ingest-deploy/flink-cdc-iceberg-deployed.yaml
   ```

To stop one job and keep the other, `./ingest-deploy/build-deploy-generic.sh cleanup paimon` (or
`iceberg`) prompts for `yes`, then deletes that rendered FlinkDeployment. Clear its
HA ConfigMap before you apply it again.

---

## Fixes from the 2026-11-02 dry run

The first v3 deploy on EMR 7.14 needed three fixes. All three are in the repo now.

| Job | Symptom | Fix |
|---|---|---|
| Iceberg | `GlueCatalog.initialize` fails with `NoClassDefFoundError ...thirdparty/org/apache/hc/core5/http/ClassicHttpRequest`. Iceberg 1.12's default `apache` HTTP client is the SDK's Apache5 client, which EMR 7.14's SDK bundle (2.44.5) does not ship | `'http-client.type' = 'urlconnection'` in `ingest-deploy/sql-scripts/iceberg/01-catalog-setup.sql`, plus `url-connection-client-2.44.5.jar` in the image |
| Paimon | `NoAuthWithAWSException` on the warehouse. Paimon 2.1 opens `s3://` through the `paimon-s3` plugin, an isolated S3A that reads only `s3.*` catalog options and never `hadoop-conf-dir`, so it had no IRSA provider | `'s3.aws.credentials.provider' = 'software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider'` in `ingest-deploy/sql-scripts/paimon/01-catalog-setup.sql` |
| both | A node runs a stale copy of the rebuilt `7.14.0` tag | `imagePullPolicy: Always` in both templates |

The same run found that Paimon's Iceberg metadata deleted its previous `metadata.json`
on every commit, so a path read from Glue returned 404 within one checkpoint. That is
fixed by `metadata.iceberg.previous-versions-max` in the table settings below (3
from 2026-11-02, 100 since 2026-11-04).

---

## Sink table settings: Paimon vs Iceberg

`ingest-deploy/sql-scripts/{paimon,iceberg}/03-*-sinks.sql` pair every setting both formats have.
They apply to **newly created tables only** (`CREATE TABLE IF NOT EXISTS` leaves an
existing table alone), so they take effect with a fresh `SESSION_SUFFIX`.

| Setting | Paimon (`03-paimon-sinks.sql`) | Iceberg (`03-iceberg-sinks.sql`) |
|---|---|---|
| Snapshot retention, 12 h | `snapshot.time-retained = 12h`, `snapshot.num-retained.min = 5`, `snapshot.num-retained.max = 1000` (above 720 commits in 12 h, so age binds, not count) | `history.expire.max-snapshot-age-ms = 43200000`, `history.expire.min-snapshots-to-keep = 5` |
| Previous `metadata.json` kept: 100 (3 until 2026-11-04) | `metadata.iceberg.previous-versions-max = 100`, `metadata.iceberg.delete-after-commit.enabled = true` (Paimon-as-iceberg; Paimon's default kept 0) | `write.metadata.previous-versions-max = 100`, `write.metadata.delete-after-commit.enabled = true` |
| Manifest merge after 5 | `manifest.merge-min-count = 5`; Iceberg metadata: `metadata.iceberg.compaction.min.file-num = 5`, `max.file-num = 50` | `commit.manifest-merge.enabled = true`, `commit.manifest.min-count-to-merge = 5` (default 100) |
| Manifest target size, 8 MB | `manifest.target-file-size = 8mb` | `commit.manifest.target-size-bytes = 8388608` |
| Column stats `truncate(16)` | `metadata.stats-mode = truncate(16)` | `write.metadata.metrics.default = truncate(16)` (was `full`) |
| Target data file: 32 MB (customers, products), 64 MB (orders, order_items); 64 / 128 MB until 2026-11-04 | `target-file-size = 32mb` / `64mb` | `write.target-file-size-bytes = 33554432` / `67108864` |
| Parquet row group: 8 MB / 16 MB | `parquet.block.size = 8388608` / `16777216` | `write.parquet.row-group-size-bytes = 8388608` / `16777216` |
| Compression | `file.compression = zstd`, `file.compression.zstd-level = 1` | `write.parquet.compression-codec = zstd`, `write.parquet.compression-level = 1` |
| Changelog | `changelog-producer = none` (nothing reads one, and Iceberg writes none) | none written |
| Iceberg format version | `metadata.iceberg.format-version = ${ICEBERG_FORMAT_VERSION}` | `format-version = ${ICEBERG_FORMAT_VERSION}` |

Paimon-only settings without an Iceberg counterpart: fixed `bucket` 8 (dimension
tables; per category on `products`) / 16 per date partition (fact tables), `bucket = 32`
until 2026-11-04, `deletion-vectors.enabled = true`,
`deletion-vectors.bitmap64 = ${PAIMON_DV_BITMAP64}`,
`write-buffer-size` (64 / 128 MB) / `write-buffer-spillable` (also `INSERT` hints, below),
`num-sorted-run.compaction-trigger = 8`,
`snapshot.expire.execution-mode = async`, `sink.writer-coordinator.enabled = true`,
`metadata.iceberg.storage = hive-catalog` (the Iceberg v3 metadata, registered in
Glue as `table_type=ICEBERG`) and `metadata.iceberg.manifest-compression = snappy`
(a codec every Iceberg reader of it has). Iceberg-only: `write.upsert.enabled = true`, `write.distribution-mode = hash`,
and the `write.{delete,update,merge}.mode = merge-on-read` properties. The
write-side differences that are kept on purpose (writer parallelism, compaction,
snapshot-expiry timing) are listed in
[PAIMON-VS-ICEBERG.md](./PAIMON-VS-ICEBERG.md#write-side-asymmetries-kept-on-purpose).

---

## Current job sizing

From the templates. Both jobs are kept in lockstep, because these are fair-comparison
variables.

| Setting | Value |
|---|---|
| JobManager | 1 replica, `highAvailabilityEnabled: true`, 8 Gi / 4 cpu, `driver-nodepool`, on-demand |
| TaskManager | 32 Gi / 8 cpu, `taskmanager.numberOfTaskSlots: 2`, `executor-memorynodepool`, on-demand, annotated `karpenter.sh/do-not-disrupt: "true"` |
| Job parallelism | 12 (`job.parallelism` and `--parallelism 12`; 32 until 2026-11-04, then 16, then 24 until 2026-11-05), so 6 TMs per job |
| `parallelism.default` / `pipeline.max-parallelism` | 4 / 128 |
| JobManager metaspace | `jobmanager.memory.jvm-metaspace.size: 512m` (Paimon's JobManager exited 239 on `OutOfMemoryError: Metaspace` at the default) |
| Operator chaining / object reuse | **off** since 2026-11-06 (`pipeline.operator-chaining.enabled: false`, both jobs: the source vertex is the CDC reader alone, so its busy time shows whether the binlog reader is the limit) / on |
| `taskmanager.memory.managed.fraction` | 0.1. Managed memory was RocksDB's, and the state backend is now `hashmap`, so it goes to task heap. The CDC deserializer OOMed inside `RowDataDebeziumDeserializeSchema` at the original sizing |
| Checkpointing | 60 s interval, 600 s timeout, 30 s min-pause, `tolerable-failed-checkpoints: 10`, `RETAIN_ON_CANCELLATION` |
| State | `hashmap`, checkpoints on S3 (`s3p://…/flink/cdc-<format><suffix>/checkpoints/`). RocksDB + changelog + EBS local recovery before 2026-11-02 |
| Failover | `jobmanager.execution.failover-strategy: full`, `restart-strategy.type: exponential-delay` (10 s to 2 min), `jobmanager.scheduler: default` |
| Autoscaler | off |
| `server-id` ranges | Paimon 5401–5432 / 5441–5472 / 5481–5512 / 5521–5552; Iceberg 6401–6432 / 6441–6472 / 6481–6512 / 6521–6552 |

Notes on the less obvious ones:

- **`failover-strategy: full`, not `region`.** Each source subtask is its own
  pipelined region, so with `region` one source subtask died while the rest stayed
  RUNNING, Flink did not consider the job failed, and every checkpoint aborted at
  trigger time (*"Not all required tasks are currently running"*) for 65 minutes.
  Aborted-at-trigger checkpoints are not counted as FAILED, so
  `tolerable-failed-checkpoints` never fired either.
- **`exponential-delay`, not `fixed-delay`.** `fixed-delay`'s attempt counter is
  global, so one TaskManager loss consumed the whole budget (*"Restarting job."* 200
  times inside 20 ms, then *"Recovery is suppressed by
  FixedDelayRestartBackoffTimeStrategy"*).
- **`scheduler: default`, not `adaptive`.** Adaptive let a subtask sit FAILED while
  the job reported RUNNING with `failure-cause` `None`. Autoscaling is off, so
  adaptive buys nothing.
- **`server-id` ranges must not overlap.** Both jobs read the same RDS instance, and
  MySQL evicts the first binlog client when a second one reuses its id. Each range
  is 32 wide, covering parallelism 12 with room to raise it to 32.

---

## The executor script comes from S3

`ingest-deploy/flink-cdc-executor.py` is the `job.jarURI`:

```yaml
job:
  jarURI: s3://${BUCKET_NAME}/flink/scripts/flink-cdc-executor.py
  args: ["-py", "/opt/flink/usrlib/flink-cdc-executor.py", "--sql-dir", "s3://${BUCKET_NAME}/flink/sql-scripts/<format>/", ...]
```

EMR on EKS supports an `s3://` jarURI natively. The operator injects an `aws-cli`
initContainer with its own `flink-artifact` volume and stages the object into
`/opt/flink/usrlib`, where the `-py` argument points. Staging happens only when the
CR is created, so a running JobManager keeps the old copy, and an `apply` over an
existing CR leaves the directory empty. Re-deploy with
[Step 7](#step-7--re-deploy-after-a-change).

**Do not add a `volumes:` list to the JobManager pod template.** The operator owns
`flink-artifact`. A pod template that supplies `volumes:` without it leaves the
injected initContainer with nothing to mount, and the Deployment is rejected with
`initContainers[0].volumeMounts[0].name: Not found: "flink-artifact"`, wedging the
FlinkDeployment in `UPGRADING`. Recover with:

```bash
kubectl patch flinkdeployment flink-cdc-paimon -n emr-flink --type=json \
  -p '[{"op":"remove","path":"/spec/jobManager/podTemplate/spec/initContainers"}]'
```

or by deleting and recreating the CR.

---

## Generic Executor Usage

The executor can run standalone for testing. Options: `--sql-file` (comma-separated)
or `--sql-dir`, `--pattern` (default `*.sql`), `--mode streaming|batch`,
`--checkpoint-interval` (default `60s`), `--parallelism`. Every environment variable
is available as `${VAR}` or `${VAR:default}` in the SQL.

```bash
python ingest-deploy/flink-cdc-executor.py --sql-file setup.sql,pipelines.sql
python ingest-deploy/flink-cdc-executor.py --sql-dir s3://my-bucket/flink-sql/ --mode streaming --parallelism 8
```

---

## Deployment Commands

```bash
./ingest-deploy/build-deploy-generic.sh build              # image (Kaniko, both archs) + upload executor and SQL
./ingest-deploy/build-deploy-generic.sh render             # render *-deployed.yaml, do not apply (benchmark runs)
./ingest-deploy/build-deploy-generic.sh deploy             # render + apply, Paimon then Iceberg (~30 s apart)
./ingest-deploy/build-deploy-generic.sh all                # build + deploy
./ingest-deploy/build-deploy-generic.sh monitor paimon     # FlinkDeployment + pods (or iceberg)
./ingest-deploy/build-deploy-generic.sh cleanup [paimon|iceberg]   # prompts, deletes the rendered FlinkDeployment(s)
```

`LAKEHOUSE_FORMAT` (`paimon` | `iceberg` | `both`, default `both`) selects which
manifests `build`, `render`, `deploy` and `all` touch. `cleanup` takes the format as
an argument instead. `cleanup` removes the jobs, **not** the data: warehouses, Glue
tables and checkpoints survive. The load generator and the monitor are separate:

```bash
./mysql-data-generator/deploy-data-generator.sh stop
kubectl scale deployment/flink-cdc-monitor -n emr-flink --replicas=0
```

---

## Troubleshooting

### SQL parsing errors

Every statement must end with a semicolon.

### `Environment variable 'VAR' is not set`

Set the variable on the JobManager, or give it a default in the SQL:
`'${MYSQL_PORT:3306}'`. `ICEBERG_FORMAT_VERSION` and `PAIMON_DV_BITMAP64` must
never render empty: the executor treats `""` as set, and the DDL then gets `= ''`.

### CDC job not starting

```bash
kubectl logs -l app=flink-cdc-paimon,component=jobmanager -n emr-flink -c flink-main-container --tail=200
kubectl describe pod -l app=flink-cdc-paimon,component=jobmanager -n emr-flink   # secret / image errors
```

Common causes: the `mysql-credentials` secret is missing
(`CreateContainerConfigError`), the binlog is not enabled, the CDC user lacks
permissions, or the IAM role is denied on S3 or Glue.

### SQL files not found / `NoSuchFileException` on the executor

```bash
aws s3 ls s3://${BUCKET_NAME}/flink/sql-scripts/paimon/
aws s3 ls s3://${BUCKET_NAME}/flink/scripts/flink-cdc-executor.py
```

`java.nio.file.NoSuchFileException: /tmp/pyflink/<uuid>/<uuid>/flink-cdc-executor.py`
means the `jarURI` object is missing from S3, or the CR was applied over an existing
one. `/tmp/pyflink` is where `PythonEnvUtils` symlinks the `-py` argument, so the
message names the temp copy. Re-upload ([Step 5](#step-5--upload-sql-or-executor-changes-without-a-rebuild))
and re-deploy ([Step 7](#step-7--re-deploy-after-a-change)).

### A config change had no effect

Do the full [Step 7](#step-7--re-deploy-after-a-change), including the HA
ConfigMap check. Table options are CREATE-time only: an existing table keeps its old
options, so use a new `SESSION_SUFFIX` for new sink settings.

### Job cycles RUNNING → RESTARTING → CREATED → RUNNING

Look for Karpenter consolidation first:

```bash
kubectl get events -n emr-flink --sort-by=.lastTimestamp | grep -i underutilized
kubectl get nodepool executor-memorynodepool -o jsonpath='{.spec.disruption}'
```

`executor-memorynodepool` runs `consolidationPolicy: WhenEmptyOrUnderutilized` with
`consolidateAfter: 2m`, and with `failover-strategy: full` every evicted TaskManager
restarts the whole graph. The TaskManager pods now carry
`karpenter.sh/do-not-disrupt: "true"`, which stops voluntary consolidation of their
nodes. If evictions still show up, check that the rendered manifest has the
annotation under `taskManager.podTemplate.metadata.annotations`.

The pool's instance mix still fragments: a TaskManager requests about 8100m cpu /
32868Mi, so a 4xlarge fits 1 TM, an 8xlarge 3, a 12xlarge 5 and a 16xlarge 7 (six do
not fit on 48 vCPU once DaemonSets are counted). Pinning the pool to one size would
give an even layout, but it is a shared nodepool, so that change needs explicit
sign-off.

### Glue region / IMDS 401 log sequence is noise

```
AWSGlueClientFactory - No region info found, using SDK default region: us-east-1
EC2MetadataUtils - Unable to retrieve the requested metadata (/latest/dynamic/instance-identity/document)
com.amazonaws.AmazonServiceException: Unauthorized (Status Code: 401)
```

This is not a failure and does not cause restarts. The job reaches RUNNING with these
lines present, and they do not appear in steady-state TaskManager logs. Setting
`AWS_DEFAULT_REGION` does not silence them, because the legacy EMR Glue client
resolves its region from the Hadoop/Hive Configuration. The 401 is IMDSv2 being
unreachable from the pod.

`AWS_REGION` **must** still be set on the TaskManagers, not only the JobManager.
Paimon's `IcebergHiveMetadataCommitter` and the EMR Glue client run in the TM
(`CommitterOperator.initializeState`). Without a region, the client blocks on a
cross-region Glue call until `heartbeat.timeout` (180 s) kills the TM, and nothing in
the JM log names the cause.

---

## Next Steps

- **Generate source changes**: [Data Generator Guide](./2-DATA-GEN-GUIDE.md), the upsert
  load generator: inserts, upserts and, since 2026-11-03, DELETEs on orders /
  order_items. The mixed generator was removed on 2026-11-02.
- **Monitor the pipelines**: [Monitor Guide](./3-MONITOR.md)
- **Compact the tables** (before the snapshot test): `compaction/run.sh`, [README Step 10](./README.md#the-one-off-compaction-compaction)
- **Query the tables**: [Query benchmark, DuckDB vs Trino](./querybench/README.md);
  Athena (v2 sets only) in [README, Athena](./README.md#athena)

---

**Last Updated**: 2026-11-08
**EMR Version**: 7.14.0 (Flink operator chart 7.14.0, see Step 1)
**Flink Version**: 1.20.0-amzn-8 (Java 17)
**Paimon Version**: 2.1.0 (patched `paimon-flink-1.20`: apache/paimon PR #10329 backport)
**Iceberg Version**: 1.12.0 OSS Flink runtime (replaces EMR's 1.10.1-amzn-1); format v3 by default on this branch
**Flink CDC MySQL connector**: 3.7.0-1.20
**MySQL Version**: 8.0.45
