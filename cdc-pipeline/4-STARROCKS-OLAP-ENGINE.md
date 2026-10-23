# OLAP Query Layer Guide — Athena & StarRocks

## Overview

Complete guide for setting up interactive query engines on top of your Paimon and Iceberg lakehouses.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  MySQL RDS (Source)                         │
└────────────────────────┬────────────────────────────────────┘
                         │ Flink CDC
                         ↓
┌─────────────────────────────────────────────────────────────┐
│              Lakehouse Storage Layer (S3)                   │
│  ┌──────────────────────┐    ┌──────────────────────────┐  │
│  │  Paimon Tables       │    │  Iceberg Tables          │  │
│  │  (Hive/Glue Catalog) │    │  (AWS Glue Catalog)      │  │
│  └──────────┬───────────┘    └───────────┬──────────────┘  │
└─────────────┼──────────────────────────────┼────────────────┘
              │                              │
              ↓                              ↓
┌─────────────────────────────────────────────────────────────┐
│              OLAP Query Layer (Choose One or Both)          │
│                                                             │
│  Option 1: AWS Athena (Serverless)                         │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  • Iceberg: Native support via Glue                  │  │
│  │  • Paimon: SQL on exported Iceberg metadata (2.1),   │  │
│  │            or Athena Spark + Hadoop catalog (2.2)    │  │
│  │  • Pay-per-query pricing                             │  │
│  │  • No infrastructure management                      │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  Option 2: StarRocks on EKS (Self-hosted)                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  • Paimon: Native catalog support                    │  │
│  │  • Iceberg: Native catalog support                   │  │
│  │  • Sub-second query performance                      │  │
│  │  • Vectorized execution engine                       │  │
│  │  • Materialized views & caching                      │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
              │
              ↓
      BI Tools (QuickSight, Tableau, Superset)
```

---

## Option 1: AWS Athena

### Iceberg with Athena (Native Support)

**✅ Iceberg has native Athena support via Glue Catalog**

#### 1.1 Verify Glue Database

```bash
aws glue get-database --name flink_iceberg_db --region $AWS_REGION
aws glue get-tables --database-name flink_iceberg_db --region $AWS_REGION
```

#### 1.2 Query Iceberg Tables (Athena SQL)

Open Athena Console → Data source: `AwsDataCatalog` → Database: `flink_iceberg_db`

```sql
-- Get totla rows
SELECT
    (SELECT COUNT(*) FROM flink_iceberg_db.customers) as customers,
    (SELECT COUNT(*) FROM flink_iceberg_db.products) as products,
    (SELECT COUNT(*) FROM flink_iceberg_db.orders) as orders,
    (SELECT COUNT(*) FROM flink_iceberg_db.order_items) as order_items;

-- Daily revenue
SELECT
    DATE(order_date) as order_day,
    COUNT(*) as order_count,
    SUM(total_amount) as daily_revenue
FROM flink_iceberg_db.orders
WHERE order_date >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY DATE(order_date)
ORDER BY order_day DESC;

-- Customer analytics
SELECT
    c.customer_name, c.city, c.state,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent
FROM flink_iceberg_db.customers c
LEFT JOIN flink_iceberg_db.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name, c.city, c.state
ORDER BY total_spent DESC
LIMIT 20;

-- Product sales
SELECT
    p.product_name, p.category,
    SUM(oi.quantity) as units_sold,
    SUM(oi.subtotal) as revenue
FROM flink_iceberg_db.products p
JOIN flink_iceberg_db.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 20;
```

#### 1.3 Iceberg Time Travel

```sql
-- Query as of specific timestamp
SELECT * FROM flink_iceberg_db.customers
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-02-20 00:00:00';

-- Query specific snapshot
SELECT * FROM flink_iceberg_db.customers
FOR SYSTEM_VERSION AS OF 1234567890;

-- View snapshot history
SELECT * FROM flink_iceberg_db."customers$snapshots";

-- View table manifests
SELECT * FROM flink_iceberg_db."customers$manifests";
```

---

### Paimon with Athena (via Iceberg Compatibility)

**⚠️ Paimon tables are registered in Glue as `table_type = PAIMON`. Athena cannot read them directly.**

Paimon's Iceberg compatibility mode (`metadata.iceberg.storage = 'hive-catalog'`) writes Iceberg-format metadata under the `iceberg/` subdirectory in S3. There are two ways to read it; **2.1 is the one verified on this account.**

#### 2.1 Athena SQL, via a shadow Glue database (verified)

Register Paimon's *exported* Iceberg metadata as a normal Iceberg table in a
**separate** Glue database. Plain Athena SQL then reads it — no Spark needed.
Use a separate database because the `flink_paimon_db` entries are owned by
Paimon's `IcebergHiveMetadataCommitter` and carry `table_type = PAIMON`.

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

Then query from the Athena Console (data source `AwsDataCatalog`, database
`paimon_athena_test`) exactly like any Iceberg table:

```sql
SELECT COUNT(*) FROM customers;

SELECT order_status, COUNT(*) AS cnt, SUM(total_amount) AS revenue
FROM orders GROUP BY order_status ORDER BY cnt DESC;
```

> **`Columns` is not optional.** With an empty column list, `COUNT(*)` still
> succeeds — it is answered from manifest statistics and scans 0 bytes — but any
> projection fails with `COLUMN_NOT_FOUND: Relation contains no accessible
> columns`. Athena takes the column list from Glue while taking the data from
> the Iceberg metadata, so both have to be right.

> **`metadata_location` pins a snapshot.** The registered table is frozen at the
> `v<N>.metadata.json` you named and does not advance as Paimon commits. Re-read
> `version-hint.text` and `UpdateTable` to refresh.

Measured on a suspended-job snapshot (16.8M customers / 86.4M orders / 137.6M
order_items): `COUNT(*)` 0.8 s / 0 B scanned, `GROUP BY order_status` 3.3 s /
303 MB, a join over `order_items ⋈ products` 2.9 s / 937 MB.
`COUNT(*) = COUNT(DISTINCT customer_id)` exactly, confirming the Iceberg reader
honours Paimon's LSM merge and deletion vectors rather than double-counting
superseded row versions. Full results: `bench-results/ATHENA-PAIMON-TEST-20260928.md`.

#### 2.2 Athena Spark Notebook

```python
# Configure Iceberg Hadoop catalog — reads metadata files directly from S3
spark.conf.set("spark.sql.catalog.paimon_iceberg", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.paimon_iceberg.type", "hadoop")
spark.conf.set("spark.sql.catalog.paimon_iceberg.warehouse","${PAIMON_WAREHOUSE}/iceberg")

# Query Paimon tables via Iceberg-compatible metadata
spark.sql("SELECT * FROM paimon_iceberg.flink_paimon_db.customers LIMIT 10").show()

spark.sql("""
    SELECT order_status, COUNT(*) as cnt, SUM(total_amount) as revenue
    FROM paimon_iceberg.flink_paimon_db.orders
    GROUP BY order_status
""").show()
```

> **Why Hadoop catalog?** The GlueCatalog tries to load `PaimonStorageHandler` (not on Athena's classpath) and fails with `ClassNotFoundException`. The Hadoop catalog reads Iceberg `v*.metadata.json` files directly from S3, resolving the current version from `version-hint.text` in the same prefix.

> **Important:** Paimon sink tables must have `'metadata.iceberg.format-version' = '2'` (not `'3'`). Athena engine v3 only supports Iceberg format V2.

> **Use `SparkCatalog`, not `SparkSessionCatalog`.** `SparkSessionCatalog` exists
> only to override the *default* catalog (`spark_catalog`) so Hive and Iceberg
> tables can coexist under it; it is wrong for a named catalog like
> `paimon_iceberg`.

> **Not verified on this account.** `StartSession` on the Spark workgroup
> (`icebergv3`) succeeds and reaches `IDLE`, but `StartCalculationExecution` is
> refused with `UNAUTHORIZED` — a restriction on the calling identity, not on
> Paimon or the execution role. Use section 2.1 instead unless that permission is
> granted. Also note `StartSession` rejects `DefaultExecutorDpuSize` for this
> engine config; pass `{"MaxConcurrentDpus": 2}` alone.

---

## Option 2: StarRocks on EKS

**🚀 StarRocks provides native support for both Paimon and Iceberg!**

### 3.1 Deploy StarRocks

```bash
export AWS_REGION=us-west-2
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME=emr-on-eks-test-${AWS_ACCOUNT_ID}-${AWS_REGION}
export EKS_CLUSTER_NAME=loadtest-mcp

# Install IRSA + operator + cluster.
# Defaults, all overridable: NAMESPACE=emr-flink, RELEASE_NAME=starrocks,
# CHART_VERSION=1.11.7, IMAGE_TAG=4.1.4.
./starrocks/deploy-starrocks.sh install-iam
```

This creates:
- IAM policy `StarRocksS3GluePolicy` (S3 + Glue read access)
- IRSA service account `starrocks-sa` in `$NAMESPACE` (default: `emr-flink`)
- StarRocks operator + FE/BE cluster as **one** Helm release named `starrocks`

Cluster shape comes from `helm/starrocks-eks-values.yaml`: **1 FE** (2–4 cpu,
8–16 Gi, 50 Gi gp3) and **3 BE** (4–6 cpu, 16–24 Gi, 200 Gi gp3 each).

If you install by hand instead of via the script, this is the command the script
runs — note the single release and the two pins:

```bash
helm repo add starrocks https://starrocks.github.io/starrocks-kubernetes-operator
helm repo update
helm upgrade --install starrocks starrocks/kube-starrocks \
  --version 1.11.7 \
  --namespace emr-flink \
  --values helm/starrocks-eks-values.yaml \
  --set starrocks.starrocksFESpec.image.tag=4.1.4 \
  --set starrocks.starrocksBeSpec.image.tag=4.1.4 \
  --wait --timeout 15m
```

> **The operator and the cluster cannot be split into two Helm releases.** Chart
> 1.11.x has exactly two top-level values keys — verify with
> `helm show values starrocks/kube-starrocks --version 1.11.7 | grep '^[a-zA-Z]'`,
> which prints only `operator:` and `starrocks:`. There is no `starrocksCluster`
> top-level key and no `operator.enabled`; the real switches are nested
> (`operator.starrocksOperator.enabled`, `starrocks.starrocksCluster.*`). So
> `--set operator.enabled=…` and `--set starrocksCluster.enabled=…` are silently
> ignored — `helm template` output is byte-identical with them set either way —
> and **both** releases render the full chart. The first release takes Helm
> ownership of `ServiceAccount/starrocks` and `ConfigMap/kube-starrocks-{fe,be}-cm`,
> and the second then dies with:
> ```
> Error: unable to continue with install: ServiceAccount "starrocks" in namespace
> "emr-flink" exists and cannot be imported into the current release: invalid
> ownership metadata; annotation validation error: key "meta.helm.sh/release-name"
> must equal "starrocks": current value is "starrocks-operator"
> ```
> and, once that one is annotated away, the same error for
> `ConfigMap "kube-starrocks-be-cm"`. Install once, as one release.
>
> **The namespace must be `emr-flink`.** The chart does not create the FE/BE
> ServiceAccount — it only references one by name
> (`starrocks.starrocks{FE,Be}Spec.serviceAccount: starrocks-sa`). That SA gets its
> S3 + Glue access from `emr-on-eks-test-execution-role`, whose trust policy allows
> `system:serviceaccount:emr-flink:*` and nothing else. A `starrocks-sa` in any
> other namespace is not covered, and the external catalogs fail on S3/Glue. The
> script now defaults `NAMESPACE=emr-flink` for this reason; the rationale is in the
> header comments of `helm/starrocks-eks-values.yaml`. Note the side effect: this is
> a *shared* namespace, so `uninstall` no longer deletes it (see 3.6).
>
> **Both the chart and the images are pinned.** Chart 1.11.7's appVersion is
> `4.1-latest` — a **mutable** tag, so with `image.tag: ""` the engine version
> drifts on every pod restart. The script pins `CHART_VERSION=1.11.7` and
> `IMAGE_TAG=4.1.4`, passing the tag as
> `--set starrocks.starrocks{FE,Be}Spec.image.tag`. The values file leaves
> `image.tag: ""` with a commented-out `tag: "4.1.4"` alongside if you prefer to pin
> there instead; in that case run with `IMAGE_TAG=` so the two do not fight.
> Verified on the running cluster: `starrocks/fe-ubuntu:4.1.4` and
> `starrocks/be-ubuntu:4.1.4`, while `helm list -n emr-flink` still reports the
> release's APP VERSION as `4.1-latest` (that column is the chart's appVersion, not
> the image actually pulled — do not read the engine version off it).

### 3.2 Connect to StarRocks

There is no external endpoint. `helm/starrocks-eks-values.yaml` sets
`starrocksFESpec.service.type: ClusterIP`, so the FE service has no
`.status.loadBalancer.ingress` and never will.

```bash
# Print the FE service/pod and every working access path
./starrocks/deploy-starrocks.sh connection

# Path 1 — exec into the FE pod. This is what the bench scripts do.
kubectl exec -it -n emr-flink kube-starrocks-fe-0 -- mysql -h127.0.0.1 -P9030 -uroot

# Path 2 — port-forward, then use a local mysql client
kubectl port-forward -n emr-flink svc/kube-starrocks-fe-service 9030:9030 8030:8030 &
mysql -h 127.0.0.1 -P 9030 -u root
```

The `root` user has no password in this deployment (`initPassword.enabled` is left
at the chart default of `false`).

> **Do not query `.status.loadBalancer` on the FE service.** It returns empty
> forever, not "empty until the LB provisions" — verify with
> `kubectl get svc -n emr-flink | grep starrocks`, which shows all four StarRocks
> services as `ClusterIP` with no `EXTERNAL-IP`. Earlier revisions of this section
> and of `get_connection_info()` printed an empty endpoint and a `mysql -h <blank>`
> command line.
>
> **Two services match the `fe` label**, and the one you want is not the first
> alphabetically. `kubectl get svc -l app.kubernetes.io/component=fe` returns the
> headless `kube-starrocks-fe-search` (`CLUSTER-IP: None`, FE peer discovery) *and*
> the real `kube-starrocks-fe-service`. A `jsonpath='{.items[0]...}'` selects the
> headless one; filter on `{.items[?(@.spec.clusterIP!="None")]}` instead.
>
> **Component labels are `app.kubernetes.io/component`**, not
> `app.starrocks.io/component`. On chart 1.11.7 the latter matches nothing
> (`kubectl get pods -l app.starrocks.io/component=fe` → "No resources found"); the
> StarRocks-specific label actually present is `app.starrocks.ownerreference/name`.

### 3.3 Configure Catalogs

The two catalog files carry `${BUCKET_NAME}` / `${AWS_REGION}` placeholders, so
render them first, then pipe them in. `install`/`install-iam` render them to `/tmp`
for you and print these commands; to do it by hand:

```bash
for f in 01-starrocks-paimon-catalog.sql 02-starrocks-iceberg-catalog.sql; do
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" -e "s|\${AWS_REGION}|${AWS_REGION}|g" \
    sql-scripts/starrocks/$f > /tmp/$f
  kubectl exec -i -n emr-flink kube-starrocks-fe-0 \
    -- mysql -h127.0.0.1 -P9030 -uroot < /tmp/$f
done
```

> **`SOURCE` will not work here.** It is a client-side mysql command that reads a
> path on the machine running the client, and the client is running inside the FE
> pod — which has no copy of this repo (`ls /` in `kube-starrocks-fe-0` shows a bare
> Ubuntu root). Pipe the rendered file into `kubectl exec -i` instead, or
> `port-forward` and run a local client where the relative paths do resolve.

### 3.4 Query Both Formats

```sql
-- Query Paimon tables
SET CATALOG paimon_catalog;
USE flink_paimon_db;
SELECT * FROM customers LIMIT 10;

-- Query Iceberg tables
SET CATALOG iceberg_catalog;
USE flink_iceberg_db;
SELECT * FROM customers LIMIT 10;

-- Cross-catalog comparison
SELECT
    'paimon' as source, COUNT(*) as cnt
FROM paimon_catalog.flink_paimon_db.customers
UNION ALL
SELECT
    'iceberg' as source, COUNT(*) as cnt
FROM iceberg_catalog.flink_iceberg_db.customers;
```

### 3.5 Monitor StarRocks

```bash
# Release, StarRocksCluster CR, and the StarRocks pods/services/PVCs only
./starrocks/deploy-starrocks.sh monitor

# Or directly. Filter by label — emr-flink also holds the Flink operator and the
# CDC TaskManagers, so an unfiltered get buries the 5 StarRocks pods.
kubectl get pods -n emr-flink -l app.kubernetes.io/component=fe
kubectl get pods -n emr-flink -l app.kubernetes.io/component=be

# Cluster-level health, from the operator's point of view
kubectl get starrockscluster -n emr-flink
```

`kubectl get starrockscluster` is the quickest single check —
`PHASE/FESTATUS/BESTATUS` all reading `running` means the operator considers the
cluster reconciled. Note the operator pod carries plain `app=kube-starrocks-operator`,
not the `app.kubernetes.io/component` label the FE/BE pods use, so it does not
appear in the label-filtered gets above.

**Cost:** ~$300–500/month for the 1 FE + 3 BE shape above (rough order of
magnitude; it is EC2 + EBS, so it tracks whatever nodes the BEs land on).

### 3.6 Scaling down and tearing down

**Scale the BEs down before the FE, not after.** The FE holds the cluster
metadata and the backend registry; if it goes away first, the operator has
nothing to reconcile the remaining BEs against and they are left orphaned.

Prefer editing the `StarRocksCluster` CR so the operator stays in agreement with
the desired state:

```bash
kubectl get starrockscluster -n emr-flink            # name is kube-starrocks
kubectl edit starrockscluster kube-starrocks -n emr-flink   # set starrocksBeSpec.replicas
```

Scaling the BE StatefulSet directly (`kubectl scale statefulset … -n emr-flink`)
works for a quick pause, but the operator will reconcile it back to the CR's
`replicas` — change the CR if you want the reduction to stick.

Tear down with:

```bash
./starrocks/deploy-starrocks.sh uninstall
```

> **The uninstall does not delete the namespace, deliberately.** `emr-flink` is
> shared with the Flink operator and the CDC jobs, so deleting it would take the
> whole pipeline with it. Older revisions of the script did
> `kubectl delete namespace $NAMESPACE` — safe only while the default was a
> dedicated `starrocks` namespace, and destructive once the default moved to
> `emr-flink` for the IRSA trust policy (3.1).
>
> **`helm uninstall` does not delete the PVCs either**, so the script deletes them
> explicitly by label. StatefulSet PVCs outlive the StatefulSet by design, and the
> deployed shape holds eight of them: 3 × 200 Gi BE data, 3 × 20 Gi BE log,
> 50 Gi FE meta, 5 Gi FE log — all gp3 (confirmed with
> `kubectl get pvc -n emr-flink`; note the 20 Gi BE log and 5 Gi FE log volumes are
> created by the chart and are not in `starrocks-eks-values.yaml`). After any
> uninstall, or after a bare `helm uninstall`:
> ```bash
> kubectl get pvc -n emr-flink -l app.kubernetes.io/component=be
> kubectl get pvc -n emr-flink -l app.kubernetes.io/component=fe
> ```
> and delete what you no longer need. Leftover PVCs also mean a fresh install can
> pick up stale BE data directories.
>
> The scale-down ordering (BEs before FE) is not asserted anywhere in the repo's
> scripts and was not exercised in-cluster; the PVC retention behaviour and the
> resource inventory above were verified against the live deployment.

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ServiceAccount "starrocks" … cannot be imported into the current release` | You are installing the chart twice. Collapse to one release (section 3.1) — the annotate workaround only defers the same failure to `ConfigMap/kube-starrocks-be-cm` |
| FE label query returns empty | Use `app.kubernetes.io/component=fe`. `app.starrocks.io/component` matches nothing on chart 1.11.7 |
| FE service endpoint is empty | Expected — the FE service is `ClusterIP`. Use `kubectl exec` or `port-forward` (section 3.2) |
| `kubectl get svc -l …component=fe` returns the wrong service | Two match; the headless `fe-search` sorts first. Filter `{.items[?(@.spec.clusterIP!="None")]}` |
| External catalogs fail on S3/Glue | `starrocks-sa` is outside `emr-flink`, so the execution role's trust policy (`system:serviceaccount:emr-flink:*`) does not cover it |
| Engine version changed between benchmark runs | `image.tag: ""` resolved to the mutable `4.1-latest`. Pin `IMAGE_TAG` (section 3.1) |
| FE "not alive yet" on first boot | Normal — FE tries to find peers, times out ~30s, then bootstraps as leader |
| Athena `PaimonStorageHandler` error | Use Athena SQL on a shadow Glue database (section 2.1), or the Hadoop catalog in Athena Spark (section 2.2) |
| Athena can't read Paimon's exported Iceberg metadata | Set `metadata.iceberg.format-version = '2'` in Paimon sink tables — Athena engine v3 reads Iceberg V2 only |

---

## Performance Comparison

Results below are from the 40k/s upsert run of **2026-09-22** (`bench-results/starrocks-audit-20260922T160312Z.csv`).
Every timing is the median of 3 server-side `Time=` values from the FE audit log, taken
**while the 40k/s load was still running** — so these are concurrent-ingest read latencies,
not quiet-table latencies.

> Measured on **Paimon 1.3.0**. The image has since moved to 1.3.2 (a patch bump, same
> table format and deletion-vector metadata), so these numbers remain the comparison
> baseline and were not re-measured. Re-run the suite before quoting them against any
> later minor version.
>
> Also captured before the generator gained hot-window sharding (`SHARD_HOT_KEYS`) and
> the C-extension MySQL driver. Those changes alter how the *write* rate is reached, not
> the key distribution the comparison depends on — each pod now draws from a disjoint
> slice, which makes individual keys hotter rather than colder. Read-side ratios are
> unaffected; the write rate at a given replica count is now higher.

### Workload

| Parameter | Value |
|-----------|-------|
| Write rate | **~39,960 rows/s** sustained (8 pods × 4,997/s measured avg) |
| Operation mix | 80% `INSERT … ON DUPLICATE KEY UPDATE` on existing PKs, 20% new inserts |
| Key skew | 60% of upserts into a 50,000-key hot window, 40% across the full key range |
| Table mix | order_items 45%, orders 30%, customers 15%, products 10% |
| Duration before benchmark | ~17 min sustained (≈41M row-modifications) |
| Generator | `mysql-data-generator/mysql-upsert-loadgen.yaml` (StatefulSet), 8 replicas × 4 threads × 500-row batches |
| Both pipelines | `RUNNING`, 34/34 checkpoints completed, 0 failed, parallelism 16 |

> Every row in this table is the configuration **as it was on 2026-09-22** and none
> of it matches the current manifests. The generator now ships `RATE_PER_POD 80000`
> / `THREADS 8` / `BATCH_ROWS 8000` / `UPSERT_RATIO 0.5` / `HOT_KEY_RATIO 0.5` /
> `HOT_KEY_WINDOW 5000000`, and both Flink jobs run at **parallelism 32**
> (808 tasks Paimon, 520 Iceberg). Re-run the suite before comparing anything
> against these figures — see [2-DATA-GEN-GUIDE.md](2-DATA-GEN-GUIDE.md) for the
> current values and why each changed.

### Table state at benchmark time

| Table | Paimon rows | Iceberg rows | Iceberg data files | pos-delete files | eq-delete files | delete records |
|-------|------------:|-------------:|-------------------:|-----------------:|----------------:|---------------:|
| customers | 3,978,888 | 3,994,246 | 544 | 521 | 544 | 10,090,377 |
| products | 4,084,032 | 4,095,398 | 306 | 288 | 306 | 8,366,034 |
| orders | 9,509,789 | 9,370,063 | 69 | 65 | 69 | 23,045,340 |
| order_items | 20,706,066 | 21,012,997 | 73 | 66 | 72 | 41,244,366 |

Iceberg carries roughly **one delete file per data file** and, on the large tables, *more
delete records than live rows*. That single fact drives every number below: StarRocks must
read the deletes to answer any query, so its scan volume is the row count plus the delete
volume, while Paimon's LSM has already merged them away.

### Snapshot queries (full-table, historical)

| Query | Paimon ms | Iceberg ms | Ratio | Paimon ScanRows | Iceberg ScanRows |
|-------|----------:|-----------:|------:|----------------:|-----------------:|
| Q1_count | 212 | 457 | **2.16x** | 5,299,513 | 13,289,772 |
| Q2_group_by | 246 | 555 | **2.26x** | 5,299,513 | 13,289,772 |
| Q3_join | 666 | 2,804 | **4.21x** | 16,815,938 | 45,746,838 |
| Q4_product_sales | 1,230 | 3,992 | **3.25x** | 29,016,478 | 63,787,030 |
| Q5_time_window | 347 | 2,323 | **6.69x** | 11,516,425 | 32,457,066 |
| Q6_multi_join | 1,616 | 5,094 | **3.15x** | 45,832,416 | 109,533,868 |

### Hot-data queries (recent upserts, `UTC_TIMESTAMP()` windows)

| Query | Paimon ms | Iceberg ms | Ratio | Paimon ScanRows | Iceberg ScanRows |
|-------|----------:|-----------:|------:|----------------:|-----------------:|
| Q7_freshness | 218 | 1,955 | **8.97x** | 2,752,437 | 16,249,483 |
| Q8_hot_pk_lookup | 480 | 2,880 | **6.00x** | 12,042,127 | 48,762,824 |
| Q9_recent_window | 235 | 2,378 | **10.12x** | 4,829,282 | 19,358,652 |
| Q10_hot_join | 693 | 4,422 | **6.38x** | 29,583,354 | 72,966,002 |

### What the numbers say

- **Paimon wins every query, and wins hot data far more decisively than snapshot data.**
  Snapshot ratios sit at 2.2–6.7x; hot-window ratios at 6.0–10.1x. The hot path is exactly
  where the upserts are landing, so it carries the freshest and densest delete files.
- **The cause is scan volume, not CPU.** Iceberg's ScanRows exceeds Paimon's by 2.5–4.0x on
  the same logical result. Q9 is the clearest case: 4.8M rows scanned vs 19.4M for the same
  answer — a 4.0x scan difference producing a 10.1x time difference (super-linear because
  merge-on-read must also *apply* the deletes, not just read them).
- **Ratios track delete density per table, which is the mechanism working as predicted.**
  Q7 (freshness, single-table) at 9.0x sits on tables whose delete records outnumber live
  rows ~2:1. Q1/Q2 at ~2.2x are the cheapest because a `COUNT`/`GROUP BY` can lean on
  metadata more than a join can.
- **Paimon pays for this on the write side, in storage and checkpoints.** Paimon's warehouse
  is 2.94 GB / 6,708 objects against Iceberg's 1.86 GB / 3,564 — background compaction
  rewrites data, so it costs ~1.6x the storage and ~1.9x the object count. Avg checkpoint
  duration was 11,745 ms for Paimon vs 981 ms for Iceberg (**12x**): Paimon's writers do
  merge work inside the checkpoint that Iceberg defers to read time. That is the trade being
  measured — Paimon moves cost from query time to write time, Iceberg does the reverse.

### Reproducing

```bash
# Flink-side: records written, throughput, checkpoint success/duration
./monitoring/capture-flink-metrics.sh

# StarRocks-side: server-side Time/ScanRows/ScanBytes per query
./sql-scripts/starrocks/bench-server-side.sh            # snapshot + hot suites
SUITE=hot RUNS=5 ./sql-scripts/starrocks/bench-server-side.sh

# Upsert load (8 pods). This is a StatefulSet, not a Deployment:
# each pod derives its own disjoint slice of the hot-key window from its ordinal,
# and only a StatefulSet hands out exact 0..N-1 ordinals. If the older Deployment
# version was ever applied, delete it first — apply cannot convert kinds in place.
kubectl delete deployment mysql-upsert-loadgen -n emr-flink --ignore-not-found
kubectl apply -f mysql-data-generator/mysql-upsert-loadgen.yaml

# or, equivalently, via the management script:
./mysql-data-generator/deploy-data-generator.sh deploy upsert

# stop the load when the benchmark is done:
./mysql-data-generator/deploy-data-generator.sh stop upsert
```

Note the StarRocks catalogs must exist and point at the *current* warehouses — this cluster
still carries stale `paimon_catalogv3` / `icebergv3_catalog` entries from an earlier run,
and the first benchmark attempt returned all-zero timings purely because `paimon_catalog`
did not exist yet. All-zero `ScanRows` means the queries errored, not that they were fast.

### Measurement notes — read before trusting any number

- **Use `bench-server-side.sh`, not `bench-hot-data.sh`.** Timings must come from the
  StarRocks FE audit log (`/opt/starrocks/fe/log/fe.audit.log` — the `Time=`, `ScanRows=`
  and `ScanBytes=` fields), never from a stopwatch around a client call. A
  `kubectl exec ... mysql -e` wrapper costs ~5.5 s on this cluster, which swamped every
  query: a bare `SELECT 1;` measured 5,665 ms while a `COUNT(*)` measured 6,079 ms, and
  the audit log recorded that same `COUNT(*)` as `Time=684`. About 89% of each reading was
  harness, so the resulting ratios compared process-spawn overhead rather than StarRocks.
  `bench-hot-data.sh` still times client-side; its absolute numbers are not quotable.
- **`ScanRows` is what makes a timing interpretable.** A query that looks fast may simply
  have matched nothing. Always report scan volume next to elapsed time.
- **Hot-window predicates must use `UTC_TIMESTAMP()`, never `NOW()`.** The FE session
  timezone is +08:00 while CDC writes UTC, so `NOW()` silently matches zero rows and a
  broken query reads as a fast one.
- **Check pipeline health before believing a result.** A Flink job reports `RUNNING` with
  dead sink committers — `/jobs` looks fine while the table is frozen. Confirm freshness
  (`MAX(updated_at)` vs `UTC_TIMESTAMP()`) and checkpoint counts on both sides first;
  comparing a live table against a stalled one measures nothing. When you check
  checkpoints, read the `history` array from `/jobs/<id>/checkpoints`, **not** the
  `counts.failed` / `cp_fail` number — that counter is cumulative for the life of
  the job and never decreases, so an unchanged value proves nothing either way.
  See [3-MONITOR.md](3-MONITOR.md#checkpoint-failure-counts-are-cumulative-not-current).
- **A benchmark run can be silently invalidated by a TaskManager restart.** Karpenter
  consolidation evicts nodes that are still hosting RUNNING TaskManagers, and with
  `failover-strategy: full` one lost TM restarts the entire task graph. Confirm the
  jobs were not restarting during the window — see the troubleshooting entry in the
  [README](README.md#troubleshooting-quick-reference).
- **Compare like with like.** Paimon compacts continuously in the background; Iceberg
  needs an explicit `rewrite_data_files`. Capture Iceberg both before and after
  compaction, and say which state each number came from.

---

## Cost Comparison (Monthly, 1TB data, 1000 queries/day)

| Component | Athena | StarRocks |
|-----------|--------|-----------|
| **Compute** | $1,500 (queries) | $350 (EC2 instances) |
| **Storage** | $23 (S3) | $23 (S3) + $50 (EBS) |
| **Metadata** | $1 (Glue) | Included |
| **Infrastructure** | $0 | $100 (management) |
| **Total** | **~$1,524** | **~$523** |

- **Athena**: Better for sporadic queries (<100/day)
- **StarRocks**: Better for frequent queries (>500/day)
- **Break-even**: ~300 queries/day

---

## Use Case Matrix

| Use Case | Recommendation |
|----------|---------------|
| **Ad-hoc Analysis** | Athena |
| **Real-time Dashboards** | StarRocks + Paimon |
| **Historical Analysis** | StarRocks + Iceberg |
| **BI Integration** | Athena (QuickSight native) |
| **Data Science** | StarRocks (faster iteration) |
| **Cost Optimization** | Athena (sporadic) or StarRocks (frequent) |
| **Operational Analytics** | StarRocks + Paimon |
| **Compliance/Audit** | Iceberg (time travel) |

---

## Architecture Decision Tree

```
Start: Need OLAP Query Layer
│
├─ Query Frequency?
│  ├─ < 100 queries/day → Athena
│  └─ > 500 queries/day → StarRocks
│
├─ Latency Requirements?
│  ├─ Sub-second needed → StarRocks
│  └─ Seconds acceptable → Athena or StarRocks
│
├─ Infrastructure Preference?
│  ├─ Serverless → Athena
│  └─ Self-managed OK → StarRocks
│
├─ Lakehouse Format?
│  ├─ Paimon → StarRocks (best support)
│  ├─ Iceberg → Athena or StarRocks (both good)
│  └─ Both → StarRocks (query both)
│
└─ BI Tool Integration?
   ├─ QuickSight → Athena (native)
   ├─ Tableau/Superset → StarRocks (JDBC)
   └─ Custom → Either
```

---

**Status**: ✅ Production Ready
**Last Updated**: 2026-09-27
**Technologies**: Flink 1.20, Paimon 1.3.2, Iceberg 1.10.0-amzn-1, StarRocks 4.1.4
(benchmark numbers above were captured on Paimon 1.3.0 — see the note in
[Performance Comparison](#performance-comparison))
