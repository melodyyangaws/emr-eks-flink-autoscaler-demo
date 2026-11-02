# MySQL Data Generator Guide

## Overview

The **MySQL upsert load generator** (`mysql-data-generator/mysql-upsert-loadgen.yaml`,
StatefulSet `mysql-upsert-loadgen`, 4 replicas) is the only CDC source for this
pipeline. Each pod runs a Python script that writes batched
`INSERT … ON DUPLICATE KEY UPDATE` statements against the `ecommerce` database
(`customers`, `products`, `orders`, `order_items`), so the binlog carries `+I` events
for new keys and `-U`/`+U` pairs for existing ones. `deploy-data-generator.sh` manages it.

| | |
|---|---|
| Manifest | `mysql-upsert-loadgen.yaml` |
| Kubernetes kind | **StatefulSet**, 4 replicas |
| Statements | `INSERT … ON DUPLICATE KEY UPDATE`, `BATCH_ROWS` rows/statement (manifest: 5000) |
| Rate | measure it — `RATE_PER_POD` is 80,000, a deliberately unreachable token-bucket ceiling |
| DELETE events | none |
| Use it for | the Paimon-vs-Iceberg storage benchmark and the hot query test |

> **The mixed generator is gone.** The row-at-a-time INSERT/UPDATE/DELETE generator
> (`mysql-data-generator.yaml`, Deployment `mysql-data-generator`, ~400 rows/s) was
> removed on 2026-11-02; it is recoverable from git history. It was the only source of
> DELETE events, so **the benchmark has no DELETE events**: everything it measures comes
> from inserts and upserts.

> **`RATE_PER_POD` is not a throughput claim.** It is set
> far above anything the instance has delivered (4 x 80,000 = 320,000 rows/s of
> *target*; the 2026-11-02 dry run reached ~64,000 rows/s on 8 pods) precisely so the bucket
> never binds and the pods run flat out. Read the achieved rate off the pod logs
> (`inst=`), and read the gap to the target as a headroom measurement.

Repeated upserts on a skewed hot-key window are what
make Iceberg accumulate equality-delete files while Paimon's LSM compacts them away; a
uniformly random key stream over the whole id range would measure raw ingest and show neither effect.

> **The generator is a StatefulSet, not a Deployment.** Each pod owns a *disjoint
> slice* of the hot-key window and derives that slice from its ordinal, and only a
> StatefulSet hands out exact `0..N-1` ordinals (`mysql-upsert-loadgen-0` … `-3`). This
> matters for two commands: `kubectl` calls must say `statefulset`, not `deployment`, and
> if you ever ran the older Deployment version you must `kubectl delete deployment
> mysql-upsert-loadgen -n emr-flink` first — `kubectl apply` cannot convert a Deployment
> into a StatefulSet of the same name.

---

## Running the generator

Used by the Paimon-vs-Iceberg comparison and the hot query test
([querybench/README.md](querybench/README.md)). Run the script from `cdc-pipeline/`:
its manifest path (`mysql-data-generator/mysql-upsert-loadgen.yaml`) is relative to
that directory. A trailing `upsert` argument (`deploy upsert`, `scale 4 upsert`) and
`WORKLOAD=upsert` are still accepted from the two-generator days; `WORKLOAD=mixed`
is rejected.

### Step 1 — Deploy

1. Load the connection values. The script creates the `mysql-credentials` secret from
   them on the first deploy, and reuses an existing one after that:

   ```bash
   cd cdc-pipeline
   source mysql-cdc-env.sh          # MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD
   ```

2. One time only, and only if the older Deployment version was ever applied
   (`kubectl apply` cannot turn a Deployment into a StatefulSet of the same name):

   ```bash
   kubectl delete deployment mysql-upsert-loadgen -n emr-flink --ignore-not-found
   ```

3. Deploy:

   ```bash
   ./mysql-data-generator/deploy-data-generator.sh deploy
   ```

4. Check that all 4 pods came up together (`podManagementPolicy: Parallel`) and that
   each logged a distinct ordinal and the live id ranges:

   ```bash
   kubectl logs -l app=mysql-upsert-loadgen -n emr-flink --tail=-1 \
     | grep -E '^(pod=|id ranges:|mysql C extension)'
   ```

   ```
   mysql C extension: available
   pod=mysql-upsert-loadgen-3 ordinal=3 of 4
   id ranges: {'customers': (<min>, <max>), 'products': (…), 'orders': (…), 'order_items': (…)}
   ```

   A duplicate ordinal means two pods are fighting over the same rows.

### Step 2 — Read the achieved rate

```bash
./mysql-data-generator/deploy-data-generator.sh status
kubectl logs -l app=mysql-upsert-loadgen -n emr-flink --tail=40 | grep '^\['
```

Each pod prints a counter line every `REPORT_SECONDS` (15):

```
[  120s] rows=1,234,567 inst=8,104/s avg=7,910/s upserts=… inserts=… batches=… errors=… | <per-table rows>
```

- Quote **`inst=`** (the last 15 s), summed across the pods, after the pods have
  settled for a few intervals. `avg=` is a lifetime average since pod start. It
  stays diluted by the ramp-up and by any earlier settings for as long as the pod
  lives.
- `status` prints an "aggregate" line that sums `avg=`, not `inst=`. Use it
  only on pods that have run unchanged for a long time.
- **`errors=` explains a low `inst=`.** Deadlocks (1213) and lock-wait timeouts
  (1205) are retried up to 3 times and never printed. Each one rolls back the whole
  multi-row statement. `errors=` counts every failed attempt, and `batches=` counts
  only committed batches, so compare `errors=` against `batches=` before you blame
  RDS. `1153` (`max_allowed_packet`) has not been seen at these settings.

`RATE_PER_POD` is a token-bucket ceiling set far above anything the instance has
delivered (4 x 80,000 = 320,000 rows/s of *target*), so the bucket never binds and
the pods run flat out. The achieved rate is the real limit of MySQL plus the driver.

### Rates seen

| Run | Settings | Aggregate `inst=` | `errors=` / `batches=` | RDS CPU |
|---|---|---:|---:|---:|
| **Dry run, 2026-11-02** (source shrunk to ~37.5M rows) | manifest values below, but 8 pods | **~64,000 rows/s** | **~34%** | **64–79%** |
| Best earlier measurement at 8 pods | 64,000 IOPS volume, full-size source | 94,327 rows/s | — | — |

On the dry run the bottleneck was RDS CPU plus row-lock contention, not storage. The
Flink jobs kept up with that rate. The manifest went from 8 to 4 pods afterwards
(2026-11-02) to cut that contention; the 4-pod rate is not measured yet.

### Step 3 — Restart the pods after a source shrink

Each pod reads `MIN(pk)` and `MAX(pk)` per table **once, at startup**. After the
source tables are shrunk, pods that started before the shrink keep the old `MIN` and
regrow the table (see [the cold tail](#the-cold-tail-spans-minpk-maxpk-not-1-maxpk)).
Restart them all at once, then check the new `id ranges:` lines as in Step 1:

```bash
kubectl scale statefulset mysql-upsert-loadgen -n emr-flink --replicas=0
kubectl scale statefulset mysql-upsert-loadgen -n emr-flink --replicas=4
```

`kubectl rollout restart statefulset/mysql-upsert-loadgen -n emr-flink` also works,
but it replaces pods one at a time, so for a few minutes old and new ranges run side
by side.

### Step 4 — Stop or pause

```bash
./mysql-data-generator/deploy-data-generator.sh stop
```

This deletes the StatefulSet, its headless Service and the script ConfigMap. To pause
without deleting anything, which is also how to bound a measurement window, scale to
zero. `DURATION_SECONDS=0` means run forever, and a pod that exited by itself would
just be restarted by the StatefulSet:

```bash
kubectl scale statefulset mysql-upsert-loadgen -n emr-flink --replicas=0
```

The query benchmark's snapshot test scales it to 0 the same way
(`GENERATOR_RESOURCES` in `.bench-env`).

### Scale

```bash
./mysql-data-generator/deploy-data-generator.sh scale 16
```

The script sets `POD_COUNT=16` **before** scaling. `POD_COUNT` is the divisor that
sizes each pod's hot-key slice (`HOT_SLICE = HOT_KEY_WINDOW // POD_COUNT`), so it has
to track the replica count. Too low, and slices overlap and the cross-pod lock convoy
returns. Too high, and part of the window goes unwritten. If you scale with a bare
`kubectl scale` to a new count, set `POD_COUNT` yourself.

### Tune rate and key skew

```bash
# rate/pod, worker threads, rows per statement (script defaults = manifest values)
./mysql-data-generator/deploy-data-generator.sh config 80000 8 5000

# upsert ratio, hot-key ratio, hot-key window (script defaults = manifest values)
./mysql-data-generator/deploy-data-generator.sh upsert-mix 0.5 0.6 5000000
```

`config` changes *how fast*. `upsert-mix` changes *what is being measured*.

Both commands are `kubectl set env`, which starts a rolling restart. The StatefulSet
replaces pods one at a time in descending ordinal order, so for 4 pods the cluster
runs a mix of old and new settings for a few minutes. Do not start a measurement
window until every pod has printed its startup banner again, and do not interrupt a
rollout, because the pods not yet replaced keep the old values.

### Current manifest values

`mysql-data-generator/mysql-upsert-loadgen.yaml` is the ground truth; the script's
CLI defaults copy it. A `config` or `upsert-mix` run changes the live StatefulSet
but not this file: check the live values with
`kubectl get statefulset mysql-upsert-loadgen -n emr-flink -o jsonpath='{.spec.template.spec.containers[0].env}'`.

| Variable | Value | Note |
|---|---|---|
| `replicas` | 4 | `podManagementPolicy: Parallel`; was 8 until 2026-11-02 |
| `RATE_PER_POD` | 80000 | token-bucket ceiling, deliberately unreachable |
| `THREADS` | 8 | |
| `BATCH_ROWS` | 5000 | the measured optimum; 10,000 was worse. Multiplies with `THREADS` x `replicas` into lock contention |
| `UPSERT_RATIO` | 0.5 | the rest are new inserts |
| `HOT_KEY_RATIO` | 0.6 | share of upserts drawn from the hot window |
| `HOT_KEY_WINDOW` | 5000000 | sized to the buffer pool, about 16 GB of rows |
| `POD_COUNT` | 4 | must equal `replicas` |
| `SHARD_HOT_KEYS` | true | a disjoint hot slice per ordinal |
| `REPORT_SECONDS` | 15 | interval behind `inst=` |
| `DURATION_SECONDS` | 0 | run forever |
| `USE_PURE` | false | use the C extension; pure Python caps a pod near 5,000 rows/s |
| resources | requests 2 cpu / 2 Gi, limits 4 cpu / 4 Gi | `nodeSelector: karpenter.sh/nodepool: driver-nodepool`, off the TaskManager pool |

The Python defaults (used only when a variable is unset) are `HOT_KEY_RATIO=0.95`
and `HOT_KEY_WINDOW=2000000`. The manifest overrides both.

### How keys are picked

#### The hot window

`HOT_KEY_RATIO` of the upserts land in the newest `HOT_KEY_WINDOW` ids,
`[max(MIN, MAX - HOT_KEY_WINDOW), MAX]`. With `SHARD_HOT_KEYS=true` each pod draws
only from its own slice of that window (`HOT_SLICE` keys starting at
`ordinal x HOT_SLICE`), so pods do not collide. Rewriting the same keys over and over
is what makes Iceberg accumulate equality deletes while Paimon's LSM compacts them.

Keys within a batch are sorted ascending, so every session takes row locks in the
same order. In random order, about 9% of batches deadlocked.

#### Sizing `HOT_KEY_WINDOW`: it is a buffer-pool number

A window only behaves as "hot" while the rows it spans fit in the InnoDB buffer pool.
Past that, the hot draws miss cache just like the cold tail, and the skew buys
nothing:

```
window = 0.65 x buffer_pool_bytes / avg_row_bytes
```

On `db.m5.2xlarge` (32 GiB, `innodb_buffer_pool_size = {DBInstanceClassMemory*3/4}`,
about 24 GB) with rows of about 3.36 KB:

| `HOT_KEY_WINDOW` | Span | vs 24 GB pool |
|---:|---:|---|
| 200,000,000 | 641 GB | 27x the pool. This ran once, and it was wrong |
| **5,000,000** (current) | 16 GB | fits, with room for the cold tail |

At 200,000,000: ReadIOPS 6,619 against WriteIOPS 8,690 (nearly every upsert read a
page from disk first), CPU 60–73%, WriteIOPS 14% of the 64,000 provisioned, about 50%
of batches failing, about 3,000 rows/s per pod. Buffer-pool misses and CPU were the
wall, not storage. Expect 1213/1205 errors to remain at the narrower window:
concentrating writes raises per-key contention even as it fixes cache misses.

#### The cold tail spans `[MIN(pk), MAX(pk)]`, not `[1, MAX(pk)]`

At startup each pod reads `MIN(pk)` and `MAX(pk)` per table (both ends of the primary
key index, so both are instant lookups) and logs them on the `id ranges:` line as
`(min, max)` pairs. The cold `1 - HOT_KEY_RATIO` of upserts is drawn uniformly from
`[MIN, MAX]`.

The lower bound matters because the source tables were shrunk to their newest ~10% of
ids (2026-11-02, see [0-MYSQL-SETUP-GUIDE.md](./0-MYSQL-SETUP-GUIDE.md#step-1-create-rds-mysql-instance)).
After that, about 90% of `[1, MAX]` is ids that no longer exist. A cold "upsert" on
one of them finds no row, `ON DUPLICATE KEY UPDATE` takes the INSERT path at that old
id, and the table grows back toward its old size. Drawing from `[MIN, MAX]` keeps cold
upserts on live rows. A contiguous newest-N shrink leaves no holes inside that range.
Empty tables read as `(1, 1)`. Because the range is read once, restart the pods after
a shrink ([Step 3](#step-3--restart-the-pods-after-a-source-shrink)).

Foreign-key parents for new `orders` and `order_items` come from a sample of the
newest 20,000 customer, product and order ids, also read at startup.

#### `BATCH_ROWS` is not monotonic

Bigger batches amortize the redo-log flush, but a batch is **one transaction holding
every row lock it touches for the whole statement**. At `BATCH_ROWS=10000`: `inst=`
4,000/s per pod with 53 of 130 batches erroring, about 30,000 rows/s aggregate, while
RDS had about 5x storage headroom (`DiskQueueDepth` 18–76: requests queued behind lock
waits, not behind the device). 5,000 is the manifest value. Do not raise it without
also reducing `THREADS` x `replicas`.

### Measured ceiling (historical)

> **This section is a historical run at 12,000 provisioned IOPS, on the full-size
> source (before the 2026-11-02 shrink).** The instance is now 500 GB gp3 with
> **64,000 IOPS / 2,500 MB/s**, so these absolute numbers no longer bound anything.
> The *shape* still holds: adding replicas past saturation buys lock contention, not
> throughput.

A replica ramp (`mysql-data-generator/find-rds-ceiling.sh`) found the aggregate rate
peaked near **32 replicas at ~69,000 rows/s and then declined**:

| replicas | target | achieved | % of target | RDS CPU | write IOPS |
|---------:|-------:|---------:|------------:|--------:|-----------:|
| 8 | 40,000 | 34,989 | 87.5% | 32% | 7,363 |
| 16 | 80,000 | 58,265 | 72.8% | 47% | 8,013 |
| 24 | 120,000 | 65,914 | 54.9% | 47% | 7,955 |
| **32** | 160,000 | **68,818** | 43.0% | 48% | 7,997 |
| 40 | 200,000 | 66,404 | 33.2% | 46% | 7,481 |

This was first read as "row-lock contention, not hardware", because write IOPS sat at
about 8,000 of 12,000. That ignored reads: ReadIOPS 5,655 + WriteIOPS 6,518 = 11,878
of 12,000. The instance was IOPS-saturated, and the reads came from the cold tail
missing the buffer pool. Fixing the key distribution took throughput from about 15,000
to about 58,300 rows/s at the same 8 replicas and moved the bottleneck to CPU.

Re-run the ramp before quoting a ceiling:

```bash
# hold_seconds, then replica steps; results in ./bench-results/rds-ceiling-<UTC>.csv
./mysql-data-generator/find-rds-ceiling.sh 300 8 16 24 32 40
```

---

## Verify CDC Pipeline is Working

### **1. Check MySQL Data**

```sql
-- mysql -h $MYSQL_HOST -u $MYSQL_USER -p ecommerce
SELECT
    (SELECT COUNT(*) FROM customers) AS customers,
    (SELECT COUNT(*) FROM products) AS products,
    (SELECT COUNT(*) FROM orders) AS orders,
    (SELECT COUNT(*) FROM order_items) AS order_items;

-- freshness on the source, to compare with the lakehouse side
SELECT MAX(updated_at), UTC_TIMESTAMP() FROM orders;
```

With the generator running, the counts should grow only by the insert share
(`1 - UPSERT_RATIO`). If a shrunk table grows much faster, the pods are running with
a stale `MIN` ([Step 3](#step-3--restart-the-pods-after-a-source-shrink)).

### **2. Check Flink CDC Ingestion**

```bash
kubectl get flinkdeployment -n emr-flink
kubectl logs -f -l app=flink-cdc-iceberg,component=jobmanager -n emr-flink -c flink-main-container
```

For committed rows/s per table, checkpoint health and delete-file growth, use the
monitor ([3-MONITOR.md](./3-MONITOR.md)).

### **3. Check Lakehouse Data (S3)**

The databases carry the table-set suffix (`CDC_DB_SUFFIX` in `.bench-env`, `_v3r1`
now):

```bash
source .bench-env
# Paimon: <warehouse>/<db>.db/<table>/
aws s3 ls s3://${BUCKET_NAME}/paimon-warehouse/flink_paimon_db${CDC_DB_SUFFIX}.db/orders/snapshot/ | tail -3
# Iceberg (GlueCatalog default location): <warehouse>/<db>.db/<table>/metadata/
aws s3 ls s3://${BUCKET_NAME}/iceberg-warehouse/flink_iceberg_db${CDC_DB_SUFFIX}.db/orders/metadata/ | tail -3
```

New `snapshot-N` and `*.metadata.json` objects should appear about once a minute (one
per 60 s checkpoint).

### **4. Query the tables**

Athena reads Iceberg v2 only, so it cannot read the v3 table set. Query it with the
DuckDB and Trino arms of [querybench/](./querybench/README.md). On a v2 set
(`ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX=`), Athena reads `flink_iceberg_db`
directly ([README Step 4](./README.md#step-4--olap-query-layer-athena)):

```sql
SELECT COUNT(*) AS total_customers, MAX(updated_at) AS latest_update
FROM flink_iceberg_db.customers;
```

Hot-window predicates must use UTC: CDC writes UTC timestamps.

---

## Troubleshooting

### **Data Generator Not Starting**

```bash
# Check pod status
kubectl get pods -l app=mysql-upsert-loadgen -n emr-flink

# If CrashLoopBackOff, check logs
kubectl logs -l app=mysql-upsert-loadgen -n emr-flink --tail=100

# Common issues:
# 1. MySQL credentials secret not found
kubectl get secret mysql-credentials -n emr-flink

# 2. Cannot connect to RDS (security group)
# - Verify RDS security group allows traffic from EKS
# - Check RDS endpoint is correct
```

### **Achieved Rate Too Low**

`RATE_PER_POD` is already an unreachable ceiling, so raising it changes nothing.
Compare `errors=` with `batches=` in the counter lines first (lock contention), then
RDS CPU and IOPS; see [Rates seen](#rates-seen) and [How keys are picked](#how-keys-are-picked).

### **Too Much Load**

```bash
# Cap the per-pod rate so the token bucket binds (rate, threads, rows/batch)
./mysql-data-generator/deploy-data-generator.sh config 5000 8 5000

# Fewer pods (POD_COUNT follows)
./mysql-data-generator/deploy-data-generator.sh scale 2

# Pause, or stop completely
kubectl scale statefulset mysql-upsert-loadgen -n emr-flink --replicas=0
./mysql-data-generator/deploy-data-generator.sh stop
```

### **Database Connection Errors**

```bash
# Test connection from pod
kubectl run mysql-test --rm -it --restart=Never \
  --image=mysql:8.0 \
  --namespace=emr-flink \
  --command -- mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1;"
```

---

## Resource Usage

Per pod: requests 2 cpu / 2 Gi, limits 4 cpu / 4 Gi, on `driver-nodepool` (off the
TaskManager pool). Throughput: see [Rates seen](#rates-seen).

---

## Integration with CDC Pipeline

### **Complete Flow**

```
1. Generator pods (StatefulSet mysql-upsert-loadgen)
   ↓ SQL: INSERT … ON DUPLICATE KEY UPDATE (no DELETEs)
2. MySQL RDS
   ↓ Binlog events
3. Flink CDC Connector
   ↓ Change records
4. Paimon/Iceberg Tables
   ↓ Parquet files + metadata
5. AWS Glue Catalog
   ↓ Table registration
6. Query Engines (DuckDB, Trino; Athena on v2 only)
```

### **Monitoring End-to-End Lag**

Compare `MAX(updated_at)` in MySQL with the same column on the lakehouse side
(DuckDB or Trino through querybench; Athena on a v2 table set only), both in UTC.
With 60 s checkpoints, a healthy pipeline lags by about one checkpoint interval
plus the commit time. Several minutes of lag means the writers are falling behind
or checkpoints are failing; check the monitor's per-table committed rows/s and
"Failed Checkpoints (Δ)".

---

## Commands Reference

```bash
# One-time, only if the older Deployment version was ever applied
kubectl delete deployment mysql-upsert-loadgen -n emr-flink --ignore-not-found

# Deploy: 4 pods, RATE_PER_POD 80000 (an unreachable ceiling by design)
./mysql-data-generator/deploy-data-generator.sh deploy

# Status + achieved rate per pod (quote the summed inst=, not avg=)
./mysql-data-generator/deploy-data-generator.sh status

# Restart all pods at once, e.g. after a source shrink (re-reads MIN/MAX ids)
kubectl scale statefulset mysql-upsert-loadgen -n emr-flink --replicas=0
kubectl scale statefulset mysql-upsert-loadgen -n emr-flink --replicas=4

# Pause without deleting (also how to bound a measurement window)
kubectl scale statefulset mysql-upsert-loadgen -n emr-flink --replicas=0

# Stop (removes StatefulSet + headless Service + ConfigMap)
./mysql-data-generator/deploy-data-generator.sh stop

# Live logs
./mysql-data-generator/deploy-data-generator.sh logs

# Scale: also sets POD_COUNT so hot-key slices stay disjoint
./mysql-data-generator/deploy-data-generator.sh scale 16

# Rate: rows/s per pod, threads, rows per statement (rolling restart)
./mysql-data-generator/deploy-data-generator.sh config 80000 8 5000

# Key skew: upsert ratio, hot-key ratio, hot-key window (rolling restart)
./mysql-data-generator/deploy-data-generator.sh upsert-mix 0.5 0.6 5000000

# Find the RDS ceiling: hold_seconds, then replica steps
./mysql-data-generator/find-rds-ceiling.sh 300 8 16 24 32 40
```

The manifest values are in [Current manifest values](#current-manifest-values).

---

## Summary

✅ **One generator**: StatefulSet `mysql-upsert-loadgen`, 4 pods, rate is measured
   not configured. The mixed INSERT/UPDATE/DELETE generator was removed 2026-11-02,
   so the benchmark has no DELETE events
✅ **Deployed in**: `emr-flink` namespace, run from `cdc-pipeline/`
✅ **Purpose**: the storage-format benchmark and the hot query test
✅ **Operations**: batched `ON DUPLICATE KEY UPDATE` on 4 tables
✅ **Keys**: hot window sharded per pod; cold tail drawn from the live
   `[MIN(id), MAX(id)]`, read at pod start (restart the pods after a shrink)
✅ **Measured**: ~64,000 rows/s at 8 pods on the 2026-11-02 dry run (RDS CPU 64–79%,
   errors/batches ~34%). Re-measure; never quote a stale ceiling.
