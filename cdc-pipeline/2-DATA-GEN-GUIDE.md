# MySQL Data Generator Guide

## Overview

The **MySQL upsert load generator** (`mysql-data-generator/mysql-upsert-loadgen.yaml`,
StatefulSet `mysql-upsert-loadgen`, 4 replicas) is the only CDC source for this
pipeline. Each pod runs a Python script that writes batched
`INSERT … ON DUPLICATE KEY UPDATE` statements against the `ecommerce` database
(`customers`, `products`, `orders`, `order_items`), so the binlog carries `+I` events
for new keys and `-U`/`+U` pairs for existing ones. On `orders` and `order_items` a
small share of each batch is a multi-row `DELETE`, which shows up as `-D` events.
`deploy-data-generator.sh` manages it.

| | |
|---|---|
| Manifest | `mysql-upsert-loadgen.yaml` |
| Kubernetes kind | **StatefulSet**, 4 replicas |
| Statements | `INSERT … ON DUPLICATE KEY UPDATE`, `BATCH_ROWS` rows per batch (manifest: 5000); on `orders` / `order_items`, `DELETE_RATIO` of those rows go to a `DELETE … WHERE pk IN (…)` instead |
| Rate | measure it — `RATE_PER_POD` is 80,000, a deliberately unreachable token-bucket ceiling |
| DELETE events | `DELETE_RATIO` 0.05 of `orders` / `order_items` rows (~3.75% of all rows, plus the items deleted with their orders); never `customers` / `products`. Added 2026-11-03, not yet measured on RDS |
| Iceberg position deletes | **design target ~50%** of the Iceberg writer's deletes (`RECENT_KEY_RATIO`), not yet measured; ~2% before 2026-11-02 |
| Use it for | the Paimon-vs-Iceberg storage benchmark and the hot query test |

> **The mixed generator is gone.** The row-at-a-time INSERT/UPDATE/DELETE generator
> (`mysql-data-generator.yaml`, Deployment `mysql-data-generator`, ~400 rows/s) was
> removed on 2026-11-02; it is recoverable from git history. It was the only source of
> DELETE events, so from 2026-11-02 until `DELETE_RATIO` was added to the upsert
> generator (2026-11-03) the benchmark had no DELETE events. Results from that period
> measure inserts and upserts only. See [Deletes](#deletes-and-foreign-keys).

> **`RATE_PER_POD` is not a throughput claim.** It is set
> far above anything the instance has delivered (4 x 80,000 = 320,000 rows/s of
> *target*; the 2026-11-02 dry run reached ~64,000 rows/s on 8 pods) precisely so the bucket
> never binds and the pods run flat out. Read the achieved rate off the pod logs
> (`inst=`), and read the gap to the target as a headroom measurement.

Repeated upserts on a skewed hot-key window are what
make Iceberg accumulate equality-delete files while Paimon's LSM compacts them away; a
uniformly random key stream over the whole id range would measure raw ingest and show neither effect.

On top of that, `RECENT_KEY_RATIO` (0.6) of every batch **re-writes keys the same
worker thread wrote a few seconds earlier**, so that about half of the deletes the
Iceberg writer produces are position deletes (deletion vectors on v3) instead of
equality deletes. See [Recent re-writes](#recent-re-writes-iceberg-position-deletes).

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
   cd cdc-pipeline                  # MYSQL_HOST / MYSQL_USER come from .bench-env(.local);
                                    # the password from Secret mysql-credentials
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
[  120s] rows=1,234,567 inst=8,104/s avg=7,910/s upserts=… inserts=… recent=… deletes=… deleted=… batches=… errors=… | <per-table rows>
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
- **`recent=`** counts rows that re-wrote a key from the thread's recent-key buffer.
  It is a subset of `upserts=` (so `upserts= + inserts= = rows=` still holds), and
  cumulative like the other counters. `recent= / rows=` is the achieved re-write
  share; expect it a few points under `RECENT_KEY_RATIO` (the simulation gave ~0.55
  at 0.6), because a buffer that runs short hands its rows to the normal path.
  `status` prints it as a percentage. It says nothing yet about Iceberg: for that,
  see [Measure the position-delete share](#measure-the-position-delete-share).
- **`deletes=`** counts the keys put into `DELETE` statements, so
  `upserts= + inserts= + deletes= = rows=`, and `rows=`/`inst=` include deletes.
  **`deleted=`** counts the rows MySQL actually removed (`rowcount`): the deletes
  that found their row, plus the `order_items` removed together with their orders.
  Each of those is one `-D` in the binlog. `deleted=` can be above `deletes=` (an
  order's items) or below it (a key already gone). `status` prints `deletes=` as a
  share of `rows=` (~3.7% at the manifest values). Failed delete transactions count
  in `errors=` like failed upserts. `batches=` counts upsert statements only.

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

# upsert ratio, hot-key ratio, hot-key window (script defaults = manifest values);
# optional 5th/6th: RECENT_KEY_RATIO, RECENT_KEY_MAX_AGE_S (left as-is if omitted)
./mysql-data-generator/deploy-data-generator.sh upsert-mix 0.5 0.6 5000000

# only the recent-key knobs: ratio, max age in seconds
./mysql-data-generator/deploy-data-generator.sh recent-mix 0.6 20

# only DELETE_RATIO (orders / order_items); 0 turns deletes off
./mysql-data-generator/deploy-data-generator.sh delete-mix 0.05
```

`config` changes *how fast*. `upsert-mix`, `recent-mix` and `delete-mix` change *what
is being measured*. `recent-mix` warns if the age is 60 s or more (the checkpoint
interval). `delete-mix` rejects values outside `[0, 1)` and warns above 0.1, where
deletes clearly outpace the ~4–5% inserts and the fact tables shrink.

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
| `UPSERT_RATIO` | 0.9 | applies to the **non-recent** rows only: of those, 90% update a `pick_key()` key, 10% are new inserts (was 0.5 until 2026-11-02: the tables outgrew the buffer pool and the rate fell from ~85K to ~30K rows/s in 4 h) |
| `RECENT_KEY_RATIO` | 0.6 | share of **all** rows that re-write a key this thread wrote within `RECENT_KEY_MAX_AGE_S`. Design target ~50% Iceberg position deletes; 0 disables it. Added 2026-11-02 |
| `RECENT_KEY_MAX_AGE_S` | 20 | well under the 60 s checkpoint interval |
| `TABLE_RUN_BATCHES` | 8 | consecutive batches a thread spends on one table; long-run mix unchanged. 1 = the old one draw per batch |
| `DELETE_RATIO` | 0.05 | share of every `orders` / `order_items` batch that is deleted instead of written; `customers` / `products` are never deleted. 0 turns deletes off. Added 2026-11-03 |
| `HOT_KEY_RATIO` | 0.6 | share of upserts drawn from the hot window |
| `HOT_KEY_WINDOW` | 5000000 | sized to the buffer pool, about 16 GB of rows |
| `POD_COUNT` | 4 | must equal `replicas` |
| `SHARD_HOT_KEYS` | true | a disjoint hot slice per ordinal |
| `REPORT_SECONDS` | 15 | interval behind `inst=` |
| `DURATION_SECONDS` | 0 | run forever |
| `USE_PURE` | false | use the C extension; pure Python caps a pod near 5,000 rows/s |
| resources | requests 2 cpu / 2 Gi, limits 4 cpu / 4 Gi | `nodeSelector: karpenter.sh/nodepool: driver-nodepool`, off the TaskManager pool |

The Python defaults (used only when a variable is unset) are `HOT_KEY_RATIO=0.95`
and `HOT_KEY_WINDOW=2000000`. The manifest overrides both. The new knobs have the
same defaults in Python as in the manifest. Three more exist only in Python:
`RECENT_KEY_BUFFER=50000` (entries per table per thread), `FK_PARENT_MOD=16` and
`ORDER_DELETE_MAX_ITEMS=20` (see [Deletes](#deletes-and-foreign-keys)). The startup
banner prints them on the `recent keys :` and `deletes     :` lines.

Stream-wide, the manifest values give at most 60% recent re-writes, 36% hot/cold
upserts and 4% inserts, with 5% of the `orders` / `order_items` rows (75% of the
mix) turned into deletes. A batch never lists the same primary key twice: duplicates
are dropped from the recent draw and redrawn in the normal path.

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

#### Recent re-writes: Iceberg position deletes

**Why.** The Iceberg sink runs in upsert mode with `write.distribution-mode=hash`, so
a key always reaches the same writer subtask. That writer emits one delete for
**every** row it writes, inserts included (orders, live: `total-equality-deletes`
28.3M against `total-records` 28.8M). Which kind depends on one thing. Iceberg's
`BaseEqualityDeltaWriter` keeps an in-memory map of the rows it inserted in the
current, not yet committed checkpoint:

- If the key is in that map, the writer emits a **position delete** (a DV on v3).
- Otherwise it emits an **equality delete**.

The map resets at every checkpoint (60 s, min-pause 30 s). The hot window repeats a
key minutes to hours apart, so before this change only ~2% of deletes were
positional (orders: `total-position-deletes` 0.5M).

So the position share is roughly the share of rows whose key was already written,
by any pod, earlier in the same checkpoint. To get ~50%, about half the rows must
re-write a key written a few seconds before.

**How.** Each worker thread keeps a bounded buffer per table of the keys it
committed (upserts and inserts; new ids are derived from `LAST_INSERT_ID()` and
`@@auto_increment_increment`, read at startup), with commit timestamps.

- `RECENT_KEY_RATIO` of each batch is drawn from the entries younger than
  `RECENT_KEY_MAX_AGE_S`.
- The rest of the batch follows the old rules: `UPSERT_RATIO` upserts through the
  hot window or cold tail, and the remainder new inserts.
- Keys stay sorted ascending (the deadlock fix) and distinct.

The buffer is per thread, and only the thread's own committed rows go into it, so a
re-write touches a row whose last statement has already committed on that same
connection. That adds no cross-thread or cross-pod row-lock contention, and the row
is still in the buffer pool. Foreign keys work as before. A re-write of an existing
`orders`/`order_items` row takes the update path, which does not touch the FK
columns. New rows still draw their parents from the startup samples.

**Same-checkpoint probability.** Checkpoint boundaries are unrelated to the
generator, so a re-write `a` seconds after the previous write of the key shares its
checkpoint with probability `1 - a/60`. Ages spread evenly over `[0, MAX_AGE]`
average `1 - MAX_AGE/120`: 0.83 at 20 s, 0.75 at 30 s. That is why the age limit is
well under 60 s.

The age is never near 0, though, because a thread writes one table per batch. With
an independent table draw per batch, `products` (10%) comes back only every ~10
batches, 25–50 s at 1,000–2,000 rows/s per thread. `TABLE_RUN_BATCHES=8` keeps a
thread on one table for 8 batches, chosen with the same weights, so the previous
same-table batch is one batch back.

**Why 0.6 for a 50% target.** The position share is about
`RECENT_KEY_RATIO` × (share of re-writes the buffer can serve, ~0.92) ×
(same-checkpoint probability, ~0.89), plus the ~1–2% the hot window already makes.
An offline simulation of the planner gave these numbers. It used the real
`plan_keys` / `RecentKeys` / `pick_table` code, a fake clock, fixed 60 s checkpoint
windows and one pod of 8 threads, with ratio 0.6, max age 20 s and runs of 8:

| rows/s per thread | 500 | 1,000 | 1,500 | 2,000 | 3,000 |
|---|---:|---:|---:|---:|---:|
| position share, all tables | 0.42 | 0.47 | 0.49 | 0.51 | 0.52 |

All four tables came out within ±0.03 of each other. With one table draw per batch
(runs of 1) the overall share was 0.38–0.47, and `products` only 0.15–0.30. The
per-thread rate is the achieved `inst=` divided by `THREADS` (8). At 4 pods, ~50,000
rows/s is ~1,560 rows/s per thread. **This 50% is a design target, not a
measurement.** The model ignores Flink-side jitter between the two writes and
checkpoints that run long, and both of those lower the share. Measure it and adjust
`RECENT_KEY_RATIO`.

**Side effects to expect (not yet measured):**

- **Inserts.** Fewer rows are new inserts (about 4–5% of rows at `UPSERT_RATIO` 0.9,
  instead of 50%), so the source tables grow far more slowly. On the fact tables,
  the deletes roughly offset them.
- **Write cost.** More rows are updates, and an update writes undo and redo for the
  old image, so achieved rows/s may move either way. Re-written rows are hot in the
  buffer pool, so they add no read I/O.
- **Paimon.** Paimon gets the identical stream and collapses the repeats in its
  write buffer. That asymmetry is intended. Its committed row counts per checkpoint
  can come out lower than Iceberg's for the same source rows.

#### Deletes and foreign keys

**What.** `DELETE_RATIO` (0.05) of the rows of every `orders` and `order_items` batch
are deletes of existing keys. They run as one extra multi-row statement right after
the batch's upsert: `DELETE … WHERE pk IN (…)`, with sorted keys, for the same lock
order reason as the upsert. The delete keys are distinct from each other and from the
batch's upsert keys. Like the writes, `RECENT_KEY_RATIO` of them come from the
thread's recent-key buffer, and the rest come through `pick_key()` (hot window or
cold tail). Deleted keys are then removed from the buffer, so a later "recent
re-write" cannot quietly re-insert them.

**Why 0.05.** Deletes become a visible event class: about 3.75% of all rows,
because the two fact tables are 75% of the mix, plus the items deleted with their
orders. That is small enough not to change what the benchmark mostly measures. It
also roughly balances the ~4–5% inserts left at `UPSERT_RATIO` 0.9, so the fact
tables stay near their size instead of outgrowing the buffer pool again. The
buffer pool problem is why `UPSERT_RATIO` went to 0.9.

**Foreign keys.** The three FKs re-added by the 2026-11-02 shrink
(`orders_ibfk_1` orders→customers, `order_items_ibfk_1` / `_2`
order_items→orders/products) have **no `ON DELETE` clause, so they are RESTRICT**
(see `shrink_10pct_drop_old_and_add_fks` and the DDL in
[0-MYSQL-SETUP-GUIDE.md](./0-MYSQL-SETUP-GUIDE.md)). Each table is handled as
follows:

| Table | Deleted? | How the FKs hold |
|---|---|---|
| `order_items` | yes | a leaf: nothing references it |
| `orders` | yes, except ids that are multiples of `FK_PARENT_MOD` (16) | One `READ COMMITTED` transaction. First a non-locking `SELECT order_id, COUNT(*) FROM order_items … GROUP BY order_id` drops orders with more than `ORDER_DELETE_MAX_ITEMS` (20) items, then `DELETE FROM order_items WHERE order_id IN (…)`, then `DELETE FROM orders WHERE order_id IN (…)`. The orders FK sample that new `order_items` rows draw their parent from is taken **only** from the `order_id % 16 = 0` pool, which is never deleted, so no insert can reference a deleted order. The pool is a residue rather than "the newest 20,000 at startup", so it is the same set in every pod whatever its start time |
| `customers`, `products` | **never** | They have children under RESTRICT. The only rows provably unreferenced are the thread's own fresh inserts, so a "safe" delete there is just insert-then-delete churn, and anything else needs a child probe per key or risks errno 1451 rolling back the batch. The CDC and sink delete path is the same for every table, so the fact tables already exercise it. Real shops soft-delete customers and products, which is an update. Because nothing is deleted, the `customers` / `products` FK samples need no filter |

Why `READ COMMITTED` for the delete transaction only: under the server's default
REPEATABLE READ, a key that is already gone leaves a gap lock. For `orders`, the
child delete through `idx_order_id` would gap-lock the range above the highest
referenced `order_id` and block new `order_items` inserts behind it. RC takes no
gap locks for these searches. The upserts keep the server default.

Why `ORDER_DELETE_MAX_ITEMS`: every earlier run drew `order_items` parents from the
newest 20,000 orders. A small set of orders therefore carries most of the items,
although the source averages only ~1.5 items per order. Deleting one of those would
turn a 250-key delete into a transaction over thousands of item rows.

**Re-inserts are allowed.** A later `pick_key()` upsert that lands on a deleted id
takes the INSERT path of `ON DUPLICATE KEY UPDATE` and re-creates the row at that id,
with FK values from the protected samples, so it is FK-legal. That is the same thing
that happens today when a cold draw hits a hole. In the fake-DB test about 7% of
deleted ids came back this way. Its tables were small enough that the hot window
covered them, so treat that as an upper bound. It makes the net shrink smaller, and it is a
realistic delete-then-recreate pattern for the sinks. A deleted order's items are not
re-created.

**What the sinks do with a `-D`:**

- **Iceberg** (upsert mode). The writer turns a `-D` into a delete of the key. It
  becomes a **position delete (a DV on v3)** if the writer wrote that key earlier in
  the same checkpoint, which is the case for recent-buffer deletes, and an **equality
  delete** otherwise. The writer then drops the key from its in-checkpoint map. A
  delete adds no data row, so `added-records` does not move.
- **Paimon** (primary-key table). A `-D` becomes a delete record in the LSM. The merge
  drops the key, either in the write buffer if it is still there or at compaction.
  With deletion vectors on, compaction marks the old row in the data file.

**Effect on the position-delete share.** Deletes split recent and older keys in the
same proportion as writes, so the share stays near 50%. The items deleted with old
orders are almost always equality deletes. Simulation (same model as above, plus
deletes; items modelled at 1.5 per pre-existing order):

| rows/s per thread | 500 | 1,000 | 1,500 | 2,000 | 3,000 |
|---|---:|---:|---:|---:|---:|
| position share, `DELETE_RATIO` 0 | 0.43 | 0.48 | 0.49 | 0.52 | 0.53 |
| position share, `DELETE_RATIO` 0.05 | 0.42 | 0.47 | 0.49 | 0.50 | 0.52 |
| delete events / all row events, 0.05 | 4.8% | 5.6% | 5.8% | 5.7% | 5.3% |

At 0.05, about 3.5 points of those delete events are the batch's own deletes and
about 2 points are items. At `DELETE_RATIO` 0.1 the share was 0.40–0.51 and deletes
were ~11% of events. **These are design targets, not measurements.** A real run's item
count per deleted order is unknown (the cap is 20), so expect `deleted=` / `deletes=`
anywhere from ~1 to ~2.

**Risks, not yet measured:**

- **Rate.** There is one extra round trip, plus a 4-statement transaction for
  `orders`, per fact-table batch, so expect a few percent off `inst=`.
- **Locks.** Deletes take X locks on hot keys like upserts do, and in the same
  ascending order. A child delete locks `order_items` rows in `order_id` order, not
  `order_item_id` order, so it can still deadlock with an item upsert. That shows
  up as retried 1213s in `errors=`.
- **Table size.** Watch `COUNT(*)` on `orders` / `order_items`. If they shrink
  noticeably, lower `DELETE_RATIO`.

#### Measure the position-delete share

1. Wait until every pod has printed the new startup banner (`recent keys :` line).
   Use only commits after that.
2. Read the per-commit snapshot summaries. Iceberg reports
   `added-position-deletes` and `added-equality-deletes` per commit, and the share is
   `added-position-deletes / (added-position-deletes + added-equality-deletes)`.
   Skip `replace` (compaction) commits. From the current metadata file:

   ```bash
   source .bench-env
   DB=flink_iceberg_db${CDC_DB_SUFFIX}
   for T in customers products orders order_items; do
     LOC=$(aws glue get-table --database-name "$DB" --name "$T" \
             --query 'Table.Parameters.metadata_location' --output text)
     aws s3 cp "$LOC" - | python3 -c '
   import json, sys
   m = json.load(sys.stdin)
   snaps = sorted(m["snapshots"], key=lambda s: s["timestamp-ms"])[-20:]   # ~20 min
   snaps = [s["summary"] for s in snaps if s["summary"].get("operation") != "replace"]
   p = sum(int(s.get("added-position-deletes", 0)) for s in snaps)
   e = sum(int(s.get("added-equality-deletes", 0)) for s in snaps)
   print(f"{sys.argv[1]:12s} position {p:>12,}  equality {e:>12,}  share {p / max(1, p + e):.1%}")' "$T"
   done
   ```

3. Cross-check in the monitor ([3-MONITOR.md](./3-MONITOR.md)). Compare
   `↳ Position Deletes (Δ)` with `↳ Equality Deletes (Δ)` in the Iceberg column. The
   two should be about equal, and `↳ Deletion Vectors (added / removed)` should be
   non-zero on v3. These rows are deltas of the `total-*` counters, so a compaction
   commit inside the window distorts them. Use step 2 for a clean number.

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

A replica ramp (a ramp script, removed 2026-11-04; it is in git history) found the aggregate rate
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

These numbers predate the current instance; to measure today's ceiling, step the
generator with `deploy-data-generator.sh scale N upsert` and read `status`.

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

With the generator running, `customers` and `products` should grow only by the
insert share, `(1 - RECENT_KEY_RATIO) x (1 - UPSERT_RATIO)`: about 4–5% of their
row rate at the manifest values. `orders` and `order_items` lose `deleted=` on top
of that, so they should stay roughly flat or shrink slowly. In total, the change in
the sum of the four counts should be about `inserts= − deleted=`, plus the few
re-inserts of deleted ids. If a shrunk table grows much faster, the pods are running
with a stale `MIN` ([Step 3](#step-3--restart-the-pods-after-a-source-shrink)). If
the growth clearly exceeds that, the ids derived for recent re-writes are wrong and
those re-writes are taking the insert path.

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
   ↓ SQL: INSERT … ON DUPLICATE KEY UPDATE; DELETE … WHERE pk IN (…) on orders / order_items
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

# Recent re-writes (Iceberg position deletes): ratio, max age s (rolling restart)
./mysql-data-generator/deploy-data-generator.sh recent-mix 0.6 20

# Deletes on orders / order_items: ratio, 0 = off (rolling restart)
./mysql-data-generator/deploy-data-generator.sh delete-mix 0.05
```

The manifest values are in [Current manifest values](#current-manifest-values).

---

## Summary

✅ **One generator**: StatefulSet `mysql-upsert-loadgen`, 4 pods, rate is measured
   not configured. The mixed INSERT/UPDATE/DELETE generator was removed 2026-11-02;
   since 2026-11-03 the upsert generator emits DELETEs itself (`DELETE_RATIO`)
✅ **Deployed in**: `emr-flink` namespace, run from `cdc-pipeline/`
✅ **Purpose**: the storage-format benchmark and the hot query test
✅ **Operations**: batched `ON DUPLICATE KEY UPDATE` on 4 tables; 5% of
   `orders` / `order_items` rows are multi-row `DELETE`s (FKs are RESTRICT: an
   order's items go first; `customers` / `products` are never deleted)
✅ **Keys**: hot window sharded per pod; cold tail drawn from the live
   `[MIN(id), MAX(id)]`, read at pod start (restart the pods after a shrink);
   60% of rows re-write a key written < 20 s earlier by the same thread, so that
   ~50% of Iceberg deletes are position deletes (design target, not yet measured)
✅ **Measured**: ~64,000 rows/s at 8 pods on the 2026-11-02 dry run (RDS CPU 64–79%,
   errors/batches ~34%). Re-measure; never quote a stale ceiling.
