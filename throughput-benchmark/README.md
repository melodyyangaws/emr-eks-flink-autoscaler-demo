# Flink Throughput Benchmark — Paimon vs Iceberg

Measures the **Flink write-path performance of Apache Paimon vs Apache Iceberg**
at a target of **8 GiB/s with a 4 KiB mean row** (2,097,152 rows/s), on the same
image the CDC pipeline runs — `emr-flink-cdc-paimon` (EMR 7.13, Paimon 1.3.2,
Iceberg 1.10.0-amzn-1).

| Format | Table shape | Compaction |
|---|---|---|
| `paimon` | append-only, `PARTITIONED BY (dt)`, `JOB_PARALLELISM` buckets on `uuid` | **in-engine** — inline in the writer (`write-only=false`) |
| `iceberg` | format v2, append-only, `PARTITIONED BY (dt)`, `write.distribution-mode=none` | **none** — the sink never rewrites data files, and no compactor ships here |

**Both sides are append-only** — no primary key, no upsert, no merge-on-read
deletes, no changelog producer. Each pipeline is `INSERT INTO cid_events
SELECT …` over the source view with no stateful operator, so the planner cannot
produce a retraction: every record reaching either sink is a new row.

⚠ **Not comparable with pre-append-only (upsert-test-case) runs.** The update
machinery that used to dominate the difference between the two formats is gone.
What remains is each format's **encode → upload → commit → compact** path at the
same row rate.

There is **no docker image build here.** Build it once via
`cd ../cdc-pipeline && ./build-deploy-generic.sh build`; the deploy script checks
the tag exists in ECR before rendering.

---

## Quick start

```bash
cd throughput-benchmark
source .bench-env                    # compulsory, always first — needs .bench-env.local, see below
./jsongz-gen/run-jsongz-gen.sh start # put gzip NDJSON source data to s3 with the rate of 3 million rows/s. Hot watch script will terminate the datagen when hot query benchmark is completed.

# ingest/writer performance test
./build-deploy-cid.sh upload         # executor + SQL to S3, verified
./build-deploy-cid.sh deploy both    # or paimon or iceberg, Append-only throughput test. Each job reads the generator's files via its own SQS queue.
./monitoring/deploy-cid-monitor.sh   # side-by-side monitor pod + 30 s CSV collectors per RUNNING job -> bench-results/

# query/reader performance test
./querybench/duckdb-warm.sh up       # once: the two warm DuckDB servers
SUITE=hot ./querybench/starrocks.sh  # hot suites need the cid Flink jobs - writers RUNNING
SUITE=hot ./querybench/duckdb-warm.sh run      # after StarRocks, never at the same time
SUITE=snapshot ./querybench/starrocks.sh # stops the writers, leaves them suspended
SUITE=snapshot ./querybench/duckdb-warm.sh run
./build-deploy-cid.sh cleanup        # then wipe — see step 7
./monitoring/deploy-cid-monitor.sh stop
```

**First time: create `.bench-env.local`.** The committed `.bench-env` holds
placeholders (`account`) for the account-specific values, and sources the
git-ignored `.bench-env.local` to override them. Without it, every S3 path,
Glue call and the job role point at a bucket and role that don't exist:

```bash
cat > .bench-env.local <<'EOF'
export AWS_ACCOUNT_ID=<your-account-id>
export BUCKET_NAME=emr-on-eks-test-<your-account-id>-us-west-2
export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::<your-account-id>:role/emr-on-eks-test-execution-role
EOF
```

Keep account IDs, bucket names and ARNs out of `.bench-env` and out of
committed results — they belong only in `.bench-env.local`.

**`source .bench-env` is the most common way to lose ten minutes.** Without it
the script aborts on missing `AWS_REGION` / `AWS_ACCOUNT_ID` / `BUCKET_NAME` /
`EMR_EXECUTION_ROLE_ARN`. It also **overrides benchmark defaults**, so you are
not running the script's defaults when you source it:

| | script default | `.bench-env` |
|---|---|---|
| `JOB_PARALLELISM` | 128 | **320** |
| `PAIMON_BUCKETS` | -1 (unaware-bucket) | -1 |
| `ICEBERG_COMPAT` | false | **true** |

`PAIMON_BUCKETS` is listed because it changes the write path: the default `-1`
is an unaware-bucket append table with no shuffle, like the Iceberg side. Set
`PAIMON_BUCKETS=N` (use `N = JOB_PARALLELISM`) only for a fixed-bucket-on-`uuid` run.

---

## Deployment steps

Run every step from `throughput-benchmark/` in one shell that has done
`source .bench-env` first. It sets the generator's landing prefix
(`OUTPUT_URI`). The jobs never list it; they read the files announced on SQS.
The bucket's S3 event notification fires for **one** prefix (now
`cid-landing-jsongz/`), so a file written anywhere else is never read.
- **The notification lives on the bucket, not in this repo.** Changing
  `OUTPUT_URI` alone does not move it. On 2026-10-18 that disconnected both jobs
  for 18 minutes without an error.
- **`./build-deploy-cid.sh notify`** sets the notification from `OUTPUT_URI`. It
  keeps the SNS topic, the `.json.gz` suffix and any other rule on the bucket.
  It then sends an empty test file through to both queues and removes it.
  It refuses to run while a writer is running.
- **`OUTPUT_URI` is the one landing location.**
  - **The generator** writes only there. `run-jsongz-gen.sh` has no fallback
    path.
  - **Each job's source** reads only keys under it. `'key-prefix'` in
    `01-raw-s3sqs.sql` comes from `LANDING_KEY_PREFIX`, which
    `build-deploy-cid.sh` derives from `OUTPUT_URI`.
What is reset between runs is the **queues** — see
[Reading through SQS](#reading-through-sqs).

### 1. Check capacity before deploying

Per deployment: `1 JM × 4 cpu + (JOB_PARALLELISM / 4 slots) TMs × 4 cpu`. TMs
are 4 cpu / 16 GiB with `taskmanager.numberOfTaskSlots: "4"`, so
`JOB_PARALLELISM=160` (what `.bench-env` sets) is **40 TMs ≈ 164 cpu per
format**, and the script default of 128 is 32 TMs ≈ 132.

```bash
kubectl get nodepool executor-memorynodepool -o jsonpath='{.spec.limits.cpu}'      # ceiling
kubectl get nodepool executor-memorynodepool -o jsonpath='{.status.resources.cpu}' # in use
```
Keep total requests under ~80% of nodepool allocatable. Above that the pods
evict each other in a loop: a displaced writer TM's replacement hits
`FailedScheduling: Insufficient cpu` and the job restarts.

### 2. Start the data generator

The jobs read the `.json.gz` files this lands, so start it before or alongside
the writers, or they have nothing to read. `OUTPUT_URI` must be the notified prefix
(`s3://$BUCKET_NAME/cid-landing-jsongz/`); after changing it, run
`./build-deploy-cid.sh notify`.

```bash
./jsongz-gen/run-jsongz-gen.sh start                # Job, 3 pods; each writes DURATION_SECONDS (default 1800; 0 = until stopped, for a hot query run), then exits and its node is released
./jsongz-gen/run-jsongz-gen.sh scale 4              # change Job parallelism (rate = N x 500K rows/s)
./jsongz-gen/run-jsongz-gen.sh status               # per-pod rows/s, MB/s
./jsongz-gen/run-jsongz-gen.sh logs                 # follow the first pod (POD=N for another)
./jsongz-gen/run-jsongz-gen.sh stop                 # end a run (deletes the Job + ConfigMap); written files stay
```

Knobs, sizing and file layout:
[Raw landing-zone generator](#raw-landing-zone-generator-gzipped-ndjson).

### 3. Upload all artifacts and scripts to S3

Every artifact the jobs run is read from S3 at pod boot, not from this
directory, so stage and verify it before triggering anything:

```bash
./build-deploy-cid.sh upload
```

This creates the Glue databases and uploads, then `head-object`-verifies, every
artifact:

| S3 key | from | read by |
|---|---|---|
| `flink/scripts/flink-cid-executor.py` | `flink-cid-executor.py` | `spec.job.jarURI`, which EMR copies into `/opt/flink/usrlib` |
| `flink/cid-sql/<fmt>/00-source.sql` | `01-raw-s3sqs.sql` **concatenated with** `01-parse-s3json.sql` (both in `sql-scripts/common/`). The same file for both formats | the executor's `--sql-dir` |
| `flink/cid-sql/<fmt>/02..04-*.sql` | `sql-scripts/<fmt>/` (synced with `--delete`) | the executor's `--sql-dir` |
| `flink/cid-sql/common/01-*.sql` | `sql-scripts/common/` (synced with `--delete`) | nothing at runtime — a copy of the two fragments; no `--sql-dir` points here, since neither runs on its own |
| `flink/jars/sqs-delta-filename.jar` | `sqs-delta-filename/target/sqs-delta-filename-*.jar`, rebuilt with `mvn -q package` when missing or older than `src/`/`pom.xml` | `ADD JAR '${SQS_SOURCE_JAR}'` in `00-source.sql`, on the JobManager |

It runs no `kubectl`. A missing or zero-length object stops it at the shell
with `[ERROR] NOT IN S3` / `ZERO-LENGTH`, instead of 10 minutes later as a
`NoSuchFileException` inside a JobManager. Check the result:

```bash
aws s3 ls "s3://$BUCKET_NAME/flink/scripts/flink-cid-executor.py"
aws s3 ls --recursive "s3://$BUCKET_NAME/flink/cid-sql/"   # 4 objects per format
```

`deploy` runs the same upload again before it creates the FlinkDeployments, so
S3 always matches this directory; see
[step 4](#4-deploy-then-confirm-it-is-actually-working).

### 4. Deploy, then confirm it is actually working

```bash
./build-deploy-cid.sh deploy paimon             # or: iceberg, or: both (= contention run)
kubectl get flinkdeployment -n emr-flink -w     # ctrl-C at RUNNING/STABLE
```
In case of stopping jobs:
```bash
./build-deploy-cid.sh cleanup paimon  # or: iceberg, or: both
```
Once the jobs are RUNNING, start monitoring (step 5): it also starts the
per-job CSV collectors.

### 5. Monitoring

```bash
kubectl get flinkdeployment,pods -n emr-flink                           # FlinkDeployments + pods, one shot
kubectl port-forward svc/flink-cid-paimon-rest 8081:8081 -n emr-flink   # one job's REST/UI
```

`deploy-cid-monitor.sh` does both kinds of monitoring in one step:
- **Side-by-side monitor:** a plain `Deployment` running the Python in
  `monitoring/` (forked from `cdc-pipeline/monitoring`; record counts here are
  operator-scoped, so the chained Iceberg job reports real rows/s).
- **CSV collectors:** `collect-metrics.sh` started locally, in the background,
  for each job that is RUNNING: Paimon on `LOCAL_PORT` 8091, Iceberg on 8092.
  Output: `bench-results/cid-<format>-<UTC stamp>.csv`, logs
  `bench-results/collect-<format>-<stamp>.log`. Re-running the script replaces
  the collectors; `stop` ends them. A collector exits when its job stops or
  after `COLLECT_DURATION`.

```bash
./monitoring/deploy-cid-monitor.sh        # upload code, apply, restart, wait, start collectors
kubectl logs -f deployment/flink-cid-monitor -n emr-flink
./monitoring/deploy-cid-monitor.sh stop   # scale to 0 and stop collectors; re-run without args to start
COLLECT=0 ./monitoring/deploy-cid-monitor.sh  # deploy the monitor without the local collectors
COLLECT_INTERVAL=30 COLLECT_DURATION=3600 ./monitoring/deploy-cid-monitor.sh
```
`collect-metrics.sh paimon|iceberg [interval-s] [duration-s]` still runs on its
own. A second concurrent copy needs its own `LOCAL_PORT`: both default to 8091,
so the second one would read the *first* job's samples.
The script uploads the three monitor files to
`s3://${BUCKET_NAME}/flink/monitoring/` and the pod fetches them at boot with
`boto3` under its IRSA service account. After a code change, just re-run
`./monitoring/deploy-cid-monitor.sh`: it re-uploads to S3 and does a rollout restart.

**Reading the comparison table.** "Throughput (rows/s)" is the one throughput
row, with its source in brackets:
- **`commits`:** the rows each table commit added, divided by the time between
  commits. It's read from the table's own history, with the last commit before
  the window as the anchor. It's exact, and it's the same method the
  `bench-results/` write-ups use.
- **`rate meter`:** used when the window saw no commit. It's the median of the
  source subtasks' `numRecordsOutPerSecond`.
- **`counter`:** the last resort, the change in the source `numRecordsOut`
  counters divided by the time. Those REST counters are cached and move in
  steps, so this is the least reliable.

"Restarts in window" counts counter resets. The rates behind all three are in
the JSON report (`commit_`, `meter_`, `counter_records_per_sec`).

Rows written are `IcebergStreamWriter`/`Writer` `numRecordsIn`. "New Records in
Tables" comes from table snapshots. (Vertex-level counts only see
records crossing a vertex boundary; Iceberg chains source → IcebergStreamWriter
into one vertex, which once read as ~3 rec/s and −100%. `collect-metrics.sh`
has the same fix.) "Committed bytes/s (Parquet)" and "New Bytes in Tables" are
Parquet bytes the window's commits added:
- **Iceberg:** the committer's `committedDataFilesByteCount`.
- **Paimon:** the files added by APPEND commits, read from each commit's delta
  manifests. COMPACT commits are skipped. Paimon's own committer counter
  (`sink.numBytesOut`) is not used, because it also counts compaction rewrites.

"New Data Files" is the change in the number of live files, read from table
metadata on both sides. For Paimon that is the sum of added minus deleted
files over the latest snapshot's manifest lists. Compaction nets out, so
Paimon's figure is lower than its append file count. An S3 listing would also
see compaction inputs that haven't expired yet and files written before their
commit.

All of these step once per commit. `MONITOR_DURATION` is 120 s per job, about
two 60 s commits; the commit rate is taken from the last commit before the
window to the last one inside it. The vertex `numBytesIn/Out` used earlier counted only network-shuffle
bytes, a few KB/s in a chained job.
Flink REST counters update in steps, so rec/s over the monitor's short window can
read below a point sample. In the first run the monitor showed Iceberg 1.198M vs
Paimon 756k rec/s (+58.4%), while point samples gave ~1.75M vs ~830k.

### 6. Run the query benchmark (the read half)

Both tables must hold rows. The **hot** suite reads the last 30 s of data, so
it needs the writers **RUNNING**; the **snapshot** suite stops the writers
itself and leaves them suspended. So run both engines' hot suites first, then
both snapshot suites:

```bash
./querybench/duckdb-warm.sh up                        # once: the two warm DuckDB servers
SUITE=hot      ./querybench/starrocks.sh              # arms: paimon, pmn_as_iceberg, iceberg
SUITE=hot      ./querybench/duckdb-warm.sh run        # one engine after the other, never at once
SUITE=snapshot ./querybench/starrocks.sh              # suspends the writers
SUITE=snapshot ./querybench/duckdb-warm.sh run
```

`SUITE=all` (the default) runs hot, stops the writers, then snapshot — fine for
one engine, but the second engine's hot suite would then run against stopped
writers. Other knobs: `RUNS=3`, `WARMUP=1`, `ARMS=...`, `SUSPEND_WRITERS=0`
(StarRocks); `./querybench/duckdb-warm.sh status` / `down` (DuckDB). Output:
`bench-results/starrocks-<stamp>.csv` and `bench-results/duckdb_warm-<stamp>.csv`,
same columns. Details:
[Query benchmarks — the read half](#query-benchmarks--the-read-half).

### 7. Teardown between runs

**Order matters: nothing may be writing when you wipe.** A live JobManager
recommits into a half-deleted warehouse and produces a table that looks valid
and is not.

```bash
# 7a. Stop the writers FIRST — the generator too, if it is still running.
./jsongz-gen/run-jsongz-gen.sh stop             # delete the Job + ConfigMap; files stay
# Then the Flink writers — suspend, or delete with `cleanup` (prompts "yes").
kubectl patch flinkdeployment flink-cid-paimon -n emr-flink --type=merge \
  -p '{"spec":{"job":{"state":"suspended"}}}'
./build-deploy-cid.sh cleanup both

# 7b. VERIFY before going further.
kubectl get pods -n emr-flink | grep cid        # must print nothing

# 7c. Glue tables. Drop before any scoring run, and ALWAYS before changing
#     PAIMON_BUCKETS (the Paimon bucket count is baked at CREATE TABLE) or
#     flipping ICEBERG_COMPAT.
aws glue delete-table --database-name flink_cid_paimon_db  --name cid_events --region "$AWS_REGION"
aws glue delete-table --database-name flink_cid_iceberg_db --name cid_events --region "$AWS_REGION"
```

`cleanup` deletes FlinkDeployments only — warehouse data and Glue tables stay,
and it says so.

The 7a patch is the same one the query harnesses issue for themselves when
the snapshot suite is in scope (see [step 6](#6-run-the-query-benchmark-the-read-half)).
It is kept here because teardown is also reached from a cold start, with no
benchmark having run — and because teardown needs *both* writers down plus an
empty `kubectl get pods`, which `cleanup` enforces and a suspend alone does not.
Running it after a snapshot benchmark is harmless: suspending an
already-suspended deployment is a no-op.

**ConfigMap cleanup depends on *how* the job stopped.** The operator's own
per-job ConfigMaps (`flink-config-*`, `pod-template-*`,
`flink-fluentd-config-*`) carry an ownerReference to the FlinkDeployment and are
garbage-collected with it. **The HA ConfigMaps do not.**
`flink-cid-<format>-cluster-config-map` and
`flink-cid-<format>-<jobid>-config-map` have no ownerReference at all — they are
removed by Flink's own HA teardown on a *graceful* cancel, so they **leak** if
the deployment was force-deleted or the JM died. A leaked
`*-cluster-config-map` is how a "fresh" deploy resumes a run you thought you had
discarded. Check and sweep explicitly:

```bash
kubectl get cm -n emr-flink | grep -E 'flink-cid-.*config-map'   # expect nothing
```

**7d. DESTRUCTIVE AND IRREVERSIBLE — no undo, no versioning.** Read the prefixes
twice. A typo dropping a path segment targets the whole bucket, which is shared
with the CDC pipeline:

```bash
aws s3 rm --recursive "s3://$BUCKET_NAME/cid-warehouse-paimon/"
aws s3 rm --recursive "s3://$BUCKET_NAME/cid-warehouse-iceberg/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/cid-paimon/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/cid-iceberg/"
aws s3 rm --recursive "${OUTPUT_URI}/"        # this run's landing prefix
# then purge both queues — see "Reading through SQS"
# Optional, and only if you want a genuinely cold next run:
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/logs/cid-paimon/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/logs/cid-iceberg/"
aws s3 rm --recursive "s3://$BUCKET_NAME/duckdb-cid-pinned/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/cid-sql/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/querybench/"   # retired prefix
```

The first two are the warehouses (data + metadata); the next two are Flink state
— HA metadata, checkpoints, savepoints. `${OUTPUT_URI}/` is the generator's
landing data; drop it once the run is scored (the next run uses a fresh prefix
anyway). `upgradeMode: stateless` means nothing
of value is lost, but stale HA metadata is how a "restarted" job resumes a run
you thought you had discarded, and a `completedCheckpoint*` left pointing at a
warehouse you just emptied is worse than useless: resuming from it commits
against files that no longer exist.

The optional prefixes are the rest of the CID footprint, and each loses
something different. `flink/logs/cid-*/` is the sidecar's JM/TM `stdout.gz` /
`stderr.gz` — **the only record of the run once the JobManager is gone**, so
pull anything you need from [Reading the results](#reading-the-results) first;
it is a couple of MiB, so there is no cost reason to drop it.
`duckdb-cid-pinned/` is pinned Iceberg metadata that 404s mid-scan once the
table it describes is deleted; each DuckDB run now deletes its own `RUN_TAG`
subprefix, so this only sweeps leftovers from killed runs. `flink/cid-sql/` is job code, not data —
deleting it costs nothing because the next `deploy` re-uploads it, but a bare
`kubectl apply` in between will fail (see
[step 4](#4-deploy-then-confirm-it-is-actually-working)).
`flink/querybench/` is a retired prefix and safe to delete: the queries are now
read locally (StarRocks) or shipped in a ConfigMap (DuckDB), with no S3 publish.

Finally, if you started the monitor, scale it to zero (it is a Deployment, not a
FlinkDeployment):

```bash
kubectl scale deployment flink-cid-monitor -n emr-flink --replicas=0
```

---

## Query benchmarks — the read half

**Step-by-step guide and run times per arm:** [querybench/README.md](querybench/README.md).

Ten queries, two engines, the same tables. `querybench/lib.sh` holds everything
both engines must agree on (query files, rendering, windows, partition, timeout,
median, CSV format, writer stop), so a cross-engine gap measures engines, not
harnesses.

- **`querybench/starrocks.sh`**, three arms: `paimon` (Paimon reader → Paimon
  table), `pmn_as_iceberg` (Iceberg reader → the *same* Paimon files via
  Paimon's Iceberg export, `ICEBERG_COMPAT=true`), `iceberg` (Iceberg reader →
  native Iceberg table). paimon vs pmn_as_iceberg changes only the reader;
  pmn_as_iceberg vs iceberg only the writer.
- **`querybench/duckdb-warm.sh`**, two arms, both `iceberg_scan`, because DuckDB
  has no Paimon reader: `iceberg` (native Iceberg table) and `paimon` (Paimon's
  Iceberg export, so `ICEBERG_COMPAT=true` is required). The reader is the same
  on both sides, so only the writer differs. That makes it the DuckDB
  counterpart of StarRocks' `pmn_as_iceberg` vs `iceberg`. **Warm**: one
  long-lived server per suite, like the always-up StarRocks cluster; see "Warm
  DuckDB" below.
  - History: the cold harness (`duckdb.sh`, a fresh `duckdb` process timed per
    attempt, a 20-30 s floor on the hot queries) was removed on 2026-10-18 and
    is in git history.

Prerequisites: both tables hold rows, the FE pod is up, and `envsubst` exists
for DuckDB (`brew install gettext`). StarRocks creates its catalogs idempotently
and aborts if it cannot read a row through each.

### Running it

The **hot** suite reads the last 30 s of data, so it needs the writers
**running**. The **snapshot** suite needs them **stopped**: it suspends them
itself and leaves them suspended. So run both engines' hot suites, then both
snapshot suites:

```bash
./querybench/duckdb-warm.sh up                         # once: the two warm DuckDB servers
SUITE=hot      ./querybench/starrocks.sh
SUITE=hot      ./querybench/duckdb-warm.sh run         # after StarRocks, never at the same time
SUITE=snapshot ./querybench/starrocks.sh               # suspends the writers
SUITE=snapshot ./querybench/duckdb-warm.sh run
./build-deploy-cid.sh deploy both                      # resume later: a fresh, stateless restart

ARMS="paimon iceberg" ./querybench/starrocks.sh        # e.g. with ICEBERG_COMPAT off
./querybench/duckdb-warm.sh status                     # health, busy, cache bytes
./querybench/duckdb-warm.sh down                       # servers + their cache volumes
```

### Warm DuckDB (`duckdb-warm.sh`)

One long-lived server per suite (`duckdb-warm-hot`, `duckdb-warm-snapshot`,
`duckdb-warm.yaml`), each holding a single DuckDB process
(`duckdb_warm_server.py`) for the pod's life. Extensions, the S3 secret, Parquet
footers and the block cache carry over from query to query, as they do on a
StarRocks cluster that is already up.

| | StarRocks (live, 2026-10-13) | warm DuckDB |
|---|---|---|
| CPU / RAM | 3 BEs × 6 cpu / 24Gi limit = 18 cpu / 72Gi, m5 | one pod, 18 cpu / 72Gi, m5.8xlarge |
| process | always up | always up |
| data cache (on, both defaults) | `datacache`: 3 × 150 GiB disk (gp3, 125 MB/s) + 3 × ~4.2 GB memory; the io adaptor sends reads to S3 when the disk saturates | `cache_httpfs` on disk (`CACHE_MODE=on_disk`): 450 GiB cap (`CACHE_DISK_BYTES`), LRU, on `gp3-fast` (1000 MB/s), + 12.5 GB memory read cache (`CACHE_MEM_BYTES`) |
| metadata | FE catalog caches | `parquet_metadata_cache`, `enable_http_metadata_cache`, cache_httpfs metadata/glob/handle caches |
| timing | server-side (audit log), planning included | in-process around execute + fetch, planning included |
| arms | serial on one cluster | serial on one process |

**Why DuckDB's cache disk is faster.** DuckDB's cache is on a `gp3-fast`
volume (16000 IOPS, 1000 MB/s; `querybench/gp3-fast-sc.yaml`), not default gp3.
- **On default gp3 the cache made DuckDB slower than S3.** cache_httpfs serves
  every hit from the disk, never S3, so on 2026-10-17 the snapshot S3 pass ran
  for hours at 130 MB/s from the cache disk, 0 MB/s from S3 and 0.3 cores.
- **StarRocks' BEs stay on default gp3.** Their `enable_datacache_io_adaptor`
  sends reads back to S3 when the disk saturates, and cache_httpfs has nothing
  like it.
- **`CACHE_MODE=in_mem` keeps only the memory cache.**

`DUCKDB_MEMORY_LIMIT` is 44GB, not the removed cold harness's 56GB: DuckDB does not
count the cache_httpfs memory, and 44 + 11.7 GB leaves the same headroom under
72Gi. Resolving and pinning `${TABLE}` and refreshing the secret happen outside
the timer, as StarRocks' catalog refresh does. RUNS/WARMUP, the median
(`qb_median`) and the timeouts (`qb_query_timeout`) are `lib.sh`'s, shared with
`starrocks.sh`; the server applies the ZERO_SCAN guard to each pass.

```bash
./querybench/duckdb-warm.sh up                    # both servers; ~5-10 min (Karpenter + pip + extensions)
./querybench/duckdb-warm.sh status                # health, busy, cache bytes
SUITE=hot      ./querybench/duckdb-warm.sh run    # writers running
SUITE=snapshot ./querybench/duckdb-warm.sh run    # suspends the writers, like the others
./querybench/duckdb-warm.sh clear-cache [suite]   # a cold cache on demand
./querybench/duckdb-warm.sh down                  # deletes the servers and their cache volumes
```

**Running a benchmark that survives the laptop.** `./querybench/qb-runner.sh
start '<cmd>'` runs one step in an in-cluster Job (`qb-runner`, on
`driver-nodepool`, `do-not-disrupt`), so closing the laptop does not stop it. The
Job and its permissions are in `querybench/qb-runner.yaml`; the script ships the
code bundle and applies that template.
- **Permissions:** a namespace Role only (pod exec / port-forward, deployment
  restarts, FlinkDeployment read / patch), and **no AWS credentials**. Pin
  `SNAPSHOT_DT` and `SNAPSHOT_S4_FROM`/`_TO` (or `HOT_TO`) so `lib.sh` never
  calls aws.
- **`wait_for_sr_run <RUN_ID>`** at the start of `<cmd>` waits until that
  `starrocks.sh` run has nothing left in the FE pod, so the engines never
  overlap.
- **`logs`** follows the output. **`fetch`** copies the pod's `bench-results/`
  into the local one, then removes the Job, its ConfigMap and Secret (`KEEP=1`
  keeps them). The pod keeps its results for 1 day.

```bash
./querybench/qb-runner.sh start 'wait_for_sr_run <RUN_ID>; STAMP=<stamp> SUITE=snapshot SNAPSHOT_DT=2026-10-10 \
  SNAPSHOT_S4_FROM="2026-10-10 15:06:00" SNAPSHOT_S4_TO="2026-10-10 15:09:00" ./querybench/duckdb-warm.sh run'
./querybench/qb-runner.sh logs
./querybench/qb-runner.sh fetch
```

**Placement.** Both engines run on Karpenter's `driver-nodepool` (on-demand,
consolidates only empty nodes). Warm DuckDB is pinned there by `duckdb-warm.yaml`.
StarRocks is pinned by its Helm values,
`cdc-pipeline/helm/starrocks-eks-values.yaml` (`starrocksFESpec` / `starrocksBeSpec`):
- **Placement:** `nodeSelector` for m5.2xlarge on `driver-nodepool`, and
  `podAntiAffinity` so there is one BE per node and the FE stays off the BE
  nodes.
- **No auto-instrumentation:** pod `annotations` opt the FE and BEs out of the
  CloudWatch add-on's OpenTelemetry injection. That injection had put a Java
  agent in StarRocks only.
- **They survive a `helm upgrade`,** because they're in the values. Until
  2026-10-19 a separate `starrocks-placement.sh` patched them onto the live
  cluster, and an upgrade dropped them.
- **Check before a run** that no pod has the agent. This prints nothing when
  clean:

  ```bash
  for p in $(kubectl get pods -n emr-flink -l app.starrocks.ownerreference/name -o name); do
      kubectl get -n emr-flink "$p" -o jsonpath='{.spec.containers[*].env[?(@.name=="JAVA_TOOL_OPTIONS")].value}' \
        | grep -q javaagent && echo "$p: OpenTelemetry agent INJECTED"
  done
  ```

**Comparing the two engines.** `querybench/compare.py` builds one markdown table
per suite from both CSVs. It shows median seconds per arm and like-for-like
ratios: DuckDB ÷ StarRocks on the same storage, and Iceberg ÷ Paimon-export
inside each engine. Cells that are not OK are listed.

```bash
./querybench/compare.py --sr bench-results/starrocks-<STAMP>.csv \
                        --duck bench-results/duckdb_warm-<STAMP>.csv [--suite snapshot]
```

**Fair-comparison rules.**
- **One engine at a time.** `run` refuses while `starrocks.sh` or the other
  warm server is busy (`ALLOW_CONCURRENT=1` overrides). There are two
  servers but StarRocks has one cluster, so they must never run together, and
  the hot/snapshot writer states already keep them apart.
- **Start both caches in the same state.** StarRocks' datacache persists across
  BE restarts and holds earlier runs' blocks. Either empty it (each BE's
  `storage/datacache/`, then restart the BE) and `clear-cache` DuckDB, or warm
  both with the same discarded pass (`WARMUP`).
- **Both caches are on, as both engines' defaults.** StarRocks keeps its
  datacache (150 GiB disk + ~3.8 GB memory per BE), and warm DuckDB keeps a
  matching disk + memory cache (`CACHE_MODE=on_disk`, see below).
- **S4 runs with a narrower scan on StarRocks.** `S4_wide_json.sql` carries
  `-- bench: io-tasks=1`, so `starrocks.sh` sets
  `connector_io_tasks_per_scan_operator = 1` for that one query (2 elsewhere).
  - With the cache on, S4's wide `event_json` scan filled the whole 17.5 GiB
    query pool on a BE (`MEM_LIMIT_EXCEEDED` on both passes, 2026-10-17).
  - With the cache off it held 0.2-0.5 GB, but turning the cache off would make
    the two engines' setups differ.
- Output: `bench-results/duckdb_warm-<STAMP>.csv` (`engine=duckdb_warm`) and
  `.log`, every pass and the pinned scan per query.
- DuckDB **2.0 is not released** (PyPI has 2.0.0 dev builds only, 2026-10);
  `DUCKDB_VERSION` defaults to stable 1.5.6, and community extensions such as
  cache_httpfs are not published for dev builds.

`SUITE=all` (the default) runs hot, stops the writers, then runs snapshot: right
for one engine alone, wrong for two, since the second engine's hot suite would
meet stopped writers. The stop patches each FlinkDeployment to `suspended` and
waits (600 s, then aborts) until no job is `RUNNING` and no
`<job>-taskmanager-*` pod is left; only then has the last commit landed.
Before it, StarRocks aborts if a `cid_events` query is already running on the FE:
killing a harness does not kill its FE query. The debug `--shell` has the
prelude, a fresh secret and a view `t` over the arm's scan; its windows are
fresh, so nothing timed there is quotable.

### Query benchmark knobs

| Env var | Default | Effect |
|---|---|---|
| `SUITE` / `RUNS` / `WARMUP` | `all` / `3` / `1` | `hot`\|`snapshot`\|`all`; timed / discarded passes; result = median of timed passes |
| `SUSPEND_WRITERS` / `FLINK_JOBS` | `1` / `flink-cid-paimon flink-cid-iceberg` | `0` = never touch the writers; which writers are stopped and recorded |
| `HOT_WINDOW_SECONDS` | `30` | hot window length, shared by H1-H6 (~90M rows at ~3M rows/s) |
| `HOT_TO`, `HOT_FROM`, `HOT_DT` | now, `HOT_TO` − `HOT_WINDOW_SECONDS`, `HOT_TO`'s date (UTC) | the hot window; pin to rerun an earlier one |
| `SNAPSHOT_DT` | oldest `dt` in **both** tables | the snapshot partition |
| `SNAPSHOT_S4_MINUTES` | `3` | width of S4's `eventTimeDT` slice, centred in the committed span |
| `SNAPSHOT_S4_FROM` / `_TO` | derived from the commit history | pin S4's slice; both, as `YYYY-MM-DD HH:MM:SS` |
| `OUT_DIR` / `STAMP` | `bench-results/` / UTC now | |
| `NAMESPACE`, `FE_POD`, `PAIMON_DB`, `ICEBERG_DB`, `TABLE_NAME` | `emr-flink`, `kube-starrocks-fe-0`, `flink_cid_paimon_db`, `flink_cid_iceberg_db`, `cid_events` | |
| `ARMS`, `AUDIT` (StarRocks) | all three; `/opt/starrocks/fe/log/fe.audit.log` | arms run serially |
| `FORMAT` (DuckDB) | `iceberg` | `iceberg`\|`paimon`\|`both` (parallel) |
| `PIN_SNAPSHOT` (DuckDB) | `1` | pin the Iceberg `metadata.json`; leave on |
| `DUCKDB_MEMORY_LIMIT` / `DUCKDB_THREADS` | `56GB` / `18` | under the 72Gi limit / = cpu limit |
| `DUCKDB_MAX_TEMP_SIZE` / `SPILL_DIR` | `180GB` / `/tmp/duckdb-spill` | spill, on its own 200Gi gp3 volume |
| `DUCKDB_VERSION`, `SCHED_GRACE`, `KEEP_JOBS` | `1.5.6`, `900` s, `0` | `duckdb-cli` version; Karpenter wait; keep Jobs (each holds a node) |

The per-query timeout is fixed and equal on both engines (**1800 s hot, 3600 s
snapshot**), so `TIMEOUT` means the same thing on both.

### Outputs

`bench-results/starrocks-<STAMP>.csv` and `bench-results/duckdb_warm-<STAMP>.csv`,
one row per arm × query, same columns:

```
stamp,engine,arm,suite,query,status,median_ms,result_rows,scan_rows,scan_bytes
```

`status`: StarRocks `OK`/`ERROR`/`MISSING`; DuckDB `OK`/`ERROR`/`TIMEOUT`/`ZERO_SCAN`.
A failed query has an empty `median_ms`, never 0. `scan_rows`/`scan_bytes` are
the FE's `ScanRows`/`ScanBytes` medians, empty on DuckDB (no such counters). The
StarRocks CSV adds `#` lines per suite: writer state (`# suite=… writers: …`),
run parameters (`dt`, `runs`, `warmup`, `arms`, `run_id`), and one `# state` line
per arm with the partition's row count from metadata (plus `$files` for the
Iceberg-reader arms). Its console table adds `ice/pai` and `pmn/pai` ratios (>1 =
slower than paimon). Warm DuckDB writes every pass (time, status, first row, the
pinned scan) to `bench-results/duckdb_warm-<STAMP>.log`, headed
`# ---- suite=… arms=… writers: …` and the server's `/health`.

### The queries

`querybench/{hot,snapshot}/*.sql`. File name = query name, lexical order = run
order; add a file and it runs. Each header says what to read it against:

| Query | Measures | Read it against |
|---|---|---|
| `H1_freshness` | newest row + count, last 30 s | sanity: `rows_in_window = 0` = writers down, every hot number an empty scan. Lag = `HOT_TO` − `newest_row` |
| `H2_hot_count` | `COUNT(*)`, same window | the planning floor: no value columns, almost all listing and pruning |
| `H3_hot_needle_uuid` | `SUBSTR(uuid, 20, 4) = '0000'` (never true for a UUIDv7), 30 s | control: the function defeats min/max pruning, so a full window scan of `uuid`. A literal `uuid = '...'` would prune almost every file, because UUIDv7 is time-ordered. Much faster here than on H4 = winning on aggregation, not the scan |
| `H4_hot_narrow_agg` | `GROUP BY eventId`, 1 of 35 cols | projection pushdown; H5/H4 is the fat-column tax, compare it across arms |
| `H5_hot_wide_json` | bytes of the ~3.3 KB `event_json` | most sensitive to file count/size; `payload_bytes` must match across engines |
| `H6_hot_customer_agg` | `COUNT(DISTINCT)` per customer, 30 s | hash table and spill, not the read path |
| `S1_partition_count` | full-partition `COUNT(*)` | the normaliser; no metadata shortcut, caches off |
| `S2_narrow_agg` | H4's shape over the partition | S2 − S1 = the aggregate's cost at constant scan |
| `S3_risk_distinct` | `COUNT(DISTINCT customerId)` by risk level | the memory-pressure query |
| `S4_wide_json` | fat-JSON decode, 30-min ingestion slice | a decode rate over a slice, **not** comparable with S1-S3 |

Placeholders: `${TABLE}` (StarRocks: `cid_events` after `SET CATALOG`/`USE`;
DuckDB: resolved in the pod per arm and query), `${BYTES_FN}` (`LENGTH` /
`STRLEN`, both count bytes; DuckDB's `LENGTH` counts characters and
`octet_length()` is BLOB-only), and the window literals `${HOT_DT}`,
`${HOT_FROM}`, `${HOT_TO}`, `${SNAPSHOT_DT}`, `${S4_FROM}`, `${S4_TO}`.
A `${...}` surviving render is fatal: on neither engine is it a syntax error, it
binds and returns a wrong answer. `-- bench:` flags feed DuckDB's zero-scan
guard: `allow-zero-rows` (H3), `leading-col-is-label` (H4, H6, S2, S3).

**No clock functions; windows are resolved once per run** and rendered as
literals, so every arm and pass reads the same rows. The FE session timezone is
`+08:00` and the data is UTC, so `NOW()` would also miss the window, and an
empty scan reports as the fastest result.

### The snapshot partition and the writers

`SNAPSHOT_DT` defaults to the **oldest `dt` present in both tables** (one
delimited S3 `LIST` per table root): the most-closed partition every arm can
read. Not "yesterday", which is often still being written (event time lags
ingest) or absent, pruning to an instant empty win. S1-S3 are bounded by `dt`
alone: full-partition scans. S4 adds a time slice, since unbounded it ran 18-41
min per pass per arm.

**S4 slices on `eventTimeDT`, not `ingestionTime`.** The comparison needs the
same rows in the same time slot of the same partition in every format.
- **`eventTimeDT` is set by the generator,** so both tables hold exactly the
  same rows in any `eventTimeDT` window.
- **`dt` is derived from it,** so the window is always inside the partition.
- **`ingestionTime` is each writer's own processing time.** The same
  `ingestionTime` window held 471M rows in Paimon and 480M in Iceberg
  (2026-10-17).
- **A slice sized by share of rows was rejected for the same reason.**

The window is `SNAPSHOT_S4_MINUTES` (default 3) wide and is **derived**, since
each run happens at a different date and time.
- **It is centred in the span where both tables were committing.** That span
  comes from the snapshots in the Iceberg table's `metadata.json` and Paimon's
  retained snapshot files.
- **The span is clipped** to `SNAPSHOT_DT`'s own day, with 120 s (two
  checkpoints) kept clear of each end.
- **Event time runs ahead of commit time by the ingest lag,** so the centre of
  the committed span is well inside the generator's run.
- **Size:** at ~2.6M rows/s, 3 min is ~465M rows. S4's own `n` is the exact
  count and must be equal on every arm.
- **Writers must be stopped and drained,** or the newest rows are not in every
  table yet.
- **Example:** for 2026-10-10 the window is `[15:06:00, 15:09:00)`.

**The writers must be stopped.** Live, the "closed" partition is a moving target
(compaction and commit contention): the same 944,023,427 rows and DuckDB query
took **332,520 ms live vs 3,742 ms stopped, 89×**, two orders of magnitude over
the 1.1-1.5× format effect. StarRocks re-reads writer state after each suite and
refuses to record one during which it changed.

### StarRocks: timed from the FE audit log

- **Server-side timing.** One `kubectl exec … mysql` costs ~5.5 s here, more than
  many queries, so shell timing measures the harness. Each arm's suite goes down
  one connection, a per-run marker `SELECT` before each statement; the audit
  records after each marker give `Time`, `ScanRows`, `ScanBytes`, `ReturnRows`,
  `State`.
- **A pass whose `State` is not `EOF`/`OK` makes the query `ERR`.** A failed query
  logs a small `Time` and `ScanRows=0`, so an OOM would otherwise score fastest.
  Slow queries are logged twice (`[slow_query]` and `[query]`); the duplicate is
  dropped, or every later record shifts one slot.
- **Datacache off** (`enable_scan_datacache`, `enable_datacache_sharing`,
  `populate_datacache_mode='never'`): else pass 1+ is a BE cache hit
  (`ScanBytes=0`, 70 s → 2 s) scored as a format result.
- **`enable_spill = true`** covers wide aggregates and `COUNT(DISTINCT)`, but
  full-partition scans OOM **scan-side** (page buffers), which spill cannot reach.
  So snapshot also sets `connector_io_tasks_per_scan_operator = 2` **and**
  `enable_connector_adaptive_io_tasks = false`; without the second the adaptive
  scheduler raises the task count again.
- **Arms run serially.** Three concurrent full-partition scans exhaust the BE
  pool: all three aborted with `Memory of default_wg exceed limit ... Used:
  18786279880, Limit: 18786186952` on `kube-starrocks-be-0`.
- The Paimon export is read via a `hadoop` Iceberg catalog: Paimon registers it
  in Glue with `PaimonStorageHandler`, which StarRocks' Glue catalog cannot load.

### DuckDB: one warm server per suite

- **Sizing.** One long-lived Deployment per suite (`duckdb-warm-hot`,
  `duckdb-warm-snapshot`), each one on-demand `m5.8xlarge` on `driver-nodepool`,
  18 cpu / 72Gi, requests = limits.
  - **CPU/RAM parity** with StarRocks' 3 BEs (3 × 6 cpu / 24Gi), same family and
    arch. Not locality parity (one node's memory bandwidth and NIC).
  - The servers land on separate nodes (anti-affinity), and the pods carry
    `karpenter.sh/do-not-disrupt`.
- **One process.** `duckdb_warm_server.py` holds one DuckDB connection for the
  pod's life and runs one query at a time.
  - `duckdb-warm.sh` renders the queries via `lib.sh` on the laptop (or in
    `qb-runner`) and submits each pass over a port-forward.
  - The server fills `${TABLE}` and times execute + fetch inside the process.
- **Pass-major, like StarRocks.** S1-S4 warm-up, then S1-S4 timed, one job per
  pass; the median is taken by the client. Query-major order (S1 S1, S2 S2, ...)
  let each DuckDB timed pass follow its own warm-up while StarRocks' S4 warm-up
  had pushed S1-S3 out of its cache.
- **A client deadline per pass:** the query timeout + 5 min. The server's own
  timeout (`con.interrupt`) did not stop an I/O-bound scan on 2026-10-17. On the
  deadline, or a lost job, the pass is `TIMEOUT`/`ERROR` and the server restarts
  (its cache is lost; the log says so).
- **IRSA via STS.** `credential_chain` cannot use IRSA in this pod, so the server
  calls `sts assume-role-with-web-identity` and writes a `CREATE SECRET`. It is
  refreshed every 20 min, outside the timer (an IRSA session lasts ~1 h).
- **`PIN_SNAPSHOT=1`** (both arms). The writer commits a `metadata.json` every
  ~60 s and keeps ~4; even a fresh one is GC'd within ~60 s and 404s mid-scan.
  Each query copies it to `s3://…/duckdb-cid-pinned/warm-<pod>/` (manifests and
  data are retained ~1 h) and scans the copy.
- **ZERO_SCAN.** No rows, or a leading `0`/`NULL` without the label flag, is
  `ZERO_SCAN`: the predicate matched nothing, **not** a win.
- **Memory.** `memory_limit` sits under the container limit because DuckDB does
  not count httpfs/Arrow buffers or the cache_httpfs memory cache. An OOM-kill
  (exit `137`) needs container headroom; **don't** raise `DUCKDB_MEMORY_LIMIT`.
- **The `paimon` arm resolves from `version-hint.text`, not Glue.** Paimon's
  `hive-catalog` export updates the Glue entry, but on 2026-10-09 Glue pointed at
  `v25.metadata.json` while `v28` was already written. That lag is exactly the
  newest minutes the hot suite reads. The arm reads
  `<PMNICE_TABLE_ROOT>/metadata/version-hint.text` and pins
  `v<N>.metadata.json` like the `iceberg` arm, since the export keeps only 3
  previous versions.
- **Arms run serially,** as on StarRocks. Hot windows are pinned once, before
  the first arm, so every arm reads the same window.

### Interpreting the numbers: rows differ across arms

The arms do **not** hold the same rows. In the measured partition Iceberg held
**1.2993×** Paimon's, because Paimon's write path is ~23% slower at equal uptime
(the bucketed hash shuffle; `bench-results/FLINK-THROUGHPUT-ANALYSIS-20261005.md`).
Raw elapsed is not comparable; divide by each arm's S1 count (or `# state …
rows=`). Per-row full-partition scan cost, serial and uncontended:

| arm | ScanBytes | B/row | CpuCostNs | ns/row |
|---|---|---|---|---|
| paimon | 122.3 GB | 38.02 | 1,426.7 B | 443.4 |
| pmn-as-ice | 120.2 GB | 37.37 | 1,398.2 B | 434.6 |
| iceberg | 176.4 GB | 42.19 | 1,966.3 B | 470.4 |

Iceberg costs **1.11× Paimon's bytes/row** and **1.06× its CPU/row**. Caveat: one
write run (2026-10-05), measured when `ScanRows` equalled each arm's full
partition (the scan volume S1-S3 read now). Earlier *concurrent* runs suggested a
6.5× bytes gap and are not reliable; trust serial full scans only. The headline
format gap is a **write-path** story, and a latency gap at equal `ScanBytes` is
engine/metadata overhead, not I/O.

Neither harness reports data-file counts or the delete-artifact counts that must
read 0; get them from [Secondary comparisons](#secondary-comparisons).

---

## Raw landing-zone generator (gzipped NDJSON)

The data source of the Flink jobs, and of anything else that reads the raw
landing zone. It writes `.json.gz` files to S3 directly — no Flink, no table
format; the jobs read them through SQS
([below](#feeding-the-flink-jobs-from-these-files)).

- **Real data shape.** The fields, value ladders and presence rates of the real
  `cid_9011075d_sample_masked` extract.
- **The source's layout, minus its `x=00/` shard level:**
  `year=YYYY/month=MM/day=DD/hour=HH/customerId=<uuid>/productCode=sds|ddr/<coupledTraceId>_NNN_new.json.gz`
  — one file per agent upload (one customer, one coupledTraceId), ~2,500 rows.
- **Per-row targets:** ~4,096 B uncompressed, ~0.2 KB gzipped (~20x; the real
  sample is 3,818 B / 195 B). The ratio comes from within-file locality
  (endpoint runs, process bursts), not from padding — see the header of
  `cid_jsongz_gen.py`.
- **Rate:** a **per-pod cap**, `JSONGZ_ROWS_PER_POD` (default 500,000 rows/s ≈
  2 GB/s raw, delivered at the cap by 60 workers on m6i.16xlarge), split
  over the pod's `WORKERS`. Cluster rate = replicas x per-pod. Replicas are
  `JSONGZ_REPLICAS` if set, else `ceil(JSONGZ_ROWS_PER_SECOND / per-pod)`;
  the default `JSONGZ_ROWS_PER_SECOND` of 1,500,000 rows/s (≈ 6.1 GB/s raw,
  ≈ 0.31 GB/s gz) gives 3. `scale N` changes it live. There is no
  `TOTAL_PODS` to set (a stray one is honoured as `JSONGZ_REPLICAS`, with a
  warning). This is the **only** rate knob in the benchmark: the Flink jobs
  read as fast as they can.
- **Run length: `DURATION_SECONDS`, the only time setting.**
  - **`> 0`:** every pod writes that long from its own start (default 1800,
    e.g. `DURATION_SECONDS=120` for a smoke test), then exits. The Job
    completes and its nodes are released.
  - **`0`:** every pod writes until stopped: `stop`, or `querybench/hot-watch.sh`
    after a hot query test. Nothing stops it by itself.

  Anything but a whole number of seconds is rejected. A pod restarted after a
  failure, or added by `scale N`, writes a full `DURATION_SECONDS` from its own
  start.
- **No pod index.** Each pod seeds its RNG from its own name (downward API),
  so file names cannot collide across replicas.

```bash
cd throughput-benchmark/jsongz-gen
source ../.bench-env
./run-jsongz-gen.sh render                # sizing summary + write jsongz-gen-job-deployed.yaml (no apply)
./run-jsongz-gen.sh start                 # render, ConfigMap, kubectl apply -f the deployed file; writes DURATION_SECONDS
./run-jsongz-gen.sh scale 4               # replicas -> 4 (new pods write DURATION_SECONDS from their own start)
./run-jsongz-gen.sh status                # per-pod rows/s, MB/s, B/row, ratio
./run-jsongz-gen.sh stop                  # delete the Job + ConfigMap; written files stay
# Knobs: JSONGZ_REPLICAS JSONGZ_ROWS_PER_POD JSONGZ_ROWS_PER_SECOND(-> replicas)
#        DURATION_SECONDS(seconds, 0 = until stopped; default 1800) WORKERS
#        GZIP_LEVEL ROWS_PER_FILE CID_CUSTOMER_COUNT CID_HOST_COUNT OUTPUT_URI
#        GEN_NODEPOOL
```

The generator is a Deployment (strategy `Recreate`, `restartPolicy: Always`)
and follows the same deploy pattern as the Flink jobs and the monitor:
`jsongz-gen-job.yaml` is rendered with `envsubst` over an explicit variable
list to `jsongz-gen-job-deployed.yaml` (gitignored by `*-deployed.yaml`), any
leftover `${...}` fails the run, and the file is applied with `kubectl apply -f`.
Re-running `start` re-applies in place: the new deadline changes the pod
template and `Recreate` replaces every pod. `start` deletes the legacy Indexed
Job `cid-jsongz-gen` if one is still there (the manifest file keeps its
`-job` name).
Placement pins only the nodepool (`GEN_NODEPOOL`, default
`executor-memorynodepool`), `amd64` and on-demand; there is no instance
family/size selector. Karpenter sizes the node from the pod's requests
(`WORKERS` cpu, 24Gi): the default `WORKERS=60` needs a >=64-vCPU node, i.e. a
16xlarge (the pool's largest size), one pod per node.

### Feeding the Flink jobs from these files

Each format's `00-source.sql` is `01-raw-s3sqs.sql` followed by
`01-parse-s3json.sql` (both in `sql-scripts/common/`), concatenated by
`upload_scripts`. The raw fragment defines `s3json_raw(line STRING)` over the
custom `sqs-delta-filename` connector (built from `sqs-delta-filename/`), which
long-polls a queue of S3 event notifications and reads each object key it is
given. Nothing lists the bucket, so job start costs the same at any backlog.
The parse fragment turns each line into the `datagen_source` view (the name is
historical) with the 34 columns of both sinks, so its `json_tuple` parse is
byte-identical across the two formats.

```bash
cd throughput-benchmark && source .bench-env          # OUTPUT_URI = .../cid-landing-jsongz
./jsongz-gen/run-jsongz-gen.sh start
./build-deploy-cid.sh deploy paimon     # builds + uploads the jar, prints the backlog, deploys
```

- **New files stop arriving after 30 minutes**, when the generator Job
  completes. The Flink job keeps running and polling, but idles once it has
  read the backlog. Size the measurement window to fit inside those 30
  minutes.
- The rate is what the generator lands, and the job reads as fast as it can.
- **The source can be the bottleneck.** Each row costs an S3 read, an
  inflate and one `json_tuple` parse of a 4 KB line. Backpressure with
  idle sinks means the read side is the wall, not the table format.

#### Reading through SQS

Infrastructure (provisioned outside this script; `deploy` only reads it):

```
s3://$BUCKET_NAME/cid-landing-jsongz/*.json.gz   ObjectCreated notifications
  └─> SNS cid-landing-jsongz-events
        ├─> SQS cid-landing-jsongz-paimon    read by flink-cid-paimon
        └─> SQS cid-landing-jsongz-iceberg   read by flink-cid-iceberg
```

The fan-out gives each format its own copy of every event, so the two jobs
never compete for messages. The job role (`EMR_EXECUTION_ROLE_ARN`) carries the
IAM policy `FlinkLandingQueueConsume`, its consume permissions on both queues;
an `AccessDenied` from the source means the policy is missing from that role.

| Env var | Default | Meaning |
|---|---|---|
| `SQS_QUEUE_PREFIX` | `cid-landing-jsongz` | queue `<prefix>-<fmt>`; the URL is built per format in `render_manifest`, so it is the **only** value that differs between the two jobs' `00-source.sql` inputs |
| `SQS_POLL_THREADS` | `8` | concurrent `ReceiveMessage` loops |
| `SQS_MAX_PENDING` | empty | cap on received, unassigned splits; empty = the connector's default (the option line is then not rendered) |
| `SQS_VISIBILITY_TIMEOUT` | `15min` | must outlast the 60 s checkpoint plus a file's read time, or the message reappears and its file is read twice |
| `SQS_SOURCE_JAR` | `s3a://$BUCKET_NAME/flink/jars/sqs-delta-filename.jar` | where `upload` puts the jar and `ADD JAR` reads it |

- **Purge between runs. The queue is the run's state.** Whatever is still
  queued, including messages a deleted job received but never deleted, is the
  first thing the next job reads, and the `rm` in
  [7d](#7-teardown-between-runs) leaves messages pointing at deleted objects.
  `deploy` prints `visible` / `in-flight` per queue and warns if either is
  non-zero, but it **never purges**. Do that yourself, with every writer and
  the generator stopped:
  ```bash
  for f in paimon iceberg; do
    aws sqs purge-queue --region "$AWS_REGION" \
      --queue-url "https://sqs.$AWS_REGION.amazonaws.com/$AWS_ACCOUNT_ID/cid-landing-jsongz-$f"
  done   # SQS allows one purge per queue per 60 s, and it takes up to 60 s to finish
  ```
- **Only new objects are announced.** Files already under a prefix, and any
  prefix other than `cid-landing-jsongz/`, produce no events. To read them,
  enqueue them with the backfill tool at `sqs-delta-filename/backfill/`,
  which turns an existing prefix's keys into queue messages.
- The TaskManagers need no SQS env vars: the executor substitutes the
  connector options on the JobManager, and they reach the TMs inside the job
  graph. The jar reaches them the same way, through `ADD JAR`.

---

## How it works

The source is the gzip NDJSON the generator lands, read through SQS (see
[above](#feeding-the-flink-jobs-from-these-files)); there is no Kafka/RDS in
the read path. If a writer can't keep up, the source backpressures and the
achieved source rate **is** that format's result — provided the source itself
is not the wall (backpressure with idle sinks), since each row costs a read, an
inflate and a JSON parse.

The row is a **mockup of real security telemetry**, not synthetic padding. It
reproduces the shape of a production CID extract (`cid_9011075d_sample_masked`:
189,013 gzipped-NDJSON records, 170 distinct fields) and emits the 33 extracted
columns + `event_json` from the ingestion PoC's `schema_sample.sql`. The *data*
has real field cardinalities, real skew and real null density, so Parquet
compression, dictionary encoding and min/max pruning behave as they do on the
real table. See `jsongz-gen/cid_jsongz_gen.py` for the measured
distributions; `event_json` is the whole raw line, verbatim.

**Row size is calibrated to a ~4096 B mean.** The real sample's mean line is
3,817 B, so the generator widens the five command-text fields (see
`PROCESS_CMD_LEN` and its neighbours in `cid_jsongz_gen.py`).

Two consequences worth knowing before citing a number:

- **The pad is on the command fields, not a filler column.** Those fields are
  already where the real bytes are (the top four are 21% of sample bytes), and
  they are variable-width and sparsely present, so widening them preserves the
  null pattern and compression behaviour. A dense fixed-width filler would hit
  the byte target while destroying the properties this mockup exists to
  reproduce.
- **The model has no heavy tail.** The sample's p95 is 8,625 B and its max
  111,445 B — the real outliers are pathological command lines.
  Irrelevant to a throughput number (driven by the mean); relevant if you are
  chasing per-row buffer high-water marks.

Because the size is a **mean over a distribution**, `rows/s × 4096` is an
expectation, not an instantaneous bytes/s guarantee.

### What append-only implies

- Paimon does not merge sorted runs to resolve keys; an append table's
  compaction only coalesces small files. Writers use `AppendOnlyFileStoreWrite`,
  and `write-only=false` leaves that coalescing inline in the writer — the only
  thing bounding file count, since no compactor ships here.
- Iceberg writes **no delete files at all**, so merge-on-read cost is absent from
  both the write and the read side.
- Primary-key knobs are **removed, not set to false**, on both sides: Paimon has
  no `changelog-producer` / `deletion-vectors` / `num-sorted-run.*`, and Iceberg
  has no `write.upsert.enabled` and no `write.*.mode=merge-on-read`. Absent is
  stronger than disabled — there is no property to accidentally flip.

Both sinks declare the identical 34 columns in the identical order with the same
identity partitioning on `dt`. The generator stamps rows with wall-clock UTC
time, so a run writes **one** live partition (two if it crosses midnight).

### Parity matrix

Every option with a counterpart is pinned on both sides. Change one, change its
pair:

| Dimension | Paimon | Iceberg |
|---|---|---|
| Compression | `file.compression=snappy` | `write.parquet.compression-codec=snappy` |
| Row groups | `parquet.block.size=33554432` | `write.parquet.row-group-size-bytes=33554432` |
| Target file | `target-file-size=256mb` | `write.target-file-size-bytes=268435456` |
| Snapshot retention | `snapshot.time-retained=1h`, `num-retained.min=5`/`max=240` | `history.expire.max-snapshot-age-ms=3600000`, `min-snapshots-to-keep=5` |
| Manifests | `manifest.target-file-size=8mb`, `merge-min-count=5` | `commit.manifest.target-size-bytes=8388608`, `min-count-to-merge=5` |
| Column stats | `metadata.stats-mode=truncate(16)` | `write.metadata.metrics.default=truncate(16)` |
| Commit cadence | `execution.checkpointing.interval=60s` | same |
| Barrier alignment | aligned only (hard Paimon limit) | `unaligned.enabled=false` to match |

Four of these are easy to break and worth restating:

- **`snappy`, with no compression level on either side.** Snappy has no levels,
  so the option is *removed* rather than set — a stray level would be ignored on
  one side and meaningful on the other.
- **`num-retained.max=240`, not 20.** Iceberg expresses snapshot retention by
  **age only** and has no count ceiling to pair with, so any Paimon count bound
  tight enough to bind before the age rule silently gives the two sides
  different history — Paimon pinned at the count while Iceberg keeps everything
  inside the age window. 240 ≈ 4 h of 60 s commits, so the 1 h **age** rule
  always binds first and both sides agree on the mechanism. It stays finite as a
  runaway-commit backstop.
- **Unaligned checkpoints must stay `false` on both.** The Paimon sink *cannot*
  support them (`paimon-flink-1.20-1.3.2.jar` throws outright). Leaving Iceberg
  unaligned would hand it an option on precisely the axis Paimon is structurally
  forced to pay.
- **Column stats are pinned explicitly**, not left to either engine's default
  happening to match. `truncate(16)` also matters on the read side: with `none`,
  StarRocks gets no file-level min/max on the Iceberg side and cannot prune
  files.

Options with **no counterpart** are either the thing being measured (Paimon's
`write-buffer-size`) or inherent asymmetries — including the fact that the two
sinks expose their upload shape through different configuration surfaces — see
[Known asymmetries](#known-asymmetries).

### Config differences that are on purpose

- Paimon uses `changelog-producer='none'` (CDC tables use `lookup`): no
  downstream changelog consumer here, and lookup adds compaction-time
  point-lookup cost Iceberg has no analog for.
- Paimon is an **unaware-bucket** append table (`bucket = -1`, no
  `bucket-key`), matching Iceberg's `PARTITIONED BY (dt)` +
  `write.distribution-mode = 'none'`. On a table with no primary key, `-1`
  means *no* bucketing, unlike the CDC tables' PK dynamic-bucket mode: it keeps no
  key→bucket index. Rows are not shuffled, so the writer chains into the source
  vertex exactly as `IcebergStreamWriter` does. The earlier fixed buckets on
  `uuid` put a `keyBy` in front of the writer. That serialized every row
  (~5.7 KB) across the network, gave the writer its own vertex on the same
  cores, and held Paimon at ~820 K rows/s against Iceberg's ~1.4 M (2026-10-09).
  `PAIMON_BUCKETS=N` brings that mode back for comparison. Use
  `N = JOB_PARALLELISM`: each bucket has one writer, so fewer buckets idle
  writers (`/2` left 80 of 160 with no rows, 2026-10-06..09).
- Both jobs set `pipeline.object-reuse: true`. With it off, every chained
  hop deep-copies the row, which was 27% of task CPU in the 2026-10-09 profile.
- Iceberg uses `write.distribution-mode='none'`, **not** `'hash'`. With one
  distinct `dt`, hashing on the partition key would serialize every write into a
  single subtask. Paimon's bucket hash is on `uuid`, not the partition column,
  so it has no equivalent failure mode.

### Iceberg-compat toggle

`ICEBERG_COMPAT` chooses which Paimon you benchmark:

| | `false` (script default) | `true` (`.bench-env`) |
|---|---|---|
| Framing | Paimon's bare write path | Paimon exactly as the CDC pipeline runs it |
| Commit cost | Paimon metadata only | + Iceberg V2 metadata export to Glue per commit |
| External reads | Paimon readers only | Athena / StarRocks / DuckDB via the export |

It maps to the table's `'metadata.iceberg.storage'` (`disabled` vs
`hive-catalog`). **The value is baked into the Paimon schema at `CREATE TABLE`**
— the DDL is `IF NOT EXISTS`, so flipping it does nothing to an existing table.
Drop the table first. The Iceberg-side job is unaffected either way. Paimon
write numbers taken at `true` are **not** comparable with `false` runs: the
export is commit-path work the Iceberg side does not pay.

The Paimon write-path options are instead applied as **job-scoped INSERT hints**
in `sql-scripts/paimon/04-pipeline.sql`, which take effect against an existing
table with no drop. Two of the three are **fixed**: `write-only=false` and
`write-buffer-spillable=true`, hardcoded in `build-deploy-cid.sh`. Iceberg's
writer has no equivalent of either, so a non-default Paimon value makes the two
sides structurally different write paths and the result stops being about
format. The `WRITE_ONLY`/`SPILLABLE` env knobs were removed 2026-10-05 along
with the exploratory arms that gave them a second value. Only `WRITE_BUFFER`
remains tunable.

---

## Reading the results

The headline number is `source_rec_per_s` in each CSV:

- At the configured rate, the format **keeps up** — compare
  `last_ckpt_duration_ms` (snapshot-commit cost) and sink `busy_ms` for
  remaining headroom.
- Below it, the source is backpressured by that format's writer and the
  achieved rate **is** its write-path ceiling. `GiB/s = source_rec_per_s × 4096 / 2³⁰`.

Watch Paimon's rate over time, not just its average: compaction cost is
periodic, so throughput saw-tooths. On an append table that saw-tooth is
small-file coalescing, not key resolution. A single mean hides it.

**Never quote the ingest rate on its own.** Either side can buy throughput by
deferring maintenance — small files and unexpired snapshots. Pair the rate with
stored bytes, live record count, **data-file count and mean file size**, and
snapshot count. Delete-file and deletion-vector counts are an **assertion that
must read 0**, not a cost explanation: non-zero means something violated
append-only and the run is void.

Check *where* a stalled job is stalled before blaming the format. A writer
reporting low `busy_ms` **and** low backpressure while its upstream vertex is
blocked is stuck inside Paimon's barrier-time buffer flush, which Flink counts as
neither busy nor backpressured — a checkpoint-tuning problem, not a write-path
ceiling. Compare `last_ckpt_duration_ms` against the 60 s interval; `kubectl top
pod` settles it, since saturated TMs mean a real ceiling and idle TMs mean a
stall.

**Paimon's flush time appears in no checkpoint metric — derive it.** The cost
lives in `PrepareCommitOperator.prepareSnapshotPreBarrier()`, which Flink
attributes to neither `sync`, `async` nor `alignment`. Compute it as a residual,
`e2e − start_delay − alignment − sync − async`:

```bash
JM=$(kubectl get pods -n emr-flink --no-headers \
       | awk '/flink-cid-paimon-[0-9a-f]{9,}/{print $1}' | head -1)
kubectl exec -n emr-flink "$JM" -c flink-main-container -- bash -c '
JID=$(curl -s localhost:8081/jobs | python3 -c "import sys,json;print(json.load(sys.stdin)[\"jobs\"][0][\"id\"])")
VID=$(curl -s localhost:8081/jobs/$JID | python3 -c "
import sys,json
print([v[\"id\"] for v in json.load(sys.stdin)[\"vertices\"] if v[\"name\"].startswith(\"Writer\")][0])")
CK=$(curl -s localhost:8081/jobs/$JID/checkpoints | python3 -c "
import sys,json
print([c[\"id\"] for c in json.load(sys.stdin)[\"history\"] if c[\"status\"]==\"COMPLETED\"][0])")
curl -s localhost:8081/jobs/$JID/checkpoints/details/$CK/subtasks/$VID | python3 -c "
import sys,json
s=json.load(sys.stdin)[\"summary\"]
e=s[\"end_to_end_duration\"][\"max\"]; d=s[\"checkpoint_duration\"]
sy=d[\"sync\"][\"max\"]; a=d[\"async\"][\"max\"]
al=s[\"alignment\"][\"duration\"][\"max\"]; sd=s[\"start_delay\"][\"max\"]
print(\"e2e=%d sync=%d async=%d align=%d start_delay=%d -> FLUSH=%d ms\"%(
  e,sy,a,al,sd,e-sy-a-al-sd))"'
```

Query a **COMPLETED** checkpoint — in-progress ones return nulls and `-1`, and
subtracting them raises a type error. `/checkpoints/details/<id>` alone gives
per-vertex objects with no timing breakdown; the `/subtasks/<vertex-id>` path is
the one with `summary`. **These fields die with the JobManager.**

**Verify Paimon actually compacted.** With `write-only=false` the writer should
produce `COMPACT` snapshots alongside `APPEND` ones; all-`APPEND` means nothing
was ever coalesced and the file count grew unbounded:

```bash
aws s3 cp s3://$BUCKET_NAME/cid-warehouse-paimon/flink_cid_paimon_db.db/cid_events/snapshot/ \
    - --recursive 2>/dev/null | grep -o '"commitKind":"[A-Z]*"' | sort | uniq -c
```

### Secondary comparisons

```bash
# Stored bytes and file counts per warehouse:
aws s3 ls --recursive --summarize s3://$BUCKET_NAME/cid-warehouse-paimon/  | tail -2
aws s3 ls --recursive --summarize s3://$BUCKET_NAME/cid-warehouse-iceberg/ | tail -2

# Iceberg file/delete accounting — from the snapshot summary, NOT from S3
# filenames. Iceberg names delete files with the same NNNNN-N-<uuid>-NNNNN.parquet
# pattern as data files, so grepping keys for "delete" returns 0 even when
# delete files are present.
LATEST=$(aws s3 ls s3://$BUCKET_NAME/cid-warehouse-iceberg/flink_cid_iceberg_db.db/cid_events/metadata/ \
           | grep '\.metadata\.json' | sort -k1,2 | tail -1 | awk '{print $4}')
aws s3 cp "s3://$BUCKET_NAME/cid-warehouse-iceberg/flink_cid_iceberg_db.db/cid_events/metadata/$LATEST" - \
  | jq -r '{snapshots:(.snapshots|length)}, (.snapshots[-1].summary
      | {total_data_files:."total-data-files", total_delete_files:."total-delete-files",
         total_records:."total-records", total_files_size:."total-files-size"})'

# Paimon's equivalent (live records + last commit kind):
aws s3 cp s3://$BUCKET_NAME/cid-warehouse-paimon/flink_cid_paimon_db.db/cid_events/snapshot/LATEST - \
  | xargs -I{} aws s3 cp s3://$BUCKET_NAME/cid-warehouse-paimon/flink_cid_paimon_db.db/cid_events/snapshot/snapshot-{} - \
  | jq -r '{commitKind,totalRecordCount,deltaRecordCount}'
```

Because the tables are append-only, read latency is **scan economics** — file
count, file size, projection pushdown, partition pruning — not merge-on-read
cost.

---

## Knobs

This table is **write-side only**. The read-side knobs, including the hot
windows and the snapshot partition (one shared resolver in `querybench/lib.sh`,
so one override pins both engines to the same rows), are in
[Query benchmark knobs](#query-benchmark-knobs).

| Env var | Default | Meaning |
|---|---|---|
| `JOB_PARALLELISM` | `128` | writer subtasks. 4 slots per TM, so 32 TMs. `.bench-env` sets `160` |
| `PAIMON_BUCKETS` | `-1` | Paimon `bucket`, **baked at `CREATE TABLE`**. `-1` = unaware-bucket: no shuffle and no `bucket-key`, the same layout as Iceberg. `N >= 1` = fixed buckets on `uuid`, and `build-deploy-cid.sh` then renders `PAIMON_BUCKET_KEY_OPT` (Paimon rejects a `bucket-key` on a `-1` table). Use `N = JOB_PARALLELISM`: each bucket has exactly one writer subtask, so fewer idle writers. Anything below 1 is treated as `-1`. **Changing it does nothing to an existing table** — the sink DDL is `CREATE TABLE IF NOT EXISTS`, so you must drop `cid_events` or use a fresh warehouse prefix, and never compare results across the change without saying which count produced them |
| `WRITE_BUFFER` | `128mb` | Paimon `write-buffer-size`. Only moves bytes between RAM and spill files; the bytes the barrier external-merges are set by rate × checkpoint interval. INSERT hint, so no table drop needed |
| `S3A_MULTIPART_SIZE` | `16M` | **Paimon side only** (floor 5M). The S3A block cut point — must stay **below** the data file size, or S3A uploads each file as a single PUT at `close()` instead of streaming blocks as they fill. `.bench-env` raises it to `32M`. Must be written as an integer number of `M`; the script rejects other units loudly rather than misconverting them |
| `S3A_MULTIPART_THRESHOLD` | `16M` | Multipart COPY path only; the write path never reads it. Kept equal to the part size for consistency, so `.bench-env`'s `32M` moves both |
| `S3A_ACTIVE_BLOCKS` | `6` | queued blocks per output stream before `write()` blocks. Above hadoop-aws's default of 4 for pipelining, deliberately not higher: the disk-buffer cost is `size × blocks × slots × TMs-per-node` against a 50 GiB root volume shared with the spill files |
| `ICEBERG_COMPAT` | `false` | Paimon per-commit Iceberg metadata export. Baked in at `CREATE TABLE` |
| `JM_NODEPOOL` / `TM_NODEPOOL` | `driver-nodepool` / `executor-memorynodepool` | Karpenter nodepools |
| `NAMESPACE` | `emr-flink` | |
| `EMR_VERSION` | `7.13.0` | same release the CDC pipeline runs |
| `IMAGE_TAG` | `$EMR_VERSION` | must exist on the `emr-flink-cdc-paimon` ECR repo |

`write-only` and `write-buffer-spillable` are **not** here: they are fixed at
`false` / `true` in `build-deploy-cid.sh` and cannot be set from the environment.

**Three places set this and they do not agree — know which one you are
running.** `build-deploy-cid.sh` defaults both to `16M`, the 2026-10-05
*reduction* from 32M argued in its own header: at 160 parallelism each subtask's
share of the target is ~51 MiB/s, so files arrive small and often, and 16M keeps
a ~60 MB file at 4 parts instead of 2 — restoring pipelining and giving the
retry path a smaller unit. `.bench-env` overrides both back to `32M`, and the
SQL fallbacks in `sql-scripts/paimon/02-catalog-setup.sql` are `32M`. So a run
that forgets to source `.bench-env` gets 16M, and a run that loses the
JobManager env gets 32M. **Report the value with any result.** The per-node disk
arithmetic that `build-deploy-cid.sh` guards passes either way —
`size × 6 blocks × 4 slots × 4 TMs` is **1.5 GiB/node at 16M** and **3 GiB/node
at 32M**, both under the script's 8 GiB warn threshold, on a 50 GiB root volume
that also holds Paimon's write-buffer spill. The threshold
affects only the multipart **COPY** path — the write path never reads it — and
is kept equal to the part size for consistency rather than because anything
depends on it.

Upload tuning is applied to **both** jobs, but by different mechanisms, because
the two write paths are different code. On the Paimon side the keys must be
**catalog** options: Paimon writes through its own shaded `S3FileIO`, which
builds `S3AFileSystem` from a bare `new Configuration()` plus only the
`s3./s3a./fs.s3a.`-prefixed catalog options — neither `flinkConfiguration` nor
EMR's `core-site.xml` reaches it, which is why these knobs exist at all.

**The Iceberg side gets the same tuning through completely different keys, and
`fs.s3a.*` would be a silent no-op there.** Its catalog sets
`io-impl = org.apache.iceberg.aws.s3.S3FileIO`, which drives the AWS SDK v2
client directly and never constructs an `S3AFileSystem`, so no `fs.s3a.*` key
from any source reaches it. `build-deploy-cid.sh` therefore *derives* two
Iceberg-shaped values from the same two knobs and substitutes them into
`sql-scripts/iceberg/02-catalog-setup.sql`:

| Derived var | From | Iceberg catalog key |
|---|---|---|
| `ICEBERG_S3_MULTIPART_SIZE_BYTES` | `S3A_MULTIPART_SIZE` × 1024² | `s3.multipart.part-size-bytes` (**bytes**, not a size string: `16777216` at the script default, `33554432` under `.bench-env`) |
| `ICEBERG_S3_MULTIPART_THREADS` | `S3A_ACTIVE_BLOCKS` × 4 slots = 24 | `s3.multipart.num-threads` |

Neither is settable directly — set the `S3A_*` knobs and both sides move
together. The thread count is a derivation, not a copy: S3A caps queued blocks
*per output stream* (per-node total = blocks × slots × TMs), while S3FileIO has
no per-stream cap and uses one shared pool per TM JVM, so 24 threads puts both
at the same per-node staging budget — 24 × 16 MiB × 4 TMs = 1.5 GiB at the
script default, 3 GiB under `.bench-env`'s 32M.
**`s3.multipart.threshold` is deliberately left unset** — in Iceberg it is a
dimensionless *factor* of the part size (default 1.5), not a byte count, so
passing `33554432` would read as a 33-million-fold multiplier and disable
multipart upload outright. In both cases this is a parity fix — uploading blocks
as they fill rather than one PUT at `close()` — not an advantage handed to
either format.

---

## Layout

```
throughput-benchmark/
├── .bench-env                       # sourceable env (region, knobs; account placeholders)
├── .bench-env.local                 # git-ignored: your account ID, bucket, role ARN
├── flink-cid-executor.py            # generic SQL executor (copy of cdc-pipeline's)
├── flink-cid-paimon.yaml            # FlinkDeployment — Paimon sink
├── flink-cid-iceberg.yaml           # FlinkDeployment — Iceberg sink
├── flink-cid-*-deployed.yaml        # rendered output of render/deploy — GENERATED
│                                    #   and not committed. Absent on a fresh
│                                    #   clone; `render` or `deploy` writes it.
├── sql-scripts/                     # uploaded to s3://.../flink/cid-sql/<fmt>/ by
│   │                                #   every render/deploy (sync --delete, BOTH
│   │                                #   formats); the jobs read S3, not this dir.
│   ├── common/01-raw-s3sqs.sql      # ADD JAR + SQS-notified s3json_raw(line STRING)
│   ├── common/01-parse-s3json.sql   # json_tuple parse -> datagen_source, 34 cols.
│   │                                #   raw + parse = both formats' 00-source.sql,
│   │                                #   which runs before the USE CATALOG.
│   ├── paimon/                      # catalog + append sink + pipeline
│   └── iceberg/                     # catalog + v2 append sink + pipeline
├── monitoring/                      # everything that measures the writers
│   ├── collect-metrics.sh           #   streaming samples -> bench-results/cid-*.csv
│   ├── deploy-cid-monitor.sh        #   start (no args) / stop: the monitor Deployment + local collectors
│   ├── entrypoint.py                #   monitor main loop (continuous)
│   ├── flink_cid_monitor.py         #   monitor code (forked from cdc-pipeline)
│   └── monitor-deployment.yaml      #   envsubst template; -deployed.yaml is generated
├── build-deploy-cid.sh              # upload / render / deploy / notify / cleanup (no build)
├── querybench/                      # READ half: StarRocks + DuckDB query benchmark
│   ├── lib.sh                       #   shared: env, windows, render, timeout,
│   │                                #   median, CSV header, writer stop, FE-idle check
│   ├── starrocks.sh                 #   3 arms, timed from the FE AUDIT LOG
│   ├── duckdb-warm.sh               #   warm DuckDB: up / status / run / clear-cache / pin / down
│   ├── duckdb_warm_server.py        #   the server: one DuckDB process, cache_httpfs
│   ├── duckdb-warm.yaml             #   envsubst Deployment (m5.8xlarge, 18 cpu / 72Gi)
│   ├── gp3-fast-sc.yaml             #   StorageClass for DuckDB's cache disk (1000 MB/s)
│   ├── hot-interleave.sh            #   hot suite: pinned versions, engines interleaved per pass
│   ├── hot-watch.sh                 #   after the hot test: stop generator, writers, monitor, duckdb-warm-hot
│   ├── qb-runner.sh                 #   start / logs / fetch: a step in an in-cluster Job (laptop-independent)
│   ├── qb-runner.yaml               #   its Job + ServiceAccount/Role template (envsubst)
│   ├── compare.py                   #   StarRocks vs DuckDB markdown report from two CSVs
│   ├── hot/H1..H6*.sql              #   the queries. Filename = name = order.
│   └── snapshot/S1..S4*.sql
├── jsongz-gen/                      # RAW landing zone: gzipped NDJSON on S3, no Flink
│   ├── cid_jsongz_gen.py            #   the generator (multiprocess, boto3 PUTs)
│   ├── jsongz-gen-job.yaml          #   Job, one pod per node (60 cpu -> 16xlarge)
│   ├── run-jsongz-gen.sh            #   start / scale / status / logs / stop / render
│   └── jsongz-gen-job-deployed.yaml #   rendered manifest — GENERATED, gitignored
└── bench-results/
```

`querybench/` holds both engines' harnesses and the one copy of the queries
they share. Both read the `.sql` files locally: StarRocks renders and pipes them
to the FE, and DuckDB ships them, rendered, in a ConfigMap. There is no S3
publish step. Editing a query is the whole deploy.

Each format gets its own Glue database (`flink_cid_paimon_db` /
`flink_cid_iceberg_db`), S3 warehouse prefix (`cid-warehouse-paimon/` /
`cid-warehouse-iceberg/`) and Flink state prefix (`flink/cid-paimon/` /
`flink/cid-iceberg/`); SQL goes to `flink/cid-sql/<fmt>/`. The CDC pipeline's
databases are never touched. The manifests differ only in format-specific tokens
plus the Paimon job's `aws.glue.enabled: "true"` (required by its
Glue-via-Hive catalog), an explicit `unaligned.enabled: "false"` on Iceberg, and
each side's own S3 upload tuning, which cannot be shared because the two sinks
read it from different places.

Each format is **one** deployment. Paimon compacts inline in the writer and
Iceberg's sink never rewrites data files, so neither side runs a companion job —
the `paimon-compact` target, the `iceberg-compaction/` Spark harness, burst mode
and the jsongen dataset generator were all removed 2026-10-03.

The DuckDB harness here is **separate from, and not a duplicate of,**
`../cdc-pipeline/duckdb/`: that one runs the CDC pipeline's `Q1_count`…
`Q6_multi_join` suite against `flink_iceberg_db`, this one runs the throughput
project's `H*`/`S*` suite against `cid_events`. Same sizing rationale, different
tables and different queries.

---

## Known asymmetries

Inherent to the formats or the surrounding infrastructure — they cannot be
equalized without breaking the setup, so cite them alongside results.

- **Compaction ownership is the central asymmetry.** Paimon's compaction is *in
  the engine*, inline in the writer, running continuously as files arrive.
  Iceberg's Flink sink **never rewrites data files at all**
  (`commit.manifest-merge.enabled` merges *manifests* only), so compaction would
  require an external engine calling `rewrite_data_files`, and none ships here —
  its file count only grows for the length of the run. **Paimon pays coalescing
  CPU inside the measured write path and Iceberg pays none**, which penalises
  Paimon on rows/s and penalises Iceberg on everything measured afterwards.
  **Compare mean data-file size alongside rows/s**, sample file count over time
  on both sides, and do not benchmark reads against these tables without
  compacting them first by some other means.
- **Commit path**: every Iceberg commit does S3 writes **plus a Glue
  `UpdateTable`** round-trip (its catalog pointer lives in Glue); Paimon commits
  are pure S3 (Glue is touched only at DDL time). A consequence of the catalog
  architectures, not a config choice.
- **Snapshot expiry enforcement**: Paimon expires snapshots automatically in the
  commit path (async); Iceberg's `history.expire.*` properties only configure
  the `ExpireSnapshots` maintenance action, which nothing runs — so its snapshot
  log grows one entry per commit for the whole run. Short runs favour Iceberg
  (no expiry work), long runs punish it (planning over a growing log). Quote
  snapshot counts with the results.
- **Upload shape**: Paimon writes through its own shaded `S3FileIO`; Iceberg
  uses its own `io-impl`. Both jobs are tuned toward the same shape — fewer,
  larger multipart uploads per data file — but the configuration surface, key
  names and units differ by write path, hence the Paimon-specific `S3A_*` knobs
  above and the Iceberg job's own equivalents. See each manifest for the
  mechanism actually in force on that side.
- **Spill**: Paimon's write buffers can spill to pod ephemeral disk under
  pressure; Iceberg's writer never uses local disk.
- **Barrier alignment is asymmetrically unavoidable**: Iceberg *could* use
  unaligned checkpoints and Paimon cannot. Equalized downward (both `false`), so
  if Iceberg's alignment time ever climbs it is stuck paying it to keep the
  comparison fair.
- **StarRocks catalog type**: Paimon is read via a `filesystem` catalog
  (metadata straight from S3), Iceberg via `glue` (a Glue `GetTable` per plan
  plus different FE cache behaviour). Normal deployment for each; warmup
  amortizes most of it, but keep per-run times, not just medians.

---

## Caveats

- **Scale trial first.** 8 GiB/s ≈ 40 TMs × (4 cpu / 16 Gi) at parallelism 160,
  writing Parquet to S3 at a rate that stores **multiple TiB per hour per
  format**. Do a 10-minute run before any long one, and clean the
  `cid-warehouse-*` prefixes after. The generator's rate is an offered rate,
  not a guarantee: if a writer cannot absorb it, the source backpressures and
  the achieved rate is what the run reports.
- **Karpenter consolidation is a real hazard here, and the mitigation is not yet
  in these manifests.** `executor-memorynodepool` consolidates with
  `WhenEmptyOrUnderutilized`, `consolidateAfter: 2m` and a 25%-of-nodes budget,
  and both of this benchmark's shapes look underutilized to it: a *starting*
  TaskManager is idle by definition, and a writer *waiting on S3* looks idle by
  exactly the metric consolidation uses. Each eviction discards the in-flight
  write buffer and forces a restart from the last checkpoint — up to a minute of
  re-done work at a 60 s interval — so the job stays live, healthy-looking and
  well below its achievable rate. *A restart-tolerant job can still be a starved
  one.* `karpenter.sh/do-not-disrupt: "true"` is set on both FlinkDeployments'
  TaskManager pod templates (`flink-cid-{paimon,iceberg}.yaml`, since
  2026-10-18), as well as on `querybench/duckdb-warm.yaml` and `qb-runner`'s Job.
  - **Without it,** the generator's pods finishing left the TM nodes
    underutilized, and Karpenter evicted the TMs (`Evicted pod: Underutilized`),
    a full job restart each time.
  - **The fix belongs in these manifests,** never in the nodepool: that spec is
    shared with other workloads.
- **Don't run alongside the CDC benchmark jobs** — same nodepools, and the
  Kyverno single-AZ policy plus Karpenter `limits.cpu` caps mean they starve
  each other.
- **4096 B is a calibrated mean, not a per-row constant.** It is the sum of the
  34 columns' unescaped bytes (what the Parquet writer is handed), of which
  `event_json` is ~82% (~3,345 B). Uncompressed on-wire JSON is larger again
  (escaping, key names); stored Parquet is smaller. Don't mix the three when
  sizing a run.
- **Append-only means no delete artifacts, by construction.** If a run produces
  Iceberg positional-delete files or Paimon deletion-vector indexes, the premise
  is broken. Both counts must read 0: check them with the commands in
  [Secondary comparisons](#secondary-comparisons).
- **Five schema mismatches** between `schema_sample.sql` and the raw extract
  (absent columns, scalar-vs-array fields, `eventSubId`/`eventTime` typing,
  derived columns) are bridged by the generator and documented at the bottom of
  `sql-scripts/paimon/03-sink.sql`. Read them before comparing this table
  against the source files.
- `upgradeMode: stateless` — a restart starts clean rather than resuming old
  state; a scoring run should also start with empty tables.
