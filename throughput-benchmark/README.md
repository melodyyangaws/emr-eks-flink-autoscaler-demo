# Flink Throughput Benchmark — Paimon vs Iceberg

This project compares the **Flink write performance** of Apache Paimon and Apache Iceberg on EMR on EKS. It also provides **hot and snapshot query performance** comparisons for the Paimon and Iceberg tables produced by Flink, using two query engines: **StarRocks (4.1.4)** and **DuckDB (1.5.6)**

- Image: `emr-flink-cdc-paimon` (EMR 7.13, Paimon 1.3.2, Iceberg 1.10.0-amzn-1),
  shared with the CDC pipeline. No image build here: build it once with
  `cd ../cdc-pipeline && ./build-deploy-generic.sh build`.
- Data: gzipped JSON files (~4 KiB rows) written to S3 by a data generator, and
  announced the incremental s3 files to each job through S3 → SNS → SQS.
- Both tables are **append-only**, partitioned by `dt`: no primary key, no
  upsert, no delete files.
- Results: [`bench-results/BENCH-SUMMARY-<session>.md`](./bench-results/); the raw data is in S3 `s3://$S3BUCKET/querybench-results/`.

---

## Prerequisites

| # | What | Check |
|---|---|---|
| 1 | **Image** `emr-flink-cdc-paimon:<EMR_VERSION>` containing Apache Paimon in ECR, built once from the CDC pipeline: `cd ../cdc-pipeline && ./build-deploy-generic.sh build` | `aws ecr describe-images --repository-name emr-flink-cdc-paimon --image-ids imageTag=7.13.0` |
| 2 | **`.bench-env.local`** with the account values: region, account ID, bucket, EMR execution role ARN. See [Settings](#settings-bench-env-and-bench-envlocal).| `ls .bench-env.local` |
| 3 | **`.bench-env`** edited for the test: a new `TEST_SUFFIX` per session ("-final" as default), and other variables if needed. See [Settings](#settings-bench-env-and-bench-envlocal). | `./bench.sh status` prints the session values |
| 4 | **EKS cluster** with a namespace for EMR on EKS (eg.`emr-flink`) and the EMR on EKS Flink operator installed in it, with an EMR execution IAM role for service accounts | `kubectl get pods -n emr-flink`, `kubectl describe sa -n emr-flink`, `kubectl get crd flinkdeployments.flink.apache.org` |
| 5 | **Karpenter nodepools** `driver-nodepool` (JobManagers) and `executor-memorynodepool` (TaskManagers), defined in `.bench-env`  | `kubectl get nodepool` |
| 6 | **EBS CSI driver** installed, for the gp3 disks of DuckDB and the StarRocks BEs | `kubectl get csidriver ebs.csi.aws.com` |


---

## Easiest start: ask a GenAI MCP client tool

Open a GenAI tool such as Kiro or Claude Code, navigate to the Github project's folder `throughput-benchmark/`

```bash
git clone git@github.com:melodyyangaws/emr-eks-flink-autoscaler-demo.git
cd emr-eks-flink-autoscaler-demo/throughput-benchmark
# start the GenAI tool
claude
```
- Ask to start the test from end to end (write throughput + Query performance for Paimon & Iceberg):
> follow readme to run a full test

- Only benchmark writer's throughput
> test Flink throughput to write Iceberg vs Paimon tables, monitor and summarize the result

- Hot query performance against Iceberg vs Paimon
> run hot query test

- Check the test progress at any time:
> what's the current progress?

The tool reads this README and runs every step below in order. It checks
capacity and what is still running, and asks you before anything that changes
shared state: a new `TEST_SUFFIX`, or purging leftover SQS messages. Create
`.bench-env.local` first (see [Settings](#settings-bench-env-and-bench-envlocal)).

| # | Step | Command | What it does | Time (measured) |
|---|---|---|---|---|
| 0 | Pre-checks | `./bench.sh precheck` | the S3 → SNS → SQS pipeline: set up if any piece is missing (`setup-sqs`), else left as is with its queue backlog shown; then capacity, leftover pods, a fresh `TEST_SUFFIX`, purge queues | ~2 min (2 purges, 60 s each; run 2026-10-24) |
| 1 | Throughput | `./bench.sh throughput` | deploys both Flink writers, waits for RUNNING, starts the source data generator and monitoring | ~10 min (2026-10-24: writers created 16:57Z, both RUNNING 17:04Z, generator and monitoring up 17:05Z) |
| 2 | Fill the tables | — | the writers commit while the tables grow, before the hot test; it can start once the query engines are up (step 2b) | ~10 min (2026-10-24: 17:05–17:15Z) |
| 2b | Startup query engines | `kubectl apply -f engine-deploy/gp3-fast-sc.yaml`, then `./engine-deploy/deploy-starrocks.sh install`, `./engine-deploy/deploy-duckdb-warm.sh up` | only if StarRocks or DuckDB long-runner server is missing: the fast gp3 disk classes first, then the engines (see [Query engines](#query-engines-before-a-query-test)); | ~8–10 min, alongside step 2 (StarRocks install ~4 min once nodes exist; DuckDB ~5 min on a new node) |
| 3 | Hot query test | `./bench.sh hot` | DuckDB up, empties caches, then 1 warm-up + 3 timed passes/runs while the writers run | ~4 min setup (cache clear ~3.5 min) + **~1.2 h** test (2026-10-24: runner 74 min; the watcher then needs ≤ 10 min to shut down) |
| 4 | Snapshot query test | `./bench.sh snapshot` | waits for the hot test complete, stops the writers, compacts both tables once (`COMPACT_BEFORE_SNAPSHOT=true`, [Compaction](#compaction-before-the-snapshot-test)), empties caches, runs StarRocks (3 arms) then DuckDB (2 arms) | **~5.9 h** (run 2026-10-25, `-final`): ~3 min stopping the writers; compaction 37 min (Paimon 2 passes, Iceberg 4, in parallel); StarRocks install 4 min; DuckDB up and both caches cleared 7 min; runner 5.0 h = StarRocks 2.4 h (`paimon` 36 / `pmn_as_iceberg` 35 / `iceberg` 73 min) then DuckDB 2.6 h (each arm 1 warm-up + 3 timed passes, then its untimed scan pass with the cache bypassed). Before (uncompacted): ~16.5 h |
| 5 | Summary | `./bench.sh summary` | pulls the results from S3 into `bench-results/BENCH-SUMMARY-<session>.md` | ~3 min (2026-10-25: 06:49–06:52Z) |

A full session takes about 7.6 h of running time (`-final`, 2026-10-24/25), most of it the snapshot test (about 19 h before the 2026-10-23 windows). Steps 3 and 4
run as Jobs in the cluster, so the laptop and the tool can be closed while they run.

---

## Quick start

```bash
cd throughput-benchmark
# once: create .bench-env.local (below)
./bench.sh precheck     # SQS pipeline: set up if missing, else its backlog (purge if it is an earlier run's)
# each session: set TEST_SUFFIX in .bench-env (a new table set, e.g. "-test4")
# see the following step by step section for more details
PAIMON_WRITE_ONLY=true PAIMON_WRITE_BUFFER_SPILLABLE=false \
./bench.sh throughput   # writers -> both RUNNING -> source data generator -> monitoring
kubectl apply -f engine-deploy/gp3-fast-sc.yaml   # before the engines: the fast gp3 disk classes
./engine-deploy/deploy-starrocks.sh install   # only if StarRocks is missing (status shows it)
./engine-deploy/deploy-duckdb-warm.sh up      # only if DuckDB is missing; the tests empty both caches themselves
./bench.sh hot          # hot query test while the writers run
./bench.sh snapshot     # snapshot query test (stops the writers, compacts both tables once, then queries)
./bench.sh summary      # -> bench-results/BENCH-SUMMARY-<session>.md
./bench.sh status       # what is running
./bench.sh cleanup      # stop everything; deletes no data
```

The Paimon configuration (`PAIMON_WRITE_*`) is for the append-only use case to have similar throughput performance to Iceberg write, so it goes on the `throughput` command, not in `.bench-env`.

### Components

Each folder has one `run.sh <command>`; `bench.sh` only calls them in order.

| Component | Entry point | Commands |
|---|---|---|
| Data generator | `./datagen/run.sh` | `start`, `stop`, `status`, `logs`, `scale N`, `render` |
| Flink writers | `./flinkwriter/run.sh` | `upload`, `render`, `deploy <paimon\|iceberg\|both>`, `notify`, `cleanup [fmt\|both]` |
| Monitoring | `./monitoring/run.sh` | `start`, `stop` |
| Compaction | `./compaction/run.sh` | `run [paimon\|iceberg\|both]`, `status`, `logs <fmt>`, `cleanup` |
| Query engines | `./engine-deploy/deploy-starrocks.sh` | `install`, `migrate-be-disks`, `status`, `connection`, `down`, `uninstall` |
| | `./engine-deploy/deploy-duckdb-warm.sh` | `up`, `status`, `clear-cache`, `down` |
| Query tests | `./querybench/run.sh` | `start` (with `SUITE=hot\|snapshot`), `logs [watch]` |
| Summary | `./summary/run.sh` | none |

### Settings: `.bench-env` and `.bench-env.local`

Every script loads `.bench-env` itself (through `lib/common.sh`). A value on
the command line wins for that run, and the script prints a `[bench] X=...
overrides .bench-env` line.

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

What `.bench-env` sets:

| Variable | `.bench-env` | Default | Note |
|---|---|---|---|
| `TEST_SUFFIX` | `-final` | empty | names the table set; keep it for the whole session |
| `JOB_PARALLELISM` | `320` | `128` | writer subtasks per job (80 TMs) |
| `ICEBERG_COMPAT` | `true` | `false` | Paimon also exports Iceberg metadata, so StarRocks/DuckDB can read it as Iceberg |
| `JSONGZ_REPLICAS` | `7` | required | generator pods, 500K rows/s each at most |
| `DURATION_SECONDS` | `0` | `1800` | generator run length; `0` means long running until stopped manually |
| `OUTPUT_URI` | `s3://$BUCKET_NAME/cid-landing-jsongz` | — | the only prefix S3 announces to SQS |
| `S3A_MULTIPART_SIZE` / `_THRESHOLD` | `32M` | `16M` | Paimon's S3 upload part size |
| `COMPACT_BEFORE_SNAPSHOT` | `true` | `false` | the snapshot test compacts both tables once first ([Compaction](#compaction-before-the-snapshot-test)) |
| `EMR_VERSION` | `7.13.0` | `7.13.0` | the writer and compaction image tag (`IMAGE_TAG` defaults to it); must exist in ECR |
| `NAMESPACE` | `emr-flink` | `emr-flink` | the EMR on EKS namespace for every job and pod |
| `JM_NODEPOOL` | `driver-nodepool` | `driver-nodepool` | Karpenter nodepool for the JobManagers |
| `TM_NODEPOOL` | `executor-memorynodepool` | `executor-memorynodepool` | Karpenter nodepool for the TaskManagers |

`TEST_SUFFIX` sets every name of a table set. For example:

| | Name (`TEST_SUFFIX=-test1`) |
|---|---|
| Glue databases | `flink_cid_paimon_db_test1`, `flink_cid_iceberg_db_test1` |
| Warehouses | `s3://$BUCKET_NAME/cid-warehouse-{paimon,iceberg}-test1/` |
| Flink state (checkpoints, HA) | `s3://$BUCKET_NAME/flink/cid-{paimon,iceberg}-test1/` |
| Query results | `s3://$BUCKET_NAME/querybench-results/session-test1/<STAMP>-<suite>/` |
| Compaction results | `s3://$BUCKET_NAME/querybench-results/session-test1/compaction/<STAMP>-<fmt>-pass<N>.json` |

---

## Step by step

`./bench.sh throughput` runs steps 2–5. Run everything from `throughput-benchmark/`.

### 1. Check capacity

One job = 1 JM (4 cpu) + `JOB_PARALLELISM / 4` TMs (4 cpu / 16 GiB each).
At 320 that is **80 TMs ≈ 324 cpu per job, ≈ 648 cpu for both**.

```bash
kubectl get nodepool executor-memorynodepool -o jsonpath='{.spec.limits.cpu}'      # limit
kubectl get nodepool executor-memorynodepool -o jsonpath='{.status.resources.cpu}' # in use
```

Stay under ~80% of the limit, or TMs evict each other and the jobs restart.

**The S3 → SNS → SQS pipeline: `./bench.sh precheck`** (`./flinkwriter/run.sh ensure-sqs`).
It checks the topic, both queues and their DLQs, the subscriptions, the bucket's
notification and the job role's read access to the queues. If all are there, it changes nothing and shows each queue's backlog.
If any is missing (a new account or region), it runs `setup-sqs`.
`bench.sh throughput` does not check it; `deploy` stops with "Cannot read SQS queue"
if a queue is missing.

```bash
./bench.sh precheck              # set up only what is missing; else the backlog
./flinkwriter/run.sh setup-sqs   # force the setup; writers stopped; safe to rerun
```

`setup-sqs` creates whatever is missing:
- the SNS topic `cid-landing-jsongz-events`, which only `$BUCKET_NAME` may publish to;
- per format, the queue `cid-landing-jsongz-<fmt>` (visibility 900 s, 14-day retention,
  SQS-managed encryption, writable by the topic only), with a `-dlq` after 10 receives;
- the subscriptions, with raw message delivery;
- on the role in `EMR_EXECUTION_ROLE_ARN`, the inline policy `FlinkLandingQueueConsume`
  (`SQS_ROLE_POLICY_NAME`): `ReceiveMessage`, `DeleteMessage`, `GetQueueAttributes`,
  `ChangeMessageVisibility` and `GetQueueUrl` on both queues. It is rewritten only if it
  does not already allow them, e.g. after changing `SQS_QUEUE_PREFIX`.

Existing queues keep their attributes; only their access policy and redrive are set.
It then runs `notify`, which sets the bucket's S3 event notification and checks a
test file reaches both queues. That step is skipped, with a warning, while a writer
is RUNNING.

### 2. Upload (done by `deploy`)

`deploy` and `render` upload everything first and check each object exists
and is not empty. `upload` runs only this step.

| S3 key | From |
|---|---|
| `flink/scripts/flink-cid-executor.py` | `flinkwriter/flink-cid-executor.py` |
| `flink/cid-sql/<fmt>/00-source.sql` | `sql-scripts/common/01-raw-s3sqs.sql` + `01-parse-s3json.sql`, joined; the same for both formats |
| `flink/cid-sql/<fmt>/02..04-*.sql` | `flinkwriter/sql-scripts/<fmt>/` |
| `flink/jars/sqs-delta-filename.jar` | `datagen/sqs-delta-filename/`, rebuilt by Maven when its sources change |

### 3. Deploy the writers

```bash
./flinkwriter/run.sh deploy both              # or paimon / iceberg
kubectl get flinkdeployment -n emr-flink -w    # wait for RUNNING
./flinkwriter/run.sh cleanup both             # delete them (data and tables stay)
```

`deploy` deletes and re-creates the FlinkDeployments (EMR stages the job file
only on create). It also prints each SQS queue's backlog.

### 4. Start the generator

Start it only after both writers are `RUNNING`.

```bash
./datagen/run.sh start     # Job with JSONGZ_REPLICAS pods
./datagen/run.sh status    # rows/s and MB/s per pod
./datagen/run.sh scale 8   # change the pod count live
./datagen/run.sh stop      # stop; files already written stay
```

### 5. Monitoring

```bash
./monitoring/run.sh start     # monitor Deployment + local CSV collectors
./monitoring/run.sh stop
kubectl logs -f deployment/flink-cid-monitor -n emr-flink
kubectl port-forward svc/flink-cid-paimon-rest 8081:8081 -n emr-flink   # Flink UI
```

- **Monitor:** a Deployment that reports both jobs side by side.
  Throughput comes from the tables' commit history, or from Flink metrics if
  no commit landed in the window.
- **Collectors:** `collect-metrics.sh`, one per running job (Paimon on port
  8091, Iceberg on 8092). Output: `bench-results/cid-<fmt>-<stamp>.csv`, moved
  to S3 by the summary.
  Knobs: `COLLECT=0` (skip), `COLLECT_INTERVAL` (30 s), `COLLECT_DURATION` (7200 s).

### 6. Query tests

#### Query engines: before a query test

Both tests need StarRocks and the warm DuckDB server. Check them first:

```bash
./engine-deploy/deploy-starrocks.sh status      # FE + 3 BE pods, BE volumes
./engine-deploy/deploy-duckdb-warm.sh status    # duckdb-warm: {...} or "not running"
```

- **An engine is missing:** create the fast gp3 disk classes first, then start
  the engine, then run the test.

  ```bash
  kubectl apply -f engine-deploy/gp3-fast-sc.yaml   # gp3-fast (DuckDB: 16000 IOPS / 850 MB/s)
                                                    # gp3-starrocks-be (each BE: 5333 IOPS / 283 MB/s)
  kubectl get storageclass gp3-fast gp3-starrocks-be \
    -o custom-columns=NAME:.metadata.name,IOPS:.parameters.iops,MBPS:.parameters.throughput
  ./engine-deploy/deploy-starrocks.sh install   # ~5 min: operator, FE, 3 BEs (9 cpu / 38 Gi, m5.4xlarge, gp3-starrocks-be)
  ./engine-deploy/deploy-duckdb-warm.sh up      # ~5-10 min on a new node (27 cpu / 114 Gi, m5.8xlarge, gp3-fast)
  ```

  `run.sh start` starts either engine itself if it is missing (StarRocks when its FE or a BE
  is not Ready). Both
  deploy scripts apply `gp3-fast-sc.yaml` too; creating the classes first shows
  a mismatch before any engine is half-deployed.

  A StorageClass's parameters cannot change. After editing a class in
  `gp3-fast-sc.yaml`, the apply fails with `updates to parameters are
  forbidden`: delete that class and apply again. Volumes already created keep
  their speed, so restart DuckDB (`up`) or re-create the BE volumes
  (`deploy-starrocks.sh uninstall` + `install`) to get the new one.

  ```bash
  kubectl delete storageclass gp3-fast && kubectl apply -f engine-deploy/gp3-fast-sc.yaml
  ```
- **Both are up:** only the caches need emptying, and the test does that itself
  (step 4 below). By hand, without a test:

  ```bash
  ./querybench/clear-caches.sh                  # StarRocks datacache (BE by BE) + DuckDB restart
  ```

Both engines keep the `TEST_SUFFIX` and settings they were started with. After
changing either, restart DuckDB (`up`) and re-run the StarRocks `install`.

```bash
./bench.sh hot                    # = SUITE=hot ./querybench/run.sh start
./bench.sh snapshot               # = SUITE=snapshot ./querybench/run.sh start
./querybench/run.sh logs [watch]  # follow the runner (or the watcher)
```

`querybench/run.sh start` does this, and stops if any step fails:

1. Waits while an earlier test is still running, and then while its watcher is still
   shutting it down (`FORCE=1` skips both waits).
2. Snapshot only: stops the generator and the writers, and picks the query windows.
   Then, with `COMPACT_BEFORE_SNAPSHOT=true`, compacts both tables once (below).
3. Starts StarRocks if its FE or BEs are missing (`engine-deploy/deploy-starrocks.sh install`),
   then the DuckDB server, and checks it is healthy.
4. Empties both engines' caches (`querybench/clear-caches.sh`). The hot test skips this
   when step 3 just started both engines from scratch, since their caches are empty;
   the snapshot test always does it.
5. Starts the runner Job. When the test ends, the results go to S3 under
   `querybench-results/session<TEST_SUFFIX>/`.
6. Starts the watcher Job. When the runner ends, it stops the generator,
   writers, monitor, DuckDB and StarRocks (`deploy-starrocks.sh down`: FE/BE
   pods and volumes; `STOP_STARROCKS=0` keeps it). The next test's step 3
   brings both engines back.

Both Jobs run in the cluster, so closing the laptop does not stop a test.
Queries, arms, knobs and output: [querybench/README.md](querybench/README.md).

#### Compaction before the snapshot test

The writers leave two files per subtask per 60 s checkpoint: one ~256 MiB and
one ~118 MiB tail (77K files, 14.5 TB per table on 10-21). `compaction/run.sh`
merges the tails once, with the same rule on both formats, so the snapshot
test reads tables laid out the way a maintained table would be.

| | Paimon | Iceberg |
|---|---|---|
| Picks | files under 70% of the 256 MiB target (Paimon's own rule) | the same rule, applied in the job |
| Packs | bins of ≥ 2 × target or ≥ 5 files; output rolls at 256 MiB | groups of ≤ 256 MiB, one output file each |
| Runs | `CALL sys.compact` (batch, `minor`) | Iceberg's `RowDataRewriter`, then one `RewriteFiles` commit |
| Passes | until small files are under 1% of all files (`PAIMON_SMALL_PCT`): each `sys.compact` leaves about one new small file per bin | `ICEBERG_MAX_GROUPS` groups each (4000), until none is left |

- Files at or near the target stay as they are, so each file still covers
  about one minute of `eventTimeDT`. That keeps the window pruning as it was.
  It is bin-packing, not a sort.
- Each format is a batch FlinkDeployment, `flink-cid-compact-<fmt>`. Both use
  the writers' image, catalog SQL and nodepools, at `COMPACT_PARALLELISM`
  (default 160: 40 TaskManagers each), and run side by side.
- The snapshot test runs it after the writers stop and the windows are
  picked, and before the caches are emptied. It runs from the laptop, so keep
  the laptop on until `run.sh start` has started the runner Job.
- Iceberg's own Flink `RewriteDataFilesAction` is not used: it splits every
  file at the target and rewrites any file it had to split, which here means
  nearly all of them.
- Paimon's compaction commit expires snapshots. `COMPACT_RETAIN` (7d) keeps
  the writers' APPEND snapshots for that commit, because the throughput
  numbers are computed from them.
- A failed pass commits nothing, and its FlinkDeployment is kept for the
  logs. `./compaction/run.sh cleanup` deletes it.
- Each pass writes a JSON to `compaction/` in the session's S3 prefix: before
  and after files and bytes, and timings. The summary adds a section from it.

```bash
DRY_RUN=true ./compaction/run.sh run both   # measure and plan only; nothing rewritten (JobManagers only)
./compaction/run.sh run both                # compact by hand (writers must be stopped)
./compaction/run.sh status
./compaction/run.sh logs iceberg
COMPACT_BEFORE_SNAPSHOT=false ./bench.sh snapshot   # one snapshot test without it
# rerun DuckDB only on the already-compacted tables (same windows; StarRocks untouched)
ENGINES=duckdb COMPACT_BEFORE_SNAPSHOT=false SUITE=snapshot ./querybench/run.sh start
```

Knobs: `COMPACT_PARALLELISM`, `ICEBERG_MAX_GROUPS`, `PAIMON_SMALL_PCT` (1), `COMPACT_MAX_PASSES` (20),
`COMPACT_TIMEOUT_S` (per pass, 4 h), `COMPACT_RETAIN`, `DRY_RUN`.

### 7. Summary

```bash
./bench.sh summary   # = ./summary/run.sh
```

Writes one findings file, `bench-results/BENCH-SUMMARY-<date><TEST_SUFFIX>.md`:
setup, key findings, throughput, hot and snapshot tables, and like-for-like
comparisons (reader, writer, engine). The findings are derived from the numbers,
including incidents such as one writer stopping its commits early.

- **Inputs, all from S3** (`querybench-results/session<TEST_SUFFIX>/`): the
  newest hot and snapshot runs (`HOT_STAMP=` / `SNAPSHOT_STAMP=` pick others)
  and `throughput/`, plus both tables' commit history.
- **The laptop's collector files** (`cid-<fmt>-*.csv`, `collect-*.log`) are
  uploaded to `throughput/` first, with the commit history.
- **`bench-results/` keeps Markdown only:** every other file there is deleted
  at the end, since it is in S3. Re-running rewrites the summary.
- The bucket and account ID are masked.

### 8. Teardown

Nothing may be writing when you wipe: a live job recommits into a half-deleted
warehouse.

```bash
./bench.sh cleanup                       # generator, writers, monitor, DuckDB, StarRocks, compaction jobs
./flinkwriter/run.sh cleanup both        # delete the FlinkDeployments
kubectl get pods -n emr-flink | grep cid # must print nothing
kubectl get cm -n emr-flink | grep -E 'flink-cid-.*config-map'   # leftover HA state: delete it
```

Then drop the table set. **Irreversible. Check the names twice; the bucket is
shared with the CDC pipeline.**

```bash
source .bench-env; S=$TEST_SUFFIX; D=${S//-/_}
aws glue delete-table --database-name flink_cid_paimon_db$D  --name cid_events --region "$AWS_REGION"
aws glue delete-table --database-name flink_cid_iceberg_db$D --name cid_events --region "$AWS_REGION"
aws s3 rm --recursive "s3://$BUCKET_NAME/cid-warehouse-paimon$S/"
aws s3 rm --recursive "s3://$BUCKET_NAME/cid-warehouse-iceberg$S/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/cid-paimon$S/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/cid-iceberg$S/"
aws s3 rm --recursive "$OUTPUT_URI/"     # landing files
# then purge both SQS queues (below)
```

Drop the tables before changing `PAIMON_BUCKETS` or `ICEBERG_COMPAT`: both
are fixed when the table is created.

---

## Data source

### Generator (`datagen/`)

- Writes `.json.gz` files to `OUTPUT_URI`, shaped like a real security telemetry
  extract (`cid_9011075d_sample_masked`): same fields, value distributions and
  null rates.
- Layout: `year=/month=/day=/hour=/customerId=/productCode=/<traceId>_NNN_new.json.gz`,
  ~2,500 rows per file.
- Rows: ~4,096 B raw, ~0.2 KB gzipped. The size is a mean, not a constant.
- Rate: `JSONGZ_REPLICAS` × `JSONGZ_ROWS_PER_POD` (500K) rows/s at most; 7 pods
  = 3.5M rows/s. It is the only rate setting: the jobs read as fast as they can.
- One pod per 16xlarge node (`WORKERS=60` cpu). Other knobs: `GZIP_LEVEL`,
  `ROWS_PER_FILE`, `CID_CUSTOMER_COUNT`, `CID_HOST_COUNT`, `GEN_NODEPOOL`.

### Reading through SQS

```
s3://$BUCKET_NAME/cid-landing-jsongz/*.json.gz   (ObjectCreated)
  └─> SNS cid-landing-jsongz-events
        ├─> SQS cid-landing-jsongz-paimon    -> flink-cid-paimon
        └─> SQS cid-landing-jsongz-iceberg   -> flink-cid-iceberg
```

- The connector (`datagen/sqs-delta-filename/`) reads the files each message
  names. Nothing lists the bucket.
- Each job has its own queue, so the two never compete for messages.
- **Only new files under `cid-landing-jsongz/` are announced.** The S3
  notification is set on the bucket by `./flinkwriter/run.sh setup-sqs` (once,
  [step 1](#1-check-capacity)) or `notify`. After changing `OUTPUT_URI`, run
  `./flinkwriter/run.sh notify` (writers stopped).
- **The queue is the state.** Leftover messages are read first by the next
  run. `deploy` warns about a backlog but never purges. Purge with everything
  stopped:

  ```bash
  for f in paimon iceberg; do
    aws sqs purge-queue --region "$AWS_REGION" \
      --queue-url "https://sqs.$AWS_REGION.amazonaws.com/$AWS_ACCOUNT_ID/cid-landing-jsongz-$f"
  done   # one purge per queue per 60 s
  ```

- An `AccessDenied` from the source means the job role lacks the
  `FlinkLandingQueueConsume` policy: `./bench.sh precheck` adds it.

| Knob | Default | Meaning |
|---|---|---|
| `SQS_QUEUE_PREFIX` | `cid-landing-jsongz` | queue = `<prefix>-<fmt>` |
| `SQS_ROLE_POLICY_NAME` | `FlinkLandingQueueConsume` | the job role's inline policy on the queues |
| `SQS_POLL_THREADS` | `8` | parallel `ReceiveMessage` loops |
| `SQS_MAX_PENDING` | connector default | cap on received, unassigned files |
| `SQS_VISIBILITY_TIMEOUT` | `15min` | must outlast a checkpoint, or a file is read twice |

---

## Fairness

### The same on both sides

| | Paimon | Iceberg |
|---|---|---|
| Layout | unaware-bucket (`bucket=-1`), no shuffle | `write.distribution-mode=none` |
| Compression | `snappy` | `snappy` |
| Row group | 32 MiB | 32 MiB |
| Target file | 256 MiB | 256 MiB |
| Snapshot retention | 3 h, min 5 (max 240) | 3 h, min 5 |
| Manifests | 8 MiB, merge at 5 | 8 MiB, merge at 5 |
| Column stats | `truncate(16)` | `truncate(16)` |
| Commit | checkpoint every 60 s, aligned | same (`unaligned.enabled=false`) |
| Object reuse | `pipeline.object-reuse=true` | same |
| Columns | the same 34, same order, `PARTITIONED BY (dt)` | same |

Change one side, change the other.

### Differences that remain

- **Compaction:** Paimon compacts inside the writer (unless
  `PAIMON_WRITE_ONLY=true`). Iceberg's Flink sink never compacts. Before the
  snapshot test, `compaction/run.sh` compacts both with one rule (see
  [Compaction](#compaction-before-the-snapshot-test)); the hot test always
  reads them as written. Compare file
  count and size alongside rows/s.
- **Commit:** each Iceberg commit also updates Glue. Paimon commits are S3 only.
- **Snapshot expiry:** Paimon expires snapshots as it commits. For Iceberg the
  retention properties are set, but nothing runs the expiry, so its snapshot
  list keeps growing.
- **Iceberg export (`ICEBERG_COMPAT=true`):** Paimon also writes Iceberg
  metadata on every commit, work the Iceberg job does not do. Its write numbers
  are not comparable with `ICEBERG_COMPAT=false` runs.
- **S3 upload:** the same part size and threads, set through different keys.
  Paimon uses `S3A_*` catalog options; Iceberg gets
  `ICEBERG_S3_MULTIPART_SIZE_BYTES` (= part size in bytes) and
  `ICEBERG_S3_MULTIPART_THREADS` (= `S3A_ACTIVE_BLOCKS` × 4), derived by
  `flinkwriter/run.sh`.
- **Spill:** Paimon's write buffer can spill to local disk; Iceberg's writer
  never does.
- **StarRocks catalogs:** Paimon through a `filesystem` catalog, Iceberg
  through `glue`, Paimon's Iceberg export through `hadoop`.

---

## Writer knobs

| Knob | Default | Meaning |
|---|---|---|
| `JOB_PARALLELISM` | `128` (`.bench-env`: 320) | writer subtasks; 4 per TM |
| `PAIMON_BUCKETS` | `-1` | `-1` = no buckets. `N` = fixed buckets on `uuid` (use `N = JOB_PARALLELISM`). Fixed at table creation |
| `PAIMON_WRITE_ONLY` | `false` | `true` = no compaction in the writer. Per run, no table drop |
| `PAIMON_WRITE_BUFFER_SPILLABLE` | `true` | `false` = flush a full buffer instead of spilling. Per run |
| `WRITE_BUFFER` | `128mb` | Paimon `write-buffer-size`. Per run |
| `ICEBERG_COMPAT` | `false` (`.bench-env`: true) | Paimon's Iceberg export. Fixed at table creation |
| `S3A_MULTIPART_SIZE` / `_THRESHOLD` | `16M` (`.bench-env`: 32M) | upload part size; whole `M` only |
| `S3A_ACTIVE_BLOCKS` | `6` | blocks queued per upload stream |
| `JM_NODEPOOL` / `TM_NODEPOOL` | `driver-nodepool` / `executor-memorynodepool` | Karpenter nodepools |
| `EMR_VERSION` / `IMAGE_TAG` | `7.13.0` / `$EMR_VERSION` | the tag must exist in ECR |

---

## Reading the results

- **Throughput** = rows each commit added, divided by the time between commits
  (from the tables' history; `summary/run.sh` and the monitor both use it).
  Compare over the span where both jobs were committing.
- **Never quote rows/s alone.** Pair it with file count, mean file size,
  stored bytes and snapshot count.
- **Delete files must be 0** on both sides. Anything else means append-only was
  broken and the run is void.
- **Stalled job?** Check the TMs (`kubectl top pod`): busy TMs = a real limit,
  idle TMs = a stall (e.g. a long checkpoint flush).

Check a table by hand (with `S` and `D` set as in Teardown):

```bash
# Paimon: commit kinds (COMPACT only when PAIMON_WRITE_ONLY=false)
P=s3://$BUCKET_NAME/cid-warehouse-paimon$S/flink_cid_paimon_db$D.db/cid_events
aws s3 cp "$P/snapshot/" - --recursive | grep -o '"commitKind":"[A-Z]*"' | sort | uniq -c

# Iceberg: files, delete files, rows in the latest snapshot
I=s3://$BUCKET_NAME/cid-warehouse-iceberg$S/flink_cid_iceberg_db$D.db/cid_events/metadata
M=$(aws s3 ls "$I/" | grep '\.metadata\.json' | sort | tail -1 | awk '{print $4}')
aws s3 cp "$I/$M" - | jq '.snapshots[-1].summary | {"total-data-files","total-delete-files","total-records"}'
```

---

## Layout

```
throughput-benchmark/
├── bench.sh                 # a session: throughput | hot | snapshot | summary | status | cleanup
├── .bench-env               # session values and knobs (placeholders for account values)
├── .bench-env.local         # git-ignored: account ID, bucket, role ARN
├── lib/
│   ├── common.sh            # loads .bench-env; logging; render() template -> *-deployed.yaml
│   └── querybench.sh        # shared query-test code: windows, SQL render, median, writer stop
├── datagen/
│   ├── run.sh
│   ├── jsongz-gen/          # the generator (cid_jsongz_gen.py) and its Job template
│   └── sqs-delta-filename/  # the Flink SQS file source (Maven)
├── flinkwriter/
│   ├── run.sh
│   ├── flink-cid-executor.py        # runs a format's SQL from S3 as one streaming job
│   ├── flink-cid-{paimon,iceberg}.yaml
│   └── sql-scripts/{common,paimon,iceberg}/
├── monitoring/
│   ├── run.sh
│   ├── collect-metrics.sh           # REST samples -> bench-results/cid-*.csv
│   ├── entrypoint.py, flink_cid_monitor.py, monitor-deployment.yaml
├── compaction/
│   ├── run.sh
│   ├── flink-cid-compact.py         # one-off bin-pack of one format's table (batch)
│   └── flink-cid-compact.yaml       # its FlinkDeployment template
├── querybench/              # StarRocks + DuckDB query tests: see querybench/README.md
├── engine-deploy/           # the two query engines' deployments
│   ├── deploy-starrocks.sh          # StarRocks (Helm), BE disks gp3-starrocks-be: 3 x 283 = ~850 MB/s
│   ├── deploy-duckdb-warm.sh        # the warm DuckDB server: up | status | clear-cache | down
│   ├── duckdb-warm.yaml, duckdb_warm_server.py
│   └── gp3-fast-sc.yaml             # the disk classes: gp3-fast (DuckDB, 850 MB/s), gp3-starrocks-be (283 MB/s x 3)
├── summary/
│   ├── run.sh
│   └── summarize.py                 # -> bench-results/BENCH-SUMMARY-<session>.md
└── bench-results/           # BENCH-SUMMARY-<session>.md only; the data is in S3
```

Rendered manifests (`*-deployed.yaml`) hold the bucket and account ID and are
git-ignored.

---

## Caveats

- **Try a short run first.** Each job is 80 TMs and stores several TiB per hour.
- **Don't run alongside the CDC benchmark:** same nodepools, and they starve each other.
- **Karpenter:** TMs, DuckDB and the query Jobs carry `karpenter.sh/do-not-disrupt`.
  Without it, Karpenter evicted idle-looking TMs and the jobs restarted. Set it in
  these manifests, never on the shared nodepool.
- **`upgradeMode: stateless`:** a restart starts clean. Score runs on fresh tables.
- **Schema:** five differences between `schema_sample.sql` and the raw extract
  are bridged by the generator; see the end of `flinkwriter/sql-scripts/paimon/03-sink.sql`.
