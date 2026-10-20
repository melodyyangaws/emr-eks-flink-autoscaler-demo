# Flink Throughput Benchmark — Paimon vs Iceberg

Compares the **Flink write path** of Apache Paimon and Apache Iceberg on EMR on
EKS, then **query speed** on the tables they wrote (StarRocks vs DuckDB).

- Image: `emr-flink-cdc-paimon` (EMR 7.13, Paimon 1.3.2, Iceberg 1.10.0-amzn-1),
  shared with the CDC pipeline. No image build here: build it once with
  `cd ../cdc-pipeline && ./build-deploy-generic.sh build`.
- Data: gzipped NDJSON files (~4 KiB rows) written to S3 by a generator, and
  announced to each job through S3 → SNS → SQS.
- Both tables are **append-only**, partitioned by `dt`: no primary key, no
  upsert, no delete files.
- Results: `bench-results/` (latest: `BENCH-SUMMARY-20261019-r3.md`).

---

## Quick start

```bash
cd throughput-benchmark
# once: create .bench-env.local (below)
# each session: set TEST_SUFFIX in .bench-env (a new table set, e.g. "-test4")
# see the following step by step section for more details
PAIMON_WRITE_ONLY=true PAIMON_WRITE_BUFFER_SPILLABLE=false \
./bench.sh throughput   # writers -> both RUNNING -> generator -> monitoring
./bench.sh hot          # hot query test while the writers run
./bench.sh snapshot     # snapshot query test (stops the writers first)
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

| Variable | `.bench-env` | Script default | Note |
|---|---|---|---|
| `TEST_SUFFIX` | `-final` | empty | names the table set; keep it for the whole session |
| `JOB_PARALLELISM` | `320` | `128` | writer subtasks per job (80 TMs) |
| `ICEBERG_COMPAT` | `true` | `false` | Paimon also exports Iceberg metadata, so StarRocks/DuckDB can read it as Iceberg |
| `JSONGZ_REPLICAS` | `7` | required | generator pods, 500K rows/s each at most |
| `DURATION_SECONDS` | `0` | `1800` | generator run length; `0` means long running until stopped manually |
| `OUTPUT_URI` | `s3://$BUCKET_NAME/cid-landing-jsongz` | — | the only prefix S3 announces to SQS |
| `S3A_MULTIPART_SIZE` / `_THRESHOLD` | `32M` | `16M` | Paimon's S3 upload part size |

`TEST_SUFFIX` sets every name of a table set:

| | Name (`TEST_SUFFIX=-test4`) |
|---|---|
| Glue databases | `flink_cid_paimon_db_test4`, `flink_cid_iceberg_db_test4` |
| Warehouses | `s3://$BUCKET_NAME/cid-warehouse-{paimon,iceberg}-test4/` |
| Flink state (checkpoints, HA) | `s3://$BUCKET_NAME/flink/cid-{paimon,iceberg}-test4/` |
| Query results | `s3://$BUCKET_NAME/querybench-results/session-test4/<STAMP>-<suite>/` |

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
  8091, Iceberg on 8092). Output: `bench-results/cid-<fmt>-<stamp>.csv`.
  Knobs: `COLLECT=0` (skip), `COLLECT_INTERVAL` (30 s), `COLLECT_DURATION` (7200 s).

### 6. Query tests

```bash
./bench.sh hot                    # = SUITE=hot ./querybench/run.sh start
./bench.sh snapshot               # = SUITE=snapshot ./querybench/run.sh start
./querybench/run.sh logs [watch]  # follow the runner (or the watcher)
```

`querybench/run.sh start` does this, and stops if any step fails:

1. Waits while an earlier test is still running (`FORCE=1` replaces it).
2. Snapshot only: stops the generator and the writers, and picks the query windows.
3. Starts the DuckDB server and checks it is healthy.
4. Empties both engines' caches (`querybench/clear-caches.sh`).
5. Starts the runner Job. When the test ends, the results go to S3 under
   `querybench-results/session<TEST_SUFFIX>/`.
6. Starts the watcher Job. When the runner ends, it stops the generator,
   writers, monitor and DuckDB.

Both Jobs run in the cluster, so closing the laptop does not stop a test.
Queries, arms, knobs and output: [querybench/README.md](querybench/README.md).

### 7. Summary

```bash
./bench.sh summary   # = ./summary/run.sh
```

Finds the session's newest hot and snapshot runs in S3 (`HOT_STAMP=` /
`SNAPSHOT_STAMP=` pick others), reads both tables' commit history, and writes
`bench-results/BENCH-SUMMARY-<date><TEST_SUFFIX>.md`. The bucket and account ID
are masked.

### 8. Teardown

Nothing may be writing when you wipe: a live job recommits into a half-deleted
warehouse.

```bash
./bench.sh cleanup                       # generator, writers, monitor, DuckDB
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
  notification is set on the bucket, not in this repo. After changing
  `OUTPUT_URI`, run `./flinkwriter/run.sh notify` (writers stopped).
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
  `FlinkLandingQueueConsume` policy.

| Knob | Default | Meaning |
|---|---|---|
| `SQS_QUEUE_PREFIX` | `cid-landing-jsongz` | queue = `<prefix>-<fmt>` |
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
  `PAIMON_WRITE_ONLY=true`). Iceberg's Flink sink never compacts. Compare file
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
├── querybench/              # StarRocks + DuckDB query tests: see querybench/README.md
├── summary/
│   ├── run.sh
│   └── summarize.py                 # -> bench-results/BENCH-SUMMARY-<session>.md
└── bench-results/           # CSVs, logs, summaries
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
