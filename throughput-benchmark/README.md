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
SELECT …` over a datagen view with no stateful operator, so the planner cannot
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
./jsongz-gen/run-jsongz-gen.sh start # put gzip NDJSON source data to s3 for 30 min max for 6GB/s. Run once.

# ingest/writer performance test
./build-deploy-cid.sh upload         # executor + SQL to S3, verified
./build-deploy-cid.sh deploy both    # or paimon or iceberg, Append-only throughput test. read from s3 or self-generated source stream data per job.
./monitoring/collect-metrics.sh paimon 30 3600  # or iceberg, collecting 30s metrics for 1h -> bench-results/
./monitoring/deploy-cid-monitor.sh   # show side-by-side throughput metrics comparison on console

# query/reader performance test
SUITE=hot ./querybench/starrocks.sh  # hot suites need the cid Flink jobs - writers RUNNING
SUITE=hot FORMAT=both ./querybench/duckdb.sh
SUITE=snapshot ./querybench/starrocks.sh # stops the writers, leaves them suspended
SUITE=snapshot FORMAT=both ./querybench/duckdb.sh
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
| `JOB_PARALLELISM` | 128 | **160** |
| `PAIMON_BUCKETS` | -1 (unaware-bucket) | -1 |
| `SOURCE_ROWS_PER_SECOND` | 2,000,000 | **2,097,152** |
| `HOT_KEY_FRACTION` | 0 (uniform) | **80** |
| `ICEBERG_COMPAT` | false | **true** |
| `SOURCE_MODE` | datagen | **s3json** — see [Feeding the Flink jobs from these files](#feeding-the-flink-jobs-from-these-files) |

`PAIMON_BUCKETS` is listed because it changes the write path: the default `-1`
is an unaware-bucket append table with no shuffle, like the Iceberg side. Set
`PAIMON_BUCKETS=N` (use `N = JOB_PARALLELISM`) only for a fixed-bucket-on-`uuid` run.

---

## Deployment steps

Run every step from `throughput-benchmark/` in one shell that has done
`source .bench-env` first. It points the generator (`OUTPUT_URI`) and the jobs
(`S3JSON_SOURCE_PATH`) at the **same** landing prefix, and that prefix should be
**fresh per run**: the Flink file source lists the whole prefix at JobManager
startup, and a large backlog makes the JM crash-loop (exit 239, ask timeout).
Bump the `-rN` suffix in `.bench-env` for each run.

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

### 2. Start the data generator (`SOURCE_MODE=s3json` only)

In `s3json` mode the jobs read the `.json.gz` files this lands, so start it
before or alongside the writers, or they have nothing to read. Skip this step
with `SOURCE_MODE=datagen`.

```bash
./jsongz-gen/run-jsongz-gen.sh start                # Deployment, 3 replicas; pods write DURATION_SECONDS (1..1800, max 30 min), then idle
./jsongz-gen/run-jsongz-gen.sh scale 4              # change replicas (rate = N x 500K rows/s)
./jsongz-gen/run-jsongz-gen.sh status               # per-pod rows/s, MB/s
./jsongz-gen/run-jsongz-gen.sh logs                 # follow the first pod (POD=N for another)
./jsongz-gen/run-jsongz-gen.sh stop                 # scale to 0; written files stay
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
| `flink/cid-sql/<fmt>/00-source.sql` | `sql-scripts/common/01-datagen-source.sql` or `01-s3json-source.sql`, picked by `SOURCE_MODE` | the executor's `--sql-dir` |
| `flink/cid-sql/<fmt>/02..04-*.sql` | `sql-scripts/<fmt>/` (synced with `--delete`) | the executor's `--sql-dir` |
| `flink/cid-sql/common/01-{datagen,s3json}-source.sql` | `sql-scripts/common/` (both, whatever `SOURCE_MODE` is; synced with `--delete`) | nothing at runtime — a full copy of the script set; no `--sql-dir` points here, since a second source file in `<fmt>/` would define `datagen_source` twice |

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
Then start a collector:

```bash
# ./monitoring/collect-metrics.sh paimon|iceberg [interval-s] [duration-s]
./monitoring/collect-metrics.sh iceberg 30 3600 &                   # default LOCAL_PORT=8091
LOCAL_PORT=8092 ./monitoring/collect-metrics.sh paimon 30 3600 &
```
**`LOCAL_PORT` is mandatory on a second concurrent collector.** Both default to
8091 for their `kubectl port-forward`, so the second one reads the *first* job's
samples. Output: `bench-results/cid-<format>-<UTC stamp>.csv`.

### 5. Monitoring

```bash
./build-deploy-cid.sh monitor         # FlinkDeployments + pods, one shot
kubectl port-forward svc/flink-cid-paimon-rest 8081:8081 -n emr-flink   # one job's REST/UI
```

Optional side-by-side monitor — a plain `Deployment` running the Python in
`monitoring/` (forked from `cdc-pipeline/monitoring`; record counts here are
operator-scoped, so the chained Iceberg job reports real rows/s):

```bash
./monitoring/deploy-cid-monitor.sh        # upload code, apply, restart, wait
kubectl logs -f deployment/flink-cid-monitor -n emr-flink
./monitoring/deploy-cid-monitor.sh stop   # scale to 0; re-run without args to start
```
The script uploads the three monitor files to
`s3://${BUCKET_NAME}/flink/monitoring/` and the pod fetches them at boot with
`boto3` under its IRSA service account. After a code change, just re-run
`./monitoring/deploy-cid-monitor.sh`: it re-uploads to S3 and does a rollout restart.

**Reading the comparison table.** "Records Processed" and "Throughput (rec/s)"
are rows out of the source operator (`Source__*.numRecordsOut`) over the sampling
window; rows written are `IcebergStreamWriter`/`Writer` `numRecordsIn`. "New
Records in Tables" comes from table snapshots. (Vertex-level counts only see
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

All of these step once per commit, so a 120 s window catches one or two
commits. The vertex `numBytesIn/Out` used earlier counted only network-shuffle
bytes, a few KB/s in a chained job.
Flink REST counters update in steps, so rec/s over the monitor's short window can
read below a point sample. In the first run the monitor showed Iceberg 1.198M vs
Paimon 756k rec/s (+58.4%), while point samples gave ~1.75M vs ~830k.

### 6. Run the query benchmark (the read half)

Both tables must hold rows. The **hot** suite reads the last minutes of data, so
it needs the writers **RUNNING**; the **snapshot** suite stops the writers
itself and leaves them suspended. So run both engines' hot suites first, then
both snapshot suites:

```bash
SUITE=hot                  ./querybench/starrocks.sh   # arms: paimon, pmn_as_iceberg, iceberg
SUITE=hot      FORMAT=both ./querybench/duckdb.sh
SUITE=snapshot             ./querybench/starrocks.sh   # suspends the writers
SUITE=snapshot FORMAT=both ./querybench/duckdb.sh
```

`SUITE=all` (the default) runs hot, stops the writers, then snapshot — fine for
one engine, but the second engine's hot suite would then run against stopped
writers. Other knobs: `RUNS=3`, `WARMUP=1`, `ARMS=...`, `SUSPEND_WRITERS=0`
(StarRocks); `./querybench/duckdb.sh clean` / `debug [arm]` (DuckDB). Output:
`bench-results/starrocks-<stamp>.csv` and `bench-results/duckdb-<stamp>.csv`,
same columns. Details:
[Query benchmarks — the read half](#query-benchmarks--the-read-half).

### 7. Teardown between runs

**Order matters: nothing may be writing when you wipe.** A live JobManager
recommits into a half-deleted warehouse and produces a table that looks valid
and is not.

```bash
# 7a. Stop the writers FIRST — the generator too, if it is still running.
./jsongz-gen/run-jsongz-gen.sh clean            # delete the Deployment + ConfigMap; files stay
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
aws s3 rm --recursive "${OUTPUT_URI}/"        # this run's landing prefix (s3json)
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

Ten queries, two engines, the same tables. `querybench/lib.sh` holds everything
both engines must agree on (query files, rendering, windows, partition, timeout,
median, CSV format, writer stop), so a cross-engine gap measures engines, not
harnesses.

- **`querybench/starrocks.sh`**, three arms: `paimon` (Paimon reader → Paimon
  table), `pmn_as_iceberg` (Iceberg reader → the *same* Paimon files via
  Paimon's Iceberg export, `ICEBERG_COMPAT=true`), `iceberg` (Iceberg reader →
  native Iceberg table). paimon vs pmn_as_iceberg changes only the reader;
  pmn_as_iceberg vs iceberg only the writer.
- **`querybench/duckdb.sh`**, two arms: `iceberg` (`iceberg_scan`) and `paimon`
  (`read_parquet`), one Kubernetes Job per arm.

Prerequisites: both tables hold rows, the FE pod is up, and `envsubst` exists
for DuckDB (`brew install gettext`). StarRocks creates its catalogs idempotently
and aborts if it cannot read a row through each.

### Running it

The **hot** suite reads the last minutes of data, so it needs the writers
**running**. The **snapshot** suite needs them **stopped**: it suspends them
itself and leaves them suspended. So run both engines' hot suites, then both
snapshot suites:

```bash
SUITE=hot                  ./querybench/starrocks.sh
SUITE=hot      FORMAT=both ./querybench/duckdb.sh
SUITE=snapshot             ./querybench/starrocks.sh   # suspends the writers
SUITE=snapshot FORMAT=both ./querybench/duckdb.sh
./build-deploy-cid.sh deploy both                      # resume later: a fresh, stateless restart

ARMS="paimon iceberg" ./querybench/starrocks.sh        # e.g. with ICEBERG_COMPAT off
./querybench/duckdb.sh debug iceberg   # idle pod, same spec; prints its `--shell` command
./querybench/duckdb.sh clean           # Jobs + ConfigMaps, legacy names included
```

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
| `HOT_TO`, `HOT_FROM_1M`/`5M`/`10M`, `HOT_DT` | now, now − 1/5/10 min, `HOT_TO`'s date (UTC) | hot windows; pin to rerun an earlier one |
| `SNAPSHOT_DT` | oldest `dt` in **both** tables | the snapshot partition |
| `SNAPSHOT_S4_FROM` / `_TO` | `04:30:00` / `05:00:00` | S4's slice, on `SNAPSHOT_DT` + 1 day |
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

`bench-results/starrocks-<STAMP>.csv` and `bench-results/duckdb-<STAMP>.csv`,
one row per arm × query, same columns:

```
stamp,engine,arm,suite,query,status,median_ms,result_rows,scan_rows,scan_bytes
```

`status`: StarRocks `ok`/`ERR`/`MISSING`; DuckDB `OK`/`ERROR`/`TIMEOUT`/`ZERO_SCAN`.
A failed query has an empty `median_ms`, never 0. `scan_rows`/`scan_bytes` are
the FE's `ScanRows`/`ScanBytes` medians, empty on DuckDB (no such counters). The
StarRocks CSV adds `#` lines per suite: writer state (`# suite=… writers: …`),
run parameters (`dt`, `runs`, `warmup`, `arms`, `run_id`), and one `# state` line
per arm with the partition's row count from metadata (plus `$files` for the
Iceberg-reader arms). Its console table adds `ice/pai` and `pmn/pai` ratios (>1 =
slower than paimon). DuckDB appends each arm's pod log to
`bench-results/duckdb-<arm>-<STAMP>.log`, headed `# ---- arm=… suite=… job=… writers: …`.

### The queries

`querybench/{hot,snapshot}/*.sql`. File name = query name, lexical order = run
order; add a file and it runs. Each header says what to read it against:

| Query | Measures | Read it against |
|---|---|---|
| `H1_freshness` | newest row + count, last 5 min | sanity: `rows_in_window = 0` = writers down, every hot number an empty scan. Lag = `HOT_TO` − `newest_row` |
| `H2_hot_count_1m` | `COUNT(*)`, last minute | the planning floor: almost all listing and pruning |
| `H3_hot_needle_uuid` | uuid that cannot exist, 5 min | control: uuid prunes nothing, so a full window scan. Much faster here than on H4 = winning on aggregation, not the scan |
| `H4_hot_narrow_agg` | `GROUP BY eventId`, 1 of 35 cols | projection pushdown; H5/H4 is the fat-column tax, compare it across arms |
| `H5_hot_wide_json` | bytes of the ~3.3 KB `event_json` | most sensitive to file count/size; `payload_bytes` must match across engines |
| `H6_hot_customer_agg` | `COUNT(DISTINCT)` per customer, 10 min | hash table and spill, not the read path |
| `S1_partition_count` | full-partition `COUNT(*)` | the normaliser; no metadata shortcut, caches off |
| `S2_narrow_agg` | H4's shape over the partition | S2 − S1 = the aggregate's cost at constant scan |
| `S3_risk_distinct` | `COUNT(DISTINCT customerId)` by risk level | the memory-pressure query |
| `S4_wide_json` | fat-JSON decode, 30-min ingestion slice | a decode rate over a slice, **not** comparable with S1-S3 |

Placeholders: `${TABLE}` (StarRocks: `cid_events` after `SET CATALOG`/`USE`;
DuckDB: resolved in the pod per arm and query), `${BYTES_FN}` (`LENGTH` /
`STRLEN`, both count bytes; DuckDB's `LENGTH` counts characters and
`octet_length()` is BLOB-only), and the window literals `${HOT_DT}`,
`${HOT_FROM_1M|5M|10M}`, `${HOT_TO}`, `${SNAPSHOT_DT}`, `${S4_FROM}`, `${S4_TO}`.
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
alone: full-partition scans. S4 adds a 30-minute `ingestionTime` slice, since
unbounded it ran 18-41 min per pass per arm; `ingestionTime` prunes files because
it is monotonic with write order. The slice sits on the **ingestion** day,
`SNAPSHOT_DT` + 1: `dt` derives from event time, rows of `dt=D` are written on
D+1, and a slice inside D matches zero rows.

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

### DuckDB: one Job per arm

- **Sizing.** One on-demand `m5.8xlarge` per arm, 18 cpu / 72Gi, requests =
  limits: CPU/RAM parity with StarRocks' 3 BEs (3 × 6 cpu / 24Gi), same family
  and arch. Not locality parity (one node's memory bandwidth and NIC). Arms land
  on separate nodes (anti-affinity); pods carry `karpenter.sh/do-not-disrupt`.
- **One script, two roles.** On the laptop `duckdb.sh` renders the queries via
  `lib.sh` and ships them plus itself in one ConfigMap; in the pod `--in-pod`
  fills `${TABLE}`, times each query and prints CSV rows after `===CSV===`.
- **IRSA via STS.** `credential_chain` cannot use IRSA in this pod, so before
  **every** attempt the runner calls `sts assume-role-with-web-identity` and
  writes a `CREATE SECRET` (a secret minted once expired mid-run).
- **`PIN_SNAPSHOT=1`.** The writer commits a `metadata.json` every ~60 s and keeps
  ~4; even a fresh one is GC'd within ~60 s and 404s mid-scan. Each query copies it
  to `s3://…/duckdb-cid-pinned/<RUN_TAG>/` (manifests and data are retained ~1 h),
  and the pod deletes that prefix on exit.
- **Timing and capture.** Timed around the `duckdb` process, prelude included
  (constant across arms). Rows are captured with `.output` *after* the prelude,
  because `CREATE SECRET` prints a `true` row that would read as the result and
  disarm the zero-scan guard. No rows, or a leading `0`/`NULL` without the label
  flag, is `ZERO_SCAN`: the predicate matched nothing, **not** a win.
- **Memory.** `memory_limit` sits under the container limit because DuckDB does
  not count httpfs/Arrow buffers. Container exit `137` (log: "no ===CSV===") is a
  kubelet OOM-kill: give the container headroom, **don't** raise
  `DUCKDB_MEMORY_LIMIT`.
- ⚠ **The `paimon` arm is a read-path result, not a format result.** DuckDB has no
  Paimon reader; the arm runs `read_parquet` over `dt=*/bucket-*/*.parquet` (no
  manifest planning, no stats pruning, only `dt=`), which works **only** because
  the table is append-only. `ICEBERG_DB=flink_cid_paimon_db` points the `iceberg`
  arm at Paimon's Iceberg export instead. Say which arm ran wherever a DuckDB
  ratio is quoted.
- **Arms run in parallel.** Hot arms must, or they would read a live table an hour
  apart; snapshot data is frozen, so parallel costs only S3 bandwidth. Hot
  windows are pinned at render time, before submit, so a slow Karpenter launch
  ages them, but all arms share them.

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

A **simpler, separate** data generator for when the system under test reads the
raw landing zone rather than a Paimon/Iceberg table. It writes `.json.gz` files
to S3 directly — no Flink, no table format. The Flink datagen path above
(`sql-scripts/common/01-datagen-source.sql` → Paimon/Iceberg) is unchanged.

- **Same data.** Same fields, value ladders and presence rates as
  `01-datagen-source.sql`, i.e. the real `cid_9011075d_sample_masked` extract.
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
  warning). The names are deliberately not `SOURCE_ROWS_PER_SECOND`, which
  `.bench-env` sets for the Flink path.
- **Run length: a fixed 30 minutes (1800 s).** Every pod writes for 1800 s
  from its own start, then **idles** (it does not exit, since a Deployment
  would restart it and open a new window); `stop` scales to 0.
  `DURATION_SECONDS` can only **shorten** the run (1..1800, e.g.
  `DURATION_SECONDS=120` for a smoke test). `0`, non-integers and values above
  1800 are rejected, and there is no run-until-stopped mode. `start` also
  stamps a shared `RUN_DEADLINE_EPOCH` = now + duration + 300 s (pip install,
  pod start, final flush): a pod restarted or scaled up later writes only
  until then, and not at all after it.
- **No pod index.** Each pod seeds its RNG from its own name (downward API),
  so file names cannot collide across replicas.

```bash
cd throughput-benchmark/jsongz-gen
./run-jsongz-gen.sh local                 # 30 s on this machine -> /tmp/cid-jsongz
source ../.bench-env
./run-jsongz-gen.sh render                # sizing summary + write jsongz-gen-job-deployed.yaml (no apply)
./run-jsongz-gen.sh start                 # render, ConfigMap, kubectl apply -f the deployed file; writes 30 min
./run-jsongz-gen.sh scale 4               # replicas -> 4 (new pods stop at the same deadline)
./run-jsongz-gen.sh status                # per-pod rows/s, MB/s, B/row, ratio
./run-jsongz-gen.sh stop                  # scale to 0; written files stay
./run-jsongz-gen.sh clean                 # delete Deployment, legacy Job, ConfigMap
# Knobs: JSONGZ_REPLICAS JSONGZ_ROWS_PER_POD JSONGZ_ROWS_PER_SECOND(-> replicas)
#        DURATION_SECONDS(1..1800, shorten only) WORKERS
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

`SOURCE_MODE` in `build-deploy-cid.sh` picks which file is uploaded as each
format's `00-source.sql`:

| `SOURCE_MODE` | file | reads |
|---|---|---|
| `datagen` (script default) | `sql-scripts/common/01-datagen-source.sql` | in-memory datagen at `SOURCE_ROWS_PER_SECOND` (the original path) |
| `s3json` (`.bench-env`) | `sql-scripts/common/01-s3json-source.sql` | `.json.gz` under `S3JSON_SOURCE_PATH`, continuously |

Both define the same `datagen_source` view with the same 34 columns, so
`04-pipeline.sql`, the sink tables and the queries are unchanged. **To switch
back,** set `export SOURCE_MODE=datagen` in `.bench-env` (or delete the line)
and redeploy; the deploy re-uploads `00-source.sql`.

```bash
cd throughput-benchmark && source .bench-env          # SOURCE_MODE=s3json
OUTPUT_URI=s3://$BUCKET_NAME/cid-landing-jsongz ./jsongz-gen/run-jsongz-gen.sh start
./build-deploy-cid.sh deploy paimon                   # or iceberg
```

- `S3JSON_SOURCE_PATH` (default `s3a://<bucket>/cid-landing-jsongz/`) must
  be the generator's `OUTPUT_URI`, written as `s3a://`.
  `S3JSON_MONITOR_INTERVAL` (default `10s`) is how often the source re-lists
  it.
- **Files already under the path are read too**, not only new ones. For a
  run that sees only new data, point both sides at a fresh prefix.
- **New files stop arriving after 30 minutes**, when the generator Job
  completes. The Flink job keeps running and polling, but idles once it has
  read the backlog. Size the measurement window to fit inside those 30
  minutes.
- In this mode `SOURCE_ROWS_PER_SECOND`, `HOT_KEY_FRACTION` and `CID_*` do
  nothing on the Flink side. The rate is what the generator lands, and the job
  reads as fast as it can.
- **The source can now be the bottleneck.** Each row costs an S3 read, an
  inflate and about 30 `JSON_VALUE` parses of a 4 KB line. Backpressure with
  idle sinks means the read side is the wall, so results are not comparable
  with `datagen`-mode runs.
- The continuous enumerator keeps every processed path in JobManager state and
  checkpoints, about 2 M paths/hour at 1.5 M rows/s. That is fine for a
  benchmark-length run, not for days.

---

## How it works

With the default `SOURCE_MODE=datagen`, the source is Flink's built-in
`datagen` connector (for `s3json`, see [above](#feeding-the-flink-jobs-from-these-files)) — rows are generated
in-memory on the TaskManagers (no Kafka/RDS in the read path), so the source can
never be the bottleneck and the sink write path is the only thing measured. If a
writer can't keep up, datagen backpressures and the achieved source rate **is**
that format's result.

The row is a **mockup of real security telemetry**, not synthetic padding. It
reproduces the shape of a production CID extract (`cid_9011075d_sample_masked`:
189,013 gzipped-NDJSON records, 170 distinct fields) and emits the 33 extracted
columns + `event_json` from the ingestion PoC's `schema_sample.sql`. The *data*
has real field cardinalities, real skew and real null density, so Parquet
compression, dictionary encoding and min/max pruning behave as they do on the
real table. See `sql-scripts/common/01-datagen-source.sql` for the measured
distributions and which ~95 of the 170 fields `event_json` carries.

**Row size is calibrated to a 4096 B mean.** The real sample's mean line is
3,817 B, so the generator runs ~7% wider: the five command-text fields carry
pads of 295/221/135/117/98 B — the sample's measured per-field means scaled by a
single factor of 1.233, solved by Monte-Carlo over the generator's own
probability model. Result: **mean 4,099 B (+0.07%)**, p50 4,163, p95 5,123.

Two consequences worth knowing before citing a number:

- **The pad is on the command fields, not a filler column.** Those fields are
  already where the real bytes are (the top four are 21% of sample bytes), and
  they are variable-width and sparsely present, so widening them preserves the
  null pattern and compression behaviour. A dense fixed-width filler would hit
  the byte target while destroying the properties this mockup exists to
  reproduce.
- **The model has no heavy tail.** p95 is 5,123 B vs the sample's 8,625, and max
  ~6,001 B vs 111,445 — the real outliers are pathological command lines.
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
- `HOT_KEY_FRACTION` routes the **endpoint identity** (a few noisy hosts
  producing most telemetry), not a primary key. It changes dictionary encoding
  on the endpoint columns — and therefore the generated bytes — but drives no
  merge or delete work. In fixed-bucket mode (`PAIMON_BUCKETS=N`) it does
  **not** skew Paimon's bucket distribution either: `bucket-key = 'uuid'`, and
  `uuid` comes from an independent full-range draw that this knob never touches.

Both sinks declare the identical 34 columns in the identical order with the same
identity partitioning on `dt`. `CID_DAY_SPREAD` sets how far back the generator
spreads `dt`: `datagen`'s `r_day` bounds are **inclusive**, so N spreads over
`N+1` days and the default 1 writes **two** live partitions, today and yesterday.
That is what gives the snapshot suite a previous day to read.

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
- Below it, datagen is backpressured by that format's writer and the achieved
  rate **is** its write-path ceiling. `GiB/s = source_rec_per_s × 4096 / 2³⁰`.

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
| `SOURCE_ROWS_PER_SECOND` | `2000000` | global rate; 4096 B **mean** row ⇒ ≈7.63 GiB/s. `.bench-env` raises it to `2097152` (exactly 8 GiB/s). A rate **limit**, not a guarantee — the 2026-10-05 runs delivered ~555 K rows/s against it. The SQL carries a different fallback (`1572864`, the old 6 GiB/s target) which is **unreachable in practice**: `build-deploy-cid.sh` always ships this as pod env, so the literal applies only if the JobManager env is missing |
| `JOB_PARALLELISM` | `128` | writer subtasks. 4 slots per TM, so 32 TMs. `.bench-env` sets `160` |
| `PAIMON_BUCKETS` | `-1` | Paimon `bucket`, **baked at `CREATE TABLE`**. `-1` = unaware-bucket: no shuffle and no `bucket-key`, the same layout as Iceberg. `N >= 1` = fixed buckets on `uuid`, and `build-deploy-cid.sh` then renders `PAIMON_BUCKET_KEY_OPT` (Paimon rejects a `bucket-key` on a `-1` table). Use `N = JOB_PARALLELISM`: each bucket has exactly one writer subtask, so fewer idle writers. Anything below 1 is treated as `-1`. **Changing it does nothing to an existing table** — the sink DDL is `CREATE TABLE IF NOT EXISTS`, so you must drop `cid_events` or use a fresh warehouse prefix, and never compare results across the change without saying which count produced them |
| `HOT_KEY_FRACTION` | `0` | % of rows routed to a hot 5% slice of `CID_HOST_COUNT`; `0` = uniform. Validated as an integer 0..99, because it is compared against a datagen `selector` drawn in `[0,99]`. Routes the endpoint identity, **not** a key, and does **not** affect Paimon's bucket distribution in fixed-bucket mode (`bucket-key = 'uuid'`, drawn independently). Its real effect is on **generated bytes** — the endpoint columns dictionary-encode better when skewed — so runs at different values are not comparable |
| `CID_CUSTOMER_COUNT` | `16` | distinct `customerId` ⇒ `partition_cust` cardinality |
| `CID_HOST_COUNT` | `1024` | distinct endpoints; sample measured 1,047 |
| `CID_DAY_SPREAD` | `1` | how far back `dt` spreads. `r_day`'s bounds are **inclusive**, so N ⇒ `N+1` live partitions (default 1 ⇒ today *and* yesterday). `dt` is the only partition column, so that is `JOB_PARALLELISM × (N+1)` concurrently-open files on Paimon (`PAIMON_BUCKETS × (N+1)` in fixed-bucket mode) and `JOB_PARALLELISM × (N+1)` on Iceberg (which fans out by subtask), and yesterday only closes once the writers stop. Report it with any result |
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
│   ├── common/01-datagen-source.sql # THE generator: CID telemetry mockup, 34 cols.
│   │                                #   Copied into both format prefixes AS
│   │                                #   00-source.sql — runs before the USE CATALOG.
│   ├── paimon/                      # catalog + append sink + pipeline
│   └── iceberg/                     # catalog + v2 append sink + pipeline
├── monitoring/                      # everything that measures the writers
│   ├── collect-metrics.sh           #   streaming samples -> bench-results/cid-*.csv
│   ├── deploy-cid-monitor.sh        #   side-by-side console monitor (Deployment)
│   ├── flink_cid_monitor.py         #   monitor code (forked from cdc-pipeline)
│   └── monitor-deployment.yaml      #   envsubst template; -deployed.yaml is generated
├── build-deploy-cid.sh              # render / deploy / cleanup / monitor (no build)
├── querybench/                      # READ half: StarRocks + DuckDB query benchmark
│   ├── lib.sh                       #   shared: env, windows, render, timeout,
│   │                                #   median, CSV header, writer stop, FE-idle check
│   ├── starrocks.sh                 #   3 arms, timed from the FE AUDIT LOG
│   ├── duckdb.sh                    #   Job submitter + --in-pod runner + debug / clean
│   ├── duckdb-job.yaml              #   envsubst Job template (m5.8xlarge, 18 cpu / 72Gi)
│   ├── hot/H1..H6*.sql              #   the queries. Filename = name = order.
│   └── snapshot/S1..S4*.sql
├── jsongz-gen/                      # RAW landing zone: gzipped NDJSON on S3, no Flink
│   ├── cid_jsongz_gen.py            #   the generator (multiprocess, boto3 PUTs)
│   ├── jsongz-gen-job.yaml          #   Deployment, one pod per node (60 cpu -> 16xlarge)
│   ├── run-jsongz-gen.sh            #   start / scale / status / logs / stop / clean / local / render
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
  `cid-warehouse-*` prefixes after. `SOURCE_ROWS_PER_SECOND` is a rate **limit**
  and not a guarantee: if a writer cannot absorb it, datagen backpressures and
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
  one.* `karpenter.sh/do-not-disrupt: "true"` is currently set **only** on
  `querybench/duckdb-job.yaml`, **not** on either FlinkDeployment's pod
  templates; add it there before a scoring run. The fix belongs in these
  manifests, never in the nodepool — that spec is shared with other workloads.
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
