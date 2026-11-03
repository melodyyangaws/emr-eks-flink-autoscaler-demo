# CDC query benchmark: DuckDB vs Trino on Iceberg and on native Paimon

The read half of the CDC pipeline benchmark: the same queries on two engines
over the four tables the two Flink CDC jobs write (`customers`, `products`,
`orders`, `order_items`; MySQL `ecommerce` → `flink-cdc-paimon` /
`flink-cdc-iceberg`). Flink is the only writer; DuckDB and Trino only read.
The harness follows
[../../throughput-benchmark/querybench/](../../throughput-benchmark/querybench/README.md)
and changes nothing there.

| Suite | Reads | Writers | Queries |
|---|---|---|---|
| **hot** | a 30-s window (`HOT_WINDOW_SECONDS`) ending 5 min ago (`HOT_LAG_SECONDS`): `orders.updated_at`, `order_items.created_at` | running, generator live | H1–H9 ([hot/QUERIES.md](hot/QUERIES.md)) |
| **snapshot** | one closed partition (`SNAPSHOT_DT`) of the fact tables, the whole dimension tables; S4 reads a 10-min `orders.order_date` slice | stopped | S1–S7 ([snapshot/QUERIES.md](snapshot/QUERIES.md)) |

Every command below runs from `cdc-pipeline/`. The tests themselves run in
the cluster (Jobs `cdc-qb-runner` and `cdc-qb-watch`), so the laptop can
disconnect once `run.sh start` returns.

## Run it

### 0. Dry run (offline, optional)

```bash
SUITE=hot ./querybench/run.sh render                # or RENDER_DIR=/tmp/qbr SUITE=hot ...
```

No `kubectl`, no AWS call. It writes into a temp dir (or `RENDER_DIR`): the
runner and watcher manifests, the step script, the bundle, the DuckDB
manifest, Trino's Helm values (`trino-values-deployed.yaml`), `run-meta.txt`
(table set, engines, formats, results and pins prefixes) and
`sql-<suite>-<engine>-<arm>.sql` for both suites and all four arms with sample
pins. It fails on any `${...}` left after rendering or a missing dialect file.
Check `run-meta.txt` names the table set the writers write.

### 1. Prerequisites

| Check | How |
|---|---|
| Account values in `cdc-pipeline/.bench-env.local` (git-ignored; else `throughput-benchmark/.bench-env.local`) | `AWS_REGION`, `AWS_ACCOUNT_ID`, `BUCKET_NAME`, `EMR_EXECUTION_ROLE_ARN` |
| `.bench-env` names the table set the writers write | `ICEBERG_FORMAT_VERSION=3`, `CDC_DB_SUFFIX=_v3r1` on this branch → `flink_iceberg_db_v3r1` / `flink_paimon_db_v3r1`. Unset `CDC_DB_SUFFIX` means `_v3`; `CDC_DB_SUFFIX=` (empty) is the v2 set |
| Both writers RUNNING on that set | `./querybench/run.sh status` (writers, generator, engines, Jobs) or `kubectl get flinkdeployment flink-cdc-paimon flink-cdc-iceberg -n emr-flink`. Deploy: [../1-BUILD-DEPLOY-FLINK-APP-GUIDE.md](../1-BUILD-DEPLOY-FLINK-APP-GUIDE.md) |
| Hot only: the upsert generator running | `./mysql-data-generator/deploy-data-generator.sh status` (`deploy` to start). A stopped generator is logged as a WARNING, not refused |
| Engines with native Paimon readers | DuckDB image `duckdb-cdc-server:2.0.0` (the `paimon` extension) and Trino image `trino-cdc:484-paimon-2.1.0` (catalog `paimon`); see [../engine-deploy/README.md](../engine-deploy/README.md) |
| Laptop tools | `kubectl` on the EKS cluster, `helm` (Trino), `envsubst` (`brew install gettext`), AWS CLI, `python3` |
| IRSA ServiceAccount `starrocks-sa` in `emr-flink` (historical name, shared by DuckDB and Trino) | `kubectl get sa starrocks-sa -n emr-flink`; see [../engine-deploy/README.md](../engine-deploy/README.md) |
| The DuckDB image | `DUCKDB_IMAGE` (`.bench-env`: `<account>.dkr.ecr.<region>.amazonaws.com/duckdb-cdc-server:2.0.0`) pushed by [../engine-deploy/duckdb-image/](../engine-deploy/duckdb-image/README.md); `DUCKDB_IMAGE=` uses the PyPI wheel instead |
| No throughput-benchmark test active | `run.sh start` refuses while its `qb-runner` / `qb-watch` Job is active (`FORCE=1` overrides) |

**Engines: automatic vs by hand.** `run.sh start` brings both engines up on
the laptop before it starts the runner:

- **Writers first** (hot only): `start` waits until both writers have been
  RUNNING 15 min (`WRITERS_UP_S`, 900 s; `wait_writers_up` in `lib/common.sh`)
  before it touches an engine, then resumes (or creates) the Iceberg compaction
  FlinkDeployment `iceberg-maintain` (`iceberg-compaction/run-maintain.sh`; README Step 9a) and
  brings the job monitor up (`flink-cdc-monitor` scaled to 1, or deployed; its
  reports feed the summary's ingest throughput; `MONITOR_UP=0` skips). The
  snapshot `start` suspends `iceberg-maintain` instead, so the pinned tables stay put.
- **DuckDB** (`duckdb-warm-cdc`): `engine-deploy/deploy-duckdb-cdc.sh up` when
  the server does not answer `/health`, or answers for another
  `ICEBERG_FORMAT_VERSION` or image. It must then report `"busy": false`.
- **Trino**: `engine-deploy/deploy-trino.sh up` when fewer than
  `TRINO_WORKERS` (3) workers are active. Replicas the watcher scaled to 0
  are restored first. `up` makes the same 15 min writer check
  (`TRINO_AFTER_WRITERS_S`), which a hot `start` has already passed. A Trino that is already up must run `TRINO_VERSION`
  (484), else `start` stops (`TRINO_VERSION_CHECK=0` measures it as is).

By hand: building the DuckDB image, `deploy-trino.sh down`, and redeploying
DuckDB after changing `SKIP_PUFFIN_VERIFICATION_ARMS` or any `DUCKDB_*` /
`CACHE_*` sizing (`start` only compares format version and image). The
writers and the generator are never started by the harness.

### 2. Hot test

```bash
SUITE=hot ./querybench/run.sh start
./querybench/run.sh logs                 # follow the runner (kubectl logs -f job/cdc-qb-runner)
./querybench/run.sh logs watch           # follow the watcher
```

`start`, on the laptop: waits for an active `cdc-qb-runner` (`FORCE=1`
replaces it), waits for and deletes the previous `cdc-qb-watch`, brings the
engines up, empties both caches (`clear-caches.sh`; skipped when every engine
was just started), then creates the runner and the watcher. It prints the
results path.

The runner (`hot-interleave.sh`):

1. waits until the queried writers (`flink-cdc-<format>` per format in
   `FORMATS`) are RUNNING: once a minute, up to `STABLE_CHECKS` (10) times;
2. sets the window, then waits (up to `WAIT_COMMIT_S`, 1800 s) until every
   format's `orders` and `order_items` committed at least
   `HOT_COMMIT_MARGIN_S` (60 s) after `HOT_TO`;
3. pins every table in every format (`pin.sh`), checks Trino reads the pins,
   picks H8's key;
4. runs warm-up pass p0 and `RUNS` (3) timed passes; each pass runs DuckDB's
   arms, then Trino's, on the same pins, re-pinned before every pass
   (`HOT_REPIN=pass`);
5. writes the medians, then the step uploads `bench-results/`.

**Done** looks like this in the runner log, then the watcher's teardown (it
checks every `WATCH_INTERVAL_S`, 600 s, so up to 10 min later):

```
  [querybench] done. Pins: .../hot-interleave-<STAMP>.pins  Log: ...
status: results-uploaded querybench-results/cdc-session-v3r1/<STAMP>-hot/
status: runner-exit=0 <date>
...                                      # watcher:
  [querybench] hot test completed: tearing the run down
status: qb-watch-done <date>
```

A query that is not `OK` on an arm (`ERROR`, `TIMEOUT`, `ZERO_SCAN`, ...) is a
result, not a failure: it stays in the CSVs and the summary, the runner log
lists it (`hot: N of M queries not OK: <engine>:<format>:<query>=<status> ...`),
and the runner still ends `runner-exit=0`. Only a driver failure in a pass
(engine not up or gone, a render or pin failure, no result rows) is an
`ERROR: the <engine> driver failed in pass <N>` line; the other passes still
run, the medians are written, and the runner then exits non-zero, so the
watcher leaves everything running.

Results: `s3://$BUCKET_NAME/querybench-results/cdc-session<set tag><SESSION_SUFFIX>/<STAMP>-hot/`
(set tag = `CDC_DB_SUFFIX` with `-` for `_`: `-v3r1` here).

The completed-test teardown stops the generator, suspends the writers and
stops the job monitor. To keep the pipeline live for another hot run, start
with `STOP_GENERATOR=0 SUSPEND_WRITERS=0` (they reach the watcher).

### 3. Snapshot test

Run it after the hot test: it stops the writers.

```bash
SUITE=snapshot ./querybench/run.sh start
./querybench/run.sh logs
```

`start`, on the laptop, before any Job:

1. the same runner and watcher waits as the hot test;
2. **stops the writers**: scales the generator(s) in `GENERATOR_RESOURCES`
   to 0, waits `WRITER_DRAIN_S` (180 s) so both jobs commit the binlog tail,
   suspends both FlinkDeployments (`FLINK_JOBS`, patched to `suspended`) and
   waits (up to 600 s) until no TaskManager is left;
3. resolves `SNAPSHOT_DT`: the newest date before today (UTC) that `orders`
   and `order_items` hold in every format (`SNAPSHOT_DT_PICK=oldest`, or set
   `SNAPSHOT_DT`);
4. suspends `iceberg-maintain`. There is no pre-test compaction: the Iceberg
   tables are measured as the writers and `iceberg-maintain` left them, and the
   summary reports their data/delete file and delete counts;
5. brings the engines up and starts the runner and the watcher (no cache
   clear here: the runner does it).

The runner (`snapshot.sh`):

1. **gates**: writers stopped; every engine up (DuckDB
   `busy: false`, Trino with `TRINO_WORKERS` active; up to
   `ENGINE_GATE_TIMEOUT_S`, 1800 s);
2. **pins** every table after the gate;
3. the **S4 slice** `[S_FROM, S_TO)`, untimed, on the setup engine;
4. **cold start**: `clear-caches.sh` (DuckDB restarted, `deploy-trino.sh clear-cache`);
5. each engine in `ENGINES` order: `WARMUP` (1) + `RUNS` (3) passes over its arms;
6. uploads `bench-results/`.

**Done** looks like:

```
status: duckdb-snapshot-exit=0
status: trino-snapshot-exit=0
status: results-uploaded-snapshot querybench-results/cdc-session-v3r1/<STAMP>-snapshot/
status: runner-exit=0 <date>
```

As in the hot test, a query that is not `OK` does not change an engine's
exit: it stays in the CSVs and the summary, and the driver's last line lists
it (`snapshot: N of M queries not OK: duckdb:paimon:<query>=ERROR ...`). On
the v3 set `duckdb:paimon` records `ERROR` (see Troubleshooting) and the run
still ends `duckdb-snapshot-exit=0` / `runner-exit=0` and is torn down. An
engine's exit is non-zero only when its driver fails (engine not up at the
start or the end of the suite, a render or pin failure, the writers changed
under the suite, no result rows); then `runner-exit` is non-zero and the
watcher leaves everything running.

`snapshot-<STAMP>.pins` records each gate time, the pins, the slice, the
cache-clear time and each engine's finish time and exit.

### 4. Summary

```bash
./querybench/run.sh summary
```

Syncs the session's newest hot and snapshot run from S3 (`HOT_STAMP=` /
`SNAPSHOT_STAMP=` pick others; `SUMMARY_DIR=<dir with hot/ and/or snapshot/>`
reads a local copy) and writes
`bench-results/QUERYBENCH-SUMMARY-<yyyymmdd><set tag><SESSION_SUFFIX>.md`
(`SESSION=` overrides the name). Bucket name and account id are masked.
`bench-results/` is git-ignored: `git add -f` to commit a summary.

Before `summarize.py`, `summary` runs `ingest_stats.py` (read-only boto3, on
the laptop's AWS credentials) for the Flink ingest over the run's window and
writes `bench-results/QUERYBENCH-INGEST-<session>.json`. It runs through `uv`
when installed (`uv run --with boto3 fastavro zstandard ...`: Paimon's
manifests are zstd Avro), else `python3` (then Paimon file counts are blank).
A failure there never stops the summary: section 3 says "Not available:
<reason>". `INGEST=0` skips it.

The summary, modelled on `bench-results/SUMMARY-20260927.md`:

| Section | What | From |
|---|---|---|
| Run configuration | Flink parallelism / TMs / checkpoint interval, source RDS class and storage, generator (pods × `RATE_PER_POD`, `BATCH_ROWS`, `UPSERT_RATIO`, `RECENT_KEY_RATIO`), engines, table set, runs, ingest window | `ingest-deploy/flink-cdc-*-deployed.yaml`; `aws rds describe-db-instances` (host from `mysql-cdc-env.sh`); the live StatefulSet's env (else `mysql-upsert-loadgen.yaml`) and the `.pins` `generator_before=` replicas; the `.pins` run-meta lines |
| 1. Headline | 2–5 computed sentences: Trino / DuckDB per format, Paimon / Iceberg (the export's coverage caveat on pre-2026-11-04 runs), arms that errored on every query, ingest throughput (rows/s, MB/s: the job monitor's when it has reports for the window, else table metadata, labelled so) and the delete mix | the sections below |
| 2. Query latency | median seconds per query and arm, Trino / DuckDB ratio, geometric means | `duckdb_warm-<STAMP>.csv`, `trino-<STAMP>.csv` |
| 3. Ingest | **Throughput**: rows/s and data size/s per format, median (max) over the job-monitor reports overlapping the window, plus median checkpoint duration and max backpressure share; "job-monitor reports not available for this window" when there are none. **From table metadata**: rows/s and MB/s (rows / bytes committed ÷ window seconds), rows, write commits and median interval, new data and delete files, equality and position deletes, compaction commits; RDS CPU over the window | job-monitor reports and `ingest_stats.py`, below |
| 4. Table state | per table at the window end: records, files, deletes, retained snapshots | `ingest_stats.py` |
| Conclusions / Open items | one line per measured result; arms that failed, row gaps, missing suites or monitor reports | computed |
| Details (`<details>`) | Trino server-side breakdown, data scanned, result equality and first rows, every pin (tables TSV), DuckDB restarts, the `.pins` files, ingest per table | as before |

**Where the ingest numbers come from.** Each table's own commit history, so
they work for any past run while its snapshots are retained (Paimon
`snapshot.time-retained` 12 h; Iceberg until the compaction job expires them):

- Window: the hot suite's first pin commit minus 5 min to its last pin commit
  (`QB_PIN_COMMIT_MS_*` in the `.pins` file); for a snapshot-only summary the
  writers' last 30 min before the snapshot run.
- Iceberg: the current `metadata.json` (Glue `metadata_location`), snapshots
  committed in the window. Write commits = every operation but `replace`:
  `added-records`, `added-data-files`, `added-delete-files`,
  `added-equality-deletes`, `added-position-deletes`, `added-dvs`. `replace`
  (the compaction job) is counted apart. State = the last snapshot's `total-*`.
- Paimon: `snapshot/snapshot-N` (EARLIEST..LATEST) in the window. APPEND
  commits: `deltaRecordCount`, data files from the delta manifest list
  (`_NUM_ADDED_FILES`). COMPACT commits apart, with the deletion-vector index
  files they wrote (names new in each snapshot's index manifest). Paimon
  writes no equality deletes. State = the last snapshot's `totalRecordCount`,
  live files, live DV index files and the rows they mark deleted.
- Rows/s and MB/s = rows / bytes committed (Iceberg `added-files-size`,
  Paimon the APPEND commits' new files) ÷ window seconds. Paimon's rows are
  after its write buffer merged re-written keys within a checkpoint, so for
  the same input it commits fewer rows than Iceberg (a format effect, not speed).
- Job monitor (`monitoring/`, Deployment `flink-cdc-monitor`): its report
  rows/s (`throughput_records_per_sec`: rows the tables committed / time
  between commits) and bytes/s (`throughput_bytes_per_sec`), median and max
  over the reports whose sampling window overlaps the ingest window, plus
  median checkpoint duration and max backpressure share. Each report JSON is
  copied to
  `s3://$BUCKET_NAME/querybench-results/cdc-session<set tag><SESSION_SUFFIX>/monitor/`
  (`REPORT_S3_URI`, set by `monitoring/deploy-monitor.sh`; best-effort). The
  monitor samples Paimon and Iceberg in parallel over the same 120 s, with a
  15 s pause between cycles (`REPEAT_INTERVAL`): about 10 reports per format
  in a 30-min window. `SUITE=hot run.sh start` brings it up with the writers
  (scales it to 1, or runs `deploy-monitor.sh` when it is missing, set up for
  another table set, or without the S3 copy; `MONITOR_UP=0` skips), and the
  watcher's teardown scales it to 0. Runs before the S3 copy existed (e.g.
  2026-11-02, `_v3r2`) have no reports: the reports lived only in the pod's
  `/tmp/reports` and were lost at teardown, so the summary says so and shows
  the table-metadata numbers.

### 5. Stop / teardown

```bash
SUITE=hot ./querybench/run.sh stop       # = qb-watch.sh teardown, on the laptop
```

In order: generator to 0 (no drain wait), writers suspended (wait for the
TaskManagers to go), `flink-cdc-monitor` to 0, `duckdb-warm-cdc` deleted,
Trino's coordinator and workers scaled to 0 (the Helm release stays), the
runner's ConfigMap and Secret deleted. `STOP_GENERATOR=0`, `SUSPEND_WRITERS=0`,
`STOP_DUCKDB=0`, `STOP_TRINO=0`, `STOP_COMPACTION_CRON=0` each keep one running. `SUITE` only labels
the log. Finished Jobs are deleted by their TTL (1 day) or the next `start`.

Remove Trino completely: `./engine-deploy/deploy-trino.sh down`.

Bring the pipeline back after a teardown or a snapshot test:

```bash
for j in flink-cdc-paimon flink-cdc-iceberg; do
  kubectl patch flinkdeployment "$j" -n emr-flink --type=merge -p '{"spec":{"job":{"state":"running"}}}'
done                                      # resumes from the last checkpoint; never re-apply them
kubectl scale deploy/flink-cdc-monitor -n emr-flink --replicas=1
./mysql-data-generator/deploy-data-generator.sh scale 4
```

### 6. After a failure

Only `status: runner-exit=0` is torn down. A non-zero exit, or a failed or
vanished runner pod or Job, leaves the generator, writers, job monitor, DuckDB
and Trino **running**, so a fix costs a re-run, not a new MySQL snapshot. The
watcher then logs the runner's last error lines and what is still up, and
ends with:

```
  [querybench] fix and re-run with SUITE=<suite> ./querybench/run.sh start, or tear down with SUITE=<suite> ./querybench/run.sh stop
status: qb-watch-failed-left-running <date>
```

1. Read the cause: `./querybench/run.sh logs` (or
   `kubectl logs job/cdc-qb-runner -n emr-flink --tail=400`); the watcher's
   report: `./querybench/run.sh logs watch`. The runner pod log is kept for a
   day. The step uploads `bench-results/` even after a failure, so partial
   results are under the run's S3 prefix.
2. Check what is up: `./querybench/run.sh status`.
3. Fix, then re-run `SUITE=<suite> ./querybench/run.sh start` (it replaces
   the finished runner), or tear down with `SUITE=<suite> ./querybench/run.sh stop`.

A failure inside `start` itself stops before the runner Job is created
(`...; no query Job left running` or a `[querybench] ERROR:` line). Whatever
it already did stays done: for a snapshot start the generator and writers may
already be stopped.

## Troubleshooting

Issues met on the first cluster runs (2026-11-02), and the refusals the
scripts print.

| Symptom | Cause | Fix |
|---|---|---|
| Runner ends with a non-zero `runner-exit` and no ERROR line | A command failing under `set -e` ended the script before it logged anything | Fixed: the runner step and `snapshot.sh` run with `set -uo pipefail` and record each engine's exit (`status: <engine>-snapshot-exit=N`); the other scripts stop through `qb_die` with a message |
| Trino rejects a query that DuckDB runs, e.g. `Cannot apply operator: timestamp(6) >= varchar(19)` | Dialect: no implicit VARCHAR → TIMESTAMP coercion, no byte-length function on VARCHAR, table alias only after `FOR VERSION AS OF` | Fixed in the SQL: `TIMESTAMP '…'` literals, the `BYTE_LEN_OPEN`/`BYTE_LEN_CLOSE` dialect keys (`dialect-*.env`), aliases after the placeholder. Rules: [hot/QUERIES.md](hot/QUERIES.md) "Writing for both engines". Check a change offline with `run.sh render` |
| `duckdb:paimon` queries all `ERROR`: `Manifest ... missing required Avro key-value metadata field 'schema'` | Paimon 2.1's v3 Iceberg export writes manifests without the `schema` key DuckDB 2.0 requires | Not fixed (upstream). The arm records `ERROR`, is not retried and is never counted as a mismatch; it does not change the exit (listed in the `N of M queries not OK` line). `FORMATS=iceberg` skips both paimon arms |
| `404` / `NoSuchKey` on a Paimon export `vN.metadata.json` mid-scan | Paimon's default kept no previous export metadata, so the path read from `version-hint.text` was gone within a checkpoint | Pinned copies (`pin.sh` copies every metadata.json to `querybench-pins/`, 3 attempts), and the Paimon sinks now set `metadata.iceberg.previous-versions-max = 3` + `delete-after-commit.enabled`. Table options apply to new tables only: a fresh `CDC_DB_SUFFIX` |
| `ENGINES must name duckdb and/or trino, got 'starrocks'` | StarRocks was removed from this harness (4.1.7 reads neither the v3 Paimon tables nor Iceberg v3 deletion vectors) | Use `duckdb` / `trino`. Its scripts and results stay in git history; `starrocks-sa` keeps its name |
| `throughput-benchmark's qb-runner/qb-watch is active` | Same cluster, bucket and nodepools | Wait, or `FORCE=1` |
| `Trino runs <v>, not TRINO_VERSION=484` | A different Trino is up | `./engine-deploy/deploy-trino.sh up`, or `TRINO_VERSION_CHECK=0` |
| `no date before <today> in the orders and order_items partitions of every format` | No closed partition in both formats | `SNAPSHOT_DT=YYYY-MM-DD` |
| `snapshot slice: ... under the 10-min slice` / `no snapshot slice span on any format` | The pinned span is too short or unreadable | Lower `SNAPSHOT_SLICE_MINUTES`, set `S_FROM`/`S_TO`, or `SNAPSHOT_SLICE_MINUTES=0` (whole day) |
| `the queried writers (...) are not all RUNNING after 10 checks` | A writer is suspended or failing | Resume it (step 5); `kubectl get flinkdeployment -n emr-flink` |
| `not committed past HOT_TO+60s after 1800s` | A writer is not checkpointing | Check the writer's checkpoints; `WAIT_COMMIT_S` raises the wait |
| `no H8 candidate on <engine>:<arm>` | No older order updated in the window | Check the generator; or set `LOOKUP_ORDER_ID` and `LOOKUP_ORDER_DT` |
| `Trino cannot read <ICEBERG_DB>.<t> at its pinned snapshot` | Catalog or IRSA problem (a Paimon table is only a WARNING) | `./engine-deploy/deploy-trino.sh sql "SELECT 1 FROM iceberg.<db>.<t> LIMIT 1"` |
| `duckdb-warm-cdc is not ready and idle` | Server down, starting, or busy | `./engine-deploy/deploy-duckdb-cdc.sh status` |

## The four arms

`ENGINES` × `FORMATS`, both default to both: four arms.
`ENGINES=duckdb|trino` and `FORMATS=iceberg|paimon` select a subset for
either suite; `ENGINES` order is the run order.

| Arm | Engine | Data | Table expression at the pin (`qb_table_expr`) |
|---|---|---|---|
| `duckdb:iceberg` | DuckDB 2.0.0 warm server `duckdb-warm-cdc` | Iceberg tables, `flink_iceberg_db<suffix>` | `iceberg_scan('<copy of the pinned metadata.json>')` |
| `trino:iceberg` | Trino 484, catalog `iceberg` (Glue) | the same | `iceberg.<ICEBERG_DB>.<t> FOR VERSION AS OF <snapshot id>` |
| `duckdb:paimon` | DuckDB, the **native** `paimon` extension (paimon-cpp, bitmap64 DVs; built into `duckdb-cdc-server:2.0.0`) | Paimon tables, `paimon-warehouse/<PAIMON_DB>.db/<t>` | `paimon_scan('<warehouse>/<PAIMON_DB>.db/<t>', snapshot_from_id=<N>)` |
| `trino:paimon` | Trino, the **native** Paimon connector (paimon-trino 2.1.0 ported to 484, image `trino-cdc:484-paimon-2.1.0`), catalog `paimon` (`QB_TRINO_PAIMON_CATALOG`; filesystem metastore on the warehouse) | the same | `paimon.<PAIMON_DB>.<t> FOR VERSION AS OF BIGINT '<N>'` (the connector maps it to `scan.snapshot-id`) |

Since 2026-11-04 both Paimon arms read Paimon itself, not its Iceberg export
(which listed only top-LSM-level files until a full compaction, and whose v3
manifests DuckDB could not read). Runs before that (`_v3r3` and older) read the
export; `summarize.py` keeps their `(export)` labels (no `paimon_reader=native` in
their pins).

- **One pin per format, both engines.** `pin.sh` copies each Iceberg table's
  current metadata.json to `s3://$BUCKET_NAME/querybench-pins/...` and reads its
  `current-snapshot-id`. For Paimon it reads the native snapshot `N` from
  `<table>/snapshot/LATEST`, checked against `snapshot-N` (commit time, record
  count, commit kind). There is no copy: a Paimon snapshot's files stay until it
  expires (`snapshot.time-retained` 12 h, at least 5 kept), longer than a run.
- **Validity.** Both engines on one format read the same snapshot, so any row
  difference on a query both ran OK makes the suite **INVALID**. Iceberg vs
  Paimon differences are a **pipeline gap**, not a failure: the two writers
  checkpoint at different moments.
- **An arm that cannot read is a result.** Its queries record `ERROR`, are not
  run again and are not compared. A pinned Paimon table Trino cannot read is a
  WARNING; a pinned Iceberg table it cannot read stops the run.
- **Native Paimon caveats (state them with results):**
  - DuckDB: paimon-cpp has its own S3 client, so the server creates a
    `TYPE paimon` secret (`paimon_irsa`, the same IRSA keys as `irsa`). Neither
    `cache_httpfs` nor `profile_query`'s scan-bytes counter sees those reads:
    `duckdb:paimon` reads S3 **uncached** on every pass (not cache-comparable
    with `duckdb:iceberg`), and its scan bytes are missing or zero.
  - DuckDB: `count(*)` without a filter is answered from manifest metadata.
  - Trino's `paimon` catalog has `fs.cache.enabled=true`, like `iceberg`.
  - **Not yet run live** (wired 2026-11-04, checked offline with `run.sh render`):
    the first run on a new table set is the test of `snapshot_from_id`, the
    `paimon_irsa` secret, and `FOR VERSION AS OF` on the Paimon connector.

Table sets (`.bench-env`):

| Set | `ICEBERG_FORMAT_VERSION` | `CDC_DB_SUFFIX` | Glue databases | Results prefix / summary |
|---|---|---|---|---|
| v3, this branch | `3` | `_v3r1` (unset = `_v3`) | `flink_iceberg_db_v3r1`, `flink_paimon_db_v3r1` | `cdc-session-v3r1<SESSION_SUFFIX>/`, `QUERYBENCH-SUMMARY-<date>-v3r1<SESSION_SUFFIX>.md` |
| v2 | `2` | empty | `flink_iceberg_db`, `flink_paimon_db` | `cdc-session<SESSION_SUFFIX>/`, `QUERYBENCH-SUMMARY-<date><SESSION_SUFFIX>.md` |

`ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX= ./querybench/run.sh ...` runs the v2
set. Every `.pins` file and the summary header record the set, engines,
formats, engine versions and DuckDB's puffin setting; `pin.sh` warns when a
pinned metadata.json's `format-version` differs from `ICEBERG_FORMAT_VERSION`.

## Table settings parity

The sink DDLs (`ingest-deploy/sql-scripts/{paimon,iceberg}/03-*-sinks.sql`) are paired
setting by setting: format version 3, primary keys and partitioning, zstd
level 1, row-group and target file sizes, manifest size and merge count,
file statistics (`truncate(16)`), snapshot retention (12 h, at least 5 kept),
3 previous metadata.json files kept (Paimon's export too). Paimon writes no
changelog (`changelog-producer = none`). New settings apply to new tables
only (a fresh `CDC_DB_SUFFIX`). Differences kept on purpose; state them with
every result:

- **Writer parallelism:** Paimon writes the fact tables with 32 writers;
  Iceberg's `hash` distribution sends each date partition to one writer. At
  the source-limited ~52K rows/s (2026-11-02) both keep up; above that the
  single Iceberg writer is the expected ceiling.
- **Compaction:** Paimon compacts inline (deletion vectors); Iceberg only in
  the separate `iceberg-maintain` job, which may not keep up (2026-11-03: it
  completed passes on `products` only).
- **Snapshot expiry:** Paimon expires continuously in the writer; Iceberg
  only in `iceberg-maintain`.

## Sizing parity

| | DuckDB `duckdb-warm-cdc` | Trino (`engine-deploy/deploy-trino.sh`) |
|---|---|---|
| query compute | 1 pod, **27 cpu / 114 GiB** (m5.8xlarge), requests = limits | **3 workers × 9 cpu / 38 GiB = 27 cpu / 114 GiB** (one m5.4xlarge each) |
| control plane | same process | coordinator 4 cpu / 16 GiB (m5.2xlarge), no splits: **on top of** the 27 cpu |
| query memory | `memory_limit` 84049 MiB | `query.max-memory` 3 × 20233 MB = 59.3 GiB |
| disk cache | cache_httpfs, 450 GiB gp3-fast, 256 KiB blocks | alluxio file system cache, 3 × 167 Gi × 90 % ≈ 450 GiB, 1 MB pages (`TRINO_CACHE_PAGE_SIZE`) |
| memory cache | 22 GB cache_httpfs memory cache + buffer pool | none (OS page cache in ~7.6 GiB non-heap per worker) |
| placement | `driver-nodepool`, m5, amd64, on-demand, `do-not-disrupt` | the same |

CPU and memory are equal; Trino's extra coordinator, DuckDB's memory cache
and the different cache page sizes are not.

## Cold start and warm passes

- **Snapshot:** after the gates, pins and slice, `clear-caches.sh` restarts
  `duckdb-warm-cdc` (a new pod: new cache volume, empty memory caches;
  `/clear_cache` would miss the Parquet-footer and HTTP-metadata caches) and
  runs `deploy-trino.sh clear-cache` (empties `/cache`, restarts workers and
  coordinator on fresh volumes, waits for `TRINO_WORKERS`). The engines then
  run one after the other, each `WARMUP` + `RUNS` passes, pass-major. Neither
  shares a cache, so the second also starts cold.
- **Hot:** `start` clears both caches the same way (skipped when every engine
  was just started); p0 is the warm-up, p1..pRUNS are timed.
- **Setup reads** (S4 slice span, H8 key) are untimed, on `QB_SETUP_ENGINE`
  (Trino when it runs, else DuckDB): before the cold start (snapshot), before
  the warm-up pass (hot).

## Files

| File | Role |
|---|---|
| `run.sh` | Laptop entry point: `start`, `render`, `status`, `logs [watch]`, `summary`, `stop` |
| `hot-interleave.sh` | Hot runner step: writers RUNNING, window, commit wait, pins per pass, interleaved engines, medians |
| `snapshot.sh` | Snapshot runner step: gates, pins, slice, cold start, each engine's passes, upload |
| `duckdb-warm.sh` | DuckDB driver (`run`, `render <suite> [arm]`): POSTs each query to the server; detects a server restart (CRASH); untimed scan-bytes profile pass (`PROFILE_SCAN`) |
| `trino.sh` | Trino driver (`run`, `render <suite> [arm]`), below |
| `pin.sh` | Pins every table of each format, appends `tables-<STAMP>.tsv`, checks Trino reads every pin |
| `clear-caches.sh` | The cold start; refuses while either engine is busy (`ALLOW_CONCURRENT=1` skips) |
| `qb-runner.yaml` | Runner / watcher Job: namespace Role, IRSA on `EMR_EXECUTION_ROLE_ARN`, `python:3.12-slim` + kubectl + awscli, `backoffLimit: 0`, 1-day deadline and TTL |
| `qb-watch.sh` | Watcher (`cdc-qb-watch`) and `teardown` (= `run.sh stop`) |
| `summarize.py` | The Markdown summary |
| `ingest_stats.py` | Flink ingest over the run's window from the tables' commit history (`summary` runs it) |
| `dialect-duckdb.env`, `dialect-trino.env` | Per-engine SQL fragments (`KEY=VALUE`, parsed, never sourced) |
| `../lib/querybench.sh`, `../lib/common.sh` | Everything shared: arms, pins, windows, rendering, timeouts, median, CSV format, gates, writer and generator control; `.bench-env` loading and overrides |

### The Trino driver

- **The CLI runs in the coordinator pod** (`kubectl exec deploy/trino-coordinator -c trino-coordinator -- trino ...`):
  the image ships the CLI at the server's version, so the runner installs no
  JDK and the CLI cannot drift. Its JVM is capped by `TRINO_CLI_JAVA_OPTS`
  (`-Xmx512m`).
- **Tagging:** `--source qb-<run>-<suite>-<arm>-p<pass>-q<n>`, `--client-tags querybench,cdc,<suite>,<query>`.
- **Server-side time:** the query's row in `system.runtime.queries` gives
  `queued_time_ms` and elapsed (`"end" - created`); recorded time =
  **elapsed − queued** (planning + execution + delivering the result, as
  DuckDB's execute + fetch). CPU and physical input come from
  `/v1/query/<id>` (Service `trino:8080`, else `curl` in the coordinator;
  blank if neither answers).
- **Timeout:** session `query_max_run_time` = the suite timeout; a client
  deadline 300 s later kills the query. `EXCEEDED_TIME_LIMIT` → `TIMEOUT`,
  memory errors → `OOM`, else `ERROR`.
- **Rows:** `--output-format JSON`, duplicate column names kept, printed as
  the DuckDB server prints them (`|`-joined, `NULL`, Python `str()`).

## Placeholders (`qb_render`)

| Placeholder | Value |
|---|---|
| `${CUSTOMERS}` `${PRODUCTS}` `${ORDERS}` `${ORDER_ITEMS}` | the arm's table expression; an alias may follow (`${ORDERS} AS o`) |
| `${ORDERS_PART}` / `${ORDER_ITEMS_PART}` | iceberg: `order_dt` / `created_dt`; paimon: `order_date_str` / `created_date_str` (STRING `YYYY-MM-DD`) |
| `${HOT_DT}` `${HOT_FROM}` `${HOT_TO}` | hot window, UTC `YYYY-MM-DD HH:MM:SS`, bare (the SQL writes `TIMESTAMP '${HOT_FROM}'`) |
| `${SNAPSHOT_DT}` `${S_FROM}` `${S_TO}` | snapshot partition, S4 slice on `orders.order_date` |
| `${LOOKUP_ORDER_ID}` `${LOOKUP_ORDER_DT}` | H8's key, one per run |
| dialect keys | every `KEY=VALUE` of `dialect-<engine>.env`; may not shadow a key above |

Any `${...}` left after rendering (comments included) stops the run.
`-- bench:` flags: `allow-zero-rows`, `leading-col-is-label` (the ZERO_SCAN guard).

## Knobs

Set on the `run.sh start` command line; the ones the runner needs are passed
to the pods (`PASS_VARS` in `run.sh`).

| Knob | Default | Meaning |
|---|---|---|
| `SUITE` | — | `hot` / `snapshot` (`start` requires one) |
| `ENGINES` / `FORMATS` | `duckdb trino` / `both` | the arms; `ENGINES` order = run order |
| `RUNS` / `WARMUP` | `3` / `1` | timed / discarded passes (hot: one warm-up pass p0) |
| `SESSION_SUFFIX` / `STAMP` | `""` / now (UTC) | results session (`-r2`) / run id |
| `QB_SETUP_ENGINE` | `trino` if in `ENGINES`, else `duckdb` | engine of the untimed setup reads |
| `SKIP_PUFFIN_VERIFICATION_ARMS` | empty | DuckDB arms with `iceberg_unsafe_skip_puffin_verification` (was `paimon` on v3, for the export); applied at `deploy-duckdb-cdc.sh up` |
| `QB_TRINO_PAIMON_CATALOG` | `paimon` | Trino catalog of the `trino:paimon` arm |
| `DUCKDB_IMAGE` | ECR `duckdb-cdc-server:2.0.0` | empty = PyPI `duckdb==2.0.0` |
| `TRINO_VERSION` / `TRINO_VERSION_CHECK` | `484` / `1` | version `start` requires of a running Trino |
| `TRINO_WORKERS` / `TRINO_USER` / `TRINO_CATALOG` | `3` / `cdcbench` / `iceberg` | readiness, CLI user, catalog |
| `CLEAR_COORDINATOR` | `1` | `0`: `clear-cache` keeps the coordinator |
| `HOT_WINDOW_SECONDS` / `HOT_LAG_SECONDS` | `30` / `300` | hot window (30 s since 2026-11-02, as the throughput benchmark; was 300); `HOT_TO`, `HOT_FROM`, `HOT_DT` pin it |
| `HOT_COMMIT_TABLES` / `HOT_COMMIT_MARGIN_S` / `WAIT_COMMIT_S` | `orders order_items` / `60` / `1800` | commit-past-the-window wait |
| `STABLE_CHECKS` | `10` | writer RUNNING checks, 60 s apart |
| `HOT_REPIN` | `pass` | `run` = one pin for every pass (keep passes well under ~10 min) |
| `QB_QUERY_WRITERS` | `flink-cdc-<format>` per format | writers the hot test needs RUNNING |
| `LOOKUP_ORDER_ID` / `LOOKUP_ORDER_DT` | picked | H8 key |
| `SNAPSHOT_DT` / `SNAPSHOT_DT_PICK` | newest closed / `newest` | snapshot partition |
| `SNAPSHOT_SLICE_MINUTES` / `SNAPSHOT_SLICE_MARGIN_S` / `SNAPSHOT_SLICE_COLUMNS` | `10` / `120` / `orders:order_date` | S4 slice (`0` = whole day); `S_FROM`, `S_TO` pin it |
| `WRITER_DRAIN_S` | `180` | generator stop → writer suspend (snapshot `start`) |
| `ENGINE_GATE_TIMEOUT_S` | `1800` | runner waits for the engines |
| `HOT_QUERY_TIMEOUT_S` / `SNAPSHOT_QUERY_TIMEOUT_S` | `1800` / `3600` | per query, both engines |
| `MAX_RESULT_ROWS` | `10000` | rows kept per query for the comparison |
| `PROFILE_SCAN` | `1` | DuckDB's untimed scan-bytes pass (hot: last pass only) |
| `WATCH_INTERVAL_S` | `600` | watcher poll |
| `STOP_GENERATOR` / `SUSPEND_WRITERS` / `STOP_DUCKDB` / `STOP_TRINO` / `STOP_COMPACTION_CRON` | `1` | what the teardown stops (`STOP_COMPACTION_CRON`: `iceberg-maintain`) |
| `WRITERS_UP_S` | `900` | a hot `start` waits until the writers have been RUNNING this long |
| `FORCE` / `ALLOW_CONCURRENT` | off | replace an active runner, ignore throughput's Jobs / skip the one-test-at-a-time guard |
| `RENDER_DIR` | temp dir | `render` output |
| `HOT_STAMP` / `SNAPSHOT_STAMP` / `SUMMARY_DIR` / `SESSION` | newest / S3 / derived | `summary` inputs and name |
| DuckDB / Trino sizing | | [../engine-deploy/README.md](../engine-deploy/README.md) |

## Output

In `s3://$BUCKET_NAME/querybench-results/cdc-session<set tag><SESSION_SUFFIX>/<STAMP>-<suite>/`:

| File | Content |
|---|---|
| `duckdb_warm-<STAMP>.csv`, `trino-<STAMP>.csv` | `stamp,engine,arm,suite,query,status,median_ms,result_rows,scan_rows,scan_bytes`; status `OK`, `ERROR`, `MISSING`, `TIMEOUT`, `ZERO_SCAN`, `CRASH` (DuckDB restarted under the query), `OOM` (Trino) |
| `trino-<STAMP>-queries.csv` | every Trino pass: query id, elapsed, queued, execution, CPU, physical input, error |
| `*-rows.tsv`, `*-firstrows.csv` | result rows of the last timed pass |
| `duckdb_warm-<STAMP>-events.csv`, `*.log` | DuckDB restarts and cold queries; every pass |
| `tables-<STAMP>.tsv` | pinned snapshots' state per pin tag × format × table |
| `hot-interleave-<STAMP>.pins` / `snapshot-<STAMP>.pins` | windows, pins, H8 key, gates, writer and generator state |
| hot: `*-p<N>.csv`, `hot-interleave-<STAMP>.log` | every pass (p0 = warm-up) |

Pinned metadata copies: `s3://$BUCKET_NAME/querybench-pins/cdc-session<set tag><SESSION_SUFFIX>/<tag>/<format>/<table>/`.

## Caveats

1. **Native Paimon arms are new (2026-11-04) and not yet run live**; `duckdb:paimon`
   reads S3 uncached (paimon-cpp's own client) and reports no scan bytes.
2. **Runs before 2026-11-04 read Paimon's Iceberg export**, a partial view
   (top-LSM-level files only until a full compaction); their summaries say so.
3. **The hot window (5 min) and the S4 slice (10 min)** are guesses; size them
   from the first run's H1 and S4 rows.
4. **The coordinator runs the CLI** (≤ 512 MB heap) during the timed window,
   off Trino's split path.
5. **Status:** checked offline (`bash -n`, `py_compile`, `run.sh render` for
   both suites, v3 and v2, `ENGINES=trino`; stubbed `pin.sh`, `trino.sh run`
   and `summarize.py`). First cluster runs 2026-11-02 (Troubleshooting);
   every duration above is an estimate until a full run completes.
