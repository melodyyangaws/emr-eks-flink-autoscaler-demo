# CDC query benchmark: DuckDB vs Trino on Iceberg and on Paimon (as iceberg)

The read half of the CDC pipeline benchmark: the same queries on two engines
over the four tables the two Flink CDC jobs write (`customers`, `products`,
`orders`, `order_items`; MySQL `ecommerce` → `flink-cdc-paimon` /
`flink-cdc-iceberg`). Flink is the only writer; DuckDB and Trino only read.
The harness follows
[../../throughput-benchmark/querybench/](../../throughput-benchmark/querybench/README.md)
and changes nothing there. This file is the reference for the tests; the
end-to-end runbook is [../README.md](../README.md) (Steps 8–11).

| Suite | Reads | Writers | Queries |
|---|---|---|---|
| **hot** | a 30-s window (`HOT_WINDOW_SECONDS`) ending 5 min ago (`HOT_LAG_SECONDS`): `orders.updated_at`, `order_items.created_at` | running, generator live | H1–H9 ([hot/QUERIES.md](hot/QUERIES.md)) |
| **snapshot** | one closed partition (`SNAPSHOT_DT`) of the fact tables, the whole dimension tables; S4 reads the partition's busiest 10-min `orders.order_date` slice | stopped, both formats compacted | S1–S7 ([snapshot/QUERIES.md](snapshot/QUERIES.md)) |

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
| Account values in `cdc-pipeline/.bench-env.local` (git-ignored; else `throughput-benchmark/.bench-env.local`) | `AWS_REGION`, `AWS_ACCOUNT_ID`, `BUCKET_NAME`, `EMR_EXECUTION_ROLE_ARN`, `MYSQL_HOST` |
| `.bench-env` names the table set the writers write | `ICEBERG_FORMAT_VERSION=3`, `SESSION_SUFFIX=<suffix>` (a free-text round name) → `flink_iceberg_db<suffix>` / `flink_paimon_db<suffix>`. Unset or empty `SESSION_SUFFIX` means the unsuffixed databases |
| Both writers RUNNING on that set | `./querybench/run.sh status` (writers, generator, engines, Jobs) or `kubectl get flinkdeployment flink-cdc-paimon flink-cdc-iceberg -n emr-flink`. Deploy: [../1-BUILD-DEPLOY-FLINK-APP-GUIDE.md](../1-BUILD-DEPLOY-FLINK-APP-GUIDE.md) |
| Hot only: the upsert generator running | `./mysql-data-generator/deploy-data-generator.sh status` (`deploy` to start). A stopped generator is logged as a WARNING, not refused |
| Snapshot only: the hot watcher's compaction done and OK | `./compaction/run.sh status` shows no `cdc-compact-*` left (README Step 10) |
| Engine images | DuckDB `duckdb-cdc-server:2.0.0` (`DUCKDB_IMAGE` in `.bench-env`) and Trino `trino-cdc:484-paimon-2.1.0` (catalog `iceberg`); see [../engine-deploy/README.md](../engine-deploy/README.md). `DUCKDB_IMAGE=` uses the PyPI wheel instead (a separate timing baseline) |
| Laptop tools | `kubectl` on the EKS cluster, `helm` (Trino), `envsubst` (`brew install gettext`), AWS CLI, `python3`, `uv` (summary) |
| IRSA ServiceAccount `starrocks-sa` in `emr-flink` (historical name, shared by DuckDB, Trino and the Jobs) | `kubectl get sa starrocks-sa -n emr-flink`; see [../engine-deploy/README.md](../engine-deploy/README.md) |
| No throughput-benchmark test active | `run.sh start` refuses while its `qb-runner` / `qb-watch` Job is active (`FORCE=1` overrides) |

**Engines: automatic vs by hand.** `run.sh start` brings both engines up on
the laptop before it starts the runner:

- **Writers first** (hot only): `start` waits until both writers have been
  RUNNING 30 min (`WRITERS_UP_S`, 1800 s; `wait_writers_up` in `lib/common.sh`)
  before it touches an engine, then brings the job monitor up (`flink-cdc-monitor` scaled to 1, or
  deployed; its reports feed the summary's ingest throughput; `MONITOR_UP=0` skips).
- **DuckDB** (`duckdb-warm-cdc`): `engine-deploy/deploy-duckdb-cdc.sh up` when
  the server does not answer `/health`, or answers for another
  `ICEBERG_FORMAT_VERSION`, image or set of skip-puffin arms. It must then report `"busy": false`.
- **Trino**: `engine-deploy/deploy-trino.sh up` when fewer than
  `TRINO_WORKERS` workers are active (`.bench-env`: 9; script default 3). Replicas the watcher scaled to 0
  are restored first. `up` makes the same 30 min writer check
  (`TRINO_AFTER_WRITERS_S`), which a hot `start` has already passed. A Trino that is already up must run `TRINO_VERSION`
  (484), else `start` stops (`TRINO_VERSION_CHECK=0` measures it as is).

By hand: building the images, `deploy-trino.sh down`, and redeploying
DuckDB after changing any `DUCKDB_*` / `CACHE_*` sizing (`start` does not
compare sizing). The writers and the generator are never started by the harness.

### 2. Hot test

```bash
SUITE=hot ./querybench/run.sh start
./querybench/run.sh logs                 # follow the runner (kubectl logs -f job/cdc-qb-runner)
./querybench/run.sh logs watch           # follow the watcher
```

`start`, on the laptop: waits for an active `cdc-qb-runner` (`FORCE=1`
replaces it), waits for and deletes the previous `cdc-qb-watch`, waits for the
writers (above), brings the monitor and the engines up, empties both caches
(`clear-caches.sh`; skipped when every engine was just started), then creates
the runner and the watcher. It prints the results path.

The runner (`hot-interleave.sh`; its header is the authoritative description):

1. waits until the queried writers (`flink-cdc-<format>` for the table sets in
   `FORMATS`, `QB_QUERY_WRITERS`) are RUNNING: once a minute, up to
   `STABLE_CHECKS` (10) times, and logs the generator's state;
2. sets the window: `HOT_WINDOW_SECONDS` (30) ending `HOT_LAG_SECONDS` (300)
   before now, then waits (up to `WAIT_COMMIT_S`, 1800 s) until every format's
   `orders` and `order_items` (`HOT_COMMIT_TABLES`) committed a snapshot at
   least `HOT_COMMIT_MARGIN_S` (60 s) after `HOT_TO`;
3. pins every table in every format (`pin.sh`), checks Trino reads the pins,
   and re-pins every 60 s until every format's **pinned** `orders` and
   `order_items` hold a row at or past `HOT_TO` in partition `HOT_DT`
   (`HOT_DATA_WAIT_S`, 3600 s; `0` skips; the wait is recorded as
   `data_wait_s` in the pins file). A commit past the window is not enough: in
   a binlog backlog a writer commits every checkpoint with minutes-old events
   (on `_v3final2` Paimon was ~17 min behind and 5 of 9 of its hot queries were
   ZERO_SCAN). Then picks H8's key, once, from orders the generator never deletes
   (`order_id % FK_PARENT_MOD = 0`);
4. runs warm-up pass p0 and `RUNS` (3) timed passes; each pass runs DuckDB's
   arms, then Trino's (`ENGINES` order), on the same pins, re-pinned before every pass
   (`HOT_REPIN=pass`; `run` keeps the first pin);
5. writes the medians of the timed passes, then the step uploads `bench-results/`.

The pins file `hot-interleave-<STAMP>.pins` records the window, every pin,
`data_wait_s`, the H8 key, `queries_started_at` and `finished_at`, and the
generator's state before and after. Each pin's table state (snapshot id,
commit time, `total-*` counts, and a note with `format-version=`,
`operation=` and `spec=`, the default partition spec) goes to
`tables-<STAMP>.tsv` ([Output](#output)).

**Done** looks like this in the runner log, then the watcher's teardown (it
checks every `WATCH_INTERVAL_S`, 300 s, so up to 5 min later):

```
  [querybench] done. Pins: .../hot-interleave-<STAMP>.pins  Log: ...
status: results-uploaded querybench-results/cdc-session<tag>/<STAMP>-hot/
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

Results: `s3://$BUCKET_NAME/querybench-results/cdc-session<set tag>/<STAMP>-hot/`
(set tag = `SESSION_SUFFIX` with `-` for `_`).

The completed-test teardown stops the generator, suspends the writers, stops
the job monitor, DuckDB and Trino, and then runs the one-off compaction of both
formats (`compaction/run.sh run both`; `COMPACT_AFTER_HOT=0` skips it; the
compaction itself: [../README.md, Step 10](../README.md#the-one-off-compaction-compaction)).
To keep the pipeline live for another hot run, start with
`STOP_GENERATOR=0 SUSPEND_WRITERS=0 COMPACT_AFTER_HOT=0` (they reach the watcher).

### 3. Snapshot test

Run it after the hot test and its watcher's compaction: it reads the tables
compacted and the writers stopped.

```bash
SUITE=snapshot ./querybench/run.sh start
./querybench/run.sh logs
```

`start`, on the laptop, before any Job:

1. the same runner and watcher waits as the hot test;
2. **checks the compaction** (`check_compaction_done`): waits while a
   `cdc-compact-*` FlinkDeployment runs (`COMPACT_WAIT_TIMEOUT_S`, 14400 s),
   stops on a FAILED one, and requires, for every format and table, a result JSON
   with status `OK` newer than the latest hot run (`REQUIRE_COMPACTION=0` skips);
3. **stops the writers**: scales the generator(s) in `GENERATOR_RESOURCES`
   to 0, waits `WRITER_DRAIN_S` (180 s) so both jobs read the binlog tail,
   waits until each running writer completed a checkpoint at least 60 s after
   the generator stopped (`qb_wait_writers_committed`; refuses otherwise),
   suspends both FlinkDeployments and waits (up to 600 s) until no TaskManager is left;
4. resolves `SNAPSHOT_DT`: the newest date before today (UTC) that `orders`
   and `order_items` hold in every format (`SNAPSHOT_DT_PICK=oldest`, or set
   `SNAPSHOT_DT`);
5. brings the engines up and starts the runner and the watcher (no cache
   clear here: the runner does it).

The runner (`snapshot.sh`):

1. **gates**: writers stopped; every engine up (DuckDB
   `busy: false`, Trino with `TRINO_WORKERS` active; up to
   `ENGINE_GATE_TIMEOUT_S`, 1800 s);
2. **pins** every table after the gate; a `paimon` table
   Trino cannot read is a WARNING (its queries record the error);
3. the **S4 slice** `[S_FROM, S_TO)`, untimed, on the pinned tables: each
   format's `orders` counted per `SNAPSHOT_SLICE_MINUTES` (10) bucket of
   `order_date` in the partition, and the bucket whose smallest per-format count
   is largest wins, so every format has rows in it (until 2026-11-08 the centre
   of `MIN..MAX(order_date)`, which fell between the generator's bursts and made
   S4 ZERO_SCAN on every arm);
4. **cold start**: `clear-caches.sh` (DuckDB restarted, `deploy-trino.sh clear-cache`);
5. each engine in `ENGINES` order: `SNAPSHOT_RUNS` (3) passes over its arms,
   no warm-up: pass 0 is timed as the cold pass (`cold_ms`), `median_ms` is
   the median of the rest;
6. uploads `bench-results/`.

**Done** looks like:

```
status: duckdb-snapshot-exit=0
status: trino-snapshot-exit=0
status: results-uploaded-snapshot querybench-results/cdc-session<tag>/<STAMP>-snapshot/
status: runner-exit=0 <date>
```

As in the hot test, a query that is not `OK` does not change an engine's
exit: it stays in the CSVs and the summary, and the driver's last line lists
it (`snapshot: N of M queries not OK: <engine>:<arm>:<query>=ERROR ...`). An
engine's exit is non-zero only when its driver fails (engine not up at the
start or the end of the suite, a render or pin failure, the writers changed
under the suite, no result rows); then `runner-exit` is non-zero and the
watcher leaves everything running.

`snapshot-<STAMP>.pins` records each gate time, the pins, the slice, the
cache-clear time, the pass layout and each engine's finish time and exit.

### 4. Summary

```bash
./querybench/run.sh summary
```

Syncs the session's newest hot and snapshot run from S3 (`HOT_STAMP=` /
`SNAPSHOT_STAMP=` pick others; `SUMMARY_DIR=<dir with hot/ and/or snapshot/>`
reads a local copy) and writes
`bench-results/QUERYBENCH-SUMMARY-<yyyymmdd><set tag>.md`
(`SESSION=` overrides the name). Bucket name and account id are masked.

Before `summarize.py`, `summary` runs `ingest_stats.py` (read-only boto3, on
the laptop's AWS credentials) for the Flink ingest over the run's window and
uploads its JSON to
`s3://$BUCKET_NAME/querybench-results/cdc-session<set tag>/<session>-summary/QUERYBENCH-INGEST-<session>.json`.
It runs through `uv` when installed (`uv run --with boto3 fastavro zstandard ...`:
Paimon's manifests are zstd Avro), else `python3` (then Paimon file counts are
blank). A failure there never stops the summary: the ingest section says
"Not available: <reason>". `INGEST=0` skips it.

The summary (the latest:
[QUERYBENCH-SUMMARY-20261110-v3.md](../bench-results/QUERYBENCH-SUMMARY-20261110-v3.md)):

| Section | What | From |
|---|---|---|
| Buckets | a row of the run-configuration table: N per table, in plain words (Iceberg since 2026-11-09 has the same N and key as Paimon). The write-side differences that remain are in [../PAIMON-VS-ICEBERG.md](../PAIMON-VS-ICEBERG.md#write-side-asymmetries-kept-on-purpose), not in the summary | `tables-<STAMP>.tsv` (`buckets_row` in `summarize.py`) |
| Run configuration | Open Table Format(OTF) (Iceberg and Paimon versions, how they are added to the EMR on EKS Flink image; first row), Flink parallelism / TMs / checkpoint interval, ingest window, source RDS class and storage, generator (pods × `RATE_PER_POD`, the CDC sources' change events/s), query engines, table set, buckets, runs | `ingest-deploy/flink-cdc-*-deployed.yaml`, `ingest-deploy/docker/Dockerfile.cdc-paimon`, the `.pins` files, the ingest JSON |
| 1. Headline | plain-English bullets computed from the results (which format and which query engine was faster, by how much, numbers in bold), ingest, why live Iceberg queries are slower; then **Speed** (every geometric mean: hot warm, snapshot warm, snapshot cold) and **Estimated cost** (each suite's total cost, A vs B in %) tables | computed (`plain_headline`, `geo_summary`) |
| 2. Query latency | the compaction results before the snapshot test, with TaskManagers and estimated cost (US$): job duration × pod requests × (EC2 + EMR on EKS uplift), and a total per format; per suite: median seconds per query and arm with an estimated cost (US$) column after each arm (seconds × the engine's EC2 on-demand node price; query time only) and a total row, Trino / DuckDB ratios (snapshot: the cold pass too); queries recovered as OK after a lost Trino result stream | `compaction/*.json`, the runs' CSVs, `EC2_PRICE_H` / `POD_*_H` / `EMR_*_H` in `summarize.py` |
| 3. Ingest | **Throughput**: change events/s (an UPDATE counts twice) and data size/s per format, median (max) over the job-monitor reports overlapping the window, median checkpoint duration and max backpressure share, then a plain-English **Outcome** line; "job-monitor reports not available for this window" when there are none. **From table metadata**: rows/s and MB/s (rows / bytes committed ÷ window seconds), rows, write commits and median interval, new data and delete files, equality and position deletes, compaction commits. **What each format stored**: rows saved, old versions (delete markers), data size while loading and after the full compaction, source RDS CPU, in plain words | job-monitor reports, `ingest_stats.py`, `compaction/*.json` |
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
  ingest rows/s (the CDC sources' rate meter) and committed bytes/s, median
  and max over the reports whose sampling window overlaps the ingest window,
  plus median checkpoint duration and max backpressure share. How the monitor
  computes them and where its reports go: [../3-MONITOR.md](../3-MONITOR.md).
  `SUITE=hot run.sh start` brings it up with the writers (scales it to 1, or
  runs `deploy-monitor.sh` when it is missing, set up for another table set, or
  without the S3 copy; `MONITOR_UP=0` skips), and the watcher's teardown scales
  it to 0. Runs before the S3 copy existed (e.g. 2026-11-02) have no reports,
  so the summary says so and shows the table-metadata numbers.

### 5. Stop / teardown

```bash
SUITE=hot ./querybench/run.sh stop       # = qb-watch.sh teardown, on the laptop
```

In order: generator to 0 (no drain wait), writers suspended (wait for the
TaskManagers to go), `flink-cdc-monitor` to 0, `duckdb-warm-cdc` deleted,
Trino's coordinator and workers scaled to 0 (the Helm release stays), the
runner's ConfigMap and Secret deleted. `STOP_GENERATOR=0`, `SUSPEND_WRITERS=0`,
`STOP_DUCKDB=0`, `STOP_TRINO=0` each keep one running. `SUITE` only labels
the log. `stop` never runs the compaction. Finished Jobs are deleted by their
TTL (1 day) or the next `start`.

Remove Trino completely: `./engine-deploy/deploy-trino.sh down`.

Bring the pipeline back after a teardown or a snapshot test:

```bash
for j in flink-cdc-paimon flink-cdc-iceberg; do
  kubectl patch flinkdeployment "$j" -n emr-flink --type=merge -p '{"spec":{"job":{"state":"running"}}}'
done                                      # resumes from the last checkpoint; never re-apply them
kubectl scale deploy/flink-cdc-monitor -n emr-flink --replicas=1
./mysql-data-generator/deploy-data-generator.sh scale 24
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

Issues met on the cluster runs (from 2026-11-02), and the refusals the
scripts print. Pipeline-side symptoms: [../README.md, Troubleshooting](../README.md#troubleshooting).

| Symptom | Cause | Fix |
|---|---|---|
| Runner ends with a non-zero `runner-exit` and no ERROR line | A command failing under `set -e` ended the script before it logged anything | Fixed: the runner step and `snapshot.sh` run with `set -uo pipefail` and record each engine's exit (`status: <engine>-snapshot-exit=N`); the other scripts stop through `qb_die` with a message |
| Trino rejects a query that DuckDB runs, e.g. `Cannot apply operator: timestamp(6) >= varchar(19)` | Dialect: no implicit VARCHAR → TIMESTAMP coercion, no byte-length function on VARCHAR, table alias only after `FOR VERSION AS OF` | Fixed in the SQL: `TIMESTAMP '…'` literals, the `BYTE_LEN_OPEN`/`BYTE_LEN_CLOSE` dialect keys (`dialect-*.env`), aliases after the placeholder. Rules: [hot/QUERIES.md](hot/QUERIES.md) "Writing for both engines". Check a change offline with `run.sh render` |
| A Trino query recorded OK with "FINISHED, result stream lost" | Trino finished the query; only the CLI's result stream back through `kubectl exec` broke (e.g. "connection reset by peer", H8 on `_v3final`, 2026-11-07) | Nothing to fix: `trino.sh` takes the server-side time from `system.runtime.queries` and records `OK` with the rows lost; that pass's rows are not compared, and the summary lists the query as recovered |
| `duckdb:paimon` queries all `ERROR`: `Manifest ... missing required Avro key-value metadata field 'schema'` (runs before 2026-11-07) | Stock Paimon 2.1 writes its Iceberg v3 manifests without the `schema` key DuckDB 2.0 requires | The writer image's patched `paimon-flink-1.20` (apache/paimon PR #10329 backport, [patched-jars/README.md](../ingest-deploy/docker/patched-jars/README.md)) writes it; a table set written by an older image needs a new `SESSION_SUFFIX`. Without it the arm records `ERROR`, is not retried and is never counted as a mismatch; it does not change the exit |
| `404` / `NoSuchKey` on a Paimon-as-iceberg `vN.metadata.json` mid-scan | Paimon's default kept no previous Iceberg metadata, so the path read from `version-hint.text` was gone within a checkpoint | Pinned copies (`pin.sh` copies every metadata.json to `querybench-pins/`, 3 attempts), and the Paimon sinks set `metadata.iceberg.previous-versions-max = 100` + `delete-after-commit.enabled` (3 until 2026-11-04). Table options apply to new tables only: a fresh `SESSION_SUFFIX` |
| `ENGINES must name duckdb and/or trino, got 'starrocks'` | StarRocks was removed from this harness (4.1.7 reads neither the v3 Paimon tables nor Iceberg v3 deletion vectors) | Use `duckdb` / `trino`. Its scripts and results stay in git history; `starrocks-sa` keeps its name |
| `throughput-benchmark's qb-runner/qb-watch is active` | Same cluster, bucket and nodepools | Wait, or `FORCE=1` |
| `Trino runs <v>, not TRINO_VERSION=484` | A different Trino is up | `./engine-deploy/deploy-trino.sh up`, or `TRINO_VERSION_CHECK=0` |
| `compaction FAILED: ...` / `compaction still running after ...` / a format/table with no OK run after the hot run | The hot watcher's compaction failed, is still running or never ran | `./compaction/run.sh logs <fmt> <table>`; fix, `./compaction/run.sh down`, re-run (README Step 10); `REQUIRE_COMPACTION=0` measures the tables as they are |
| `no date before <today> in the orders and order_items partitions of every format` | No closed partition in both formats | `SNAPSHOT_DT=YYYY-MM-DD` |
| `snapshot slice: no 10-min bucket holds rows in every format` / `no snapshot slice histogram on any format` | No bucket of the partition has rows in every format, or no engine could count them | Lower `SNAPSHOT_SLICE_MINUTES`, set `S_FROM`/`S_TO`, or `SNAPSHOT_SLICE_MINUTES=0` (whole day) |
| `the queried writers (...) are not all RUNNING after 10 checks` | A writer is suspended or failing | Resume it (§5); `kubectl get flinkdeployment -n emr-flink` |
| `not committed past HOT_TO+60s after 1800s` | A writer is not checkpointing | Check the writer's checkpoints; `WAIT_COMMIT_S` raises the wait |
| `no data at or past HOT_TO=... after 3600s` | A writer is still behind on the binlog | Check its source fetch lag in the monitor ([../3-MONITOR.md](../3-MONITOR.md)); `HOT_DATA_WAIT_S` raises the wait, `0` skips the check |
| `no H8 candidate on <engine>:<arm>` | No older order updated in the window | Check the generator; or set `LOOKUP_ORDER_ID` and `LOOKUP_ORDER_DT` |
| `Trino cannot read <ICEBERG_DB>.<t> at its pinned snapshot` | Catalog or IRSA problem (a Paimon table is only a WARNING) | `./engine-deploy/deploy-trino.sh sql "SELECT 1 FROM iceberg.<db>.<t> LIMIT 1"` |
| `duckdb-warm-cdc is not ready and idle` | Server down, starting, or busy | `./engine-deploy/deploy-duckdb-cdc.sh status` |

## The four arms

`ENGINES` × `FORMATS`, by default `duckdb trino` × `iceberg paimon`: two arms
per engine, Iceberg and Paimon-as-iceberg. `ENGINES=duckdb|trino` and
`FORMATS` (`iceberg` and/or `paimon`, space- or comma-separated; `both` and
`all` mean both; any other value, a removed one included, stops the run) select a
subset for either suite; `ENGINES` order is the run order. `paimon` reads the
Paimon tables through their Iceberg v3 metadata (Paimon-as-iceberg), with the
same Iceberg readers as `iceberg`. `lib/querybench.sh` (`_qb_resolve_formats`,
`qb_table_expr`) is the reference.

| Arm | Engine | Data | Table expression at the pin (`qb_table_expr`) |
|---|---|---|---|
| `duckdb:iceberg` | DuckDB 2.0.0 warm server `duckdb-warm-cdc` | Iceberg tables, `flink_iceberg_db<suffix>` | `iceberg_scan('<copy of the pinned metadata.json>')` |
| `trino:iceberg` | Trino 484, catalog `iceberg` (Glue) | the same | `iceberg.<ICEBERG_DB>.<t> FOR VERSION AS OF BIGINT '<snapshot id>'` |
| `duckdb:paimon` | DuckDB, `iceberg_scan` with `iceberg_unsafe_skip_puffin_verification` (Paimon's DV files are not real Puffin containers) | Paimon-as-iceberg, `paimon-warehouse/iceberg/<PAIMON_DB>/<t>` (`PMNICE_ROOT`) | `iceberg_scan('<copy of the pinned v<N>.metadata.json>')` |
| `trino:paimon` | Trino 484, catalog `iceberg` (the Paimon tables' Glue entries, `table_type=ICEBERG`) | the same | `iceberg.<PAIMON_DB>.<t> FOR VERSION AS OF BIGINT '<N>'` |

History: native Paimon reading (`paimon_native`, DuckDB paimon_scan / Trino's
paimon connector; the `paimon` arm also read that way from 2026-11-04 to
2026-11-06 under `PAIMON_READER=native`) was tried on _v3, 2026-11-08: the same
rows, 4–29× slower than Paimon-as-iceberg (no file skipping, no DuckDB cache).
Removed 2026-11-09; earlier summaries say which reader they used.

- **One pin per format, both engines.** `pin.sh` copies each Iceberg table's
  current metadata.json to `s3://$BUCKET_NAME/querybench-pins/...` and reads its
  `current-snapshot-id`. For Paimon-as-iceberg it copies
  `<PMNICE_ROOT>/<t>/metadata/v<N>.metadata.json` (`N` from `version-hint.text`)
  the same way; `N` is Paimon's snapshot id, which the Iceberg metadata reuses.
- **Validity.** Both engines on one format read the same snapshot, so any row
  difference on a query both ran OK makes the suite **INVALID**. Iceberg vs
  Paimon differences are a **pipeline gap**, not a failure: the two writers
  are pinned at the same moment, not the same binlog point.
- **An arm that cannot read is a result.** Its queries record `ERROR`, are not
  run again and are not compared. A pinned Paimon table Trino cannot read is a
  WARNING; a pinned Iceberg table it cannot read stops the run.
- **Paimon-as-iceberg coverage.** On v3 the Iceberg metadata lists every Paimon
  file above LSM level 0 plus its deletion vectors, so it trails the Paimon table by
  about one checkpoint (with `PAIMON_WRITE_ONLY=false`, the default; write-only
  leaves new data in level 0, invisible to it). After the full compaction it is
  complete. On a v2 set it lists only top-level files (no DVs), a stale view
  until a full compaction; the summaries of runs before 2026-11-04 say so.
Table sets (`.bench-env`):

| Set | `ICEBERG_FORMAT_VERSION` | `SESSION_SUFFIX` | Glue databases | Results prefix / summary |
|---|---|---|---|---|
| any round | `3` (or `2`) | `<suffix>`, free text | `flink_iceberg_db<suffix>`, `flink_paimon_db<suffix>` | `cdc-session<tag>/`, `QUERYBENCH-SUMMARY-<date><tag>.md` |
| unsuffixed | `3` (or `2`) | empty | `flink_iceberg_db`, `flink_paimon_db` | `cdc-session/`, `QUERYBENCH-SUMMARY-<date>.md` |

`ICEBERG_FORMAT_VERSION=2 SESSION_SUFFIX= ./querybench/run.sh ...` runs the v2
set. Every `.pins` file and the summary header record the set, engines,
formats, engine versions and DuckDB's puffin setting; `pin.sh` warns when a
pinned metadata.json's `format-version` differs from `ICEBERG_FORMAT_VERSION`.

## Table settings and sizing parity

The sink DDLs are paired setting by setting
([../1-BUILD-DEPLOY-FLINK-APP-GUIDE.md, Sink table settings](../1-BUILD-DEPLOY-FLINK-APP-GUIDE.md#sink-table-settings-paimon-vs-iceberg)).
The write-side differences kept on purpose (writer parallelism, compaction,
snapshot-expiry timing; state them with every result) are in
[../PAIMON-VS-ICEBERG.md](../PAIMON-VS-ICEBERG.md#write-side-asymmetries-kept-on-purpose).

**Buckets (table sets created since 2026-11-09).** The Iceberg tables carry
`bucket(N, <id>)` partition fields with Paimon's `N` and bucket key: customers
`bucket(8, customer_id)`; products `category` + `bucket(8, product_id)`; orders
`order_dt` + `bucket(16, order_id)`; order_items `created_dt` +
`bucket(16, order_item_id)`. Flink DDL cannot declare them, so
`ingest-deploy/flink-cdc-executor.py` adds them through the Iceberg API from the
`'bench.bucket'` / `'bench.bucket-key'` properties in
`ingest-deploy/sql-scripts/iceberg/03-iceberg-sinks.sql`, only on an empty
table: a table set created before keeps its unbucketed spec (a new
`SESSION_SUFFIX` gets the buckets). Hash distribution then sends each bucket of a
partition to one writer, as Paimon does; until 2026-11-09 it sent each date
partition to ONE writer. Which spec a run read is in `tables-<STAMP>.tsv`
(`spec=` in the note column).

The engines have equal CPU and memory (about 81 cpu / 342 GiB each, from
`.bench-env`) and about 450 GiB of disk cache each; Trino's extra coordinator,
DuckDB's memory cache and the different cache page sizes are not equal. The
numbers: [../engine-deploy/README.md](../engine-deploy/README.md).

## Cold start and warm passes

- **Snapshot:** after the gates, pins and slice, `clear-caches.sh` restarts
  `duckdb-warm-cdc` (a new pod: new cache volume, empty memory caches;
  `/clear_cache` would miss the Parquet-footer and HTTP-metadata caches) and
  runs `deploy-trino.sh clear-cache` (empties `/cache`, restarts workers and
  coordinator on fresh volumes, waits for `TRINO_WORKERS`). The engines then
  run one after the other, each `SNAPSHOT_RUNS` passes, pass-major, the first
  timed as cold. Neither shares a cache, so the second also starts cold.
- **Hot:** `start` clears both caches the same way (skipped when every engine
  was just started); p0 is the warm-up, p1..pRUNS are timed.
- **Setup reads** (S4 slice histogram, H8 key) are untimed, on `QB_SETUP_ENGINE`
  (Trino when it runs, else DuckDB): before the cold start (snapshot), before
  the warm-up pass (hot).

## Files

| File | Role |
|---|---|
| `run.sh` | Laptop entry point: `start`, `render`, `status`, `logs [watch]`, `summary`, `stop` |
| `hot-interleave.sh` | Hot runner step: writers RUNNING, window, commit and data waits, pins per pass, interleaved engines, medians |
| `snapshot.sh` | Snapshot runner step: gates, pins, slice, cold start, each engine's passes, upload |
| `duckdb-warm.sh` | DuckDB driver (`run`, `render <suite> [arm]`): POSTs each query to the server; detects a server restart (CRASH); untimed scan-bytes profile pass (`PROFILE_SCAN`) |
| `trino.sh` | Trino driver (`run`, `render <suite> [arm]`), below |
| `pin.sh` | Pins every table of each format, appends `tables-<STAMP>.tsv`, checks Trino reads every pin |
| `clear-caches.sh` | The cold start; refuses while either engine is busy (`ALLOW_CONCURRENT=1` skips) |
| `qb-runner.yaml` | Runner / watcher Job: namespace Role, IRSA on `EMR_EXECUTION_ROLE_ARN`, `python:3.12-slim` + kubectl + awscli, `backoffLimit: 0`, 1-day deadline and TTL |
| `qb-watch.sh` | Watcher (`cdc-qb-watch`): teardown after a completed test, then the compaction; `teardown` (= `run.sh stop`) |
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
- **Lost result stream:** when the server says `FINISHED` but the CLI exits
  non-zero (the stream back through `kubectl exec` broke), the query is `OK`
  at its server-side time, with its rows lost (not compared for that pass).
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
| `ENGINES` / `FORMATS` | `duckdb trino` / `iceberg paimon` | the arms; `ENGINES` order = run order |
| `RUNS` / `WARMUP` | `3` / `1` | hot: timed passes / warm-up pass p0 |
| `SNAPSHOT_RUNS` | `3` | snapshot passes per engine, the first timed as cold |
| `STAMP` | now (UTC) | run id |
| `QB_SETUP_ENGINE` | `trino` if in `ENGINES`, else `duckdb` | engine of the untimed setup reads |
| `PMNICE_ROOT` | `<PAIMON_WAREHOUSE>/iceberg/<PAIMON_DB>` | the root of Paimon's Iceberg metadata (`<root>/<t>/metadata/`) |
| `SKIP_PUFFIN_VERIFICATION_ARMS` | `paimon` (`qb_skip_puffin_default`) | DuckDB arms with `iceberg_unsafe_skip_puffin_verification`; applied at `deploy-duckdb-cdc.sh up` (`run.sh start` redeploys a server whose arms differ) |
| `DUCKDB_IMAGE` | ECR `duckdb-cdc-server:2.0.0` | empty = PyPI `duckdb==2.0.0` |
| `TRINO_VERSION` / `TRINO_VERSION_CHECK` | `484` / `1` | version `start` requires of a running Trino |
| `TRINO_WORKERS` / `TRINO_USER` / `TRINO_CATALOG` | `.bench-env` `9` (script `3`) / `cdcbench` / `iceberg` | readiness, CLI user, catalog |
| `CLEAR_COORDINATOR` | `1` | `0`: `clear-cache` keeps the coordinator |
| `HOT_WINDOW_SECONDS` / `HOT_LAG_SECONDS` | `30` / `300` | hot window (30 s since 2026-11-02, as the throughput benchmark; was 300); `HOT_TO`, `HOT_FROM`, `HOT_DT` pin it |
| `HOT_COMMIT_TABLES` / `HOT_COMMIT_MARGIN_S` / `WAIT_COMMIT_S` | `orders order_items` / `60` / `1800` | commit-past-the-window wait |
| `HOT_DATA_WAIT_S` | `3600` | data-past-the-window wait on the pins (re-pin every 60 s); `0` skips |
| `STABLE_CHECKS` | `10` | writer RUNNING checks, 60 s apart |
| `HOT_REPIN` | `pass` | `run` = one pin for every pass (keep passes well under ~10 min) |
| `QB_QUERY_WRITERS` | `flink-cdc-<format>` per table set in `FORMATS` | writers the hot test needs RUNNING |
| `LOOKUP_ORDER_ID` / `LOOKUP_ORDER_DT` | picked | H8 key |
| `FK_PARENT_MOD` | `16` | H8 picks only `order_id % FK_PARENT_MOD = 0`, the orders the generator never deletes; keep it equal to the generator's `FK_PARENT_MOD` |
| `SNAPSHOT_DT` / `SNAPSHOT_DT_PICK` | newest closed / `newest` | snapshot partition |
| `SNAPSHOT_SLICE_MINUTES` / `SNAPSHOT_SLICE_COLUMNS` | `10` / `orders:order_date` | S4 slice: the busiest `SNAPSHOT_SLICE_MINUTES` bucket of `order_date` in the partition that every format has rows in (`0` = whole day); `S_FROM`, `S_TO` pin it |
| `REQUIRE_COMPACTION` / `COMPACT_WAIT_TIMEOUT_S` | `1` / `14400` | snapshot `start`: require the hot watcher's compaction, and how long to wait for a running one |
| `COMPACT_AFTER_HOT` | `1` | hot watcher: run `compaction/run.sh run both` after the teardown. `COMPACT_TABLES`, `COMPACT_PARALLELISM`, `COMPACT_TIMEOUT_S`, `ICEBERG_MAX_GROUP_DELETE_KEYS`, `ICEBERG_BIG_TM_TABLES`, `ICEBERG_BIG_TM_MEMORY` reach it too (README Step 10) |
| `WRITER_DRAIN_S` | `180` | generator stop → writer suspend (snapshot `start`) |
| `ENGINE_GATE_TIMEOUT_S` | `1800` | runner waits for the engines |
| `HOT_QUERY_TIMEOUT_S` / `SNAPSHOT_QUERY_TIMEOUT_S` | `1800` / `3600` | per query, both engines |
| `MAX_RESULT_ROWS` | `10000` | rows kept per query for the comparison |
| `PROFILE_SCAN` | `1` | DuckDB's untimed scan-bytes pass (hot: last pass only) |
| `WATCH_INTERVAL_S` | `300` | watcher poll |
| `STOP_GENERATOR` / `SUSPEND_WRITERS` / `STOP_DUCKDB` / `STOP_TRINO` | `1` | what the teardown stops |
| `WRITERS_UP_S` / `MONITOR_UP` | `1800` / `1` | a hot `start` waits until the writers have been RUNNING this long; brings the job monitor up |
| `FORCE` / `ALLOW_CONCURRENT` | off | replace an active runner, ignore throughput's Jobs / skip the one-test-at-a-time guard |
| `RENDER_DIR` | temp dir | `render` output |
| `HOT_STAMP` / `SNAPSHOT_STAMP` / `SUMMARY_DIR` / `SESSION` / `INGEST` | newest / S3 / derived / `1` | `summary` inputs, name, ingest step |
| DuckDB / Trino sizing | | [../engine-deploy/README.md](../engine-deploy/README.md) |

## Output

In `s3://$BUCKET_NAME/querybench-results/cdc-session<set tag>/<STAMP>-<suite>/`:

| File | Content |
|---|---|
| `duckdb_warm-<STAMP>.csv`, `trino-<STAMP>.csv` | `stamp,engine,arm,suite,query,status,median_ms,result_rows,scan_rows,scan_bytes`; status `OK`, `ERROR`, `MISSING`, `TIMEOUT`, `ZERO_SCAN`, `CRASH` (DuckDB restarted under the query), `OOM` (Trino) |
| `trino-<STAMP>-queries.csv` | every Trino pass: query id, elapsed, queued, execution, CPU, physical input, error |
| `*-rows.tsv`, `*-firstrows.csv` | result rows of the last timed pass |
| `duckdb_warm-<STAMP>-events.csv`, `*.log` | DuckDB restarts and cold queries; every pass |
| `tables-<STAMP>.tsv` | pinned snapshots' state per pin tag × format × table: `pin_tag`, `format`, `table`, `snapshot_id`, `commit_ms`, the snapshot's `total-*` counts, and `note` (`format-version=N operation=<op> spec=<default partition spec>`, e.g. `spec=order_dt,bucket[16](order_id)`, `none` when unpartitioned; since 2026-11-09), plus any pin warning |
| `hot-interleave-<STAMP>.pins` / `snapshot-<STAMP>.pins` | windows, pins, H8 key, gates, writer and generator state, `queries_started_at` (hot) and `finished_at` |
| hot: `*-p<N>.csv`, `hot-interleave-<STAMP>.log` | every pass (p0 = warm-up) |

Compaction results: `s3://$BUCKET_NAME/querybench-results/cdc-session<set tag>/compaction/<STAMP>-<fmt>-<table>.json`.
Job-monitor reports: `.../cdc-session<set tag>/monitor/`. Pinned metadata
copies: `s3://$BUCKET_NAME/querybench-pins/cdc-session<set tag>/<tag>/<format>/<table>/`.

## Caveats

1. **Runs before 2026-11-07 read Paimon differently** (history note in
   [The four arms](#the-four-arms); before 2026-11-04 through the Iceberg
   metadata, on v2 a top-level-only view); their summaries say which.
2. **Iceberg layout depends on the table set.** Sets created before 2026-11-09
   have no bucket field (one writer per date partition, no bucket pruning on
   id lookups such as H8); a comparison across sets with a different `spec=` in
   `tables-<STAMP>.tsv` must say so.
3. **The hot window (30 s) and the S4 slice (10 min)** are sized from earlier
   runs' H1 and S4 rows; recheck them when the generator rate changes.
4. **The coordinator runs the CLI** (≤ 512 MB heap) during the timed window,
   off Trino's split path.
