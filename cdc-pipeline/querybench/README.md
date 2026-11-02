# CDC query benchmark: DuckDB vs Trino on Iceberg and on Paimon's Iceberg export

The read half of the CDC pipeline benchmark: the same queries, on two query
engines, over the four tables the two Flink CDC jobs write (`customers`,
`products`, `orders`, `order_items`; MySQL `ecommerce` → `flink-cdc-paimon` /
`flink-cdc-iceberg`). Flink is the only writer. DuckDB and Trino only read.
The harness follows
[../../throughput-benchmark/querybench/](../../throughput-benchmark/querybench/README.md),
and the throughput benchmark is not changed by any of it.

> **Not run on the cluster yet in this form.** The scripts were checked with
> `bash -n`, `py_compile`, a dry-run render, and offline stubs (see "What is
> verified"). Every time and duration below is an estimate.

| Suite | Reads | Writers | Queries |
|---|---|---|---|
| **hot** | a 5-min window (`HOT_WINDOW_SECONDS`) ending 5 min ago: `orders.updated_at`, `order_items.created_at` | running, generator live | H1–H9 ([hot/QUERIES.md](hot/QUERIES.md)) |
| **snapshot** | one closed partition (`SNAPSHOT_DT`) of the fact tables, the whole dimension tables; S4 reads a 10-min `orders.order_date` slice | stopped, Iceberg compacted | S1–S7 ([snapshot/QUERIES.md](snapshot/QUERIES.md)) |

## The four arms

`ENGINES` × `FORMATS`. Both default to both, so four arms by default.
`ENGINES=duckdb|trino` and `FORMATS=iceberg|paimon` select a subset, for either suite.

| Arm | Engine | Data | Table expression at the pin (`lib/querybench.sh` `qb_table_expr`) |
|---|---|---|---|
| `duckdb:iceberg` | DuckDB 2.0.0 warm server `duckdb-warm-cdc` | Iceberg tables, `flink_iceberg_db<suffix>` | `iceberg_scan('<copy of the pinned metadata.json>')` |
| `trino:iceberg` | Trino 484, catalog `iceberg` (Glue) | the same | `iceberg.<ICEBERG_DB>.<t> FOR VERSION AS OF <snapshot id>` |
| `duckdb:paimon` | DuckDB | Paimon's Iceberg export (`paimon-warehouse/iceberg/<PAIMON_DB>/<t>/metadata/vN.metadata.json`) | `iceberg_scan('<copy of the pinned vN.metadata.json>')`, with `iceberg_unsafe_skip_puffin_verification = true` on v3 |
| `trino:paimon` | Trino, the same Iceberg connector | the same export. The hive-catalog export registers each table in the Paimon Glue DB as `table_type=ICEBERG` | `iceberg.<PAIMON_DB>.<t> FOR VERSION AS OF <export snapshot id>` |

How the arms relate:

- **One pin per format, both engines.** `pin.sh` copies each table's current
  `metadata.json` to `s3://$BUCKET_NAME/querybench-pins/...` and reads its
  snapshot id. DuckDB reads the copy, and Trino reads the same snapshot by id.
  For Iceberg the id is `current-snapshot-id`. For Paimon's export it is `N`
  from `version-hint.text` → `vN.metadata.json`, which is Paimon's own snapshot
  id reused by the export. `pin.sh` checks that `N` is in that metadata's
  `snapshots`; if it is not, it uses `current-snapshot-id` and logs a WARNING,
  which also lands in the tables TSV.
- **Validity.** The two engines on one format read the same snapshot, so they
  must return the same rows. Any difference on a query both ran OK makes the
  suite **INVALID** in the summary. Iceberg vs Paimon differences are a
  **pipeline gap** and are not counted as a failure: the two writers
  checkpoint at different moments, the formats are pinned at the same time but
  not at the same binlog point, and the export lags native Paimon by about one
  checkpoint (it lists only top-LSM-level files until a full compaction).
- **An arm that cannot read is a result, not an abort.** On the v3 set
  `duckdb:paimon` currently fails with `Manifest ... missing required Avro
  key-value metadata field 'schema'`, a Paimon 2.1 export bug. Its queries are
  recorded as `ERROR` in the CSV, are not run again in later passes, and are
  never counted as mismatches. The other arms run as usual. If Trino cannot
  read a pinned Paimon table, `pin.sh` logs a WARNING and those queries record
  the error. A pinned Iceberg table that Trino cannot read stops the run.
- **No native Paimon reader.** Trino 484 has no Paimon connector
  (apache/paimon-trino targets 476 and has no release), and DuckDB has none.
  Both Paimon arms therefore measure the export through an Iceberg reader, not
  Paimon's LSM read path.

Table sets, switched in `cdc-pipeline/.bench-env` (this branch defaults to v3):

| Set | `ICEBERG_FORMAT_VERSION` | `CDC_DB_SUFFIX` | Glue databases | Results / pins prefix, summary |
|---|---|---|---|---|
| **v3** (default) | `3` | `_v3` | `flink_iceberg_db_v3`, `flink_paimon_db_v3` | `cdc-session-v3<SESSION_SUFFIX>/`, `QUERYBENCH-SUMMARY-<date>-v3<SESSION_SUFFIX>.md` |
| v2 | `2` | empty | `flink_iceberg_db`, `flink_paimon_db` | `cdc-session<SESSION_SUFFIX>/`, `QUERYBENCH-SUMMARY-<date><SESSION_SUFFIX>.md` |

`ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX= ./querybench/run.sh ...` runs the v2
set. Every `.pins` file and the summary header record the set, the engines,
the formats, the engine versions and DuckDB's puffin setting. `pin.sh` also
records each pinned metadata.json's own `format-version`, and logs a WARNING
when it does not match `ICEBERG_FORMAT_VERSION`.

## Table settings parity (and the differences kept on purpose)

The two sink DDLs (`sql-scripts/{paimon,iceberg}/03-*-sinks.sql`) are paired setting
by setting: format version 3, primary keys and partitioning, zstd level 1, row-group
and target file sizes, manifest size and merge count (5), file statistics
(`truncate(16)`), snapshot retention (12 h, at least 5 kept; Paimon's count cap 1000
so the age rule binds), and 3 previous `metadata.json` files kept (Paimon's export too).
Paimon writes no changelog (`changelog-producer = none`), and the compaction job
expires Iceberg snapshots with Paimon's rule. New settings apply to newly created
tables only, i.e. a fresh `CDC_DB_SUFFIX`.

Differences kept on purpose; state them with every result:

- **Writer parallelism:** Paimon writes `orders`/`order_items` with 32 writers (one
  per bucket); Iceberg's `hash` distribution on a date-partitioned upsert table sends
  each date partition to ONE writer, so today's changes go through a single writer.
  Both stay date-partitioned (same layout and query pruning). At a source-limited
  rate (~52K rows/s in the 2026-11-02 dry run) both keep up; above that, Iceberg's
  single writer is the expected ceiling.
- **Compaction:** Paimon compacts inline (lookup compaction, deletion vectors);
  Iceberg only when the external compaction job runs before the snapshot test.
- **Snapshot expiry timing:** Paimon expires snapshots continuously in the writer;
  Iceberg only when the compaction job runs.

## Sizing parity

DuckDB runs on one node. Trino spreads the same CPU and memory over three
workers, plus a coordinator that runs no splits.

| | DuckDB `duckdb-warm-cdc` | Trino (`engine-deploy/deploy-trino.sh`) |
|---|---|---|
| query compute | 1 pod: **27 cpu / 114 GiB** (m5.8xlarge), requests = limits | **3 workers × 9 cpu / 38 GiB = 27 cpu / 114 GiB** (one m5.4xlarge each), requests = limits |
| control plane | in the same process | coordinator **4 cpu / 16 GiB** (m5.2xlarge), `node-scheduler.include-coordinator=false`. This is on top of the 27 cpu |
| query memory | `memory_limit` 84049 MiB (82 GiB) | `query.max-memory` 3 × 19.8 GiB = 59.3 GiB. Heap is 80 % of each container, max-memory-per-node 65 % of heap, headroom 30 % |
| disk cache | cache_httpfs, 450 GiB on gp3-fast, 256 KiB blocks | file system cache (alluxio), 3 × 167 Gi gp3-fast × 90 % ≈ 450 GiB, 1 MB pages (`TRINO_CACHE_PAGE_SIZE` can be set to 256kB to match) |
| memory cache | 22 GB cache_httpfs memory cache, plus the buffer pool | none; only the OS page cache in the workers' ~7.6 GiB of non-heap memory |
| placement | `driver-nodepool`, m5, amd64, on-demand, `do-not-disrupt` | the same |
| ServiceAccount | `starrocks-sa` (IRSA) | the same (`TRINO_SERVICE_ACCOUNT`) |

The CPU and memory are equal. Two things are not, and the summary should be
read with them in mind: Trino has 4 extra coordinator cpu for planning and
result delivery, and DuckDB has a 22 GB memory cache that Trino has no
counterpart for. The disk caches are the same size, but their page sizes
differ (1 MB vs 256 KiB).

## Cold start and warm passes

- **Snapshot**: right before the first warm-up pass, after the gates, the pins
  and the slice, `clear-caches.sh` restarts `duckdb-warm-cdc`. A new pod gets
  a new cache volume and empty memory caches; the server's `/clear_cache`
  would miss the Parquet-footer and HTTP-metadata caches. It then runs
  `engine-deploy/deploy-trino.sh clear-cache`, which empties `/cache` on every
  Trino pod and rollout-restarts the workers and the coordinator onto fresh
  EBS volumes with a cold JVM and cold Glue metadata. It waits until all
  `TRINO_WORKERS` are active, about 1–3 min. The engines then run one after
  the other (`ENGINES` order, default DuckDB then Trino). Each arm does
  `WARMUP` (1) discarded passes, then `RUNS` (3) timed passes, pass-major. No
  engine shares a cache with the other, so the second one also starts cold.
- **Hot**: `run.sh start` empties both caches the same way before the runner
  starts. It skips this when every engine was just started fresh. Each pass
  then runs every engine on the same pins: p0 is the warm-up, p1..pRUNS are
  timed.
- **Setup reads** (the S4 slice span, the H8 key) are untimed and run on
  `QB_SETUP_ENGINE`: Trino when it is in `ENGINES`, otherwise DuckDB. In the
  snapshot suite they happen before the cold start. In the hot suite the H8
  pick runs before the warm-up pass, which the summary discards.

## How each piece works

| File | Role |
|---|---|
| `run.sh` | Entry point on the laptop: `render`, `start`, `status`, `logs`, `summary`, `stop` (the watcher's teardown, by hand). `start` stops the writers (snapshot), submits the compaction, brings the engines up, then starts the runner and watcher Jobs |
| `hot-interleave.sh` | Runner step for the hot suite: writers RUNNING, window, commit-past-window wait, pins per pass, then the engines interleaved per pass, then the medians |
| `snapshot.sh` | Runner step for the snapshot suite: gates (writers stopped, Iceberg compaction completed, engines up), pin, slice, cold start, then each engine's passes, then upload |
| `duckdb-warm.sh` | DuckDB driver. It POSTs each rendered query to the server, one pass at a time. It detects a server restart under a query (CRASH), and runs an untimed scan-bytes profile pass |
| `trino.sh` | Trino driver (below) |
| `pin.sh` | Pins both formats (lib `qb_pin_tables`), writes `tables-<STAMP>.tsv`, and checks Trino reads every pin |
| `clear-caches.sh` | The cold start: DuckDB restart, then `deploy-trino.sh clear-cache` |
| `qb-runner.yaml` | Runner and watcher Job: a namespace Role and IRSA; the bundle is `querybench/`, `lib/`, `engine-deploy/`, `.bench-env` |
| `qb-watch.sh` | After a COMPLETED test (`runner-exit=0`): generator to 0, writers suspended, monitor to 0, `duckdb-warm-cdc` deleted, Trino scaled to 0. After a failure: nothing is stopped; it reports the runner's errors and what is still running. `qb-watch.sh teardown` (= `run.sh stop`) runs the teardown now |
| `summarize.py` | The Markdown summary (below) |
| `dialect-duckdb.env`, `dialect-trino.env` | Per-engine SQL fragments, `KEY=VALUE` (e.g. `BYTE_LEN_OPEN=STRLEN(` / `LENGTH(TO_UTF8(`) |
| `../lib/querybench.sh` | Everything the drivers share: arms, pins, windows, rendering, timeouts, median, CSV format, gates, writer and generator control |

### The Trino driver (`trino.sh`)

- **The CLI runs in the coordinator pod.** Each query is
  `kubectl exec deploy/trino-coordinator -c trino-coordinator -- trino --server http://localhost:8080 ... --execute "<SQL>"`.
  The coordinator image ships the CLI at the server's own version, with the
  JDK it needs, so the runner pod (`python:3.12-slim`) installs nothing and the
  CLI cannot drift from the server. Installing a JDK and the CLI jar in the
  runner would add a second Java toolchain and a download on every start, and
  could pick up a CLI that does not match the server. kubectl exec costs
  seconds per call, so no time is taken on the client. The CLI's own JVM is
  capped at `-Xmx512m` (`TRINO_CLI_JAVA_OPTS`), well inside the coordinator's
  non-heap memory.
- **Tagging.** Each query, pass and arm gets its own
  `--source qb-<run>-<suite>-<arm>-p<pass>-q<n>`, plus
  `--client-tags querybench,cdc,<suite>,<query>`.
- **Server-side time.** After the CLI returns, its row in
  `system.runtime.queries` (`WHERE source = '<tag>'`) gives `queued_time_ms`
  and elapsed = `to_milliseconds("end" - created)`. The recorded time is
  **execution = elapsed − queued**: analysis, planning, execution and
  delivering the result, which matches DuckDB's execute + fetch. CPU
  (`totalCpuTime`) and physical input bytes and rows come from
  `/v1/query/<id>` (`queryStats`). It tries the Service `trino:8080` from the
  runner first, then `curl` in the coordinator pod; this is best effort and
  left blank if neither answers. Finished queries stay in that history for at
  least 60 min.
- **Timeout.** The session sets `query_max_run_time` to
  `HOT_QUERY_TIMEOUT_S` (1800) or `SNAPSHOT_QUERY_TIMEOUT_S` (3600), the same
  cap DuckDB gets. A client deadline 300 s later kills the query
  (`system.runtime.kill_query`). `EXCEEDED_TIME_LIMIT` is recorded as
  `TIMEOUT`, a memory error as `OOM`, anything else as `ERROR`.
- **Rows.** `--output-format JSON`, read with duplicate column names kept.
  Each value is printed as the DuckDB server prints it (`|`-joined, `NULL`,
  Python `str()`), so the summary can compare every row across arms.
- **ZERO_SCAN guard and CSV.** The guard is the same as DuckDB's: an empty
  result, or a leading `0` / `NULL` unless the query is marked
  `leading-col-is-label` or `allow-zero-rows`. The CSV schema is the same as
  DuckDB's, plus `trino-<STAMP>-queries.csv` with every pass's breakdown.

## Prerequisites

1. **The CDC pipeline is deployed and has data**: both FlinkDeployments
   ([../1-BUILD-DEPLOY-FLINK-APP-GUIDE.md](../1-BUILD-DEPLOY-FLINK-APP-GUIDE.md))
   and, for the hot test, the upsert generator
   (`WORKLOAD=upsert ../mysql-data-generator/deploy-data-generator.sh deploy`).
2. **Paimon's Iceberg export is on** (`metadata.iceberg.storage = hive-catalog`
   in `sql-scripts/paimon/03-paimon-sinks.sql`). It writes
   `paimon-warehouse/iceberg/<PAIMON_DB>/<table>/metadata/` (`vN.metadata.json`
   and `version-hint.text`) and registers the tables in the Paimon Glue DB as
   `table_type=ICEBERG` (`trino:paimon` reads them through that entry).
3. **Account values** in `cdc-pipeline/.bench-env.local` (git-ignored; if it
   is missing, `throughput-benchmark/.bench-env.local` is used):
   `AWS_REGION`, `AWS_ACCOUNT_ID`, `BUCKET_NAME`, `EMR_EXECUTION_ROLE_ARN`.
4. **Laptop tools**: `kubectl` on the EKS cluster (namespace `emr-flink`),
   `helm` (for `deploy-trino.sh up`), `envsubst` (`brew install gettext`), the
   AWS CLI, `python3`.
5. **The IRSA ServiceAccount `starrocks-sa`.** The name is historical: it is
   the IRSA ServiceAccount the DuckDB server and Trino share
   (`TRINO_SERVICE_ACCOUNT` overrides it for Trino). It is annotated with the
   EMR execution role, which needs Glue `GetTable(s)` / `GetDatabase(s)` on
   both databases and S3 read on both warehouses and on `querybench-pins/`. If
   it is missing:
   ```bash
   kubectl create serviceaccount starrocks-sa -n emr-flink
   kubectl annotate serviceaccount starrocks-sa -n emr-flink \
     eks.amazonaws.com/role-arn="$EMR_EXECUTION_ROLE_ARN"
   ```
   The runner Job has its own ServiceAccount (`cdc-qb-runner`) on the same
   role. It also needs S3 write on `querybench-pins/` and `querybench-results/`.
6. **Engines.** `duckdb-warm-cdc` is this harness's own server: the same pod
   shape as the throughput benchmark's `duckdb-warm`, and a copied server that
   returns every result row. Its image is `DUCKDB_IMAGE` (default: DuckDB
   2.0.0 built from source, `engine-deploy/duckdb-image/`). Trino is
   `engine-deploy/deploy-trino.sh`
   ([../engine-deploy/README.md](../engine-deploy/README.md)). `run.sh start`
   brings each one up when needed, refuses a Trino that runs a version other
   than `TRINO_VERSION` (`TRINO_VERSION_CHECK=0` skips the check), and
   redeploys a DuckDB server whose `/health` reports another table set or
   image. `run.sh start` also refuses while throughput's `qb-runner` /
   `qb-watch` is active (`FORCE=1` overrides).

## Quick start

```bash
cd cdc-pipeline
SUITE=hot ./querybench/run.sh render        # dry run: Job manifests + every query for all 4 arms, no cluster
SUITE=hot ./querybench/run.sh start         # writers + generator RUNNING
./querybench/run.sh logs                    # follow the runner (wait for "status: runner-exit=")
SUITE=snapshot ./querybench/run.sh start    # after hot: stops generator + writers, submits the Iceberg
                                            # compaction; the runner waits for it, then cold start + passes
./querybench/run.sh summary                 # -> bench-results/QUERYBENCH-SUMMARY-<date><suffix>.md
./querybench/run.sh status
ENGINES=trino SUITE=snapshot ./querybench/run.sh start        # Trino arms only
FORMATS=iceberg SUITE=hot ./querybench/run.sh start           # Iceberg arms only
```

## Step by step

### 1. Dry run (no cluster, no AWS)

`SUITE=hot ./querybench/run.sh render` (or `RENDER_DIR=/tmp/r`) writes the
runner and watcher manifests, the step script, the bundle, the DuckDB
manifest, Trino's Helm values (`trino-values-deployed.yaml`), `run-meta.txt`,
and `sql-<suite>-<engine>-<arm>.sql`: every query rendered for every arm with
sample pins. It fails on any `${...}` left after rendering and on a missing
dialect file. The drivers render on their own too:
`./querybench/trino.sh render snapshot paimon`,
`./querybench/duckdb-warm.sh render hot iceberg` (these need the windows and
pins in the environment).

### 2. Hot test (estimate: ~1–2 h)

`SUITE=hot ./querybench/run.sh start`:

1. Waits for an earlier `cdc-qb-runner` (`FORCE=1` replaces it), then waits
   for and deletes the old `cdc-qb-watch`.
2. Brings `duckdb-warm-cdc` up if it does not answer `/health`. Brings Trino
   up (`deploy-trino.sh up`) if fewer than `TRINO_WORKERS` workers are active;
   if the watcher scaled it to 0, the replicas are restored first. Then
   empties both caches, unless every engine was just started.
3. Starts the runner, which runs `hot-interleave.sh`:
   - waits until the queried writers (`flink-cdc-<format>` for each format)
     are `RUNNING`, and logs the generator state;
   - sets the window: `HOT_WINDOW_SECONDS` (300) ending `HOT_LAG_SECONDS`
     (300) before now, on whole minutes, inside one day;
   - waits until each format's `orders` and `order_items` have a commit at
     least 60 s after `HOT_TO`;
   - pins every table in every format (`pin.sh`), checks Trino reads the pins,
     and picks H8's key (an order updated in the window and created before it)
     on the first format's pinned `orders`;
   - runs warm-up pass p0, then `RUNS` (3) timed passes. Each pass runs
     DuckDB (its arms), then Trino (its arms), on the same pins. By default
     (`HOT_REPIN=pass`) the tables are pinned again before every pass, because
     Paimon keeps only 5–10 snapshots (~10 min) and expiry deletes files an
     older pin still lists;
   - writes the medians of the timed passes and uploads `bench-results/`.
4. Starts the watcher. When the runner ends with `status: runner-exit=0`
   (the test completed), the watcher scales the generator to 0, suspends the writers (they resume from their last checkpoint with
   `kubectl patch ... state: running`; never re-apply them), scales
   `flink-cdc-monitor` to 0, deletes `duckdb-warm-cdc`, and scales Trino to 0.
   If the test did NOT complete (non-zero exit, failed or vanished runner),
   it stops nothing: it logs the runner's last errors and what is still
   running, and ends with `status: qb-watch-failed-left-running`. Fix and
   re-run `start`, or tear down with `SUITE=hot ./querybench/run.sh stop`.
   `STOP_GENERATOR=0`, `SUSPEND_WRITERS=0`, `STOP_DUCKDB=0` and `STOP_TRINO=0`
   each keep one running. The pod has no helm, so `deploy-trino.sh down` is
   the way to remove the release.

### 3. Snapshot test

`SUITE=snapshot ./querybench/run.sh start`, on the laptop:

1. Same waits as the hot test.
2. Scales the generator to 0 and waits `WRITER_DRAIN_S` (180 s) so both jobs
   commit the binlog tail. Then suspends both writers and waits until no
   TaskManager is left.
3. `SNAPSHOT_DT` = the newest date before today (UTC) that `orders` and
   `order_items` hold in every format (`SNAPSHOT_DT_PICK=oldest` takes the oldest).
4. With `iceberg` in `FORMATS` and `COMPACT_BEFORE_SNAPSHOT=true` (the
   `.bench-env` value), it **submits** one Spark compaction Job per
   `COMPACT_TABLES` table (`../iceberg-compaction/run-compaction.sh`), in the
   background, and waits only until each Job object exists. Without it, the
   compaction gate is checked now, and the run is refused unless every Job
   already succeeded.
5. Brings the engines up, then starts the runner (`snapshot.sh`) and the
   watcher. The runner:
   1. **gate**: the writers stopped; with `iceberg` in `FORMATS`, the
      compaction completed (`qb_compaction_gate`: every Job
      `iceberg-compact-<t>` SUCCEEDED, created after this run's submission,
      and the table's current snapshot is its `replace` commit; it waits up to
      `COMPACT_GATE_TIMEOUT_S`, 6 h); every engine up (`duckdb-warm-cdc`
      `busy: false`, Trino with `TRINO_WORKERS` active workers);
   2. **pins** every format's tables after the gate, so the compacted Iceberg
      snapshot is the one measured, and checks Trino reads them;
   3. derives the **S4 slice** `[S_FROM, S_TO)`: `SNAPSHOT_SLICE_MINUTES` (10)
      of `orders.order_date`, centred in the span every format's pinned
      `orders` hold on `SNAPSHOT_DT`, read untimed on the setup engine. A
      format no engine can read is left out of the span, with a log line;
   4. **cold start** (`clear-caches.sh`);
   5. runs each engine in `ENGINES` order over its arms: warm-up + `RUNS` timed passes;
   6. uploads `bench-results/`.
6. The watcher shuts the run down after a completed test, as in the hot test; after a failure it leaves everything running and reports.

`snapshot-<STAMP>.pins` records `gate_writers_stopped_at`,
`compaction_<table>=…`, `gate_compaction_finished_at`,
`gate_engines_ready_at`, the pins, `pinned_at`, the slice,
`caches_cleared_at` and each engine's finish time. Paimon needs no external
compaction (its writer compacts inline). Uncompacted Iceberg `order_items`
carries hundreds of millions of equality deletes, and in 2026-09 a bare count
on it ran engines out of memory. `ICEBERG_REQUIRE_COMPACTION=0` measures it
uncompacted anyway, and the pins say so.

You can set the values by hand:
`SNAPSHOT_DT=2026-09-27 S_FROM='2026-09-27 10:00:00' S_TO='2026-09-27 10:10:00'`.

### 4. Summary

`./querybench/run.sh summary` syncs the session's newest hot and snapshot run
(`HOT_STAMP=` / `SNAPSHOT_STAMP=` pick others; `SUMMARY_DIR=<dir with hot/ snapshot/>`
reads a local copy) and writes `bench-results/QUERYBENCH-SUMMARY-<session>.md`:

- the median seconds per query and arm, and Trino / DuckDB per format;
- geometric means: Trino vs DuckDB on each format, and Paimon's export vs
  Iceberg on each engine;
- Trino's server-side breakdown (elapsed, queued, execution, CPU, physical
  input) and the GB each arm scanned;
- **result equality**: all rows of the last timed pass, compared as text
  (the queries print engine-neutral text; numbers and timestamps are
  normalised once more). DuckDB vs Trino on one format must match, or the
  suite is **INVALID**. Iceberg vs Paimon differences are listed as a pipeline
  gap. An arm that did not run OK shows its status and is not compared;
- the pinned tables per format and table: snapshot id, records, data files,
  delete files, equality / position deletes (DVs included), size, and the
  native Paimon snapshot each export pin came from;
- DuckDB server restarts, and every `.pins` file.

`cdc-pipeline/bench-results/` is git-ignored. Use `git add -f` to commit a
summary. The bucket name and account ID are masked.

## Placeholders (`lib/querybench.sh`, `qb_render`)

| Placeholder | Value |
|---|---|
| `${CUSTOMERS}` `${PRODUCTS}` `${ORDERS}` `${ORDER_ITEMS}` | the arm's table expression (table above). An alias may follow: `FROM ${ORDERS} AS o` (Trino takes it after `FOR VERSION AS OF`) |
| `${ORDERS_PART}` / `${ORDER_ITEMS_PART}` | iceberg arms: `order_dt` / `created_dt`; paimon arms: `order_date_str` / `created_date_str` (all STRING `YYYY-MM-DD`) |
| `${HOT_DT}` `${HOT_FROM}` `${HOT_TO}` | hot window, UTC, `YYYY-MM-DD HH:MM:SS`, bare (the SQL writes `TIMESTAMP '${HOT_FROM}'`) |
| `${SNAPSHOT_DT}` `${S_FROM}` `${S_TO}` | snapshot partition, and the S4 slice on `orders.order_date` |
| `${LOOKUP_ORDER_ID}` `${LOOKUP_ORDER_DT}` | H8's key, one per run |
| dialect keys | every `KEY=VALUE` line of `dialect-<engine>.env`. They are parsed, never sourced, and may not shadow a key above |

Any `${...}` left after rendering stops the run and names the placeholder.
`-- bench:` flags: `allow-zero-rows`, `leading-col-is-label` (the ZERO_SCAN guard).

## Knobs

| Knob | Default | Meaning |
|---|---|---|
| `SUITE` | — | `hot` / `snapshot` |
| `ENGINES` / `FORMATS` | `duckdb trino` / `both` | the arms; `ENGINES` order is the run order |
| `RUNS` / `WARMUP` | `3` / `1` | timed / discarded passes |
| `QB_SETUP_ENGINE` | `trino` if in `ENGINES`, else `duckdb` | where the untimed slice-span and H8 reads run |
| `SKIP_PUFFIN_VERIFICATION_ARMS` | `paimon` on v3, empty on v2 | DuckDB arms run with `iceberg_unsafe_skip_puffin_verification = true` (`deploy-duckdb-cdc.sh up`) |
| `TRINO_VERSION` / `TRINO_VERSION_CHECK` | `484` / `1` | the Trino version `start` requires (`0`: measure whatever runs) |
| `TRINO_WORKERS` / `TRINO_USER` / `TRINO_CATALOG` | `3` / `cdcbench` / `iceberg` | Trino readiness, CLI user, catalog |
| `TRINO_COORDINATOR` / `TRINO_CONTAINER` / `TRINO_URL` | `deploy/trino-coordinator` / `trino-coordinator` / `http://trino.<ns>.svc.cluster.local:8080` | where the CLI runs; where `/v1/query` is read |
| `CLEAR_COORDINATOR` | `1` | `0`: `clear-cache` keeps the coordinator (and its metadata cache) |
| `HOT_WINDOW_SECONDS` / `HOT_LAG_SECONDS` / `HOT_TO` `HOT_FROM` `HOT_DT` | `300` / `300` / derived | hot window |
| `HOT_COMMIT_TABLES` / `HOT_COMMIT_MARGIN_S` / `WAIT_COMMIT_S` | `orders order_items` / `60` / `1800` | the committed-past-the-window wait |
| `HOT_REPIN` | `pass` | `run` = one pin for every hot pass |
| `QB_QUERY_WRITERS` | `flink-cdc-<format>` per format | writers the hot test needs RUNNING |
| `LOOKUP_ORDER_ID` / `LOOKUP_ORDER_DT` | picked | H8 key |
| `SNAPSHOT_DT` / `SNAPSHOT_DT_PICK` | newest closed / `newest` | snapshot partition |
| `SNAPSHOT_SLICE_MINUTES` / `SNAPSHOT_SLICE_MARGIN_S` / `S_FROM` `S_TO` | `10` / `120` / derived | S4 slice (`0` = the whole day) |
| `WRITER_DRAIN_S` | `180` | wait between stopping the generator and suspending the writers |
| `COMPACT_BEFORE_SNAPSHOT` / `COMPACT_TABLES` | `true` / all four | submit the Iceberg compaction at snapshot start; the tables the gate requires |
| `ICEBERG_REQUIRE_COMPACTION` | `1` | `0`: skip the compaction gate (uncompacted run) |
| `COMPACT_GATE_TIMEOUT_S` / `ENGINE_GATE_TIMEOUT_S` | `21600` / `1800` | how long the runner waits for compaction / the engines |
| `HOT_QUERY_TIMEOUT_S` / `SNAPSHOT_QUERY_TIMEOUT_S` | `1800` / `3600` | per query, both engines |
| `MAX_RESULT_ROWS` | `10000` | rows kept per query for the comparison (both engines) |
| `PROFILE_SCAN` | `1` | DuckDB's untimed scan-bytes pass |
| `SUSPEND_WRITERS` / `STOP_GENERATOR` / `STOP_DUCKDB` / `STOP_TRINO` | `1` | what the watcher (or `run.sh stop`) stops after a completed test |
| `FORCE` / `ALLOW_CONCURRENT` | off | replace a runner / skip the one-test-at-a-time guard |
| DuckDB sizing (`DUCKDB_*`, `CACHE_*`) | as throughput | `engine-deploy/deploy-duckdb-cdc.sh` |
| Trino sizing (`TRINO_WORKER_*`, `TRINO_COORD_*`, `TRINO_CACHE_*`) | above | `engine-deploy/deploy-trino.sh` |

## Output

In `s3://$BUCKET_NAME/querybench-results/cdc-session<set tag><SESSION_SUFFIX>/<STAMP>-<suite>/`:

| File | Content |
|---|---|
| `duckdb_warm-<STAMP>.csv`, `trino-<STAMP>.csv` | `stamp,engine,arm,suite,query,status,median_ms,result_rows,scan_rows,scan_bytes`. `status`: `OK`, `ERROR`, `MISSING`, `TIMEOUT`, `ZERO_SCAN`, `CRASH` (the DuckDB server restarted under the query), `OOM` (Trino memory limit) |
| `trino-<STAMP>-queries.csv` | every Trino pass: arm, query id, elapsed, queued, execution, CPU, physical input bytes/rows, error |
| `*-rows.tsv`, `*-firstrows.csv` | result rows of the last timed pass, per arm |
| `duckdb_warm-<STAMP>-events.csv`, `*.log` | DuckDB restarts and cold queries; every pass |
| `tables-<STAMP>.tsv` | the pinned snapshots' state, one line per pin tag × format × table |
| `hot-interleave-<STAMP>.pins` / `snapshot-<STAMP>.pins` | windows, pins, H8 key, gates, writer and generator state |
| hot: `*-p<N>.csv`, `hot-interleave-<STAMP>.log` | every pass (p0 = warm-up) |

Pinned metadata copies go to `s3://$BUCKET_NAME/querybench-pins/cdc-session<set tag><suffix>/<tag>/<format>/<table>/`.

## Decisions and caveats

1. **The Paimon arms read the export, not Paimon.** Their numbers measure an
   Iceberg reader over Paimon's exported metadata. That is the only way either
   engine reads Paimon today, but it is not Paimon's own read path.
2. **Paimon full compaction is a TODO** (`TODO(paimon-compaction)` in
   `run.sh`). Until one runs, the export lists only top-LSM-level files and
   can trail native Paimon. Both Paimon arms read the same export, so they
   still agree with each other.
3. **The hot window (5 min) and the snapshot slice (10 min)** are guesses for
   the ~40k rows/s upsert load. Size them from the first run's H1 and S4 rows.
4. **The coordinator runs the CLI.** Its JVM (≤ 512 MB heap) streams results
   during the timed window, but it is not on Trino's split path.
5. **History**: the StarRocks harness this replaces, and its results, stay in
   git history and `bench-results/`.

## What is verified, what is not

- **Verified offline (2026-11-02)**: `bash -n` on every script;
  `python3 -m py_compile summarize.py`; `SUITE=hot` and `SUITE=snapshot`
  `run.sh render` with the v3 default, with `ICEBERG_FORMAT_VERSION=2
  CDC_DB_SUFFIX=`, and with `ENGINES=trino`. Every query renders for all four
  arms with no placeholder left; an unknown placeholder, a missing dialect
  file and `ENGINES=starrocks` fail as they should. `pin.sh` was run against a
  stubbed `aws`: both formats, the export-id fallback, and the tables TSV.
  `trino.sh run` was run against a stubbed `kubectl` (CLI JSON rows with
  duplicate column names, the `system.runtime.queries` lookup with empty
  trailing fields, TIMEOUT and ERROR mapping, ZERO_SCAN, the paimon arm
  failing while the iceberg arm runs, and the `/v1/query` fallback through
  `curl`). `summarize.py` was run on synthetic four-arm results: an INVALID
  engine mismatch, a Paimon gap, and an ERROR arm left out of the comparison.
- **Needs the cluster**: every Job, engine start and cache clear; the trino
  CLI's JSON/TSV output and the `system.runtime.queries` columns as the
  helper parses them; `FOR VERSION AS OF` on Trino 484 for both the Iceberg
  tables and the Paimon export (its Glue entry reachable through the
  `iceberg` catalog); the export snapshot id equal to N; `/v1/query` reachable
  from the runner pod; the watcher's scale to 0 and `run.sh`'s restore before
  `helm upgrade`; DuckDB 2.0 on the v3 export (expected to fail today);
  the IAM permissions above; and every timing.
