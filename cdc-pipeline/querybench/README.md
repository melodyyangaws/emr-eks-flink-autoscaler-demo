# CDC query benchmark: StarRocks vs warm DuckDB on Paimon and Iceberg

The read half of the CDC pipeline benchmark: the same queries, on both engines,
over the four tables the two Flink CDC jobs write (`customers`, `products`,
`orders`, `order_items`; MySQL `ecommerce` → `flink-cdc-paimon` /
`flink-cdc-iceberg`). It follows
[../../throughput-benchmark/querybench/](../../throughput-benchmark/querybench/README.md),
and most of its scripts are adapted copies of that harness. The throughput
benchmark is not changed by any of this.

> **Nothing here has been run on the cluster yet.** Every time below is an
> estimate. The scripts were checked with `bash -n`, a dry-run render and
> offline stubs only (see "What is verified").

| Suite | Reads | Writers | Queries |
|---|---|---|---|
| **hot** | a 5-min window (`HOT_WINDOW_SECONDS`), ending 5 min ago: `orders.updated_at`, `order_items.created_at` | running, generator live | H1–H9 ([hot/QUERIES.md](hot/QUERIES.md)) |
| **snapshot** | one closed partition (`SNAPSHOT_DT`) of the fact tables and the whole dimension tables; S4 a 10-min `orders.order_date` slice | stopped | S1–S7 ([snapshot/QUERIES.md](snapshot/QUERIES.md)) |

Two table sets, switched in `.bench-env` (this branch defaults to v3):

| Set | `ICEBERG_FORMAT_VERSION` | `CDC_DB_SUFFIX` | Glue databases | StarRocks catalogs | Results / pins prefix, summary |
|---|---|---|---|---|---|
| **v3** (default here) | `3` | `_v3` | `flink_paimon_db_v3`, `flink_iceberg_db_v3` | `cdc_paimon_catalog_v3`, `cdc_pmn_as_ice_v3`, `cdc_iceberg_catalog_v3` | `cdc-session-v3<SESSION_SUFFIX>/`, `QUERYBENCH-SUMMARY-<date>-v3<SESSION_SUFFIX>.md` |
| v2 | `2` | empty | `flink_paimon_db`, `flink_iceberg_db` | `cdc_paimon_catalog`, `cdc_pmn_as_ice`, `cdc_iceberg_catalog` | `cdc-session<SESSION_SUFFIX>/`, `QUERYBENCH-SUMMARY-<date><SESSION_SUFFIX>.md` |

`ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX= ./querybench/run.sh ...` runs the v2
set (`CDC_DB_SUFFIX` set, but empty). The format version is that of both
formats' Iceberg metadata (the Iceberg tables' `format-version`, Paimon's
`metadata.iceberg.format-version`); the writers' side is in
[../README.md](../README.md). Every `.pins` file and the summary header record
the set (`iceberg_format_version=`, `cdc_db_suffix=`, the databases, catalogs,
skipped arms, engine versions and the DuckDB puffin setting), and `pin.sh`
records each pinned metadata.json's own `format-version` (a mismatch with
`ICEBERG_FORMAT_VERSION` is logged as a WARNING).

| Engine | Arm | Reader → data | v2 | v3 |
|---|---|---|---|---|
| StarRocks | `paimon` | Paimon reader → Paimon tables (`flink_paimon_db<suffix>`) | native reader | runs, on the **JNI reader** (`SET paimon_force_jni_reader = true`) |
| StarRocks | `pmn_as_iceberg` | Iceberg reader → Paimon's Iceberg export | runs | **skipped** |
| StarRocks | `iceberg` | Iceberg reader → Iceberg tables (`flink_iceberg_db<suffix>`) | runs | **skipped** |
| DuckDB | `paimon` | `iceberg_scan` → Paimon's Iceberg export | runs | runs, with `iceberg_unsafe_skip_puffin_verification = true` |
| DuckDB | `iceberg` | `iceberg_scan` → Iceberg tables | runs | runs |

Why the v3 column (2026-11-02):

- **StarRocks Iceberg readers skipped.** StarRocks 4.1.x (4.1.7 included; also
  branch-4.2 and main) refuses any scan of an Iceberg v3 table that has a
  deletion vector: `Iceberg V3 Deletion Vectors are not supported`
  (`IcebergConnectorScanRangeSource.buildScanRanges`; StarRocks PR #76228 is
  open). Both Iceberg-reader arms read v3 metadata with DVs, so they are left
  out of `ARMS` (an explicit `ARMS=` loses them too, with a log line).
  `SR_V3_ARMS=1` runs them anyway to record that failure: their catalog and
  pin checks then warn instead of stopping, and their queries come out as
  `ERROR` in the CSV. The H8 key is then picked on the native `paimon` arm
  and not confirmed on Iceberg, and the S4 slice is centred in the Paimon
  span only (both logged and in the pins).
- **StarRocks `paimon` on the JNI reader.** On the v3 set Paimon writes 64-bit
  deletion vectors (`deletion-vectors.bitmap64 = true`). StarRocks 4.1.x's
  native C++ Paimon reader decodes only 32-bit DVs
  (`be/src/exec/paimon/paimon_delete_file_builder.cpp`; its magic check is a
  DCHECK only: a crash or wrong rows), while its JNI reader (Paimon's Java
  reader) reads both. So every StarRocks session that reads the `paimon` arm
  on v3 sets `paimon_force_jni_reader = true` (a 4.1.7 session variable;
  `starrocks.sh` checks `SHOW VARIABLES LIKE 'paimon_force_jni_reader'` and
  stops if the FE lacks it; `SR_PAIMON_JNI=0` opts out). The JNI reader is
  slower: **v3 SR `paimon` times are not like-for-like with a v2 run's native
  SR `paimon`**; the pins (`sr_paimon_force_jni_reader=true`) and the summary
  say so. Unset on v2.
- **DuckDB `paimon` puffin setting.** Paimon's export writes its DV index files
  as bare blobs, not Puffin containers, which DuckDB 2.0's Iceberg reader
  rejects unless `iceberg_unsafe_skip_puffin_verification = true` (new in 2.0,
  global, default false). The server sets it to true for the queries of the
  arms in `SKIP_PUFFIN_VERIFICATION_ARMS` (default `paimon` on v3, none on v2)
  and to false for every other query (one query runs at a time); `/health`,
  the pins (`duckdb_skip_puffin_verification_arms=`) and the summary record it.
  It skips a check, so wrong DV offsets would silently delete the wrong rows:
  the ZERO_SCAN guard and the cross-arm row comparison stay on, and on v3 the
  validity check (below) compares DuckDB `paimon` with StarRocks `paimon`, two
  independent DV decoders.

---

## Prerequisites

1. **The CDC pipeline is deployed and has data**: both FlinkDeployments
   (`flink-cdc-paimon`, `flink-cdc-iceberg`) from
   [../1-BUILD-DEPLOY-FLINK-APP-GUIDE.md](../1-BUILD-DEPLOY-FLINK-APP-GUIDE.md),
   and, for the hot test, the MySQL load generator
   (`WORKLOAD=upsert ../mysql-data-generator/deploy-data-generator.sh deploy`,
   StatefulSet `mysql-upsert-loadgen`). On 2026-11-02 neither CDC job was
   deployed, and each fact table had one partition, `2026-09-27`.
2. **Paimon's Iceberg export is on** (`metadata.iceberg.storage =
   hive-catalog` in `sql-scripts/paimon/03-paimon-sinks.sql`). It lands under
   `s3://$BUCKET_NAME/paimon-warehouse/iceberg/flink_paimon_db<CDC_DB_SUFFIX>/<table>/metadata/`
   (`vN.metadata.json` + `version-hint.text`, N = Paimon's snapshot id).
   Checked on 2026-11-02 for all four tables.
3. **Account values**: `cdc-pipeline/.bench-env.local` (git-ignored), or else
   `throughput-benchmark/.bench-env.local` is used:
   ```bash
   export AWS_REGION=us-west-2
   export AWS_ACCOUNT_ID=<12 digits>
   export BUCKET_NAME=emr-on-eks-test-<account>-<region>
   export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::<account>:role/emr-on-eks-test-execution-role
   ```
4. **Laptop tools**: `kubectl` on the EKS cluster (namespace `emr-flink`),
   `helm` (StarRocks install), `envsubst` (`brew install gettext`), the AWS CLI,
   `python3`.
5. **IAM (not checked)**: the runner Job uses the EMR execution role through
   IRSA. It needs `glue:GetTable` on both databases, S3 read on both
   warehouses, and S3 write on `querybench-pins/` and `querybench-results/`.
   The DuckDB server and StarRocks use `starrocks-sa`, which needs S3 read on
   both warehouses and on `querybench-pins/`.
6. **Engines are shared with the throughput benchmark**, not run at the same time:
   - StarRocks: the same cluster (`kube-starrocks`), installed by
     `throughput-benchmark/engine-deploy/deploy-starrocks.sh install`. This
     harness creates its own catalogs: `cdc_paimon_catalog` (Paimon
     `filesystem` on `paimon-warehouse/`), `cdc_iceberg_catalog` (Iceberg on
     Glue) and `cdc_pmn_as_ice` (Iceberg `hadoop` on `paimon-warehouse/iceberg/`),
     each with the `CDC_DB_SUFFIX` appended (`cdc_paimon_catalog_v3`, ...).
     **Version:** `run.sh start` installs StarRocks only when it is not up,
     passing `.bench-env`'s `SR_IMAGE_TAG` / `SR_CHART_VERSION` (4.1.7 / 1.11.8)
     as that script's `SR_IMAGE_TAG` / `CHART_VERSION`; the script's own
     defaults are not changed. As the cluster is shared, a version set here
     is the one the throughput benchmark then runs on, and `install` is a
     `helm upgrade` that rolls a running cluster. A StarRocks that is up on
     another FE/BE tag is refused (`SR_VERSION_CHECK=0` measures it as is).
     The Glue entry of a Paimon table carries `PaimonStorageHandler`, which
     StarRocks' Glue Iceberg catalog cannot load.
   - DuckDB: its **own** Deployment `duckdb-warm-cdc`
     ([../engine-deploy/](../engine-deploy/)). It has the same pod size, cache and
     settings as throughput's `duckdb-warm`, and a copied server that returns
     every result row and its start time. The throughput server returns only
     the first row, and it resolves one `${TABLE}` itself. **Engine: DuckDB
     2.0.0.** `DUCKDB_IMAGE` (`.bench-env`; default
     `<account>.dkr.ecr.<region>.amazonaws.com/duckdb-cdc-server:2.0.0`, built
     from source by [duckdb-image/](duckdb-image/)) runs as is:
     no pip install at start; iceberg, httpfs, aws and avro are statically
     linked and cache_httpfs is preinstalled, so the server INSTALLs nothing.
     `DUCKDB_IMAGE=` (empty) falls back to `python:3.12-slim` + `pip install
     duckdb==2.0.0` (the PyPI wheel) at every start, the extensions downloaded
     then. The image is built with GCC 14 and no LTO, the PyPI wheels with
     clang and ThinLTO: **DuckDB timings from the image are a new baseline**,
     comparable neither with the 1.5.6 runs nor with the pip fallback.
     `run.sh start` redeploys a running server whose `/health` reports another
     image or Iceberg format version.
   - `run.sh start` refuses while throughput's `qb-runner`/`qb-watch` is active
     (`FORCE=1` overrides). The reverse is not guarded: the throughput harness
     does not know this one, so do not start a throughput test while a CDC test runs.

## Configuration

`cdc-pipeline/.bench-env` (tracked) holds the session values, and
`.bench-env.local` holds the account values, as in throughput-benchmark. A
value set on the command line wins, e.g. `RUNS=1 WARMUP=1 SUITE=hot ./querybench/run.sh start`.
`mysql-cdc-env.sh` is not used: it is the MySQL source's connection file.

| Key | Default | Meaning |
|---|---|---|
| `ICEBERG_FORMAT_VERSION` | `3` (this branch) | `2` / `3`: the table set's Iceberg format version; decides the arms (above) |
| `CDC_DB_SUFFIX` | `_v3` (this branch) | empty or like `_v3`: the table set. Databases, catalogs, results / pins prefixes and the summary name carry it |
| `SESSION_SUFFIX` | empty | results go to `querybench-results/cdc-session<set tag><suffix>/` (set tag: `-v3` for `CDC_DB_SUFFIX=_v3`). It labels results only: `CDC_DB_SUFFIX` selects the tables |
| `PAIMON_WAREHOUSE` / `ICEBERG_WAREHOUSE` | as the FlinkDeployments | the warehouses |
| `PAIMON_DB` / `ICEBERG_DB` | derived | `flink_paimon_db<CDC_DB_SUFFIX>` / `flink_iceberg_db<CDC_DB_SUFFIX>` (`lib/common.sh`, after the command-line overrides) |
| `SR_IMAGE_TAG` / `SR_CHART_VERSION` | `4.1.7` / `1.11.8` | the StarRocks `run.sh start` installs (shared cluster, see Prerequisites) |
| `DUCKDB_IMAGE` | the ECR `duckdb-cdc-server:2.0.0` | empty = pip `duckdb==2.0.0` on `python:3.12-slim` |
| `FLINK_JOBS` | `flink-cdc-paimon flink-cdc-iceberg` | writers to check and suspend |
| `GENERATOR_RESOURCES` | `statefulset/mysql-upsert-loadgen deployment/mysql-data-generator` | scaled to 0 for snapshot and after a test |
| `COMPACT_BEFORE_SNAPSHOT` / `COMPACT_TABLES` | `true` / all four | submit the Iceberg compaction Jobs at snapshot start; the tables the Iceberg gate requires |

---

## Quick start

```bash
cd cdc-pipeline
SUITE=hot ./querybench/run.sh render        # dry run: Job manifests + every query rendered, no cluster
SUITE=hot ./querybench/run.sh start         # writers + generator RUNNING
./querybench/run.sh logs                    # follow the runner (wait for "status: runner-exit=")
SUITE=snapshot ./querybench/run.sh start    # after hot; stops generator + writers, submits the
                                            # Iceberg compaction, Paimon phase now, Iceberg phase
                                            # once compacted (SNAPSHOT_FORMAT=paimon|iceberg: one phase)
./querybench/run.sh summary                 # -> bench-results/QUERYBENCH-SUMMARY-<date><suffix>.md
./querybench/run.sh status
```

## Step by step

### 1. Dry run (no cluster, no AWS)

```bash
SUITE=hot ./querybench/run.sh render        # or RENDER_DIR=/tmp/r
```

This writes the runner and watcher manifests (`cdc-qb-runner-deployed.yaml`,
`cdc-qb-watch-deployed.yaml`), the step script, the bundle, the DuckDB
manifest, and `sql-<suite>-<engine>-<arm>.sql`: every query rendered for every
arm with sample pins. It fails on any `${...}` left after rendering. Read the
StarRocks and DuckDB files once before the first run. You can also render
from the drivers: `./querybench/starrocks.sh render snapshot iceberg` and
`./querybench/duckdb-warm.sh render hot paimon`. These need the windows and
pins in the environment.

### 2. Hot test (estimate: ~1–2 h)

`SUITE=hot ./querybench/run.sh start`:

1. Waits for an earlier `cdc-qb-runner` (`FORCE=1` replaces it), then waits
   for and deletes the old `cdc-qb-watch`.
2. Brings StarRocks up if the FE or a BE is not Ready (estimate ~5–10 min),
   and `duckdb-warm-cdc` if it does not answer (estimate ~5–10 min on a new
   node). Empties both caches with `clear-caches.sh`, unless both engines were
   just started.
3. Starts the runner Job, which runs `hot-interleave.sh`:
   - waits until both writers are `RUNNING`, then logs the generator state;
   - sets the window to `HOT_WINDOW_SECONDS` (300) ending `HOT_LAG_SECONDS`
     (300) before now, on whole minutes, inside one day;
   - waits until both formats' `orders` and `order_items` have a commit at
     least 60 s after `HOT_TO`. It does not wait for equal row counts: under
     live upserts the rows in an `updated_at` window keep moving, and Paimon's
     export lags;
   - pins all 4 tables × 2 formats (`pin.sh`), checks every StarRocks arm reads
     its pins, and picks H8's key (`LOOKUP_ORDER_ID` / `LOOKUP_ORDER_DT`: an
     order updated in the window, created before it, found on Iceberg and
     confirmed on native Paimon; on v3 found on native Paimon, unconfirmed);
   - runs warm-up pass p0, then `RUNS` (3) timed passes. Each pass runs
     StarRocks (3 arms; v3: `paimon` only), then DuckDB (2 arms). By default (`HOT_REPIN=pass`)
     the tables are pinned again before every pass. Paimon keeps only 5–10
     snapshots (~10 min at 60 s checkpoints), and expiry deletes files an
     older pin still lists;
   - computes the medians of the timed passes and uploads `bench-results/` to S3.
4. Starts the watcher. When the runner ends, it scales the generator to 0,
   suspends the writers (they resume from their last checkpoint:
   `upgradeMode: last-state`), and scales `flink-cdc-monitor` to 0. It then
   takes `duckdb-warm-cdc` and StarRocks down. `STOP_STARROCKS=0`,
   `STOP_DUCKDB=0`, `SUSPEND_WRITERS=0` and `STOP_GENERATOR=0` keep each one.

To resume the writers: `kubectl patch flinkdeployment flink-cdc-paimon -n emr-flink --type=merge -p '{"spec":{"job":{"state":"running"}}}'`
(and the same for `flink-cdc-iceberg`). Patch them. Do not re-apply: an apply
over an existing EMR FlinkDeployment empties `/opt/flink/usrlib`.

### 3. Snapshot test: two phases, one per format, each from a cold start

| Phase | Arms (v2) | Arms (v3) | Can start when | Waits for compaction? |
|---|---|---|---|---|
| **Paimon** | SR `paimon`, SR `pmn_as_iceberg`, DuckDB `paimon` | SR `paimon` (JNI reader), DuckDB `paimon` (puffin verification skipped) | the CDC writers are suspended, StarRocks is up (FE + all 3 BEs Ready), `duckdb-warm-cdc` answers `busy: false`; pins taken | **no**: Paimon compacts inside its writer, and the Iceberg compaction never blocks this phase |
| **Iceberg** | SR `iceberg`, DuckDB `iceberg` | DuckDB `iceberg` only: StarRocks is neither run nor cache-cleared in this phase (it stays up for the engine gate) | the same, **and** every `COMPACT_TABLES` Job `iceberg-compact-<table>` has **succeeded** and each table's current snapshot is that compaction's `replace` commit; the Iceberg pins are taken **after** that | **yes**. Failed: refuses. Missing or running: `SNAPSHOT_FORMAT=iceberg` refuses at once; `both` waits up to `COMPACT_GATE_TIMEOUT_S` (6 h) |

`SNAPSHOT_FORMAT` selects the phases:

```bash
SUITE=snapshot ./querybench/run.sh start                          # both (default): Paimon now, Iceberg once compacted
SUITE=snapshot SNAPSHOT_FORMAT=paimon ./querybench/run.sh start   # Paimon phase only (also submits the compaction)
COMPACT_BEFORE_SNAPSHOT=false SUITE=snapshot SNAPSHOT_FORMAT=iceberg ./querybench/run.sh start   # later: Iceberg phase only
ICEBERG_REQUIRE_COMPACTION=0 SUITE=snapshot SNAPSHOT_FORMAT=iceberg ./querybench/run.sh start    # measure Iceberg uncompacted
```

`SUITE=snapshot ./querybench/run.sh start`, on the laptop:

1. Same waits as the hot test.
2. Scales the generator to 0 and waits `WRITER_DRAIN_S` (180 s) so both CDC
   jobs commit the binlog tail. Then suspends both writers and waits until no
   TaskManager is left.
3. Sets `SNAPSHOT_DT` to the newest date before today (UTC) that both formats'
   `orders` and `order_items` hold (`SNAPSHOT_DT_PICK=oldest` takes the
   oldest). Today that is `2026-09-27`.
4. If `COMPACT_BEFORE_SNAPSHOT=true` (the `.bench-env` value), it **submits**
   one Spark compaction Job per `COMPACT_TABLES` table (default: all four)
   with [../iceberg-compaction/run-compaction.sh](../iceberg-compaction/run-compaction.sh),
   all at once, in the background. It waits only until each Job object
   exists, so the Jobs then run in the cluster without the laptop. Each Job
   asks for 24 cpu / 150 Gi on an m5.12xlarge, so four run on four nodes.
   Estimate: tens of minutes for `order_items`. It is not allowed with
   `SNAPSHOT_FORMAT=iceberg`, which only measures a compaction that has
   already completed. With `SNAPSHOT_FORMAT=iceberg`, or `both` without
   `COMPACT_BEFORE_SNAPSHOT=true`, the Iceberg gate is checked now, and the
   run is refused if it does not pass.
5. Brings the engines up and creates the session's StarRocks catalogs. On
   StarRocks, it derives the S4 slice `[S_FROM, S_TO)`:
   `SNAPSHOT_SLICE_MINUTES` (10) of `orders.order_date`, centred in the span
   both formats hold in `SNAPSHOT_DT`, with datacache off (on v3 the Paimon
   span only, read through the JNI reader: StarRocks cannot read the v3
   Iceberg tables). This is the same slice for both phases.
6. Starts the runner (`snapshot.sh`), then the watcher. For each phase, the
   runner:
   1. runs the gate (table above) and records each gate time;
   2. pins that format's four tables (`pin.sh`, `QB_PIN_FORMATS=<format>`)
      and checks the StarRocks arms read them;
   3. **cold start**: runs `clear-caches.sh` from inside the runner (wipes and
      restarts each BE's datacache, restarts `duckdb-warm-cdc`) right before
      the warm-up pass;
   4. runs StarRocks (the phase's arms), then DuckDB (the phase's arm):
      warm-up + `RUNS` timed passes;
   5. uploads `bench-results/` so far. In `both` mode, the Paimon results are
      in S3 before the Iceberg phase starts waiting.
7. The watcher shuts the run down after the last phase, as in the hot test.

Each phase writes `snapshot-<STAMP>-<format>.pins` with
`gate_writers_stopped_at`, `gate_engines_ready_at`, and for Iceberg,
`compaction_<table>=… completed <time>` and `gate_compaction_finished_at`.
It also records `pinned_at`, `caches_cleared_at`, `phase_finished_at` and the
pinned snapshot ids. Results go to `<STAMP>-snapshot/` (`both`) or
`<STAMP>-snapshot-<format>/` (one phase). The summary merges the newest
Paimon and Iceberg phase.

Estimates (not measured): ~30–90 min per phase of queries, after ~5–10 min of
gate checks and cache clearing. The Iceberg phase starts only after the
compaction, which may take longer than the Paimon phase.

**Caveat (recorded, not a gate):** `pmn_as_iceberg` and DuckDB `paimon` read
Paimon's Iceberg export, which lists only top-LSM-level files. Their row
counts can trail native Paimon's. The summary shows every arm's rows per
query, and marks the run INVALID when a `MUST_MATCH` query differs between
SR `paimon` and `pmn_as_iceberg` (v2), or, on v3, where `pmn_as_iceberg` is
skipped or only records its error, between SR `paimon` and DuckDB `paimon`.

You can set the values by hand: `SNAPSHOT_DT=2026-09-27 S_FROM='2026-09-27 10:00:00' S_TO='2026-09-27 10:10:00'`.
For one engine only, use `ENGINES=duckdb` (or `starrocks`). This needs
`S_FROM`/`S_TO` set, or `SNAPSHOT_SLICE_MINUTES=0`, because the slice is
derived on StarRocks.

### 4. Summary

`./querybench/run.sh summary` syncs the session's newest hot and snapshot run
(`HOT_STAMP=` / `SNAPSHOT_STAMP=` pick others; `SUMMARY_DIR=<dir with hot/ snapshot/>`
reads a local copy) and writes `bench-results/QUERYBENCH-SUMMARY-<session>.md`
with:

- medians and ratios, the geometric-mean comparisons, and the scanned GB;
- **all result rows compared across arms**, with numbers and timestamps
  normalised. The run is **INVALID** if a `MUST_MATCH` query (default
  `S1_order_items_count S7_dedup_keys`) differs between SR `paimon` and
  `pmn_as_iceberg` (v2): the export is incomplete until Paimon is fully
  compacted. On v3 (read from the pins' `iceberg_format_version=`) the pair is
  SR `paimon` and DuckDB `paimon`, never the skipped `pmn_as_iceberg`: the same
  Paimon files, the DVs decoded by two independent readers, so this is also the
  guard against a wrong DV decode. Skipped arms show `skip`. A
  Paimon-vs-Iceberg difference (v3: DuckDB's two arms) is reported as a
  pipeline gap (the binlog point each job reached), not a failure. In the hot
  suite, differences are informational only;
- on v3, a header with the caveats: skipped arms, StarRocks `paimon` on the
  JNI reader (not like-for-like with v2's native reader), DuckDB's puffin
  setting, plus one more geometric-mean pair, DuckDB `paimon` vs SR `paimon`;
- the pinned table state: snapshot ids, records, data/delete files,
  equality/position deletes, and the native Paimon snapshot's
  `totalRecordCount` / `commitKind`;
- DuckDB server restarts (`restart`) and the queries after them in the same
  pass (`cold`);
- the pins and windows file.

`cdc-pipeline/bench-results/` is git-ignored. Use `git add -f` to commit a
summary. The bucket name and account ID are masked.

---

## Placeholders (`lib/querybench.sh`, `qb_render`)

| Placeholder | StarRocks | DuckDB |
|---|---|---|
| `${CUSTOMERS}` `${PRODUCTS}` `${ORDERS}` `${ORDER_ITEMS}` | `cdc_<arm catalog>.<db>.<t> FOR VERSION AS OF CAST(<id> AS BIGINT)`, per table. An alias may follow: StarRocks' grammar puts it after the version clause. `SR_PIN_FORM=subquery` renders `(SELECT * FROM … FOR VERSION AS OF …)` instead, adding `AS <table>` when no alias follows. `pin.sh` switches to it by itself if the clause does not parse | `iceberg_scan('<copied metadata.json>')`, per table and format |
| `${ORDERS_PART}` / `${ORDER_ITEMS_PART}` | `order_date_str` / `created_date_str` for `paimon` and `pmn_as_iceberg`; `order_dt` / `created_dt` for `iceberg` (partition-specs checked 2026-11-02) | the same: `paimon` (the export) uses Paimon's names |
| `${BYTES_FN}` | `LENGTH` | `STRLEN` |
| `${HOT_DT}` `${HOT_FROM}` `${HOT_TO}` | hot window, UTC `YYYY-MM-DD HH:MM:SS` | same |
| `${SNAPSHOT_DT}` `${S_FROM}` `${S_TO}` | snapshot partition, S4 slice on `orders.order_date` | same |
| `${LOOKUP_ORDER_ID}` `${LOOKUP_ORDER_DT}` | H8's key, one per run | same |

Pin ids: Iceberg is the copied metadata's `current-snapshot-id`. For
`pmn_as_iceberg` and native `paimon` it is N of `vN.metadata.json`, which is
Paimon's snapshot id. If native Paimon cannot read `FOR VERSION AS OF`, that
table is unpinned on the `paimon` arm and the pins file says so. Any `${...}`
left after rendering stops the run and names the placeholder. `-- bench:`
flags: `allow-zero-rows`, `leading-col-is-label` (DuckDB's ZERO_SCAN guard),
`io-tasks=1` (StarRocks).

## Knobs

| Knob | Default | Meaning |
|---|---|---|
| `SUITE` | — | `hot` / `snapshot` |
| `RUNS` / `WARMUP` | `3` / `1` | timed / discarded passes |
| `ARMS` / `FORMAT` | all / `both` | StarRocks arms (v3: `paimon` only) / DuckDB arms |
| `SR_V3_ARMS` | `0` | `1`: on v3, also run SR `pmn_as_iceberg` and `iceberg`, to record their DV error |
| `SR_PAIMON_JNI` | `1` | `0`: on v3, SR `paimon` on the native reader (cannot decode 64-bit DVs) |
| `SR_VERSION_CHECK` | `1` | `0`: run on a StarRocks whose FE/BE tag is not `SR_IMAGE_TAG` |
| `SKIP_PUFFIN_VERIFICATION_ARMS` | `paimon` on v3, empty on v2 | DuckDB arms run with `iceberg_unsafe_skip_puffin_verification = true` (`deploy-duckdb-cdc.sh up`) |
| `ENGINES` | `starrocks duckdb` | snapshot: one engine only |
| `HOT_WINDOW_SECONDS` / `HOT_LAG_SECONDS` / `HOT_TO` `HOT_FROM` `HOT_DT` | `300` / `300` / derived | hot window |
| `HOT_COMMIT_TABLES` / `HOT_COMMIT_MARGIN_S` / `WAIT_COMMIT_S` | `orders order_items` / `60` / `1800` | the committed-past-the-window wait |
| `HOT_REPIN` | `pass` | `run` = one pin for every hot pass |
| `LOOKUP_ORDER_ID` / `LOOKUP_ORDER_DT` | picked | H8 key |
| `SNAPSHOT_DT` / `SNAPSHOT_DT_PICK` | newest closed / `newest` | snapshot partition |
| `SNAPSHOT_SLICE_MINUTES` / `SNAPSHOT_SLICE_MARGIN_S` / `S_FROM` `S_TO` | `10` / `120` / derived | S4 slice (`0` = the whole day) |
| `WRITER_DRAIN_S` | `180` | wait between stopping the generator and suspending the writers |
| `SNAPSHOT_FORMAT` | `both` | `paimon` / `iceberg` / `both`: the snapshot phases |
| `ICEBERG_REQUIRE_COMPACTION` | `1` | `0`: the Iceberg phase skips the compaction gate (uncompacted run) |
| `COMPACT_GATE_TIMEOUT_S` / `ENGINE_GATE_TIMEOUT_S` | `21600` / `1800` | how long `both` waits for the compaction / a phase waits for the engines |
| `COMPACT_NOT_BEFORE` | set by run.sh | epoch s; older `iceberg-compact-*` Jobs never satisfy the gate |
| `HOT_QUERY_TIMEOUT_S` / `SNAPSHOT_QUERY_TIMEOUT_S` | `1800` / `3600` | per query, both engines |
| `SR_PIN_FORM` | `clause` | `subquery`: see Placeholders |
| `SR_SNAPSHOT_IO_TASKS` | `2` | `connector_io_tasks_per_scan_operator` on snapshot (memory note: the scan-side OOM lever) |
| `PROFILE_SCAN` | `1` | DuckDB's untimed scan-bytes pass |
| `SUSPEND_WRITERS` / `STOP_GENERATOR` / `STOP_DUCKDB` / `STOP_STARROCKS` | `1` | what the watcher stops |
| `FORCE` / `ALLOW_CONCURRENT` | off | replace a runner / skip the one-engine-at-a-time guard |
| `MUST_MATCH` | `S1_order_items_count S7_dedup_keys` | summary validity queries |
| DuckDB sizing (`DUCKDB_*`, `CACHE_*`) | as throughput | `engine-deploy/deploy-duckdb-cdc.sh` |

## Output

In `s3://$BUCKET_NAME/querybench-results/cdc-session<set tag><SESSION_SUFFIX>/<STAMP>-<suite>/`
(set tag `-v3` for `CDC_DB_SUFFIX=_v3`, empty for v2):

| File | Content |
|---|---|
| `starrocks-<STAMP>.csv`, `duckdb_warm-<STAMP>.csv` | `stamp,engine,arm,suite,query,status,median_ms,result_rows,scan_rows,scan_bytes`. `status`: `OK`, `ERROR`, `MISSING`, `TIMEOUT`, `ZERO_SCAN`, `CRASH` (the DuckDB server restarted under the query) |
| `*-rows.tsv`, `*-firstrows.csv` | result rows of the last timed pass |
| `duckdb_warm-<STAMP>-events.csv`, `.log` | restarts and cold queries; every pass |
| `tables-<STAMP>.tsv` | the pinned snapshots' state, one line per pin tag × format × table |
| `hot-interleave-<STAMP>.pins` / `snapshot-<STAMP>.pins` | windows, pins, H8 key, writer and generator state, compaction choice |
| hot: `*-p<N>.csv`, `hot-interleave-<STAMP>.log` | every pass (p0 = warm-up) |

Pinned metadata copies go to `s3://$BUCKET_NAME/querybench-pins/cdc-session<set tag><suffix>/<tag>/`.

---

## Decisions for you

1. **Compaction state decides S1/S2/S5/S7 and H8.** The Iceberg phase
   measures the compacted tables by default (its gate). Iceberg `order_items`
   is uncompacted today (124 delete files, 365M equality deletes), and both
   engines OOMed on it in 2026-09. `ICEBERG_REQUIRE_COMPACTION=0` measures it
   uncompacted: expect an `ERROR` on StarRocks and a `CRASH` (OOM kill, then
   container restart) on DuckDB. Report the two as separate results. The hot
   test always reads the live, uncompacted state.
2. **Paimon full compaction is a TODO.** The export arms need it to be
   complete (the INVALID gate). The hook is marked `TODO(paimon-compaction)`
   in `run.sh`. It would be a batch Flink job running `CALL sys.compact(...,
   compact_strategy => 'full')` per table. Until it exists, expect the gate
   to fire if the export lags.
3. **Hot window size (5 min) and snapshot slice (10 min)** are guesses for
   the ~40k rows/s upsert load. Size them from the first run's H1 and S4 row
   counts.
4. **Hot pins per pass (`HOT_REPIN=pass`) vs per run.** Per pass avoids
   Paimon's ~10-minute snapshot expiry. The cost: the timed passes read
   slightly different versions (all recorded).
5. **The watcher suspends the CDC writers after a hot test**, as throughput
   does. Set `SUSPEND_WRITERS=0` / `STOP_GENERATOR=0` if the pipeline must
   keep running.
6. **`SNAPSHOT_DT` = the newest closed date.** Throughput takes the oldest.
   Either works while only `2026-09-27` exists.

## What is verified, what is not

- Verified offline: `bash -n` on every script (shellcheck is not installed
  here); `run.sh render` for both suites, in both pin forms (every hot and
  snapshot query rendered for all 5 arms, no placeholder left); the
  unknown-placeholder failure; `pin.sh` and `qb_commit_ms` against a stubbed
  `aws`; the snapshot slice derivation and the StarRocks row capture, both
  with stubbed engines; `summarize.py` on synthetic results;
  `duckdb_cdc_server.py` compiles. Live reads on 2026-11-02 (read only):
  partition columns, export layout, and Paimon snapshot id = export version.
- Iceberg v3 / DuckDB 2.0 (2026-11-02, offline): `run.sh render` for both
  suites with the v3 default, with `ICEBERG_FORMAT_VERSION=2 CDC_DB_SUFFIX=`
  and with `SR_V3_ARMS=1` (arms, catalogs, database names, prefixes, no
  placeholder left); every rendered DuckDB query parsed and run on DuckDB 2.0.0
  (PyPI wheel) against stand-in tables; `duckdb_cdc_server.py` on 2.0.0
  locally (extension loading, the per-arm puffin setting, the JSON profile's
  bytes and TABLE_SCAN rows, errors from a streamed result).
  `paimon_force_jni_reader` exists in StarRocks 4.1.7's SessionVariable.java.
- **Not verified (needs the cluster):** every Job, engine start and cache
  clear; StarRocks actually accepting `FOR VERSION AS OF … AS o` and native
  Paimon time travel; the IAM permissions above; DuckDB reading these
  exports and equality deletes; the H8 key query's cost; all timings. For v3:
  StarRocks' DV refusal on these tables (taken from its source, not seen
  here), the JNI reader reading Paimon's 64-bit DVs at a pinned version,
  DuckDB 2.0 reading Paimon's v3 export with the puffin check skipped, the
  image's `io.total_bytes_read` over S3 with cache_httpfs `noop` being the
  same byte count as 1.5's HTTPFS `in:` line, and the nodes pulling the ECR
  image.
