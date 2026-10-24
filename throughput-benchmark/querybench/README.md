# Query benchmark: StarRocks vs warm DuckDB on Paimon and Iceberg

The read half of the benchmark: the same queries, on both engines, over the
tables the Flink writers produce. The write half and table set-up are in
[../README.md](../README.md).

| Suite | Reads | Writers | Queries |
|---|---|---|---|
| **hot** | a 30 s `eventTimeDT` window of today's partition (~90M rows) | running | H1–H6 (`hot/`) |
| **snapshot** | a 10-min `eventTimeDT` window of one closed `dt` (~1.8B rows at 3M rows/s); S4 a 1-min slice inside it (25 and 3 min until 2026-10-23) | stopped | S1–S4 (`snapshot/`) |

| Engine | Arm | Reader → data |
|---|---|---|
| StarRocks | `paimon` | Paimon reader → Paimon table |
| StarRocks | `pmn_as_iceberg` | Iceberg reader → Paimon's Iceberg export |
| StarRocks | `iceberg` | Iceberg reader → Iceberg table |
| DuckDB | `paimon` | `iceberg_scan` → Paimon's Iceberg export |
| DuckDB | `iceberg` | `iceberg_scan` → Iceberg table |

- `paimon` vs `pmn_as_iceberg`: only the reader differs.
- `pmn_as_iceberg` vs `iceberg` (and both DuckDB arms): only the writer differs.
- DuckDB has no Paimon reader, so it needs `ICEBERG_COMPAT=true`.

---

## Quick start

```bash
cd throughput-benchmark
./bench.sh hot        # = SUITE=hot ./querybench/run.sh start   (writers RUNNING)
./bench.sh snapshot   # = SUITE=snapshot ./querybench/run.sh start  (after hot)
./querybench/run.sh logs          # follow the runner
./querybench/run.sh logs watch    # follow the watcher
./bench.sh summary    # pulls the results from S3 -> bench-results/BENCH-SUMMARY-<session>.md
```

Before this: the session's tables must hold data (`./bench.sh throughput`,
see [../README.md](../README.md)). Run the generator with `DURATION_SECONDS=0`
(the `.bench-env` value): the hot test can take ~2 h, and the writers must stay
live until its last pass.

Values set on the command line for one run reach the pods, e.g.
`RUNS=1 WARMUP=1 ./bench.sh snapshot`.

---

## What `run.sh start` does

Both tests run as Jobs in the cluster, so closing the laptop does not stop them.
`start` stops at the first failed step, before any Job starts:

1. **Waits while an earlier runner is still running** (e.g. the hot test).
   `FORCE=1` replaces it.
2. **Waits for the old watcher Job to finish its shutdown, then deletes it.** It checks
   the runner only every 10 min, so deleting it right after its runner ended dropped
   that shutdown (monitor and engines left up). Waits up to `WATCH_INTERVAL_S` + 15 min;
   `FORCE=1` skips it.
3. **Snapshot only:** stops the generator, suspends both writers (waits until no
   TaskManager is left), and picks the partition, the S1–S3 window and the S4 slice.
4. **Starts StarRocks and DuckDB** if they aren't up: StarRocks when its FE or
   any of the 3 BEs is not Ready (`../engine-deploy/deploy-starrocks.sh install`,
   ~5 min), DuckDB (`duckdb-warm`) likewise; checks DuckDB's `/health` says
   `busy: false`. New nodes take ~5–10 min.
5. **Empties both caches** (`clear-caches.sh`): each StarRocks BE's datacache
   (delete + restart, one BE at a time), then a DuckDB restart. Checks every BE
   reports `DiskUsage: 0B`. It refuses while a `cid_events` query runs.
   The hot test skips it when step 4 just started both engines from scratch (their caches
   are empty already); the snapshot test always empties them.
6. **Starts the runner Job** (`qb-runner`):
   - hot: `hot-interleave.sh`;
   - snapshot: `starrocks.sh`, then `duckdb-warm.sh run`.

   `ENGINES=duckdb` (or `starrocks`) makes a snapshot run one engine only: the
   other is not started, cleared or queried. Use it to rerun one engine against
   the same tables, e.g. after a DuckDB setting changed (keep the writers stopped
   and set `COMPACT_BEFORE_SNAPSHOT=false`, so the table version and windows stay
   the ones the other engine read). The hot test always needs both.

   At the end it uploads `bench-results/` to
   `s3://$BUCKET_NAME/querybench-results/session<TEST_SUFFIX>/<STAMP>-<suite>/`.
7. **Starts the watcher Job** (`qb-watch.sh`). Every 10 min it checks the
   runner. When the runner ends it stops the generator, suspends the writers,
   scales the monitor to 0, takes DuckDB down and takes StarRocks down
   (`deploy-starrocks.sh down`: FE/BE pods and volumes, so their nodes go too;
   the Helm release and operator stay). `STOP_STARROCKS=0` keeps StarRocks up,
   e.g. to run the snapshot test right after the hot one without a reinstall.

Notes:
- Finished Jobs delete themselves after a day.
- In logs, wait for the `status:` lines (`runner-exit=`, `results-uploaded`,
  `qb-watch-done`). The log also echoes the command, so plain text can match early.
- The laptop's metric collectors are not stopped by the watcher:
  `./monitoring/run.sh stop`.
- DuckDB keeps the `TEST_SUFFIX` it started with. After changing it, run
  `./engine-deploy/deploy-duckdb-warm.sh up`.

---

## Hot test (`hot-interleave.sh`)

1. Waits until both writers are `RUNNING` (once a minute, `STABLE_CHECKS`=10 times).
2. Sets the window to end **5 min ago** (`HOT_TO`), 30 s long.
3. Waits until both tables hold the same, stable row count for it
   (up to `WAIT_COMMIT_S`=5400 s).
4. **Pins one version of each table** (`duckdb-warm.sh pin`): DuckDB reads the
   copied `metadata.json`, StarRocks the same snapshot with `FOR VERSION AS OF`.
5. Runs warm-up, then `RUNS` timed passes. Each pass runs StarRocks (all arms),
   then DuckDB (all arms), so both see the same version and similar write load.
6. Writes the medians, every pass (`-p<N>.csv`, p0 = warm-up) and the pins
   (`.pins`, including whether the generator was live before and after).

Pinning matters: run one engine after the other and the second plans over more
snapshots, under a different write load.

## Snapshot test

- **Partition:** `SNAPSHOT_DT` = the oldest `dt` present in both tables.
- **S1–S3 window:** `SNAPSHOT_MINUTES` (10) of `eventTimeDT`, centred in the
  span both tables were committing, 120 s clear of each end. `0` = the whole
  partition.
  - Why: the partition's size depends on how long the writers ran. 2 h at 3M
    rows/s put ~22B rows in one `dt`, and full-partition S1–S3 ran
    out of BE memory or hit the 1 h timeout. 10 min ≈ 1.8B rows.
- **S4 slice:** `SNAPSHOT_S4_MINUTES` (1), centred inside the S1–S3 window. The
  3-min slice used before read ~340 GB, ~75% of each engine's disk cache, and
  pushed S1–S3 out of it between passes; the 1-min slice reads ~120–160 GB.
- Pin by hand (all `YYYY-MM-DD HH:MM:SS`): example - `SNAPSHOT_DT=2026-10-20
  SNAPSHOT_FROM='2026-10-20 13:25:00' SNAPSHOT_TO='2026-10-20 13:50:00'
  SNAPSHOT_S4_FROM='2026-10-20 13:36:00' SNAPSHOT_S4_TO='2026-10-20 13:39:00'`.
- **Writers must be stopped.** Live, the same query took 89× longer (332 s vs
  3.7 s). If writer state changes during a suite, it is not recorded.

## By hand, from the laptop

The laptop must stay awake, and nothing stops the generator or writers afterwards.

```bash
./engine-deploy/deploy-duckdb-warm.sh up      # start DuckDB
./querybench/clear-caches.sh            # empty both caches
./querybench/hot-interleave.sh          # hot, both engines
SUITE=snapshot ./querybench/starrocks.sh        # suspends the writers itself
SUITE=snapshot ./querybench/duckdb-warm.sh run
./engine-deploy/deploy-duckdb-warm.sh status  # health, busy, cache bytes
./engine-deploy/deploy-duckdb-warm.sh down
```

---

## Fair-comparison rules

| | StarRocks | DuckDB |
|---|---|---|
| Hardware | 3 BEs × 9 cpu / 38 Gi (m5.4xlarge) | one pod, 27 cpu / 114 Gi, 27 threads (m5.8xlarge) |
| Node pool | `driver-nodepool` | `driver-nodepool` |
| Process | always up | one long-lived server |
| Data cache | datacache: 150 GiB disk + 7.34 GB memory per BE (20% of the 34.2 GiB `mem_limit`) (disk: `gp3-starrocks-be`, 283 MB/s / 5333 IOPS, once `engine-deploy/deploy-starrocks.sh` has run) | `cache_httpfs`: 450 GiB disk (gp3-fast) + 22.03 GB memory, plus DuckDB's own external file cache (its default) inside the 44 GB `memory_limit` |
| Timing | FE audit log, planning included | in the server, execute + fetch, planning included |
| Arms | one at a time | one at a time |

- **Caches start empty** for every test, and the same warm-up pass fills them.
- **Same rows:** windows are on `eventTimeDT`, set by the generator, so every
  table holds the same rows in them. Window values are rendered as literals
  (no `NOW()`).
- **Same order:** all queries' warm-up pass first, then each timed pass.
  Result = median of the timed passes.
- **One engine at a time:** a harness refuses if a `cid_events` query runs on
  the FE, DuckDB is busy, or the other harness runs (`ALLOW_CONCURRENT=1` overrides).
- **Same timeout:** 1,800 s per hot query, 3,600 s per snapshot query.
- StarRocks placement and the OpenTelemetry check: see "Placement" in
  [../README.md](../README.md).

### StarRocks specifics (`starrocks.sh`)

- Timed from the FE audit log: a `kubectl exec` costs ~5.5 s, more than many queries.
- A pass whose audit `State` isn't `EOF`/`OK` is an error, so a failed query
  can't score fastest.
- Catalogs: Paimon `filesystem`, Iceberg `glue`, Paimon export `hadoop` (Glue
  can't load Paimon's entry). `starrocks.sh catalogs` creates them.
- Snapshot scans set `connector_io_tasks_per_scan_operator=2` and turn off
  adaptive I/O tasks; S4 uses `1` (`-- bench: io-tasks=1`), or it fills the
  BE's query memory.

### DuckDB specifics (`duckdb-warm.sh`; the server: `../engine-deploy/deploy-duckdb-warm.sh`, `duckdb_warm_server.py`)

- **Cache block size: 256 KiB, the same as StarRocks** (`CACHE_BLOCK_BYTES`, since
  2026-10-25). StarRocks' datacache uses 256 KiB blocks (`datacache_block_size=262144`,
  its default, read from each BE's `/varz` on 4.1.4), so both engines cache in
  blocks of one size. With `cache_httpfs`'s own 512 KiB default, the snapshot
  queries' narrow column reads each pulled a whole block: S1 fetched 87.4 GB for
  the 15.2 GB it needs, and its cached passes read that back at the cache disk's
  850 MB/s limit (~100 s, 1-1.5 of 27 cores busy). 64 KiB was faster still (24.3 GB
  fetched; cached S1/S2/S3 29/35/34 s instead of 101/135/134 s, 2026-10-25) but finer
  than StarRocks' blocks, so it is not used. At 256 KiB (partial run 2026-10-24,
  Paimon arm, same windows and table version), the cached S1/S2/S3/S4 passes took
  62-69 / 78 / 77 / 234 s, against 101 / 135 / 134 / 245 s at 512 KiB. The cold S4
  warm-up took longer (908 s vs 425 s), but warm-up passes are not timed. DuckDB
  results before 2026-10-25 used 512 KiB and are not comparable.
- **Warm-up is the same on both engines.** `starrocks.sh` and `duckdb-warm.sh`
  share `WARMUP=1` / `RUNS=3` (`lib/querybench.sh`), start from caches emptied by
  `clear-caches.sh`, and run each arm pass-major: S1-S4 once as a discarded
  warm-up, then S1-S4 three times timed (median reported). So both are measured
  warm, each from a cache it filled itself the same way.
- **Data scanned** (since 2026-10-24): after an arm's timed passes, each query runs
  once more, untimed, as `EXPLAIN ANALYZE` with the block cache bypassed. Its HTTPFS
  `in:` bytes are what the query reads from S3, the counterpart of StarRocks'
  `ScanBytes`, and go to the CSV's `scan_bytes`. `scan_rows` is the rows the scan
  returns after the filters pushed into it, not StarRocks' rows-read-before-filters.
  DuckDB 1.5.6's JSON profile can't give either (its byte/row counters miss Parquet
  reads, and `custom_profiling_settings` crashes). Runs before 2026-10-24 have no
  DuckDB scan figures.

- One DuckDB connection for the pod's life; one query at a time.
- **Pinned metadata (`PIN_SNAPSHOT=1`):** each query reads a copy of the
  table's `metadata.json`. Iceberg keeps only a few, so the live one 404s mid-scan.
- The `paimon` arm finds the export through `version-hint.text`, not Glue
  (Glue lags behind).
- **ZERO_SCAN:** no rows (or a leading 0/NULL) counts as a failure, not a win.
- A pass gets the query timeout + 5 min. Past that the server restarts and
  loses its cache (the log says so).
- S3 access through IRSA, refreshed every 20 min outside the timer.
- The cache disk must be `gp3-fast` (`../engine-deploy/gp3-fast-sc.yaml`, 850 MB/s): on default
  gp3 the cache was slower than S3.
- The StarRocks BEs' disks: `../engine-deploy/deploy-starrocks.sh` puts them on
  `gp3-starrocks-be`, 283 MB/s / 5333 IOPS each, so the 3 BEs together match
  DuckDB's ~850 MB/s / 16,000 IOPS. Use `install` for a new cluster, or
  `migrate-be-disks` for the running one (one BE at a time). Sessions before
  that ran StarRocks on default gp3 (125 MB/s / 3000 IOPS per BE).

---

## Queries

`hot/*.sql` and `snapshot/*.sql`. File name = query name; name order = run
order. A new file just runs.

| Query | Measures |
|---|---|
| `H1_freshness` | newest row + count. `0` rows = writers down, every hot number is void |
| `H2_hot_count` | `COUNT(*)`: listing and pruning cost |
| `H3_hot_needle_uuid` | a `uuid` predicate that never matches: full scan of one column |
| `H4_hot_narrow_agg` | `GROUP BY` one narrow column: projection pushdown |
| `H5_hot_wide_json` | bytes of `event_json` (~3.3 KB/row): file count and size. `payload_bytes` must match across engines |
| `H6_hot_customer_agg` | `COUNT(DISTINCT)` per customer: hash table, spill |
| `S1_partition_count` | `COUNT(*)` over the snapshot window: the row count to normalise by |
| `S2_narrow_agg` | H4 over the snapshot window |
| `S3_risk_distinct` | `COUNT(DISTINCT customerId)` by risk level: memory pressure |
| `S4_wide_json` | JSON decode over the S4 slice; not comparable with S1–S3 |

Placeholders: `${TABLE}`, `${BYTES_FN}` (`LENGTH` on StarRocks, `STRLEN` on
DuckDB; both count bytes) and the window values. A placeholder left after
rendering stops the run. `-- bench:` flags: `allow-zero-rows`,
`leading-col-is-label`, `io-tasks=1`.

---

## Knobs

| Knob | Default | Meaning |
|---|---|---|
| `SUITE` | — | `hot` or `snapshot` (`run.sh`) |
| `RUNS` / `WARMUP` | `3` / `1` | timed / discarded passes |
| `ARMS` | all three | StarRocks arms |
| `FORMAT` | `both` | DuckDB arms: `iceberg`, `paimon` or `both` |
| `HOT_TO` / `HOT_WINDOW_SECONDS` | now − 5 min / `30` | the hot window |
| `STABLE_CHECKS` / `WAIT_COMMIT_S` | `10` / `5400` | hot: writer and commit waits |
| `SNAPSHOT_DT` | oldest `dt` in both | snapshot partition |
| `SNAPSHOT_MINUTES` / `SNAPSHOT_FROM` / `_TO` | `10` / derived | S1–S3 window; `0` = whole partition |
| `SNAPSHOT_S4_MINUTES` / `_FROM` / `_TO` | `1` / derived | S4 slice |
| `SUSPEND_WRITERS` | `1` | `0` = never touch the writers |
| `WATCH_INTERVAL_S` | `600` | watcher check interval |
| `FORCE` / `ALLOW_CONCURRENT` | off | replace a running runner / skip the one-engine guard |
| `ENGINES` | `starrocks duckdb` | snapshot only: `duckdb` or `starrocks` runs one engine; the other is neither started, cleared nor queried |
| `DUCKDB_MEMORY_LIMIT` / `DUCKDB_THREADS` | `84049MiB` (82.08 GiB) / `27` | 90% of the 114 Gi pod minus the 22.03 GB cache, the same shares as a BE's `mem_limit` |
| `DUCKDB_VERSION` | `1.5.6` | DuckDB 2.0 has no stable release yet |
| `CACHE_MODE` | `on_disk` | `in_mem` = memory cache only |
| `CACHE_BLOCK_BYTES` | `262144` | `cache_httpfs` block size (256 KiB = StarRocks' `datacache_block_size`, not its 512 KiB default; see DuckDB specifics) |
| `PROFILE_SCAN` | `1` | DuckDB's scan profile: one untimed pass per arm after the timed ones (hot: after the last pass); `0` skips it |

---

## Output

In S3 under `querybench-results/session<TEST_SUFFIX>/<STAMP>-<suite>/`:
`starrocks-<STAMP>.csv` and `duckdb_warm-<STAMP>.csv`, one row per arm × query:

```
stamp,engine,arm,suite,query,status,median_ms,result_rows,scan_rows,scan_bytes
```

- `status`: `OK`, `ERROR`, `MISSING` (StarRocks), `TIMEOUT`, `ZERO_SCAN` (DuckDB).
  A failed query has an empty `median_ms`, never 0.
- `scan_rows` / `scan_bytes`: StarRocks only.
- StarRocks `#` lines: writer state, run settings, partition row counts per arm.
- DuckDB `.log`: every pass, with its time, first row and pinned table.
- `*-firstrows.csv`: first result rows, which the summary uses to check S1/S4
  counts match across arms (a mismatch marks the run INVALID).

`./bench.sh summary` turns them into the session's findings. To compare one
suite by hand (only CSVs from the same run):

```bash
aws s3 sync s3://$BUCKET_NAME/querybench-results/session<TEST_SUFFIX>/<STAMP>-snapshot/ /tmp/qb/
python3 querybench/compare.py --sr /tmp/qb/starrocks-<STAMP>.csv \
    --duck /tmp/qb/duckdb_warm-<STAMP>.csv --suite snapshot
```

The DuckDB log can contain the bucket name; mask it in anything you commit.

---

## How long it takes

**Hot** (`RUNS=3 WARMUP=1`, 2026-10-18): ~45 min.

| Arm | Time |
|---|---|
| StarRocks `paimon` / `pmn_as_iceberg` / `iceberg` | ~5.5 / ~6.5 / ~15.5 min |
| DuckDB `paimon` / `iceberg` | ~9 / ~9 min |

H5 dominates (75–95 s per pass); the rest takes seconds once warm.

**Snapshot** (`RUNS=3 WARMUP=1`, run `20261023T003027Z`): **~4.5 h** of queries
after ~6 min of stopping writers and emptying caches. Compacted `-final2` tables,
S1–S3 over 10 min of `eventTimeDT` (1.80B rows), S4 over 1 min (181M rows);
StarRocks 3 BEs × 9 cpu / 38 Gi, DuckDB 27 cpu / 114 Gi.

| Arm | Time (warm-up + 3 timed passes) |
|---|---|
| StarRocks `paimon` / `pmn_as_iceberg` / `iceberg` | 38 / 40 / 81 min (2,288 / 2,379 / 4,849 s) |
| DuckDB `paimon` / `iceberg` | 56 / 50 min (3,389 / 3,006 s) |

Median of the timed passes, seconds:

| Query | SR `paimon` | SR `pmn_as_iceberg` | SR `iceberg` | DuckDB `paimon` | DuckDB `iceberg` |
|---|---|---|---|---|---|
| S1 | 36.0 | 15.5 | 41.9 | 106.5 | 90.2 |
| S2 | 11.5 | 31.1 | 52.6 | 139.7 | 121.0 |
| S3 | 14.0 | 46.6 | 49.5 | 139.2 | 120.2 |
| S4 | 280.1 | 244.5 | 314.2 | 245.6 | 219.5 |

Data scanned per query, StarRocks (GB; DuckDB recorded none until 2026-10-24, see DuckDB specifics):

| Query | SR `paimon` | SR `pmn_as_iceberg` | SR `iceberg` |
|---|---|---|---|
| S1 | 8.8 | 15.6 | 63.6 |
| S2 | 9.0 | 20.5 | 74.8 |
| S3 | 8.8 | 20.1 | 68.9 |
| S4 | 121.7 | 126.1 | 156.2 |
| one pass (S1–S4) | ~148 | ~182 | ~364 |

A pass now fits each engine's 450 GiB disk cache, so the timed passes are cache
hits; the warm-up pass, from S3, is the slowest (e.g. StarRocks `iceberg` S2:
1,661 s cold, 44 s warm). StarRocks reads 2.8× more bytes from the Iceberg table
than from Paimon's export (geometric mean).

Before (25-min S1–S3, 3-min S4, uncompacted, BEs 6 cpu / 24 Gi, DuckDB 18 cpu /
72 Gi, 2026-10-21): StarRocks ~10.5 h (171 / 190 / ~190 min per arm), DuckDB ~6 h;
a pass read ~450 GB per arm, about the whole 450 GiB cache.

---

## Check before trusting a run

- **H1 shows rows,** and the `.pins` file says the generator was `live` before
  and after (hot).
- **S1 and S4 counts match** across arms (the summary checks this).
- **No pass hit S3 503 (SlowDown).** On 2026-10-18, with 8 generator pods, S3
  throttled the Iceberg writer and it restarted. Don't time queries across a
  writer restart.
