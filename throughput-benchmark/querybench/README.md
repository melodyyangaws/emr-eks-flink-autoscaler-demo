# Query benchmark: StarRocks vs warm DuckDB on Paimon and Iceberg

The read half of the benchmark: the same queries, on both engines, over the
tables the Flink writers produce. The write half and table set-up are in
[../README.md](../README.md).

| Suite | Reads | Writers | Queries |
|---|---|---|---|
| **hot** | a 30 s `eventTimeDT` window of today's partition (~90M rows) | running | H1–H6 (`hot/`) |
| **snapshot** | one closed `dt` partition (~4.6B rows); S4 a 3-min slice | stopped | S1–S4 (`snapshot/`) |

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
2. **Deletes the old watcher Job.**
3. **Snapshot only:** stops the generator, suspends both writers (waits until no
   TaskManager is left), and picks the partition and S4 slice.
4. **Starts DuckDB** (`duckdb-warm`) if it isn't up, and checks `/health` says
   `busy: false`. A new node takes ~5–10 min.
5. **Empties both caches** (`clear-caches.sh`): each StarRocks BE's datacache
   (delete + restart, one BE at a time), then a DuckDB restart. Checks every BE
   reports `DiskUsage: 0B`. It refuses while a `cid_events` query runs.
6. **Starts the runner Job** (`qb-runner`):
   - hot: `hot-interleave.sh`;
   - snapshot: `starrocks.sh`, then `duckdb-warm.sh run`.

   At the end it uploads `bench-results/` to
   `s3://$BUCKET_NAME/querybench-results/session<TEST_SUFFIX>/<STAMP>-<suite>/`.
7. **Starts the watcher Job** (`qb-watch.sh`). Every 10 min it checks the
   runner. When the runner ends it stops the generator, suspends the writers,
   scales the monitor to 0 and takes DuckDB down.

Notes:
- Finished Jobs delete themselves after a day.
- In logs, wait for the `status:` lines (`runner-exit=`, `results-uploaded`,
  `qb-watch-done`). The log also echoes the command, so plain text can match early.
- The laptop's metric collectors are not stopped by the watcher:
  `./monitoring/run.sh stop`.
- DuckDB keeps the `TEST_SUFFIX` it started with. After changing it, run
  `./querybench/duckdb-warm.sh up`.

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
- **S4 slice:** `SNAPSHOT_S4_MINUTES` (3) of `eventTimeDT`, centred in the span
  both tables were committing, 120 s clear of each end.
- Pin either by hand: `SNAPSHOT_DT=2026-10-10 SNAPSHOT_S4_FROM='2026-10-10 15:06:00' SNAPSHOT_S4_TO='2026-10-10 15:09:00'`.
- **Writers must be stopped.** Live, the same query took 89× longer (332 s vs
  3.7 s). If writer state changes during a suite, it is not recorded.

## By hand, from the laptop

The laptop must stay awake, and nothing stops the generator or writers afterwards.

```bash
./querybench/duckdb-warm.sh up          # start DuckDB
./querybench/clear-caches.sh            # empty both caches
./querybench/hot-interleave.sh          # hot, both engines
SUITE=snapshot ./querybench/starrocks.sh        # suspends the writers itself
SUITE=snapshot ./querybench/duckdb-warm.sh run
./querybench/duckdb-warm.sh status      # health, busy, cache bytes
./querybench/duckdb-warm.sh down
```

---

## Fair-comparison rules

| | StarRocks | DuckDB |
|---|---|---|
| Hardware | 3 BEs × 6 cpu / 24 Gi (m5) | one pod, 18 cpu / 72 Gi (m5.8xlarge) |
| Node pool | `driver-nodepool` | `driver-nodepool` |
| Process | always up | one long-lived server |
| Data cache | datacache: 150 GiB disk + ~4.2 GB memory per BE | `cache_httpfs`: 450 GiB disk (gp3-fast) + 12.5 GB memory |
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

### DuckDB specifics (`duckdb-warm.sh`, `duckdb_warm_server.py`)

- One DuckDB connection for the pod's life; one query at a time.
- **Pinned metadata (`PIN_SNAPSHOT=1`):** each query reads a copy of the
  table's `metadata.json`. Iceberg keeps only a few, so the live one 404s mid-scan.
- The `paimon` arm finds the export through `version-hint.text`, not Glue
  (Glue lags behind).
- **ZERO_SCAN:** no rows (or a leading 0/NULL) counts as a failure, not a win.
- A pass gets the query timeout + 5 min. Past that the server restarts and
  loses its cache (the log says so).
- S3 access through IRSA, refreshed every 20 min outside the timer.
- The cache disk must be `gp3-fast` (`gp3-fast-sc.yaml`, 1000 MB/s): on default
  gp3 the cache was slower than S3.

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
| `S1_partition_count` | full-partition `COUNT(*)`: the row count to normalise by |
| `S2_narrow_agg` | H4 over the whole partition |
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
| `SNAPSHOT_S4_MINUTES` / `_FROM` / `_TO` | `3` / derived | S4 slice |
| `SUSPEND_WRITERS` | `1` | `0` = never touch the writers |
| `WATCH_INTERVAL_S` | `600` | watcher check interval |
| `FORCE` / `ALLOW_CONCURRENT` | off | replace a running runner / skip the one-engine guard |
| `DUCKDB_MEMORY_LIMIT` / `DUCKDB_THREADS` | `44GB` / `18` | under the 72 Gi pod limit (the cache is counted separately) |
| `DUCKDB_VERSION` | `1.5.6` | DuckDB 2.0 has no stable release yet |
| `CACHE_MODE` | `on_disk` | `in_mem` = memory cache only |

---

## Output

`bench-results/starrocks-<STAMP>.csv` and `duckdb_warm-<STAMP>.csv`, one row
per arm × query:

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

Compare one suite side by side (only CSVs from the same run):

```bash
python3 querybench/compare.py --sr bench-results/starrocks-<STAMP>.csv \
    --duck bench-results/duckdb_warm-<STAMP>.csv --suite snapshot
```

The DuckDB log can contain the bucket name. `summary/run.sh` masks it; mask it
yourself in anything else you commit.

---

## How long it takes

**Hot** (`RUNS=3 WARMUP=1`, 2026-10-18): ~45 min.

| Arm | Time |
|---|---|
| StarRocks `paimon` / `pmn_as_iceberg` / `iceberg` | ~5.5 / ~6.5 / ~15.5 min |
| DuckDB `paimon` / `iceberg` | ~9 / ~9 min |

H5 dominates (75–95 s per pass); the rest takes seconds once warm.

**Snapshot** (`RUNS=1 WARMUP=1`, 4.64B rows, 2026-10-17): ~6 h.

| Arm | Time |
|---|---|
| StarRocks `paimon` / `pmn_as_iceberg` / `iceberg` | ~68 / ~58 / ~86 min |
| DuckDB `paimon` / `iceberg` | ~67 / ~86 min |

The warm-up pass (empty caches, reads S3) takes most of the time. Each extra
timed pass adds ~20–40 min per arm.

---

## Check before trusting a run

- **H1 shows rows,** and the `.pins` file says the generator was `live` before
  and after (hot).
- **S1 and S4 counts match** across arms (the summary checks this).
- **No pass hit S3 503 (SlowDown).** On 2026-10-18, with 8 generator pods, S3
  throttled the Iceberg writer and it restarted. Don't time queries across a
  writer restart.
