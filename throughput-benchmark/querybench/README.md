# Query benchmark: StarRocks vs warm DuckDB on Paimon and Iceberg

Step-by-step guide for running the **hot** and **snapshot** query suites. This
covers the read half; the write half and the table set-up are in
[../README.md](../README.md).

| Suite | What it reads | Writers | Queries |
|---|---|---|---|
| **hot** | one 30 s `eventTimeDT` window of today's partition (~90M rows at ~3M rows/s) | **running** | H1–H6 (`hot/`) |
| **snapshot** | one closed `dt` partition (4.64B rows in the 2026-10-10 run); S4 a 3-min `eventTimeDT` slice (~463M rows) | **stopped** | S1–S4 (`snapshot/`) |

**Arms:**

| Engine | Arm | Reader → data |
|---|---|---|
| StarRocks | `paimon` | native Paimon reader → Paimon table |
| StarRocks | `pmn_as_iceberg` | Iceberg reader → Paimon's Iceberg export |
| StarRocks | `iceberg` | Iceberg reader → native Iceberg table |
| warm DuckDB | `paimon` | `iceberg_scan` → Paimon's Iceberg export (pairs with StarRocks `pmn_as_iceberg`) |
| warm DuckDB | `iceberg` | `iceberg_scan` → native Iceberg table |

**Files:**
- **Harnesses:** `starrocks.sh` and `duckdb-warm.sh`. Both source `lib.sh`, which
  holds the shared windows, rendering, timeouts, median, CSV format, writer
  stop and one-engine-at-a-time guard.
- **Warm DuckDB server:** `duckdb_warm_server.py` and `duckdb-warm.yaml`.
- **Cache disk:** `gp3-fast-sc.yaml`.
- **Running without the laptop:** `qb-runner.sh` and `qb-runner.yaml`.
- **Reports:** `compare.py`.

## Fair-comparison rules (the harnesses enforce what they can)

- **Same hardware.**
  - **StarRocks:** 3 BEs × 6 cpu / 24 Gi.
  - **Warm DuckDB:** one pod of 18 cpu / 72 Gi.
  - **Both:** m5, on Karpenter's `driver-nodepool`, with no OpenTelemetry agent.
  - **Where it's set:** StarRocks' placement is in
    `cdc-pipeline/helm/starrocks-eks-values.yaml`, DuckDB's in
    `duckdb-warm.yaml`.
- **Same cache.** Both engines' caches are on, as their defaults.
  - **StarRocks datacache:** 3 × 150 GiB disk + ~3.8 GB memory per BE.
  - **DuckDB `cache_httpfs`:** 450 GiB disk on gp3-fast + 12.5 GB memory.
  - **Start them empty for every scored run** (step 3).
- **Same rows.** Both windows are on `eventTimeDT`, which the generator sets, so
  every format holds exactly the same rows in them.
- **Same passes.** Both engines run every query's warm-up pass first, then
  every timed pass: S1–S4 warm-up, then S1–S4 timed. The result is the
  median of the timed passes.
- **One engine at a time.** `qb_assert_alone` refuses to start if any of these
  is true (`ALLOW_CONCURRENT=1` overrides):
  - a `cid_events` query is running on the FE;
  - a DuckDB server is busy;
  - the other harness is running.
- **Writer state unchanged.** If the writers change state during a suite, the
  suite is not recorded.

## Step by step

### 0. Once per session

```bash
cd throughput-benchmark
source .bench-env                      # BUCKET_NAME etc. (+ .bench-env.local)
export CID_SUFFIX=-r2                  # which table set to query; empty = the original
                                       # set. Must match build-deploy-cid.sh's.
```

### 1. Tables with data

The tables come from the write half. Deploy the writers with the same
`CID_SUFFIX`, then start the generator once both jobs are `RUNNING`:

```bash
CID_SUFFIX=$CID_SUFFIX PAIMON_WRITE_ONLY=true PAIMON_WRITE_BUFFER_SPILLABLE=false \
  ./build-deploy-cid.sh deploy both
# datagen: with 7 workers, run the job nonstop until manually stop  
JSONGZ_REPLICAS=7 DURATION_SECONDS=0 ./jsongz-gen/run-jsongz-gen.sh start      
```

- **Hot runs: run the generator until the hot test is done.** The writers must
  stay live until the last pass ends, and how long the test takes depends on
  the queries. On 2026-10-19 it took ~2 h from the generator's start, and a
  1-hour generator stopped during the second timed pass. So start it with
  `DURATION_SECONDS=0`, and run `hot-watch.sh` (step 4a) to stop it and the
  writers afterwards. Without a watcher, run `./jsongz-gen/run-jsongz-gen.sh stop`
  when done: nothing stops it by itself.

- **7 replicas** give ~3.0M rows/s.
- **Snapshot** needs a closed partition, so run it after the generator has
  finished and the jobs have drained. Check that the SQS queues are at 0.

### 2. Start the warm DuckDB server for the suite you run

There is one server per suite (`duckdb-warm-hot`, `duckdb-warm-snapshot`).
Start only the one you need: `up` follows `SUITE`. It stays up across runs.
Allow ~5–10 min for Karpenter to launch the node, plus pip and the extensions.

```bash
SUITE=hot CID_SUFFIX=$CID_SUFFIX ./querybench/duckdb-warm.sh up   # only duckdb-warm-hot
./querybench/duckdb-warm.sh status                                # busy=false, cache_mode=on_disk
./querybench/duckdb-warm.sh down snapshot                         # if the other one is still up
```

StarRocks is one cluster for both suites. `starrocks.sh` runs only `SUITE`.

`CID_SUFFIX` is baked into the servers, so re-run `up` whenever it changes.
That restarts them and empties their caches.

### 3. Empty both caches

```bash
# StarRocks: the datacache survives BE restarts, so empty it, then restart each BE.
for b in 0 1 2; do p=kube-starrocks-be-$b
  kubectl exec -n emr-flink $p -c be -- sh -c 'rm -rf /opt/starrocks/be/storage/datacache/*'
  kubectl delete pod -n emr-flink $p --wait=true && sleep 10
  kubectl wait -n emr-flink --for=condition=Ready pod/$p --timeout=600s
done
# DuckDB
SUITE=hot ./querybench/duckdb-warm.sh clear-cache   # the suite's server only
# Check: every BE shows DiskUsage 0B.
kubectl exec -n emr-flink kube-starrocks-fe-0 -c fe -- mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" | grep DataCacheMetrics
```

Also check that no StarRocks pod carries the OpenTelemetry agent; see
"Placement" in [../README.md](../README.md).

### 4a. Hot suite: writers running

**Start only once both Flink jobs are up and stable.** They must have been
`RUNNING` without a break for **10 minutes**, with the generator writing. A
restart or a job still starting up resets the 10 minutes. Before then the jobs
are still ramping up or catching up a backlog, so the newest data is neither
complete nor written at a steady rate. The in-cluster runner below waits for
this itself:

```bash
stable=0; while [ $stable -lt 600 ]; do
  s=$(kubectl get flinkdeployment -n emr-flink -o jsonpath='{.items[*].status.jobStatus.state}')
  [ "$s" = "RUNNING RUNNING" ] && stable=$((stable+30)) || stable=0; sleep 30
done
```

Then pin the window **5 minutes back**, so both tables have committed it. With a
backlog, check first that both tables hold the same count for it: the runner
example in step 5 does this automatically. Then run the engines one after the
other, on the **same** `HOT_TO`:

```bash
export SUITE=hot RUNS=3 WARMUP=1 STAMP=$(date -u +%Y%m%dT%H%M%SZ)
export HOT_TO="$(date -u -v-5M '+%Y-%m-%d %H:%M:%S')"      # GNU: date -u -d '5 minutes ago' ...
./querybench/starrocks.sh
./querybench/duckdb-warm.sh run
```

**Recommended for comparing the engines: `hot-interleave.sh`.** Pinning the
window fixes the rows but not the table. Run back to back, StarRocks finishes
before DuckDB starts, so DuckDB plans over more snapshots and manifests and
runs under different write load. On 2026-10-19 StarRocks ran during the backlog
drain and DuckDB against idle writers. `hot-interleave.sh` does the whole hot
comparison in one go:
1. **Waits for the window to be committed** in both tables.
2. **Pins one version of each table:** `duckdb-warm.sh pin` copies one
   `metadata.json` per table for DuckDB and reports the matching snapshot IDs.
   StarRocks reads those IDs with `FOR VERSION AS OF`.
3. **Checks StarRocks reads the window's rows at each pinned version.** If its
   native Paimon reader can't time-travel, that one arm runs unpinned and the
   log says so.
4. **Runs warm-up, then timed passes 1..`RUNS`.** Each pass runs on StarRocks
   (all arms), then on DuckDB (all arms), so both engines see the same table
   version and about the same write load.
5. **Merges the timed passes** into one median CSV per engine.
6. **Records whether the generator was writing** before the first pass and
   after the last, in the `.pins` file. It's a writers-live result only if both
   say `live`.

```bash
SUITE=hot ./querybench/duckdb-warm.sh up          # if not already up
HOT_TO="$(date -u -v-5M '+%Y-%m-%d %H:%M:%S')" ./querybench/hot-interleave.sh
# or in the cluster (survives the laptop), after the 10-minute stability wait:
./querybench/qb-runner.sh start 'HOT_TO="$(date -u -d "5 minutes ago" "+%Y-%m-%d %H:%M:%S")" ./querybench/hot-interleave.sh'
```

Then start the watcher in a second in-cluster Job. Every 10 minutes
(`WATCH_INTERVAL_S=600`) it checks the runner's log. Once the runner prints
`status: runner-exit=`, or its Job or pod is gone or failed, the watcher deletes
the generator Job, suspends both Flink jobs, scales the job monitor to 0 once
they have stopped, and takes `duckdb-warm-hot` down. The metric collectors run
on the laptop, so end them there with `./monitoring/deploy-cid-monitor.sh stop`:

```bash
QB_RUNNER_NAME=hot-watch ./querybench/qb-runner.sh start ./querybench/hot-watch.sh
QB_RUNNER_NAME=hot-watch ./querybench/qb-runner.sh logs     # "status: hot-watch-done" when it has stopped them
QB_RUNNER_NAME=hot-watch ./querybench/qb-runner.sh fetch    # afterwards: copies its (empty) results, deletes the Job
```

Outputs:
- **Medians:** `bench-results/starrocks-<STAMP>.csv` and
  `duckdb_warm-<STAMP>.csv`.
- **Per pass:** `…-<STAMP>-p<N>.csv`, where p0 is the warm-up.
- **Pinned versions:** `hot-interleave-<STAMP>.pins`.

### 4b. Snapshot suite: writers stopped

Each harness suspends the writers itself (`qb_stop_writers`). Pin the
partition and the S4 slice, so both engines read the same rows. S4 can also be
derived from the commit history by setting `SNAPSHOT_S4_MINUTES` instead.

```bash
export SUITE=snapshot RUNS=1 WARMUP=1 STAMP=$(date -u +%Y%m%dT%H%M%SZ)
export SNAPSHOT_DT=2026-10-10 SNAPSHOT_S4_FROM='2026-10-10 15:06:00' SNAPSHOT_S4_TO='2026-10-10 15:09:00'
./querybench/starrocks.sh
./querybench/duckdb-warm.sh run
```

### 5. Run it in the background (survives closing the laptop)

A snapshot run takes hours, so run it from the in-cluster Job instead of the
laptop. Each StarRocks arm already runs detached inside the FE pod.
`qb-runner.sh` also moves the harness itself into the cluster:

```bash
./querybench/qb-runner.sh start "CID_SUFFIX=$CID_SUFFIX STAMP=$STAMP SUITE=snapshot RUNS=1 WARMUP=1 \
  SNAPSHOT_DT=2026-10-10 SNAPSHOT_S4_FROM='2026-10-10 15:06:00' SNAPSHOT_S4_TO='2026-10-10 15:09:00' \
  ./querybench/starrocks.sh; ./querybench/duckdb-warm.sh run"
./querybench/qb-runner.sh logs          # follow it; the step's end is the "status: runner-exit=" line
./querybench/qb-runner.sh fetch         # copy its bench-results/ to ../bench-results/, then delete the Job (it waits up to 1 day for a fetch)
```

- **No AWS credentials in the pod.** The runner has none, so pin every window
  (`SNAPSHOT_DT`, `SNAPSHOT_S4_FROM`/`_TO`, `HOT_TO`); otherwise `lib.sh` would
  call `aws`.
- **Waiting on a StarRocks run:** the helper `wait_for_sr_run <RUN_ID>` waits
  for a running `starrocks.sh` to finish in the FE pod first.
- **The pod's log echoes the whole command at the start.** When waiting on the
  log, match the `status:` lines, not bare text such as `duckdb_warm-exit=`.

### 6. Compare and record

```bash
python3 querybench/compare.py --sr bench-results/starrocks-$STAMP.csv \
    --duck bench-results/duckdb_warm-$STAMP.csv --suite snapshot
```

- **Pair only CSVs from the same run.** They need the same window, caches and
  hardware.
- **DuckDB's pass log can name the bucket.** Don't commit it.

## How long each arm takes

Measured times. Each arm runs in the background, and the engines run one after
the other, so a suite's total is the sum of its arms.

**Hot**: 30 s window, ~90M rows, `RUNS=3 WARMUP=1`, writers live
(StarRocks: run `20261018T114000Z`):

| Arm | Time | Note |
|---|---|---|
| StarRocks `paimon` | ~5.5 min (332 s) | |
| StarRocks `pmn_as_iceberg` | ~6.5 min (392 s) | |
| StarRocks `iceberg` | ~15.5 min (935 s) | reads 3–7× more bytes per window |
| warm DuckDB `paimon` | ~9 min | ~6 min measured at 1 + 1 passes, plus ~1.4 min per extra timed pass |
| warm DuckDB `iceberg` | ~9 min | as above |
| **Total** | **~45 min** | plus ~10 min of steady writing before the window, and the committed-window check |

- **H5 dominates on both engines.** It decodes `event_json`, 75–95 s per pass.
- **Everything else is seconds** once the cache is warm.

**Snapshot**: 4.64B-row partition, `RUNS=1 WARMUP=1`, writers stopped (run
`20261017T140929Z`):

| Arm | Time | Slowest queries (warm-up / timed) |
|---|---|---|
| StarRocks `paimon` | ~68 min (4,087 s) | S2 721 / 59 s, S4 1,131 / 800 s |
| StarRocks `pmn_as_iceberg` | ~58 min (3,491 s) | S3 502 / 316 s, S4 1,044 / 1,055 s; S1 is 0.4 s (from metadata) |
| StarRocks `iceberg` | ~86 min (5,164 s) | S3 792 / 768 s, S4 1,279 / 1,185 s |
| warm DuckDB `paimon` | ~67 min (3,992 s) | S1 1,032 / 82 s, S2 919 / 161 s, S4 1,014 / 462 s |
| warm DuckDB `iceberg` | ~86 min (5,151 s) | S2 1,158 / 292 s, S4 1,134 / 629 s |
| **Total** | **~6 h** | StarRocks ~3.5 h, then DuckDB ~2.5 h |

**How the times scale:**
- **Warm-up passes** read from S3 into empty caches, so they take most of the
  time.
- **Each extra timed pass** (`RUNS`) adds roughly the timed column per query.
  For snapshot that's +20–40 min per arm per pass.
- **S1–S3 scale with the partition size.** S4 scales with its slice
  (`SNAPSHOT_S4_MINUTES`).
- **Per-query cap:** 1,800 s for hot and 3,600 s for snapshot
  (`qb_query_timeout`). DuckDB also has a client deadline 5 min past it.

## Known issues to check before trusting a run

- **StarRocks S4 needs `io-tasks=1`.** With the cache on and 2 scan tasks it
  filled the 17.5 GiB query pool and failed (`MEM_LIMIT_EXCEEDED`). The S4 file
  carries `-- bench: io-tasks=1`.
- **DuckDB's cache disk must be gp3-fast.** On default gp3 (125 MB/s) its cache
  served hits slower than S3.
- **Check the hot window is committed before running.** If the jobs have a
  backlog, the two tables may not hold the window's rows yet. Count it in both
  until the counts match and stay the same.
- **S3 503 (SlowDown).** With 8 generator pods and both writers at ~3M rows/s,
  S3 throttled the Iceberg writer once (2026-10-18), and it restarted. Don't
  time queries during such a restart.
