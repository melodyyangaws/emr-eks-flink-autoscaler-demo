# engine-deploy: the CDC query benchmark's engines

| Engine | Script | Manifest / values | Name in `emr-flink` |
|---|---|---|---|
| DuckDB 2.0.0 warm server | `deploy-duckdb-cdc.sh` | `duckdb-cdc.yaml`, `duckdb_cdc_server.py`, image from [duckdb-image/](duckdb-image/README.md) | Deployment `duckdb-warm-cdc` |
| Trino 484 | `deploy-trino.sh` | `trino-values.yaml`, chart `trinodb/trino` 1.45.0 | Helm release `trino`: `trino-coordinator`, `trino-worker`, Service `trino:8080` |

Both scripts read `cdc-pipeline/.bench-env` (+ `.bench-env.local`) through
`lib/common.sh` and deploy into `NAMESPACE` (`emr-flink`). `*-deployed.yaml`
files are rendered output and git-ignored. Run everything from `cdc-pipeline/`.

`querybench/run.sh start` brings both engines up by itself when needed, and
the watcher takes them down after a completed test (DuckDB deleted, Trino
scaled to 0). Use the steps below to start, check or remove an engine by hand.

## Shared prerequisites

1. **IRSA ServiceAccount `starrocks-sa`** in `emr-flink` (historical name; the
   DuckDB server and Trino both run as it, `TRINO_SERVICE_ACCOUNT` overrides it
   for Trino). Its role (`emr-on-eks-test-execution-role`, checked
   2026-11-02) has Glue `GetDatabase(s)`, `GetTable(s)`, `GetPartition(s)` on
   `*` and S3 Get/List on the benchmark bucket. If it is missing:
   ```bash
   kubectl create serviceaccount starrocks-sa -n emr-flink
   kubectl annotate serviceaccount starrocks-sa -n emr-flink eks.amazonaws.com/role-arn="$EMR_EXECUTION_ROLE_ARN"
   ```
2. **StorageClass `gp3-fast`** (gp3, 16000 IOPS, 850 MB/s, ext4, encrypted).
   `up` applies `../throughput-benchmark/engine-deploy/gp3-fast-sc.yaml` if it
   is missing.
3. **Karpenter `driver-nodepool`** with on-demand amd64 m5 nodes: m5.8xlarge
   (DuckDB), m5.4xlarge (each Trino worker), m5.2xlarge (Trino coordinator).
4. Laptop: `kubectl`, `envsubst` (`brew install gettext`), `helm` for Trino.

## Trino

### Steps

```bash
./engine-deploy/deploy-trino.sh render        # 1. trino-values-deployed.yaml only; no cluster call
./engine-deploy/deploy-trino.sh up            # 2. wait for the writers (30 min), helm upgrade --install, wait, print version()
./engine-deploy/deploy-trino.sh status        # 3. check
./engine-deploy/deploy-trino.sh sql "SELECT count(*) FROM iceberg.flink_iceberg_db<suffix>.orders"   # 4. smoke test
./engine-deploy/deploy-trino.sh clear-cache   # cold start (querybench does it itself)
./engine-deploy/deploy-trino.sh down          # remove the release and leftover cache PVCs
```

1. **render** fills `trino-values.yaml` (envsubst over the computed sizing)
   into `trino-values-deployed.yaml` and logs the shape (workers, heap,
   max-memory-per-node, `query.max-memory`). Use it to check a sizing change.
2. **up** first waits until both writers (`flink-cdc-paimon`, `flink-cdc-iceberg`) have been RUNNING for
   `TRINO_AFTER_WRITERS_S` (default `WRITERS_UP_S`, 1800 s) since its current
   job start (a restart resets it), so the writers start up without Trino on
   the nodes (`wait_writers_up` in `lib/common.sh`, shared with `run.sh start`).
   Suspended writers are skipped, a writer that is not deployed stops `up`, and
   after `WRITER_WAIT_MAX_S` (7200 s) it gives up; `TRINO_AFTER_WRITERS_S=0`
   skips the wait. Then it checks the StorageClass and the ServiceAccount, renders, runs
   `helm upgrade --install trino <chart URL> -n emr-flink -f trino-values-deployed.yaml`
   with a private, empty helm repo config (no `helm repo add`, no stale repo
   cache), then waits up to `READY_GRACE` (1200 s) for both Deployments and
   `TRINO_WORKERS` active workers. Done: `trino <version> up: 3 workers, Service trino.emr-flink:8080`.
3. **status** prints the pods, `version()`, every node from
   `system.runtime.nodes` (coordinator + 3 workers, all `active`), the
   `flink_*` schemas `SHOW SCHEMAS FROM iceberg` sees (both
   `flink_iceberg_db<suffix>` and `flink_paimon_db<suffix>` should be there),
   and `/cache` use per pod. `not running` = no release.
4. **sql** `'<SQL>' [format]` runs one statement through the coordinator's CLI
   (TSV, no header, unless a format such as `TSV_HEADER` or `JSON` is given;
   `TRINO_SOURCE` tags it). **qstats** `<query_id>` prints one query's
   server-side timing and `queryStats` (what the harness records).
- **clear-cache** empties `/cache` in every worker (and the coordinator,
  unless `CLEAR_COORDINATOR=0`), rollout-restarts them onto fresh ephemeral
  volumes, and waits for all workers: about 1–3 min.
- **down** = `helm uninstall trino --wait` + delete PVCs labelled
  `trino.cdcbench/cache=true`. After a watcher teardown the release is still
  installed at 0 replicas; `run.sh start` scales it back before `up`.

### Versions

- **Trino 484** (2026-10-15), image `trinodb/trino:484` (tag pinned:
  `TRINO_VERSION`). `querybench/run.sh start` refuses an already-running
  Trino of another version (`TRINO_VERSION_CHECK=0` skips).
- **Chart 1.45.0** (`TRINO_CHART_VERSION`) = appVersion 484 (1.44.0 is 483),
  pulled by URL: `github.com/trinodb/charts/releases/download/trino-1.45.0/trino-1.45.0.tgz`
  (`TRINO_CHART` overrides).
- **Iceberg v3:** nothing to set. The 484 connector reads v3 (deletion
  vectors, equality deletes); its docs call v3 support experimental. The
  Paimon export tables are read through the same `iceberg` catalog (Glue
  `table_type=ICEBERG`); Trino 484 has no Paimon connector.

### Sizing

Parity with the DuckDB server (27 cpu / 114 Gi on one m5.8xlarge): 3 workers
× 9 cpu / 38 Gi, requests = limits, one worker per node (pod anti-affinity),
plus a coordinator that runs no splits (`node-scheduler.include-coordinator=false`).
Placement: `driver-nodepool`, m5, amd64, on-demand, `do-not-disrupt`.

| | coordinator | worker (each) |
|---|---|---|
| node | m5.2xlarge (an m5.xlarge cannot fit 4 cpu) | m5.4xlarge |
| cpu / memory (req = limit) | 4 / 16 Gi | 9 / 38 Gi |
| `-Xmx` (`TRINO_HEAP_PCT` 80 % of memory) | 13107M | 31129M |
| `query.max-memory-per-node` (`TRINO_QMEM_PCT` 65 % of heap) | 8519MB | 20233MB |
| `memory.heap-headroom-per-node` (`TRINO_HEADROOM_PCT` 30 % of heap) | 3932MB | 9338MB |
| non-heap left | 3.2 GiB | 7.6 GiB |

`query.max-memory` = 3 × 20233MB = 60699MB. Spill is off. The script
computes these from `TRINO_WORKERS`, `TRINO_WORKER_CPU`,
`TRINO_WORKER_MEMORY_GI`, `TRINO_WORKER_INSTANCE_SIZE`, `TRINO_COORD_CPU`,
`TRINO_COORD_MEMORY_GI`, `TRINO_COORD_INSTANCE_SIZE` and the three
percentages (65 + 30 must stay ≤ 100), e.g.
`TRINO_WORKERS=4 TRINO_WORKER_CPU=6 TRINO_WORKER_MEMORY_GI=28 ./engine-deploy/deploy-trino.sh render`.
A changed `TRINO_WORKERS` must also be set for `querybench/run.sh` (its
readiness check).

### Catalog and cache

One catalog, `iceberg` (the chart's `tpch`/`tpcds` removed):
`iceberg.catalog.type=glue`, `hive.metastore.glue.use-web-identity-token-credentials-provider=true`,
`fs.native-s3.enabled=true`, `fs.cache.enabled=true`.

File system cache (parity with DuckDB's 450 GiB on-disk cache):

- Since 484 the cache manager is per node: every node loads
  `/etc/trino/cache-manager-alluxio.properties` (`cache-manager.name=alluxio`,
  `fs.cache.directories=/cache`, `fs.cache.max-disk-usage-percentages=90`
  (`TRINO_CACHE_DISK_PCT`), `fs.cache.page-size=1MB` (`TRINO_CACHE_PAGE_SIZE`)).
  A node without it fails to start the catalog.
- `/cache` is a generic ephemeral gp3-fast volume per pod: 167 Gi per worker
  (`TRINO_CACHE_VOLUME`), 20 Gi on the coordinator (`TRINO_COORD_CACHE_VOLUME`).
  3 × 167 Gi × 90 % ≈ 450 GiB. `fsGroup: 1000` makes it writable.
- No in-memory data cache (DuckDB has 22 GB); only the OS page cache.
- `strategy: Recreate` and ephemeral volumes: every restarted pod starts on a
  new empty volume with a cold JVM and cold Glue metadata.
- Finished queries stay readable for the harness for at least 60 min, up to
  1000 (`query.min-expire-age=60m`, `query.max-history=1000`).

### Checks and fixes

| Symptom | Fix |
|---|---|
| `serviceaccount starrocks-sa not found in emr-flink` | Create it (prerequisite 1), or `TRINO_SERVICE_ACCOUNT=<sa>` |
| `storageclass <x> not found` | Only `gp3-fast` is applied automatically; create the class or unset `CACHE_STORAGE_CLASS` |
| `deploy/trino-<d> not ready after 1200s` | `kubectl describe pod -n emr-flink -l app.kubernetes.io/instance=trino`: Pending = no m5.4xlarge / m5.2xlarge from `driver-nodepool`; CrashLoop = `kubectl logs deploy/trino-coordinator -n emr-flink` (config or catalog error) |
| `<n>/3 workers active after 1200s` | A worker cannot join or start the catalog: `kubectl logs deploy/trino-worker -n emr-flink` |
| `SHOW SCHEMAS FROM iceberg` empty / catalog error in `status` | IRSA or Glue access: check the ServiceAccount annotation and the role's Glue permissions |
| helm `no cached repo found ...` | Should not happen (private repo config); update helm if it does |
| Trino of another version up | `./engine-deploy/deploy-trino.sh up` upgrades it in place |

## DuckDB

### Steps

```bash
# 0. once: build and push the image (in-cluster Kaniko; 34-43 min measured at COMPILE_JOBS=8) -> duckdb-image/README.md
./engine-deploy/duckdb-image/build-duckdb-image.sh
./engine-deploy/deploy-duckdb-cdc.sh render        # 1. duckdb-cdc-deployed.yaml only; no cluster call
./engine-deploy/deploy-duckdb-cdc.sh up            # 2. start or restart the server
./engine-deploy/deploy-duckdb-cdc.sh status        # 3. /health?cache=1
./engine-deploy/deploy-duckdb-cdc.sh clear-cache   # empty cache_httpfs only (querybench restarts the pod instead)
./engine-deploy/deploy-duckdb-cdc.sh down          # delete the Deployment and its ConfigMap
```

1. **render** writes `duckdb-cdc-deployed.yaml` from `duckdb-cdc.yaml`.
2. **up** applies the gp3-fast StorageClass if missing (`CACHE_MODE=on_disk`),
   renders, recreates the ConfigMap `duckdb-warm-cdc` from
   `duckdb_cdc_server.py`, applies the Deployment, **always** rollout-restarts
   it (so a changed server reaches the pod; the caches are lost), and waits up
   to `READY_GRACE` (1200 s). Done: the `/health` JSON is logged.
3. **status** prints `/health?cache=1`. Check:
   - `"image"`: the `DUCKDB_IMAGE` URI (or `pip` for the fallback);
     `"duckdb": "2.0.0"`; `"engine"`: `v2.0.0`;
   - `"iceberg_format_version"` and `"skip_puffin_verification_arms"`
     (`["paimon"]` on v3) match the table set;
   - `"busy": false` (the harness needs it idle), `"started_at"` (a change =
     the server restarted, e.g. an OOM kill), the cache size.
- **down** deletes the Deployment (its ephemeral cache and spill volumes go
  with the pod) and the ConfigMap.

### Image

`DUCKDB_IMAGE` (`.bench-env`) = `<account>.dkr.ecr.<region>.amazonaws.com/duckdb-cdc-server:2.0.0`,
run as is (no pip, statically linked iceberg/httpfs/aws/avro, `cache_httpfs`
preinstalled). `DUCKDB_IMAGE= ./engine-deploy/deploy-duckdb-cdc.sh up` runs
`python:3.12-slim` + `pip install duckdb==2.0.0` (`DUCKDB_VERSION`) at every
start instead: a different build (clang + ThinLTO), so a separate timing
baseline. Don't `kubectl set image`: the next `up` re-renders from
`DUCKDB_IMAGE`. `querybench/run.sh start` redeploys a server whose `/health`
reports another image or format version.

### Sizing

The throughput benchmark's `duckdb-warm` shape: `DUCKDB_CPU` 27,
`DUCKDB_POD_MEMORY` 114Gi (requests = limits) on an m5.8xlarge
(`DUCKDB_INSTANCE_SIZE`, `DUCKDB_NODEPOOL` `driver-nodepool`),
`DUCKDB_THREADS` 27, `DUCKDB_MEMORY_LIMIT` 84049MiB, spill 200Gi gp3
(`DUCKDB_MAX_TEMP_SIZE` 180GB), cache `CACHE_MODE=on_disk`: 500Gi gp3-fast
volume, `CACHE_DISK_BYTES` 483183820800 (450 GiB), `CACHE_MEM_BYTES`
22033182228, `CACHE_BLOCK_BYTES` 262144 (256 KiB). `MAX_RESULT_ROWS` 10000.
A sizing or `SKIP_PUFFIN_VERIFICATION_ARMS` change needs an explicit `up`;
`run.sh start` does not compare them.

### Checks and fixes

| Symptom | Fix |
|---|---|
| `deploy/duckdb-warm-cdc not ready after 1200s` | `kubectl describe pod -n emr-flink -l app=duckdb-warm-cdc`: Pending = no m5.8xlarge; `ImagePullBackOff` = image not pushed (build it) or wrong `DUCKDB_IMAGE` |
| `no /health from duckdb-warm-cdc` | `kubectl logs deploy/duckdb-warm-cdc -n emr-flink -c duckdb`: extension load or credentials (`starrocks-sa`) |
| `"busy": true` when idle | A query is still running (a client gave up); `up` restarts it |
| Pod restarts during a run | OOM kill: the run records `CRASH`; check `memory_limit` vs the pod memory |
| `duckdb:paimon` `ERROR` on v3 (`missing required Avro key-value metadata field 'schema'`) | Paimon 2.1 export bug, not a server problem: see [../querybench/README.md](../querybench/README.md#troubleshooting) |
