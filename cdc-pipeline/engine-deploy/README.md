# engine-deploy: the CDC query benchmark's engines

| Engine | Script | Manifest / values |
|---|---|---|
| DuckDB (warm server) | `deploy-duckdb-cdc.sh` | `duckdb-cdc.yaml` |
| Trino | `deploy-trino.sh` | `trino-values.yaml`, chart `trinodb/trino` |

All of them read `cdc-pipeline/.bench-env` (through `lib/common.sh`) and
deploy into `NAMESPACE` (`emr-flink`). `*-deployed.yaml` files are rendered
output and git-ignored.

## Trino

```bash
./engine-deploy/deploy-trino.sh render        # fill trino-values.yaml -> trino-values-deployed.yaml (helm's -f input); no cluster change
./engine-deploy/deploy-trino.sh up            # helm upgrade --install, wait for all workers active, print SELECT version()
./engine-deploy/deploy-trino.sh status        # pods, version, nodes, SHOW SCHEMAS FROM iceberg, /cache use per pod
./engine-deploy/deploy-trino.sh clear-cache   # empty /cache everywhere, restart workers + coordinator, wait (cold start)
./engine-deploy/deploy-trino.sh down          # helm uninstall + delete leftover cache PVCs
./engine-deploy/deploy-trino.sh sql "SELECT count(*) FROM iceberg.flink_iceberg_db_v3.orders"
./engine-deploy/deploy-trino.sh qstats <query_id>
```

### Versions

- **Trino 484** (released 2026-10-15), image `trinodb/trino:484` (tag pinned
  in `trino-values.yaml`, `TRINO_VERSION`).
- **Chart `trinodb/trino` 1.45.0** (`TRINO_CHART_VERSION`): the chart index
  (https://trinodb.github.io/charts) maps 1.45.0 to appVersion 484 (1.44.0 and
  1.43.0 are 483). The script pulls the archive by URL
  (`github.com/trinodb/charts/releases/download/trino-1.45.0/trino-1.45.0.tgz`,
  index digest `44d918d3...f830`), with a private, empty helm repo config, so
  it needs no `helm repo add` and does not trip over stale entries in the
  local repo cache.

### Iceberg v3

There is nothing extra to set. The 484 Iceberg connector docs list
`format_version` values `1`, `2` and `3`, say "Version `3` support is
experimental", and say that row-level deletes on v3 tables use deletion
vectors. The 484 release notes fix memory accounting "when reading tables with
deletion vectors" and "for queries reading tables with equality delete files",
plus the reading of equality deletes on nested `row` keys. The Flink Iceberg
sink writes equality deletes and Puffin deletion vectors, so these are the
read paths the benchmark uses. Unsupported on v3 (writes only, not used here):
`add_files` / `add_files_from_table`, and writing encrypted tables.

### Shape and memory

This matches the DuckDB warm server's resources (27 cpu / 114 Gi on one
m5.8xlarge): 3 workers × 9 cpu / 38 Gi = 27 cpu / 114 Gi. The coordinator runs
no splits (`node-scheduler.include-coordinator=false`). Every pod uses
requests = limits (Guaranteed QoS) and the same Karpenter placement as
`duckdb-cdc.yaml`: `driver-nodepool`, m5, amd64, on-demand,
`karpenter.sh/do-not-disrupt`. The workers have pod anti-affinity, so there is
one worker per node.

| | coordinator | worker (each) |
|---|---|---|
| node | m5.2xlarge (an m5.xlarge cannot fit 4 cpu) | m5.4xlarge |
| cpu (req = limit) | 4 | 9 |
| memory (req = limit) | 16 Gi | 38 Gi |
| `-Xmx` (80 % of memory) | 13107M (12.8 GiB) | 31129M (30.4 GiB) |
| `query.max-memory-per-node` (65 % of heap) | 8519MB | 20233MB (19.8 GiB) |
| `memory.heap-headroom-per-node` (30 % of heap) | 3932MB | 9338MB |
| non-heap left in the container | 3.2 GiB | 7.6 GiB |

`query.max-memory` = 3 × 20233MB = 60699MB (59.3 GiB) of user memory for one
query across the cluster. `query.max-total-memory` keeps its default.
Max-memory-per-node plus headroom (65 % + 30 %) stays under the heap, as Trino
requires at startup. Spill is not enabled.

The script works these numbers out from its variables, so you can change any
of them: `TRINO_WORKERS`, `TRINO_WORKER_CPU`, `TRINO_WORKER_MEMORY_GI`,
`TRINO_WORKER_INSTANCE_SIZE`, `TRINO_COORD_CPU`, `TRINO_COORD_MEMORY_GI`,
`TRINO_COORD_INSTANCE_SIZE`, `TRINO_HEAP_PCT` (80), `TRINO_QMEM_PCT` (65),
`TRINO_HEADROOM_PCT` (30), `TRINO_NODEPOOL`. For example,
`TRINO_WORKERS=4 TRINO_WORKER_CPU=6 TRINO_WORKER_MEMORY_GI=28 ./engine-deploy/deploy-trino.sh render`.

### Catalog and credentials

There is one catalog, `iceberg` (the chart's default `tpch`/`tpcds` are
removed):

```properties
connector.name=iceberg
iceberg.catalog.type=glue
hive.metastore.glue.region=${AWS_REGION}
hive.metastore.glue.use-web-identity-token-credentials-provider=true
fs.native-s3.enabled=true
s3.region=${AWS_REGION}
fs.cache.enabled=true
```

The coordinator and the workers run as ServiceAccount `starrocks-sa`
(`TRINO_SERVICE_ACCOUNT`), the same one `duckdb-cdc.yaml` uses. Its IRSA role,
`emr-on-eks-test-execution-role`, has these permissions (checked 2026-11-02):

- Glue `GetDatabase(s)`, `GetTable(s)` and `GetPartition(s)` on `*`
- S3 Get/List on the benchmark bucket

The Iceberg tables are `iceberg.flink_iceberg_db<CDC_DB_SUFFIX>.<table>`, for
example `iceberg.flink_iceberg_db_v3.orders`. `SHOW SCHEMAS FROM iceberg` lists
every Glue database in the account.

### File system cache (parity with DuckDB's on-disk cache)

- Since 484, the cache is configured per node, not per catalog. Each node's
  `config.properties` sets
  `cache-manager.config-files=/etc/trino/cache-manager-alluxio.properties`.
  That file holds `cache-manager.name=alluxio`, `fs.cache.directories=/cache`,
  `fs.cache.max-disk-usage-percentages=90` and `fs.cache.page-size=1MB` (the
  Trino default). The catalog only sets `fs.cache.enabled=true`.
- The 484 docs say every node, coordinator included, must load the same
  manager, or the catalog fails to start. The coordinator's cache holds only
  the metadata reads made during planning.
- Each pod's `/cache` is a generic ephemeral volume on StorageClass `gp3-fast`
  (exists on the cluster: gp3, 16000 IOPS, 850 MB/s, ext4, encrypted). Workers
  get 167 Gi each (`TRINO_CACHE_VOLUME`) and the coordinator 20 Gi. With the
  90 % cap that is about 150 GiB per worker, about 450 GiB in total, the same
  as DuckDB's `CACHE_DISK_BYTES` (450 GiB).
- A percentage, not `fs.cache.max-sizes`, so the identical config fits both
  volume sizes. `fsGroup: 1000` makes the fresh volume writable by the trino
  user.
- There is no separate in-memory data cache like DuckDB's 22 GB one. The OS
  page cache inside each worker's 7.6 GiB of non-heap memory is the nearest
  equivalent.
- `node-scheduler.cache-preferred-hosts-count` keeps its default (2), so a
  file can be cached on two workers.
- `clear-cache` runs `rm -rf /cache/*` in every pod, then a rollout restart of
  `trino-worker` and `trino-coordinator` (`CLEAR_COORDINATOR=0` keeps the
  coordinator), then waits until all `TRINO_WORKERS` are active.
- Both deployments use `strategy: Recreate`, and an ephemeral volume is
  deleted with its pod. Every restarted pod therefore starts on a new, empty
  EBS volume with a cold JVM, cold Glue metadata and cold file caches. Expect
  about 1 to 3 minutes per clear (EBS create and attach, then JVM start).

### Running a query and its server-side timing (for the harness)

The harness reaches the coordinator through Service `trino.emr-flink:8080`
(ClusterIP), or through the CLI in the coordinator pod (the 484 image has
`trino`, `bash` and `curl`, but no `grep`).

1. Run the query, tagged with a unique `--source`:

```bash
kubectl exec -n emr-flink deploy/trino-coordinator -c trino-coordinator -- \
  trino --server http://localhost:8080 --user cdcbench \
        --source "cdcqb-<stamp>-<query>" --output-format TSV \
        --execute "<SQL>"
# or: TRINO_SOURCE="cdcqb-<stamp>-<query>" ./engine-deploy/deploy-trino.sh sql "<SQL>"
```

2. Read the wall-clock times from `system.runtime.queries`. In 484 its
   columns are query_id, state, user, source, query, resource_group_id,
   queued_time_ms, analysis_time_ms, planning_time_ms, created, started,
   last_heartbeat, end, error_type and error_code. It has no CPU column.

```sql
SELECT query_id, state, queued_time_ms, analysis_time_ms, planning_time_ms,
       to_milliseconds("end" - created) AS elapsed_ms,
       to_milliseconds("end" - started) AS execution_ms,
       error_code
FROM system.runtime.queries
WHERE source = 'cdcqb-<stamp>-<query>'
```

3. Read CPU time and bytes from the coordinator's REST API. This uses the
   `queryStats` fields `elapsedTime`, `queuedTime`, `executionTime`,
   `totalCpuTime`, `physicalInputDataSize`, `processedInputDataSize`,
   `peakUserMemoryReservation` and `totalDrivers`:

```bash
kubectl exec -n emr-flink deploy/trino-coordinator -c trino-coordinator -- \
  curl -sf -H 'X-Trino-User: cdcbench' http://localhost:8080/v1/query/<query_id>
```

`deploy-trino.sh qstats <query_id>` prints the results of steps 2 and 3 as
TSV. Finished queries stay readable for at least 60 minutes, up to 1000
queries (`query.min-expire-age=60m`, `query.max-history=1000`).

### Not verified yet

Nothing has been installed. `render` (helm template) and `bash -n` pass, and
the StorageClass, ServiceAccount, nodepool, IAM role and Glue databases were
checked read-only. Still to confirm on the cluster:

- Glue access with `use-web-identity-token-credentials-provider`
- the native S3 file system with IRSA
- the alluxio cache starting on the fresh volumes with the percentage cap
- v3 reads of the Flink-written deletion vectors
- `node.id=${ENV:HOSTNAME}`
- that the default `task.concurrency` suits 9 cpu
