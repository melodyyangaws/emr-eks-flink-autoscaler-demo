# Iceberg compaction for the CDC tables

Iceberg's Flink writer never merges files: every checkpoint adds a data file,
an equality-delete file and a deletion vector per writer subtask. Paimon
compacts inside its writer; Iceberg needs this external job.

| File | What |
|---|---|
| `maintain.py` | PyFlink script: `maintain` (long-running streaming job) and `plan` (read-only table stats) |
| `maintain.yaml` | FlinkDeployment template, `jarURI` = the script on S3, `entryClass` = `PythonDriver` |
| `run-maintain.sh` | `render \| up \| status \| logs \| suspend \| resume \| down \| plan [tables]` |

No jar to build: the classes it drives (`org.apache.iceberg.flink.maintenance.api.TableMaintenance`,
`RewriteDataFiles`, `ExpireSnapshots`) are stock Iceberg 1.12.0, already on the
writer image `emr-flink-cdc-paimon:7.14.0`, so its Iceberg version always
matches the writers'. Same pattern as `throughput-benchmark/compaction/flink-cid-compact.py`.

## How it works

One FlinkDeployment, `iceberg-maintain`, for all four tables of the current
set (`flink_iceberg_db${CDC_DB_SUFFIX}`). For each table it appends a
`TableMaintenance` graph that watches the table's own commits and, every
`SCHEDULE_COMMITS` (10) writer commits (~10 min at the writers' 60 s
checkpoints), runs:

- `RewriteDataFiles`: rewrite all files to the sinks' target size (64 MiB
  customers/products, 128 MiB orders/order_items), partial progress in up to 10 commits;
- `ExpireSnapshots`: older than 12 h, keep the last 5 (the writers' `history.expire` rule).

Why continuously and not hourly: a Flink rewrite reloads the table's whole
equality-delete set for every task, so its cost grows with the backlog. Short,
frequent passes each apply only the deletes added since the previous pass.

`TableMaintenance.forTable(env, loader)` (2-arg) passes a null lock factory.
That is safe because exactly one maintenance job runs per table.

## Commands

```bash
source .bench-env
./iceberg-compaction/run-maintain.sh plan          # read-only: data/delete file and delete counts per table
./iceberg-compaction/run-maintain.sh up            # upload maintain.py, create the FlinkDeployment
./iceberg-compaction/run-maintain.sh status
./iceberg-compaction/run-maintain.sh suspend       # or resume (patches spec.job.state)
./iceberg-compaction/run-maintain.sh down
```

`SUITE=hot ./querybench/run.sh start` resumes it (or creates it), and the
watcher's teardown suspends it. Re-deploying is `down` + `up`, never an apply
over the live CR (EMR stages `jarURI` on CREATE only).

Settings (environment or `.bench-env`): `MAINT_TABLES`, `SCHEDULE_COMMITS` (10),
`PARTIAL_PROGRESS_MAX_COMMITS` (10), `EXPIRE_HOURS` (12), `EXPIRE_RETAIN_LAST`
(5), `TM_CPU` / `TM_MEMORY` (8 / 32Gi), `TM_SLOTS` (4), `JM_CPU` / `JM_MEMORY` (2 / 4Gi).
State, HA and logs go to `s3://$BUCKET_NAME/flink/iceberg-maintain${CDC_DB_SUFFIX}/`.

## Known limits (2026-11-03, `_v3r3`)

- **Start it with the writers, not later.** The writers' initial MySQL snapshot plus
  ~30 min of upserts left ~35M equality deletes on `customers` before the first pass;
  the first `Rewrite` task OOMed on an 8 Gi TaskManager, hence the 32 Gi default.
- **py4j gotchas** (fixed in the script): `TableIdentifier.of(String...)` is varargs,
  use `TableIdentifier.parse("db.table")`; `Duration.ofHours(long)` rejects a Python
  float, so the script goes through `ofMinutes(int)`.
- **The operator requires** `state.checkpoints.dir` and `jobManager.storageDir` for
  `upgradeMode: last-state`; both are in `maintain.yaml`.
- **No bounded one-off mode.** With the writers suspended there are no commits, so nothing
  triggers. The snapshot test's pre-test compaction used the removed Java app.

## History

The Java maintenance app (`flinkApp/`: an hourly binpack CronJob and a MySQL-gated
`MergeRewrite` for `customers`) and `run-flink-maintenance.sh` were removed on
2026-11-03. Archive: `s3://$BUCKET_NAME/archive/iceberg-compaction-archive-20261103.tgz`.
The Spark compaction before it was removed on 2026-11-02.
