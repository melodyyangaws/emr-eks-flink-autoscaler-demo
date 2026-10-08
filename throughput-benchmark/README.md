# Flink Throughput Benchmark — Paimon vs Iceberg

Measures the **Flink write-path performance of Apache Paimon vs Apache Iceberg**
at a target of **8 GiB/s with a 4 KiB mean row** (2,097,152 rows/s), on the same
image the CDC pipeline runs — `emr-flink-cdc-paimon` (EMR 7.13, Paimon 1.3.2,
Iceberg 1.10.0-amzn-1).

| Format | Table shape | Compaction |
|---|---|---|
| `paimon` | append-only, `PARTITIONED BY (dt)`, `JOB_PARALLELISM` buckets on `uuid` | **in-engine** — inline in the writer (`write-only=false`) |
| `iceberg` | format v2, append-only, `PARTITIONED BY (dt)`, `write.distribution-mode=none` | **none** — the sink never rewrites data files, and no compactor ships here |

**Both sides are append-only** — no primary key, no upsert, no merge-on-read
deletes, no changelog producer. Each pipeline is `INSERT INTO cid_events
SELECT …` over a datagen view with no stateful operator, so the planner cannot
produce a retraction: every record reaching either sink is a new row.

⚠ **Not comparable with pre-append-only (upsert-test-case) runs.** The update
machinery that used to dominate the difference between the two formats is gone.
What remains is each format's **encode → upload → commit → compact** path at the
same row rate.

There is **no docker image build here.** Build it once via
`cd ../cdc-pipeline && ./build-deploy-generic.sh build`; the deploy script checks
the tag exists in ECR before rendering.

---

## Quick start

```bash
cd throughput-benchmark
source .bench-env                      # compulsory — see below
./build-deploy-cid.sh upload           # executor + SQL to S3, verified
./build-deploy-cid.sh deploy both    # or paimon or iceberg
./collect-metrics.sh paimon 30 3600    # 30s samples for 1h -> output to bench-results/
./build-deploy-cid.sh cleanup paimon
```

`deploy` does everything: creates the Glue databases, uploads
`flink-cid-executor.py` and the per-format SQL to S3, renders the manifest and
applies it. `render` does the same without executing, if you want to inspect
`flink-cid-*-deployed.yaml` first. `deploy` and `cleanup` take the same three
targets — `paimon`, `iceberg`, `both`.

**`deploy both` is for exercising the pipeline, not for scoring.** It uploads
once, then renders and applies each format in turn, and prints the contention
warning from [step 1](#1-check-capacity-before-deploying) plus a distinct
`LOCAL_PORT` per collector. Use it to drive the side-by-side monitor; use one
format at a time for any number you intend to quote.

**`source .bench-env` is the most common way to lose ten minutes.** Without it
the script aborts on missing `AWS_REGION` / `AWS_ACCOUNT_ID` / `BUCKET_NAME` /
`EMR_EXECUTION_ROLE_ARN`. It also **overrides benchmark defaults**, so you are
not running the script's defaults when you source it:

| | script default | `.bench-env` |
|---|---|---|
| `JOB_PARALLELISM` | 128 | **160** |
| `PAIMON_BUCKETS` | 128 (derived: `= JOB_PARALLELISM`) | **160** (derived) |
| `SOURCE_ROWS_PER_SECOND` | 2,000,000 | **2,097,152** |
| `HOT_KEY_FRACTION` | 0 (uniform) | **80** |
| `ICEBERG_COMPAT` | false | **true** |
| `SOURCE_MODE` | datagen | **s3json** — see [Feeding the Flink jobs from these files](#feeding-the-flink-jobs-from-these-files) |

`PAIMON_BUCKETS` is listed because it is **derived**, not because `.bench-env`
sets it — it follows `JOB_PARALLELISM` unless you override it explicitly.


---

## Deployment steps

### 1. Check capacity before deploying

Per deployment: `1 JM × 4 cpu + (JOB_PARALLELISM / 4 slots) TMs × 4 cpu`. TMs
are 4 cpu / 16 GiB with `taskmanager.numberOfTaskSlots: "4"`, so
`JOB_PARALLELISM=160` (what `.bench-env` sets) is **40 TMs ≈ 164 cpu per
format**, and the script default of 128 is 32 TMs ≈ 132.

```bash
kubectl get nodepool executor-memorynodepool -o jsonpath='{.spec.limits.cpu}'      # ceiling
kubectl get nodepool executor-memorynodepool -o jsonpath='{.status.resources.cpu}' # in use
```
Keep total requests under ~80% of nodepool allocatable. Above that the pods
evict each other in a loop: a displaced writer TM's replacement hits
`FailedScheduling: Insufficient cpu` and the job restarts.

### 2. Upload all artifacts and scripts to S3

Every artifact the jobs run is read from S3 at pod boot, not from this
directory, so stage and verify it before triggering anything:

```bash
./build-deploy-cid.sh upload
```

This creates the Glue databases and uploads, then `head-object`-verifies, every
artifact:

| S3 key | from | read by |
|---|---|---|
| `flink/scripts/flink-cid-executor.py` | `flink-cid-executor.py` | `spec.job.jarURI`, which EMR copies into `/opt/flink/usrlib` |
| `flink/cid-sql/<fmt>/00-source.sql` | `sql-scripts/common/01-datagen-source.sql` or `01-s3json-source.sql`, picked by `SOURCE_MODE` | the executor's `--sql-dir` |
| `flink/cid-sql/<fmt>/02..04-*.sql` | `sql-scripts/<fmt>/` (synced with `--delete`) | the executor's `--sql-dir` |
| `flink/cid-sql/common/01-{datagen,s3json}-source.sql` | `sql-scripts/common/` (both, whatever `SOURCE_MODE` is; synced with `--delete`) | nothing at runtime — a full copy of the script set; no `--sql-dir` points here, since a second source file in `<fmt>/` would define `datagen_source` twice |

It runs no `kubectl`. A missing or zero-length object stops it at the shell
with `[ERROR] NOT IN S3` / `ZERO-LENGTH`, instead of 10 minutes later as a
`NoSuchFileException` inside a JobManager. Check the result:

```bash
aws s3 ls "s3://$BUCKET_NAME/flink/scripts/flink-cid-executor.py"
aws s3 ls --recursive "s3://$BUCKET_NAME/flink/cid-sql/"   # 4 objects per format
```

`deploy` runs the same upload again before it creates the FlinkDeployments, so
S3 always matches this directory; see
[The deploy is what puts the SQL in S3](#the-deploy-is-what-puts-the-sql-in-s3-and-it-is-authoritative).

### 3. Deploy, then confirm it is actually working

```bash
./build-deploy-cid.sh deploy paimon             # or: iceberg, or: both
kubectl get flinkdeployment -n emr-flink -w     # ctrl-C at RUNNING/STABLE
```
In case of stopping jobs:
```bash
./build-deploy-cid.sh cleanup paimon  # or: iceberg, or: both
```
Then start a collector:

```bash
# ./collect-metrics.sh paimon|iceberg [interval-s] [duration-s]
./collect-metrics.sh iceberg 30 3600 &                   # default LOCAL_PORT=8091
LOCAL_PORT=8092 ./collect-metrics.sh paimon 30 3600 &
```
**`LOCAL_PORT` is mandatory on a second concurrent collector.** Both default to
8091 for their `kubectl port-forward`, so the second one reads the *first* job's
samples. Output: `bench-results/cid-<format>-<UTC stamp>.csv`.

### 4. Monitoring

```bash
./build-deploy-cid.sh monitor         # FlinkDeployments + pods, one shot
kubectl port-forward svc/flink-cid-paimon-rest 8081:8081 -n emr-flink   # one job's REST/UI
```

Optional side-by-side monitor — a plain `Deployment`, with its Python coming
from `../cdc-pipeline/monitoring`, so that directory must be present:

```bash
./monitoring/deploy-cid-monitor.sh        # upload code, apply, restart, wait
kubectl logs -f deployment/flink-cid-monitor -n emr-flink
./monitoring/deploy-cid-monitor.sh stop   # scale to 0; re-run without args to start
```

The script uploads the three monitor files to
`s3://${BUCKET_NAME}/flink/monitoring/` and the pod fetches them at boot with
`boto3` under its IRSA service account. There is **no `flink-cid-monitor-code`
ConfigMap** — the deploy script deletes it if an older run left one behind.
Two reasons for S3: `flink_cdc_monitor.py` is ~36 KB against a ConfigMap's
1 MiB etcd ceiling, and every other piece of code this project runs
(`flink-cid-executor.py`, all the SQL) already ships to `s3://${BUCKET_NAME}/flink/`,
so `aws s3 ls s3://${BUCKET_NAME}/flink/` now lists everything the cluster
executes.

To ship a monitor fix you therefore re-run the script — or, if only the code
changed, `aws s3 cp` the file and `kubectl rollout restart
deployment/flink-cid-monitor`: the fetch is at boot, so a restart is the pull.
The fetch is also the failure boundary. `/app` is an `emptyDir`, so a restart
always re-downloads rather than reusing the last boot's copy, and a missing key
raises out of the fetch under `set -e`, which means the pod CrashLoops with
`NoSuchKey` in its logs instead of silently re-running yesterday's monitor.
`boto3` is pip-installed **before** the fetch, pinned to the same `1.40.0` that
`requirements.txt` pins, so the second `pip install` finds it satisfied — the
image is `python:3.11-slim`, which has no `aws` CLI, and installing `awscli`
for three GETs would pull a second botocore at every boot.

Checkpoint and per-subtask detail (including Paimon's barrier flush) is in
[Reading the results](#reading-the-results). **Those fields die with the
JobManager — capture them before teardown.**

### 5. Teardown between runs

**Order matters: nothing may be writing when you wipe.** A live JobManager
recommits into a half-deleted warehouse and produces a table that looks valid
and is not.

```bash
# 5a. Stop the writers FIRST — suspend, or delete with `cleanup` (prompts "yes").
kubectl patch flinkdeployment flink-cid-paimon -n emr-flink --type=merge \
  -p '{"spec":{"job":{"state":"suspended"}}}'
./build-deploy-cid.sh cleanup both

# 5b. VERIFY before going further.
kubectl get pods -n emr-flink | grep cid        # must print nothing

# 5c. Glue tables. Drop before any scoring run, and ALWAYS before changing
#     JOB_PARALLELISM or PAIMON_BUCKETS (the Paimon bucket count is baked at
#     CREATE TABLE, and PAIMON_BUCKETS is derived from JOB_PARALLELISM, so
#     changing either moves it) or flipping ICEBERG_COMPAT.
aws glue delete-table --database-name flink_cid_paimon_db  --name cid_events --region "$AWS_REGION"
aws glue delete-table --database-name flink_cid_iceberg_db --name cid_events --region "$AWS_REGION"
```

`cleanup` deletes FlinkDeployments only — warehouse data and Glue tables stay,
and it says so.

The 5a patch is the same one the query harnesses now issue for themselves when
the snapshot suite is in scope (see
[The snapshot suite runs a pre-flight gate first](#the-snapshot-suite-runs-a-pre-flight-gate-first)).
It is kept here because teardown is also reached from a cold start, with no
benchmark having run — and because teardown needs *both* writers down plus an
empty `kubectl get pods`, which `cleanup` enforces and a suspend alone does not.
Running it after a snapshot benchmark is harmless: suspending an
already-suspended deployment is a no-op.

**ConfigMap cleanup depends on *how* the job stopped.** The operator's own
per-job ConfigMaps (`flink-config-*`, `pod-template-*`,
`flink-fluentd-config-*`) carry an ownerReference to the FlinkDeployment and are
garbage-collected with it. **The HA ConfigMaps do not.**
`flink-cid-<format>-cluster-config-map` and
`flink-cid-<format>-<jobid>-config-map` have no ownerReference at all — they are
removed by Flink's own HA teardown on a *graceful* cancel, so they **leak** if
the deployment was force-deleted or the JM died. A leaked
`*-cluster-config-map` is how a "fresh" deploy resumes a run you thought you had
discarded. Check and sweep explicitly:

```bash
kubectl get cm -n emr-flink | grep -E 'flink-cid-.*config-map'   # expect nothing
```

**5d. DESTRUCTIVE AND IRREVERSIBLE — no undo, no versioning.** Read the prefixes
twice. A typo dropping a path segment targets the whole bucket, which is shared
with the CDC pipeline:

```bash
aws s3 rm --recursive "s3://$BUCKET_NAME/cid-warehouse-paimon/"
aws s3 rm --recursive "s3://$BUCKET_NAME/cid-warehouse-iceberg/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/cid-paimon/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/cid-iceberg/"
# Optional, and only if you want a genuinely cold next run:
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/logs/cid-paimon/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/logs/cid-iceberg/"
aws s3 rm --recursive "s3://$BUCKET_NAME/duckdb-cid-pinned/"
aws s3 rm --recursive "s3://$BUCKET_NAME/flink/cid-sql/"
```

The first two are the warehouses (data + metadata); the next two are Flink state
— HA metadata, checkpoints, savepoints. `upgradeMode: stateless` means nothing
of value is lost, but stale HA metadata is how a "restarted" job resumes a run
you thought you had discarded, and a `completedCheckpoint*` left pointing at a
warehouse you just emptied is worse than useless: resuming from it commits
against files that no longer exist.

The four optional prefixes are the rest of the CID footprint, and each loses
something different. `flink/logs/cid-*/` is the sidecar's JM/TM `stdout.gz` /
`stderr.gz` — **the only record of the run once the JobManager is gone**, so
pull anything you need from [Reading the results](#reading-the-results) first;
it is a couple of MiB, so there is no cost reason to drop it.
`duckdb-cid-pinned/` is pinned Iceberg metadata that 404s mid-scan once the
table it describes is deleted. `flink/cid-sql/` is job code, not data —
deleting it costs nothing because the next `deploy` re-uploads it, but a bare
`kubectl apply` in between will fail (see
[step 3](#the-deploy-is-what-puts-the-sql-in-s3-and-it-is-authoritative)).
Leave `flink/querybench/` alone: it is the live query suite, shared by all
three read harnesses, and nothing in the deploy path republishes it —
`querybench-script/bench-sql.sh upload` does.

Finally, if you started the monitor, scale it to zero (it is a Deployment, not a
FlinkDeployment):

```bash
kubectl scale deployment flink-cid-monitor -n emr-flink --replicas=0
```

---

## Query benchmarks — the read half

Three harnesses, same tables. Prerequisites for the StarRocks pair: both tables
exist and hold rows, and the StarRocks FE pod is up (`FE_POD` defaults to
`kube-starrocks-fe-0`). Both create their external catalogs idempotently —
`filesystem` for Paimon, `glue` for Iceberg, IRSA via
`use_aws_sdk_default_behavior`.

**Two constraints govern every query here.** Neither is precautionary, and both
fail *silently in the flattering direction*:

- **The FE session timezone is `Asia/Shanghai` and all data is UTC**, so
  `SELECT NOW(), UTC_TIMESTAMP()` disagree by the zone offset. `NOW()` against a
  UTC column matches **zero rows**, and an empty scan is the fastest possible
  result — so a timezone slip reports an empty scan as a win. **Use
  `UTC_TIMESTAMP()` only**; never `NOW()`, never `CURRENT_DATE`.
- **Unbounded aggregates do not return** at the table sizes this benchmark
  produces with the writers live. A bare `SELECT MAX(ingestionTime)` or a bare
  `COUNT(*)` has to be killed; the same query bounded to today's `dt` plus a
  short trailing `ingestionTime` window completes. Every query in the two newer
  harnesses is bounded by `dt`.

### The queries live in one place

`querybench-script/` is the suite. Both harnesses render **these queries**;
neither contains one.

`querybench-script/` is the **editing copy**, and
`s3://${BUCKET_NAME}/flink/querybench/` is what the harnesses actually read.
`./bench-sql.sh upload` publishes the tree, and both harnesses `aws s3 sync`
that same prefix at startup. So "both engines ran the same queries" is a
property of **one object set**, not of two checkouts that happen to agree — and
an edit you forgot to upload cannot reach one engine and miss the other.
Neither harness looks at a local directory at all.

That replaced a flattened ConfigMap. A ConfigMap key cannot contain a `/`, so
the tree had to ship as `hot__H1_freshness.sql` and be split back into
directories in-pod; S3 keys carry the `/`, so the tree arrives already shaped
and the unpack step is gone.

```
querybench-script/
├── bench-sql-lib.sh          the renderer + window resolvers, sourced by both harnesses
├── bench-sql.sh              plan / render / oneline / check — inspect without running
├── hot/
│   ├── H1_freshness.sql          newest committed row in the hot window
│   ├── H2_hot_count_1m.sql       COUNT(*) over 1m — the planning-cost floor
│   ├── H3_hot_needle_uuid.sql    point lookup on a uuid that cannot exist
│   ├── H4_hot_narrow_agg.sql     projection pushdown: 1 skinny column of 35
│   ├── H5_hot_wide_json.sql      fat-column decode: bytes of the ~3.3 KB JSON
│   └── H6_hot_customer_agg.sql   COUNT(DISTINCT) per customer over 10m
└── snapshot/
    ├── S1_partition_count.sql    honest full-partition COUNT(*)
    ├── S2_narrow_agg.sql         full partition, one narrow column
    ├── S3_risk_distinct.sql      GROUP BY filterRiskLevel over the partition
    └── S4_wide_json.sql          fat JSON over a 30-minute ingestion slice
```

**The filename is the name, and the `H<n>`/`S<n>` prefix is the order.** A
lexical sort over the directory *is* the execution sequence, and
`basename .sql` *is* the string in every CSV, JSON and log line. That replaced
two invariants that were load-bearing but unenforced: "one statement per line"
(the old runner paired output line N with query index N) and positionally
aligned `HOT_NAMES`/`SNAPSHOT_NAMES` strings. Add a file, it runs and reports
itself; there is no second list to update.

Read the suites with:

```bash
cd querybench-script
./bench-sql.sh plan              # the readable step list + the resolved bounds
./bench-sql.sh plan hot          # one suite
./bench-sql.sh render snapshot   # the exact SQL, as submitted
ENGINE=duckdb ./bench-sql.sh render hot      # the DuckDB rendering
./bench-sql.sh oneline hot       # as the StarRocks runner sends it
./bench-sql.sh check             # render all 10 under BOTH dialects — run after any edit
```

Publish with:

```bash
./bench-sql.sh upload            # check, then sync to s3://.../flink/querybench/
./bench-sql.sh ls                # what is published right now
```

`upload` runs `check` first and refuses to publish if it fails, so a file that
cannot render cannot become the thing a run measures. It syncs with `--delete`,
which is what makes a *deleted* query actually disappear: both harnesses sync
into a fresh directory per run, so a stale object — not a stale local file — is
the only way a retired query could come back.

**`aws s3 sync` of an empty or misspelled prefix succeeds with no output and
exit 0.** Both harnesses therefore assert that `bench-sql-lib.sh` arrived and
that the hot and snapshot counts are non-zero, and abort otherwise. Without
that, a typo'd prefix yields an empty suite, and an empty suite is
indistinguishable from a suite whose queries all returned instantly.

`check` is the guard worth knowing about. **An unsubstituted `${FOO}` is not a
syntax error on either engine** — it binds as an identifier or a string literal
and the query returns a plausible wrong answer — so `bench_render` fails on any
surviving `${...}` rather than passing it through, and `check` exercises every
file under both dialects (`6 / 4 / 20 renders / OK`). It does not connect to an
engine, so it cannot catch a column typo; that is what the first real run is
for.

Each file opens with a header saying what the step measures and **which other
step to read it against** — H5 against H2 isolates JSON decode from the planning
cost both pay; S4 against S1 is why raw elapsed is not comparable across arms.
That is the "readable steps" part: the explanation lives next to the SQL, not in
a harness comment that describes a query several hundred lines away.

Three conventions make one file render on two dialects:

| in the `.sql` | resolves to | why |
|---|---|---|
| `${TABLE}` | StarRocks: `cid_events`. DuckDB: deferred — `iceberg_scan('…')` or `read_parquet([…])`, resolved per query at run time | DuckDB's scan expression depends on a Glue lookup and a pinned snapshot, so it cannot be fixed up front |
| `${BYTES_FN}` | StarRocks `LENGTH`, DuckDB `STRLEN` | **the only genuine dialect difference.** DuckDB's `LENGTH` counts *characters*; both of these count bytes, so `payload_bytes` is comparable and serves as the cross-engine equality check. **Not `octet_length()`** — that is BLOB/BIT-only in DuckDB and a `BinderException` on VARCHAR |
| `${HOT_DT}`, `${HOT_FROM_1M\|5M\|10M}`, `${HOT_TO}`, `${SNAPSHOT_DT}`, `${S4_FROM}`, `${S4_TO}` | pinned literals from `bench_resolve_hot_windows` / `bench_resolve_snapshot_window` | see below |

**No query contains a clock function.** Not `NOW()`, not `UTC_TIMESTAMP()`, not
`current_timestamp` — the windows are resolved once per run and rendered in as
bare string literals. That closes three failure modes at once: serial StarRocks
arms each reading a *different* window (the arms must run serially, so a clock
call drifts between them); DuckDB's window silently growing over a long suite;
and the FE-timezone hazard above, where `NOW()` against UTC data matched zero
rows and reported the empty scan as a win.

The literals are written as bare strings — `ingestionTime >= '2026-10-06
07:59:48'`, no `CAST`, no `TIMESTAMP` keyword — because that is the one spelling
that is valid and identical in both dialects.

A query can declare its own guard semantics with a `-- bench:` directive, read by
`bench_query_flag`:

| directive | effect | used by |
|---|---|---|
| `-- bench: allow-zero-rows` | exempt from the zero-scan guard; no rows is the designed outcome | `H3_hot_needle_uuid` (the all-zero uuid cannot be generated) |
| `-- bench: leading-col-is-label` | don't test the first column for `0`; it's a label, not a count | `S3_risk_distinct` (first column is `filterRiskLevel`, where `"0"` is a legitimate value) |

Both exemptions used to be hardcoded name lists inside *each* harness — so a
query added later was silently ungoverned, and the `leading-col-is-label` case
scored a perfectly good 5-group result as `ZERO_SCAN`. Declaring them in the
`.sql` puts the exemption next to the query it describes.

### `starrocks-cid-audit.sh` — server-side timing (prefer this)

**Client-side timing on this cluster is mostly harness noise.** The
`kubectl exec … mysql` wrapper carries seconds of fixed per-invocation cost —
pod exec setup, client handshake, result formatting — which a `SELECT 1` pays in
full and which dwarfs the FE's own `Time=` for anything short. For sub-second
queries the harness *is* the measurement. So this script times nothing from the
shell — it runs the whole suite down one `mysql` connection in one
`kubectl exec`, then reads back the `fe.audit.log` lines appended since it
started.

```bash
./starrocks/starrocks-cid-audit.sh                # both suites, WARMUP=1 RUNS=3
SUITE=hot ./starrocks/starrocks-cid-audit.sh      # SUITE=all|hot|snapshot
RUNS=5 ./starrocks/starrocks-cid-audit.sh         # tighter medians
SUSPEND_WRITERS=0 ./starrocks/starrocks-cid-audit.sh   # leave the writers alone
```

Writes `bench-results/starrocks-cid-audit-<stamp>.csv` —
`stamp,suite,query,metric,paimon,pmn_as_iceberg,iceberg,ratio_pmnice_over_paimon,ratio_iceberg_over_paimon`
(nine columns — **three** arms, the third being the Paimon table read through its
Iceberg metadata export), three rows per query (`time_ms`, `scan_rows`,
`scan_bytes`). Suites:

- **hot** (today's partition, trailing `ingestionTime` window): `H1_freshness`,
  `H2_hot_count_1m`, `H3_hot_needle_uuid`, `H4_hot_narrow_agg`,
  `H5_hot_wide_json`, `H6_hot_customer_agg`. Windowed, not cumulative, so the
  second format's pass does not inherit the first's elapsed rows. **Requires the
  writers live** — never suspended automatically.
- **snapshot** (the whole *oldest* `dt` partition, read as cold):
  `S1_partition_count`, `S2_narrow_agg`, `S3_risk_distinct`, `S4_wide_json`.
  S1-S3 are bounded by `dt` **alone**, so they are honest full-partition
  aggregates. S4 adds a 30-minute `ingestionTime` window, which — unlike a
  `uuid` predicate — **does** prune whole files; it is windowed because the
  fat-JSON decode is what made this suite a multi-hour run. This is where
  compaction state shows up. **Runs the pre-flight gate first** — asserts no
  query is already on the FE, then stops both writers and leaves them stopped.
  See [the snapshot bound](#the-snapshot-bound-dt-for-s1-s3-an-ingestiontime-window-for-s4).

`SNAPSHOT_DT` names the partition the snapshot suite reads, and **defaults to the
oldest `dt` in the table**, resolved from Iceberg's `$partitions` metadata
(`MIN(partition_value.dt)`, ~1 s, never a scan). The oldest partition is the one
guaranteed *closed* — the generator's `r_day` spread keeps the **newest** `dt`
live — so "oldest" and "safe to read cold" are the same choice.

**This replaced a yesterday-in-UTC default, which was silently wrong in two
directions.** The writers ran on 2026-10-05 and produced `dt={2026-10-04,
2026-10-05}`. Run the suite on 2026-10-06 and "yesterday" resolves to
`2026-10-05` — the *newest*, i.e. wrong — and on 2026-10-07 it resolves to a
`dt` that does not exist at all, which prunes to zero files and returns
instantly. That reads as a spectacular win and measures nothing. (An earlier
pinned literal `2026-10-01` failed exactly this way.) The default must come from
the **data**, not the clock. The harness prints which source it used as
`dt source :` in the preamble; if the metadata lookup fails it falls back to
yesterday-in-UTC and says so.

`CID_DAY_SPREAD=1`
makes the generator write **two live `dt` partitions by design**, today *and*
yesterday: `datagen`'s `fields.r_day.min`/`max` are **inclusive**, so
`r_day` draws from `{0, 1}`. Yesterday is therefore never a closed partition
while the writers run — measured 2026-10-03T10:49Z, `dt=2026-10-02` took
370,733,820 rows in the trailing 30 minutes and its `MAX(ingestionTime)` equalled
today's. That is the intended generator behaviour, not a defect: two partitions
is what gives the snapshot suite a *previous* day's worth of accumulated data to
read at all. It does mean the partition only closes once nothing is writing.
Export `SNAPSHOT_DT=<date>` to name a different day.

### The snapshot bound: `dt` for S1-S3, an `ingestionTime` window for S4

The bound is **not uniform across the four queries**, because they do not have
the same problem:

```sql
-- S1-S3
WHERE dt = '${SNAPSHOT_DT}'
-- S4
WHERE dt = '${SNAPSHOT_DT}'
  AND ingestionTime >= '${SNAPSHOT_INGEST_DATE} ${SNAPSHOT_S4_FROM}'
  AND ingestionTime <  '${SNAPSHOT_INGEST_DATE} ${SNAPSHOT_S4_TO}'
```

`dt` is the **oldest** partition (see `SNAPSHOT_DT` above — the closed one). The
window is half-open, so adjacent windows neither overlap nor gap. The window
fragment is built by its own helper (`snapshot_s4_window()`) kept separate from
the `dt` bound, so S1-S3 provably carry no window.

**The window is anchored to the INGESTION date, not the partition date, and
those differ.** `dt` derives from `eventTimeDT = event_ts - r_day days` while
`ingestionTime` is wall-clock at write, so `dt=2026-10-04` rows were *written*
on 2026-10-05. `SNAPSHOT_INGEST_DATE` therefore defaults to `SNAPSHOT_DT + 1
day`. Anchoring to `SNAPSHOT_DT` itself matches **zero rows** and returns
instantly, which reads as a spectacular win and measures nothing.

#### Why S1-S3 dropped the sample

The predecessor bound was a uniform 1/N `uuid` modulo sample on every snapshot
query. It **could not prune** — `uuid` is `MD5(random BIGINT)`
(`sql-scripts/common/01-datagen-source.sql`), so Parquet row-group min/max spans
the whole keyspace in every file — so it never reduced the scan; it only shrank
the aggregate, discarding 499/500 of what it had already read. Measured at
N=1000, `ScanRows` equalled each arm's exact full partition count
(3,217,389,927 / 3,217,389,927 / 4,180,412,213).

Dropping it from S1-S3 therefore costs **nothing in scan volume** and makes them
honest full-partition aggregates instead of full-partition scans that throw most
of the result away. `ScanRows` is unchanged; what changes is that the returned
counts are now the **real** partition figures rather than a sample to be scaled
up.

#### Why S4 got a time window instead

S4 is the fat-JSON decode, and it is what made this suite a multi-hour run:
18-41 min per pass per arm. The sample did not help it, because the decode
happens **during** the scan, before the filter. An `ingestionTime` window does
help, because it prunes whole files — `ingestionTime` is monotonic with write
order, so its row-group min/max are tight.

The consequence: **S4's number is not comparable with S1-S3's.** Its `ScanBytes`
is a fraction of the partition. Read S4 as a fat-column decode rate over a fixed
30-minute slice, not as a full-partition figure. S1-S3 remain full-partition
scans — with only `dt` in the predicate, `ScanRows` should equal the partition
row counts printed in the preamble, which is what makes them scan-cost
measurements.

Two operational consequences for S1-S3:

- **The suite is expensive and cannot be made cheap.** Each pass is a
  full-partition scan per arm. Measured serial wall-clock at N=1000: paimon
  267 s, pmn-as-ice 245 s, iceberg 365 s. Budget **~15 min per pass** across
  three arms and **keep `QUERY_TIMEOUT` at 3600 s**.
- **Run the arms serially.** Three concurrent full-partition scans exhaust the BE
  memory pool — measured 2026-10-06, all three aborted with
  `Memory of default_wg exceed limit ... Used: 18786279880, Limit: 18786186952`
  on `kube-starrocks-be-0`. The harness already serialises arms; do not
  parallelise them.

#### Rows are not equal across arms, by construction

The arms do not hold the same number of rows in this partition. Iceberg holds
**1.2993×** Paimon's rows in this partition because Paimon's write path is ~23%
slower at equal uptime (the bucketed hash shuffle — see
`bench-results/FLINK-THROUGHPUT-ANALYSIS-20261005.md`). So Iceberg reads ~1.30×
more and **raw elapsed time is not directly comparable**. Divide by each arm's S1
`COUNT(*)` for cost per row, which is.

Per-row scan cost, serial and uncontended — the cleanest read-path numbers
measured so far. These were taken under the retired `uuid` sample at N=1000,
where `ScanRows` equalled the full partition, i.e. **the same scan volume S1-S3
now read**, so the per-row figures carry over unchanged:

| arm | ScanBytes | B/row | CpuCostNs | ns/row |
|---|---|---|---|---|
| paimon | 122.3 GB | 38.02 | 1,426.7 B | 443.4 |
| pmn-as-ice | 120.2 GB | 37.37 | 1,398.2 B | 434.6 |
| iceberg | 176.4 GB | 42.19 | 1,966.3 B | 470.4 |

Iceberg costs **1.11× Paimon's bytes/row** and **1.06× its CPU/row**. Earlier
*concurrent* measurements suggested a 6.5× bytes gap; **those are not reliable**
— contention and differing pruning made the arms non-comparable. Trust serial
full-scan numbers only. The headline format gap in this benchmark is
overwhelmingly a **write-path** story, not a read-path one.

#### Rejected alternatives

| candidate | why it fails |
|---|---|
| `LIMIT n` | limits an aggregate's *output* (1 row), not the scan. No effect on bytes read. |
| subquery `LIMIT` | StarRocks pushes a per-tablet limit down, so *which* rows land differs per arm and the scan can short-circuit. Measures limit pushdown. |
| `uuid IN (<explicit list>)` | `r_uuid` is drawn from a 2⁶³ keyspace, so a 10,000-value list matches ~4.5×10⁻⁶ rows — always 0. The count is a property of the keyspace, not the data. |
| `uuid` modulo | **retired 2026-10-06.** Uniform and unbiased, but it cannot prune, so it never reduced the scan — it only discarded 499/500 of the rows *after* reading them. For S1-S3 that bought nothing over plain `dt`; for S4 it bought nothing at all, because the JSON decode happens during the scan, before the filter. |
| `ingestionTime` window | **adopted for S4 only.** It prunes whole files, which is exactly the point there: S4's decode cost is what made the suite multi-hour, and pruning is the only bound that reduces it. Deliberately *not* used on S1-S3, which must stay full-partition scans. Requires the ingestion-date fixup above — `dt` is a *generated* day field, so `dt=2026-10-04` rows carry `ingestionTime` on **2026-10-05**, and anchoring the window to `dt` selects zero rows. |

| knob | default | effect |
|---|---|---|
| `SNAPSHOT_DT` | oldest `dt` in table | partition under test; override to name a different day |
| `SNAPSHOT_INGEST_DATE` | `SNAPSHOT_DT + 1 day` | the date S4's window is anchored to — the **write** date, not the partition date. Override if `CID_DAY_SPREAD` changes or the data is reloaded on a different day |
| `SNAPSHOT_S4_FROM` / `SNAPSHOT_S4_TO` | `04:30:00` / `05:00:00` | S4's half-open `ingestionTime` window |

All three are **shape-validated, not merely checked for non-emptiness**, and
`FROM < TO` is asserted. The failure mode they guard is a BSD-`date` flag-order
slip that prints a ctime string with exit status 0 — no error to catch — which
then lands inside the SQL literal and matches zero rows, returning instantly and
reading as a win.

`SNAPSHOT_SAMPLE_N` **is gone from both harnesses** (2026-10-06). It was the
`uuid` modulo sample; see the rejected-alternatives table above for why it never
reduced a scan. The three knobs above are read from the same
`bench_resolve_snapshot_window` on both engines, so they cannot disagree.

The predicate is identical on all three arms, so unlike the pre-2026-10-06
scheme there are no per-arm cutoffs to recompute after a new write run — **only
the reference row counts in the tables above are pinned** to the 2026-10-05
02:56→06:11Z run.

### The snapshot suite runs a pre-flight gate first

Every snapshot-scoped run passes through `snapshot-preflight.sh`, shared by both
harnesses, before a single query is issued. Two steps, in this order:

1. **Assert no active `cid_events` query on the FE.** `SHOW PROCESSLIST` is
   filtered on `Info` **or** `Db` — not on catalog, because a fully-qualified
   statement logs against `default_catalog`, and not on `Info` alone, because
   the FE truncates it to ~100 chars so a long SELECT list can push the table
   name out of view. A hit **aborts the run** and prints the query id, its
   elapsed seconds, and the `KILL QUERY` command.
2. **Stop both Flink streaming jobs** and wait until their TaskManagers are
   gone.

Then control returns to the harness, which runs the tests.

**Step 1 exists because concurrency here is not merely noise.** Three concurrent
full-partition scans exhausted the BE memory pool outright on 2026-10-06
(`Memory of default_wg exceed limit … Used: 18786279880, Limit: 18786186952`),
killing two arms. And a leftover query is the *common* case, not a rare one:
killing a harness does **not** kill the FE query it submitted, so an abandoned
run keeps scanning for as long as it takes. Checking is a second; discovering it
afterwards costs the whole run.

**Step 1 runs before step 2 deliberately.** If the FE is busy the run cannot
proceed anyway, and stopping the writers first would have taken the cluster down
for a run that then aborts.

**Both steps fail CLOSED, which took two fixes to get right.** A gate that
cannot tell "nothing is wrong" from "I could not look" is not a gate:

- The FE probe captures `mysql`'s exit status **separately** from the row
  filtering — through a temp file, not a pipeline, because a pipeline hands back
  `awk`'s status, which is `0` even when `kubectl` died. A non-zero status, or
  an empty result (the probe's own connection should always appear), is a hard
  failure. Otherwise a typo in `FE_POD` disables step 1 entirely while printing
  a reassuring `FE idle` line.
- The TaskManager wait counts pods by **name prefix**
  (`<deployment>-taskmanager-`), not by label selector. A selector that matches
  nothing is indistinguishable from "no pods left" — it succeeds on the first
  poll and proves nothing — and the operator's label scheme has moved between
  versions (`component=taskmanager` vs `app.kubernetes.io/*`). Pod naming is
  stable API, and a name prefix cannot silently match the empty set.

**Step 2 exists because the two suites need opposite writer states, and both
failure modes are silent and flattering** — nothing errors, every arm completes,
and the numbers look plausible:

- **hot with the writers stopped** matches **zero rows** in its trailing
  `ingestionTime` windows, and an empty scan is the fastest possible result. It
  reports a false win rather than failing.
- **snapshot with the writers running** reads a *live* partition as "cold", so
  `ScanBytes` drifts between arms as files are committed mid-suite and **the arms
  stop being comparable** — which is the entire point of the suite. The cost is
  measured, not assumed: the same closed partition, the same 944,023,427 rows and
  the same DuckDB query took **332,520 ms live vs 3,742 ms stopped — 89x**, two
  orders of magnitude more than the 1.1-1.5x format effect being resolved.

So the gate fires for `snapshot` and `all`, and **never for `SUITE=hot`**.
Because the suites are opposite, `SUITE=all` is always wrong for one of them; the
harnesses warn and carry on (they do not abort), and the writer state recorded in
the output says which regime each run actually got.

**Step 2 does not trust the patch returning.** The patch returns immediately; the
operator then has to cancel the job, the JM to finish its in-flight checkpoint,
and the TMs to drain their write buffers. A half-suspended job still commits, and
a commit landing between arm1 and arm3 is the exact corruption this prevents. So
it polls (every `CID_STOP_INTERVAL`, default 15 s) for both of: the
FlinkDeployment's `status.jobStatus.state` to leave `RUNNING`, and **every
TaskManager pod for the job to be gone** (counted by name prefix — see the
fail-closed note above). Once no TaskManager exists, no process remains that
*could* write to the warehouse — a sufficient proof, and one that needs no S3
polling to establish. On timeout (`CID_STOP_TIMEOUT`, default 600 s)
it **fails loudly and does not query** — a benchmark against a still-committing
table is worse than no benchmark, because it produces a complete, plausible CSV
whose dominant variable moved mid-run.

`SUSPEND_WRITERS` has three states:

| value | behaviour |
|---|---|
| unset | **auto** — run the gate iff the selected suite includes snapshot (`snapshot` or `all`) |
| `0` | **skip** — touch nothing, check nothing. For an already-manually-stopped cluster, or a deliberately live table |
| `1` | **force** — run the gate whatever the suite. Note this makes a hot suite meaningless |

Stopping is idempotent and safe against an already-suspended deployment or one
that does not exist at all (absent is logged, not an error).

**There is no resume, by design.** The writers are left stopped and restarting
them is your call, because `upgradeMode: stateless` means a resume is a *fresh
restart* that reads whatever SQL is currently in S3 — not the SQL the run just
measured. That makes it a decision about the **next** experiment, not cleanup
after the last one, so no script does it and no harness prints a one-liner for
it. Restart deliberately, per deployment:

```bash
kubectl patch flinkdeployment flink-cid-paimon  -n emr-flink --type=merge \
  -p '{"spec":{"job":{"state":"running"}}}'
kubectl patch flinkdeployment flink-cid-iceberg -n emr-flink --type=merge \
  -p '{"spec":{"job":{"state":"running"}}}'
```

`snapshot_writer_state` prints the current state of both. `NAMESPACE`, `FE_POD`
and the deployment list (`FLINK_JOBS`, or `CID_WRITER_DEPLOYMENTS`) are
env-overridable and shared by both harnesses, so one override moves both.

Writer state is recorded in the artifacts, **sampled after the gate** so a
snapshot run's output proves the regime it was measured under: a
`# writers: …  (suspend_writers=… stopped_by_run=…)` header in the StarRocks CSV
and at the top of the DuckDB runner's `.log`. A run that changes writer state
partway through compares cluster states, not formats — the StarRocks harness
re-checks after every suite and aborts on a change.

**`S4_wide_json` dominates the suite's wall clock — budget for it.** Its
`SUM(LENGTH(event_json))` on StarRocks / `SUM(STRLEN(event_json))` on DuckDB
(DuckDB's `octet_length()` binds on `BLOB`/`BIT` only and will not accept a
`VARCHAR`, so `STRLEN` is the portable spelling there) is the fat-JSON decode,
and it is by far the most expensive query in either suite. It is kept because it
is the only query that touches the fat column at full-partition **scan** scale,
which is where compaction state and decode cost compound; `H5_hot_wide_json`
runs the same expression over a short trailing window and does not reach that
scale. Use `SUITE=hot` when you only need a quick signal.

The `ingestionTime` window changed what S4 costs by changing what it **reads**:
the window prunes, so S4 no longer scans the partition at all. The timings
quoted below (1,541 s DuckDB, 2,460 s StarRocks) are from the earlier
full-partition form and are therefore **upper bounds**; they remain the right
basis for sizing the timeout, and widening `SNAPSHOT_S4_FROM`/`TO` to the whole
day reproduces them.

`enable_spill = true` is pinned unconditionally in the StarRocks harness, but
**it is not what makes the full-partition form of S4 survivable, and it was
mis-credited here.** That failure is a **scan-side** OOM — the error names
`read and decompress page` / `get chunk from scanner`, i.e. the scanner's own
page buffers, not an operator's working set. Spill covers aggregate, sort and
join operators only, so it cannot reach that allocation; a run with spill on and
nothing else changed still dies. The setting is kept because
`S3_risk_distinct`'s `COUNT(DISTINCT customerId)` over a whole partition is a
genuine aggregate-memory risk, and because a cost paid equally by all three arms
cannot bias a format — which is what makes it safe to pin rather than sweep.

The actual fix is to cap the scanner's I/O concurrency, and it takes **both**
statements:

```sql
SET connector_io_tasks_per_scan_operator = 2;
SET enable_connector_adaptive_io_tasks = false;
```

The second is not optional: with the adaptive scheduler left on it re-raises the
task count at runtime and the cap has no effect. **Neither is currently set by
`starrocks-cid-audit.sh`** — if you widen S4's window back toward the full
partition, add them first.

And the timeout is
**suite-dependent on both engines**: `QUERY_TIMEOUT` (StarRocks) /
`QUERY_TIMEOUT_S` (DuckDB) default to **3600 s when the snapshot suite is in
scope** (`SUITE=all` or `SUITE=snapshot`) and **1800 s for `SUITE=hot`**. 1800 s
is only ~1.17x S4's 1,541 s DuckDB pass and ~0.73x its 2,460 s StarRocks pass, so
at the old flat default a slow pass was truncated by the cap and recorded as
`ERR`/`TIMEOUT` rather than a number; for the hot suite, whose slowest query is
tens of seconds, the lower cap surfaces a genuine hang sooner. The rule is
identical on both sides deliberately — a cap that fires on one engine and not the
other turns the comparison into a comparison of caps — and an explicitly exported
value still wins over it.

**`ScanBytes` is the point, not a bonus column.** A latency gap at equal
ScanBytes is engine/metadata overhead, not I/O, and must not be reported as a
format win. `RATIO > 1` means Iceberg took longer than Paimon.

Two guards worth knowing before reading a cell: a slot whose audit `State` is not
`EOF`/`OK` prints **`ERR`**, not a time — a failed query writes a small `Time`
with `ScanRows=0`, i.e. the fastest-looking result, so without the guard an OOM
on one side is scored as a win for it. And `QUERY_TIMEOUT` (default 3600 s with
the snapshot suite in scope, 1800 s for `SUITE=hot`) is a server-side
per-statement cap, so one pathological scan aborts instead of hanging the suite.
It is deliberately equal to the DuckDB harness's `QUERY_TIMEOUT_S`, which applies
the same suite-dependent rule: a cap that fires on one engine but not the other
turns a cross-engine comparison into a comparison of the two caps.

### `starrocks-bench.sh` — client-side harness

Still the only thing that captures the **file-layout** stats
(`-tablestats.txt`: per-warehouse data-file count, mean size, and the
delete-artifact counts that **must read 0**) — an S3 listing, not SQL, which is
why this script was not retired.

**Its own five queries were merged into the shared suite on 2026-10-06**, so it
now renders the same `.sql` files as the other two harnesses and contains no
query of its own. Four were this suite's queries with no bound; the fifth,
`q1_count`'s unbounded `COUNT(*)`, cannot return at this table size and is not
ported — see [The queries live in one place](#the-queries-live-in-one-place) and
each absorbing file's header.

| was | is now | the difference |
|---|---|---|
| `q1_count` | — | unbounded `COUNT(*)`; its "is COUNT(\*) a metadata shortcut?" question is answered in `S1`'s header, and the harnesses' `cid_events$partitions` read is the O(metadata) counterpart |
| `q2_partition_prune` | `S1_partition_count` | `DATE_FORMAT(NOW(), …)` → pinned `${SNAPSHOT_DT}` |
| `q3_needle_uuid` | `H3_hot_needle_uuid` | + the 5-minute hot window |
| `q4_narrow_agg` | `S2_narrow_agg` | + `dt`, + `LIMIT 20` |
| `q5_wide_scan` | `S4_wide_json` | + `dt` + ingestion window; `LENGTH` → `${BYTES_FN}` |

An old `q*` name in `QUERY_NAMES` fails with the successor named, rather than
just "unknown query".

What remains specific to this script: timing is **client-side**, measured around
the `kubectl exec`, so it includes the exec and mysql round trip — the audit
harness reads the FE audit log and reports the engine's own time, so **never
compare a number from one against a number from the other**. It also interleaves
the two catalogs per query, which keeps the volume gap between arms to one
query's duration with the writers live. Session settings (`enable_spill`, and
the two scan-side IO caps on the snapshot suite) are copied from the audit
harness so both measure the same engine configuration.

```bash
./starrocks/starrocks-bench.sh                                       # hot suite (default)
SUITE=snapshot ./starrocks/starrocks-bench.sh                        # closed partition — S4 is 18-41 min/pass/arm
QUERY_NAMES="H2_hot_count_1m H5_hot_wide_json" ./starrocks/starrocks-bench.sh
```

`SUITE` defaults to **hot**, not to the old whole-table behaviour. For a real
snapshot run prefer `starrocks-cid-audit.sh`, which also runs the pre-flight
gate and suspends the writers.

### `duckdb-cid/run-duckdb-cid.sh` — single-node DuckDB

A `batch/v1` Job, not a FlinkDeployment and not a `build-deploy-cid.sh` target.
It sources `../.bench-env` itself and ships **one** ConfigMap,
`duckdb-cid-bench`, holding **one file: itself**. Editing it is enough, no ECR
push.

**One script, two roles, dispatched by argv.** `run-duckdb-cid.sh` with no
arguments is the submitter and runs on your laptop; `run-duckdb-cid.sh --in-pod`
is the benchmark and is what the Job's entrypoint `exec`s. They were two files
until 2026-10-08, and the merge was not cosmetic: each half independently
derived `QUERYBENCH_S3`, resolved `BUCKET_NAME` and validated `FORMAT`, and they
had **split the `PIN_SNAPSHOT` default** — the submitter chose `1` for the
iceberg arm while the runner defaulted to `0` for that same arm, so a hand-run
pod measured the unpinned path the submitter exists to avoid. One
`cid_pin_default()` now answers for both. The two roles still set **different
shell options** (`-e` is off in the pod, because `run_attempt()` returns the
`duckdb` exit code as data and `137` is a TIMEOUT *result*), which is why the
mode is chosen before the first `set` rather than once at the top: `set -e`
inside a function persists after it returns.

The **queries do not ship with it.** The pod `aws s3 sync`s them from
`$QUERYBENCH_S3` (`s3://${BUCKET_NAME}/flink/querybench/`) into
`$BENCH_SQL_DIR` (`/tmp/bench-sql` — writable, because it is a sync target and
not a mount point) at startup, using the IRSA role that already reads the
warehouse. `starrocks-cid-audit.sh` syncs the same prefix, so it runs the
**same objects**, not merely the same query names — see
[The queries live in one place](#the-queries-live-in-one-place). A missing or
empty prefix aborts the run rather than yielding an empty suite.

There is no longer a `duckdb-cid-bench-sql` ConfigMap (the flattened query tree,
retired when the queries moved to S3) and no `duckdb-cid-bench-py` (the two-file
runner, retired by the merge): `run-duckdb-cid.sh clean` deletes both legacy
names if an earlier run left them behind.

**It needs 18 cpu / 72 GiB of its own** from `executor-memorynodepool`. That is
StarRocks' aggregate BE sizing (3 BEs × 6 cpu / 24 GiB) collapsed onto one node,
which is why the pod is pinned to an `m5.8xlarge` **on-demand** node:
`instance-family: m5` matches the BEs and `arch: amd64` is required because the
nodepool's other nodes are m6g Graviton. A few minutes of `Unschedulable` is
normal — hence `SCHED_GRACE=900`.

```bash
./duckdb-cid/run-duckdb-cid.sh                   # FORMAT=iceberg (default)
FORMAT=paimon ./duckdb-cid/run-duckdb-cid.sh     # separate Job name
./duckdb-cid/run-duckdb-cid.sh clean             # delete both Jobs + the ConfigMap
```

**Shell is the entrypoint; the `.sql` files are the parameters.** The pod runs
`run-duckdb-cid.sh --in-pod`, which renders each `.sql`, feeds a prelude plus the
one statement to the `duckdb` CLI (`duckdb-cli==1.5.6` — the same version as the
Python module, so a reproduction in the debug session runs the same engine as the
measurement), and times the process. Four details are measured rather than
assumed:

- **The elapsed time is process wall clock, not a parsed `.timer` line.** The CLI
  prints one `Run Time (s): real …` **per statement, including every prelude
  `SET` and the `CREATE SECRET`**, so attributing one of them to the query under
  test means counting lines and trusting that count — the same positional
  alignment trap the `.sql` files exist to remove. The prelude measures ~0.3 s
  including `INSTALL`/`LOAD` from the extension cache, is constant across arms
  and passes, and so cannot bias a ratio.
- **The query's rows are captured with `.output`, not by redirecting stdout.**
  Every statement prints to stdout and `CREATE SECRET` prints a one-row `true`,
  so a plain `>` captured *that* as the result. Measured: every query reported
  `first_row=true`, which silently disarms the zero-scan guard (the leading value
  is never `0`) **and** destroys `payload_bytes` as the cross-engine equality
  check — the only thing proving both engines read the same rows.
- **A SQL error still exits 1 with the message on stderr** with `.output` active,
  so the per-attempt status check is unaffected.
- **A failed query reports no median at all, not a zero one.** Found and fixed
  2026-10-08: `printf '%s\n'` with zero arguments still prints one empty line, so
  the median helper saw `NR==1` and `%d`-formatted `""` to `0`. Every `ERROR` and
  `TIMEOUT` query therefore published `"median_ms": 0` — the fastest possible
  number — making a failure the best-looking result in the suite. The array
  length is now tested before the pipeline and `median_ms` is `null`. Same
  empty-result-looks-like-a-win failure the `ZERO_SCAN` guard exists to catch,
  arriving through the summary instead of through the data.

`duckdb-cid-bench.py` **no longer runs the suite.** It is the interactive session
for chasing a result down in the pod, with the same credentials, scan expression
and DuckDB configuration already in place — `scan`, `sql '<stmt>'`,
`query <name>`, `explain <name>`, `repl`. `query`/`explain` render from the
**shared** `.sql` files by calling `bench-sql-lib.sh` rather than reimplementing
the renderer, so what you inspect is what ran; its window literals are resolved
fresh, so they are **not** the measured run's bounds and nothing timed there is
quotable.

It is **not mounted** — the ConfigMap carries one key, the runner itself, since
the measured path never calls this file and a second key would be one more thing
to keep in sync for a by-hand session. `kubectl cp` it into a running Job pod
(the command is in its own docstring). Its `import boto3` is why
`duckdb-cid-job.yaml`'s entrypoint installs `boto3` alongside `awscli`: awscli 1.x
**vendors** its own botocore under `awscli/botocore` and brings neither a
top-level `botocore` nor `boto3`, so this file could not run in-pod at all until
2026-10-08. The two botocore copies are independent by design, which is why
`aws --version` reports a different version than `pip list` and why there is no
conflict to resolve. awscli stays on **1.x** because v2 is not on PyPI at all —
it ships only as a platform bundle, and it exposes no Python library, so it would
not have replaced `boto3` anyway. Nothing here uses a v2-only feature.

It runs the **same pre-flight gate** as the StarRocks harness — same shared
helper, same FE-idle assert, same `SUSPEND_WRITERS` semantics, and the writers
likewise left stopped with no resume — and runs it *before submitting the Job*,
because the pod resolves its pinned snapshot and (on `FORMAT=paimon`) its
`read_parquet` file list at startup. See
[The snapshot suite runs a pre-flight gate first](#the-snapshot-suite-runs-a-pre-flight-gate-first).

Writes `bench-results/duckdb-cid-<format>-<stamp>.log` and, on success, `.json`
(per-query `median_ms`, `runs_ms`, `status`, plus `provenance`). Env patched in
at submit time: `FORMAT`, `SUITE`, `RUNS`, `WARMUP`, `QUERY_TIMEOUT_S`
(unset by default — the pod derives it from `SUITE`; the manifest deliberately
carries no literal, because that literal used to win over the Python and the two
drifted to 900 vs 3600), `SNAPSHOT_DT`, `HOT_DT`, `SNAPSHOT_INGEST_DATE`,
`SNAPSHOT_S4_FROM`, `SNAPSHOT_S4_TO`, `QUERYBENCH_S3`, `BENCH_SQL_DIR`,
`PIN_SNAPSHOT`, `DUCKDB_MEMORY_LIMIT` (56GB, deliberately below the container's
72Gi), `DUCKDB_THREADS` (18), `ICEBERG_DB`, `TABLE_NAME`, `PAIMON_TABLE_ROOT`.
The window overrides are the **same four names the StarRocks harness reads**, so
setting one pins both engines to the same bound; `SNAPSHOT_SAMPLE_N` is gone.
Container exit `137` is a kubelet OOM-kill — give the container more headroom,
**not** a higher `DUCKDB_MEMORY_LIMIT`.

**Both suites are comparable across the two engines, because both engines render
the same ten `.sql` files.** The bound is not merely "the same rule implemented
twice" — `bench_resolve_snapshot_window` and `bench_resolve_hot_windows` are one
shell function each, sourced by both harnesses, and the literals they produce are
recorded in each run's `provenance`. The only text that differs between the two
renderings of a query is `${BYTES_FN}`, and that difference exists precisely so
that `payload_bytes` means the same thing on both sides and can serve as the
cross-engine equality check. See
[The queries live in one place](#the-queries-live-in-one-place).

This replaced a real divergence, kept here because it is the reason the design is
what it is: until 2026-10-06 the queries existed twice — shell heredocs in
`starrocks-cid-audit.sh`, Python f-strings in the DuckDB runner, each
asserting in its own comments that it mirrored the other one-for-one. StarRocks
retired the `uuid % N` sample and switched S4 to an `ingestionTime` window;
DuckDB kept the sample on all four. S1-S3 then read a 1/500 sample on one engine
against the full partition on the other, and S4 the full partition here against
a pruned slice there. **Nothing failed.** Both harnesses ran, both wrote CSV/JSON
with every query `ok`, and the ratios between them were a comparison of two
different experiments. A duplicated query set has no failure mode that announces
itself, which is why the fix was to delete one copy rather than to re-align it.

Two findings from that era are preserved because they are still the right answer
if anyone reintroduces a sampled arm:

- DuckDB has **no `CONV()`**. The StarRocks equivalent is
  `('0x'||substr(uuid,1,8))::BIGINT % N = 0`. The 8-hex-digit block maxes at
  4,294,967,295, well inside `BIGINT`, so there is no overflow and no need for
  `UBIGINT`. Verified on DuckDB 1.5.4 against the boundaries `00000000` and
  `ffffffff`, and against 2,000 real `uuid` values from the warehouse — **zero
  disagreements** with StarRocks' `CONV` semantics. Selectivity on three real
  Paimon files (278,905 rows): 0.2051% at N=500 and 1.9842% at N=50, against
  ideals of 0.2% and 2%.
- **Do not substitute `hash(uuid)`.** It is a different sample, so results would
  stop being comparable even against this harness's own earlier runs.

Dropping the sample cost nothing in scan volume, because **`uuid` cannot prune**:
it is MD5-derived, so every Parquet row group's min/max spans the whole keyspace.
Measured at N=1000, each arm's `ScanRows` equalled its exact full partition count
(3,217,389,927 / 3,217,389,927 / 4,180,412,213) — the predicate was evaluated on
every row it claimed to skip.

The oldest-`dt` resolver lists the `dt=` prefixes with `Delimiter='/'` — one
`LIST`, not a scan of the ~62,000 data files. It probes **candidate roots in
order** rather than branching on `FORMAT`, because `FORMAT` does not identify
the layout: the native Iceberg table nests data under `/data/`, Paimon puts
`dt=` directly under the table root, and the `ICEBERG_DB=flink_cid_paimon_db`
export arm is read by the Iceberg reader while its data files live in **Paimon's**
warehouse. A single-root version returned nothing for that third arm and fell
through to the yesterday-UTC fallback, i.e. to the *newest, live* partition —
the precise silent failure the oldest-`dt` rule exists to prevent. All three
arms now resolve `2026-10-04`, and the chosen source prints as `dt source`.

**`PIN_SNAPSHOT=1` on any `iceberg_scan` arm.** The writers retain only a few
`metadata.json` files and commit a new one every ~60 s, so a location resolved
at startup is deleted within minutes and the scan 404s mid-run. This applies to
the Paimon-exported Iceberg tree as well as the native table.

⚠ **A DuckDB paimon-vs-iceberg number is a READ-PATH result, not a format
result.** DuckDB has no Paimon reader. With `ICEBERG_COMPAT=false`,
`FORMAT=paimon` reads the warehouse's Hive-partitioned Parquet via
`read_parquet`, which works **only** because the table is append-only with zero
deletion-vector indexes — and that path cannot prune by column statistics.
`iceberg_scan` does manifest planning against a pinned snapshot; `read_parquet`
does an S3 `LIST` and sees whatever is live now, including a file mid-upload
(`paimon_files_at_start` records that drift). With `ICEBERG_COMPAT=true`
(`.bench-env`'s setting) you can instead point `ICEBERG_DB=flink_cid_paimon_db`
at the export and get `iceberg_scan` on both sides. **State which arm you ran
anywhere a DuckDB ratio is quoted.** The format comparison that holds
unconditionally is StarRocks', which uses a native reader on both sides.

---

## Raw landing-zone generator (gzipped NDJSON)

A **simpler, separate** data generator for when the system under test reads the
raw landing zone rather than a Paimon/Iceberg table. It writes `.json.gz` files
to S3 directly — no Flink, no table format. The Flink datagen path above
(`sql-scripts/common/01-datagen-source.sql` → Paimon/Iceberg) is unchanged.

- **Same data.** Same fields, value ladders and presence rates as
  `01-datagen-source.sql`, i.e. the real `cid_9011075d_sample_masked` extract.
- **The source's layout, minus its `x=00/` shard level:**
  `year=YYYY/month=MM/day=DD/hour=HH/customerId=<uuid>/productCode=sds|ddr/<coupledTraceId>_NNN_new.json.gz`
  — one file per agent upload (one customer, one coupledTraceId), ~2,500 rows.
- **Per-row targets:** ~4,096 B uncompressed, ~0.2 KB gzipped (~20x; the real
  sample is 3,818 B / 195 B). The ratio comes from within-file locality
  (endpoint runs, process bursts), not from padding — see the header of
  `cid_jsongz_gen.py`.
- **Rate:** `JSONGZ_ROWS_PER_SECOND` (default 1,500,000 rows/s ≈ 6.1 GB/s raw,
  ≈ 0.31 GB/s gz) is a cluster-wide **cap**, split evenly over
  `TOTAL_PODS x WORKERS`. `TOTAL_PODS` defaults to
  `ceil(rate / (WORKERS x 25,000))`; check `status` and add pods if the
  delivered rate is short. It is deliberately not `SOURCE_ROWS_PER_SECOND`,
  which `.bench-env` sets for the Flink path.
- **Run length: a fixed 30 minutes (1800 s).** Every pod stops writing at
  1800 s, then the Job completes on its own; `stop` is only needed to end
  it early. `DURATION_SECONDS` can only **shorten** the run (1..1800, e.g.
  `DURATION_SECONDS=120` for a smoke test). `0`, non-integers and values above
  1800 are rejected, and there is no run-until-stopped mode. The Job also
  carries `activeDeadlineSeconds` = duration + 300 s as a backstop for pip
  install, pod start and the final upload flush.

```bash
cd throughput-benchmark/jsongz-gen
./run-jsongz-gen.sh local                 # 30 s on this machine -> /tmp/cid-jsongz
source ../.bench-env
./run-jsongz-gen.sh render                # sizing summary + rendered Job
./run-jsongz-gen.sh start                 # ConfigMap + Indexed Job; runs 30 min, then completes
./run-jsongz-gen.sh status                # per-pod rows/s, MB/s, B/row, ratio
./run-jsongz-gen.sh stop                  # end early / delete the Job; written files stay
# Knobs: JSONGZ_ROWS_PER_SECOND DURATION_SECONDS(1..1800, shorten only) WORKERS TOTAL_PODS
#        GZIP_LEVEL ROWS_PER_FILE CID_CUSTOMER_COUNT CID_HOST_COUNT OUTPUT_URI
#        GEN_NODEPOOL GEN_INSTANCE_FAMILY GEN_INSTANCE_SIZE
```

### Feeding the Flink jobs from these files

`SOURCE_MODE` in `build-deploy-cid.sh` picks which file is uploaded as each
format's `00-source.sql`:

| `SOURCE_MODE` | file | reads |
|---|---|---|
| `datagen` (script default) | `sql-scripts/common/01-datagen-source.sql` | in-memory datagen at `SOURCE_ROWS_PER_SECOND` (the original path) |
| `s3json` (`.bench-env`) | `sql-scripts/common/01-s3json-source.sql` | `.json.gz` under `S3JSON_SOURCE_PATH`, continuously |

Both define the same `datagen_source` view with the same 34 columns, so
`04-pipeline.sql`, the sink tables and the queries are unchanged. **To switch
back,** set `export SOURCE_MODE=datagen` in `.bench-env` (or delete the line)
and redeploy; the deploy re-uploads `00-source.sql`.

```bash
cd throughput-benchmark && source .bench-env          # SOURCE_MODE=s3json
OUTPUT_URI=s3://$BUCKET_NAME/cid-landing-jsongz ./jsongz-gen/run-jsongz-gen.sh start
./build-deploy-cid.sh deploy paimon                   # or iceberg
```

- `S3JSON_SOURCE_PATH` (default `s3a://<bucket>/cid-landing-jsongz/`) must
  be the generator's `OUTPUT_URI`, written as `s3a://`.
  `S3JSON_MONITOR_INTERVAL` (default `10s`) is how often the source re-lists
  it.
- **Files already under the path are read too**, not only new ones. For a
  run that sees only new data, point both sides at a fresh prefix.
- **New files stop arriving after 30 minutes**, when the generator Job
  completes. The Flink job keeps running and polling, but idles once it has
  read the backlog. Size the measurement window to fit inside those 30
  minutes.
- In this mode `SOURCE_ROWS_PER_SECOND`, `HOT_KEY_FRACTION` and `CID_*` do
  nothing on the Flink side. The rate is what the generator lands, and the job
  reads as fast as it can.
- **The source can now be the bottleneck.** Each row costs an S3 read, an
  inflate and about 30 `JSON_VALUE` parses of a 4 KB line. Backpressure with
  idle sinks means the read side is the wall, so results are not comparable
  with `datagen`-mode runs.
- The continuous enumerator keeps every processed path in JobManager state and
  checkpoints, about 2 M paths/hour at 1.5 M rows/s. That is fine for a
  benchmark-length run, not for days.

---

## How it works

With the default `SOURCE_MODE=datagen`, the source is Flink's built-in
`datagen` connector (for `s3json`, see [above](#feeding-the-flink-jobs-from-these-files)) — rows are generated
in-memory on the TaskManagers (no Kafka/RDS in the read path), so the source can
never be the bottleneck and the sink write path is the only thing measured. If a
writer can't keep up, datagen backpressures and the achieved source rate **is**
that format's result.

The row is a **mockup of real security telemetry**, not synthetic padding. It
reproduces the shape of a production CID extract (`cid_9011075d_sample_masked`:
189,013 gzipped-NDJSON records, 170 distinct fields) and emits the 33 extracted
columns + `event_json` from the ingestion PoC's `schema_sample.sql`. The *data*
has real field cardinalities, real skew and real null density, so Parquet
compression, dictionary encoding and min/max pruning behave as they do on the
real table. See `sql-scripts/common/01-datagen-source.sql` for the measured
distributions and which ~95 of the 170 fields `event_json` carries.

**Row size is calibrated to a 4096 B mean.** The real sample's mean line is
3,817 B, so the generator runs ~7% wider: the five command-text fields carry
pads of 295/221/135/117/98 B — the sample's measured per-field means scaled by a
single factor of 1.233, solved by Monte-Carlo over the generator's own
probability model. Result: **mean 4,099 B (+0.07%)**, p50 4,163, p95 5,123.

Two consequences worth knowing before citing a number:

- **The pad is on the command fields, not a filler column.** Those fields are
  already where the real bytes are (the top four are 21% of sample bytes), and
  they are variable-width and sparsely present, so widening them preserves the
  null pattern and compression behaviour. A dense fixed-width filler would hit
  the byte target while destroying the properties this mockup exists to
  reproduce.
- **The model has no heavy tail.** p95 is 5,123 B vs the sample's 8,625, and max
  ~6,001 B vs 111,445 — the real outliers are pathological command lines.
  Irrelevant to a throughput number (driven by the mean); relevant if you are
  chasing per-row buffer high-water marks.

Because the size is a **mean over a distribution**, `rows/s × 4096` is an
expectation, not an instantaneous bytes/s guarantee.

### What append-only implies

- Paimon does not merge sorted runs to resolve keys; an append table's
  compaction only coalesces small files. Writers use `AppendOnlyFileStoreWrite`,
  and `write-only=false` leaves that coalescing inline in the writer — the only
  thing bounding file count, since no compactor ships here.
- Iceberg writes **no delete files at all**, so merge-on-read cost is absent from
  both the write and the read side.
- Primary-key knobs are **removed, not set to false**, on both sides: Paimon has
  no `changelog-producer` / `deletion-vectors` / `num-sorted-run.*`, and Iceberg
  has no `write.upsert.enabled` and no `write.*.mode=merge-on-read`. Absent is
  stronger than disabled — there is no property to accidentally flip.
- `HOT_KEY_FRACTION` routes the **endpoint identity** (a few noisy hosts
  producing most telemetry), not a primary key. It changes dictionary encoding
  on the endpoint columns — and therefore the generated bytes — but drives no
  merge or delete work, and it does **not** skew Paimon's bucket distribution:
  `bucket-key = 'uuid'`, and `uuid` comes from an independent full-range draw
  that this knob never touches.

Both sinks declare the identical 34 columns in the identical order with the same
identity partitioning on `dt`. `CID_DAY_SPREAD` sets how far back the generator
spreads `dt`: `datagen`'s `r_day` bounds are **inclusive**, so N spreads over
`N+1` days and the default 1 writes **two** live partitions, today and yesterday.
That is what gives the snapshot suite a previous day to read.

### Parity matrix

Every option with a counterpart is pinned on both sides. Change one, change its
pair:

| Dimension | Paimon | Iceberg |
|---|---|---|
| Compression | `file.compression=snappy` | `write.parquet.compression-codec=snappy` |
| Row groups | `parquet.block.size=33554432` | `write.parquet.row-group-size-bytes=33554432` |
| Target file | `target-file-size=256mb` | `write.target-file-size-bytes=268435456` |
| Snapshot retention | `snapshot.time-retained=1h`, `num-retained.min=5`/`max=240` | `history.expire.max-snapshot-age-ms=3600000`, `min-snapshots-to-keep=5` |
| Manifests | `manifest.target-file-size=8mb`, `merge-min-count=5` | `commit.manifest.target-size-bytes=8388608`, `min-count-to-merge=5` |
| Column stats | `metadata.stats-mode=truncate(16)` | `write.metadata.metrics.default=truncate(16)` |
| Commit cadence | `execution.checkpointing.interval=60s` | same |
| Barrier alignment | aligned only (hard Paimon limit) | `unaligned.enabled=false` to match |

Four of these are easy to break and worth restating:

- **`snappy`, with no compression level on either side.** Snappy has no levels,
  so the option is *removed* rather than set — a stray level would be ignored on
  one side and meaningful on the other.
- **`num-retained.max=240`, not 20.** Iceberg expresses snapshot retention by
  **age only** and has no count ceiling to pair with, so any Paimon count bound
  tight enough to bind before the age rule silently gives the two sides
  different history — Paimon pinned at the count while Iceberg keeps everything
  inside the age window. 240 ≈ 4 h of 60 s commits, so the 1 h **age** rule
  always binds first and both sides agree on the mechanism. It stays finite as a
  runaway-commit backstop.
- **Unaligned checkpoints must stay `false` on both.** The Paimon sink *cannot*
  support them (`paimon-flink-1.20-1.3.2.jar` throws outright). Leaving Iceberg
  unaligned would hand it an option on precisely the axis Paimon is structurally
  forced to pay.
- **Column stats are pinned explicitly**, not left to either engine's default
  happening to match. `truncate(16)` also matters on the read side: with `none`,
  StarRocks gets no file-level min/max on the Iceberg side and cannot prune
  files.

Options with **no counterpart** are either the thing being measured (Paimon's
`write-buffer-size`) or inherent asymmetries — including the fact that the two
sinks expose their upload shape through different configuration surfaces — see
[Known asymmetries](#known-asymmetries).

### Config differences that are on purpose

- Paimon uses `changelog-producer='none'` (CDC tables use `lookup`): no
  downstream changelog consumer here, and lookup adds compaction-time
  point-lookup cost Iceberg has no analog for.
- Paimon uses fixed buckets = `PAIMON_BUCKETS` = `JOB_PARALLELISM` on
  `uuid`, not the CDC tables' dynamic `bucket=-1`: dynamic bucketing maintains a
  key→bucket index, which would dominate the measurement. `uuid` is unique per
  record, so `hash(uuid) % bucket` is uniform by construction. Each bucket has
  exactly one writer subtask, so the count must be at least the parallelism:
  `JOB_PARALLELISM / 2` (2026-10-06 to 2026-10-09) left 80 of 160 writers with
  no rows and the source 13% backpressured.
- Iceberg uses `write.distribution-mode='none'`, **not** `'hash'`. With one
  distinct `dt`, hashing on the partition key would serialize every write into a
  single subtask. Paimon's bucket hash is on `uuid`, not the partition column,
  so it has no equivalent failure mode.

### Iceberg-compat toggle

`ICEBERG_COMPAT` chooses which Paimon you benchmark:

| | `false` (script default) | `true` (`.bench-env`) |
|---|---|---|
| Framing | Paimon's bare write path | Paimon exactly as the CDC pipeline runs it |
| Commit cost | Paimon metadata only | + Iceberg V2 metadata export to Glue per commit |
| External reads | Paimon readers only | Athena / StarRocks / DuckDB via the export |

It maps to the table's `'metadata.iceberg.storage'` (`disabled` vs
`hive-catalog`). **The value is baked into the Paimon schema at `CREATE TABLE`**
— the DDL is `IF NOT EXISTS`, so flipping it does nothing to an existing table.
Drop the table first. The Iceberg-side job is unaffected either way. Paimon
write numbers taken at `true` are **not** comparable with `false` runs: the
export is commit-path work the Iceberg side does not pay.

The Paimon write-path options are instead applied as **job-scoped INSERT hints**
in `sql-scripts/paimon/04-pipeline.sql`, which take effect against an existing
table with no drop. Two of the three are **fixed**: `write-only=false` and
`write-buffer-spillable=true`, hardcoded in `build-deploy-cid.sh`. Iceberg's
writer has no equivalent of either, so a non-default Paimon value makes the two
sides structurally different write paths and the result stops being about
format. The `WRITE_ONLY`/`SPILLABLE` env knobs were removed 2026-10-05 along
with the exploratory arms that gave them a second value. Only `WRITE_BUFFER`
remains tunable.

---

## Reading the results

The headline number is `source_rec_per_s` in each CSV:

- At the configured rate, the format **keeps up** — compare
  `last_ckpt_duration_ms` (snapshot-commit cost) and sink `busy_ms` for
  remaining headroom.
- Below it, datagen is backpressured by that format's writer and the achieved
  rate **is** its write-path ceiling. `GiB/s = source_rec_per_s × 4096 / 2³⁰`.

Watch Paimon's rate over time, not just its average: compaction cost is
periodic, so throughput saw-tooths. On an append table that saw-tooth is
small-file coalescing, not key resolution. A single mean hides it.

**Never quote the ingest rate on its own.** Either side can buy throughput by
deferring maintenance — small files and unexpired snapshots. Pair the rate with
stored bytes, live record count, **data-file count and mean file size**, and
snapshot count. Delete-file and deletion-vector counts are an **assertion that
must read 0**, not a cost explanation: non-zero means something violated
append-only and the run is void.

Check *where* a stalled job is stalled before blaming the format. A writer
reporting low `busy_ms` **and** low backpressure while its upstream vertex is
blocked is stuck inside Paimon's barrier-time buffer flush, which Flink counts as
neither busy nor backpressured — a checkpoint-tuning problem, not a write-path
ceiling. Compare `last_ckpt_duration_ms` against the 60 s interval; `kubectl top
pod` settles it, since saturated TMs mean a real ceiling and idle TMs mean a
stall.

**Paimon's flush time appears in no checkpoint metric — derive it.** The cost
lives in `PrepareCommitOperator.prepareSnapshotPreBarrier()`, which Flink
attributes to neither `sync`, `async` nor `alignment`. Compute it as a residual,
`e2e − start_delay − alignment − sync − async`:

```bash
JM=$(kubectl get pods -n emr-flink --no-headers \
       | awk '/flink-cid-paimon-[0-9a-f]{9,}/{print $1}' | head -1)
kubectl exec -n emr-flink "$JM" -c flink-main-container -- bash -c '
JID=$(curl -s localhost:8081/jobs | python3 -c "import sys,json;print(json.load(sys.stdin)[\"jobs\"][0][\"id\"])")
VID=$(curl -s localhost:8081/jobs/$JID | python3 -c "
import sys,json
print([v[\"id\"] for v in json.load(sys.stdin)[\"vertices\"] if v[\"name\"].startswith(\"Writer\")][0])")
CK=$(curl -s localhost:8081/jobs/$JID/checkpoints | python3 -c "
import sys,json
print([c[\"id\"] for c in json.load(sys.stdin)[\"history\"] if c[\"status\"]==\"COMPLETED\"][0])")
curl -s localhost:8081/jobs/$JID/checkpoints/details/$CK/subtasks/$VID | python3 -c "
import sys,json
s=json.load(sys.stdin)[\"summary\"]
e=s[\"end_to_end_duration\"][\"max\"]; d=s[\"checkpoint_duration\"]
sy=d[\"sync\"][\"max\"]; a=d[\"async\"][\"max\"]
al=s[\"alignment\"][\"duration\"][\"max\"]; sd=s[\"start_delay\"][\"max\"]
print(\"e2e=%d sync=%d async=%d align=%d start_delay=%d -> FLUSH=%d ms\"%(
  e,sy,a,al,sd,e-sy-a-al-sd))"'
```

Query a **COMPLETED** checkpoint — in-progress ones return nulls and `-1`, and
subtracting them raises a type error. `/checkpoints/details/<id>` alone gives
per-vertex objects with no timing breakdown; the `/subtasks/<vertex-id>` path is
the one with `summary`. **These fields die with the JobManager.**

**Verify Paimon actually compacted.** With `write-only=false` the writer should
produce `COMPACT` snapshots alongside `APPEND` ones; all-`APPEND` means nothing
was ever coalesced and the file count grew unbounded:

```bash
aws s3 cp s3://$BUCKET_NAME/cid-warehouse-paimon/flink_cid_paimon_db.db/cid_events/snapshot/ \
    - --recursive 2>/dev/null | grep -o '"commitKind":"[A-Z]*"' | sort | uniq -c
```

### Secondary comparisons

```bash
# Stored bytes and file counts per warehouse:
aws s3 ls --recursive --summarize s3://$BUCKET_NAME/cid-warehouse-paimon/  | tail -2
aws s3 ls --recursive --summarize s3://$BUCKET_NAME/cid-warehouse-iceberg/ | tail -2

# Iceberg file/delete accounting — from the snapshot summary, NOT from S3
# filenames. Iceberg names delete files with the same NNNNN-N-<uuid>-NNNNN.parquet
# pattern as data files, so grepping keys for "delete" returns 0 even when
# delete files are present.
LATEST=$(aws s3 ls s3://$BUCKET_NAME/cid-warehouse-iceberg/flink_cid_iceberg_db.db/cid_events/metadata/ \
           | grep '\.metadata\.json' | sort -k1,2 | tail -1 | awk '{print $4}')
aws s3 cp "s3://$BUCKET_NAME/cid-warehouse-iceberg/flink_cid_iceberg_db.db/cid_events/metadata/$LATEST" - \
  | jq -r '{snapshots:(.snapshots|length)}, (.snapshots[-1].summary
      | {total_data_files:."total-data-files", total_delete_files:."total-delete-files",
         total_records:."total-records", total_files_size:."total-files-size"})'

# Paimon's equivalent (live records + last commit kind):
aws s3 cp s3://$BUCKET_NAME/cid-warehouse-paimon/flink_cid_paimon_db.db/cid_events/snapshot/LATEST - \
  | xargs -I{} aws s3 cp s3://$BUCKET_NAME/cid-warehouse-paimon/flink_cid_paimon_db.db/cid_events/snapshot/snapshot-{} - \
  | jq -r '{commitKind,totalRecordCount,deltaRecordCount}'
```

Because the tables are append-only, read latency is **scan economics** — file
count, file size, projection pushdown, partition pruning — not merge-on-read
cost.

---

## Knobs

This table is **write-side only**. The read-side knobs are the window overrides,
and **every one of them is read by both harnesses from the same shared
resolver**, so setting one pins both engines to the same rows:

| Env var | Default | Overrides |
|---|---|---|
| `SNAPSHOT_DT` | the **oldest** `dt` in the table, from the data | the partition S1-S4 read |
| `SNAPSHOT_INGEST_DATE` | `SNAPSHOT_DT` + 1 day | the *ingestion* date in S4's window — not the partition date; `dt=2026-10-04` was written on 2026-10-05 |
| `SNAPSHOT_S4_FROM` / `SNAPSHOT_S4_TO` | a 30-minute slice | S4's `ingestionTime` bounds |
| `HOT_DT` | today, UTC | the live partition H1-H6 read |
| `HOT_TO` | now, UTC, resolved once | the shared upper bound of all three hot windows |
| `HOT_FROM_1M` / `HOT_FROM_5M` / `HOT_FROM_10M` | `HOT_TO` − 1/5/10 min | the three hot window widths. Derived from the same instant as `HOT_TO`, so the windows are strict subsets of each other; override one and that stops being true |

Details and the reasoning behind the defaults are with
[the snapshot bound](#the-snapshot-bound-dt-for-s1-s3-an-ingestiontime-window-for-s4)
and [The queries live in one place](#the-queries-live-in-one-place).
`SNAPSHOT_SAMPLE_N` is **retired** — it has no counterpart on either engine now.

| Env var | Default | Meaning |
|---|---|---|
| `SOURCE_ROWS_PER_SECOND` | `2000000` | global rate; 4096 B **mean** row ⇒ ≈7.63 GiB/s. `.bench-env` raises it to `2097152` (exactly 8 GiB/s). A rate **limit**, not a guarantee — the 2026-10-05 runs delivered ~555 K rows/s against it. The SQL carries a different fallback (`1572864`, the old 6 GiB/s target) which is **unreachable in practice**: `build-deploy-cid.sh` always ships this as pod env, so the literal applies only if the JobManager env is missing |
| `JOB_PARALLELISM` | `128` | writer subtasks. 4 slots per TM, so 32 TMs. `.bench-env` sets `160` |
| `PAIMON_BUCKETS` | `JOB_PARALLELISM` | Paimon `bucket` count, **baked at `CREATE TABLE`**. Derived in `build-deploy-cid.sh`; set it explicitly to break the 1:1 relation. Each bucket has exactly one writer subtask, so fewer buckets than subtasks idles writers (the `/2` used 2026-10-06..09 left 80 of 160 with no rows). Floored at 1. **Changing it does nothing to an existing table** — the sink DDL is `CREATE TABLE IF NOT EXISTS`, so you must drop `cid_events` or use a fresh warehouse prefix, and never compare results across the change without saying which count produced them |
| `HOT_KEY_FRACTION` | `0` | % of rows routed to a hot 5% slice of `CID_HOST_COUNT`; `0` = uniform. Validated as an integer 0..99, because it is compared against a datagen `selector` drawn in `[0,99]`. Routes the endpoint identity, **not** a key, and does **not** affect Paimon's bucket distribution (`bucket-key = 'uuid'`, drawn independently). Its real effect is on **generated bytes** — the endpoint columns dictionary-encode better when skewed — so runs at different values are not comparable |
| `CID_CUSTOMER_COUNT` | `16` | distinct `customerId` ⇒ `partition_cust` cardinality |
| `CID_HOST_COUNT` | `1024` | distinct endpoints; sample measured 1,047 |
| `CID_DAY_SPREAD` | `1` | how far back `dt` spreads. `r_day`'s bounds are **inclusive**, so N ⇒ `N+1` live partitions (default 1 ⇒ today *and* yesterday). `dt` is the only partition column, so that is `PAIMON_BUCKETS × (N+1)` concurrently-open files on Paimon (which fans out by bucket) and `JOB_PARALLELISM × (N+1)` on Iceberg (which fans out by subtask), and yesterday only closes once the writers stop. Report it with any result |
| `WRITE_BUFFER` | `128mb` | Paimon `write-buffer-size`. Only moves bytes between RAM and spill files; the bytes the barrier external-merges are set by rate × checkpoint interval. INSERT hint, so no table drop needed |
| `S3A_MULTIPART_SIZE` | `16M` | **Paimon side only** (floor 5M). The S3A block cut point — must stay **below** the data file size, or S3A uploads each file as a single PUT at `close()` instead of streaming blocks as they fill. `.bench-env` raises it to `32M`. Must be written as an integer number of `M`; the script rejects other units loudly rather than misconverting them |
| `S3A_MULTIPART_THRESHOLD` | `16M` | Multipart COPY path only; the write path never reads it. Kept equal to the part size for consistency, so `.bench-env`'s `32M` moves both |
| `S3A_ACTIVE_BLOCKS` | `6` | queued blocks per output stream before `write()` blocks. Above hadoop-aws's default of 4 for pipelining, deliberately not higher: the disk-buffer cost is `size × blocks × slots × TMs-per-node` against a 50 GiB root volume shared with the spill files |
| `ICEBERG_COMPAT` | `false` | Paimon per-commit Iceberg metadata export. Baked in at `CREATE TABLE` |
| `JM_NODEPOOL` / `TM_NODEPOOL` | `driver-nodepool` / `executor-memorynodepool` | Karpenter nodepools |
| `NAMESPACE` | `emr-flink` | |
| `EMR_VERSION` | `7.13.0` | same release the CDC pipeline runs |
| `IMAGE_TAG` | `$EMR_VERSION` | must exist on the `emr-flink-cdc-paimon` ECR repo |

`write-only` and `write-buffer-spillable` are **not** here: they are fixed at
`false` / `true` in `build-deploy-cid.sh` and cannot be set from the environment.

**Three places set this and they do not agree — know which one you are
running.** `build-deploy-cid.sh` defaults both to `16M`, the 2026-10-05
*reduction* from 32M argued in its own header: at 160 parallelism each subtask's
share of the target is ~51 MiB/s, so files arrive small and often, and 16M keeps
a ~60 MB file at 4 parts instead of 2 — restoring pipelining and giving the
retry path a smaller unit. `.bench-env` overrides both back to `32M`, and the
SQL fallbacks in `sql-scripts/paimon/02-catalog-setup.sql` are `32M`. So a run
that forgets to source `.bench-env` gets 16M, and a run that loses the
JobManager env gets 32M. **Report the value with any result.** The per-node disk
arithmetic that `build-deploy-cid.sh` guards passes either way —
`size × 6 blocks × 4 slots × 4 TMs` is **1.5 GiB/node at 16M** and **3 GiB/node
at 32M**, both under the script's 8 GiB warn threshold, on a 50 GiB root volume
that also holds Paimon's write-buffer spill. The threshold
affects only the multipart **COPY** path — the write path never reads it — and
is kept equal to the part size for consistency rather than because anything
depends on it.

Upload tuning is applied to **both** jobs, but by different mechanisms, because
the two write paths are different code. On the Paimon side the keys must be
**catalog** options: Paimon writes through its own shaded `S3FileIO`, which
builds `S3AFileSystem` from a bare `new Configuration()` plus only the
`s3./s3a./fs.s3a.`-prefixed catalog options — neither `flinkConfiguration` nor
EMR's `core-site.xml` reaches it, which is why these knobs exist at all.

**The Iceberg side gets the same tuning through completely different keys, and
`fs.s3a.*` would be a silent no-op there.** Its catalog sets
`io-impl = org.apache.iceberg.aws.s3.S3FileIO`, which drives the AWS SDK v2
client directly and never constructs an `S3AFileSystem`, so no `fs.s3a.*` key
from any source reaches it. `build-deploy-cid.sh` therefore *derives* two
Iceberg-shaped values from the same two knobs and substitutes them into
`sql-scripts/iceberg/02-catalog-setup.sql`:

| Derived var | From | Iceberg catalog key |
|---|---|---|
| `ICEBERG_S3_MULTIPART_SIZE_BYTES` | `S3A_MULTIPART_SIZE` × 1024² | `s3.multipart.part-size-bytes` (**bytes**, not a size string: `16777216` at the script default, `33554432` under `.bench-env`) |
| `ICEBERG_S3_MULTIPART_THREADS` | `S3A_ACTIVE_BLOCKS` × 4 slots = 24 | `s3.multipart.num-threads` |

Neither is settable directly — set the `S3A_*` knobs and both sides move
together. The thread count is a derivation, not a copy: S3A caps queued blocks
*per output stream* (per-node total = blocks × slots × TMs), while S3FileIO has
no per-stream cap and uses one shared pool per TM JVM, so 24 threads puts both
at the same per-node staging budget — 24 × 16 MiB × 4 TMs = 1.5 GiB at the
script default, 3 GiB under `.bench-env`'s 32M.
**`s3.multipart.threshold` is deliberately left unset** — in Iceberg it is a
dimensionless *factor* of the part size (default 1.5), not a byte count, so
passing `33554432` would read as a 33-million-fold multiplier and disable
multipart upload outright. In both cases this is a parity fix — uploading blocks
as they fill rather than one PUT at `close()` — not an advantage handed to
either format.

---

## Layout

```
throughput-benchmark/
├── .bench-env                       # sourceable env (region, bucket, role, knobs)
├── flink-cid-executor.py            # generic SQL executor (copy of cdc-pipeline's)
├── flink-cid-paimon.yaml            # FlinkDeployment — Paimon sink
├── flink-cid-iceberg.yaml           # FlinkDeployment — Iceberg sink
├── flink-cid-*-deployed.yaml        # rendered output of render/deploy — GENERATED
│                                    #   and not committed. Absent on a fresh
│                                    #   clone; `render` or `deploy` writes it.
├── sql-scripts/                     # uploaded to s3://.../flink/cid-sql/<fmt>/ by
│   │                                #   every render/deploy (sync --delete, BOTH
│   │                                #   formats); the jobs read S3, not this dir.
│   ├── common/01-datagen-source.sql # THE generator: CID telemetry mockup, 34 cols.
│   │                                #   Copied into both format prefixes AS
│   │                                #   00-source.sql — runs before the USE CATALOG.
│   ├── paimon/                      # catalog + append sink + pipeline
│   └── iceberg/                     # catalog + v2 append sink + pipeline
├── monitoring/                      # side-by-side monitor; code from cdc-pipeline
├── build-deploy-cid.sh              # render / deploy / cleanup / monitor (no build)
├── collect-metrics.sh               # streaming samples -> bench-results/cid-*.csv
├── snapshot-preflight.sh            # pre-flight gate: assert FE idle, stop the
│                                    #   writers; shared by both query harnesses
│                                    #   (snapshot suite). No resume.
├── querybench-script/               # THE QUERIES — editing copy; published to
│   ├── hot/H1..H6*.sql              #   s3://.../flink/querybench/ and synced
│   ├── snapshot/S1..S4*.sql         #   from there by BOTH harnesses.
│   ├── bench-sql-lib.sh             #   Filename = name = order.
│   └── bench-sql.sh                 #   plan / render / check / upload / ls
├── starrocks/
│   ├── starrocks-cid-audit.sh       # READ half, timed from the FE AUDIT LOG
│   └── starrocks-bench.sh           # READ half, client-side (older; +tablestats)
├── duckdb-cid/                      # READ half, single-node DuckDB batch Job
│   ├── run-duckdb-cid.sh            #   ONE script, TWO roles: bare = submit
│   │                                #   (ConfigMap + env patch); --in-pod = the
│   │                                #   benchmark. Merged 2026-10-08.
│   ├── duckdb-cid-bench.py          #   NOT the runner, and NOT mounted —
│   │                                #   interactive debug session, kubectl cp'd in
│   └── duckdb-cid-job.yaml          #   Job spec (m5.8xlarge, 18 cpu / 72Gi)
├── jsongz-gen/                      # RAW landing zone: gzipped NDJSON on S3, no Flink
│   ├── cid_jsongz_gen.py            #   the generator (multiprocess, boto3 PUTs)
│   ├── jsongz-gen-job.yaml          #   Indexed Job, one pod per m6i.16xlarge
│   └── run-jsongz-gen.sh            #   start / status / logs / stop / clean / local
└── bench-results/
```

`querybench-script/` is deliberately a **sibling** of the two harness
directories rather than living inside either, and it is not read by either one:
`./bench-sql.sh upload` publishes it to `s3://${BUCKET_NAME}/flink/querybench/`
and both harnesses sync from there. Editing a query touches neither runner —
publishing it is the whole deploy.

Each format gets its own Glue database (`flink_cid_paimon_db` /
`flink_cid_iceberg_db`), S3 warehouse prefix (`cid-warehouse-paimon/` /
`cid-warehouse-iceberg/`) and Flink state prefix (`flink/cid-paimon/` /
`flink/cid-iceberg/`); SQL goes to `flink/cid-sql/<fmt>/`. The CDC pipeline's
databases are never touched. The manifests differ only in format-specific tokens
plus the Paimon job's `aws.glue.enabled: "true"` (required by its
Glue-via-Hive catalog), an explicit `unaligned.enabled: "false"` on Iceberg, and
each side's own S3 upload tuning, which cannot be shared because the two sinks
read it from different places.

Each format is **one** deployment. Paimon compacts inline in the writer and
Iceberg's sink never rewrites data files, so neither side runs a companion job —
the `paimon-compact` target, the `iceberg-compaction/` Spark harness, burst mode
and the jsongen dataset generator were all removed 2026-10-03.

The DuckDB harness here is **separate from, and not a duplicate of,**
`../cdc-pipeline/duckdb/`: that one runs the CDC pipeline's `Q1_count`…
`Q6_multi_join` suite against `flink_iceberg_db`, this one runs the throughput
project's `H*`/`S*` suite against `cid_events`. Same sizing rationale, different
tables and different queries.

---

## Known asymmetries

Inherent to the formats or the surrounding infrastructure — they cannot be
equalized without breaking the setup, so cite them alongside results.

- **Compaction ownership is the central asymmetry.** Paimon's compaction is *in
  the engine*, inline in the writer, running continuously as files arrive.
  Iceberg's Flink sink **never rewrites data files at all**
  (`commit.manifest-merge.enabled` merges *manifests* only), so compaction would
  require an external engine calling `rewrite_data_files`, and none ships here —
  its file count only grows for the length of the run. **Paimon pays coalescing
  CPU inside the measured write path and Iceberg pays none**, which penalises
  Paimon on rows/s and penalises Iceberg on everything measured afterwards.
  **Compare mean data-file size alongside rows/s**, sample file count over time
  on both sides, and do not benchmark reads against these tables without
  compacting them first by some other means.
- **Commit path**: every Iceberg commit does S3 writes **plus a Glue
  `UpdateTable`** round-trip (its catalog pointer lives in Glue); Paimon commits
  are pure S3 (Glue is touched only at DDL time). A consequence of the catalog
  architectures, not a config choice.
- **Snapshot expiry enforcement**: Paimon expires snapshots automatically in the
  commit path (async); Iceberg's `history.expire.*` properties only configure
  the `ExpireSnapshots` maintenance action, which nothing runs — so its snapshot
  log grows one entry per commit for the whole run. Short runs favour Iceberg
  (no expiry work), long runs punish it (planning over a growing log). Quote
  snapshot counts with the results.
- **Upload shape**: Paimon writes through its own shaded `S3FileIO`; Iceberg
  uses its own `io-impl`. Both jobs are tuned toward the same shape — fewer,
  larger multipart uploads per data file — but the configuration surface, key
  names and units differ by write path, hence the Paimon-specific `S3A_*` knobs
  above and the Iceberg job's own equivalents. See each manifest for the
  mechanism actually in force on that side.
- **Spill**: Paimon's write buffers can spill to pod ephemeral disk under
  pressure; Iceberg's writer never uses local disk.
- **Barrier alignment is asymmetrically unavoidable**: Iceberg *could* use
  unaligned checkpoints and Paimon cannot. Equalized downward (both `false`), so
  if Iceberg's alignment time ever climbs it is stuck paying it to keep the
  comparison fair.
- **StarRocks catalog type**: Paimon is read via a `filesystem` catalog
  (metadata straight from S3), Iceberg via `glue` (a Glue `GetTable` per plan
  plus different FE cache behaviour). Normal deployment for each; warmup
  amortizes most of it, but keep per-run times, not just medians.

---

## Caveats

- **Scale trial first.** 8 GiB/s ≈ 40 TMs × (4 cpu / 16 Gi) at parallelism 160,
  writing Parquet to S3 at a rate that stores **multiple TiB per hour per
  format**. Do a 10-minute run before any long one, and clean the
  `cid-warehouse-*` prefixes after. `SOURCE_ROWS_PER_SECOND` is a rate **limit**
  and not a guarantee: if a writer cannot absorb it, datagen backpressures and
  the achieved rate is what the run reports.
- **Karpenter consolidation is a real hazard here, and the mitigation is not yet
  in these manifests.** `executor-memorynodepool` consolidates with
  `WhenEmptyOrUnderutilized`, `consolidateAfter: 2m` and a 25%-of-nodes budget,
  and both of this benchmark's shapes look underutilized to it: a *starting*
  TaskManager is idle by definition, and a writer *waiting on S3* looks idle by
  exactly the metric consolidation uses. Each eviction discards the in-flight
  write buffer and forces a restart from the last checkpoint — up to a minute of
  re-done work at a 60 s interval — so the job stays live, healthy-looking and
  well below its achievable rate. *A restart-tolerant job can still be a starved
  one.* `karpenter.sh/do-not-disrupt: "true"` is currently set **only** on
  `duckdb-cid/duckdb-cid-job.yaml`, **not** on either FlinkDeployment's pod
  templates; add it there before a scoring run. The fix belongs in these
  manifests, never in the nodepool — that spec is shared with other workloads.
- **Don't run alongside the CDC benchmark jobs** — same nodepools, and the
  Kyverno single-AZ policy plus Karpenter `limits.cpu` caps mean they starve
  each other.
- **4096 B is a calibrated mean, not a per-row constant.** It is the sum of the
  34 columns' unescaped bytes (what the Parquet writer is handed), of which
  `event_json` is ~82% (~3,345 B). Uncompressed on-wire JSON is larger again
  (escaping, key names); stored Parquet is smaller. Don't mix the three when
  sizing a run.
- **Append-only means no delete artifacts, by construction.** If a run produces
  Iceberg positional-delete files or Paimon deletion-vector indexes, the premise
  is broken — `starrocks-bench.sh` captures both counts and they must read 0.
- **Five schema mismatches** between `schema_sample.sql` and the raw extract
  (absent columns, scalar-vs-array fields, `eventSubId`/`eventTime` typing,
  derived columns) are bridged by the generator and documented at the bottom of
  `sql-scripts/paimon/03-sink.sql`. Read them before comparing this table
  against the source files.
- `upgradeMode: stateless` — a restart starts clean rather than resuming old
  state; a scoring run should also start with empty tables.
