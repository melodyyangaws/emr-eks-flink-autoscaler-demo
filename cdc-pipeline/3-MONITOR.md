# Flink CDC Pipeline Monitoring Guide

## Overview

A monitoring pod that tracks both CDC pipelines, **Paimon** and **Iceberg**, and
compares ingest rows/s (the sources' rate meter), backpressure, checkpoint health and
table growth side by side. Its reports feed the query summary's ingest section
([querybench/README.md](querybench/README.md#4-summary)).

```
┌─────────────────────────────────────────────────────────────┐
│              Flink CDC Monitor Pod (emr-flink)              │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  entrypoint.py        — loop: both windows in parallel, │  │
│  │                         comparison, sleep              │  │
│  │  flink_cdc_monitor.py — Flink REST + table metadata    │  │
│  │                                                       │  │
│  │  Data sources:                                        │  │
│  │  • Flink REST API → job state, source rate, checkpts  │  │
│  │  • pyiceberg + Glue → Iceberg snapshots and summaries │  │
│  │  • boto3 + fastavro → Paimon snapshots and manifests  │  │
│  │                       read directly from S3           │  │
│  └───────────────────────────────────────────────────────┘  │
│  Output: /tmp/reports/*.json (+ S3 copy), comparison in logs│
└─────────────────────────────────────────────────────────────┘
         │                              │
         ↓                              ↓
┌──────────────────┐          ┌──────────────────┐
│ flink-cdc-paimon │          │ flink-cdc-iceberg│
│ -rest:8081       │          │ -rest:8081       │
└──────────────────┘          └──────────────────┘
```

The monitor is a port of `throughput-benchmark/monitoring/flink_cid_monitor.py`,
extended from one table to the four CDC tables.

---

## Step 1 — Prerequisites

- Both FlinkDeployments are running ([1-BUILD-DEPLOY-FLINK-APP-GUIDE.md](./1-BUILD-DEPLOY-FLINK-APP-GUIDE.md)).
  The monitor reaches them at `flink-cdc-{paimon,iceberg}-rest:8081`.
- `kubectl` on the cluster, the AWS CLI, and `envsubst` (`brew install gettext`).
- A ServiceAccount in `emr-flink` with IRSA that can read Glue and the warehouse
  buckets. The default is `starrocks-sa` (`MONITOR_SERVICE_ACCOUNT` overrides it).
  The name is historical: the query engines share it. If the ServiceAccount has no
  `eks.amazonaws.com/role-arn` annotation, the deploy script annotates it with
  `EMR_EXECUTION_ROLE_ARN`. An existing annotation is never changed. The role's
  trust policy must allow that ServiceAccount (see [IRSA](#irsa)).

## Step 2 — Deploy

```bash
cd cdc-pipeline
source .bench-env                      # BUCKET_NAME, AWS_REGION, SESSION_SUFFIX (the current round)
./monitoring/deploy-monitor.sh
```

The script:

1. resolves `AWS_REGION` (default `us-west-2`), `AWS_ACCOUNT_ID` (always from
   `aws sts get-caller-identity`), `BUCKET_NAME` (default
   `emr-on-eks-test-<account>-<region>`), `NAMESPACE` (default `emr-flink`) and
   `EMR_EXECUTION_ROLE_ARN`
2. resolves `SESSION_SUFFIX` (below; rejected at 100 characters or more) and its
   results tag (`CDC_SET_TAG`, the suffix with `-` for `_`)
3. annotates the ServiceAccount if it has no role yet
4. recreates the `flink-cdc-monitor-code` ConfigMap from `flink_cdc_monitor.py`,
   `entrypoint.py` and `requirements.txt`
5. renders `monitor-deployment.yaml` to `monitor-deployment-deployed.yaml`
   (git-ignored) with `envsubst`, and fails on any placeholder left
6. applies it, runs `kubectl rollout restart`, and waits up to 300 s for the rollout

There is **no image to build**. The pod runs stock `python:3.11-slim`, copies the code
from the ConfigMap, and `pip install`s the pinned requirements at boot (about 1–2
minutes on a cold pod). To pick up a code change, re-run the script.

Check the banner it prints. `Glue DBs:` must name the databases the Flink jobs write:

```
  Glue DBs:  flink_paimon_db<suffix>, flink_iceberg_db<suffix>
```

### Which table set it reads: `SESSION_SUFFIX`

| `SESSION_SUFFIX` in your shell | Monitored databases |
|---|---|
| unset | the value in `cdc-pipeline/.bench-env`; empty if that file does not set it |
| set, e.g. `_<suffix>` | `flink_paimon_db_<suffix>`, `flink_iceberg_db_<suffix>` |
| set but empty (`SESSION_SUFFIX=`) | the unsuffixed set: `flink_paimon_db`, `flink_iceberg_db` |

```bash
SESSION_SUFFIX= ./monitoring/deploy-monitor.sh  # the unsuffixed set
```

`ingest-deploy/build-deploy-generic.sh` resolves it the same way, so with an unchanged `.bench-env`
the monitor and the jobs agree. The suffix also selects the Paimon warehouse path the
monitor reads: `paimon-warehouse/flink_paimon_db<suffix>.db/<table>/`.

## Step 3 — Read the reports

```bash
kubectl logs -f deployment/flink-cdc-monitor -n emr-flink
```

Each cycle samples Paimon and Iceberg **in parallel over the same 120 s window**
(two threads; until 2026-11-04 the windows ran back to back, so the two columns
described adjacent intervals), logs the comparison table, writes
`paimon-cdc_<UTC>.json` and `iceberg-cdc_<UTC>.json` to `/tmp/reports` in the pod,
copies each to `REPORT_S3_URI`
(`s3://<bucket>/querybench-results/cdc-session<tag>/monitor/`, best-effort), then
sleeps `REPEAT_INTERVAL` (15 s). A cycle is the window plus the table reads, so a
30-minute hot-test window gets about 10 reports per format.

`/tmp/reports` is an `emptyDir`, lost when the pod restarts or is scaled to 0 (the
query benchmark's teardown does that); the S3 copy is what `querybench/run.sh summary`
reads. To copy the local reports out by hand:

```bash
POD=$(kubectl get pod -n emr-flink -l app=flink-cdc-monitor -o name | head -1)
kubectl cp emr-flink/${POD#pod/}:/tmp/reports ./bench-results/monitor-$(date -u +%Y%m%d)
```

## Step 4 — Stop or remove

```bash
# Pause (keeps the Deployment and ConfigMaps; resuming skips nothing but pip)
kubectl scale deployment/flink-cdc-monitor -n emr-flink --replicas=0

# Resume
kubectl scale deployment/flink-cdc-monitor -n emr-flink --replicas=1

# Remove
kubectl delete deployment/flink-cdc-monitor -n emr-flink
kubectl delete configmap flink-cdc-monitor-config flink-cdc-monitor-code -n emr-flink
```

`deploy-monitor.sh` has no stop action. The monitor is independent of the pipelines
and the load generator: `./ingest-deploy/build-deploy-generic.sh cleanup` does not stop it, and
stopping it does not affect them.

---

## How the numbers are computed

### Rows/s: ingest (source rate meter) and committed (table commits)

The headline **"Throughput (rows/s)"** is the ingest rate: the median over the window
of the CDC sources' `numRecordsOutPerSecond` (Flink's rate meter), summed over their
subtasks; if the meter has no samples, the source counter delta. Both jobs read the
same MySQL change stream, so equal values mean both kept up; a lower one means its
writers backpressure its sources. (Until 2026-11-04 the headline was committed rows/s,
with the meter as a separate "Source rate meter" row.)

**Backpressure rows** (added 2026-11-04), from each sample's `/jobs/<id>/vertices/<v>/backpressure`:
the share of the window's samples in which any vertex reported `HIGH`; the median over the
samples of the CDC sources' highest subtask `backPressuredRatio`, in ms per second (how long
a source waited on its downstream); and the median of the writers' highest subtask `busyRatio`
in ms/s (near 1,000 = one writer saturated: on Iceberg sets created until 2026-11-09, the
single writer per date partition; newer sets bucket each date 16 ways, like Paimon, spread
unevenly over the 12 writers, see the
[1-BUILD guide, Buckets](1-BUILD-DEPLOY-FLINK-APP-GUIDE.md#buckets)).
`-` = no RUNNING sample in the window. JSON: `backpressured_sample_ratio` (unchanged),
`source_backpressured_ms_per_s`, `writer_busy_max_ms_per_s`.

**Source busy and fetch lag rows** (added 2026-11-06): the median over the samples of the
sources' highest subtask `busyRatio` (`busyTimeMsPerSecond` / 1000, from the same
`/backpressure` response), in ms/s. Near 1,000 with the writers idle and the sources not
backpressured means the source subtask itself is the bottleneck. Both jobs run with
operator chaining off since 2026-11-06, so the source vertex is the CDC reader alone
(with chaining on, its busy time also included the chained operators' work).
And the median of the sources' highest `currentFetchEventTimeLag` (the Flink CDC MySQL
reader gauge: now minus the binlog timestamp of the last event fetched, in ms), read as
the operator-scoped `Source__<operator>.currentFetchEventTimeLag` from
`/vertices/<v>/subtasks/metrics?agg=max`; values < 0 (-1 = no binlog event seen yet) are
ignored. A growing lag means the binlog read falls behind. `-` = no data. JSON:
`source_busy_max_ms_per_s`, `source_fetch_lag_max_ms`.

**Committed rows/s** (not in the side-by-side table since 2026-11-05; still in the
JSON as `commit_records_per_sec` / `table_commit_records_per_sec`, which the query
summary reads) is what each format wrote, computed from each table's own commits, not from
Flink counters. Paimon's is lower for the same input: it merges re-written keys in its
write buffer before committing, while Iceberg writes every version plus a delete:

1. At the start of the window the monitor records each table's current snapshot,
   then polls Flink every 20 s for 120 s, then reads the tables again.
2. For each of the four tables it builds the list of commits as
   `(commit time, rows added)`:
   - **Iceberg**: every snapshot in the current metadata (`timestamp-ms`,
     summary `added-records`). In upsert mode `added-records` counts the data rows
     written: inserts and the new image of each update. The equality deletes and
     DVs that retire old images are counted separately.
   - **Paimon**: snapshots from the one current at the window start to the one
     current at the end, read from `snapshot/snapshot-N` on S3 (`timeMillis`,
     `deltaRecordCount`). Only `APPEND` commits count. `COMPACT` commits rewrite rows
     already counted, so they are skipped, except that the start snapshot is always
     kept as the anchor.
3. The **anchor** is the last commit at or before the window start. The table's
   rate is the rows of the commits inside the window, divided by the time from the
   anchor to the last commit inside:

   ```
   rate(table) = Σ rows(commits in (t0, t1]) / (time(last commit in window) − time(anchor))
   ```

   A table with no anchor or no commit in the window contributes 0.
4. The pipeline rate is the sum over the four tables; the per-table terms are in
   the JSON (`table_commit_records_per_sec`), which shows where one format falls
   behind (order_items carries most of the upsert load). Neither is a row of the
   comparison table any more.

Because the rate comes from what was committed, it has no REST-counter caching and no
restart artifacts: rows replayed after a restart are not committed twice. With 60 s
checkpoints, a 120 s window holds one or two commits per table.

**Fallbacks**, used only when no commit landed in the window. The label next to the
number says which one was used:

| Label | Source |
|---|---|
| `commits` | the commit history, as above |
| `rate meter` | median over the window of the sum of the sources' `numRecordsOutPerSecond` (Flink's smoothed per-second meter), RUNNING samples only |
| `counter` | delta of the sources' operator-scoped `numRecordsOut` over the longest stretch without a counter reset. Each reset is reported as a restart |

Records come from **operator-scoped** metrics: the sources' `numRecordsOut` and the
writers' `numRecordsIn`. Summing vertex-level `numRecordsOut` counts a row once per
hop, and (when chaining was on) a chained writer's vertex counters saw only committables.
Figures from the earlier monitor (before 2026-11-01, including
the `bench-results/JOB-MONITOR-2026092*.md` reports, now in git history only) summed every vertex and are **not
comparable**: 6x on the Paimon graph, 3x on Iceberg.

**Bytes**: Iceberg uses the committer's `committedDataFilesByteCount`. Paimon uses
the file sizes of the `APPEND` commits' delta manifests, because Paimon's committer
byte counter also counts compaction rewrites. Both exclude compaction output.

### Failed checkpoints: a delta per window

"Failed Checkpoints (Δ)" (`failed_checkpoints_in_window` in the JSON) is
`counts.failed` from `/jobs/<id>/checkpoints` at the last poll of the window minus the
same value at the first. "Checkpoints" is the same delta of `counts.completed`. The
raw `counts.failed` is cumulative for the job's life and never decreases, so a job
that failed checkpoints during its snapshot phase reports the same non-zero value
forever. Only the delta says something about the window.

Checkpoints *aborted at trigger time* (because not every task is running) are not
counted as FAILED, so neither `counts.failed` nor `tolerable-failed-checkpoints`
reacts to them. That failure mode is what the manifests' `failover-strategy: full`
prevents. To see whether a job is checkpointing right now, read the `history` array
by hand:

```bash
JM=$(kubectl get pods -n emr-flink -l app=flink-cdc-paimon,component=jobmanager -o name | head -1)
JID=$(kubectl exec -n emr-flink "$JM" -c flink-main-container -- \
        curl -s localhost:8081/jobs | python3 -c 'import sys,json; print(json.load(sys.stdin)["jobs"][0]["id"])')
kubectl exec -n emr-flink "$JM" -c flink-main-container -- \
  curl -s "localhost:8081/jobs/$JID/checkpoints" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); [print(c["id"], c["status"], c["trigger_timestamp"]) for c in d["history"]]'
```

Recent `COMPLETED` entries with advancing ids mean the job is healthy, whatever the
lifetime counter says.

### Deletion vectors (Iceberg v3)

Iceberg 1.12's snapshot summary has no running DV total. Each commit carries
`added-dvs` and `removed-dvs`, and `total-delete-files` / `added-delete-files` count
DVs together with equality-delete files. So the monitor keeps the per-commit DV counts
of every retained snapshot, at the start and at the end of the window, and sums the
commits present at the end but not at the start. Snapshot expiry in between does not
distort it.

- The comparison shows `↳ Deletion Vectors (added / removed)` as `+<added> / -<removed>`
  when either side's summaries carry DV keys, and `n/a` for a side without them.
- The JSON adds `total_new_dvs` / `total_removed_dvs`, and per table `added_dvs` (the
  current snapshot), `dvs_added_in_window` and `dvs_removed_in_window`.
- A v2 table has no such keys, and its report is unchanged.
- Paimon's deletion vectors live in per-bucket DV index files listed by each
  snapshot's index manifest (written by compaction, not the writer). The monitor
  counts the DV index files new / gone between the window's start and end
  snapshots, and the live data files that carry a DV; the row then reads
  `+<added> / -<removed> index files (<n> data files with DVs)`. JSON per table:
  `dv_index_files_added_in_window`, `dv_index_files_removed_in_window`,
  `dv_index_files_live`, `dv_data_files_live`. (Added 2026-11-08; before that
  Paimon's side showed `n/a`.)
- "New Delete Files" counts DVs too, so on v3 it is equality-delete files plus DV
  files.

### Table stats

| Format | How stats are read |
|---|---|
| **Iceberg** | `pyiceberg` (0.12.0) loads the table through Glue and reads the current snapshot summary: `total-records`, `total-data-files`, `total-files-size`, `total-delete-files`, `total-position-deletes`, `total-equality-deletes`. Snapshot count = snapshots in the metadata |
| **Paimon** | Its own metadata on S3: `snapshot/LATEST` → `snapshot-N` (`totalRecordCount`), live files = Σ(`_NUM_ADDED_FILES` − `_NUM_DELETED_FILES`) over the base and delta manifest lists, snapshot count = retained `snapshot-*` files. New snapshots = snapshot-id delta (Paimon ids are sequential; Iceberg's are random, so Iceberg uses the count delta) |

All table deltas are clamped at 0, because snapshot expiry between the two reads can
make a raw delta negative.

---

## Sample report

The table `compare_reports()` logs each cycle. The rows are fixed; the values here
are placeholders, not a measurement:

```
================================================================================
  CDC Pipeline Comparison: Paimon CDC vs Iceberg CDC
  Time: <start> → <end>
================================================================================
| Metric                                 | Paimon CDC         | Iceberg CDC        | Delta |
| Job Name                               | …                  | …                  |       |
| Status                                 | RUNNING            | RUNNING            |       |
| Parallelism                            | 12                 | 12                 |       |
| Duration (s)                           | 120                | 120                |       |
| Throughput (rows/s)                    | <n> (source rate meter) | <n> (source rate meter) | ±x% |
| Backpressure (sources, share of samples HIGH) | <x%>        | <x%>               |       |
|   ↳ sources backpressured (ms/s, max subtask) | <n>          | <n>                |       |
|   ↳ busiest writer (busy ms/s, max subtask)   | <n>          | <n>                |       |
|   ↳ busiest source (busy ms/s, max subtask)   | <n>          | <n>                |       |
|   ↳ source fetch lag (ms, max subtask)        | <n>          | <n>                |       |
| Restarts in window                     | 0                  | 0                  |       |
| Records Processed (counter)            | <n>                | <n>                |       |
| Committed bytes/s (Parquet)            | <n> MB             | <n> MB             |       |
| Committed Bytes (Parquet)              | <n> MB             | <n> MB             |       |
| Checkpoints                            | 2                  | 2                  |       |
| Failed Checkpoints (Δ)                 | 0                  | 0                  |       |
| Avg Checkpoint (ms)                    | <n>                | <n>                |       |
| New Table Snapshots                    | <n>                | <n>                |       |
| New Data Files                         | <n>                | <n>                |       |
| New Delete Files                       | 0                  | <n>                |       |
|   ↳ Position Deletes (Δ)               | 0                  | <n>                |       |
|   ↳ Equality Deletes (Δ)               | 0                  | <n>                |       |
|   ↳ Deletion Vectors (added / removed) | +<n> / -<n> index files (<n> data files with DVs) | +<n> / -<n> |   |
| New Records in Tables                  | <n>                | <n>                |       |
| New Bytes in Tables                    | <n> MB             | <n> MB             |       |
| Errors                                 | None               | None               |       |   (only when an error occurred)
```

---

## Key configuration (ConfigMap `flink-cdc-monitor-config`)

| Env var | Value | Description |
|---|---|---|
| `PAIMON_FLINK_URL` | `http://flink-cdc-paimon-rest:8081` | Paimon Flink REST endpoint |
| `ICEBERG_FLINK_URL` | `http://flink-cdc-iceberg-rest:8081` | Iceberg Flink REST endpoint |
| `AWS_REGION` | `${AWS_REGION}` | |
| `PAIMON_GLUE_DB` | `flink_paimon_db${SESSION_SUFFIX}` | also the Paimon path `<warehouse>/<db>.db/` |
| `ICEBERG_GLUE_DB` | `flink_iceberg_db${SESSION_SUFFIX}` | |
| `PAIMON_WAREHOUSE` | `s3://${BUCKET_NAME}/paimon-warehouse` | required; the pod exits if unset |
| `ICEBERG_WAREHOUSE` | `s3://${BUCKET_NAME}/iceberg-warehouse/` | required |
| `CDC_TABLES` | `customers,products,orders,order_items` | |
| `MONITOR_DURATION` | `120` | window per pipeline, s; must span at least one 60 s checkpoint |
| `POLL_INTERVAL` | `20` | Flink REST poll interval inside the window, s |
| `REPEAT_INTERVAL` | `15` | sleep between cycles, s (the code's default is 300) |
| `RUN_MODE` | `continuous` | `once` runs one cycle and exits |
| `REPORT_S3_URI` | `s3://${BUCKET_NAME}/querybench-results/cdc-session${CDC_SET_TAG}/monitor/` | S3 copy of every report (best-effort); empty = local only |

`REPORT_OUTPUT_DIR` (default `/tmp/reports`) is read by `entrypoint.py` but not set in
the ConfigMap. To change any value, edit `monitoring/monitor-deployment.yaml` and
re-run `./monitoring/deploy-monitor.sh`.

### Files

```
monitoring/
├── deploy-monitor.sh                ← Code ConfigMap + envsubst + apply + rollout restart
├── flink_cdc_monitor.py             ← FlinkCDCMonitor: Flink REST, Iceberg and Paimon metadata readers
├── entrypoint.py                    ← Loop: both windows in parallel → comparison → S3 copy → sleep
├── monitor-deployment.yaml          ← Template: ConfigMap + Deployment (python:3.11-slim)
├── monitor-deployment-deployed.yaml ← Rendered output (git-ignored)
├── capture-flink-metrics.sh         ← One bounded REST-only window, from the laptop
└── requirements.txt                 ← boto3 1.40.0, pyiceberg[glue,s3fs] 0.12.0, pyarrow 21.0.0,
                                       requests, tabulate, fastavro, zstandard (all pinned)
```

`pyiceberg` is 0.12.0 rather than 0.9.1 for v3 tables. The monitor reads only metadata
and snapshot summaries, which both versions load.

---

## IRSA

The monitor needs read access to Glue and to both warehouse prefixes. The role in the
ServiceAccount's annotation must trust that ServiceAccount. Check what is there first:

```bash
SA=${MONITOR_SERVICE_ACCOUNT:-starrocks-sa}
kubectl get sa $SA -n emr-flink -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}'; echo
aws iam get-role --role-name emr-on-eks-test-execution-role \
  --query 'Role.AssumeRolePolicyDocument.Statement[].Condition' --output json
```

If the trust policy does not list `system:serviceaccount:emr-flink:<SA>`, **add** a
statement for it. Do not replace the policy with
`aws iam update-assume-role-policy` and a single-statement document: that would drop
the trust the Flink jobs, the query engines and EMR on EKS already rely on. Then
restart the monitor:

```bash
kubectl rollout restart deployment/flink-cdc-monitor -n emr-flink
```

---

## Monitoring Flink directly (without the monitor pod)

`monitoring/capture-flink-metrics.sh` takes two REST samples `WINDOW` seconds apart
(default 120) through `kubectl exec` into each JobManager, and writes
`bench-results/flink-metrics-<UTC>.csv`. It reports checkpoint failures as the delta
over the window and the lifetime counter separately. Quote the delta.

```bash
./monitoring/capture-flink-metrics.sh
WINDOW=300 ./monitoring/capture-flink-metrics.sh

kubectl get flinkdeployment -n emr-flink
kubectl logs -f -l app=flink-cdc-iceberg,component=jobmanager -n emr-flink -c flink-main-container
kubectl port-forward svc/flink-cdc-paimon-rest 8081:8081 -n emr-flink    # http://localhost:8081
```

---

**Last Updated**: 2026-11-08
