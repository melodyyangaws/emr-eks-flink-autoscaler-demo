#!/bin/bash
###############################################################################
# Flink Throughput Benchmark — Paimon vs Iceberg — deploy
#
# Compares the Flink WRITE-PATH performance of the two table formats on the
# SAME image the CDC pipeline already uses (emr-flink-cdc-paimon, EMR 7.13
# base — it bundles Paimon 1.3.2 AND Iceberg 1.10.0-amzn-1):
#   paimon  : append-only table, PARTITIONED BY (dt), bucketed on uuid
#   iceberg : format v2, append-only, PARTITIONED BY (dt), distribution-mode=none
# Both fed by a built-in datagen source at 4 KiB mean row. The rate is
# SOURCE_ROWS_PER_SECOND, default 2,000,000 rows/s ≈ 7.63 GiB/s; .bench-env
# raises it to 2,097,152 = exactly 8 GiB/s, which is the benchmark's stated
# target. It is a rate LIMIT, not a guarantee — the 2026-10-05 runs delivered
# ~555 K rows/s against it. Do not quote a GiB/s figure here that is not
# derived from SOURCE_ROWS_PER_SECOND; four such literals had drifted to three
# different numbers before 2026-10-06.
#
# APPEND-ONLY. Both sinks are pure INSERT: no primary key, no upsert, no
# equality/merge-on-read deletes, no changelog producer, and the pipelines are
# INSERT INTO ... SELECT over a datagen view with no stateful operator, so the
# planner cannot emit a retraction. The formats are therefore compared on
# encode + upload + commit + small-file compaction, NOT on merge-on-read cost.
# Results are NOT comparable with the earlier upsert series in bench-results/.
#
# There is deliberately NO image build here. The benchmark reuses the CDC
# image byte-for-byte — build it (once) with:
#   cd ../cdc-pipeline && ./build-deploy-generic.sh build
#
# USAGE (source .bench-env first; it sets the account, bucket, role and knobs):
#   source .bench-env
#   ./build-deploy-cid.sh upload                # Glue DBs + executor/SQL to S3, verified; no kubectl
#   ./build-deploy-cid.sh deploy paimon         # one writer — the only way to get a quotable rows/s
#   ./build-deploy-cid.sh deploy iceberg
#   ./build-deploy-cid.sh deploy both           # side by side: measures contention, not a write path
#   ./build-deploy-cid.sh render                # write flink-cid-<fmt>-deployed.yaml, don't apply
#   ./build-deploy-cid.sh monitor               # FlinkDeployments + pods
#   ./build-deploy-cid.sh cleanup [paimon|iceberg|both]   # delete the CR(s); asks "yes"; keeps data
# deploy always deletes and re-CREATEs the CR, since EMR stages jarURI on CREATE only.
# Suspend without deleting:
#   kubectl patch flinkdeployment flink-cid-<fmt> -n emr-flink --type=merge \
#     -p '{"spec":{"job":{"state":"suspended"}}}'
# Knobs (env, defaults below): SOURCE_MODE=datagen|s3json (+S3JSON_SOURCE_PATH),
#   SOURCE_ROWS_PER_SECOND, JOB_PARALLELISM, PAIMON_BUCKETS (-1 = unaware),
#   ICEBERG_COMPAT, WRITE_BUFFER, CID_CUSTOMER_COUNT, CID_HOST_COUNT,
#   CID_DAY_SPREAD, HOT_KEY_FRACTION, JM_NODEPOOL, TM_NODEPOOL.
# Changing JOB_PARALLELISM/PAIMON_BUCKETS or ICEBERG_COMPAT requires dropping the
# tables first (README "Teardown"): the bucket count is fixed at CREATE TABLE.
###############################################################################

set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()   { echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" >&2; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
warn()  { echo -e "${YELLOW}[WARNING]${NC} $1"; }
info()  { echo -e "${BLUE}[INFO]${NC} $1"; }

export AWS_REGION=${AWS_REGION:-us-west-2}
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-$AWS_REGION}

# ---------------------------------------------------------------------------
# Benchmark knobs (all overridable from the environment)
# ---------------------------------------------------------------------------
export EMR_VERSION=${EMR_VERSION:-7.13.0}
export IMAGE_TAG=${IMAGE_TAG:-$EMR_VERSION}
export SOURCE_ROWS_PER_SECOND=${SOURCE_ROWS_PER_SECOND:-2000000}
# This rate needs real parallelism. At the default 2,000,000 rows/s (≈7.63
# GiB/s) 128 subtasks is ~61 MiB/s per subtask on the write path; at
# .bench-env's 160 subtasks and 8 GiB/s it is ~51 MiB/s. Scale up only if the
# source backpressures on BOTH formats — that means the harness, not the format,
# is the wall.
export JOB_PARALLELISM=${JOB_PARALLELISM:-128}

# Paimon 'bucket': -1 (unaware-bucket) by default, matching the Iceberg side,
# which is PARTITIONED BY dt with 'write.distribution-mode' = 'none' and no
# bucketing. Fixed buckets force a keyBy(uuid) shuffle in front of the writer:
# every row is serialized across the network (~5.7 KB/row measured) and the
# writer runs as its own vertex on the same cores as the source chain. That
# was Paimon at ~820K rows/s vs Iceberg's ~1.4M on 2026-10-09.
#
# PAIMON_BUCKETS=N (>= 1) restores fixed buckets on uuid. Use N = JOB_PARALLELISM:
# each bucket has exactly ONE writer subtask, so fewer buckets idle writers
# (JOB_PARALLELISM / 2 from 2026-10-06 to 2026-10-09 left 80 of 160 with no
# rows). Paimon rejects 'bucket-key' on a bucket = -1 table, so the key line is
# rendered only in fixed mode.
export PAIMON_BUCKETS=${PAIMON_BUCKETS:--1}
if (( PAIMON_BUCKETS >= 1 )); then
    export PAIMON_BUCKET_KEY_OPT="'bucket-key' = 'uuid',"
else
    export PAIMON_BUCKETS=-1 PAIMON_BUCKET_KEY_OPT=""
fi

# Endpoint-skew knob.
# HOT_KEY_FRACTION: percentage of rows routed to the hot slice [0,99].
#   0  = uniform (default; what the earlier series used)
#   80 = 80% of rows attributed to a hot 5% slice of the endpoints (80/20 shape)
# It substitutes into 01-datagen-source.sql via pod env on the JobManager.
#
# WHAT IT MEANS SINCE THE SCHEMA CHANGE. The sink tables are APPEND-ONLY now
# (schema_sample.sql has no primary key), so there is no key for a row to be
# "hot" on and skew no longer drives LSM key-merge or delete-file work — which
# was the entire point of this knob in the upsert era. The selector now routes
# the ENDPOINT identity instead: HOT_KEY_FRACTION% of events are attributed to a
# hot 5% slice of CID_HOST_COUNT (CID_HOST_COUNT/20). That still changes real
# work — a few noisy hosts make the endpoint columns dictionary-encode better —
# but it is a much weaker lever than it was. Do not read an old 80/20 result as
# comparable with a new one.
#
# It does NOT affect Paimon's bucket distribution. 'bucket-key' = 'uuid', and
# uuid comes from r_uuid, a full-range draw this knob never touches.
#
# It still changes the BYTES the writers encode, so changing it invalidates
# comparison with runs at a different value; .bench-env sets 80.
export HOT_KEY_FRACTION=${HOT_KEY_FRACTION:-0}
if [[ ! "$HOT_KEY_FRACTION" =~ ^[0-9]+$ ]] || (( HOT_KEY_FRACTION > 99 )); then
    error "HOT_KEY_FRACTION must be an integer 0..99 (got '$HOT_KEY_FRACTION').
       It is compared against a datagen 'selector' drawn in [0,99]."
fi

# ---------------------------------------------------------------------------
# Source selection (added 2026-10-08)
# ---------------------------------------------------------------------------
# SOURCE_MODE picks which file upload_scripts() publishes as 00-source.sql.
# Both define the same datagen_source view (same 34 columns), so nothing else
# in the pipeline changes:
#   datagen (default) — sql-scripts/common/01-datagen-source.sql, the in-memory
#                       rate-limited generator. The original behaviour; set
#                       this (or unset SOURCE_MODE) to switch back.
#   s3json            — sql-scripts/common/01-s3json-source.sql, a CONTINUOUS
#                       filesystem source over the gzipped NDJSON written by
#                       jsongz-gen/. SOURCE_ROWS_PER_SECOND and the CID_*/
#                       HOT_KEY_FRACTION knobs do nothing in this mode — the
#                       rate and data shape are whatever the generator lands.
export SOURCE_MODE=${SOURCE_MODE:-datagen}
case "$SOURCE_MODE" in
    datagen|s3json) ;;
    *) error "SOURCE_MODE must be 'datagen' or 's3json' (got '$SOURCE_MODE')." ;;
esac
# Must be the generator's OUTPUT_URI (jsongz-gen/run-jsongz-gen.sh), as s3a://
# — the image's Hadoop S3A is what the filesystem connector resolves. Every
# file already under the prefix is read at job start, so use a fresh prefix
# for a run that should see only newly added files.
export S3JSON_SOURCE_PATH=${S3JSON_SOURCE_PATH:-s3a://${BUCKET_NAME}/cid-landing-jsongz/}
# How often the source re-lists the path for new files.
export S3JSON_MONITOR_INTERVAL=${S3JSON_MONITOR_INTERVAL:-10s}

# ---------------------------------------------------------------------------
# CID source-mockup knobs
# ---------------------------------------------------------------------------
# The datagen source reproduces the shape of the real security-telemetry extract
# (cid_9011075d_sample_masked). These three control its fan-out; every other
# distribution in sql-scripts/common/01-datagen-source.sql is a measured
# frequency from that sample and is deliberately NOT parameterised.
#
# CID_CUSTOMER_COUNT: distinct customerId values (also partition_cust). 16 keeps
#   the column dictionary-encoded, as it is in the source's per-customer files.
# CID_HOST_COUNT: distinct endpoints. 1024 ≈ the sample's measured 1,047 — this
#   is the cardinality that drives endpointGuid/endpointHostName/vpcId encoding.
# CID_DAY_SPREAD: how many distinct dt values the generator emits, i.e. how many
#   PARTITIONS the sink tables get. 1 matches the source extract (a single-hour
#   dump) and is the default.
#
#   READ THIS BEFORE RAISING CID_DAY_SPREAD. dt is the partition column on both
#   sinks. Raising it multiplies open files per writer by the day count on both
#   sides — 64 writers x N days of concurrently-open Parquet files — which turns
#   the benchmark into a small-file test. It does NOT make the Iceberg side
#   distribute better either: write.distribution-mode is 'none' precisely so that
#   one dt value cannot serialize all writes into one subtask, and that choice is
#   independent of the day count. Raise it only to exercise multi-partition
#   commit cost, and say so when reporting the result.
export CID_CUSTOMER_COUNT=${CID_CUSTOMER_COUNT:-16}
export CID_HOST_COUNT=${CID_HOST_COUNT:-1024}
export CID_DAY_SPREAD=${CID_DAY_SPREAD:-1}

# ---------------------------------------------------------------------------
# Paimon write path — FIXED at Paimon's defaults. Change it only if testing Paimon at huge scale, not for a fair comparison with Iceberg.
# ---------------------------------------------------------------------------
# write-only=false  : writers compact inline. One job, self-contained.
# write-buffer-spillable=true : BinaryExternalSortBuffer.write() spills a sorted
#                     run and retries rather than rejecting the record, so the
#                     barrier flush external-merges the checkpoint interval.
#
# THE WRITE_ONLY AND SPILLABLE ENV KNOBS WERE REMOVED on 2026-10-05. Both
# existed only to select between this configuration and an exploratory one
# (WRITE_ONLY=true installed a NoopCompactManager; SPILLABLE=false flushed an
# L0 file inside the record path). Those arms are no longer part of the
# project, so the knobs had exactly one reachable value each and became a way
# to silently reinstate a retired arm — .bench-env was in fact overriding both
# to the exploratory values.
# Exception: PAIMON_WRITE_ONLY=true may be set per run (2026-10-09) to measure
# Paimon with no inline compaction or expiry, like Iceberg's writer. Small files
# and expired snapshots then accumulate until something compacts the table.
export PAIMON_WRITE_ONLY=${PAIMON_WRITE_ONLY:-false}
if [[ "$PAIMON_WRITE_ONLY" != true && "$PAIMON_WRITE_ONLY" != false ]]; then
    echo "PAIMON_WRITE_ONLY must be true or false, got '$PAIMON_WRITE_ONLY'" >&2; exit 1
fi
export PAIMON_WRITE_BUFFER_SPILLABLE=true

# Paimon write buffer size. The default 128 MiB is the same as Iceberg's
export WRITE_BUFFER=${WRITE_BUFFER:-128mb}
if [[ ! "$WRITE_BUFFER" =~ ^[0-9]+(kb|mb|gb)$ ]]; then
    error "WRITE_BUFFER must be a Paimon MemorySize like '256mb' (got '$WRITE_BUFFER')"
fi
PAIMON_WRITE_BUFFER_SIZE="$WRITE_BUFFER"
export PAIMON_WRITE_BUFFER_SIZE

# ---------------------------------------------------------------------------
# S3A upload tuning for Paimon — sized for parallelism 160 on 2026-10-05
# ---------------------------------------------------------------------------
#
# EMR ships a tuned core-site.xml (fs.s3a.multipart.size=128M, fs.s3a.buffer.dir=/mnt/s3,/mnt1/s3)
# and the hadoop-aws-3.4.2-amzn-0 connector. The CONNECTOR is what we get to
# use; the core-site.xml is NOT, because Paimon's shaded S3FileIO builds its
# S3AFileSystem from a bare `new Configuration()` and /glue/confs/hadoop/conf is
# not on the JVM classpath (full bytecode trace in 02-catalog-setup.sql). So
# every value here has to be passed as a catalog option, and EMR's own defaults
# are irrelevant — Hadoop's core-default.xml applies unless we override it.
# Note EMR's buffer.dir would be actively WRONG here anyway: /mnt/s3 and
# /mnt1/s3 are instance-store paths that do not exist in these pods.
#
# SIZING AT PARALLELISM 160 (was 64 when these defaults were set). The scale
# that matters is per-NODE, not per-job: 160 subtasks / 4 slots = 40
# TaskManagers at 4 vcpu + 16Gi, which pack 4-per-m5.4xlarge, so ~10 nodes with
# 16 concurrent writer subtasks each sharing ONE ENI and ONE 50 GiB gp3 root
# volume (ec2nodeclass memoryec2 declares no instance store).
#
#   multipart.size 32M -> 16M
#     This is the block cut point, and the ONLY option that changes the write
#     path (S3ABlockOutputStream cuts on blockSize alone; `javap | grep -ci
#     threshold` returns 0). It must stay BELOW the data file size or a file
#     that never fills one block is uploaded as a single PUT at close() — the
#     defect this knob was introduced to fix, measured at 1,664/1,664
#     non-multipart ETags under the 64M default. At 160 parallelism each
#     subtask's share of the 8 GiB/s target is ~51 MiB/s, so files arrive
#     SMALLER and more often than at 64; 16M keeps a ~60 MB file at 4 parts
#     instead of 2, which both restores pipelining on the small files and gives
#     the retry path a smaller unit. Floor is 5 MiB — S3AUtils.
#     getMultipartSizeProperty() clamps anything smaller UP and only warns, so a
#     too-small value fails SILENTLY.
#
#   active.blocks 8 -> 6, and why this is the knob that must come DOWN
#     This is the one place where raising parallelism makes a bigger value
#     actively harmful. The buffer budget is per-STREAM, and the real multiplier
#     is subtasks-per-node: 16 x blocks x part-size of disk buffer against a
#     50 GiB root volume that ALSO holds Paimon's write-buffer spill files
#     (spillable=true, so a whole checkpoint interval can land there) and the
#     container logs. At the old 8 x 32M x 16 that is 4 GiB/node of upload
#     buffer alone; 6 x 16M x 16 is 1.5 GiB, which leaves the spill path room on
#     a volume with no instance store to fall back to. 6 still exceeds
#     hadoop-aws's default of 4, so uploads stay pipelined — the point is to
#     keep enough queue depth to cover S3 latency, not to maximise it.
#
#   multipart.threshold 32M -> 16M
#     Write path does NOT read this (see above); kept aligned with the part size
#     purely so multipart COPY, which does consult it, behaves consistently.
#
# These are DEFAULTS, still overridable — unlike the write-path options above,
# they have a legitimate range and both formats' upload shape is a fair thing to
# tune. Report the values with any result: they change bytes-in-flight per node.
export S3A_MULTIPART_THRESHOLD=${S3A_MULTIPART_THRESHOLD:-16M}
export S3A_MULTIPART_SIZE=${S3A_MULTIPART_SIZE:-16M}
# S3A's hard floor is 5 MiB (Constants.MULTIPART_MIN_SIZE); below it the client
# rejects the part size at init rather than silently rounding.
for v in S3A_MULTIPART_THRESHOLD S3A_MULTIPART_SIZE; do
    if [[ ! "${!v}" =~ ^[0-9]+[KMG]$ ]]; then
        error "$v must be a Hadoop size like '16M' (got '${!v}')"
    fi
done
# Blocks queued per output stream before write() blocks (hadoop-aws default 4).
# Each costs up to S3A_MULTIPART_SIZE of buffer; 'disk' buffering (set in the
# catalog options) keeps that off the task heap and onto the root volume, which
# is what bounds it — see the per-node arithmetic above.
export S3A_ACTIVE_BLOCKS=${S3A_ACTIVE_BLOCKS:-6}
if [[ ! "$S3A_ACTIVE_BLOCKS" =~ ^[0-9]+$ ]]; then
    error "S3A_ACTIVE_BLOCKS must be an integer (got '$S3A_ACTIVE_BLOCKS')"
fi

# ---------------------------------------------------------------------------
# The same upload shape, expressed for Iceberg's S3FileIO
# ---------------------------------------------------------------------------
# DERIVED from the S3A knobs above, NOT independent — the two sides must agree
# or the "fair comparison" compares upload configurations instead of formats.
# They are separate variables only because the two clients want different
# units and spell the concept differently:
#
#   s3.multipart.part-size-bytes  wants BYTES, where S3A wants '16M'. Same
#       block cut point, so it is S3A_MULTIPART_SIZE x 1024^2.
#   s3.multipart.num-threads      is a per-JVM UPLOAD POOL, where S3A's
#       fs.s3a.fast.upload.active.blocks is a per-STREAM queue depth. One TM
#       runs 4 slots, so the pool that matches N queued blocks per stream is
#       N x 4.
#
# The division/multiplication has to happen here: flink-cid-executor.py's
# ${VAR} substitution is a plain env lookup with no arithmetic. Overriding
# either directly is allowed and breaks the derivation on purpose.
#
# This block was MISSING until 2026-10-06 while flink-cid-iceberg.yaml already
# referenced both names — `render` aborted on "Unsubstituted placeholders
# remain", so the Iceberg side could not be deployed at all.
if [[ "$S3A_MULTIPART_SIZE" =~ ^([0-9]+)M$ ]]; then
    _ib_part=$(( ${BASH_REMATCH[1]} * 1024 * 1024 ))
elif [[ "$S3A_MULTIPART_SIZE" =~ ^([0-9]+)K$ ]]; then
    _ib_part=$(( ${BASH_REMATCH[1]} * 1024 ))
elif [[ "$S3A_MULTIPART_SIZE" =~ ^([0-9]+)G$ ]]; then
    _ib_part=$(( ${BASH_REMATCH[1]} * 1024 * 1024 * 1024 ))
fi
export ICEBERG_S3_MULTIPART_SIZE_BYTES=${ICEBERG_S3_MULTIPART_SIZE_BYTES:-$_ib_part}
export ICEBERG_S3_MULTIPART_THREADS=${ICEBERG_S3_MULTIPART_THREADS:-$(( S3A_ACTIVE_BLOCKS * 4 ))}
unset _ib_part
# Iceberg's S3FileIO enforces the same 5 MiB floor as S3A, but by REJECTING the
# value at init rather than clamping it — fail here with the arithmetic visible
# instead of in a TaskManager stack trace.
if (( ICEBERG_S3_MULTIPART_SIZE_BYTES < 5 * 1024 * 1024 )); then
    error "ICEBERG_S3_MULTIPART_SIZE_BYTES must be >= 5 MiB (got ${ICEBERG_S3_MULTIPART_SIZE_BYTES}
       from S3A_MULTIPART_SIZE=${S3A_MULTIPART_SIZE})"
fi
# Guard the per-node disk budget rather than only the per-stream one: the
# failure mode is ephemeral-storage eviction of a TaskManager mid-run, which
# reads as a job failure rather than as a misconfiguration.
if [[ "$S3A_MULTIPART_SIZE" =~ ^([0-9]+)M$ ]]; then
    _ps=${BASH_REMATCH[1]}
    _slots=4            # taskmanager.numberOfTaskSlots in flink-cid-paimon.yaml
    _tm_per_node=4      # 4 vcpu / 16Gi TMs per m5.4xlarge
    _per_node_mib=$(( _ps * S3A_ACTIVE_BLOCKS * _slots * _tm_per_node ))
    if (( _per_node_mib > 8192 )); then
        warn "S3A upload buffer is ~$(( _per_node_mib / 1024 )) GiB/node
       ($S3A_MULTIPART_SIZE x $S3A_ACTIVE_BLOCKS blocks x $_slots slots x $_tm_per_node TMs),
       on a 50 GiB root volume shared with Paimon's write-buffer spill. Lower
       S3A_ACTIVE_BLOCKS or S3A_MULTIPART_SIZE unless the TM disk was resized."
    fi
    unset _ps _slots _tm_per_node _per_node_mib
fi

# Paimon Iceberg-compatibility toggle. 'false' (default) benchmarks Paimon's
# bare write path; 'true' adds the per-commit Iceberg V2 metadata export to
# Glue, i.e. Paimon exactly as the CDC pipeline runs it (and the table becomes
# queryable from Athena/StarRocks).
export ICEBERG_COMPAT=${ICEBERG_COMPAT:-false}
case "$ICEBERG_COMPAT" in
    true)  PAIMON_ICEBERG_STORAGE="hive-catalog" ;;
    false) PAIMON_ICEBERG_STORAGE="disabled" ;;
    *)     error "ICEBERG_COMPAT must be 'true' or 'false' (got '$ICEBERG_COMPAT')" ;;
esac
export PAIMON_ICEBERG_STORAGE

export JM_NODEPOOL=${JM_NODEPOOL:-driver-nodepool}
export TM_NODEPOOL=${TM_NODEPOOL:-executor-memorynodepool}
NAMESPACE=${NAMESPACE:-emr-flink}

# SQL-driven benchmark jobs (each has a sql-scripts/<fmt>/ directory).
FORMATS="paimon iceberg"
# All deployable targets. The two benchmark writers are now the only ones: the
# 'paimon-compact' and 'jsongen' targets were removed on 2026-10-03 along with
# burst mode, so every deployable target is also a FORMAT.
DEPLOY_TARGETS="paimon iceberg"

check_env_vars() {
    local missing=()
    for var in AWS_REGION AWS_ACCOUNT_ID BUCKET_NAME EMR_EXECUTION_ROLE_ARN; do
        [[ -z "${!var}" ]] && missing+=("$var")
    done
    [[ ${#missing[@]} -gt 0 ]] && error "Missing required environment variables: ${missing[*]}"
    log "✓ Required environment variables set"
}

# The benchmark rides on the CDC image; fail early with a pointer at the CDC
# build if the expected tag is not in ECR, instead of letting the pods
# ImagePullBackOff twenty minutes from now.
check_image_exists() {
    if aws ecr describe-images --repository-name emr-flink-cdc-paimon \
        --image-ids imageTag="$IMAGE_TAG" --region "$AWS_REGION" &>/dev/null; then
        log "✓ Image emr-flink-cdc-paimon:${IMAGE_TAG} found in ECR"
    else
        error "emr-flink-cdc-paimon:${IMAGE_TAG} not found in ECR (region ${AWS_REGION}).
       Build it with: cd ../cdc-pipeline && ./build-deploy-generic.sh build"
    fi
}

display_config() {
    log "Configuration:"
    echo "  AWS Region:        $AWS_REGION"
    echo "  AWS Account ID:    $AWS_ACCOUNT_ID"
    echo "  S3 Bucket:         $BUCKET_NAME"
    echo "  Namespace:         $NAMESPACE"
    echo "  EMR Version:       $EMR_VERSION"
    echo "  Image:             emr-flink-cdc-paimon:${IMAGE_TAG} (shared with cdc-pipeline)"
    if [[ "$SOURCE_MODE" == "s3json" ]]; then
    echo "  Source:            SOURCE_MODE=s3json — continuous gzip NDJSON from"
    echo "                     $S3JSON_SOURCE_PATH (re-listed every $S3JSON_MONITOR_INTERVAL)"
    echo "                     ^ rate is set by the generator, not SOURCE_ROWS_PER_SECOND"
    else
    echo "  Source:            SOURCE_MODE=datagen — streaming datagen (rate-limited), CID telemetry mockup"
    fi
    echo "  Source rate:       $SOURCE_ROWS_PER_SECOND rows/s x 4096 B mean ≈ $(( SOURCE_ROWS_PER_SECOND * 4096 / 1024 / 1024 / 1024 )) GiB/s (mean; row size varies)"
    echo "                     ^ calibrated to the 4 KB/row requirement (modelled"
    echo "                       mean 4099 B); sample's own mean is 3817 B."
    echo "  Job parallelism:   $JOB_PARALLELISM"
    echo "  Paimon buckets:    $PAIMON_BUCKETS (-1 = unaware-bucket, no shuffle; fixed at CREATE TABLE)"
    echo "  Endpoint skew:     HOT_KEY_FRACTION=$HOT_KEY_FRACTION (0=uniform; % of rows on a hot 5% of hosts)"
    echo "  CID fan-out:       customers=$CID_CUSTOMER_COUNT hosts=$CID_HOST_COUNT days=$CID_DAY_SPREAD"
    if [[ "$CID_DAY_SPREAD" -gt 1 ]]; then
        echo "                     ^ days>1 means $CID_DAY_SPREAD dt PARTITIONS on both sinks,"
        echo "                       so $(( PAIMON_BUCKETS >= 1 ? PAIMON_BUCKETS : JOB_PARALLELISM ))x${CID_DAY_SPREAD} (paimon, by $( (( PAIMON_BUCKETS >= 1 )) && echo bucket || echo subtask)) /"
        echo "                       ${JOB_PARALLELISM}x${CID_DAY_SPREAD} (iceberg, by subtask) concurrently-open files."
        echo "                       Report this with any result — it is not comparable to days=1."
    fi
    # The tables are append-only now; make sure nobody reads a new number as
    # comparable with the upsert-era series.
    echo "  Table shape:       APPEND-ONLY, PARTITIONED BY (dt), 34 cols (schema_sample.sql)"
    echo "                     ^ no primary key. NOT comparable with pre-CID upsert runs."
    echo "  Iceberg compat:    $ICEBERG_COMPAT (paimon metadata.iceberg.storage = $PAIMON_ICEBERG_STORAGE)"
    echo "  Paimon write path: write-only=$PAIMON_WRITE_ONLY spillable=$PAIMON_WRITE_BUFFER_SPILLABLE (spillable fixed)"
    echo "                     ^ inline compaction on; the write buffer spills and"
    echo "                       external-merges at the checkpoint barrier."
    echo "  Paimon write buf:  $WRITE_BUFFER (write-buffer-size INSERT hint; heap"
    echo "                     4 slots x ($WRITE_BUFFER + 256mb local-merge) of ~11.5g)"
    echo "                     ^ with spillable=true this only moves bytes between"
    echo "                       RAM and spill files; barrier bytes are set by"
    echo "                       arrival rate x checkpoint interval instead."
    echo "  Paimon S3A upload: multipart size=$S3A_MULTIPART_SIZE (the block cut point)"
    echo "                     threshold=$S3A_MULTIPART_THRESHOLD (copy path only), blocks=$S3A_ACTIVE_BLOCKS,"
    echo "                     buffer=disk — catalog options, the only channel"
    echo "                     into Paimon's shaded S3FileIO"
    echo "  Iceberg S3 upload: part-size=$ICEBERG_S3_MULTIPART_SIZE_BYTES B threads=$ICEBERG_S3_MULTIPART_THREADS"
    echo "                     ^ DERIVED from the S3A values above (x1024², and"
    echo "                       blocks x 4 slots) so both clients get one shape."
}

# head-object the key the pods will fetch, and report the size it will get.
# Called after every upload in upload_scripts, because EVERY artifact this
# project runs is resolved from S3 at pod boot — the executor via spec.job.jarURI
# (EMR's webhook, at CR CREATE, adds an initContainer that downloads it into
# /opt/flink/usrlib) and the SQL via the executor's --sql-dir. A key that is
# absent or zero-length is invisible to `kubectl create`: the CR is accepted,
# the JM boots, and only then does PythonDriver.main die with a NoSuchFileException naming a /tmp/pyflink path
# instead of the missing object. Checking here turns a 10-minute in-pod failure
# into an immediate shell error.
verify_s3_object() {
    local uri=$1 bucket key size
    bucket=${uri#s3://}; key=${bucket#*/}; bucket=${bucket%%/*}
    # --query on ContentLength, not `s3 ls`: `s3 ls` of a missing key exits 0
    # with no output, so it cannot distinguish absent from empty.
    size=$(aws s3api head-object --bucket "$bucket" --key "$key" \
             --region "$AWS_REGION" --query ContentLength --output text 2>/dev/null) \
      || error "NOT IN S3: ${uri}
  The pods resolve this key at boot. Applying the CR without it gives a
  JobManager that dies in PythonDriver.main with NoSuchFileException."
    (( size > 0 )) || error "ZERO-LENGTH in S3: ${uri}"
    echo "  ✓ ${size} B  ${uri}"
}

upload_scripts() {
    log "Uploading executor and SQL scripts to S3..."
    # This IS what the job runs. spec.job.jarURI points at this key; EMR on
    # EKS's webhook (at CR CREATE — see deploy) arranges for it to be
    # downloaded into /opt/flink/usrlib, which is why both manifests pass a
    # local /opt/flink/usrlib/... path to -py.
    local exec_uri="s3://${BUCKET_NAME}/flink/scripts/flink-cid-executor.py"
    aws s3 cp flink-cid-executor.py "$exec_uri" --region "$AWS_REGION"
    verify_s3_object "$exec_uri"

    # EVERY source variant, whatever SOURCE_MODE is, so S3 always holds the
    # full script set. They go to common/, which no job's --sql-dir points
    # at: the executor runs every .sql in its directory, so a second source
    # file in <fmt>/ would define datagen_source twice. Only the copy below,
    # 00-source.sql, is what a job runs.
    local common_uri="s3://${BUCKET_NAME}/flink/cid-sql/common"
    aws s3 sync sql-scripts/common/ "${common_uri}/" --region "$AWS_REGION" --delete
    for f in sql-scripts/common/*.sql; do
        verify_s3_object "${common_uri}/$(basename "$f")"
    done

    # SOURCE_MODE picks the file; both define the same datagen_source view.
    # (Burst mode and its bounded JSON replay source were removed on 2026-10-03;
    # s3json is a continuous source, not that.)
    local src_file src_desc
    case "$SOURCE_MODE" in
        s3json)
            src_file="sql-scripts/common/01-s3json-source.sql"
            src_desc="continuous gzip NDJSON from ${S3JSON_SOURCE_PATH}" ;;
        *)
            src_file="sql-scripts/common/01-datagen-source.sql"
            src_desc="unbounded datagen at ${SOURCE_ROWS_PER_SECOND} rows/s" ;;
    esac

    local fmt f
    for fmt in $FORMATS; do
        aws s3 sync "sql-scripts/${fmt}/" "s3://${BUCKET_NAME}/flink/cid-sql/${fmt}/" \
            --region "$AWS_REGION" --delete
        # The source runs FIRST in each format's directory (00- sorts before
        # 02-), before the USE CATALOG switch — the temporary source table must
        # live in default_catalog, which 04-pipeline.sql references explicitly.
        aws s3 cp "$src_file" \
            "s3://${BUCKET_NAME}/flink/cid-sql/${fmt}/00-source.sql" \
            --region "$AWS_REGION"
        # Verified per LOCAL file rather than by counting remote keys: the
        # executor globs --sql-dir and runs whatever is there in name order, so
        # a file that failed to sync is a pipeline silently missing a step, not
        # an error. 00-source.sql is checked separately — it is a rename of the
        # common/ file and has no counterpart in sql-scripts/<fmt>/.
        for f in "sql-scripts/${fmt}"/*.sql; do
            verify_s3_object "s3://${BUCKET_NAME}/flink/cid-sql/${fmt}/$(basename "$f")"
        done
        verify_s3_object "s3://${BUCKET_NAME}/flink/cid-sql/${fmt}/00-source.sql"
    done
    log "✓ Format SQL uploaded (SOURCE_MODE=${SOURCE_MODE}: ${src_desc})"
}

# ONLY the Iceberg database is pre-created here — same split as the CDC
# deploy script. The Paimon database must be created by the Paimon catalog's
# own CREATE DATABASE IF NOT EXISTS (02-catalog-setup.sql): Paimon's
# HiveCatalog derives every table location from the Glue database's
# LocationUri, and a database pre-created via `aws glue create-database`
# without a LocationUri makes CREATE TABLE die with
# "IllegalArgumentException: Can not create a Path from an empty string"
# in HiveCatalog.getTableLocation.
create_glue_databases() {
    local db="flink_cid_iceberg_db"
    if aws glue get-database --name "$db" --region "$AWS_REGION" &>/dev/null; then
        log "✓ Glue database exists: $db"
    else
        aws glue create-database \
            --database-input "{\"Name\":\"$db\",\"Description\":\"Flink Paimon-vs-Iceberg throughput benchmark\"}" \
            --region "$AWS_REGION"
        log "✓ Glue database created: $db"
    fi

    # Guard against the empty-LocationUri trap for the Paimon database: if it
    # exists WITHOUT a location (e.g. created by an earlier version of this
    # script), Paimon table creation will fail — refuse to proceed.
    local paimon_db="flink_cid_paimon_db" loc
    if aws glue get-database --name "$paimon_db" --region "$AWS_REGION" &>/dev/null; then
        loc=$(aws glue get-database --name "$paimon_db" --region "$AWS_REGION" \
                --query 'Database.LocationUri' --output text)
        if [[ -z "$loc" || "$loc" == "None" ]]; then
            error "Glue database ${paimon_db} exists with an empty LocationUri — Paimon
       CREATE TABLE will fail with 'Can not create a Path from an empty string'.
       Delete it and let the Paimon catalog recreate it with the right location:
         aws glue delete-database --name ${paimon_db} --region ${AWS_REGION}"
        fi
        log "✓ Glue database ${paimon_db} exists (location: ${loc})"
    else
        log "✓ Glue database ${paimon_db} absent — Paimon catalog will create it with the warehouse location"
    fi
}

render_manifest() {
    local fmt=$1
    local template="flink-cid-${fmt}.yaml"
    local output="flink-cid-${fmt}-deployed.yaml"
    [[ -f "$template" ]] || error "Template not found: $template"

    sed -e "s|\${AWS_REGION}|${AWS_REGION}|g" \
        -e "s|\${AWS_ACCOUNT_ID}|${AWS_ACCOUNT_ID}|g" \
        -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
        -e "s|\${EMR_EXECUTION_ROLE_ARN}|${EMR_EXECUTION_ROLE_ARN}|g" \
        -e "s|\${EMR_VERSION}|${EMR_VERSION}|g" \
        -e "s|\${IMAGE_TAG}|${IMAGE_TAG}|g" \
        -e "s|\${SOURCE_ROWS_PER_SECOND}|${SOURCE_ROWS_PER_SECOND}|g" \
        -e "s|\${JOB_PARALLELISM}|${JOB_PARALLELISM}|g" \
        -e "s|\${PAIMON_BUCKETS}|${PAIMON_BUCKETS}|g" \
        -e "s|\${PAIMON_BUCKET_KEY_OPT}|${PAIMON_BUCKET_KEY_OPT}|g" \
        -e "s|\${HOT_KEY_FRACTION}|${HOT_KEY_FRACTION}|g" \
        -e "s|\${S3JSON_SOURCE_PATH}|${S3JSON_SOURCE_PATH}|g" \
        -e "s|\${S3JSON_MONITOR_INTERVAL}|${S3JSON_MONITOR_INTERVAL}|g" \
        -e "s|\${CID_CUSTOMER_COUNT}|${CID_CUSTOMER_COUNT}|g" \
        -e "s|\${CID_HOST_COUNT}|${CID_HOST_COUNT}|g" \
        -e "s|\${CID_DAY_SPREAD}|${CID_DAY_SPREAD}|g" \
        -e "s|\${PAIMON_ICEBERG_STORAGE}|${PAIMON_ICEBERG_STORAGE}|g" \
        -e "s|\${PAIMON_WRITE_ONLY}|${PAIMON_WRITE_ONLY}|g" \
        -e "s|\${PAIMON_WRITE_BUFFER_SPILLABLE}|${PAIMON_WRITE_BUFFER_SPILLABLE}|g" \
        -e "s|\${PAIMON_WRITE_BUFFER_SIZE}|${PAIMON_WRITE_BUFFER_SIZE}|g" \
        -e "s|\${S3A_MULTIPART_THRESHOLD}|${S3A_MULTIPART_THRESHOLD}|g" \
        -e "s|\${S3A_MULTIPART_SIZE}|${S3A_MULTIPART_SIZE}|g" \
        -e "s|\${S3A_ACTIVE_BLOCKS}|${S3A_ACTIVE_BLOCKS}|g" \
        -e "s|\${ICEBERG_S3_MULTIPART_SIZE_BYTES}|${ICEBERG_S3_MULTIPART_SIZE_BYTES}|g" \
        -e "s|\${ICEBERG_S3_MULTIPART_THREADS}|${ICEBERG_S3_MULTIPART_THREADS}|g" \
        -e "s|\${JM_NODEPOOL}|${JM_NODEPOOL}|g" \
        -e "s|\${TM_NODEPOOL}|${TM_NODEPOOL}|g" \
        "$template" > "$output"

    if grep -qE '\$\{[A-Z0-9_]+\}' "$output"; then
        error "Unsubstituted placeholders remain in ${output}:
$(grep -nE '\$\{[A-Z0-9_]+\}' "$output" | head)"
    fi
    log "✓ Rendered $output"
    echo "$output"
}

# 'both' is accepted here as well as in cleanup, so `deploy both` and
# `cleanup both` are symmetric — a side-by-side run no longer needs the
# two-step `render` + hand-written `kubectl apply` the help text used to
# describe. See the warning deploy prints: both at once measures contention.
validate_format() {
    local fmt=$1 t
    [[ "$fmt" == "both" ]] && return 0
    for t in $DEPLOY_TARGETS; do
        [[ "$fmt" == "$t" ]] && return 0
    done
    error "Unknown target '$fmt' (expected one of: $DEPLOY_TARGETS, or 'both')"
}

main() {
    local action=${1:-help}
    echo "============================================================================"
    echo "  Flink Throughput Benchmark: Paimon vs Iceberg — APPEND-ONLY"
    echo "  EMR ${EMR_VERSION}, $(( SOURCE_ROWS_PER_SECOND * 4096 / 1024 / 1024 / 1024 )) GiB/s target, 4 KiB mean row, cid_events"
    echo "============================================================================"

    case $action in
        upload)
            # Stage everything the pods resolve from S3 at boot, and nothing
            # else: no manifest, no kubectl. deploy re-runs this anyway, so the
            # point is to catch a missing/empty object before triggering jobs.
            check_env_vars; display_config
            create_glue_databases
            upload_scripts
            info "Uploaded and head-object-verified. Next: $0 deploy paimon|iceberg|both"
            ;;
        render)
            check_env_vars; display_config
            check_image_exists
            create_glue_databases
            upload_scripts
            local fmt
            for fmt in $DEPLOY_TARGETS; do render_manifest "$fmt" >/dev/null; done
            info "Rendered but NOT applied. For a fair comparison run ONE format at"
            info "a time (they compete for nodes and S3 bandwidth):"
            echo "  $0 deploy paimon"
            echo "  ... run, collect, tear down, then ..."
            echo "  $0 deploy iceberg"
            info "The SQL is already in S3 — render uploaded it. By hand, CREATE the"
            info "CR fresh (EMR stages the jarURI only on CREATE; apply over an"
            info "existing CR leaves /opt/flink/usrlib empty). 'deploy' does this:"
            echo "  kubectl delete flinkdeployment flink-cid-<fmt> -n ${NAMESPACE} --ignore-not-found --wait"
            echo "  kubectl create -n ${NAMESPACE} -f flink-cid-<fmt>-deployed.yaml"
            ;;
        deploy)
            local fmt=${2:?usage: $0 deploy paimon|iceberg|both}
            validate_format "$fmt"
            check_env_vars; display_config
            check_image_exists
            local manifest f deployed=()
            create_glue_databases
            # Once for the whole action, not once per format: it syncs BOTH
            # format directories (FORMATS), so calling it inside the loop would
            # re-upload the same objects twice.
            upload_scripts
            for f in $DEPLOY_TARGETS; do
                [[ "$fmt" == "$f" || "$fmt" == "both" ]] || continue
                manifest=$(render_manifest "$f")
                # ALWAYS a fresh CREATE, never an apply over an existing CR.
                # EMR on EKS stages the s3:// jarURI through its mutating
                # webhook, which runs on CREATE only: it rewrites jarURI to
                # local:///opt/flink/usrlib/..., adds an aws-cli initContainer
                # that copies the object into an emptyDir, and mounts that
                # emptyDir at /opt/flink/usrlib. An apply over an existing
                # (e.g. suspended) CR is an UPDATE — the webhook does not run,
                # the merge patch replaces the containers list and drops the
                # mount, and the JM dies with NoSuchFileException on an empty
                # usrlib (2026-10-08). The stale initContainer also keeps the
                # S3 key from the original CREATE, so a renamed executor is
                # never picked up. upgradeMode is stateless, so deleting first
                # loses nothing.
                if kubectl get flinkdeployment "flink-cid-${f}" -n "$NAMESPACE" &>/dev/null; then
                    log "Deleting existing flink-cid-${f} so EMR re-stages the jarURI on CREATE..."
                    kubectl delete flinkdeployment "flink-cid-${f}" -n "$NAMESPACE" --wait=true --timeout=300s
                fi
                kubectl create -f "$manifest" -n "$NAMESPACE"
                log "✓ Deployed flink-cid-${f}"
                deployed+=("$f")
            done
            if (( ${#deployed[@]} > 1 )); then
                warn "BOTH writers deployed. They share ${TM_NODEPOOL} and one S3"
                warn "bucket, so this measures CONTENTION, not either write path."
                warn "A rows/s number you intend to quote must come from one"
                warn "format at a time, with the other suspended or deleted."
            fi
            # Each format needs its own LOCAL_PORT: collect-metrics.sh defaults
            # every invocation to 8091, so a second collector on the default
            # port reads the FIRST job's samples.
            local port=8091
            for f in ${deployed[@]+"${deployed[@]}"}; do
                echo "  Watch:  kubectl get flinkdeployment flink-cid-${f} -n ${NAMESPACE} -w"
                echo "  UI:     kubectl port-forward svc/flink-cid-${f}-rest 8081:8081 -n ${NAMESPACE}"
                echo "  Metrics: LOCAL_PORT=${port} ./monitoring/collect-metrics.sh ${f} 30 3600"
                port=$(( port + 1 ))
            done
            ;;
        cleanup)
            local fmt=${2:-both}
            warn "Deleting flink-cid deployment(s): ${fmt}"
            read -p "Are you sure? (yes/no): " -r; echo
            [[ $REPLY =~ ^[Yy][Ee][Ss]$ ]] || { log "Cancelled"; exit 0; }
            local f
            for f in $DEPLOY_TARGETS; do
                if [[ "$fmt" == "$f" || "$fmt" == "both" ]]; then
                    kubectl delete flinkdeployment "flink-cid-${f}" -n "$NAMESPACE" --ignore-not-found
                fi
            done
            log "✓ Cleanup complete (S3 warehouse/Glue tables left in place)"
            ;;
        monitor)
            kubectl get flinkdeployment -n "$NAMESPACE" | grep -E 'NAME|cid' || true
            kubectl get pods -n "$NAMESPACE" | grep -E 'NAME|cid' || true
            ;;
        help|*)
            echo "Usage: $0 {upload|render|deploy TARGET|cleanup [TARGET]|monitor}"
            echo ""
            echo "  upload             Create Glue DBs, upload + verify executor and SQL in S3 (no kubectl)"
            echo "  render             Render all manifests, upload scripts, prep Glue/ConfigMap"
            echo "  deploy paimon|iceberg|both"
            echo "                     Deploy one writer, or 'both' for a side-by-side"
            echo "                     run. Both at once measures CONTENTION, not"
            echo "                     either write path — deploy warns when you do it."
            echo "  cleanup [paimon|iceberg|both]"
            echo "                     Delete deployment(s); warehouse data is kept"
            echo "  monitor            Show benchmark FlinkDeployments and pods"
            echo ""
            echo "NO COMPACTION SHIPS WITH THIS PROJECT. The paimon-compact target, the"
            echo "Spark iceberg-compaction/ harness, burst mode and the jsongen dataset"
            echo "generator were all removed on 2026-10-03. This project measures the"
            echo "WRITE PATH over a bounded window and nothing else: neither side is"
            echo "maintained, so both accumulate small files for as long as they run."
            echo "Say so when reporting any result, and do not benchmark READS against"
            echo "the tables these jobs leave behind without compacting them first."
            echo ""
            echo "The image is the CDC pipeline's emr-flink-cdc-paimon:${IMAGE_TAG}."
            echo "Build it (once) with: cd ../cdc-pipeline && ./build-deploy-generic.sh build"
            echo ""
            echo "Required env: AWS_REGION AWS_ACCOUNT_ID BUCKET_NAME EMR_EXECUTION_ROLE_ARN"
            echo "Knobs: SOURCE_ROWS_PER_SECOND JOB_PARALLELISM IMAGE_TAG"
            echo "       SOURCE_MODE=datagen|s3json (default datagen). s3json reads the"
            echo "       gzip NDJSON from S3JSON_SOURCE_PATH (jsongz-gen/), continuously."
            echo "       PAIMON_BUCKETS=N (default -1 = unaware-bucket) — Paimon 'bucket';"
            echo "       N >= 1 = fixed buckets on uuid, use N = JOB_PARALLELISM."
            echo "       BAKED IN at CREATE TABLE: drop cid_events before changing it."
            echo "       HOT_KEY_FRACTION=0..99 (default 0=uniform; 80=80/20 skew)"
            echo "       Routes the ENDPOINT identity, not a primary key — the tables"
            echo "       are append-only, so skew no longer drives merge/delete work."
            echo "       It changes the generated BYTES, so it invalidates comparison"
            echo "       across values."
            echo "       CID_CUSTOMER_COUNT=N (default 16) — distinct customerId values"
            echo "       CID_HOST_COUNT=N (default 1024) — distinct endpoints (sample: 1047)"
            echo "       CID_DAY_SPREAD=N (default 1) — distinct dt values, i.e. the"
            echo "       PARTITION COUNT on both sinks. >1 multiplies concurrently-open"
            echo "       files per writer by N on both sides; report it with any result."
            echo "       ICEBERG_COMPAT=true|false (default false) — Paimon's per-commit"
            echo "       Iceberg metadata export, i.e. Paimon as the CDC pipeline runs it."
            echo "       Baked in at table creation: drop the Paimon table before flipping."
            echo "       NOT configurable: Paimon's write path is pinned to"
            echo "       write-only=false + write-buffer-spillable=true (Paimon's own"
            echo "       defaults, and the only configuration this project measures)."
            echo "       The WRITE_ONLY and SPILLABLE env knobs were removed 2026-10-05."
            echo "       WRITE_BUFFER=<size> (default 128mb) — Paimon write-buffer-size."
            echo "       Only moves bytes between RAM and spill files; the bytes the"
            echo "       barrier external-merges are set by rate x checkpoint interval."
            echo "       Applied as an INSERT hint, so no table drop needed to flip it."
            echo "       S3A_MULTIPART_SIZE=<size> (default 16M) — Paimon only, floor 5M."
            echo "       THE knob: it is the block cut point, so it must stay BELOW the"
            echo "       data file size or S3A uploads each file as a single PUT at"
            echo "       close() instead of streaming blocks as they fill. Set as a"
            echo "       catalog option: the shaded S3FileIO ignores flinkConfiguration"
            echo "       and core-site.xml."
            echo "       S3A_MULTIPART_THRESHOLD=<size> (default 16M) — copy path only;"
            echo "       the write path never reads it. Kept aligned with the part size."
            echo "       S3A_ACTIVE_BLOCKS=N (default 6) — queued blocks per stream."
            echo ""
            echo "Side-by-side run (both writers, started together):"
            echo "  source .bench-env"
            echo "  $0 deploy both"
            echo "  LOCAL_PORT=8091 ./monitoring/collect-metrics.sh paimon 30 3600 &"
            echo "  LOCAL_PORT=8092 ./monitoring/collect-metrics.sh iceberg 30 3600 &"
            exit 1
            ;;
    esac
    log "Done!"
}

main "$@"
