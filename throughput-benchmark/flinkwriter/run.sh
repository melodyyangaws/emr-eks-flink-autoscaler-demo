#!/bin/bash
###############################################################################
# Flink Throughput Benchmark — Paimon vs Iceberg — deploy
#
# Compares the Flink WRITE-PATH performance of the two table formats on the
# SAME image the CDC pipeline already uses (emr-flink-cdc-paimon, EMR 7.13
# base — it bundles Paimon 1.3.2 AND Iceberg 1.10.0-amzn-1):
#   paimon  : append-only table, PARTITIONED BY (dt), bucketed on uuid
#   iceberg : format v2, append-only, PARTITIONED BY (dt), distribution-mode=none
# Both read the same gzip NDJSON files (~4 KiB mean row) that datagen/jsongz-gen/
# lands under OUTPUT_URI (.bench-env). Each PUT fires an S3 event
# to SNS cid-landing-jsongz-events, which fans out to one SQS queue per job
# (<SQS_QUEUE_PREFIX>-paimon / -iceberg); the sqs-delta-filename connector
# reads the files those messages name. There is no rate knob here: the jobs
# read as fast as they can, so the offered rate is whatever the generator
# lands (JSONGZ_REPLICAS x JSONGZ_ROWS_PER_POD in datagen/run.sh).
#
# APPEND-ONLY. Both sinks are pure INSERT: no primary key, no upsert, no
# equality/merge-on-read deletes, no changelog producer, and the pipelines are
# INSERT INTO ... SELECT over the source view with no stateful operator, so the
# planner cannot emit a retraction. The formats are therefore compared on
# encode + upload + commit + small-file compaction, NOT on merge-on-read cost.
# Results are NOT comparable with the earlier upsert series in bench-results/.
#
# There is deliberately NO image build here. The benchmark reuses the CDC
# image byte-for-byte — build it (once) with:
#   cd ../cdc-pipeline && ./build-deploy-generic.sh build
#
# USAGE (from throughput-benchmark/; it sources .bench-env itself, through lib/common.sh):
#   ./flinkwriter/run.sh upload                # optional pre-check: Glue DBs + executor/SQL to S3, verified; no kubectl
#   ./flinkwriter/run.sh deploy paimon         # runs upload first, always; one writer — the only way to get a quotable rows/s
#   ./flinkwriter/run.sh deploy iceberg
#   ./flinkwriter/run.sh deploy both           # side by side, as the benchmark runs them
#   ./flinkwriter/run.sh render                # write flink-cid-<fmt>-deployed.yaml, don't apply
#   ./flinkwriter/run.sh cleanup [paimon|iceberg|both]   # delete the CR(s); asks "yes"; keeps data
# deploy always deletes and re-CREATEs the CR, since EMR stages jarURI on CREATE only.
# Suspend without deleting:
#   kubectl patch flinkdeployment flink-cid-<fmt> -n emr-flink --type=merge \
#     -p '{"spec":{"job":{"state":"suspended"}}}'
# Knobs (env, defaults below): SQS_QUEUE_PREFIX, SQS_POLL_THREADS,
#   SQS_MAX_PENDING, SQS_VISIBILITY_TIMEOUT, SQS_SOURCE_JAR, JOB_PARALLELISM,
#   PAIMON_BUCKETS (-1 = unaware), ICEBERG_COMPAT, WRITE_BUFFER, JM_NODEPOOL,
#   TM_NODEPOOL.
# Changing JOB_PARALLELISM/PAIMON_BUCKETS or ICEBERG_COMPAT requires dropping the
# tables first (README "Teardown"): the bucket count is fixed at CREATE TABLE.
###############################################################################

set -e
# .bench-env, NAMESPACE, TEST_SUFFIX/TEST_DB_SUFFIX, log/info/warn/error, render.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
# Everything below is relative to flinkwriter/: the templates, the executor, sql-scripts/.
cd "${BENCH_DIR}/flinkwriter"

export AWS_REGION=${AWS_REGION:-us-west-2}
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-$AWS_REGION}

# ---------------------------------------------------------------------------
# Benchmark knobs (all overridable from the environment)
# ---------------------------------------------------------------------------
export EMR_VERSION=${EMR_VERSION:-7.13.0}
export IMAGE_TAG=${IMAGE_TAG:-$EMR_VERSION}
# Each subtask parses as well as writes (~1 core per subtask at ~100 us of CPU
# per row), so parallelism caps the rate a job can absorb; .bench-env sizes it
# against the generator's rate. Scale up only if the source backpressures on
# BOTH formats — that means the harness, not the format, is the wall.
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

# ---------------------------------------------------------------------------
# Source: S3 event notifications via SQS (the only source)
# ---------------------------------------------------------------------------
# upload_scripts() publishes 01-raw-s3sqs.sql + 01-parse-s3json.sql as each
# format's 00-source.sql. Data shape and rate are whatever datagen/jsongz-gen/ lands;
# its own knobs (CID_CUSTOMER_COUNT, CID_HOST_COUNT, JSONGZ_*) live there.
#
# One queue per format, both subscribed to SNS cid-landing-jsongz-events, so
# each job sees every file: <prefix>-paimon and <prefix>-iceberg. The URL is
# built per format in render_manifest (sqs_queue_url).
export SQS_QUEUE_PREFIX=${SQS_QUEUE_PREFIX:-cid-landing-jsongz}
# Concurrent ReceiveMessage loops in the source's enumerator.
export SQS_POLL_THREADS=${SQS_POLL_THREADS:-8}
if [[ ! "$SQS_POLL_THREADS" =~ ^[0-9]+$ ]] || (( SQS_POLL_THREADS < 1 )); then
    error "SQS_POLL_THREADS must be an integer >= 1 (got '$SQS_POLL_THREADS')"
fi
# Cap on received-but-unassigned splits. EMPTY = the connector's own default:
# the option line is then not rendered at all, because the executor
# substitutes an empty env var as '' (it only falls back to a ${VAR:default}
# when the var is ABSENT), and '' is not an integer. Same trick as
# PAIMON_BUCKET_KEY_OPT.
export SQS_MAX_PENDING=${SQS_MAX_PENDING:-}
if [[ -n "$SQS_MAX_PENDING" ]]; then
    [[ "$SQS_MAX_PENDING" =~ ^[0-9]+$ ]] \
        || error "SQS_MAX_PENDING must be an integer or empty (got '$SQS_MAX_PENDING')"
    export SQS_MAX_PENDING_OPT="'max-pending-splits' = '${SQS_MAX_PENDING}',"
else
    export SQS_MAX_PENDING_OPT=""
fi
# How long a received message stays hidden. It must outlast the 60 s
# checkpoint interval plus a file's read time, or the message reappears and
# its file is read twice. A Flink Duration string.
export SQS_VISIBILITY_TIMEOUT=${SQS_VISIBILITY_TIMEOUT:-15min}
if [[ ! "$SQS_VISIBILITY_TIMEOUT" =~ ^[0-9]+(ms|s|min|h)$ ]]; then
    error "SQS_VISIBILITY_TIMEOUT must be a duration like '15min' or '900s' (got '$SQS_VISIBILITY_TIMEOUT')"
fi
# The connector jar, as the SQL's ADD JAR sees it. s3a:// because it is
# resolved by the image's Flink FileSystem (Hadoop S3A), not by the AWS CLI.
# upload_scripts() uploads to the s3:// form of the same key.
# Built from datagen/sqs-delta-filename/ (mvn -q package; the shaded jar is the main
# artifact) when missing or older than its sources.
export SQS_SOURCE_JAR=${SQS_SOURCE_JAR:-s3a://${BUCKET_NAME}/flink/jars/sqs-delta-filename.jar}
SQS_CONNECTOR_DIR=../datagen/sqs-delta-filename

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
# Exception: PAIMON_WRITE_BUFFER_SPILLABLE=false may be set per run (2026-10-11)
# with PAIMON_WRITE_ONLY=true: a full write buffer then flushes an L0 file in
# the record path instead of spilling a sorted run for the barrier to merge.
export PAIMON_WRITE_BUFFER_SPILLABLE=${PAIMON_WRITE_BUFFER_SPILLABLE:-true}
if [[ "$PAIMON_WRITE_BUFFER_SPILLABLE" != true && "$PAIMON_WRITE_BUFFER_SPILLABLE" != false ]]; then
    echo "PAIMON_WRITE_BUFFER_SPILLABLE must be true or false, got '$PAIMON_WRITE_BUFFER_SPILLABLE'" >&2; exit 1
fi

# Paimon write buffer size. The default 128 MiB is the same as Iceberg's
export WRITE_BUFFER=${WRITE_BUFFER:-128mb}
if [[ ! "$WRITE_BUFFER" =~ ^[0-9]+(kb|mb|gb)$ ]]; then
    error "WRITE_BUFFER must be a Paimon MemorySize like '256mb' (got '$WRITE_BUFFER')"
fi
PAIMON_WRITE_BUFFER_SIZE="$WRITE_BUFFER"
export PAIMON_WRITE_BUFFER_SIZE

# ---------------------------------------------------------------------------
# S3A upload tuning for Paimon
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
    echo "  Source:            gzip NDJSON keys from S3 events on SQS"
    echo "                     queues ${SQS_QUEUE_PREFIX}-{paimon,iceberg} (one per format)"
    echo "                     poll-threads=$SQS_POLL_THREADS max-pending=${SQS_MAX_PENDING:-<connector default>} visibility=$SQS_VISIBILITY_TIMEOUT"
    echo "                     jar: $SQS_SOURCE_JAR"
    echo "                     ^ rate and data shape are set by datagen/jsongz-gen/, not here"
    echo "  Job parallelism:   $JOB_PARALLELISM"
    echo "  Paimon buckets:    $PAIMON_BUCKETS (-1 = unaware-bucket, no shuffle; fixed at CREATE TABLE)"
    # The tables are append-only now; make sure nobody reads a new number as
    # comparable with the upsert-era series.
    echo "  Table shape:       APPEND-ONLY, PARTITIONED BY (dt), 34 cols (schema_sample.sql)"
    echo "                     ^ no primary key. NOT comparable with pre-CID upsert runs."
    echo "  Iceberg compat:    $ICEBERG_COMPAT (paimon metadata.iceberg.storage = $PAIMON_ICEBERG_STORAGE)"
    echo "  Paimon write path: write-only=$PAIMON_WRITE_ONLY spillable=$PAIMON_WRITE_BUFFER_SPILLABLE"
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

# Per-format queue: the Paimon job reads <prefix>-paimon, Iceberg <prefix>-iceberg.
sqs_queue_url() {
    echo "https://sqs.${AWS_REGION}.amazonaws.com/${AWS_ACCOUNT_ID}/${SQS_QUEUE_PREFIX}-$1"
}

# The shaded sqs-delta-filename connector jar, rebuilt when it is missing or
# any file under src/ or the pom is newer than it. Sets SQS_JAR_LOCAL (a
# global, not stdout, so error() is not swallowed by a command substitution).
# original-*.jar (the pre-shade artifact) does not match the glob; -sources,
# -tests and -javadoc jars are filtered out, and the newest remaining jar wins.
newest_sqs_jar() {
    ls -t "${SQS_CONNECTOR_DIR}"/target/sqs-delta-filename-*.jar 2>/dev/null \
        | grep -vE -- '-(sources|tests|javadoc)\.jar$' | head -1 || true
}
build_sqs_jar() {
    local dir=$SQS_CONNECTOR_DIR
    [[ -f "${dir}/pom.xml" ]] || error "The SQS source needs the connector source at ${dir}/pom.xml"
    SQS_JAR_LOCAL=$(newest_sqs_jar)
    if [[ -z "$SQS_JAR_LOCAL" ]] \
       || [[ -n "$(find "${dir}/src" "${dir}/pom.xml" -type f -newer "$SQS_JAR_LOCAL" 2>/dev/null | head -1)" ]]; then
        log "Building ${dir} (mvn -q package)..."
        ( cd "$dir" && mvn -q package ) || error "mvn package failed in ${dir}"
        SQS_JAR_LOCAL=$(newest_sqs_jar)
        [[ -n "$SQS_JAR_LOCAL" ]] || error "mvn package produced no ${dir}/target/sqs-delta-filename-*.jar"
    else
        log "✓ ${SQS_JAR_LOCAL} is up to date with its sources"
    fi
}

# Read-only preflight for the SQS source: what each queue already holds is
# what the job will read first. Visible = waiting; NotVisible = received by a
# consumer (e.g. a job that was just deleted) and not yet deleted — those come
# back after the visibility timeout. NO purge here; see the README.
check_sqs_backlog() {
    local fmt url counts
    for fmt in "$@"; do
        url=$(sqs_queue_url "$fmt")
        counts=$(aws sqs get-queue-attributes --queue-url "$url" --region "$AWS_REGION" \
                   --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
                   --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]' \
                   --output text 2>/dev/null) \
          || error "Cannot read SQS queue ${url}
       It must exist (subscribed to SNS cid-landing-jsongz-events) before an
       the job starts; check SQS_QUEUE_PREFIX and the region."
        read -r _vis _invis <<<"$counts"
        echo "  ${fmt}: ${SQS_QUEUE_PREFIX}-${fmt}  visible=${_vis} in-flight=${_invis}"
        if (( _vis + _invis > 0 )); then
            warn "${SQS_QUEUE_PREFIX}-${fmt} already holds $(( _vis + _invis )) message(s): the job
       reads those files first. If they belong to an earlier run, purge first:
         aws sqs purge-queue --queue-url ${url} --region ${AWS_REGION}"
        fi
    done
    unset _vis _invis
}

# ── S3 -> SNS link (the bucket's event notification) ─────────────────────────
# Which landing files reach the queues is decided by an event notification ON
# THE BUCKET, not by anything in this repo: OUTPUT_URI only tells the generator
# where to write.
# OUTPUT_URI (.bench-env) is the ONE landing location: the generator writes
# only there, and each job's source reads only keys under it ('key-prefix' in
# 01-raw-s3sqs.sql, from LANDING_KEY_PREFIX in render_manifest).
: "${OUTPUT_URI:?OUTPUT_URI is not set in .bench-env}"
[[ "$OUTPUT_URI" == s3://*/* ]] || { echo "OUTPUT_URI must be s3://bucket/prefix, got '$OUTPUT_URI'" >&2; exit 1; }
export SNS_TOPIC_NAME=${SNS_TOPIC_NAME:-cid-landing-jsongz-events}
landing_bucket() { local p=${OUTPUT_URI#s3://}; echo "${p%%/*}"; }
landing_prefix() { local p=${OUTPUT_URI#s3://}; p=${p#*/}; echo "${p%/}/"; }
sns_topic_arn()  { echo "arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:${SNS_TOPIC_NAME}"; }

# Set the bucket's notification for SNS_TOPIC_NAME to OUTPUT_URI's prefix
# (+ .json.gz, ObjectCreated), keeping every other notification on the bucket,
# then prove it end to end: an empty gzip file under the prefix must show up in
# BOTH queues. Only the test file's messages and S3's s3:TestEvent (sent on
# every configuration change) are deleted; anything else received is made
# visible again at once. Refuses while a writer is RUNNING, so
# no job can read the test file.
cmd_notify() {
    local bucket prefix topic cfg key fmt url found i
    [[ "$OUTPUT_URI" == s3://* ]] || error "OUTPUT_URI must be s3://bucket/prefix (got '${OUTPUT_URI}')"
    bucket=$(landing_bucket); prefix=$(landing_prefix); topic=$(sns_topic_arn)
    if kubectl get flinkdeployment -n "$NAMESPACE" -o jsonpath='{.items[*].status.jobStatus.state}' 2>/dev/null | grep -q RUNNING; then
        error "A Flink writer is RUNNING; it would read the test file. Suspend both first:
         kubectl patch flinkdeployment flink-cid-{paimon,iceberg} -n ${NAMESPACE} --type=merge -p '{\"spec\":{\"job\":{\"state\":\"suspended\"}}}'"
    fi
    cfg=$(mktemp)
    aws s3api get-bucket-notification-configuration --bucket "$bucket" --region "$AWS_REGION" --output json \
      | python3 -c 'import json, sys
topic, prefix = sys.argv[1], sys.argv[2]
d = json.load(sys.stdin)
d.pop("ResponseMetadata", None)
mine = {"Id": prefix.strip("/") + "-to-sns", "TopicArn": topic, "Events": ["s3:ObjectCreated:*"],
        "Filter": {"Key": {"FilterRules": [{"Name": "Prefix", "Value": prefix},
                                           {"Name": "Suffix", "Value": ".json.gz"}]}}}
d["TopicConfigurations"] = [c for c in d.get("TopicConfigurations", []) if c.get("TopicArn") != topic] + [mine]
print(json.dumps(d))' "$topic" "$prefix" > "$cfg" || error "Cannot read the notification configuration of bucket ${bucket}"
    log "Setting the notification on bucket ${bucket}: ${prefix}*.json.gz -> SNS ${SNS_TOPIC_NAME}"
    aws s3api put-bucket-notification-configuration --bucket "$bucket" --region "$AWS_REGION" \
        --notification-configuration "file://${cfg}" || error "put-bucket-notification-configuration failed"
    rm -f "$cfg"

    key="${prefix}_notify-test/$(date -u +%Y%m%dT%H%M%SZ)-$$.json.gz"
    printf '' | gzip > "${TMPDIR:-/tmp}/notify-test.json.gz"
    aws s3 cp --quiet "${TMPDIR:-/tmp}/notify-test.json.gz" "s3://${bucket}/${key}" --region "$AWS_REGION" \
        || error "Cannot write the test file s3://${bucket}/${key}"
    log "Test file s3://${bucket}/${key}; waiting for it on both queues..."
    for fmt in $DEPLOY_TARGETS; do
        url=$(sqs_queue_url "$fmt"); found=0
        for i in 1 2 3 4 5 6 7 8 9; do
            found=$(aws sqs receive-message --queue-url "$url" --region "$AWS_REGION" --max-number-of-messages 10 \
                      --wait-time-seconds 10 --visibility-timeout 30 --output json \
                | python3 -c 'import json, subprocess, sys
key, url, region = sys.argv[1:4]
hit = 0
for m in (json.load(sys.stdin) if not sys.stdin.isatty() else {}).get("Messages", []):
    # S3 also sends an s3:TestEvent whenever the configuration changes: delete it too.
    ours = key in m["Body"] or "s3:TestEvent" in m["Body"]
    act = ["delete-message"] if ours else ["change-message-visibility", "--visibility-timeout", "0"]
    hit |= key in m["Body"]
    subprocess.run(["aws", "sqs", *act, "--queue-url", url, "--region", region,
                    "--receipt-handle", m["ReceiptHandle"]], check=True, capture_output=True)
print(int(hit))' "$key" "$url" "$AWS_REGION" 2>/dev/null || echo 0)
            [[ "$found" == 1 ]] && break
        done
        if [[ "$found" == 1 ]]; then log "✓ ${SQS_QUEUE_PREFIX}-${fmt}: test event received (and deleted)"
        else warn "${SQS_QUEUE_PREFIX}-${fmt}: no event for the test file within ~90 s"; fi
    done
    aws s3 rm --quiet "s3://${bucket}/${key}" --region "$AWS_REGION"
    rm -f "${TMPDIR:-/tmp}/notify-test.json.gz"
    log "✓ Test file removed. Files under ${prefix} now reach both queues."
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

    # The connector jar. Its key is SQS_SOURCE_JAR with the scheme swapped
    # to s3:// for the CLI; ADD JAR reads the s3a:// form.
    local jar_uri
    case "$SQS_SOURCE_JAR" in
        s3a://*) jar_uri="s3://${SQS_SOURCE_JAR#s3a://}" ;;
        s3://*)  jar_uri="$SQS_SOURCE_JAR" ;;
        *) error "SQS_SOURCE_JAR must be s3a:// or s3:// (got '$SQS_SOURCE_JAR')" ;;
    esac
    build_sqs_jar
    aws s3 cp "$SQS_JAR_LOCAL" "$jar_uri" --region "$AWS_REGION"
    verify_s3_object "$jar_uri"

    # The RAW fragment (defines s3json_raw) followed by the parse fragment,
    # concatenated into a local temp file, so the parse text cannot drift
    # between formats. Neither fragment runs on its own, so neither is
    # uploaded by itself.
    local src_file
    src_file=$(mktemp "${TMPDIR:-/tmp}/cid-00-source.XXXXXX")
    cat sql-scripts/common/01-raw-s3sqs.sql sql-scripts/common/01-parse-s3json.sql > "$src_file"

    local fmt f
    for fmt in $FORMATS; do
        aws s3 sync "sql-scripts/${fmt}/" "s3://${BUCKET_NAME}/flink/cid-sql/${fmt}/" \
            --region "$AWS_REGION" --delete
        # The source runs FIRST in each format's directory (00- sorts before
        # 02-), before the USE CATALOG switch — the temporary source table must
        # live in default_catalog, which 04-pipeline.sql references explicitly.
        # The same file for both formats: the queue URL differs per format but
        # is a ${SQS_QUEUE_URL} the executor fills from the JM env.
        aws s3 cp "$src_file" \
            "s3://${BUCKET_NAME}/flink/cid-sql/${fmt}/00-source.sql" \
            --region "$AWS_REGION"
        # Verified per LOCAL file rather than by counting remote keys: the
        # executor globs --sql-dir and runs whatever is there in name order, so
        # a file that failed to sync is a pipeline silently missing a step, not
        # an error. 00-source.sql is checked separately — it is built from the
        # common/ fragments and has no counterpart in sql-scripts/<fmt>/.
        for f in "sql-scripts/${fmt}"/*.sql; do
            verify_s3_object "s3://${BUCKET_NAME}/flink/cid-sql/${fmt}/$(basename "$f")"
        done
        verify_s3_object "s3://${BUCKET_NAME}/flink/cid-sql/${fmt}/00-source.sql"
    done
    rm -f "$src_file"
    log "✓ Format SQL uploaded (source: gzip NDJSON keys from SQS ${SQS_QUEUE_PREFIX}-<fmt>)"
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
    local db="flink_cid_iceberg_db${TEST_DB_SUFFIX}"
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
    local paimon_db="flink_cid_paimon_db${TEST_DB_SUFFIX}" loc
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
    # Two locals: in one `local fmt=$1 output=...${fmt}...`, ${fmt} expands to
    # the CALLER's fmt (`both` under `deploy both`), so every format was
    # written to flink-cid-both-deployed.yaml (fixed 2026-10-21).
    local fmt=$1
    local output="flink-cid-${fmt}-deployed.yaml"
    local SQS_QUEUE_URL LANDING_KEY_PREFIX
    SQS_QUEUE_URL=$(sqs_queue_url "$fmt"); LANDING_KEY_PREFIX=$(landing_prefix)
    # Each template's own placeholders: the shared ones, then the format's.
    local vars=(AWS_REGION AWS_ACCOUNT_ID BUCKET_NAME EMR_EXECUTION_ROLE_ARN EMR_VERSION
        IMAGE_TAG JOB_PARALLELISM SQS_QUEUE_URL SQS_SOURCE_JAR SQS_POLL_THREADS
        LANDING_KEY_PREFIX SQS_MAX_PENDING_OPT SQS_VISIBILITY_TIMEOUT JM_NODEPOOL TM_NODEPOOL
        TEST_SUFFIX TEST_DB_SUFFIX)
    case "$fmt" in
        paimon)  vars+=(PAIMON_BUCKETS PAIMON_BUCKET_KEY_OPT PAIMON_ICEBERG_STORAGE PAIMON_WRITE_ONLY
                        PAIMON_WRITE_BUFFER_SPILLABLE PAIMON_WRITE_BUFFER_SIZE
                        S3A_MULTIPART_THRESHOLD S3A_MULTIPART_SIZE S3A_ACTIVE_BLOCKS) ;;
        iceberg) vars+=(ICEBERG_S3_MULTIPART_SIZE_BYTES ICEBERG_S3_MULTIPART_THREADS) ;;
    esac
    render "flink-cid-${fmt}.yaml" "$output" "${vars[@]}" || error "could not render ${output}"
    log "✓ Rendered $output"
    echo "$output"
}

# 'both' is accepted here as well as in cleanup, so `deploy both` and
# `cleanup both` are symmetric — a side-by-side run no longer needs the
# two-step `render` + hand-written `kubectl apply` the help text used to
# describe.
validate_format() {
    local fmt=$1 t
    [[ "$fmt" == "both" ]] && return 0
    for t in $DEPLOY_TARGETS; do
        [[ "$fmt" == "$t" ]] && return 0
    done
    error "Unknown target '$fmt' (expected one of: $DEPLOY_TARGETS, or 'both')"
}

# The upload step: everything the pods resolve from S3 at boot (Glue DBs, the
# executor, both formats' SQL), head-object verified. `deploy` and `render`
# always run it first, so `upload` on its own is only a pre-check.
do_upload() {
    create_glue_databases
    upload_scripts
}

main() {
    local action=${1:-help}
    echo "============================================================================"
    echo "  Flink Throughput Benchmark: Paimon vs Iceberg — APPEND-ONLY"
    echo "  EMR ${EMR_VERSION}, S3 -> SQS gzip NDJSON source, 4 KiB mean row, cid_events"
    echo "============================================================================"

    case $action in
        upload)
            # The upload step alone: no manifest, no kubectl. deploy runs it
            # anyway; this only catches a missing/empty object before any job.
            check_env_vars; display_config
            do_upload
            info "Uploaded and head-object-verified. Next: $0 deploy paimon|iceberg|both (it uploads again)"
            ;;
        render)
            check_env_vars; display_config
            check_image_exists
            do_upload
            local fmt
            for fmt in $DEPLOY_TARGETS; do render_manifest "$fmt" >/dev/null; done
            info "Rendered but NOT applied. To start them: $0 deploy paimon|iceberg|both"
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
            # Always the full upload step, once for the whole action, not once
            # per format: upload_scripts syncs BOTH format directories
            # (FORMATS), so inside the loop it would upload everything twice.
            do_upload
            log "SQS backlog (read-only; the job reads these first):"
            if [[ "$fmt" == "both" ]]; then check_sqs_backlog $DEPLOY_TARGETS
            else check_sqs_backlog "$fmt"; fi
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
                info "Both writers deployed, side by side: they share ${TM_NODEPOOL}"
                info "and one S3 bucket, so each rate is under the same shared load."
            fi
            for f in ${deployed[@]+"${deployed[@]}"}; do
                echo "  Watch:  kubectl get flinkdeployment flink-cid-${f} -n ${NAMESPACE} -w"
                echo "  UI:     kubectl port-forward svc/flink-cid-${f}-rest 8081:8081 -n ${NAMESPACE}"
            done
            echo "  Monitor: ./monitoring/run.sh start   (logs: kubectl logs -f deployment/flink-cid-monitor -n ${NAMESPACE})"
            ;;
        notify)
            # Point the bucket's S3 event notification at OUTPUT_URI's prefix and
            # check a test file reaches both queues (see cmd_notify).
            check_env_vars
            cmd_notify
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
        help|*)
            echo "Usage: $0 {upload|render|deploy TARGET|cleanup [TARGET]}"
            echo ""
            echo "  upload             Create Glue DBs, upload + verify executor and SQL in S3 (no kubectl)"
            echo "  render             Render all manifests, upload scripts, prep Glue/ConfigMap"
            echo "  deploy paimon|iceberg|both"
            echo "  notify                      Point the bucket's S3 event notification at"
            echo "                              OUTPUT_URI's prefix and test it through both queues"
            echo "                              (deploy does not check it: run notify after changing OUTPUT_URI)"
            echo "                     Deploy one writer, or 'both' side by side (the"
            echo "                     benchmark's run: same nodepool, same bucket)"
            echo "  cleanup [paimon|iceberg|both]"
            echo "                     Delete deployment(s); warehouse data is kept"
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
            echo "Knobs: JOB_PARALLELISM IMAGE_TAG"
            echo "       The source is the gzip NDJSON datagen/jsongz-gen/ lands, read from S3"
            echo "       events on SQS queues \${SQS_QUEUE_PREFIX}-paimon / -iceberg"
            echo "       (default prefix cid-landing-jsongz) via the sqs-delta-filename"
            echo "       connector; no listing. upload builds + uploads its jar to"
            echo "       SQS_SOURCE_JAR, and deploy prints each queue's backlog first"
            echo "       (it does not purge)."
            echo "       SQS_POLL_THREADS (8), SQS_MAX_PENDING (empty = connector default),"
            echo "       SQS_VISIBILITY_TIMEOUT (15min)."
            echo "       PAIMON_BUCKETS=N (default -1 = unaware-bucket) — Paimon 'bucket';"
            echo "       N >= 1 = fixed buckets on uuid, use N = JOB_PARALLELISM."
            echo "       BAKED IN at CREATE TABLE: drop cid_events before changing it."
            echo "       ICEBERG_COMPAT=true|false (default false) — Paimon's per-commit"
            echo "       Iceberg metadata export, i.e. Paimon as the CDC pipeline runs it."
            echo "       Baked in at table creation: drop the Paimon table before flipping."
            echo "       PAIMON_WRITE_ONLY=true|false (default false) and"
            echo "       PAIMON_WRITE_BUFFER_SPILLABLE=true|false (default true): per-run"
            echo "       exceptions to Paimon's defaults; label any result taken with them."
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
            echo "  ./monitoring/run.sh start"
            exit 1
            ;;
    esac
    log "Done!"
}

main "$@"
