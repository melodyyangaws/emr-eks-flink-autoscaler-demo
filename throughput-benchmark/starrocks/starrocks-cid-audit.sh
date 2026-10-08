#!/usr/bin/env bash
set -uo pipefail

# StarRocks THREE-ARM read benchmark over the throughput project's APPEND-ONLY
# cid_events table, timed from the FE AUDIT LOG.
#
#   arm1  PAI     Paimon reader      → Paimon table
#   arm2  PMNICE  Iceberg reader     → the SAME Paimon data files, reached through
#                                      the Iceberg metadata Paimon exports
#   arm3  ICE     Iceberg reader     → natively-written Iceberg table
#
# Arm 2 is what makes this a format comparison rather than a read-path
# comparison. arm1-vs-arm2 holds the bytes on disk constant and changes only the
# reader; arm2-vs-arm3 holds the reader constant and changes only the writer. Any
# arm1-vs-arm3 gap is the sum of both, and reporting it alone (which the earlier
# two-arm version of this script forced) cannot say which half dominates.
#
# Why this exists next to starrocks-bench.sh
# ------------------------------------------
# starrocks-bench.sh times each query client-side, around a `kubectl exec ...
# mysql` invocation. That wrapper costs ~5.5s on this cluster (measured in
# cdc-pipeline/sql-scripts/starrocks/bench-server-side.sh: a bare `SELECT 1`
# took 5,665ms while the COUNT(*) next to it took 6,079ms and appeared in the
# audit log as Time=684). For queries in the hundreds-of-ms range the harness
# IS the measurement. So this script times nothing from the shell: it runs the
# whole suite down ONE mysql connection in ONE kubectl exec, then reads back
# only the audit lines appended since it started.
#
# The audit log also gives ScanRows and ScanBytes per statement, which is the
# real prize — on an append-only table the only read variable is scan economics
# (file count, file size, projection pushdown, partition pruning), and
# ScanBytes is what makes a latency difference attributable instead of asserted.
#
# TWO MEASURED CONSTRAINTS SHAPE EVERY QUERY BELOW. Both were established by
# probe before this file existed; neither is precautionary:
#
#   1. UNBOUNDED AGGREGATES ARE NOT VIABLE HERE. Each table holds ~8 TiB /
#      ~9.7e9 rows and the writers are still running. `SELECT MAX(ingestionTime)
#      FROM <paimon>.cid_events` and a bare `COUNT(*)` both exceeded 5 minutes
#      and had to be killed. Every query here is therefore bounded by `dt`, the
#      sole partition column, so partition pruning can eliminate files wholly.
#      The same predicate bounded to today's partition plus a 5-minute
#      ingestionTime window returned 58.6M rows in 10.0s.
#
#   2. THE FE SESSION TIMEZONE IS Asia/Shanghai AND THE DATA IS UTC. Measured:
#      `SELECT NOW(), UTC_TIMESTAMP()` returns 2026-10-02 22:46:43 /
#      2026-10-02 14:46:43 — eight hours apart. NOW() against a UTC column
#      matches ZERO rows, and a zero-row scan is the fastest possible result, so
#      a timezone slip does not fail the benchmark: it silently reports an empty
#      scan as a win.
#
#      THE HAZARD IS NOW STRUCTURALLY GONE RATHER THAN GUARDED BY THIS COMMENT:
#      no query contains a clock function at all. Every bound is a literal
#      rendered from the LOCAL clock in UTC by bench_resolve_*. Do not reintroduce
#      NOW(), UTC_TIMESTAMP() or CURRENT_DATE into a .sql file.
#
# ── WHERE THE QUERIES LIVE ──────────────────────────────────────────────────
# s3://${BUCKET_NAME}/flink/querybench/{hot,snapshot}/*.sql — one statement per
# file, rendered by that prefix's bench-sql-lib.sh. This script SYNCS them at
# startup (see QUERYBENCH_S3 below) and reads no local directory, so it cannot
# be run against a stale checkout. THE DuckDB HARNESS SYNCS THE SAME PREFIX,
# which is the whole point: the two used to hold independent copies of the same
# queries and silently diverged.
#
# To change a query, edit the editing copy and republish — the S3 objects are
# the deployed artifact, not the source of truth:
#   cd ../querybench-script && vi hot/H2_hot_count_1m.sql && ./bench-sql.sh upload
#
# Print the step list without running anything (local, no AWS needed):
#   SUITE=hot ../querybench-script/bench-sql.sh plan hot
#
# Usage:
#   ./starrocks-cid-audit.sh                      # both suites, 3 arms, WARMUP=1 RUNS=3
#   SUITE=hot ./starrocks-cid-audit.sh            # hot suite only
#   RUNS=5 ./starrocks-cid-audit.sh
#   SNAPSHOT_DT=2026-10-04 ./starrocks-cid-audit.sh   # override yesterday (UTC)
#   SUSPEND_WRITERS=0 ./starrocks-cid-audit.sh    # do not touch the writers
#
# A snapshot-scoped run passes a PRE-FLIGHT GATE first (snapshot-preflight.sh):
# it verifies no cid_events query is already running on the FE, then stops both
# Flink writers and waits for their TaskManagers to disappear. A hot-only run
# does neither. SUSPEND_WRITERS is unset=auto / 0=skip / 1=force. Restarting the
# writers afterwards is MANUAL and no script does it.
#
# Catalogs are created idempotently (filesystem catalog for Paimon, Glue for
# native Iceberg, hadoop for Paimon's Iceberg export, IRSA via
# use_aws_sdk_default_behavior), so there is no separate setup step.

NAMESPACE="${NAMESPACE:-emr-flink}"
FE_POD="${FE_POD:-kube-starrocks-fe-0}"
AUDIT="${AUDIT:-/opt/starrocks/fe/log/fe.audit.log}"
AWS_REGION="${AWS_REGION:-us-west-2}"
PAIMON_CATALOG="${PAIMON_CATALOG:-cid_paimon_catalog}"
PAIMON_DB="${PAIMON_DB:-flink_cid_paimon_db}"
ICEBERG_CATALOG="${ICEBERG_CATALOG:-cid_iceberg_catalog}"
ICEBERG_DB="${ICEBERG_DB:-flink_cid_iceberg_db}"
# ARM 2 — the Paimon table read through the Iceberg metadata Paimon EXPORTS
# (metadata.iceberg.* / PAIMON_ICEBERG_STORAGE). Same data files as arm 1, read
# by StarRocks' NATIVE ICEBERG reader instead of its Paimon reader, which is what
# makes this a three-way comparison of read paths over two formats rather than a
# two-way comparison of engines.
#
# iceberg.catalog.type MUST be hadoop, not glue. Paimon registers its export in
# Glue with PaimonStorageHandler, and StarRocks' Glue-backed Iceberg catalog
# tries to load that handler class and fails. The hadoop catalog resolves the
# table purely from the warehouse path + version-hint.text, which is exactly what
# Paimon writes.
PMNICE_CATALOG="${PMNICE_CATALOG:-cid_pmn_as_ice}"
PMNICE_DB="${PMNICE_DB:-flink_cid_paimon_db}"
PMNICE_WAREHOUSE="${PMNICE_WAREHOUSE:-}"
SUITE="${SUITE:-all}"
RUNS="${RUNS:-3}"
WARMUP="${WARMUP:-1}"
# Hard server-side cap per statement. It exists to stop one pathological query
# from hanging the suite, and it has to be set against the slowest LEGITIMATE
# query or it truncates instead of protects — which is exactly what happened at
# 600 s, where S4_wide_json reported 600,161 ms / State=ERR on every arm and
# every pass: the cap, not a result. That drove a raise to 3600 s.
#
# It was lowered 3600 -> 1800 on 2026-10-04 when S4_wide_json was removed,
# because the slowest remaining query was S3_risk_distinct at 148 s and 3600 s
# was 24x that — a genuine hang would have burned an hour before surfacing.
#
# S4 IS BACK (2026-10-05, see ../querybench-script/snapshot/S4_wide_json.sql), so
# 1800 s is TOO TIGHT AGAIN for
# the snapshot suite: it is ~1.2x S4's slowest measured pass (41 min =
# 2,460 s), i.e. S4 can be truncated by the default rather than protected by
# it. A cap-truncated pass is recorded and the suite CARRIES ON, so it costs a
# data point silently rather than failing loudly.
#
# Hence the default is SUITE-DEPENDENT rather than one number: the two suites'
# slowest legitimate queries are an order of magnitude apart, so a single cap
# cannot be both a protection for one and a non-truncation for the other.
#   hot              — slowest is H5 at 57-86 s, so 1800 s is already ~20x
#                      margin and a lower cap surfaces a genuine hang sooner.
#   all | snapshot   — S4 is in scope at 2,460 s worst observed, so 3600 s
#                      (~1.5x that) is the smallest cap that does not truncate.
# THIS RULE IS DUPLICATED in duckdb-cid/run-duckdb-cid.sh's cid_query_timeout(),
# and must stay identical: a cap that fires on one engine but not the other turns
# the cross-engine comparison into a comparison of the two caps. An exported
# QUERY_TIMEOUT still wins over both.
case "$SUITE" in
    hot) _QUERY_TIMEOUT_DEFAULT=1800 ;;
    *)   _QUERY_TIMEOUT_DEFAULT=3600 ;;
esac
QUERY_TIMEOUT="${QUERY_TIMEOUT:-$_QUERY_TIMEOUT_DEFAULT}"
BUCKET_NAME="${BUCKET_NAME:?BUCKET_NAME is required (source ../.bench-env)}"

# WRITER STATE IS PART OF THE MEASUREMENT. Measured 2026-10-03/04: the SAME
# closed partition, the SAME 944,023,427 rows, read by the SAME DuckDB arm2
# query, took 332,520 ms with the Flink writers live and 3,742 ms with them
# suspended — EIGHTY-NINE TIMES. Concurrent write traffic (two pipelines
# committing ~1,000 files/min) dominates every snapshot number by two orders of
# magnitude, far larger than any format or reader effect this benchmark tries to
# resolve.
#
# That makes a mid-run change of writer state fatal, and it already happened:
# run 2 (2026-10-03T22:31 local) ran snapshot arm1 and arm2 against LIVE writers
# and arm3 after they were suspended, so arm3 would have "won" by ~89x purely on
# timing. The run was discarded. Nothing in the harness noticed, because the
# arms completed normally and all three reported plausible-looking times.
#
# So: record the writer state, and ABORT if it changes between the first and
# last query. The benchmark no longer silently produces a result whose dominant
# variable went unrecorded.
FLINK_JOBS="${FLINK_JOBS:-flink-cid-paimon flink-cid-iceberg}"
# The pre-flight gate lives in snapshot-preflight.sh because
# duckdb-cid/run-duckdb-cid.sh needs exactly the same behaviour, and a stop step
# that differed between the two harnesses would make the cross-engine snapshot
# comparison a comparison of two writer regimes. It honours FLINK_JOBS as its
# deployment list, so one override still moves both scripts.
# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/snapshot-preflight.sh"
writer_state() { snapshot_writer_state; }

# The partition the SNAPSHOT suite reads. MUST be a CLOSED partition.
#
# Computed as YESTERDAY IN UTC (constraint 2 — the data is UTC and the FE
# session is Asia/Shanghai, so a local-time "yesterday" names the wrong
# partition for 8 hours a day). It was previously PINNED to a literal
# 2026-10-01, which went stale: that dt no longer exists in the warehouse, so
# every snapshot query pruned to zero files and returned almost instantly —
# which reads as a spectacular win and measures nothing.
#
# THE YESTERDAY DEFAULT IS ONLY CORRECT WHILE THE WRITERS ARE SUSPENDED, AND BY
# DESIGN THE GENERATOR NEVER WRITES A SINGLE dt. CID_DAY_SPREAD=N draws r_day
# from [0, N] INCLUSIVE (sql-scripts/common/01-datagen-source.sql,
# fields.r_day.min=0 / fields.r_day.max=${CID_DAY_SPREAD:1}) — that is the
# intended semantics, not a bug — so the default N=1 keeps TWO LIVE dt values,
# today AND yesterday, and yesterday is STILL BEING WRITTEN. Measured
# 2026-10-03T10:49Z: dt=2026-10-02 took 370,733,820 rows in the trailing 30
# minutes and its MAX(ingestionTime) equalled today's. This is permanent: a
# snapshot run REQUIRES suspending the writers first, or an explicit
# SNAPSHOT_DT override naming a partition you have confirmed closed.
#
# Reading a live partition as "cold" makes ScanBytes drift between the three
# arms, so the arms stop being comparable — the whole point of the suite.
# VERIFY CLOSURE before trusting a snapshot number: the partition's newest
# object must predate the run. If it does not, export SNAPSHOT_DT=<date>
# explicitly.
#
# ── THE DEFAULT IS THE OLDEST dt IN THE TABLE, NOT YESTERDAY (2026-10-06) ─────
#
# Resolved from Iceberg's $partitions metadata: MIN(partition_value.dt). The
# oldest partition is the one guaranteed CLOSED — the generator's r_day spread
# keeps the NEWEST dt live, so "oldest" and "safe to read cold" are the same
# choice, whereas "yesterday" is only safe by coincidence.
#
# WHY NOT YESTERDAY: the writers ran 2026-10-05 and produced dt={10-04, 10-05}.
# Run the suite on 2026-10-06 and "yesterday" resolves to 2026-10-05 — the
# NEWEST partition, i.e. the wrong one — and on 2026-10-07 it resolves to a dt
# that does not exist at all, which prunes to zero files and returns instantly.
# That reads as a spectacular win and measures nothing. The bug is silent in
# both directions, so the default must come from the DATA, not the clock.
#
# Metadata-only (~1 s), never a table scan. Falls back to yesterday-in-UTC if
# the FE is unreachable or $partitions cannot be served, so a transient failure
# degrades to the old behaviour rather than aborting. Portable across BSD and
# GNU date: macOS has -v-1d and no -d, Linux the reverse.
_oldest_dt() {
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- \
        mysql -h127.0.0.1 -P9030 -uroot -N -B -e \
        "SELECT MIN(partition_value.dt) FROM
         ${ICEBERG_CATALOG}.${ICEBERG_DB}.\`cid_events\$partitions\`" 2>/dev/null \
      | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' | head -1
}
if [[ -z "${SNAPSHOT_DT:-}" ]]; then
    SNAPSHOT_DT=$(_oldest_dt)
    if [[ -n "$SNAPSHOT_DT" ]]; then
        SNAPSHOT_DT_SRC="oldest dt in iceberg \$partitions"
    else
        SNAPSHOT_DT=$(date -u -v-1d +%Y-%m-%d 2>/dev/null \
                      || date -u -d 'yesterday' +%Y-%m-%d)
        SNAPSHOT_DT_SRC="FALLBACK: yesterday-in-UTC (metadata lookup failed)"
    fi
else
    SNAPSHOT_DT_SRC="explicit SNAPSHOT_DT override"
fi

OUT_DIR="${OUT_DIR:-./bench-results}"
mkdir -p "$OUT_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
CSV="${OUT_DIR}/starrocks-cid-audit-${STAMP}.csv"

# PER-RUN MARKER ID — WITHOUT THIS, TWO RUNS' RECORDS MERGE SILENTLY.
#
# Markers used to be MARK_<tag>_<pass>_<qi>, which is unique WITHIN a run but
# identical ACROSS runs. That was survivable only as long as the reader was a
# byte offset that excluded everything prior. Now that audit_read() selects by
# FILE (to survive rotation) it necessarily returns some pre-run content, and
# an aborted or killed earlier run's markers are indistinguishable from this
# run's.
#
# Measured 2026-10-04: a killed 600 s-cap run and its 3600 s replacement both
# wrote MARK_Psnapshot_1_0 etc. into the same day's log. Parsing picked up
# BOTH — S4 appearing as Time=600,161/ERR (the dead run, truncated by the old
# cap) interleaved with Time=1,088,539/EOF (the live run, which completed) —
# i.e. the same slot filled from two different configurations. A median over
# that set is a median over two experiments.
#
# RUN_ID makes every marker globally unique, and the parser requires an exact
# match, so foreign records are dropped rather than averaged in. $$ distinguishes
# two runs started in the same second.
RUN_ID="${RUN_ID:-${STAMP}_$$}"

sr1() { kubectl exec -n "$NAMESPACE" "$FE_POD" -- \
            mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1" 2>/dev/null; }

# ── Queries ─────────────────────────────────────────────────────────────────
#
# THE QUERIES ARE NOT IN THIS FILE ANY MORE (2026-10-06), and they are not in
# this CHECKOUT either: they are synced from S3 (see "WHERE THE QUERIES LIVE" in
# the header) as one .sql file per step, and duckdb-cid/ syncs the SAME objects.
# Read them, and the placeholder contract, in the editing copy:
# ../querybench-script/{hot,snapshot}/ and ../querybench-script/bench-sql-lib.sh.
#
# WHY THEY MOVED. They used to be written twice — heredocs here, Python
# f-strings there — with both copies claiming in their comments to mirror the
# other "one-for-one". On 2026-10-06 they silently stopped doing so: this file
# retired the `uuid % N` snapshot sample and windowed S4, the DuckDB file kept
# the sample on all four queries, and NOTHING failed. Both harnesses ran, both
# printed full tables, both wrote CSVs — of two different experiments. A
# duplicated query set has no mechanism that can catch that.
#
# TWO LOAD-BEARING INVARIANTS ALSO DIED WITH THE HEREDOCS, and that is the other
# half of the reason:
#   - "one statement per line, no blank lines", because the runner paired line N
#     with query index N;
#   - HOT_NAMES / SNAPSHOT_NAMES, separate strings that had to stay positionally
#     aligned with those lines or the audit records were credited to the wrong
#     query.
# Both were enforced by nothing and silently broken by inserting a query
# anywhere but the end. Now the FILENAME is the name and the H<n>/S<n> prefix
# fixes the order under a lexical sort, so there is nothing left to misalign.
#
# `dt` is the ONLY partition column on both sides and both sides were given the
# same CID_DAY_SPREAD, so one rendered dt literal names the same partition in
# all three catalogs. That is what makes these queries portable across the arms.
#
# HOT suite — rows written in the last few minutes.
#
# On an append-only table "hot" does NOT mean merge-on-read cost: there is no
# primary key, no changelog, no delete file and no deletion vector on either
# side (asserted below — the counts MUST read 0). What differs is purely how
# each format's newest files are laid out and described:
#
#   Paimon  writes data files per bucket, one set per checkpoint, flushed at
#           the barrier (write-buffer-spillable=true) and coalesced inline by
#           the writer (write-only=false), so the hot window's file layout is
#           whatever that interval plus inline compaction produced.
#   Iceberg writes one data file per writer subtask per commit, listed in the
#           manifest for that snapshot.
#
# So the hot comparison is a file-count-and-metadata comparison, and ScanBytes
# is what shows whether a format read only the hot files or swept wider.
#
# THE HOT WINDOWS ARE NOW PINNED LITERALS, NOT UTC_TIMESTAMP(). They used to be
# computed in SQL as DATE_SUB(UTC_TIMESTAMP(), INTERVAL 5 MINUTE), evaluated per
# statement — and the arms run SERIALLY, so arm3's window opened after arm1's
# had closed. This file's own header used to admit it: "the second format sees
# one suite's duration of extra rows." A ratio between two different row sets is
# not a format result. bench_resolve_hot_windows() renders the bounds ONCE per
# invocation, so all three arms and all passes read the same rows, and the
# windows are half-open so their width is a constant. It also removes the
# Asia/Shanghai-session hazard outright: no query contains a clock function.
#
# ── THE QUERIES COME FROM S3, NOT FROM A SIBLING DIRECTORY (2026-10-06) ──────
# Synced from ${QUERYBENCH_S3} into a per-run temp dir below. Published by
# querybench-script/bench-sql.sh upload, and the DuckDB harness syncs the SAME
# prefix, so "both engines ran the same queries" is a property of one S3 object
# set rather than of two checkouts happening to agree. It also means this script
# has no opinion about where the repo is laid out: nothing here checks for a
# local directory, so a rename or a run from another machine cannot break it.
QUERYBENCH_S3="${QUERYBENCH_S3:-s3://${BUCKET_NAME}/flink/querybench}"
QUERYBENCH_S3="${QUERYBENCH_S3%/}"
# A FRESH TEMP DIR PER RUN, and an inherited BENCH_SQL_DIR is deliberately
# IGNORED. Reusing a directory would let a previously-synced query survive its
# deletion from S3 — the one failure `sync --delete` exists to prevent — and the
# per-run directory is also what makes the published object set, not a leftover
# local copy, the thing a result is attributable to. Point QUERYBENCH_S3 at
# another prefix to test a candidate suite.
BENCH_SQL_DIR="$(mktemp -d "${TMPDIR:-/tmp}/querybench.XXXXXX")"
export BENCH_SQL_DIR
trap 'rm -rf "$BENCH_SQL_DIR"' EXIT

echo "Syncing query suites from ${QUERYBENCH_S3}/ ..."
aws s3 sync "${QUERYBENCH_S3}/" "${BENCH_SQL_DIR}/" --region "$AWS_REGION" \
    --exclude '*' --include 'bench-sql-lib.sh' \
    --include 'hot/*.sql' --include 'snapshot/*.sql' --only-show-errors || {
    echo "ERROR: cannot sync the query suites from ${QUERYBENCH_S3}/" >&2
    echo "  Publish them first:  (cd ../querybench-script && ./bench-sql.sh upload)" >&2
    exit 1; }

# Asserted, not assumed. `aws s3 sync` of an EMPTY prefix succeeds with no
# output and exit 0, which would leave an empty suite — and an empty suite is
# indistinguishable from a suite whose queries all returned instantly.
_qb_hot=$(ls -1 "${BENCH_SQL_DIR}/hot"/*.sql 2>/dev/null | grep -c .)
_qb_snap=$(ls -1 "${BENCH_SQL_DIR}/snapshot"/*.sql 2>/dev/null | grep -c .)
if [[ ! -f "${BENCH_SQL_DIR}/bench-sql-lib.sh" ]] || (( _qb_hot == 0 || _qb_snap == 0 )); then
    echo "ERROR: ${QUERYBENCH_S3}/ is missing or incomplete" >&2
    echo "  got: bench-sql-lib.sh=$([[ -f "${BENCH_SQL_DIR}/bench-sql-lib.sh" ]] && echo yes || echo NO)" \
         "hot=${_qb_hot} snapshot=${_qb_snap}" >&2
    echo "  Publish it:  (cd ../querybench-script && ./bench-sql.sh upload)" >&2
    exit 1
fi
echo "  ✓ ${_qb_hot} hot + ${_qb_snap} snapshot queries -> ${BENCH_SQL_DIR}"

# shellcheck disable=SC1091
source "${BENCH_SQL_DIR}/bench-sql-lib.sh" || exit 1

# SNAPSHOT suite — one whole dt partition, i.e. cold/accumulated data.
#
# Bounded to a CLOSED partition, named by SNAPSHOT_DT above — a shell-side
# literal rather than an in-SQL DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY), so
# every arm and every pass reads the SAME dt even if the suite runs across
# midnight, and so the date is overridable when yesterday is not closed (see
# SNAPSHOT_DT: while the writers run, yesterday is one of the two live dt
# partitions by design, so the "cold" queries race the writers and ScanBytes
# drifts between arms).
#
# This is where COMPACTION STATE SHOWS UP. A format whose partition is still
# thousands of ~90 MiB files pays more open/seek overhead for the same bytes.
#
# S4_wide_json IS THE EXPENSIVE ONE, AND IT IS DELIBERATELY BACK (2026-10-05).
#
# It is a full-partition decode of the fat JSON column over ~944 M rows
# (~1.08 TiB scanned): the only query in either suite that reads event_json
# across a whole closed partition rather than a trailing window. That is
# exactly why it is here — it is where compaction state and fat-column decode
# cost compound, and H5_hot_wide_json (5-minute window, 57-86 s) does not
# reach that scale.
#
# IT DOMINATES THE SUITE'S WALL CLOCK. Measured: 18-41 min per pass on
# StarRocks and 25.7 min on DuckDB, i.e. ~3.6 h of a ~5 h run for ONE query.
# It was removed on 2026-10-04 for that reason and restored by request.
# Budget for it, and prefer SUITE=hot when you only need a quick signal.
#
# QUERY_TIMEOUT is LOAD-BEARING for this query specifically: it defaults to
# 3600 s whenever the snapshot suite is in scope (SUITE=all or SUITE=snapshot)
# precisely because of S4. 1800 s is only ~1.2x S4's slowest measured pass
# (41 min = 2,460 s), so S4 CAN TIME OUT AT THE OLD DEFAULT. A cap-truncated
# pass logs Time=<cap>/ERR, which the parser prints as ERR rather than a number.
#
# enable_spill = true is ALSO pinned below, but NOT for S4 — see the rationale
# at that statement. S4's full-partition OOM is scan-side (read and decompress
# page / get chunk from scanner) and spill cannot reach it.
#
# ── THE BOUND: dt ALONE FOR S1-S3, AN ingestionTime WINDOW FOR S4 ────────────
#
# The SQL is in ../querybench-script/snapshot/*.sql and the window arithmetic in
# bench_resolve_snapshot_window(); each file documents its own bound. What
# belongs here is the MEASURED history, because it is the reason the bound is
# not uniform across the four queries.
#
# A uuid MODULO SAMPLE WAS USED UNTIL 2026-10-06 and is gone:
#
#     AND CONV(SUBSTRING(uuid,1,8),16,10) % ${N} = 0   <- uniform 1/N sample
#
# It was UNBIASED — uuid is MD5(random BIGINT) in UUIDv7 shape
# (sql-scripts/common/01-datagen-source.sql:298-300,345,379-381), so the leading
# hex block is uniformly distributed. Measured at N=1000 on dt=2026-10-04,
# serial on an idle cluster, all three arms landed within 0.03% of the ideal
# 1/N and the iceberg/paimon ratio (1.2994) reproduced the full-partition row
# ratio (1.2993) to four decimals.
#
# BUT IT COULD NOT PRUNE, which is why removing it was free. uuid is random, so
# Parquet row-group min/max spans the whole keyspace in every file and NOTHING
# is eliminated: measured at N=1000, ScanRows equalled each arm's EXACT full
# partition count — 3,217,389,927 / 3,217,389,927 / 4,180,412,213. The sample
# read every byte and discarded 999/1000 of the rows at the aggregate. Dropping
# it left ScanRows unchanged and made S1-S3 honest partition aggregates instead
# of full scans reporting a sample to be scaled back up.
#
# S4 NEEDED A PRUNING PREDICATE, NOT A SMALLER AGGREGATE. Its decode happens
# during the scan, before any filter, so the sample never touched its cost:
# 18-41 min per pass per arm, and in the cancelled 2026-10-06 run 1,513 s on
# arm1 alone with a ~12 h projection for the full matrix. ingestionTime DOES
# prune (monotonic with write order, tight row-group stats), so S4 alone carries
# a 30-minute window. See ../querybench-script/snapshot/S4_wide_json.sql — in
# particular, the window is anchored to the INGESTION date, which is NOT
# SNAPSHOT_DT.
#
# ── THE SUITE IS EXPENSIVE AND CANNOT BE MADE CHEAP ──────────────────────────
# S1-S3 are a full-partition scan per arm, per pass. Measured serial wall-clock:
# paimon 267 s, pmn-as-ice 245 s, iceberg 365 s. Budget ~15 min per pass across
# three arms and KEEP QUERY_TIMEOUT AT 3600 s.
#
# RUN THE ARMS SERIALLY. Three concurrent full-partition scans EXHAUST the BE
# memory pool: measured 2026-10-06, all three arms aborted with
#   "Memory of default_wg exceed limit ... Used: 18786279880, Limit: 18786186952"
# on kube-starrocks-be-0. The harness already does; do not parallelise them.
#
# ── ROWS ARE NOT EQUAL ACROSS ARMS, BY CONSTRUCTION ──────────────────────────
# Iceberg holds 1.2993x paimon's rows in this partition (4,180,412,213 vs
# 3,217,389,927) because paimon's write path is ~23% slower at equal uptime —
# the bucketed hash shuffle, see
# bench-results/FLINK-THROUGHPUT-ANALYSIS-20261005.md. RAW ELAPSED IS THEREFORE
# NOT COMPARABLE ACROSS ARMS; divide by S1's COUNT(*) per arm.
#
# Per-row scan cost (N=1000 era, serial and uncontended — still the cleanest
# read-path numbers measured, since the sample never changed scan volume):
#
#   arm          ScanBytes   B/row   CpuCostNs    ns/row
#   paimon       122.3 GB    38.02   1,426.7 B    443.4
#   pmn-as-ice   120.2 GB    37.37   1,398.2 B    434.6
#   iceberg      176.4 GB    42.19   1,966.3 B    470.4
#
# i.e. iceberg costs 1.11x paimon's bytes/row and 1.06x its CPU/row. Earlier
# CONCURRENT measurements suggested a 6.5x bytes gap; those are NOT RELIABLE —
# contention and differing pruning made the arms non-comparable. Trust serial
# full-scan numbers only.

# Resolve the windows ONCE, here, for every arm and pass. bench_resolve_* does
# the BSD/GNU date juggling and the shape validation (a malformed bound reaches
# the SQL as a string literal that matches zero rows and returns instantly,
# which reads as a win) — see ../querybench-script/bench-sql-lib.sh.
bench_resolve_hot_windows || exit 1
bench_resolve_snapshot_window "$SNAPSHOT_DT" || exit 1

# StarRocks dialect: the table is reached by name after SET CATALOG/USE, and
# LENGTH is the BYTE-length function. (DuckDB renders the same files with
# STRLEN — its LENGTH counts characters.)
export BENCH_TABLE="cid_events"
export BENCH_BYTES_FN="LENGTH"

# suite_sql <hot|snapshot> — one statement per line, in file order.
#
# The one-statement-per-line shape is still what the runner consumes, but it is
# now PRODUCED from the .sql files rather than hand-maintained, and the names
# come from the filenames rather than from a parallel string that had to stay
# positionally aligned with them.
suite_sql() {
    local f
    while IFS= read -r f; do
        bench_render_oneline "$f" || return 1
    done < <(bench_query_files "$1")
}

suite_names() {
    local f out=""
    while IFS= read -r f; do
        out="${out}$(bench_query_name "$f") "
    done < <(bench_query_files "$1")
    echo "${out% }"
}

HOT_NAMES="$(suite_names hot)"      || exit 1
SNAPSHOT_NAMES="$(suite_names snapshot)" || exit 1

# Render everything up front so a template error aborts BEFORE the writers are
# stopped and the catalogs touched, rather than twenty minutes in. The rendered
# text is reused verbatim by build_script, so what is validated here is exactly
# what runs.
HOT_SQL_TEXT=""; SNAPSHOT_SQL_TEXT=""
if [[ "$SUITE" == all || "$SUITE" == hot ]]; then
    HOT_SQL_TEXT="$(suite_sql hot)" || { echo "ERROR: hot suite failed to render" >&2; exit 1; }
fi
if [[ "$SUITE" == all || "$SUITE" == snapshot ]]; then
    SNAPSHOT_SQL_TEXT="$(suite_sql snapshot)" || { echo "ERROR: snapshot suite failed to render" >&2; exit 1; }
fi

audit_size() { kubectl exec -n "$NAMESPACE" "$FE_POD" -- stat -c %s "$AUDIT" 2>/dev/null; }

# THE AUDIT LOG ROTATES DAILY AND THAT SILENTLY DESTROYED A RUN.
#
# The original method captured a byte offset before the run (OFF=$(audit_size))
# and afterwards read `tail -c +$((OFF+1)) fe.audit.log`. That is only correct
# while the file it measured keeps growing. The FE rolls the log at midnight to
# fe.audit.log.<YYYYMMDD>-<n> and starts a fresh, EMPTY fe.audit.log — so after
# a rollover the live file is SMALLER than OFF and `tail -c +OFF` returns
# nothing at all.
#
# Measured 2026-10-04: a snapshot run started 23:21 local, the log rolled at
# 23:59:55, and the run's records ended up split 39 in fe.audit.log.20261003-1
# and 9 in the new fe.audit.log, with OFF (~780 KB) now past the end of a 23 KB
# file. The parse would have produced ZERO records and every arm would have
# reported "-" — or worse, a partial read scoring arms on different subsets.
#
# Fix: do not trust byte offsets across a rotation. Concatenate the live log
# with any sibling rotated files whose mtime is at/after the run start, in
# chronological order, and let the MARK_ markers do the splitting — they are
# already unique per (tag,pass,query) and already the only thing the parser
# keys on. A newer-than-start filter keeps this cheap: old days' logs are
# skipped without being read.
#
# RUN_TAG makes the slice unambiguous even if two runs share a day: every
# marker in this run carries the per-suite tag, and the parser ignores lines it
# cannot attribute, so extra leading context is harmless.
audit_read() {
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- bash -c '
        set -e
        log="$1"; start="$2"
        dir=$(dirname "$log"); base=$(basename "$log")
        # -newermt needs a timestamp the shell inside the pod understands; the
        # FE pod is the only clock that matters here, which is why START_STAMP
        # is captured with `date` IN THE POD rather than locally.
        for f in $(find "$dir" -maxdepth 1 -name "${base}.*" -newermt "$start" \
                     2>/dev/null | sort); do
            cat "$f"
        done
        [ -f "$log" ] && cat "$log"
    ' _ "$AUDIT" "$1" 2>/dev/null
}

# Emit the SQL script: a literal marker SELECT before every query so the audit
# log can be split. SQL COMMENTS CANNOT BE USED for this — the audit log's Stmt
# field is stripped of them (`/*BENCH:Q1*/ SELECT ...` logs as plain
# `SELECT ...`), whereas `SELECT 'MARK_...'` is logged verbatim.
build_script() {
    local catalog=$1 db=$2 suite=$3 tag=$4
    echo "SET CATALOG ${catalog};"
    echo "USE ${db};"
    echo "SET query_timeout = ${QUERY_TIMEOUT};"
    # DISABLE THE BE LOCAL DATA CACHE. Not optional, and not a precaution —
    # measured 2026-10-03, leaving it on silently destroyed the snapshot suite.
    #
    # The snapshot suite reads a FIXED closed partition, so pass 0 populates the
    # BEs' local cache and passes 1-3 are served from it. The audit record then
    # shows the full ScanRows (944,023,427) with ScanBytes=0 — zero REMOTE bytes
    # — and Time collapsing from 70,036 ms to 2,103 ms. The median of passes 1-3
    # is therefore a cache-hit latency reported as a format result, and because
    # all three arms cache equally it looks plausible rather than broken. Worse,
    # ScanBytes=0 disables the one attribution check this benchmark has.
    #
    # The HOT suite does not have this problem — its 5-minute trailing window
    # names different files every pass — but disabling the cache uniformly keeps
    # the two suites comparable and costs the hot suite nothing.
    echo "SET enable_scan_datacache = false;"
    echo "SET enable_datacache_sharing = false;"
    echo "SET populate_datacache_mode = 'never';"
    # Spill is ON, unconditionally — the ENABLE_SPILL knob was removed on
    # 2026-10-05 along with the other alternative-configuration arms. The query
    # that forced it was the since-removed S4 (the full-partition fat-JSON
    # aggregate), which returned MEM_LIMIT_EXCEEDED on ALL THREE ARMS without it
    # — measured 2026-10-03 at 163-335 s before aborting. An all-arm failure is
    # an honest result but a useless measurement: it reports ERR for every arm
    # and compares nothing.
    #
    # Kept after S4's removal rather than reverted. H5_hot_wide_json is the same
    # shape of aggregate (70 GB scanned) and S3_risk_distinct carries a
    # COUNT(DISTINCT) over 944 M rows, so the memory ceiling is still reachable.
    # Because the spill cost applies equally to all three arms it cannot bias a
    # format, which is what makes it safe to pin rather than sweep.
    echo "SET enable_spill = true;"
    # ── S4's OOM IS SCAN-SIDE, AND enable_spill ABOVE CANNOT REACH IT ─────────
    # The failure names `read and decompress page` / `get chunk from scanner`:
    # the memory is held by the scanner's in-flight IO tasks, and spill covers
    # aggregate, sort and join operators only. Capping the per-operator IO task
    # count bounds how many compressed pages are resident at once.
    #
    # BOTH SETs ARE REQUIRED. Without the second, the adaptive scheduler raises
    # the task count back up at runtime and the first has no observable effect.
    #
    # Applied to the snapshot suite only: it costs scan parallelism, and the hot
    # suite's 5-minute window never approached the ceiling. Uniform across all
    # three arms, so it cannot bias a format.
    if [[ "$suite" == snapshot ]]; then
        echo "SET connector_io_tasks_per_scan_operator = 2;"
        echo "SET enable_connector_adaptive_io_tasks = false;"
    fi
    # One MARK_ marker per statement, so the audit parser can attribute each
    # record to a (tag,pass,query-index) triple. The statements come from
    # ${BENCH_SQL_DIR}/<suite>/*.sql rendered one-per-line, in the same lexical
    # order the report loop derives its names from — so index qi and
    # bench_query_name are two readings of one list, not two lists to keep
    # aligned by hand.
    local pass qi line suite_text
    if [[ "$suite" == snapshot ]]; then suite_text="$SNAPSHOT_SQL_TEXT"; else suite_text="$HOT_SQL_TEXT"; fi
    for (( pass = 0; pass < WARMUP + RUNS; pass++ )); do
        qi=0
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            echo "SELECT 'MARK_${RUN_ID}_${tag}_${pass}_${qi}';"
            echo "$line"
            qi=$(( qi + 1 ))
        done <<< "$suite_text"
    done
}

# --force so one statement's error does not abort the remaining passes; -N -B
# keeps the result payload small (it is never read — timing comes from the log).
run_suite_sql() {
    local catalog=$1 db=$2 suite=$3 tag=$4
    # No per-arm bound any more: S1-S3 carry dt alone and S4 carries the same
    # ingestionTime window on every arm, so there is nothing catalog-dependent
    # left to resolve here. (The previous scheme computed a per-arm cutoff and
    # scoped it to this function so one arm's value could not leak into the
    # next; with an identical predicate everywhere that risk is gone.)
    build_script "$catalog" "$db" "$suite" "$tag" \
      | kubectl exec -i -n "$NAMESPACE" "$FE_POD" -- \
          mysql -h127.0.0.1 -P9030 -uroot -N -B --force >/dev/null 2>&1
}

# ── Catalogs (idempotent) ───────────────────────────────────────────────────
echo "Ensuring StarRocks catalogs exist..."
sr1 "CREATE EXTERNAL CATALOG IF NOT EXISTS ${PAIMON_CATALOG} PROPERTIES (
    \"type\" = \"paimon\",
    \"paimon.catalog.type\" = \"filesystem\",
    \"paimon.catalog.warehouse\" = \"s3://${BUCKET_NAME}/cid-warehouse-paimon/\",
    \"aws.s3.region\" = \"${AWS_REGION}\",
    \"aws.s3.use_aws_sdk_default_behavior\" = \"true\"
)" >/dev/null
sr1 "CREATE EXTERNAL CATALOG IF NOT EXISTS ${ICEBERG_CATALOG} PROPERTIES (
    \"type\" = \"iceberg\",
    \"iceberg.catalog.type\" = \"glue\",
    \"aws.glue.region\" = \"${AWS_REGION}\",
    \"aws.glue.use_aws_sdk_default_behavior\" = \"true\",
    \"aws.s3.region\" = \"${AWS_REGION}\",
    \"aws.s3.use_aws_sdk_default_behavior\" = \"true\"
)" >/dev/null
# Arm 2. Default warehouse is Paimon's export root: the Paimon warehouse plus the
# iceberg/ subdirectory Paimon writes its exported metadata into. Resolved here
# rather than at the top so BUCKET_NAME is already validated.
: "${PMNICE_WAREHOUSE:=s3://${BUCKET_NAME}/cid-warehouse-paimon/iceberg/}"
sr1 "CREATE EXTERNAL CATALOG IF NOT EXISTS ${PMNICE_CATALOG} PROPERTIES (
    \"type\" = \"iceberg\",
    \"iceberg.catalog.type\" = \"hadoop\",
    \"iceberg.catalog.warehouse\" = \"${PMNICE_WAREHOUSE}\",
    \"aws.s3.region\" = \"${AWS_REGION}\",
    \"aws.s3.use_aws_sdk_default_behavior\" = \"true\"
)" >/dev/null

# Fail fast if any arm cannot be read at all, rather than producing a CSV of ERR
# rows after twenty minutes.
for spec in "${PAIMON_CATALOG}.${PAIMON_DB}" \
            "${PMNICE_CATALOG}.${PMNICE_DB}" \
            "${ICEBERG_CATALOG}.${ICEBERG_DB}"; do
    if ! sr1 "SELECT uuid FROM ${spec}.cid_events LIMIT 1" | grep -q .; then
        echo "ERROR: cannot read a row from ${spec}.cid_events" >&2
        echo "  Check the catalog properties and that the writers have committed." >&2
        exit 1
    fi
done

# ── Writer regime ───────────────────────────────────────────────────────────
# Run BEFORE the header, so the writers line below — and therefore the CSV
# header derived from it — records the POST-gate state. An artifact that says
# "running" while the suite actually ran against stopped writers is worse than
# no provenance at all.
#
# Placed AFTER the per-arm read checks on purpose: those fail in seconds, and
# waiting for the TaskManagers to drain can take minutes, so there is no point
# taking the writers down for a run that is about to abort on an unreadable
# catalog.
#
# THE TWO SUITES NEED OPPOSITE WRITER STATES, so SUITE=all is always wrong in
# one of them:
#   hot      — needs writers RUNNING. Its predicates are trailing windows
#              (ingestionTime >= now - 5m). With the writers stopped they match
#              ZERO rows, and a zero-row scan is the fastest possible result, so
#              the suite reports a silent false win instead of failing. The
#              pre-flight gate therefore NEVER fires for SUITE=hot on its own.
#   snapshot — needs writers SUSPENDED. Concurrent write traffic costs ~89x
#              (measured), which buries the 1.1-1.5x format effect entirely, and
#              at CID_DAY_SPREAD=1 yesterday is a LIVE partition, so its file
#              set moves between arms while the suite runs.
# Warn rather than abort on SUITE=all: it is still useful for a smoke test, and
# the per-suite guard below plus the CSV header record which regime each ran in.
if [[ "$SUITE" == all ]]; then
    echo ""
    echo "  ⚠ SUITE=all: the hot suite needs writers RUNNING and the snapshot"
    echo "    suite needs them SUSPENDED, so one of the two is being measured in"
    echo "    the wrong regime. Run them separately for quotable numbers."
fi
snapshot_preflight "$SUITE" || exit 1

echo "═══════════════════════════════════════════════════════════════════════"
echo "  StarRocks cid_events read benchmark — server-side Time from FE audit"
echo "═══════════════════════════════════════════════════════════════════════"
echo "  starrocks  : $(sr1 'SELECT CURRENT_VERSION()' | head -1)"
echo "  fe tz      : $(sr1 "SHOW VARIABLES LIKE 'time_zone'" | awk '{print $2}')  (irrelevant: no query contains a clock function)"
echo "  queries    : ${QUERYBENCH_S3}/{hot,snapshot}/*.sql — shared verbatim with the DuckDB harness"
echo "  arm1 paimon  : ${PAIMON_CATALOG}.${PAIMON_DB}.cid_events        (paimon reader)"
echo "  arm2 pmn→ice : ${PMNICE_CATALOG}.${PMNICE_DB}.cid_events        (iceberg reader, paimon's exported metadata)"
echo "  arm3 iceberg : ${ICEBERG_CATALOG}.${ICEBERG_DB}.cid_events       (iceberg reader, native table)"
echo "  suite      : ${SUITE}   runs: ${RUNS} (+${WARMUP} warmup, discarded)"
echo "  snapshot dt: ${SNAPSHOT_DT} (closed by the pre-flight gate above; source printed below)"
echo "  timeout    : ${QUERY_TIMEOUT}s per statement (server-side)"
WRITERS_AT_START="$(writer_state)"
echo "  writers    : ${WRITERS_AT_START}  (SUSPEND_WRITERS=${SUSPEND_WRITERS:-auto}${CID_WRITERS_STOPPED:+, stopped by this run})"
echo "               ^ THE dominant variable for the snapshot suite (89x). It is"
echo "                 recorded here and re-checked after every suite; a change aborts."
echo "═══════════════════════════════════════════════════════════════════════"

# ── Table state ─────────────────────────────────────────────────────────────
# Captured BEFORE timing. Row counts here are partition-bounded for the same
# reason the suite is (constraint 1); an unbounded COUNT(*) would not return.
#
# The delete-artifact counts are an ASSERTION, not a cost explanation: an
# append-only pipeline must never produce an Iceberg positional delete or a
# Paimon deletion vector. A non-zero value means something wrote an update or a
# delete and the benchmark's premise is broken.
echo ""
echo "-- Table state ------------------------------------------------------------"
TODAY=$(python3 -c "import datetime;print(datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d'))")
printf '  partitions under test: hot=%s (live)  snapshot=%s (closed)\n' "$TODAY" "$SNAPSHOT_DT"
# THESE WERE THREE FULL COLD SCANS AND THEY COST MORE THAN THE BENCHMARK'S
# FIRST QUERY (fixed 2026-10-05).
#
# `SELECT COUNT(*) ... WHERE dt='<today>'` is partition-bounded, which was the
# original reasoning, but a bounded count over a 3.2 BILLION row partition is
# still a full scan of that partition: there is no COUNT(*) metadata shortcut on
# an external catalog, the suite disables the datacache, and with the writers
# suspended every byte is a cold remote read. Measured: the preamble alone
# consumed the entire window of a run and the FE was still executing
# `COUNT(*) WHERE dt='2026-10-05'` when the run was killed — before a single
# timed query had been issued. It also counted the WRONG partition for the
# snapshot suite (today, not SNAPSHOT_DT).
#
# Read it from table metadata instead. `$partitions` carries record_count per
# partition, which is exact, O(metadata), and available on both catalog types —
# and it reports the partition each suite actually reads. The Paimon reader
# cannot serve `cid_events$partitions` (it throws "Failed to find latest
# snapshot id"), so that arm falls back to SHOW PARTITIONS, which works.
# partition_value is a STRUCT<dt varchar>, so it must be addressed by FIELD.
# `LIKE '%<dt>%'` fails analysis ("STRUCT type couldn't as Predicate") and so
# does CAST(... AS VARCHAR) ("Invalid type cast from struct") — the only form
# that binds is partition_value.dt.
#
# The SHOW PARTITIONS fallback must be split on TABS, not whitespace: its
# VisibleVersionTime column contains a space ("2026-10-04 22:11:02"), so a
# default-FS awk shifts every later field and $5 returns the partition KEY
# instead of RowCount. Field 5 of 7 is RowCount.
_dt_rows_meta() {   # $1=catalog $2=db $3=dt
    local v
    v=$(sr1 "SELECT record_count FROM ${1}.${2}.\`cid_events\$partitions\`
             WHERE partition_value.dt='${3}'" 2>/dev/null | head -1)
    [[ -n "$v" ]] && { echo "$v"; return 0; }
    sr1 "SHOW PARTITIONS FROM ${1}.${2}.cid_events" 2>/dev/null \
      | awk -F'\t' -v d="dt=${3}" '$1==d {print $5; exit}'
}
# The partition each suite reads: the snapshot suite is scored on SNAPSHOT_DT,
# so reporting only today's rows told us nothing about the data under test.
STATE_DT="$SNAPSHOT_DT"; [[ "$SUITE" == hot ]] && STATE_DT="$TODAY"
P_TODAY=$(_dt_rows_meta "$PAIMON_CATALOG"  "$PAIMON_DB"  "$STATE_DT")
X_TODAY=$(_dt_rows_meta "$PMNICE_CATALOG"  "$PMNICE_DB"  "$STATE_DT")
I_TODAY=$(_dt_rows_meta "$ICEBERG_CATALOG" "$ICEBERG_DB" "$STATE_DT")
X_FILES=$(sr1 "SELECT COUNT(*) FROM ${PMNICE_CATALOG}.${PMNICE_DB}.\`cid_events\$files\`" | head -1)
X_SNAPS=$(sr1 "SELECT COUNT(*) FROM ${PMNICE_CATALOG}.${PMNICE_DB}.\`cid_events\$snapshots\`" | head -1)
I_FILES=$(sr1 "SELECT COUNT(*) FROM ${ICEBERG_CATALOG}.${ICEBERG_DB}.\`cid_events\$files\`" | head -1)
I_SNAPS=$(sr1 "SELECT COUNT(*) FROM ${ICEBERG_CATALOG}.${ICEBERG_DB}.\`cid_events\$snapshots\`" | head -1)
printf '  %-10s rows(dt=%s)=%-16s\n' paimon "$STATE_DT" "${P_TODAY:-?}"
printf '  %-10s rows(dt=%s)=%-16s files=%-8s snapshots=%s\n' pmn-as-ice "$STATE_DT" "${X_TODAY:-?}" "${X_FILES:-?}" "${X_SNAPS:-?}"
printf '  %-10s rows(dt=%s)=%-16s files=%-8s snapshots=%s\n' iceberg "$STATE_DT" "${I_TODAY:-?}" "${I_FILES:-?}" "${I_SNAPS:-?}"
# ── What is about to run, step by step ──────────────────────────────────────
# bench_print_plan prints the bound AND the ordered step list with each query's
# one-line purpose, harvested from the .sql files themselves. Printed before any
# timing so a log always carries the exact window its numbers were measured
# under, rather than leaving it to be reconstructed from S1 afterwards.
echo ""
for s in $( [[ "$SUITE" == all || "$SUITE" == hot      ]] && echo hot
            [[ "$SUITE" == all || "$SUITE" == snapshot ]] && echo snapshot ); do
    bench_print_plan "$s"
done
if [[ "$SUITE" != hot ]]; then
    printf '  dt source  : %s\n' "${SNAPSHOT_DT_SRC:-unknown}"
    printf '  ⚠ iceberg holds ~1.30x paimon rows in this partition, so raw elapsed\n'
    printf '    is NOT comparable across arms. Divide by each arm ScanRows.\n'
    echo ""
fi

# ── Run ─────────────────────────────────────────────────────────────────────
# Captured from the FE POD's clock, not the local one: the pod is Asia/Shanghai
# while the workstation may be anything, and `find -newermt` runs inside the pod.
# A one-minute backdate absorbs clock granularity and the gap between this stamp
# and the first query, at the cost of including a little pre-run context the
# marker-keyed parser discards anyway.
START_STAMP=$(kubectl exec -n "$NAMESPACE" "$FE_POD" -- \
                date -d '1 minute ago' '+%Y-%m-%d %H:%M:%S' 2>/dev/null)
[[ -z "$START_STAMP" ]] && { echo "ERROR: cannot read clock in ${FE_POD}" >&2; exit 1; }
# Still assert the log is reachable — the old OFF capture doubled as that check.
[[ -z "$(audit_size)" ]] && { echo "ERROR: cannot stat ${AUDIT} in ${FE_POD}" >&2; exit 1; }

RUN_SUITES=""
[[ "$SUITE" == all || "$SUITE" == hot      ]] && RUN_SUITES="hot"
[[ "$SUITE" == all || "$SUITE" == snapshot ]] && RUN_SUITES="${RUN_SUITES} snapshot"
[[ -z "${RUN_SUITES// /}" ]] && { echo "ERROR: SUITE must be all|hot|snapshot" >&2; exit 1; }

# Interleaved per FORMAT, not per query, because the audit-log method needs one
# connection per suite. The writers are live, so the second format sees one
# suite's duration of extra rows; that is why rows(today) is captured above and
# why the hot queries are windowed rather than cumulative — a fixed-width
# trailing window does not grow with elapsed time the way COUNT(*) does.
for s in $RUN_SUITES; do
    echo ""
    echo "  running ${s} suite: arm1 paimon..."
    run_suite_sql "$PAIMON_CATALOG"  "$PAIMON_DB"  "$s" "P${s}"
    echo "  running ${s} suite: arm2 paimon-as-iceberg..."
    run_suite_sql "$PMNICE_CATALOG"  "$PMNICE_DB"  "$s" "X${s}"
    echo "  running ${s} suite: arm3 iceberg..."
    run_suite_sql "$ICEBERG_CATALOG" "$ICEBERG_DB" "$s" "I${s}"
    # Per SUITE, not just once at the end: the arms inside a suite are what get
    # ratioed against each other, so a state change between arm1 and arm3 is the
    # case that must not survive. Checking only after the last suite would let a
    # corrupted hot suite through on the strength of a stable snapshot suite.
    WRITERS_NOW="$(writer_state)"
    if [[ "$WRITERS_NOW" != "$WRITERS_AT_START" ]]; then
        echo "" >&2
        echo "ABORT: Flink writer state changed DURING the ${s} suite." >&2
        echo "  at start: ${WRITERS_AT_START}" >&2
        echo "  now:      ${WRITERS_NOW}" >&2
        echo "" >&2
        echo "Writer traffic is worth ~89x on snapshot queries, so the arms in" >&2
        echo "this suite are not comparable: whichever ran after the change wins" >&2
        echo "on timing alone. This is exactly how run 2 was silently corrupted." >&2
        echo "No CSV written. Re-run with the writers held in ONE state." >&2
        exit 1
    fi
done

# The FE appends audit records asynchronously after returning the result, so the
# last statement of the last suite can still be in flight here.
sleep 4

RAW=$(audit_read "$START_STAMP")

# ── Parse ───────────────────────────────────────────────────────────────────
# Walk the lines in order. A MARK_<tag>_<pass>_<qi> statement arms the next real
# query; only the FIRST query after a marker is credited, so an unexpected extra
# statement cannot be silently attributed to the wrong slot.
PARSED=$(printf '%s\n' "$RAW" | RUN_ID="$RUN_ID" python3 -c '
import sys, re, os
# Only THIS run s markers count. audit_read() reads whole files to survive
# rotation, so the slice legitimately contains other runs records; an earlier
# killed run wrote the same (tag,pass,qi) triples under a different RUN_ID and
# must be dropped, not averaged in.
RUN_ID = re.escape(os.environ["RUN_ID"])
cur = None
out = []
for line in sys.stdin:
    # The FE logs a SLOW query TWICE — once tagged [slow_query] and once
    # [query] — with identical Time/ScanRows/ScanBytes. Keeping both would put
    # two records in the same (tag,pass,qi) slot, and since only the FIRST
    # query after a marker is credited, the duplicate would shift every
    # subsequent query one slot to the left and silently mis-attribute the
    # whole suite. Drop the [slow_query] copy; [query] is emitted for every
    # statement, fast or slow.
    if "[slow_query]" in line:
        continue
    m = re.search(r"\|Stmt=(.*?)\|Digest=", line, re.S)
    if not m:
        continue
    stmt = m.group(1).strip()
    mk = re.match(r"^SELECT .MARK_" + RUN_ID + r"_(\w+?)_(\d+)_(\d+).$", stmt)
    if mk:
        cur = (mk.group(1), int(mk.group(2)), int(mk.group(3)))
        continue
    # A marker from a DIFFERENT run fails the RUN_ID match above and would
    # otherwise fall through to be credited as if it were a real query — a
    # marker SELECT is cheap, so it would land as a suspiciously fast time in
    # whichever slot was armed. Drop any MARK_ statement that is not ours.
    if stmt.startswith("SELECT ") and "MARK_" in stmt:
        continue
    if cur is None:
        continue
    def fld(name, default="0"):
        mm = re.search(r"\|" + name + r"=([^|]*)", line)
        return mm.group(1) if mm and mm.group(1) != "" else default
    tag, p, q = cur
    out.append("%s %d %d %s %s %s %s" % (tag, p, q, fld("Time"), fld("ScanRows"),
                                         fld("ScanBytes"), fld("State")))
    cur = None
print("\n".join(out))
')

# FAIL LOUDLY ON AN UNREADABLE AUDIT SLICE. Every "-" in this benchmark's CSV
# is produced by med() on an empty set, so a run whose audit records could not
# be read at all emits a complete, well-formed, entirely empty table — which
# looks like "the queries returned nothing" rather than "the harness lost the
# measurements." That is how the rotation bug above would have presented.
#
# Expected record count: one per (arm, pass, query). Demand at least the arm
# count, which is enough to prove the slice is real while tolerating genuine
# per-query errors (an ERR query still writes a record, so this is not a
# success check — only a did-we-read-the-log check).
PARSED_N=$(printf '%s\n' "$PARSED" | grep -c . || true)
if (( PARSED_N < 3 )); then
    echo "" >&2
    echo "ABORT: parsed only ${PARSED_N} audit records from ${AUDIT}." >&2
    echo "  since: ${START_STAMP} (FE pod clock)" >&2
    echo "" >&2
    echo "The queries ran, but their timings could not be read back out of the" >&2
    echo "FE audit log. Most likely causes: the log rotated and the rotated" >&2
    echo "file is no longer on disk, audit logging is disabled, or FE_POD was" >&2
    echo "replaced mid-run. No CSV written — an empty CSV of '-' rows reads as" >&2
    echo "a result and must never be produced." >&2
    exit 1
fi

med() { sort -n | awk '{a[NR]=$1} END{ if(NR==0){print "-";exit}
        if(NR%2) printf "%d\n", a[(NR+1)/2]; else printf "%d\n", (a[NR/2]+a[NR/2+1])/2 }'; }

pick() { printf '%s\n' "$PARSED" | awk -v t="$1" -v w="$2" -v q="$3" -v c="$4" \
             '$1==t && $2>=w && $3==q {print $c}' | med; }

# A failed query still writes an audit record — with a small Time and
# ScanRows=0, which reads as the FASTEST possible result. In the CDC run this
# scored two Iceberg OOMs as a 3x and 9x win over Paimon. State is the only
# field that tells the two apart, so any slot with a non-EOF/OK pass is reported
# as ERR rather than timed. Never remove this: without it the benchmark rewards
# whichever format fails hardest.
errs() { printf '%s\n' "$PARSED" | awk -v t="$1" -v w="$2" -v q="$3" \
             '$1==t && $2>=w && $3==q && $7!="EOF" && $7!="OK" {n++} END{print n+0}'; }

# Writer state goes in the CSV as a comment line, not just on stdout, so a
# result file can never be read without the variable that dominates it. A CSV
# found on disk six months from now must still say whether the writers were live.
# WRITERS_AT_START is sampled AFTER the pre-flight gate, so for a snapshot run this
# line proves the regime the numbers were measured under rather than the regime
# the cluster happened to be in when the script was invoked.
echo "# writers: ${WRITERS_AT_START}  (suspend_writers=${SUSPEND_WRITERS:-auto} stopped_by_run=${CID_WRITERS_STOPPED:-0})" > "$CSV"
echo "# snapshot_dt: ${SNAPSHOT_DT}  runs: ${RUNS}  warmup: ${WARMUP}  spill: enabled  datacache: disabled" >> "$CSV"
echo "stamp,suite,query,metric,paimon,pmn_as_iceberg,iceberg,ratio_pmnice_over_paimon,ratio_iceberg_over_paimon" >> "$CSV"

for s in $RUN_SUITES; do
    # The names are the QUERY FILENAMES, in the same lexical order build_script
    # emitted the markers in, so index qi below indexes one list read twice —
    # there is no longer a hand-maintained names string to keep aligned with the
    # statements, which is what used to credit a record to the wrong query.
    if [[ "$s" == snapshot ]]; then names="$SNAPSHOT_NAMES"; else names="$HOT_NAMES"; fi
    set -- $names
    NAMES=( "$@" )

    printf '\n%s\n' "──────────────────────────────────────────────────────────────────────────────────────────────────"
    printf '  %s suite — StarRocks server-side (median of %s runs)\n' \
        "$(echo "$s" | tr '[:lower:]' '[:upper:]')" "$RUNS"
    printf '%s\n' "──────────────────────────────────────────────────────────────────────────────────────────────────"
    printf '%-22s %9s %9s %9s %8s %8s %14s %14s %14s\n' \
        QUERY PAI_ms PMNICE_ms ICE_ms X/P I/P PAI_scanbytes PMNICE_scanb ICE_scanbytes

    for qi in "${!NAMES[@]}"; do
        name=${NAMES[$qi]}
        p_ms=$(pick "P${s}" "$WARMUP" "$qi" 4); p_sr=$(pick "P${s}" "$WARMUP" "$qi" 5); p_sb=$(pick "P${s}" "$WARMUP" "$qi" 6)
        x_ms=$(pick "X${s}" "$WARMUP" "$qi" 4); x_sr=$(pick "X${s}" "$WARMUP" "$qi" 5); x_sb=$(pick "X${s}" "$WARMUP" "$qi" 6)
        i_ms=$(pick "I${s}" "$WARMUP" "$qi" 4); i_sr=$(pick "I${s}" "$WARMUP" "$qi" 5); i_sb=$(pick "I${s}" "$WARMUP" "$qi" 6)

        # On error, blank the SCAN fields too, not just the time. A failed query
        # writes ScanRows=0/ScanBytes=0, and leaving those in the CSV next to an
        # ERR time reads as "this arm scanned nothing and was therefore cheap" —
        # exactly the inversion the errs() guard exists to prevent.
        [[ "$(errs "P${s}" "$WARMUP" "$qi")" != 0 ]] && { p_ms=ERR; p_sr=ERR; p_sb=ERR; }
        [[ "$(errs "X${s}" "$WARMUP" "$qi")" != 0 ]] && { x_ms=ERR; x_sr=ERR; x_sb=ERR; }
        [[ "$(errs "I${s}" "$WARMUP" "$qi")" != 0 ]] && { i_ms=ERR; i_sr=ERR; i_sb=ERR; }

        rat() {  # $1 numerator ms, $2 denominator ms
            if [[ "$1" != ERR && "$2" != ERR && "$1" != "-" && "$2" != "-" && "$2" -gt 0 ]]; then
                awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2fx", a/b}'
            else
                echo "-"
            fi
        }
        xp=$(rat "$x_ms" "$p_ms"); ip=$(rat "$i_ms" "$p_ms")

        printf '%-22s %9s %9s %9s %8s %8s %14s %14s %14s\n' \
            "$name" "$p_ms" "$x_ms" "$i_ms" "$xp" "$ip" "$p_sb" "$x_sb" "$i_sb"
        {
            echo "${STAMP},${s},${name},time_ms,${p_ms},${x_ms},${i_ms},${xp},${ip}"
            echo "${STAMP},${s},${name},scan_rows,${p_sr},${x_sr},${i_sr},,"
            echo "${STAMP},${s},${name},scan_bytes,${p_sb},${x_sb},${i_sb},,"
        } >> "$CSV"
    done
done

echo ""
echo "  Arms: PAI = Paimon reader over the Paimon table."
echo "        PMNICE = Iceberg reader over the SAME Paimon data files, via the"
echo "                 Iceberg metadata Paimon exports. Isolates the READ PATH:"
echo "                 X/P differs only in reader, not in bytes on disk."
echo "        ICE = Iceberg reader over the natively-written Iceberg table."
echo "  RATIO > 1 means that arm took LONGER than Paimon-native."
echo "  ScanBytes is the attribution: a latency gap with equal ScanBytes is"
echo "  engine/metadata overhead, not I/O, and must not be reported as a format win."
echo ""
echo "Results -> $CSV"
# The writers are left STOPPED. Restarting them is deliberately not done here
# and there is no hint command: upgradeMode is stateless, so a restart reads
# whatever SQL is currently in S3 rather than the SQL this run was measured
# against. That makes it the start of the NEXT experiment, not cleanup of this
# one, and it belongs in a human's hands. See snapshot-preflight.sh.
if [[ -n "${CID_WRITERS_STOPPED:-}" ]]; then
    echo ""
    echo "  The writers were STOPPED by this run and are still stopped."
fi
