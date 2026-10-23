#!/usr/bin/env bash
# Write the session's benchmark findings: bench-results/BENCH-SUMMARY-<session>.md.
#
#   ./summary/run.sh            # or ./bench.sh summary; no arguments
#
# The session is .bench-env's TEST_SUFFIX. Its data lives in S3 under
# s3://$BUCKET_NAME/querybench-results/session<TEST_SUFFIX>/:
#   <STAMP>-hot/, <STAMP>-snapshot/   query runs (querybench/run.sh uploads them)
#   throughput/                       metric collector CSVs/logs and commit histories
#   compaction/                       compaction/run.sh's per-pass JSONs, if it ran
# Steps (from throughput-benchmark/):
#   1. upload the laptop's collector files (bench-results/cid-*.csv, collect-*.log)
#      to throughput/: they exist nowhere else;
#   2. sync the newest hot and snapshot run (HOT_STAMP= / SNAPSHOT_STAMP= pick
#      others; a suite with no run is left out) and throughput/ into a temp dir;
#   3. read both tables' commit history (lib/querybench.sh) and upload it to throughput/;
#   4. read the writers' settings from the FlinkDeployments, if they still exist;
#   5. summarize.py writes the Markdown (bucket and account ID masked);
#   6. remove every non-Markdown file from bench-results/: all of it is in S3.
# SESSION (default <date of the run>${TEST_SUFFIX}) names the output.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
(( $# == 0 )) || die "summary/run.sh takes no arguments: the session is TEST_SUFFIX in .bench-env (HOT_STAMP=/SNAPSHOT_STAMP= pick other runs)"

S3="s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}"
R="${BENCH_DIR}/bench-results"
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT

# 1. The laptop-only files go up first, so nothing below depends on the laptop.
up=( "$R"/cid-paimon-20*Z.csv "$R"/cid-iceberg-20*Z.csv "$R"/collect-*.log )
for f in "${up[@]}"; do
    [[ -f "$f" ]] || continue
    aws s3 cp --only-show-errors "$f" "${S3}/throughput/$(basename "$f")" || die "could not upload ${f##*/}"
    log "uploaded ${f##*/} to ${QB_RESULTS_PREFIX}/throughput/"
done

# 2. The newest <STAMP> with a <STAMP>-<suite>/ under the session's prefix (STAMPs sort by time).
latest() { aws s3 ls "${S3}/" 2>/dev/null | sed -n "s|.* PRE \([0-9]\{8\}T[0-9]\{6\}Z\)-${1}/\$|\1|p" | sort | tail -1; }
HOT="${HOT_STAMP:-$(latest hot)}" SNAP="${SNAPSHOT_STAMP:-$(latest snapshot)}"
[[ -n "$HOT$SNAP" ]] || warn "no query runs under ${QB_RESULTS_PREFIX}/: throughput only"
log "session TEST_SUFFIX='${TEST_SUFFIX}': hot run ${HOT:-none}, snapshot run ${SNAP:-none}"
first="${HOT:-${SNAP:-$(date -u +%Y%m%d)}}"
SESSION="${SESSION:-${first:0:8}${TEST_SUFFIX}}"
for s in throughput ${HOT:+"$HOT-hot"} ${SNAP:+"$SNAP-snapshot"}; do
    aws s3 sync --only-show-errors "${S3}/${s}/" "$T/" || die "could not sync ${QB_RESULTS_PREFIX}/${s}/"
done
# The compaction before the snapshot test (compaction/run.sh), if one ran.
aws s3 sync --only-show-errors "${S3}/compaction/" "$T/compaction/" 2>/dev/null || true
compaction=()
ls "$T"/compaction/*-pass*.json >/dev/null 2>&1 && compaction=(--compaction "$T/compaction")

# 3. Commit history: what the throughput numbers are computed from.
log "reading the commit history of TEST_SUFFIX='${TEST_SUFFIX}'"
_qb_commits_paimon  > "$T/commits-paimon"  || die "no Paimon snapshots under ${PAIMON_TABLE_ROOT}/snapshot"
_qb_commits_iceberg > "$T/commits-iceberg" || die "no Iceberg metadata for ${ICEBERG_DB}.${TABLE_NAME}"
newest() { ls -t "$T"/cid-"$1"-20*Z.csv 2>/dev/null | head -1 || true; }
rp=$(newest paimon) ri=$(newest iceberg)
[[ -n "$rp" && -n "$ri" ]] || warn "no collector CSVs for this session: the REST rows are left out"

# 4. Setup, from the writers if they are still there (suspended counts).
settings=()
# env is set on the pod template and the JobManager's; read both.
envs=$(kubectl get flinkdeployment flink-cid-paimon -n "$NAMESPACE" -o json 2>/dev/null | python3 -c '
import json, sys
s = json.load(sys.stdin)["spec"]
for t in (s.get("podTemplate", {}), s.get("jobManager", {}).get("podTemplate", {}), s.get("taskManager", {}).get("podTemplate", {})):
    for c in t.get("spec", {}).get("containers", []):
        for e in c.get("env", []):
            print("%s=%s" % (e["name"], e.get("value", "")))' 2>/dev/null || true)
par=$(kubectl get flinkdeployment flink-cid-paimon -n "$NAMESPACE" -o jsonpath='{.spec.job.parallelism}' 2>/dev/null || true)
[[ -n "$par" ]] && settings+=(--setting "JOB_PARALLELISM=${par}")
for k in PAIMON_WRITE_ONLY PAIMON_WRITE_BUFFER_SPILLABLE PAIMON_WRITE_BUFFER_SIZE PAIMON_BUCKETS PAIMON_ICEBERG_STORAGE S3A_MULTIPART_SIZE; do
    v=$(sed -n "s/^${k}=//p" <<<"$envs" | head -1)
    [[ -n "$v" ]] && settings+=(--setting "${k}=${v}")
done
pods=$(grep -o 'live ([0-9]* pods)' "$T/hot-interleave-${HOT:-none}.pins" 2>/dev/null | head -1 | tr -dc 0-9 || true)
settings+=(--setting "generator pods=${pods:-${JSONGZ_REPLICAS:-?} (.bench-env)}")

# 5. The findings.
out="$R/BENCH-SUMMARY-${SESSION}.md"
python3 "${HERE}/summarize.py" --work "$T" --session "$SESSION" --suffix="$TEST_SUFFIX" "${settings[@]}" \
    --commits "$T/commits-paimon" "$T/commits-iceberg" ${rp:+--rest "$rp" "$ri"} \
    ${HOT:+--hot "$HOT"} ${SNAP:+--snapshot "$SNAP"} ${compaction[@]+"${compaction[@]}"} --out "$out" >/dev/null
# Only the bucket name and the account ID: a blanket 12-digit mask would cut through snapshot IDs.
sed -i.bak -e "s/${BUCKET_NAME}/<bucket>/g" -e "s/${AWS_ACCOUNT_ID}/<id>/g" "$out" && rm -f "$out.bak"
aws s3 cp --only-show-errors "$T"/cid-paimon-commits-*.csv "${S3}/throughput/" 2>/dev/null || true
aws s3 cp --only-show-errors "$T"/cid-iceberg-commits-*.csv "${S3}/throughput/" 2>/dev/null || true
log "findings: ${out#${BENCH_DIR}/}"

# 6. bench-results/ keeps Markdown only; the data is in S3 (above).
n=0
for f in "$R"/* "$R"/.DS_Store; do
    [[ -f "$f" && "$f" != *.md && "${f##*/}" != .gitkeep ]] || continue
    rm -f "$f"; n=$((n + 1))
done
(( n == 0 )) || log "removed ${n} non-Markdown file(s) from bench-results/ (in S3 under ${QB_RESULTS_PREFIX}/)"
