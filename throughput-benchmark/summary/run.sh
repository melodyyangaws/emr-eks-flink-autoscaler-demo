#!/usr/bin/env bash
# Summarise the session's test outcome into bench-results/BENCH-SUMMARY-<session>.md.
#
#   ./summary/run.sh            # or ./bench.sh summary; no arguments
#
# The session is .bench-env's TEST_SUFFIX: its tables, and its query runs, which
# querybench/run.sh uploads to s3://$BUCKET_NAME/querybench-results/session<TEST_SUFFIX>/.
# Steps (from throughput-benchmark/):
#   1. find the session's newest hot and newest snapshot run under that prefix
#      (HOT_STAMP= / SNAPSHOT_STAMP= pick another; a suite with no run is left
#      out) and sync them into bench-results/;
#   2. read both tables' commit history (lib/querybench.sh) for TEST_SUFFIX's set;
#   3. take the newest REST collector CSVs (monitoring/collect-metrics.sh);
#   4. summarize.py writes the Markdown; the bucket and account ID are masked in
#      it and in the synced logs.
# SESSION (default <date of the run>${TEST_SUFFIX}) names the output and the commit CSVs.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
(( $# == 0 )) || die "summary/run.sh takes no arguments: the session is TEST_SUFFIX in .bench-env (HOT_STAMP=/SNAPSHOT_STAMP= pick other runs)"

# The newest <STAMP> with a <STAMP>-<suite>/ under the session's prefix (STAMPs sort by time).
latest() {
    aws s3 ls "s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}/" 2>/dev/null \
        | sed -n "s|.* PRE \([0-9]\{8\}T[0-9]\{6\}Z\)-${1}/\$|\1|p" | sort | tail -1
}
HOT="${HOT_STAMP:-$(latest hot)}" SNAP="${SNAPSHOT_STAMP:-$(latest snapshot)}"
[[ -n "$HOT$SNAP" ]] || warn "no query runs under ${QB_RESULTS_PREFIX}/: throughput only"
log "session TEST_SUFFIX='${TEST_SUFFIX}': hot run ${HOT:-none}, snapshot run ${SNAP:-none}"
first="${HOT:-${SNAP:-$(date -u +%Y%m%d)}}"
SESSION="${SESSION:-${first:0:8}${TEST_SUFFIX}}"
R="${BENCH_DIR}/bench-results"
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT

for s in ${HOT:+"$HOT-hot"} ${SNAP:+"$SNAP-snapshot"}; do                         # 1
    log "syncing ${QB_RESULTS_PREFIX}/${s}/"
    aws s3 sync "s3://${BUCKET_NAME}/${QB_RESULTS_PREFIX}/${s}/" "$R/" --only-show-errors \
        || die "could not sync ${QB_RESULTS_PREFIX}/${s}/"
done

log "reading the commit history of TEST_SUFFIX='${TEST_SUFFIX}'"                    # 2
_qb_commits_paimon  > "$T/paimon"  || die "no Paimon snapshots under ${PAIMON_TABLE_ROOT}/snapshot"
_qb_commits_iceberg > "$T/iceberg" || die "no Iceberg metadata for ${ICEBERG_DB}.${TABLE_NAME}"

newest() { ls -t "$R"/cid-"$1"-20*Z.csv 2>/dev/null | grep -v commits | head -1 || true; }   # 3
rp=$(newest paimon) ri=$(newest iceberg)
[[ -n "$rp" && -n "$ri" ]] || warn "no REST collector CSVs in bench-results/: the REST rows are left out"

out="$R/BENCH-SUMMARY-${SESSION}.md"                                               # 4
python3 "${HERE}/summarize.py" --results "$R" --session "$SESSION" --suffix="$TEST_SUFFIX" \
    --commits "$T/paimon" "$T/iceberg" ${rp:+--rest "$rp" "$ri"} \
    ${HOT:+--hot "$HOT"} ${SNAP:+--snapshot "$SNAP"} --out "$out" >/dev/null
for f in "$out" "$R"/*"${HOT:-@none@}"*.log "$R"/*"${SNAP:-@none@}"*.log; do
    # Only the bucket name and the account ID: a blanket 12-digit mask would cut
    # through the snapshot IDs.
    [[ -f "$f" ]] && sed -i.bak -e "s/${BUCKET_NAME}/<bucket>/g" -e "s/${AWS_ACCOUNT_ID}/<id>/g" "$f" && rm -f "$f.bak"
done
log "summary: ${out#${BENCH_DIR}/}"
