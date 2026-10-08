#!/usr/bin/env bash
# bench-sql.sh — author and publish the shared query suites; inspect them
# without running a benchmark.
#
# THIS DIRECTORY IS THE EDITING COPY. THE HARNESSES READ S3.
# `upload` publishes this tree to s3://${BUCKET_NAME}/flink/querybench/, and
# both harnesses `aws s3 sync` it down at the start of a run — neither one looks
# at a local directory, so neither can be run against a stale checkout or fail
# because this folder moved. Edit here, `check`, then `upload`.
#
# USAGE
#   ./bench-sql.sh plan   [hot|snapshot|all]     readable step list + the bounds
#   ./bench-sql.sh render [hot|snapshot|all]     the exact SQL, as submitted
#   ./bench-sql.sh oneline [hot|snapshot|all]    as the StarRocks runner sends it
#   ./bench-sql.sh check                         render every file, both dialects
#   ./bench-sql.sh upload                        check, then publish to S3
#   ./bench-sql.sh ls                             list what is published
#
#   ENGINE=duckdb|starrocks   picks ${BYTES_FN} (STRLEN vs LENGTH) and, for
#                             duckdb, defers ${TABLE}. Default starrocks.
#   SNAPSHOT_DT=YYYY-MM-DD    the closed partition. Default: yesterday UTC, which
#                             is ONLY a placeholder here — both harnesses resolve
#                             it from the DATA (oldest dt= prefix) because the
#                             newest partition is still being written. This
#                             script does not touch S3, so it cannot do that.
#
# `check` is the one to run after editing a .sql file. It renders every query
# under BOTH dialects and fails if any placeholder is left unresolved — which
# matters because an unsubstituted ${FOO} is NOT a syntax error on either engine:
# it binds as an identifier or a string and the query returns a plausible wrong
# answer. It does NOT connect to an engine, so it cannot catch a column typo.
#
# `upload` RUNS `check` FIRST AND REFUSES TO PUBLISH A FAILING TREE. Publishing
# is the only way a query reaches either engine, so the gate belongs here: a
# broken render that reached S3 would be picked up by both harnesses on their
# next run, which is precisely the silent-divergence failure this layout exists
# to prevent.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1
# shellcheck disable=SC1091
source ./bench-sql-lib.sh || exit 1

CMD="${1:-plan}"
WHICH="${2:-all}"
ENGINE="${ENGINE:-starrocks}"
SNAPSHOT_DT="${SNAPSHOT_DT:-$(bench_date_plus_days "$(date -u +%Y-%m-%d)" -1)}"

# Only `upload`/`ls` need these, and only they source .bench-env — the inspect
# commands must keep working with no AWS credentials at all.
resolve_s3() {
    # shellcheck disable=SC1091
    [[ -f ../.bench-env ]] && source ../.bench-env
    : "${BUCKET_NAME:?BUCKET_NAME is required (set it or populate ../.bench-env)}"
    QUERYBENCH_S3="${QUERYBENCH_S3:-s3://${BUCKET_NAME}/flink/querybench}"
    QUERYBENCH_S3="${QUERYBENCH_S3%/}"
}

suites() {
    case "$1" in
        hot)      echo hot ;;
        snapshot) echo snapshot ;;
        all)      echo hot; echo snapshot ;;
        *) echo "ERROR: suite must be hot|snapshot|all, got '$1'" >&2; return 1 ;;
    esac
}

set_dialect() {   # $1 = duckdb|starrocks
    case "$1" in
        duckdb)
            export BENCH_BYTES_FN="STRLEN"
            # The DuckDB scan expression is resolved per query inside the pod, so
            # leaving it in is correct, not a rendering failure.
            export BENCH_DEFER="TABLE"
            export BENCH_TABLE=""
            ;;
        starrocks)
            export BENCH_BYTES_FN="LENGTH"
            export BENCH_DEFER=""
            export BENCH_TABLE="${TABLE_NAME:-cid_events}"
            ;;
        *) echo "ERROR: ENGINE must be duckdb|starrocks, got '$1'" >&2; return 1 ;;
    esac
}

bench_resolve_hot_windows        || exit 1
bench_resolve_snapshot_window "$SNAPSHOT_DT" || exit 1

case "$CMD" in
    plan)
        set_dialect "$ENGINE" || exit 1
        for s in $(suites "$WHICH") ; do bench_print_plan "$s"; done
        ;;
    render|oneline)
        set_dialect "$ENGINE" || exit 1
        rc=0
        for s in $(suites "$WHICH"); do
            echo "-- ${s} suite (engine=${ENGINE}) ---------------------------------"
            while IFS= read -r f; do
                echo "-- $(bench_query_name "$f")"
                if [[ "$CMD" == oneline ]]; then
                    bench_render_oneline "$f" || rc=1
                else
                    bench_render "$f" || rc=1
                fi
                echo ""
            done < <(bench_query_files "$s")
        done
        exit $rc
        ;;
    check|upload)
        rc=0 n=0
        for e in starrocks duckdb; do
            set_dialect "$e" || exit 1
            for s in hot snapshot; do
                while IFS= read -r f; do
                    n=$(( n + 1 ))
                    if ! bench_render "$f" >/dev/null; then rc=1; continue; fi
                    bench_render_oneline "$f" >/dev/null || rc=1
                done < <(bench_query_files "$s")
            done
        done
        # The suites are only comparable if both engines run the SAME number of
        # steps, so the count is asserted rather than eyeballed.
        h=$(bench_query_files hot | grep -c .)
        p=$(bench_query_files snapshot | grep -c .)
        echo "  hot suite      : ${h} queries"
        echo "  snapshot suite : ${p} queries"
        echo "  renders        : ${n} (every query x starrocks,duckdb)"
        if (( rc == 0 )); then
            echo "  RESULT         : OK — all placeholders resolve in both dialects"
        else
            echo "  RESULT         : FAILED — see errors above" >&2
            exit $rc
        fi
        [[ "$CMD" == upload ]] || exit 0

        resolve_s3
        echo ""
        echo "  publishing to  : ${QUERYBENCH_S3}/"
        # --delete so a DELETED query disappears from the suite. Without it a
        # retired .sql keeps being downloaded and run by both harnesses, and the
        # count assertions still pass because the count is of what arrived.
        aws s3 sync . "${QUERYBENCH_S3}/" --region "${AWS_REGION:-us-west-2}" \
            --delete --exclude '*' \
            --include 'bench-sql-lib.sh' --include 'bench-sql.sh' \
            --include 'hot/*.sql' --include 'snapshot/*.sql' \
            --only-show-errors || exit 1
        echo "  published      : $(( h + p )) .sql + bench-sql-lib.sh + bench-sql.sh"
        echo "  both harnesses sync this prefix at the start of every run."
        ;;
    ls)
        resolve_s3
        echo "-- ${QUERYBENCH_S3}/"
        aws s3 ls --recursive "${QUERYBENCH_S3}/" --region "${AWS_REGION:-us-west-2}"
        ;;
    *)
        # The header IS the help text, so the range tracks `set -uo pipefail`
        # rather than a hardcoded line number that drifts on every edit.
        sed -n "2,$(( $(grep -n '^set -uo pipefail' "${BASH_SOURCE[0]}" | cut -d: -f1) - 1 ))p" \
            "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
        exit 1
        ;;
esac
