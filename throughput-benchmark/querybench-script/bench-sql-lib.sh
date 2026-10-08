#!/usr/bin/env bash
# Shared query library for the cid_events read benchmarks.
#
# WHY THIS FILE EXISTS — ONE SOURCE OF TRUTH FOR THE QUERIES.
#
# A duplicated query set has no mechanism that can notice this. So the queries
# now live in ONE place, as files, and every harness renders those same files.
# Divergence stops being possible rather than being a thing to remember.
#
# ── LAYOUT ──────────────────────────────────────────────────────────────────
#   querybench-script/hot/H<n>_<name>.sql        the HOT suite   (trailing window)
#   querybench-script/snapshot/S<n>_<name>.sql   the SNAPSHOT suite (one closed dt)
#
# ── starrocks-bench.sh's q1..q5 ARE MERGED IN HERE (2026-10-06) ─────────────
# The last duplicated query set in the project. Four of the five were this
# suite's queries with no bound; each landed in the file that already asked its
# question, which records the merge in its own header:
#
#   q1_count            -> S1_partition_count   (NOT ported as a query — its
#                          "is COUNT(*) a metadata shortcut?" question is
#                          answered there, and unbounded it never returns)
#   q2_partition_prune  -> S1_partition_count   (NOW() -> pinned ${SNAPSHOT_DT})
#   q3_needle_uuid      -> H3_hot_needle_uuid   (+ the hot window)
#   q4_narrow_agg       -> S2_narrow_agg        (+ dt, + LIMIT 20)
#   q5_wide_scan        -> S4_wide_json         (+ dt + ingestionTime window,
#                          LENGTH -> ${BYTES_FN})
#
# WHAT THE BOUNDS CHANGE, AND WHAT THEY DO NOT. Every q* was unbounded, i.e. a
# full ~8 TiB scan; q5 alone would be a multi-hour statement. None of their
# findings needed that scale, because all four measure a RATE (per-byte decode
# cost, per-row aggregate cost, a predicate's pruning power) which the ratios
# between files recover: H5/H4 is the fat-column tax, S4/S1 the same at
# partition scale. What the bounds buy is a suite that terminates and whose
# every number is attributable to a partition whose row count S1 reports.
#
# starrocks-bench.sh keeps NO queries. It remains the only producer of the
# file-LAYOUT stats (-tablestats.txt), which are an S3 listing, not SQL.
#
# ONE STATEMENT PER FILE. The filename (minus .sql) IS the query name in every
# report, and the H<n>/S<n> prefix fixes the order under a lexical sort. That
# replaces the old scheme, where a heredoc's line N had to stay aligned with
# position N of a separate HOT_NAMES/SNAPSHOT_NAMES string — an invariant that
# was load-bearing for attribution, enforced by nothing, and silently broken by
# inserting a query anywhere but the end.
#
# ── PLACEHOLDERS ────────────────────────────────────────────────────────────
# Every .sql file is a template. The renderer substitutes exactly these, and
# REFUSES to emit SQL that still contains a ${...} it does not recognise:
#
#   ${TABLE}        the scan expression.
#                     StarRocks  cid_events            (after SET CATALOG/USE)
#                     DuckDB     iceberg_scan('s3://.../metadata.json')
#                                read_parquet('s3://.../dt=*/bucket-*/*.parquet',
#                                             hive_partitioning=1)
#                   On the DuckDB side this is resolved PER QUERY inside the pod
#                   (the Iceberg metadata_location moves), so it is the one
#                   placeholder allowed to survive the submit-time render — see
#                   BENCH_DEFER below.
#
#   ${BYTES_FN}     byte length of a VARCHAR. StarRocks LENGTH, DuckDB STRLEN.
#                   THE ONLY REAL DIALECT DIFFERENCE IN THE SUITE, and it is not
#                   cosmetic: DuckDB's LENGTH counts CHARACTERS, StarRocks' counts
#                   BYTES, and payload_bytes is the cross-engine equality check
#                   that proves both engines read the same rows. They agree only
#                   on pure ASCII, which is true of the generator's padding today
#                   and must not be relied on. NOT octet_length() — in DuckDB that
#                   is BLOB/BIT-only and a VARCHAR argument is a BinderException.
#
#   ${HOT_DT}       the live dt partition (UTC date).
#   ${HOT_FROM_1M}  'YYYY-MM-DD HH:MM:SS' — lower bounds of the hot windows,
#   ${HOT_FROM_5M}  pinned at render time.
#   ${HOT_FROM_10M}
#   ${HOT_TO}       upper bound, shared by all three hot windows.
#
#   ${SNAPSHOT_DT}  the closed dt partition the snapshot suite reads.
#   ${S4_FROM}      'YYYY-MM-DD HH:MM:SS' — S4's ingestionTime window. Anchored
#   ${S4_TO}        to the INGESTION date, which is NOT ${SNAPSHOT_DT}; see
#                   bench_resolve_snapshot_window().
#
# ── TIMESTAMP LITERALS ARE PLAIN STRINGS, DELIBERATELY ──────────────────────
# The templates compare ingestionTime against a bare 'YYYY-MM-DD HH:MM:SS'
# string with no CAST and no TIMESTAMP keyword. Both engines implicitly coerce it
# (StarRocks DATETIME has no timezone; DuckDB casts VARCHAR to TIMESTAMP in a
# comparison), whereas the explicit spellings do NOT port: `TIMESTAMP '...'` is
# not StarRocks syntax and `CAST(... AS TIMESTAMP)` is DATETIME there. The bare
# literal is the only form that is identical in both dialects.
#
# ── WHY THE WINDOWS ARE PINNED LITERALS AND NOT UTC_TIMESTAMP() ─────────────
# This is a CORRECTNESS fix, not a tidy-up. Two bugs are closed by it:
#
#   1. THE ARMS NO LONGER READ DIFFERENT DATA. The StarRocks harness used to
#      compute its hot window IN SQL as DATE_SUB(UTC_TIMESTAMP(), INTERVAL 5
#      MINUTE), evaluated per statement. Its three arms run serially, so arm3's
#      window opened after arm1's had closed — its own header admitted "the
#      second format sees one suite's duration of extra rows". The DuckDB side
#      had the mirror-image defect: each POD computed its own literal at startup,
#      so a serial two-arm run measured two different tables (the iceberg arm
#      took 54 min on 2026-10-05, so the paimon arm's window opened an hour
#      later). A ratio between two different row sets is not a format result.
#      Rendered once per invocation, every arm, pass and engine reads the SAME
#      rows.
#
#   2. THE WINDOW IS FIXED-WIDTH AGAIN. DuckDB's pinned bound was open-ended
#      (`ingestionTime >= literal`), so with the writers live the window GREW for
#      the whole run and later passes did strictly more work than earlier ones —
#      a median over them is a median over increasing workloads. The window is
#      now half-open [FROM, TO), so its width is a constant.
#
# ── DO NOT RUN THE HOT SUITE ACROSS UTC MIDNIGHT ────────────────────────────
# The hot queries are `dt = ${HOT_DT} AND ingestionTime IN [FROM, TO)`. dt is
# derived from the EVENT time and ingestionTime from the wall clock, so a window
# straddling 00:00Z returns only its post-midnight part and the suite quietly
# measures a fraction of the intended rows. bench_resolve_hot_windows() warns
# when ${HOT_TO} is within 15 minutes of midnight.

[[ -n "${_BENCH_SQL_LIB_LOADED:-}" ]] && return 0
_BENCH_SQL_LIB_LOADED=1

BENCH_SQL_DIR="${BENCH_SQL_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"

_bench_err() { echo "  [bench-sql] ERROR: $*" >&2; }

# ── Portable UTC date arithmetic ────────────────────────────────────────────
# BSD/macOS date has -v and no -d; GNU date the reverse. Every call site needs
# both, so it is wrapped once here rather than repeated per harness.
#
# BSD ORDER IS LOAD-BEARING: -v MUST precede -f. Written the other way round BSD
# date silently ignores both the adjustment and the output format and prints the
# CURRENT time in ctime form ("Sun Oct  4 03:40:05 UTC 2026") with EXIT STATUS 0.
# There is no error to catch, which is why every value this file produces is
# shape-checked below instead of being trusted.

# bench_utc_now_minus <minutes> -> 'YYYY-MM-DD HH:MM:SS'
bench_utc_now_minus() {
    local m="$1"
    date -u -v-"${m}"M '+%Y-%m-%d %H:%M:%S' 2>/dev/null \
      || date -u -d "${m} minutes ago" '+%Y-%m-%d %H:%M:%S'
}

# bench_utc_now -> 'YYYY-MM-DD HH:MM:SS'
bench_utc_now() { date -u '+%Y-%m-%d %H:%M:%S'; }

# bench_date_plus_days <YYYY-MM-DD> <n> -> YYYY-MM-DD   (n may be negative)
#
# THE SIGN IS BUILT INTO THE ADJUSTMENT STRING, not prefixed with a literal '+'.
# `-v+-1d` is not a valid BSD adjustment and `-d '<date> +-1 day'` is not valid
# GNU input, so a hardcoded '+' made EVERY negative call return the empty string
# — with exit status 0 from the `2>/dev/null` fallback chain. The callers that
# need a negative offset are the yesterday-UTC SNAPSHOT_DT fallbacks, which would
# have rendered `dt = ''`: a partition that matches nothing, prunes to zero files
# and returns instantly, i.e. the empty-scan-looks-like-a-win failure this suite
# exists to prevent. Both branches are shape-checked by the callers, but an empty
# string passes no shape check that was ever applied to it, so it is fixed here.
bench_date_plus_days() {
    local d="$1" n="$2" adj
    case "$n" in -*) adj="$n" ;; *) adj="+${n#+}" ;; esac
    date -u -j -v"${adj}"d -f %Y-%m-%d "$d" +%Y-%m-%d 2>/dev/null \
      || date -u -d "${d} ${adj} day" +%Y-%m-%d 2>/dev/null
}

_bench_is_date()     { [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; }
_bench_is_datetime() { [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:[0-9]{2}:[0-9]{2}$ ]]; }

# ── Query discovery ─────────────────────────────────────────────────────────
# bench_query_files <hot|snapshot> -> one path per line, in execution order.
# Lexical sort over the H<n>/S<n> prefix IS the order; there is no separate list
# to keep aligned.
bench_query_files() {
    local suite="$1" d="${BENCH_SQL_DIR}/$1"
    [[ -d "$d" ]] || { _bench_err "no such suite directory: ${d}"; return 1; }
    local f found=0
    for f in "$d"/*.sql; do
        [[ -e "$f" ]] || continue
        echo "$f"; found=1
    done
    (( found == 1 )) || { _bench_err "no .sql files under ${d}"; return 1; }
}

# bench_query_name <path> -> the name used in every report
bench_query_name() { local b; b=$(basename "$1"); echo "${b%.sql}"; }

# ── Per-query guard directives ──────────────────────────────────────────────
# bench_query_flag <file> <flag> -> 0 if the file declares `-- bench: <flag>`
#
# AN EMPTY RESULT IS THE FASTEST POSSIBLE RESULT, so every harness flags a
# predicate that matched nothing rather than reporting it as a win. Two queries
# must be exempt, for DIFFERENT reasons, and both exemptions used to be
# hardcoded as name lists inside each harness — i.e. duplicated, and silently
# wrong for any query added later:
#
#   allow-zero-rows     H3 returns no rows BY DESIGN (the all-zero uuid cannot be
#                       generated), so nrows==0 is the expected outcome.
#   leading-col-is-label  S3's first column is filterRiskLevel, a LABEL. Testing
#                       "is the first column the string '0'?" asks whether the
#                       risk level is literally "0" — a legitimate value. The
#                       guard fired on a perfectly good 5-group result and scored
#                       it ZERO_SCAN. For a GROUP BY, an empty result set is the
#                       only sound signal.
#
# Declaring them IN the .sql puts the exemption next to the query it describes,
# so a new query carries its own guard semantics instead of needing a matching
# edit in every harness.
bench_query_flag() {
    grep -qE "^-- bench:[[:space:]].*\b${2}\b" "$1"
}

# ── Window resolution ───────────────────────────────────────────────────────
# Sets BENCH_HOT_DT / BENCH_HOT_FROM_{1M,5M,10M} / BENCH_HOT_TO.
#
# HOT_TO is "now" at the moment of the call, so the newest rows in the window
# are a few seconds old at most — which is the point of the suite. Everything is
# derived from that ONE instant so the three windows share an upper bound and
# are strict subsets of each other.
bench_resolve_hot_windows() {
    BENCH_HOT_TO="${HOT_TO:-$(bench_utc_now)}"
    BENCH_HOT_FROM_1M="${HOT_FROM_1M:-$(bench_utc_now_minus 1)}"
    BENCH_HOT_FROM_5M="${HOT_FROM_5M:-$(bench_utc_now_minus 5)}"
    BENCH_HOT_FROM_10M="${HOT_FROM_10M:-$(bench_utc_now_minus 10)}"
    BENCH_HOT_DT="${HOT_DT:-${BENCH_HOT_TO%% *}}"

    local v
    for v in "$BENCH_HOT_TO" "$BENCH_HOT_FROM_1M" "$BENCH_HOT_FROM_5M" \
             "$BENCH_HOT_FROM_10M"; do
        _bench_is_datetime "$v" || {
            _bench_err "hot window bound '${v}' is not 'YYYY-MM-DD HH:MM:SS'"
            return 1; }
    done
    _bench_is_date "$BENCH_HOT_DT" || {
        _bench_err "HOT_DT='${BENCH_HOT_DT}' is not YYYY-MM-DD"; return 1; }

    # Midnight guard — see the header. 15 minutes covers the widest window (10m)
    # plus the time the suite needs to reach its last hot query.
    local hhmm="${BENCH_HOT_TO#* }"
    case "$hhmm" in
        00:0*|00:1[0-4]*)
            echo "  [bench-sql] ⚠ HOT_TO=${BENCH_HOT_TO} is within 15 min of UTC" >&2
            echo "              midnight. The hot windows straddle the dt boundary," >&2
            echo "              so dt=${BENCH_HOT_DT} holds only their post-midnight" >&2
            echo "              part and the suite measures fewer rows than intended." >&2
            echo "              Wait, or pin HOT_DT/HOT_FROM_*/HOT_TO explicitly." >&2
            ;;
    esac
}

# Sets BENCH_SNAPSHOT_DT / BENCH_S4_FROM / BENCH_S4_TO from SNAPSHOT_DT, which
# the CALLER resolves (each harness has its own cheapest way to find the oldest
# closed partition) and passes in as $1.
#
# ── S4's WINDOW IS ON THE INGESTION DATE, WHICH IS NOT THE PARTITION DATE ───
# dt comes from eventTimeDT = event_ts - r_day days
# (sql-scripts/common/01-datagen-source.sql:592, cut to dt at :607) while
# ingestionTime is the wall clock at write (:657). So dt=2026-10-04 was WRITTEN
# on 2026-10-05 — verified on all three arms, whose MAX(ingestionTime) for that
# partition were 2026-10-05 04:40:58 / 04:35:17 / 04:46:01. Anchoring S4's
# window to SNAPSHOT_DT itself matches ZERO rows and returns instantly, which
# reads as a spectacular win and measures nothing.
#
# Hence SNAPSHOT_INGEST_DATE defaults to SNAPSHOT_DT + 1 day, the write date
# under CID_DAY_SPREAD=1. Override it if the generator's spread changes or the
# data is reloaded on a different day.
#
# ── WHY S4 IS WINDOWED AND S1-S3 ARE NOT ────────────────────────────────────
# S1-S3 carry dt ALONE: full-partition aggregates. S4 additionally carries a
# 30-minute ingestionTime window.
#
# S4 is the fat-JSON decode and it is what made this suite a multi-hour run:
# 18-41 min per pass per arm on StarRocks, 25.7 min on DuckDB, and in the
# cancelled 2026-10-06 run it passed 1,513 s on arm1 alone with a ~12 h
# projection for the full matrix. The retired `uuid % N` sample could not help,
# because uuid is MD5-derived and therefore random, so every Parquet row group's
# min/max spans the keyspace and NOTHING is pruned — measured, ScanRows equalled
# each arm's exact full partition count. It shrank the aggregate while reading
# every byte. An ingestionTime window DOES prune, because ingestionTime is
# monotonic with write order and its row-group stats are tight, so whole files
# are skipped.
#
# That is also why dropping the sample from S1-S3 is free: it never reduced
# their scan either. What changes is that their results are now the REAL
# partition figures instead of a sample to be scaled back up.
bench_resolve_snapshot_window() {
    BENCH_SNAPSHOT_DT="${1:?bench_resolve_snapshot_window needs SNAPSHOT_DT}"
    _bench_is_date "$BENCH_SNAPSHOT_DT" || {
        _bench_err "SNAPSHOT_DT='${BENCH_SNAPSHOT_DT}' is not YYYY-MM-DD"; return 1; }

    local ing from to
    ing="${SNAPSHOT_INGEST_DATE:-$(bench_date_plus_days "$BENCH_SNAPSHOT_DT" 1)}"
    from="${SNAPSHOT_S4_FROM:-04:30:00}"
    to="${SNAPSHOT_S4_TO:-05:00:00}"

    _bench_is_date "$ing" || {
        _bench_err "SNAPSHOT_INGEST_DATE='${ing}' is not YYYY-MM-DD (derived from SNAPSHOT_DT='${BENCH_SNAPSHOT_DT}'). Set it explicitly."
        return 1; }
    local t
    for t in "$from" "$to"; do
        [[ "$t" =~ ^[0-9]{2}:[0-9]{2}:[0-9]{2}$ ]] || {
            _bench_err "S4 window bound '${t}' is not HH:MM:SS"; return 1; }
    done
    # Half-open [FROM, TO): adjacent windows neither overlap nor gap. FROM == TO
    # would match nothing and return instantly, i.e. look like a win.
    [[ "$from" < "$to" ]] || {
        _bench_err "SNAPSHOT_S4_FROM (${from}) must be < SNAPSHOT_S4_TO (${to})"
        return 1; }

    BENCH_SNAPSHOT_INGEST_DATE="$ing"
    BENCH_S4_FROM="${ing} ${from}"
    BENCH_S4_TO="${ing} ${to}"
}

# ── Rendering ───────────────────────────────────────────────────────────────
# bench_render <file>  -> rendered SQL on stdout.
#
# Inputs (environment, set by the harness):
#   BENCH_TABLE     value for ${TABLE}. May be left empty when BENCH_DEFER names
#                   TABLE, which is how the DuckDB path ships templates into the
#                   pod with every OTHER placeholder already resolved.
#   BENCH_BYTES_FN  LENGTH (StarRocks) | STRLEN (DuckDB)
#   BENCH_DEFER     space-separated placeholder names to leave UNSUBSTITUTED and
#                   accept in the guard. Default empty.
#
# Substitution is pure bash ${var//pat/rep}, NOT sed or awk, because the values
# contain /, ' and * (an S3 glob) and a replacement string containing & is
# special to both of those tools. Bash parameter expansion has no such
# metacharacters.
#
# THE GUARD IS THE POINT OF THIS FUNCTION. An unsubstituted ${FOO} reaching the
# engine is not a syntax error on either side — it is a column reference or a
# string, so the query runs and returns something plausible. Anything matching
# ${[A-Z0-9_]+} that is neither substituted nor deferred is a hard failure.
bench_render() {
    local file="$1" content kv k v
    [[ -f "$file" ]] || { _bench_err "no such query file: ${file}"; return 1; }
    content="$(cat "$file")"

    local subst=(
        "TABLE=${BENCH_TABLE:-}"
        "BYTES_FN=${BENCH_BYTES_FN:-}"
        "HOT_DT=${BENCH_HOT_DT:-}"
        "HOT_FROM_1M=${BENCH_HOT_FROM_1M:-}"
        "HOT_FROM_5M=${BENCH_HOT_FROM_5M:-}"
        "HOT_FROM_10M=${BENCH_HOT_FROM_10M:-}"
        "HOT_TO=${BENCH_HOT_TO:-}"
        "SNAPSHOT_DT=${BENCH_SNAPSHOT_DT:-}"
        "S4_FROM=${BENCH_S4_FROM:-}"
        "S4_TO=${BENCH_S4_TO:-}"
    )
    for kv in "${subst[@]}"; do
        k="${kv%%=*}"; v="${kv#*=}"
        case " ${BENCH_DEFER:-} " in *" ${k} "*) continue ;; esac
        content="${content//\$\{${k}\}/${v}}"
    done

    local leftover
    leftover=$(printf '%s\n' "$content" | grep -oE '\$\{[A-Z0-9_]+\}' | sort -u | tr '\n' ' ')
    if [[ -n "${leftover// /}" ]]; then
        local allowed=1 p
        for p in $leftover; do
            p="${p#\$\{}"; p="${p%\}}"
            case " ${BENCH_DEFER:-} " in *" ${p} "*) ;; *) allowed=0 ;; esac
        done
        if (( allowed == 0 )); then
            _bench_err "unresolved placeholders in $(basename "$file"): ${leftover}"
            echo "    deferred (allowed): '${BENCH_DEFER:-}'" >&2
            echo "    An unsubstituted \${FOO} is NOT a syntax error on either" >&2
            echo "    engine — it binds as an identifier or a string and the query" >&2
            echo "    returns a plausible wrong answer. Refusing to emit it." >&2
            return 1
        fi
    fi
    printf '%s\n' "$content"
}

# bench_render_oneline <file> -> the same SQL collapsed to ONE line ending in ';'
#
# For the StarRocks harness, whose audit-log attribution needs exactly one
# statement per marker. Strips `--` comments and folds whitespace.
#
# CAVEAT: this is a text transform, not a SQL parser. A `--` or a newline INSIDE
# a string literal would be mangled. No query in the suite has one, and a new
# query that does must not be written with one.
#
# THE TRAILING NEWLINE IS EXPLICIT (printf '%s\n'), AND THAT IS NOT COSMETIC.
# `tr '\n' ' '` consumes the file's final newline, and BSD sed — unlike GNU —
# does not add one back when its input lacked one. So the pipeline returned a
# line with no terminator, and a caller looping over several files got every
# statement concatenated onto ONE line. The StarRocks runner emits one MARK_
# marker per line, so that collapsed a six-query suite into a single marked
# statement: measured in a dry run as "1 statement, 6 names", which would have
# attributed one combined timing to H1 and reported "-" for the rest.
bench_render_oneline() {
    local out
    out=$(bench_render "$1" \
      | sed -e 's/--.*$//' \
      | tr '\n' ' ' \
      | tr -s ' ' \
      | sed -e 's/^ *//' -e 's/ *$//' -e 's/ *;* *$/;/') || return 1
    [[ -n "$out" ]] || { _bench_err "$(basename "$1") rendered to nothing"; return 1; }
    printf '%s\n' "$out"
}

# bench_print_plan <hot|snapshot> — the human-readable "what am I about to run"
# block. Both harnesses print this, so a log or a CI artifact always carries the
# exact bound its numbers were measured under instead of leaving it to be
# reconstructed from a row count afterwards.
bench_print_plan() {
    local suite="$1" f name
    echo "-- ${suite} suite plan ----------------------------------------------------"
    case "$suite" in
        hot)
            printf '  partition  : dt=%s (LIVE — writers must be RUNNING)\n' "$BENCH_HOT_DT"
            printf '  windows    : [%s, %s)   (1m)\n'  "$BENCH_HOT_FROM_1M"  "$BENCH_HOT_TO"
            printf '               [%s, %s)   (5m)\n'  "$BENCH_HOT_FROM_5M"  "$BENCH_HOT_TO"
            printf '               [%s, %s)   (10m)\n' "$BENCH_HOT_FROM_10M" "$BENCH_HOT_TO"
            printf '               ^ pinned literals, rendered ONCE: every arm, pass and\n'
            printf '                 engine reads the same rows. Half-open, so fixed width.\n'
            ;;
        snapshot)
            printf '  S1-S3      : dt=%s  (partition only, no sample)\n' "$BENCH_SNAPSHOT_DT"
            printf '               ^ full-partition aggregates; expect ScanRows to equal\n'
            printf '                 the partition row count.\n'
            printf '  S4         : dt=%s AND ingestionTime in [%s, %s)\n' \
                "$BENCH_SNAPSHOT_DT" "$BENCH_S4_FROM" "$BENCH_S4_TO"
            printf '               ^ WINDOWED because the fat-JSON decode is what made this\n'
            printf '                 suite a multi-hour run. Unlike uuid, ingestionTime PRUNES.\n'
            printf '               ^ the window is on the INGESTION date (%s), which is\n' "$BENCH_SNAPSHOT_INGEST_DATE"
            printf '                 NOT the partition date (%s).\n' "$BENCH_SNAPSHOT_DT"
            ;;
    esac
    echo "  steps      :"
    while IFS= read -r f; do
        name=$(bench_query_name "$f")
        # The first `-- ` line of each file is its one-line purpose.
        printf '    %-22s %s\n' "$name" "$(sed -n 's/^-- //p' "$f" | head -1)"
    done < <(bench_query_files "$suite")
    echo ""
}
