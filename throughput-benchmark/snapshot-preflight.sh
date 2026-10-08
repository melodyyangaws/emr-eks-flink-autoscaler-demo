#!/usr/bin/env bash
# Pre-flight gate for the SNAPSHOT suite. Sourced by starrocks-cid-audit.sh and
# duckdb-cid/run-duckdb-cid.sh.
#
# THREE STEPS, IN THIS ORDER:
#   1. verify no active cid_events query on the FE — a concurrent scan is
#      contention, and contention is worth more than the effect being measured
#   2. stop the Flink streaming jobs and wait for the TaskManagers to go away
#   3. hand back to the caller, which runs the snapshot tests
#
# RESUME IS NOT HERE. Restarting the writers is the start of the NEXT
# experiment, not cleanup of the last one: upgradeMode is stateless, so a resume
# is a fresh restart that reads whatever SQL is currently in S3 — not the SQL the
# run was measured against. Restart them deliberately, by hand:
#
#   kubectl patch flinkdeployment flink-cid-paimon -n emr-flink --type=merge \
#     -p '{"spec":{"job":{"state":"running"}}}'
#
# WHY THE SNAPSHOT SUITE NEEDS THE WRITERS STOPPED AT ALL. It scans one whole dt
# partition as accumulated/cold data, but the generator writes across TWO LIVE dt
# partitions at CID_DAY_SPREAD=1 by design (datagen's min/max are inclusive, so
# r_day draws {0,1}). The partition is therefore not closed while the writers
# run, and reading a live one makes ScanBytes drift between arms as files are
# committed mid-suite — the arms stop being comparable, which is the entire point
# of the suite. The cost is measured, not assumed: the same closed partition, the
# same 944,023,427 rows and the same query took 332,520 ms with the writers live
# and 3,742 ms with them stopped. EIGHTY-NINE TIMES, against the 1.1-1.5x format
# effect being resolved.
#
# The failure mode is silent and flattering in BOTH directions, which is why this
# is a hard gate and not a warning. A snapshot run against a live table produces a
# complete, plausible CSV whose dominant variable moved mid-run; and the hot suite
# with the writers stopped matches ZERO rows in its trailing ingestionTime
# windows, and an empty scan is the fastest possible result. Neither errors.
#
# SUSPEND_WRITERS: unset = auto (stop iff the suite includes snapshot),
#                  0 = skip (they are already stopped, or the table is
#                      deliberately live), 1 = force.

[[ -n "${_SNAPSHOT_PREFLIGHT_LOADED:-}" ]] && return 0
_SNAPSHOT_PREFLIGHT_LOADED=1

NAMESPACE="${NAMESPACE:-emr-flink}"
FE_POD="${FE_POD:-kube-starrocks-fe-0}"
# FLINK_JOBS is the name starrocks-cid-audit.sh already uses, so it is honoured
# as the default and one override still moves both harnesses.
CID_WRITER_DEPLOYMENTS="${CID_WRITER_DEPLOYMENTS:-${FLINK_JOBS:-flink-cid-paimon flink-cid-iceberg}}"
CID_STOP_TIMEOUT="${CID_STOP_TIMEOUT:-600}"
CID_STOP_INTERVAL="${CID_STOP_INTERVAL:-15}"

_pf_log() { echo "  [preflight] $*"; }
_pf_err() { echo "  [preflight] ERROR: $*" >&2; }

# ONLY the literal 1 means dry. Do NOT write `[[ -n "$DRY_RUN" ]]` here:
# run-duckdb-cid.sh normalises `DRY_RUN="${DRY_RUN:-0}"` before sourcing this
# file, so on a REAL run DRY_RUN is the non-empty string "0" and an emptiness
# test reads it as dry — the writers would be reported as "would patch" and
# never actually stopped, which is the silent flattering failure this whole
# file exists to prevent. Compared as a string, not with (( )), because an
# unexpected non-numeric value must fall through to the safe side (not dry)
# rather than raising an arithmetic error.
_pf_dry() { [[ "${DRY_RUN:-0}" == 1 ]]; }

# ── Writer state, for provenance ────────────────────────────────────────────
# "<job>=<spec.job.state>" per deployment, "absent" when the FlinkDeployment is
# gone. This is RECORDING, not control: it goes into the harnesses' output so an
# artifact found months later still says which regime produced it, and the
# harness re-checks it between suites so a state change mid-run aborts instead
# of producing a CSV whose arms are not comparable.
snapshot_writer_state() {
    local j st out=""
    for j in $CID_WRITER_DEPLOYMENTS; do
        st=$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" \
               -o jsonpath='{.spec.job.state}' 2>/dev/null || echo "absent")
        out="${out}${j}=${st:-absent} "
    done
    echo "${out% }"
}

# ── Decision ────────────────────────────────────────────────────────────────
# Split from the action so it is testable without touching the cluster: prints
# "stop <reason>" or "skip <reason>" and returns 0/1 respectively.
snapshot_stop_decision() {
    local suite="${1:-all}"
    case "${SUSPEND_WRITERS:-auto}" in
        0) echo "skip SUSPEND_WRITERS=0 (explicitly disabled)"; return 1 ;;
        1) echo "stop SUSPEND_WRITERS=1 (forced, regardless of suite)"; return 0 ;;
    esac
    case "$suite" in
        snapshot) echo "stop auto: suite=snapshot reads a dt partition as cold"; return 0 ;;
        all)      echo "stop auto: suite=all includes snapshot"; return 0 ;;
        hot)      echo "skip auto: suite=hot needs the writers RUNNING"; return 1 ;;
        *)        echo "skip auto: suite=${suite} does not include snapshot"; return 1 ;;
    esac
}

# ── Step 1: no active hot-data query ────────────────────────────────────────
# A snapshot pass is a full-partition scan per arm. Another cid_events query
# running alongside it does not merely add noise — three concurrent
# full-partition scans exhausted the BE memory pool outright on 2026-10-06
# ("Memory of default_wg exceed limit ... Used: 18786279880, Limit: 18786186952"),
# killing two arms. A leftover hot query is the common case: killing a harness
# does NOT kill the FE query it submitted, so an abandoned run keeps scanning.
#
# Matched on Info (field 10) OR on Db (field 5), because neither alone is
# sufficient: a fully-qualified statement logs against default_catalog with the
# table name only in Info, while the FE truncates Info to ~100 chars
# (ui_queries_sql_statement_max_length), so a long SELECT list or a leading
# comment can push "cid_events" past the cap and out of sight. The Db test
# catches that case. Fields verified live: NF=15, $2=Id, $5=Db, $8=Time(s),
# $10=Info.
#
# Self-exclusion is an EXACT match on "SHOW PROCESSLIST", not a substring one.
# A substring test (`$10 !~ /PROCESSLIST/`) would also skip any real cid_events
# statement whose text happened to contain the word — e.g. a trailing comment —
# punching a silent hole in the gate.
#
# THIS PROBE FAILS CLOSED. A wrong FE_POD, an FE not listening, a bad kubectl
# context or a restarting pod all produce no output, which is indistinguishable
# from a genuinely idle FE — so a typo in FE_POD would otherwise disable step 1
# entirely while printing a reassuring success line. The mysql exit status is
# therefore captured SEPARATELY from the row filtering (hence the temp file
# rather than a pipeline: a pipeline would hand back awk's status, which is 0
# even when kubectl died) and a non-zero status is a hard failure.
snapshot_assert_fe_idle() {
    local raw rc rows
    raw=$(mktemp) || { _pf_err "mktemp failed"; return 1; }
    kubectl exec -n "$NAMESPACE" "$FE_POD" -- \
        mysql -h127.0.0.1 -P9030 -uroot -N -B -e "SHOW PROCESSLIST" >"$raw" 2>/dev/null
    rc=$?
    if (( rc != 0 )); then
        rm -f "$raw"
        _pf_err "could not read SHOW PROCESSLIST from ${NAMESPACE}/${FE_POD} (exit ${rc})."
        echo "    Refusing to proceed: an unreadable FE is NOT an idle FE, and" >&2
        echo "    treating it as one would silently skip this gate. Check that" >&2
        echo "    FE_POD names a running pod and that kubectl is pointed at the" >&2
        echo "    right cluster:" >&2
        echo "      kubectl get pods -n ${NAMESPACE} | grep fe" >&2
        return 1
    fi
    # The probe's own row is always present, so an EMPTY file means the query
    # returned nothing at all — another failure wearing an idle costume.
    if [[ ! -s "$raw" ]]; then
        rm -f "$raw"
        _pf_err "SHOW PROCESSLIST returned no rows at all from ${NAMESPACE}/${FE_POD}."
        echo "    The probe's own connection should always appear, so an empty" >&2
        echo "    result is a broken probe, not an idle cluster." >&2
        return 1
    fi
    rows=$(awk -F'\t' '
        $10 == "SHOW PROCESSLIST" { next }
        ($10 ~ /cid_events/) || ($5 ~ /^flink_cid_/) {
            print $2"\t"$8"s\t"substr($10,1,70)
        }' "$raw")
    rm -f "$raw"
    if [[ -z "$rows" ]]; then
        _pf_log "FE idle: no active cid_events query"
        return 0
    fi
    _pf_err "an active cid_events query is already running on ${FE_POD}:"
    echo "$rows" | sed 's/^/      /' >&2
    echo "    A concurrent scan is contention, and contention here is worth more" >&2
    echo "    than the format effect being measured. Note that killing a harness" >&2
    echo "    does NOT kill its FE query — an abandoned run keeps scanning." >&2
    echo "    Wait for it, or kill it explicitly:" >&2
    echo "      kubectl exec -n ${NAMESPACE} ${FE_POD} -- mysql -h127.0.0.1 -P9030 -uroot -e 'KILL QUERY <id>'" >&2
    return 1
}

# ── Step 2: stop the streaming jobs ─────────────────────────────────────────
# Idempotent. An already-stopped deployment is patched anyway (a merge patch to
# the value it already holds is a no-op) and then still waited on, because
# "suspended in the spec" is not "stopped writing". A missing deployment is
# logged and skipped, not an error: tearing one down is a legitimate state.
#
# THE WAIT IS ON THE TASKMANAGERS, which is the whole proof. The patch returns
# immediately; the operator then has to cancel the job, the JM to finish the
# in-flight checkpoint, and the TMs to drain their write buffers. A
# half-suspended job still commits, and a commit landing between arm1 and arm3
# is exactly the corruption this gate exists to prevent. Once no TaskManager pod
# exists, no process remains that could write to the warehouse — that is a
# sufficient proof and it needs no S3 polling to establish.
#
# THE TM COUNT IS MATCHED ON POD NAME, NOT ON A LABEL SELECTOR, and that is
# deliberate. A label selector that matches nothing is indistinguishable from
# "no pods left": it reports success on the first poll and the wait silently
# proves nothing. The operator's own label scheme has moved between versions
# (`component=taskmanager` vs `app.kubernetes.io/*`), so pinning one here makes
# the proof depend on a version detail that fails OPEN when it is wrong. The
# operator names every TM pod `<deployment>-taskmanager-<n>-<m>`, which is
# stable API, and a name prefix cannot silently match the empty set. The
# `-l app=<job>` filter is kept as a cheap narrowing but is NOT relied on:
# the name grep is what decides.
snapshot_stop_writers() {
    local j st patched=""

    for j in $CID_WRITER_DEPLOYMENTS; do
        st=$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" \
               -o jsonpath='{.spec.job.state}' 2>/dev/null || true)
        if [[ -z "$st" ]]; then
            _pf_log "${j}: absent, nothing to stop"
            continue
        fi
        if [[ "$st" == "suspended" ]]; then
            _pf_log "${j}: already spec.job.state=suspended"
        elif _pf_dry; then
            _pf_log "${j}: DRY_RUN — would patch spec.job.state=suspended"
            continue
        else
            _pf_log "${j}: patching spec.job.state=suspended"
            kubectl patch flinkdeployment "$j" -n "$NAMESPACE" --type=merge \
                -p '{"spec":{"job":{"state":"suspended"}}}' >/dev/null || {
                _pf_err "patch failed for ${j}"; return 1; }
        fi
        patched="${patched}${j} "
    done

    if _pf_dry; then
        _pf_log "DRY_RUN — skipping the stop wait"
        return 0
    fi
    [[ -z "${patched// /}" ]] && { _pf_log "no deployments present; nothing to wait for"; return 0; }

    local deadline=$(( $(date +%s) + CID_STOP_TIMEOUT ))
    local jobs_ok pods_ok st2 tms
    while (( $(date +%s) < deadline )); do
        jobs_ok=1 pods_ok=1
        for j in $patched; do
            st2=$(kubectl get flinkdeployment "$j" -n "$NAMESPACE" \
                    -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true)
            # An empty state means the deployment went away under us, which is
            # as stopped as it gets. RUNNING is the only disqualifier.
            [[ "$st2" == "RUNNING" ]] && jobs_ok=0
            tms=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null \
                  | awk -v p="${j}-taskmanager-" '$1 ~ "^"p {n++} END {print n+0}')
            (( ${tms:-0} > 0 )) && pods_ok=0
        done
        if (( jobs_ok == 1 && pods_ok == 1 )); then
            _pf_log "writers stopped: jobs no longer RUNNING, no TaskManager pods left"
            return 0
        fi
        _pf_log "waiting: jobs_settled=${jobs_ok} taskmanagers_gone=${pods_ok}"
        sleep "$CID_STOP_INTERVAL"
    done

    # FAIL LOUDLY. A benchmark against a still-committing table is worse than no
    # benchmark: it produces a complete, plausible CSV whose dominant variable
    # moved mid-run. Never degrade to "proceed anyway".
    _pf_err "writers did not stop within ${CID_STOP_TIMEOUT}s."
    echo "    state: $(snapshot_writer_state)" >&2
    echo "    Something is still able to commit into the warehouse. Check:" >&2
    echo "      kubectl get flinkdeployment -n ${NAMESPACE}" >&2
    echo "      kubectl get pods -n ${NAMESPACE} | grep cid" >&2
    return 1
}

# ── The one call a harness makes ────────────────────────────────────────────
# Returns 0 if the run may proceed, 1 if it must not. Sets
# CID_WRITERS_STOPPED=1 when this run did the stopping.
snapshot_preflight() {
    local suite="${1:-all}" decision verdict
    # `|| true` is LOAD-BEARING. snapshot_stop_decision returns 1 to mean
    # "skip", and under `set -e` a failing command substitution inside an
    # assignment kills the whole shell. run-duckdb-cid.sh runs `set -euo
    # pipefail`, so without this every SUITE=hot run aborts here before printing
    # a line — the skip path, i.e. the common case. starrocks-cid-audit.sh runs
    # `set -uo pipefail` (no -e) and is silently unaffected, which is exactly why
    # testing the function in isolation does not catch it. The verdict is read
    # from the TEXT below, never from $?.
    decision=$(snapshot_stop_decision "$suite") || true
    verdict=${decision%% *}
    _pf_log "suite=${suite} SUSPEND_WRITERS=${SUSPEND_WRITERS:-auto} -> ${decision}"
    [[ "$verdict" == skip ]] && return 0

    # Step 1 before step 2, deliberately: if the FE is busy the run cannot
    # proceed anyway, and stopping the writers first would have taken the
    # cluster down for a run that then aborts.
    snapshot_assert_fe_idle || return 1
    snapshot_stop_writers   || return 1
    CID_WRITERS_STOPPED=1
    return 0
}
