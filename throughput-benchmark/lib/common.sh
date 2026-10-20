# lib/common.sh: what every script in throughput-benchmark/ shares. Sourced,
# not run, by each component's run.sh (datagen/, flinkwriter/, monitoring/,
# querybench/, summary/), by lib/querybench.sh and by bench.sh.
#
#   BENCH_DIR      throughput-benchmark/
#   .bench-env     sourced from BENCH_DIR: the session's values (TEST_SUFFIX, ...)
#                  and, through it, .bench-env.local's account values
#   NAMESPACE      emr-flink unless set
#   TEST_SUFFIX     checked (empty or like -test4); TEST_DB_SUFFIX = its Glue form (_test4)
#   QB_RESULTS_PREFIX  querybench-results/session<TEST_SUFFIX>: the session's query results in S3
#   log/info/warn  to stderr; die = error and exit 1 (error is the same, for the
#                  older call sites)
#   render         a manifest template -> its *-deployed.yaml (below)
[[ -n "${_BENCH_COMMON:-}" ]] && return 0
_BENCH_COMMON=1

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# .bench-env exports plain values (export X=v), so sourcing it would overwrite
# a value given for one run (`DURATION_SECONDS=120 ./datagen/run.sh start`
# silently became 0, 2026-10-20). So: remember every variable .bench-env (and
# .bench-env.local) exports that is already set, source the file, then put the
# caller's values back. The command line wins; a differing value is reported,
# which also catches a stale `source .bench-env` left in an interactive shell.
_bench_keep=()
for _v in $(sed -n -E 's/^[[:space:]]*export[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)=.*/\1/p' \
              "${BENCH_DIR}/.bench-env" "${BENCH_DIR}/.bench-env.local" 2>/dev/null | sort -u); do
    [[ -n "${!_v+set}" ]] && _bench_keep+=("${_v}=${!_v}")
done
# shellcheck disable=SC1091
source "${BENCH_DIR}/.bench-env"
for _kv in ${_bench_keep[@]+"${_bench_keep[@]}"}; do
    _v="${_kv%%=*}"
    if [[ "${!_v-}" != "${_kv#*=}" && -z "${_BENCH_OVERRIDE_SEEN:-}" ]]; then
        echo "[bench] ${_v}='${_kv#*=}' from the environment overrides .bench-env's '${!_v-}'" >&2
    fi
    export "${_kv}"
done
export _BENCH_OVERRIDE_SEEN=1      # report once, not again in every nested script
unset _bench_keep _v _kv
export NAMESPACE="${NAMESPACE:-emr-flink}"
export AWS_REGION="${AWS_REGION:-us-west-2}"
export TEST_SUFFIX="${TEST_SUFFIX:-}"
[[ "$TEST_SUFFIX" =~ ^(-[a-z0-9]+)?$ ]] || { echo "TEST_SUFFIX must be empty or like -test4, got '${TEST_SUFFIX}' (.bench-env)" >&2; exit 1; }
export TEST_DB_SUFFIX="${TEST_SUFFIX//-/_}"
# Where the session's query results go (querybench/run.sh) and are read back
# from (summary/run.sh): one prefix per TEST_SUFFIX, one <STAMP>-<suite>/ per run.
export QB_RESULTS_PREFIX="querybench-results/session${TEST_SUFFIX}"

if [[ -t 2 ]]; then _R='\033[0;31m' _G='\033[0;32m' _Y='\033[1;33m' _B='\033[0;34m' _N='\033[0m'
else _R='' _G='' _Y='' _B='' _N=''; fi
log()   { echo -e "${_G}[$(date -u +%H:%M:%SZ)]${_N} $*" >&2; }
info()  { echo -e "${_B}[INFO]${_N} $*" >&2; }
warn()  { echo -e "${_Y}[WARNING]${_N} $*" >&2; }
die()   { echo -e "${_R}[ERROR]${_N} $*" >&2; exit 1; }
error() { die "$@"; }

# render <template> <out> VAR...
# Substitutes ${VAR} for exactly the named variables, from the caller's shell
# (exported or not): envsubst leaves any other $... as it is, which the
# templates rely on for the container shells' own variables (e.g. ${rc}). Then
# fails if an UPPERCASE ${...} is left, i.e. a placeholder the caller forgot to
# name: valid YAML that would only fail later, inside the pod. *-deployed.yaml
# is gitignored: the files carry the bucket, account ID and role ARN. The caller
# applies the file (`kubectl apply -f`, or `kubectl create -f` for the
# FlinkDeployments).
render() {
    local tpl="$1" out="$2" list="" v; shift 2
    command -v envsubst >/dev/null || { echo "envsubst not found (brew install gettext)" >&2; return 1; }
    [[ -f "$tpl" ]] || { echo "template not found: ${tpl}" >&2; return 1; }
    for v in "$@"; do list+="\${${v}} "; done
    ( for v in "$@"; do export "${v}=${!v-}"; done; envsubst "$list" < "$tpl" ) > "$out"
    if grep -nE '\$\{[A-Z][A-Z0-9_]*\}' "$out" >&2; then
        echo "unrendered placeholder(s) above in ${out}: add them to the render list" >&2
        return 1
    fi
}
