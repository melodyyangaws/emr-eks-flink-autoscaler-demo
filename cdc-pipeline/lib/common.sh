# cdc-pipeline/lib/common.sh: what the CDC query benchmark's scripts share.
# Sourced, never run, by querybench/*.sh and lib/querybench.sh. Adapted from
# throughput-benchmark/lib/common.sh (same override rule, logging and render).
#
#   CDC_DIR            cdc-pipeline/
#   REPO_DIR           the repository root (throughput-benchmark/ is reused from there)
#   .bench-env         sourced from CDC_DIR (and, through it, .bench-env.local)
#   NAMESPACE          emr-flink unless set
#   SESSION_SUFFIX     checked (empty or like -r1)
#   ICEBERG_FORMAT_VERSION  checked (2 | 3; default 3 when unset)
#   CDC_DB_SUFFIX      checked (empty or like _v3): the table set
#   PAIMON_DB / ICEBERG_DB  flink_paimon_db<CDC_DB_SUFFIX> / flink_iceberg_db<CDC_DB_SUFFIX>
#   CDC_SET_TAG        CDC_DB_SUFFIX with - for _ (-v3), for results paths and names
#   QB_RESULTS_PREFIX  querybench-results/cdc-session<CDC_SET_TAG><SESSION_SUFFIX>
#   log/info/warn/die  to stderr
#   render             a manifest template -> its *-deployed.yaml (git-ignored)
[[ -n "${_CDC_COMMON:-}" ]] && return 0
_CDC_COMMON=1

CDC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(cd "${CDC_DIR}/.." && pwd)"
TB_DIR="${REPO_DIR}/throughput-benchmark"
# A value set for one run (`RUNS=1 ./querybench/run.sh start`) must survive
# sourcing .bench-env, which exports plain values: remember every variable it
# (or .bench-env.local) exports that is already set, source it, put them back.
# In the runner pod the env file is sourced by the Job's step first and then
# again here, so a value that differs only because of that is not reported twice.
_cdc_keep=()
for _v in $(sed -n -E 's/^[[:space:]]*export[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)=.*/\1/p' \
              "${CDC_DIR}/.bench-env" "${CDC_DIR}/.bench-env.local" 2>/dev/null | sort -u); do
    [[ -n "${!_v+set}" ]] && _cdc_keep+=("${_v}=${!_v}")
done
# shellcheck disable=SC1091
source "${CDC_DIR}/.bench-env"
for _kv in ${_cdc_keep[@]+"${_cdc_keep[@]}"}; do
    _v="${_kv%%=*}"
    if [[ "${!_v-}" != "${_kv#*=}" && -z "${_CDC_OVERRIDE_SEEN:-}" ]]; then
        echo "[cdc-bench] ${_v}='${_kv#*=}' from the environment overrides .bench-env's '${!_v-}'" >&2
    fi
    export "${_kv}"
done
export _CDC_OVERRIDE_SEEN=1
unset _cdc_keep _v _kv
export NAMESPACE="${NAMESPACE:-emr-flink}"
export AWS_REGION="${AWS_REGION:-us-west-2}"
export SESSION_SUFFIX="${SESSION_SUFFIX:-}"
[[ "$SESSION_SUFFIX" =~ ^(-[a-z0-9]+)?$ ]] || { echo "SESSION_SUFFIX must be empty or like -r1, got '${SESSION_SUFFIX}' (.bench-env)" >&2; exit 1; }
# The table set (.bench-env). Derived here, after the overrides above, so
# `CDC_DB_SUFFIX= ./querybench/run.sh ...` reaches the database names too.
export ICEBERG_FORMAT_VERSION="${ICEBERG_FORMAT_VERSION:-3}"
[[ "$ICEBERG_FORMAT_VERSION" =~ ^[23]$ ]] || { echo "ICEBERG_FORMAT_VERSION must be 2 or 3, got '${ICEBERG_FORMAT_VERSION}' (.bench-env)" >&2; exit 1; }
# Unset -> the v3 set (_v3), as in build-deploy-generic.sh and deploy-monitor.sh;
# an explicit CDC_DB_SUFFIX= is the unsuffixed v2 set.
export CDC_DB_SUFFIX="${CDC_DB_SUFFIX-_v3}"
[[ "$CDC_DB_SUFFIX" =~ ^(_[a-z0-9]+)?$ ]] || { echo "CDC_DB_SUFFIX must be empty or like _v3, got '${CDC_DB_SUFFIX}' (.bench-env)" >&2; exit 1; }
export PAIMON_DB="flink_paimon_db${CDC_DB_SUFFIX}"
export ICEBERG_DB="flink_iceberg_db${CDC_DB_SUFFIX}"
export CDC_SET_TAG="${CDC_DB_SUFFIX//_/-}"
if [[ "$ICEBERG_FORMAT_VERSION" == 3 && -z "$CDC_DB_SUFFIX" && -z "${_CDC_SET_WARNED:-}" ]]; then
    echo "[cdc-bench] WARNING: ICEBERG_FORMAT_VERSION=3 on the unsuffixed (v2) table set; a v3 run uses a fresh CDC_DB_SUFFIX (e.g. _v3)" >&2
    export _CDC_SET_WARNED=1
fi
export QB_RESULTS_PREFIX="querybench-results/cdc-session${CDC_SET_TAG}${SESSION_SUFFIX}"

if [[ -t 2 ]]; then _R='\033[0;31m' _G='\033[0;32m' _Y='\033[1;33m' _B='\033[0;34m' _N='\033[0m'
else _R='' _G='' _Y='' _B='' _N=''; fi
log()   { echo -e "${_G}[$(date -u +%H:%M:%SZ)]${_N} $*" >&2; }
info()  { echo -e "${_B}[INFO]${_N} $*" >&2; }
warn()  { echo -e "${_Y}[WARNING]${_N} $*" >&2; }
die()   { echo -e "${_R}[ERROR]${_N} $*" >&2; exit 1; }

# render <template> <out> VAR...: substitutes ${VAR} for exactly the named
# variables (envsubst; any other $... stays for the container's shell), then
# fails if an UPPERCASE ${...} is left. *-deployed.yaml is git-ignored.
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
