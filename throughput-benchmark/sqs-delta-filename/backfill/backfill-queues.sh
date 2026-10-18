#!/usr/bin/env bash
# Thin wrapper: load BUCKET_NAME / AWS_REGION / AWS_ACCOUNT_ID from the
# benchmark env, then run backfill_queues.py with all arguments passed through.
#
#   ./backfill-queues.sh --prefix cid-landing-jsongz/ --count-only
#   ./backfill-queues.sh --prefix cid-landing-jsongz/ --limit 20
#   ./backfill-queues.sh --prefix cid-landing-jsongz/
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$here"
# shellcheck disable=SC1091
source ../../.bench-env

if [[ "${AWS_ACCOUNT_ID:-}" == "account" || -z "${BUCKET_NAME:-}" ]]; then
  echo "WARNING: .bench-env still has placeholder values; create" \
       "throughput-benchmark/.bench-env.local (see .bench-env)." >&2
fi
export BUCKET_NAME AWS_REGION AWS_ACCOUNT_ID
exec python3 "$here/backfill_queues.py" "$@"
