#!/usr/bin/env bash
# Pin one version of each of the four tables in both formats, for one run (or
# one hot pass), and print the pins as shell exports:
#
#   eval "$(./querybench/pin.sh [tag])"     tag: default $STAMP
#
#   1. copies each table's current metadata.json (Iceberg: Glue
#      metadata_location; Paimon's export: version-hint.text) to
#      s3://$BUCKET_NAME/$QB_PIN_PREFIX/<tag>/, so DuckDB reads a file the
#      writers cannot delete, and reads its snapshot id (lib qb_pin_tables);
#   2. appends the pinned snapshots' state (records, data/delete files,
#      equality/position deletes) to $OUT_DIR/tables-$STAMP.tsv;
#   3. when StarRocks is in ENGINES and its FE is up: creates the catalogs and
#      checks every StarRocks arm reads every pinned table (qb_sr_check_pins:
#      REFRESH + retry, the subquery fallback, Paimon unpinned if it must be).
# Nothing here changes a table.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
TAG="${1:-$STAMP}"
ENGINES="${ENGINES:-starrocks duckdb}"
mkdir -p "$OUT_DIR"
export QB_TABLE_STATE_OUT="${QB_TABLE_STATE_OUT:-${OUT_DIR}/tables-${STAMP}.tsv}"

pins=$(qb_pin_tables "$TAG") || qb_die "pinning failed"
eval "$pins"
qb_log "pinned ${QB_TABLES} (tag ${TAG}); table state -> ${QB_TABLE_STATE_OUT##*/}"
extra=""
if [[ "$ENGINES" == *starrocks* ]] && kubectl get pod "$FE_POD" -n "$NAMESPACE" >/dev/null 2>&1; then
    "${HERE}/starrocks.sh" catalogs >&2 || qb_die "cannot create or read the StarRocks catalogs"
    extra=$(qb_sr_check_pins) || qb_die "a StarRocks arm cannot read its pins"
fi
echo "${pins}"
[[ -z "$extra" ]] || echo "$extra"
