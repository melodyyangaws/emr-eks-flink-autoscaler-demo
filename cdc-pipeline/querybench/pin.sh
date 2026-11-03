#!/usr/bin/env bash
# Pin one version of each of the four tables in every format of FORMATS
# (iceberg, native Paimon), for one run (or one hot pass), and print the pins
# as shell exports:
#
#   eval "$(./querybench/pin.sh [tag])"     tag: default $STAMP
#
#   1. iceberg: copies each table's current metadata.json (Glue
#      metadata_location) to s3://$BUCKET_NAME/$QB_PIN_PREFIX/<tag>/iceberg/<table>/,
#      so DuckDB reads a file the writer cannot delete, and reads its snapshot id
#      (Trino FOR VERSION AS OF); paimon: the native snapshot N of
#      <table>/snapshot/LATEST, which DuckDB reads as paimon_scan(...,
#      snapshot_from_id=N) and Trino as paimon.<db>.<t> FOR VERSION AS OF N
#      (lib qb_pin_tables): one pin per format, both engines;
#   2. appends the pinned snapshots' state (records, data/delete files,
#      equality/position deletes, bytes; Paimon: records and commit kind) to
#      $OUT_DIR/tables-$STAMP.tsv;
#   3. when trino is in ENGINES and its coordinator is up: checks Trino reads
#      every table at its pinned snapshot (qb_trino_check_pins: retried while
#      the coordinator's view of the table is older than the pin; a paimon
#      table it cannot read is a WARNING).
# Nothing here changes a table.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/querybench.sh
source "${HERE}/../lib/querybench.sh"
TAG="${1:-$STAMP}"
mkdir -p "$OUT_DIR"
export QB_TABLE_STATE_OUT="${QB_TABLE_STATE_OUT-${OUT_DIR}/tables-${STAMP}.tsv}"

pins=$(qb_pin_tables "$TAG") || qb_die "pinning failed"
eval "$pins"
qb_log "pinned ${QB_TABLES} in $(qb_formats) (tag ${TAG})${QB_TABLE_STATE_OUT:+; table state -> ${QB_TABLE_STATE_OUT##*/}}"
if qb_has_engine trino && kubectl get "$TRINO_COORDINATOR" -n "$NAMESPACE" >/dev/null 2>&1; then
    qb_trino_check_pins
fi
echo "${pins}"
