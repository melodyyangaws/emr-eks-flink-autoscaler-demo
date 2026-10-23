-- COUNT(*) over the snapshot window (SNAPSHOT_MINUTES of eventTimeDT in one
-- closed dt; 0 = the whole partition); the other queries are normalised by it.
-- A scan: the eventTimeDT bound rules out a COUNT(*) metadata shortcut.
SELECT COUNT(*) AS partition_rows
FROM ${TABLE}
WHERE dt = '${SNAPSHOT_DT}'
  AND eventTimeDT >= '${SNAP_FROM}'
  AND eventTimeDT <  '${SNAP_TO}';
