-- Fat-JSON decode over a 30-minute slice of the closed partition.
-- Only this query is windowed: unbounded it ran 18-41 min per pass per arm.
-- ingestionTime prunes because it is monotonic with write order (uuid would not).
-- The window is on the INGESTION day: rows of dt=D are written on D+1, so a
-- slice inside D itself matches zero rows (lib.sh qb_resolve_snapshot).
-- If StarRocks OOMs in the scan, cap connector_io_tasks; enable_spill can't reach it.
SELECT COUNT(*)                     AS n,
       SUM(${BYTES_FN}(event_json)) AS payload_bytes
FROM ${TABLE}
WHERE dt = '${SNAPSHOT_DT}'
  AND ingestionTime >= '${S4_FROM}'
  AND ingestionTime <  '${S4_TO}';
