-- Fat-JSON decode over a SNAPSHOT_S4_MINUTES (3) slice of the closed partition,
-- bench: io-tasks=1
-- on eventTimeDT: set by the generator, so every arm reads exactly the same rows
-- in the same time slot (ingestionTime is each writer's own clock: 471M vs 480M).
-- Only this query is windowed: unbounded it ran 18-41 min per pass per arm.
-- eventTimeDT prunes because rows are written close to their event time.
-- The window is centred in both tables' commit span, so it falls inside the run
-- that wrote them whatever its date (lib.sh qb_resolve_snapshot).
-- If StarRocks OOMs in the scan, cap connector_io_tasks; enable_spill can't reach it.
SELECT COUNT(*)                     AS n,
       SUM(${BYTES_FN}(event_json)) AS payload_bytes
FROM ${TABLE}
WHERE dt = '${SNAPSHOT_DT}'
  AND eventTimeDT >= '${S4_FROM}'
  AND eventTimeDT <  '${S4_TO}';
