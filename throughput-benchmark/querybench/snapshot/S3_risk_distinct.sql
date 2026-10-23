-- COUNT(DISTINCT customerId) over the snapshot window: the memory-pressure query.
-- bench: leading-col-is-label
-- The first column is a label (a risk level of "0" is legitimate), so the
-- zero-rows guard checks only for an empty result.
SELECT filterRiskLevel,
       COUNT(*)                   AS n,
       COUNT(DISTINCT customerId) AS custs
FROM ${TABLE}
WHERE dt = '${SNAPSHOT_DT}'
  AND eventTimeDT >= '${SNAP_FROM}'
  AND eventTimeDT <  '${SNAP_TO}'
GROUP BY filterRiskLevel
ORDER BY n DESC;
