-- Projection pushdown: group by one skinny column out of 35. event_json is
-- bench: leading-col-is-label
-- ~82% of the bytes, and a reader with working column pruning skips it.
-- H5/H4 is the fat-column tax on one read path; compare that ratio across arms.
SELECT eventId, COUNT(*) AS n
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND eventTimeDT >= '${HOT_FROM}'
  AND eventTimeDT <  '${HOT_TO}'
GROUP BY eventId
ORDER BY n DESC
LIMIT 20;
