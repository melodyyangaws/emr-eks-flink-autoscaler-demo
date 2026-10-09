-- Projection pushdown at partition scale: H4's shape over billions of rows,
-- bench: leading-col-is-label
-- where file count and metadata volume matter as much as decode. S2 - S1 is
-- the aggregate's cost with the scan held constant.
SELECT eventId, COUNT(*) AS n
FROM ${TABLE}
WHERE dt = '${SNAPSHOT_DT}'
GROUP BY eventId
ORDER BY n DESC
LIMIT 20;
