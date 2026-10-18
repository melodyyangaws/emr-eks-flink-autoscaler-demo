-- High-cardinality aggregate: COUNT(DISTINCT) per customer over the hot window.
-- bench: leading-col-is-label
-- Exercises the hash table and spill, not the read path (three skinny columns).
SELECT customerId,
       COUNT(*)                          AS n,
       COUNT(DISTINCT endpointHostName)  AS hosts
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM}'
  AND ingestionTime <  '${HOT_TO}'
GROUP BY customerId
ORDER BY n DESC
LIMIT 20;
