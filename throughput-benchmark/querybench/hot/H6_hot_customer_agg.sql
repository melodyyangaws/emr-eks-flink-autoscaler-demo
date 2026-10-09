-- High-cardinality aggregate: COUNT(DISTINCT) per customer over 10 minutes.
-- bench: leading-col-is-label
-- Exercises the hash table and spill, not the read path (three skinny columns).
-- It uses the widest window because the cost grows with distinct cardinality.
SELECT customerId,
       COUNT(*)                          AS n,
       COUNT(DISTINCT endpointHostName)  AS hosts
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM_10M}'
  AND ingestionTime <  '${HOT_TO}'
GROUP BY customerId
ORDER BY n DESC
LIMIT 20;
