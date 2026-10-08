-- High-cardinality aggregate: COUNT(DISTINCT) per customer over 10 minutes.
--
-- STEP 6 OF 6, HOT SUITE. The only hot query whose cost is not dominated by the
-- scan: COUNT(DISTINCT endpointHostName) grouped by customerId builds a hash
-- table per group, so this is where the memory ceiling and the spill path get
-- exercised rather than the read path. StarRocks runs it with enable_spill =
-- true; DuckDB with an explicit temp_directory, for the same reason.
--
-- It uses the WIDEST hot window (10 minutes) on purpose — the aggregate's cost
-- grows with distinct cardinality, not with bytes, so a narrower window would
-- not reach the regime this query is here to test.
--
-- Three skinny columns, no event_json, so a slow arm here is slow at
-- aggregation. Read it against H4, which is the same shape minus the DISTINCT.
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
