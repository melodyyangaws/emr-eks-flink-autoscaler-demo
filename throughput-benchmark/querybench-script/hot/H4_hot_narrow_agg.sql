-- Projection pushdown: group by one skinny column out of 35.
--
-- STEP 4 OF 6, HOT SUITE. Measures how well each read path AVOIDS the fat
-- event_json column, which carries ~82% of the table's bytes (~3.3 KB of a
-- ~4 KiB row). Only eventId is needed, so a reader with working column pruning
-- touches a small fraction of each file while one without it reads everything.
--
-- PAIR IT WITH H5. H5 is the same window and the same aggregate shape but DOES
-- read event_json. H5/H4 is therefore the fat-column tax on one read path, and
-- comparing that RATIO across arms is more robust than comparing either
-- absolute number, because the ratio cancels whatever each arm pays for file
-- enumeration.
SELECT eventId, COUNT(*) AS n
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM_5M}'
  AND ingestionTime <  '${HOT_TO}'
GROUP BY eventId
ORDER BY n DESC
LIMIT 20;
