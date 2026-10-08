-- Narrowest window: COUNT(*) over the last minute of the hot partition.
--
-- STEP 2 OF 6, HOT SUITE. The cheapest query in either suite, and the floor it
-- establishes is what makes the others readable: it is a count with NO value
-- columns, over the smallest window, so its cost is almost entirely file
-- enumeration and partition/stat pruning rather than data movement.
--
-- Read H5 against this one. H5 is the SAME shape of aggregate over a 5x wider
-- window with the fat JSON column added; the gap between them is the decode
-- cost of event_json, isolated from the planning cost both pay.
SELECT COUNT(*) AS rows_in_window
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM_1M}'
  AND ingestionTime <  '${HOT_TO}';
