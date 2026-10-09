-- Narrowest window: COUNT(*) over the last minute. No value columns, so the
-- cost is almost all file listing and pruning: the planning floor.
SELECT COUNT(*) AS rows_in_window
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM_1M}'
  AND ingestionTime <  '${HOT_TO}';
