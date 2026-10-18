-- The planning floor: COUNT(*) over H1's window with no value columns, so the
-- cost is almost all file listing and pruning.
SELECT COUNT(*) AS rows_in_window
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM}'
  AND ingestionTime <  '${HOT_TO}';
