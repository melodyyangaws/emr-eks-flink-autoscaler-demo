-- Freshness: the newest committed row in the last 5 minutes, and how many rows.
-- The suite's sanity check: rows_in_window = 0 means the writers are not
-- running, and every later hot number is a (fast, flattering) empty scan.
-- Lag = HOT_TO - newest_row, worked out outside SQL so no clock function is used.
SELECT MAX(ingestionTime) AS newest_row,
       COUNT(*)           AS rows_in_window
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM_5M}'
  AND ingestionTime <  '${HOT_TO}';
