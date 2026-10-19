-- Freshness: the newest committed row in the hot window, and how many rows.
-- The suite's sanity check: rows_in_window = 0 means the writers are not
-- running, and every later hot number is a (fast, flattering) empty scan.
-- Lag = HOT_TO - newest_row, worked out outside SQL so no clock function is used.
-- The window is on eventTimeDT (set by the generator), so every format holds the
-- same rows in it; ingestionTime is each writer's own clock (2026-10-18).
SELECT MAX(ingestionTime) AS newest_row,
       COUNT(*)           AS rows_in_window
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND eventTimeDT >= '${HOT_FROM}'
  AND eventTimeDT <  '${HOT_TO}';
