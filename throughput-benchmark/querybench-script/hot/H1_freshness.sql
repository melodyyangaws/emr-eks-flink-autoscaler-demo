-- Freshness: the newest committed row in the hot window, and how many rows it holds.
--
-- STEP 1 OF 6, HOT SUITE. Run first because it is the suite's sanity check: if
-- rows_in_window is 0 the writers are not running and every later hot number is
-- an empty scan — which is the FASTEST possible result and therefore reads as a
-- win rather than as a failure.
--
-- THE LAG IS NOT COMPUTED IN SQL, deliberately. The obvious column here is
-- TIMESTAMPDIFF(SECOND, MAX(ingestionTime), <now>), which the StarRocks-only
-- version of this query carried — but TIMESTAMPDIFF's argument order and
-- availability differ between the two engines, and <now> would have to be a
-- clock function. A clock function is exactly what the pinned-literal design
-- removes: it drifts with elapsed time (so serial arms stop being comparable)
-- and on StarRocks it is evaluated in the FE's Asia/Shanghai session against
-- UTC data, matching zero rows. Subtract newest_row from the ${HOT_TO} printed
-- in the plan header instead; both are pinned, so that arithmetic is exact.
SELECT MAX(ingestionTime) AS newest_row,
       COUNT(*)           AS rows_in_window
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM_5M}'
  AND ingestionTime <  '${HOT_TO}';
