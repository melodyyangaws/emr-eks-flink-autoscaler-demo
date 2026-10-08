-- COUNT(DISTINCT) over a full partition: the memory-pressure query.
-- bench: leading-col-is-label
--
-- STEP 3 OF 4, SNAPSHOT SUITE. COUNT(DISTINCT customerId) across ~3.2-4.2
-- billion rows, grouped by a low-cardinality label. The distinct set is what
-- costs: it is built over the whole partition rather than a window, which is
-- why StarRocks pins enable_spill = true for this suite and DuckDB needs an
-- explicit temp_directory. Measured at 148 s on StarRocks, making it the
-- slowest query in the suite after S4.
--
-- THE ZERO-ROWS GUARD IS RELAXED FOR THIS QUERY — that is the `bench:
-- leading-col-is-label` directive above. Its first column is filterRiskLevel, a
-- LABEL, not a count. Testing "is the first column the string '0'?" asks whether
-- the risk level is literally "0", which is a legitimate value; the guard fired
-- on a perfectly good 5-group result and scored it ZERO_SCAN. For a GROUP BY an
-- empty result set is the only sound signal, so that is all that is checked.
-- The directive is read by both harnesses from this file, so the exemption
-- travels with the query instead of being duplicated in each runner.
--
-- No sample predicate — see S1_partition_count.sql for why the uuid modulo was
-- removed on 2026-10-06.
SELECT filterRiskLevel,
       COUNT(*)                   AS n,
       COUNT(DISTINCT customerId) AS custs
FROM ${TABLE}
WHERE dt = '${SNAPSHOT_DT}'
GROUP BY filterRiskLevel
ORDER BY n DESC;
