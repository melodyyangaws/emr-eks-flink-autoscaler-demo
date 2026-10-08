-- Projection pushdown at partition scale: group by one skinny column of 35.
--
-- STEP 2 OF 4, SNAPSHOT SUITE. H4's query shape against ~3.2-4.2 BILLION rows
-- instead of a 5-minute window. Same question — how well does each read path
-- avoid the fat event_json column — at the scale where file count and metadata
-- volume start to matter as much as per-row decode.
--
-- READ IT AGAINST S1. Both scan the whole partition; S1 needs no value column
-- at all while this one materialises eventId and hashes it. The gap is the
-- aggregate's cost over a full partition, with the scan held constant.
--
-- No sample predicate — see S1_partition_count.sql for why the uuid modulo was
-- removed on 2026-10-06.
--
-- ── IT SUBSUMES starrocks-bench.sh's q4_narrow_agg ─────────────────────────
-- q4 was this aggregate with no dt bound and no LIMIT. The GROUP BY key is
-- eventId, which has ~19 distinct values (01-datagen-source.sql:407), so
-- dropping the LIMIT changed nothing about the work done — every group was
-- always built — and the bound is what makes the result attributable to a
-- partition whose row count S1 reports. LIMIT 20 is kept so the printed table
-- stays bounded if the generator's event vocabulary grows.
SELECT eventId, COUNT(*) AS n
FROM ${TABLE}
WHERE dt = '${SNAPSHOT_DT}'
GROUP BY eventId
ORDER BY n DESC
LIMIT 20;
