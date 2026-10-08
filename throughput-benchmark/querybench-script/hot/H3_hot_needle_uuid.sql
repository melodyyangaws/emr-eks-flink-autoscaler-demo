-- Needle in the haystack: point lookup on a uuid that cannot exist.
-- bench: allow-zero-rows
--
-- STEP 3 OF 6, HOT SUITE. RETURNS ZERO ROWS BY DESIGN, and that is not a
-- failure — the all-zero node uuid cannot be produced by the generator. Hence
-- the `bench: allow-zero-rows` directive above: it exempts this query from the
-- empty-result guard every harness applies, and it lives HERE rather than as a
-- hardcoded name list inside each harness, so the exemption cannot drift from
-- the query it describes.
--
-- WHAT IT MEASURES, AND WHY IT IS NOT A LOOKUP. The table has NO primary key,
-- no index and no sort order on uuid, so this cannot become a seek. uuid is
-- MD5-derived (sql-scripts/common/01-datagen-source.sql:345) and therefore
-- uniformly random, so every Parquet row group's min/max for it spans the whole
-- keyspace and the predicate prunes NOTHING. It is a full scan of the window
-- that happens to return no rows.
--
-- So this is the control for the suite: identical window to H1 and H4, two
-- skinny projected columns, and a filter that provably cannot help. Any arm
-- that is much faster here than on H4 is winning on aggregation, not on scan.
--
-- ── IT SUBSUMES starrocks-bench.sh's q3_needle_uuid ────────────────────────
-- Same projection and the same all-zero uuid literal; q3 simply had no bound,
-- so it was this control at whole-table scale — minutes to prove a predicate
-- prunes nothing. The conclusion does not depend on the scan's size: uuid is
-- MD5-derived and uniformly random, so EVERY row group's min/max spans the
-- keyspace at any window width. A 5-minute window demonstrates it for the
-- price of H1.
SELECT uuid, eventTimeDT
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM_5M}'
  AND ingestionTime <  '${HOT_TO}'
  AND uuid = '00000000-0000-4000-0000-000000000000';
