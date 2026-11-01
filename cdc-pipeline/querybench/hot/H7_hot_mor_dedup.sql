-- Merge-on-read on the hottest table: resolve order_items' window rows to their
-- current version and read the columns upserts change (quantity, unit_price ->
-- subtotal), with the key needed to merge. This is the hot query where the two
-- delete designs differ most:
--   Iceberg (equality deletes): the upsert sink writes one equality delete per
--     row written, inserts included, so the reader anti-joins the window's rows
--     against every equality delete in the partition committed after them. Keys
--     rewritten within one checkpoint are deleted by position instead: v3 set
--     (default, Iceberg 1.12 Flink writer) as deletion vectors in Puffin files,
--     v2 set as position-delete files. Upserts of keys from earlier checkpoints
--     are equality deletes in both.
--   Paimon (deletion vectors): the writer already found the old version at
--     commit (lookup compaction) and marked it in a bitmap; the reader skips
--     bitmapped rows, no key join.
-- dup_rows must be 0 on every arm: > 0 means a superseded version was returned
-- (a delete not applied, a deletion vector missed). Within one partition the
-- source key alone is unique. units / revenue must match across formats when
-- both pipelines committed past HOT_TO.
-- What can flatter an arm: Paimon's Iceberg export omits files the native
-- reader sees (v3 set: level-0 files, about the last checkpoint, with the
-- deletion vectors exported; v2 set: everything below the top LSM level, and no
-- deletion vectors), so pmn_as_iceberg / DuckDB paimon may read fewer,
-- already-merged rows (check H1); and the window holds fresh
-- inserts, so on Iceberg the deletes mostly match nothing - they still cost the
-- join. Record the pinned snapshot's delete-file and equality-delete counts for
-- this partition next to the time; that is what decides it.
SELECT COUNT(*)                                   AS live_rows,
       COUNT(DISTINCT oi.order_item_id)           AS distinct_keys,
       COUNT(*) - COUNT(DISTINCT oi.order_item_id) AS dup_rows,
       SUM(oi.quantity)                           AS units,
       SUM(oi.subtotal)                           AS revenue,
       SUM(oi.quantity * oi.unit_price)           AS revenue_recomputed
FROM ${ORDER_ITEMS} AS oi
WHERE oi.${ORDER_ITEMS_PART} = '${HOT_DT}'
  AND oi.created_at >= '${HOT_FROM}'
  AND oi.created_at <  '${HOT_TO}';
