-- The floor: COUNT(*) of order_items rows inserted in the window, no value
-- columns. On the append-only throughput table this is almost all listing and
-- pruning; on an upsert table it is NOT metadata-only on any arm, because a
-- live-row count must first resolve deletes:
--   Iceberg: the Flink upsert sink writes an equality delete for EVERY row,
--     inserts included (it cannot know a key is new), so the reader joins the
--     window's rows against the partition's equality deletes committed after
--     them (key columns order_item_id + partition column).
--   Paimon: deletion-vector bitmaps per file, no key join.
-- So H2 = planning + created_at pruning + delete resolution. Compare with H3
-- (same rows, one column, nothing returned) and H7 (same rows, key + values).
-- order_items has no change timestamp: hot-key updates to older rows land in
-- the partition of their creation date and are invisible to this window.
-- Runs unchanged on DuckDB and Trino (TIMESTAMP literals: see H1).
SELECT COUNT(*) AS rows_in_window
FROM ${ORDER_ITEMS} AS oi
WHERE oi.${ORDER_ITEMS_PART} = '${HOT_DT}'
  AND oi.created_at >= TIMESTAMP '${HOT_FROM}'
  AND oi.created_at <  TIMESTAMP '${HOT_TO}';
