-- Dedup correctness: for each table, live rows vs distinct source primary keys.
-- bench: leading-col-is-label
-- A correct upsert sink has dup_rows = 0 everywhere: every key resolved to
-- exactly one current version. dup_rows > 0 means a reader or writer kept a
-- superseded version (an equality delete not applied, a DV missed, a replayed
-- checkpoint, Paimon's Iceberg export exposing an LSM level twice).
-- Keys are the MySQL primary keys alone, not the lake keys (which add the
-- partition column), so a key duplicated across two partitions is caught too.
-- The source never moves a row between partitions, so there should be none.
-- Fact tables are bounded to the closed partition, dimensions are whole.
-- Compare across arms row by row, not only the first row:
--   paimon vs pmn_as_iceberg (same files, different reader): live_rows must be
--     equal. The export omits files the native reader sees. On the v2 set it
--     lists only the top LSM level (no deletion vectors exported): on
--     2026-09-30 that was 0.4-9.2 percent FEWER rows than the native reader at
--     the same snapshot id. On the v3 set (default; Paimon 2.1 exports bitmap64
--     deletion vectors) only level-0 files are left out. Either way, fully
--     compact Paimon before the snapshot run or that gap is timed as a speed-up.
--   paimon vs iceberg (different writers): equal only if both pipelines had
--     consumed the same binlog position when the writers stopped (orders
--     differed by 8 rows on 2026-09-28). A gap here is pipeline state, not a
--     reader bug, but it does mean the arms read different data.
-- COUNT(DISTINCT) forces a full read of the key column, so no metadata
-- shortcut applies to any table.
SELECT 'customers'                          AS tbl,
       COUNT(*)                             AS live_rows,
       COUNT(DISTINCT customer_id)          AS distinct_keys,
       COUNT(*) - COUNT(DISTINCT customer_id) AS dup_rows
FROM ${CUSTOMERS}
UNION ALL
SELECT 'order_items',
       COUNT(*),
       COUNT(DISTINCT order_item_id),
       COUNT(*) - COUNT(DISTINCT order_item_id)
FROM ${ORDER_ITEMS}
WHERE ${ORDER_ITEMS_PART} = '${SNAPSHOT_DT}'
UNION ALL
SELECT 'orders',
       COUNT(*),
       COUNT(DISTINCT order_id),
       COUNT(*) - COUNT(DISTINCT order_id)
FROM ${ORDERS}
WHERE ${ORDERS_PART} = '${SNAPSHOT_DT}'
UNION ALL
SELECT 'products',
       COUNT(*),
       COUNT(DISTINCT product_id),
       COUNT(*) - COUNT(DISTINCT product_id)
FROM ${PRODUCTS}
ORDER BY tbl;
