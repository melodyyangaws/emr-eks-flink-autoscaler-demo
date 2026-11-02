-- Grouped aggregate with a distinct count: per-product sales of the order_items
-- bench: leading-col-is-label
-- rows inserted in the window. Up to ~20,000 groups (the generator draws
-- product_id from a 20,000-id sample) with COUNT(DISTINCT order_id) inside each:
-- exercises the hash table, not the read path (four narrow columns).
-- product_id breaks ties so every arm returns the same first rows.
-- Runs unchanged on DuckDB and Trino (TIMESTAMP literals: see H1); units cast
-- to BIGINT and revenue to DECIMAL(38,2) so both engines print the same text.
SELECT oi.product_id,
       COUNT(*)                                  AS n,
       CAST(SUM(oi.quantity) AS BIGINT)          AS units,
       CAST(SUM(oi.subtotal) AS DECIMAL(38,2))   AS revenue,
       COUNT(DISTINCT oi.order_id)               AS orders
FROM ${ORDER_ITEMS} AS oi
WHERE oi.${ORDER_ITEMS_PART} = '${HOT_DT}'
  AND oi.created_at >= TIMESTAMP '${HOT_FROM}'
  AND oi.created_at <  TIMESTAMP '${HOT_TO}'
GROUP BY oi.product_id
ORDER BY n DESC, oi.product_id
LIMIT 20;
