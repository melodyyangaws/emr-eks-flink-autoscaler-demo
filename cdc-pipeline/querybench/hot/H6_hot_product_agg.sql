-- Grouped aggregate with a distinct count: per-product sales of the order_items
-- bench: leading-col-is-label
-- rows inserted in the window. Up to ~20,000 groups (the generator draws
-- product_id from a 20,000-id sample) with COUNT(DISTINCT order_id) inside each:
-- exercises the hash table, not the read path (four narrow columns).
-- product_id breaks ties so every arm returns the same first rows.
SELECT oi.product_id,
       COUNT(*)                    AS n,
       SUM(oi.quantity)            AS units,
       SUM(oi.subtotal)            AS revenue,
       COUNT(DISTINCT oi.order_id) AS orders
FROM ${ORDER_ITEMS} AS oi
WHERE oi.${ORDER_ITEMS_PART} = '${HOT_DT}'
  AND oi.created_at >= '${HOT_FROM}'
  AND oi.created_at <  '${HOT_TO}'
GROUP BY oi.product_id
ORDER BY n DESC, oi.product_id
LIMIT 20;
