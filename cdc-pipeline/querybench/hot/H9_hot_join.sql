-- Hot join: order_items inserted in the window joined to the orders that
-- bench: leading-col-is-label
-- changed in the window, by order status. Both sides pay their merge-on-read
-- cost at once; the closest query here to a real-time operational dashboard.
-- Both sides are bounded by the window, which keeps the Iceberg delete sets
-- bounded too (only deletes committed after the window's files apply). An
-- unbounded orders side would make every arm read, and Iceberg merge, the whole
-- orders history.
-- Result size is set by the generator, not the formats: items reference its
-- 20,000-order sample (the newest orders when it started), and only the sample
-- orders updated in the window match - of the order of 10^3 orders and 10^4
-- items per 30 s at the 2026-10 settings (an estimate, not measured). No row at
-- all is ZERO_SCAN. orders.order_id is the source key, unique across
-- partitions, so the orders side needs no partition filter (and cannot have
-- one: updated orders stay in their creation-date partition).
-- DuckDB and Trino both push dynamic join filters from the items side into the
-- orders scan by order_id. A big gap between engines here is that, first.
-- Runs unchanged on DuckDB and Trino (TIMESTAMP literals: see H1); sums cast
-- to BIGINT / DECIMAL(38,2) so both engines print the same text.
SELECT o.order_status,
       COUNT(*)                                  AS items,
       COUNT(DISTINCT o.order_id)                AS orders,
       CAST(SUM(oi.quantity) AS BIGINT)          AS units,
       CAST(SUM(oi.subtotal) AS DECIMAL(38,2))   AS revenue
FROM ${ORDER_ITEMS} AS oi
JOIN ${ORDERS} AS o
  ON o.order_id = oi.order_id
WHERE oi.${ORDER_ITEMS_PART} = '${HOT_DT}'
  AND oi.created_at >= TIMESTAMP '${HOT_FROM}'
  AND oi.created_at <  TIMESTAMP '${HOT_TO}'
  AND o.updated_at  >= TIMESTAMP '${HOT_FROM}'
  AND o.updated_at  <  TIMESTAMP '${HOT_TO}'
GROUP BY o.order_status
ORDER BY o.order_status;
