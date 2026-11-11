-- Hot multi-step query: a real-time "revenue by order status and product
-- bench: leading-col-is-label
-- category" board over the fresh change set, in one statement that combines
-- what H2, H6 and H9 test one at a time:
--   1. merge-on-read on BOTH fact tables: order_items inserted in the window
--      and orders changed in the window, each resolved to its current version
--      (Iceberg: equality deletes + deletion vectors; Paimon: deletion vectors);
--   2. a window function over orders, ROW_NUMBER() per order_id by updated_at:
--      on a correct read every order has exactly one live version, so
--      dup_versions (rows with rn > 1) must be 0 on every arm - the duplicate
--      check the old H7 made, kept here;
--   3. a three-way join items -> latest order -> products (the dimension side
--      for the category; dynamic join filters on product_id prune its scan);
--   4. a grouped aggregate per status x category, then two window functions
--      over the groups: RANK() of the category by revenue within its status,
--      and the status's total revenue, keeping the top 3 categories per status.
-- Both fact sides are bounded by the window, as in H9; products has no time
-- column and is read through the join filter (category is its partition).
-- No division anywhere: sums are cast to BIGINT / DECIMAL(38,2) so both engines
-- print the same text, and ties in revenue rank equally on both (RANK). The
-- result is up to (statuses x 3) rows; no row at all is ZERO_SCAN.
-- Runs unchanged on DuckDB and Trino (TIMESTAMP literals: see H1).
WITH items AS (
    SELECT oi.order_item_id, oi.order_id, oi.product_id, oi.quantity, oi.subtotal
    FROM ${ORDER_ITEMS} AS oi
    WHERE oi.${ORDER_ITEMS_PART} = '${HOT_DT}'
      AND oi.created_at >= TIMESTAMP '${HOT_FROM}'
      AND oi.created_at <  TIMESTAMP '${HOT_TO}'
),
order_versions AS (
    SELECT o.order_id, o.order_status, o.payment_method,
           ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.updated_at DESC) AS rn
    FROM ${ORDERS} AS o
    WHERE o.updated_at >= TIMESTAMP '${HOT_FROM}'
      AND o.updated_at <  TIMESTAMP '${HOT_TO}'
),
dups AS (
    SELECT CAST(COUNT(*) AS BIGINT) AS dup_versions FROM order_versions WHERE rn > 1
),
per_group AS (
    SELECT ov.order_status,
           p.category,
           COUNT(*)                                  AS items,
           COUNT(DISTINCT ov.order_id)               AS orders,
           COUNT(DISTINCT ov.payment_method)         AS payment_methods,
           CAST(SUM(i.quantity) AS BIGINT)           AS units,
           CAST(SUM(i.subtotal) AS DECIMAL(38,2))    AS revenue
    FROM items AS i
    JOIN order_versions AS ov
      ON ov.order_id = i.order_id AND ov.rn = 1
    JOIN ${PRODUCTS} AS p
      ON p.product_id = i.product_id
    GROUP BY ov.order_status, p.category
),
ranked AS (
    SELECT g.*,
           RANK() OVER (PARTITION BY g.order_status ORDER BY g.revenue DESC) AS category_rank,
           CAST(SUM(g.revenue) OVER (PARTITION BY g.order_status) AS DECIMAL(38,2)) AS status_revenue
    FROM per_group AS g
)
SELECT r.order_status, r.category_rank, r.category, r.items, r.orders, r.payment_methods,
       r.units, r.revenue, r.status_revenue, d.dup_versions
FROM ranked AS r
CROSS JOIN dups AS d
WHERE r.category_rank <= 3
ORDER BY r.order_status, r.category_rank, r.category;
