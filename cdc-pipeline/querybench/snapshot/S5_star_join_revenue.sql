-- The CDC star join: revenue by order day x product category x customer state
-- bench: leading-col-is-label
-- for the items created on the closed day. All four tables, all four merges:
-- order_items (the fact, filtered to the partition), orders (order_id ->
-- customer_id; all its partitions, since an item can belong to an order from
-- an earlier day), products (product_id -> category) and customers
-- (customer_id -> state), each resolved to its current version first.
-- Dimension lookups are by the source primary key alone (product_id,
-- customer_id); the lake keys add the partition column (products is keyed on
-- (category, product_id)), but the source never moves a product between
-- categories, so the join is 1:1 per side. A duplicate key on either format
-- would multiply revenue here, which S7 checks for directly.
-- On uncompacted Iceberg this needs the order_items delete set (S1) AND one per
-- dimension table, held at once beside three join hash tables: expect it to be
-- the first to fail. On Paimon each side is a DV-filtered scan.
-- Runs unchanged on DuckDB and Trino. Group keys are strings, units is cast
-- to BIGINT and revenue to DECIMAL(38,2), and the ORDER BY is the full group
-- key (NULLs last on both engines), so results compare as text (no AVG, no
-- float, no timestamp).
SELECT o.${ORDERS_PART}                         AS order_day,
       p.category,
       c.state,
       COUNT(*)                                 AS items,
       CAST(SUM(oi.quantity) AS BIGINT)         AS units,
       CAST(SUM(oi.subtotal) AS DECIMAL(38,2))  AS revenue
FROM ${ORDER_ITEMS} oi
JOIN ${ORDERS}    o ON o.order_id    = oi.order_id
JOIN ${PRODUCTS}  p ON p.product_id  = oi.product_id
JOIN ${CUSTOMERS} c ON c.customer_id = o.customer_id
WHERE oi.${ORDER_ITEMS_PART} = '${SNAPSHOT_DT}'
GROUP BY o.${ORDERS_PART}, p.category, c.state
ORDER BY order_day, p.category, c.state;
