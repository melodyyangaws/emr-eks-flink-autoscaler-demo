-- Top-N: the 20 customers with the highest spend on the closed day, with
-- their current name and state. orders (partition) aggregated to ~17M groups,
-- then a TopN, then the customers lookup: hash aggregation + TopN + join, the
-- dashboard shape.
-- spend is SUM(total_amount) over each order's CURRENT total_amount: the upsert
-- generator rewrites total_amount in place, so an unapplied delete (an old
-- version still visible) inflates spend and reorders the top 20. The
-- customers side is likewise its current name/state, not the first version.
-- customer_id breaks ties, so the 20 rows are the same on every arm and engine.
-- Aggregate first, join after: written that way so both engines run the same
-- plan shape instead of each optimiser choosing whether to join 95M rows to
-- customers before grouping. ORDER BY + LIMIT inside the CTE is kept by both
-- engines (a TopN); the outer ORDER BY fixes the output order.
-- Runs unchanged on DuckDB and Trino; spent is cast to DECIMAL(38,2) (the type
-- both give SUM(DECIMAL(12,2))) so both print the same text.
WITH spend AS (
    SELECT customer_id,
           COUNT(*)                                  AS n_orders,
           CAST(SUM(total_amount) AS DECIMAL(38,2))  AS spent
    FROM ${ORDERS}
    WHERE ${ORDERS_PART} = '${SNAPSHOT_DT}'
    GROUP BY customer_id
    ORDER BY spent DESC, customer_id
    LIMIT 20
)
SELECT s.customer_id,
       c.customer_name,
       c.state,
       s.n_orders,
       s.spent
FROM spend s
JOIN ${CUSTOMERS} c ON c.customer_id = s.customer_id
ORDER BY s.spent DESC, s.customer_id;
