-- Narrow aggregate over the same rows S1 reads: GROUP BY one low-cardinality
-- bench: leading-col-is-label
-- column (quantity, 1-10), summing one decimal. S2 - S1 is the aggregate's cost
-- with the scan and the delete merge held constant, the counterpart of the
-- throughput suite's S2 - S1.
-- Projection: two value columns plus the key columns the merge needs. On
-- Iceberg the equality-delete key (created_dt, order_item_id) is read whatever
-- the query projects, so a narrow query narrows Paimon's read more than
-- Iceberg's while deletes are live. That is a real format cost, not a bias.
-- Runs unchanged on DuckDB and Trino. SUM over DECIMAL(12,2) is exact on both;
-- the cast to DECIMAL(38,2) (the type both give it) pins the printed scale, so
-- results compare as text.
SELECT quantity,
       COUNT(*)                              AS n,
       CAST(SUM(subtotal) AS DECIMAL(38,2))  AS revenue
FROM ${ORDER_ITEMS}
WHERE ${ORDER_ITEMS_PART} = '${SNAPSHOT_DT}'
GROUP BY quantity
ORDER BY quantity;
