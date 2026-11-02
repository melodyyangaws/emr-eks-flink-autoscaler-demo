-- COUNT(DISTINCT customer_id) per payment method over the closed orders
-- bench: leading-col-is-label
-- partition: the memory-pressure query (~17M distinct customers in each of 5
-- groups), on the upsert-heaviest fact table (the generator rewrites
-- order_status, total_amount, payment_method and shipping_address in place).
-- A second compaction data point beside S1: the Iceberg orders table was
-- rewritten by Athena OPTIMIZE on 2026-09-28 (106 data + 113 delete files ->
-- 2 data files, 0 deletes), while order_items was not. If one table is
-- compacted and the other is not, S1 vs S3 shows the delete-merge cost; if both
-- are, it shows the file-layout cost only. Record which.
-- payment_method is the column the upserts change, so an unapplied delete
-- shows up as a wrong n or custs, not only as time.
-- Runs unchanged on DuckDB and Trino (exact COUNT(DISTINCT) on both).
SELECT payment_method,
       COUNT(*)                    AS n,
       COUNT(DISTINCT customer_id) AS custs
FROM ${ORDERS}
WHERE ${ORDERS_PART} = '${SNAPSHOT_DT}'
GROUP BY payment_method
ORDER BY payment_method;
