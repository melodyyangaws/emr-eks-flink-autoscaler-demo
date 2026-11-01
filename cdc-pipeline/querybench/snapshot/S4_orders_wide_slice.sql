-- Whole-row decode of orders (every column, the string ones measured in bytes)
-- over an order_date slice of the closed partition. The CDC tables have no fat
-- JSON column, so orders' four string columns are the widest fact row there is.
-- Only this query is time-sliced: a partition at the generator's ~69k rows/s
-- grows by ~250M rows an hour, and an unbounded full-row decode would scale
-- with how long the writers ran rather than with the format.
-- order_date is set by MySQL at insert and never updated by the upsert
-- generator, so both formats hold exactly the same rows in the slice. MySQL
-- TIMESTAMP is whole seconds, so Paimon's TIMESTAMP(6) and Iceberg's
-- TIMESTAMP(3) hold the same values and a seconds-aligned bound cuts both alike.
-- The slice prunes by min/max order_date: Iceberg writes 'full' metrics for
-- every column, Paimon's own stats and its Iceberg export carry bounds too,
-- so pruning on this timestamp is comparable. It is NOT comparable on long
-- strings (Iceberg 'full' keeps whole bounds, Paimon truncates), which is why
-- the slice is on a timestamp. How well it prunes still depends on file layout,
-- which compaction changes (a bin-packed file spans the whole day): record the
-- compaction state with the result.
SELECT COUNT(*)                         AS n,
       SUM(${BYTES_FN}(shipping_address)
         + ${BYTES_FN}(payment_method)
         + ${BYTES_FN}(order_status)
         + ${BYTES_FN}(${ORDERS_PART}))  AS string_bytes,
       SUM(total_amount)                AS revenue,
       SUM(customer_id)                 AS customer_id_sum,
       SUM(order_id)                    AS order_id_sum,
       COUNT(updated_at)                AS updated_at_n
FROM ${ORDERS}
WHERE ${ORDERS_PART} = '${SNAPSHOT_DT}'
  AND order_date >= '${S_FROM}'
  AND order_date <  '${S_TO}';
