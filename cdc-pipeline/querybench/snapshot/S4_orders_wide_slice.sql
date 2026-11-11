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
-- every column, so pruning on this timestamp is comparable across readers. It
-- is NOT comparable on long strings (Iceberg 'full' keeps whole bounds, Paimon
-- truncates), which is why the slice is on a timestamp. How well it prunes
-- still depends on file layout, which compaction changes (a bin-packed file
-- spans the whole day): record the compaction state with the result.
-- Runs unchanged on DuckDB and Trino. The bounds are TIMESTAMP literals (Trino
-- has no implicit VARCHAR -> TIMESTAMP coercion in a comparison). string_bytes
-- is UTF-8 bytes via the BYTE_LEN_OPEN / BYTE_LEN_CLOSE dialect pair (DuckDB
-- STRLEN, Trino LENGTH of TO_UTF8); integer sums are cast to BIGINT and the
-- decimal sum to DECIMAL(38,2) so both engines print the same text.
SELECT COUNT(*)                                        AS n,
       CAST(SUM(${BYTE_LEN_OPEN}shipping_address${BYTE_LEN_CLOSE}
              + ${BYTE_LEN_OPEN}payment_method${BYTE_LEN_CLOSE}
              + ${BYTE_LEN_OPEN}order_status${BYTE_LEN_CLOSE}
              + ${BYTE_LEN_OPEN}${ORDERS_PART}${BYTE_LEN_CLOSE}) AS BIGINT) AS string_bytes,
       CAST(SUM(total_amount) AS DECIMAL(38,2))        AS revenue,
       CAST(SUM(customer_id) AS BIGINT)                AS customer_id_sum,
       CAST(SUM(order_id) AS BIGINT)                   AS order_id_sum,
       COUNT(updated_at)                               AS updated_at_n
FROM ${ORDERS}
WHERE ${ORDERS_PART} = '${SNAPSHOT_DT}'
  AND order_date >= TIMESTAMP '${S_FROM}'
  AND order_date <  TIMESTAMP '${S_TO}';
