-- Wide-row read: every column of every changed order in the window, i.e. what
-- a CDC consumer pulling full current rows pays. The CDC tables have no fat
-- column (the throughput suite's event_json); here the cost is decoding all 9
-- columns of rows spread over many small, freshly committed files, plus
-- resolving their deletes. Same rows as H4: compare H5/H4 across arms.
-- The sums are the cross-engine check that both engines read the same rows:
-- text_bytes uses the byte-length shim (LENGTH counts characters on DuckDB),
-- and every value must match between paimon and pmn_as_iceberg only when the
-- export is not lagging (see H1), and between formats when both pipelines
-- committed past HOT_TO. partitions_touched = how many creation-date
-- partitions the change set spans (updates land in old partitions).
SELECT COUNT(*)                                   AS n,
       COUNT(DISTINCT o.${ORDERS_PART})           AS partitions_touched,
       SUM(o.order_id)                            AS order_id_sum,
       SUM(o.customer_id)                         AS customer_id_sum,
       SUM(o.total_amount)                        AS amount_sum,
       MIN(o.order_date)                          AS oldest_order_date,
       MAX(o.updated_at)                          AS newest_change,
       SUM(${BYTES_FN}(o.order_status)
         + ${BYTES_FN}(o.payment_method)
         + ${BYTES_FN}(o.shipping_address))       AS text_bytes
FROM ${ORDERS} AS o
WHERE o.updated_at >= '${HOT_FROM}'
  AND o.updated_at <  '${HOT_TO}';
