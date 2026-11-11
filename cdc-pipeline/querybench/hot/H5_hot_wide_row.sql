-- Wide-row read: every column of every changed order in the window, i.e. what
-- a CDC consumer pulling full current rows pays. The CDC tables have no fat
-- column (the throughput suite's event_json); here the cost is decoding all 9
-- columns of rows spread over many small, freshly committed files, plus
-- resolving their deletes. Same rows as H4: compare H5/H4 across arms.
-- The sums are the cross-engine check that both engines read the same rows,
-- and must match between formats when both pipelines committed past HOT_TO.
-- text_bytes is the UTF-8 byte length of the three text columns, through the
-- BYTE_LEN_OPEN / BYTE_LEN_CLOSE dialect pair (DuckDB STRLEN, Trino
-- LENGTH of TO_UTF8; LENGTH alone counts characters on both). partitions_touched
-- = how many creation-date partitions the change set spans (updates land in
-- old partitions). Runs unchanged on DuckDB and Trino: TIMESTAMP literals and
-- timestamps printed as 'YYYY-MM-DD HH:MM:SS' text (see H1), integer sums cast
-- to BIGINT (DuckDB's SUM(INTEGER) is HUGEINT), the decimal sum to
-- DECIMAL(38,2).
SELECT COUNT(*)                                          AS n,
       COUNT(DISTINCT o.${ORDERS_PART})                  AS partitions_touched,
       CAST(SUM(o.order_id) AS BIGINT)                   AS order_id_sum,
       CAST(SUM(o.customer_id) AS BIGINT)                AS customer_id_sum,
       CAST(SUM(o.total_amount) AS DECIMAL(38,2))        AS amount_sum,
       SUBSTR(CAST(MIN(o.order_date) AS VARCHAR), 1, 19) AS oldest_order_date,
       SUBSTR(CAST(MAX(o.updated_at) AS VARCHAR), 1, 19) AS newest_change,
       CAST(SUM(${BYTE_LEN_OPEN}o.order_status${BYTE_LEN_CLOSE}
              + ${BYTE_LEN_OPEN}o.payment_method${BYTE_LEN_CLOSE}
              + ${BYTE_LEN_OPEN}o.shipping_address${BYTE_LEN_CLOSE}) AS BIGINT) AS text_bytes
FROM ${ORDERS} AS o
WHERE o.updated_at >= TIMESTAMP '${HOT_FROM}'
  AND o.updated_at <  TIMESTAMP '${HOT_TO}';
