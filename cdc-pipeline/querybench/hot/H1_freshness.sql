-- Freshness / sanity: how many changed rows each fact table holds in the hot
-- window, and the newest one. min_rows_in_window = 0 means a writer (or the
-- CDC source) is not running, and every later hot number is a fast, flattering
-- empty scan; it leads so the ZERO_SCAN guard fires if EITHER table is empty.
-- Lag = HOT_TO - newest, worked out outside SQL (no clock function).
-- The window is on the source's own change timestamp, so both formats hold the
-- same rows once both pipelines have committed past HOT_TO:
--   orders: updated_at (MySQL ON UPDATE CURRENT_TIMESTAMP; the generator always
--     changes order_status/total_amount, so every upsert bumps it). This is the
--     change set: new orders AND updates. order_date never changes, so an
--     updated order stays in its creation-date partition: no partition filter.
--   order_items: created_at, its only timestamp. Upserts change quantity /
--     unit_price but not created_at, so the window holds the rows INSERTED in
--     it; created_at fixes the partition, so the partition filter only prunes.
-- Runs unchanged on DuckDB and Trino. Window bounds are TIMESTAMP literals:
-- Trino has no implicit VARCHAR -> TIMESTAMP coercion in a comparison. The
-- newest values print as 'YYYY-MM-DD HH:MM:SS' text (first 19 characters of
-- the VARCHAR cast): DuckDB drops a zero fraction, Trino prints timestamp(6)
-- with six digits, so the raw values would not compare as text. The source
-- (MySQL TIMESTAMP) is whole seconds, so nothing is lost.
-- Compare this row across arms before trusting any hot number.
SELECT LEAST(o.rows_in_window, i.rows_in_window)      AS min_rows_in_window,
       o.rows_in_window                               AS orders_changed,
       i.rows_in_window                               AS items_created,
       SUBSTR(CAST(o.newest AS VARCHAR), 1, 19)       AS orders_newest_change,
       SUBSTR(CAST(i.newest AS VARCHAR), 1, 19)       AS items_newest_insert
FROM (SELECT COUNT(*) AS rows_in_window, MAX(t.updated_at) AS newest
      FROM ${ORDERS} AS t
      WHERE t.updated_at >= TIMESTAMP '${HOT_FROM}'
        AND t.updated_at <  TIMESTAMP '${HOT_TO}') AS o
CROSS JOIN
     (SELECT COUNT(*) AS rows_in_window, MAX(t.created_at) AS newest
      FROM ${ORDER_ITEMS} AS t
      WHERE t.${ORDER_ITEMS_PART} = '${HOT_DT}'
        AND t.created_at >= TIMESTAMP '${HOT_FROM}'
        AND t.created_at <  TIMESTAMP '${HOT_TO}') AS i;
