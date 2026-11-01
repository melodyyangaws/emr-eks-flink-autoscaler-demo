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
-- Paimon's Iceberg export (pmn_as_iceberg, DuckDB paimon) does not list every
-- file the native reader sees, so its counts here can be LOWER than the native
-- reader's at the same snapshot:
--   v3 set (default, Paimon 2.1, metadata.iceberg.format-version 3 with
--     bitmap64 deletion vectors): the export lists every file above LSM level 0
--     and exports the deletion vectors, so it trails by the rows still in level
--     0, about one checkpoint; that is the newest data, i.e. this window.
--   v2 set (ICEBERG_FORMAT_VERSION=2, CDC_DB_SUFFIX empty): only the top LSM
--     level is listed and no deletion vectors are exported, so rows not yet
--     fully compacted are missing.
-- Compare this row across arms before trusting any hot number.
SELECT LEAST(o.rows_in_window, i.rows_in_window) AS min_rows_in_window,
       o.rows_in_window                          AS orders_changed,
       i.rows_in_window                          AS items_created,
       o.newest                                  AS orders_newest_change,
       i.newest                                  AS items_newest_insert
FROM (SELECT COUNT(*) AS rows_in_window, MAX(t.updated_at) AS newest
      FROM ${ORDERS} AS t
      WHERE t.updated_at >= '${HOT_FROM}'
        AND t.updated_at <  '${HOT_TO}') AS o
CROSS JOIN
     (SELECT COUNT(*) AS rows_in_window, MAX(t.created_at) AS newest
      FROM ${ORDER_ITEMS} AS t
      WHERE t.${ORDER_ITEMS_PART} = '${HOT_DT}'
        AND t.created_at >= '${HOT_FROM}'
        AND t.created_at <  '${HOT_TO}') AS i;
