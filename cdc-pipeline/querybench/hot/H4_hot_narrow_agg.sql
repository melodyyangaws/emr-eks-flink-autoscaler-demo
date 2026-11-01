-- Projection pushdown on the orders change set: GROUP BY one skinny column
-- bench: leading-col-is-label
-- (order_status, 5 values) and sum one decimal, out of 9 columns. H5 reads all
-- 9 over the same rows; H5/H4 is the all-columns tax on one read path.
-- The change set (updated_at in the window) spans partitions: an update keeps
-- the row in its creation-date partition, so there is no partition filter and
-- pruning rests on updated_at file stats. That is a layout difference, and it
-- is real, not a bug in either arm:
--   Iceberg: files are written in commit order, so old files have old
--     updated_at bounds and are skipped; the only deletes to apply are those
--     committed after the window's files (bounded by the pin lag).
--   Paimon: LSM files are sorted by key and compaction mixes old and new rows,
--     so most files' updated_at max is recent and few are skipped; deletion
--     vectors are cheap, the read is wide.
-- Paimon's Iceberg export lags (top LSM level only): fewer rows there is lag.
SELECT o.order_status,
       COUNT(*)            AS n,
       SUM(o.total_amount) AS amount
FROM ${ORDERS} AS o
WHERE o.updated_at >= '${HOT_FROM}'
  AND o.updated_at <  '${HOT_TO}'
GROUP BY o.order_status
ORDER BY n DESC, o.order_status;
