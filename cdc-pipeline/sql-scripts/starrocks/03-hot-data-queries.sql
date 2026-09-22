-- ============================================================================
-- Q7-Q10: Real-time HOT data queries — Paimon vs Iceberg
-- ============================================================================
--
-- Q1-Q6 (in 01-/02-*.sql) are batch/snapshot queries: they scan the whole table,
-- so their cost is dominated by total data volume and they barely notice how
-- recently a row arrived. This file targets the opposite case — rows written in
-- the last few minutes, still sitting in the write-optimised part of each table.
--
-- Why the two formats diverge here:
--
--   Paimon (LSM + changelog-producer=lookup, deletion-vectors on)
--     New rows land in small level-0 sorted runs. A point lookup or recent-window
--     scan reads those runs plus whatever levels the key range touches. The LSM
--     compacts continuously in the background (num-sorted-run.compaction-trigger
--     = 4 in 03-paimon-sinks.sql), so the number of files a hot query touches
--     stays bounded without anyone running a maintenance job.
--
--   Iceberg V3 (merge-on-read, position deletes / deletion vectors)
--     Each CDC commit writes a new data file plus delete files for the rows it
--     supersedes. A reader must apply every delete file that overlaps the data
--     files it reads, so cost grows with the number of commits since the last
--     compaction — and shrinks sharply after rewrite_data_files. That gives
--     three distinct states worth measuring, not one:
--       (a) hot / uncompacted : many small data+delete files
--       (b) after rewrite_data_files : merged data, deletes applied
--       (c) after expire_snapshots : metadata trimmed too
--
-- Run this file with bench-hot-data.sh, which times each query against both
-- catalogs and records the file/snapshot counts that explain the timings.
-- Queries are written to be valid in both catalogs; the only per-format
-- difference is the partition column name, noted at Q9.

-- ---------------------------------------------------------------------------
-- Q7: Freshness — how far behind the source is this table right now?
-- ---------------------------------------------------------------------------
-- The single most important hot-data metric: end-to-end CDC visibility lag.
-- Reads only the newest rows, so it is a pure hot-path query.
SELECT
    MAX(updated_at)                                   AS newest_row,
    TIMESTAMPDIFF(SECOND, MAX(updated_at), UTC_TIMESTAMP())     AS lag_seconds,
    COUNT(*)                                          AS rows_last_5min
FROM orders
WHERE updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 5 MINUTE);

-- ---------------------------------------------------------------------------
-- Q8: Hot point lookup by primary key
-- ---------------------------------------------------------------------------
-- Worst case for merge-on-read: a single key that has been updated many times
-- by the generator's update_orders path. Paimon resolves it from the LSM key
-- index; Iceberg must scan candidate data files and apply overlapping deletes.
-- Uses a subquery for the key so the query is stable as data grows.
SELECT order_id, customer_id, order_status, total_amount, updated_at
FROM orders
WHERE order_id = (SELECT MAX(order_id) FROM orders);

-- ---------------------------------------------------------------------------
-- Q9: Recent-window aggregation (last 10 minutes)
-- ---------------------------------------------------------------------------
-- Scans only newly written files. This is where accumulated Iceberg delete
-- files hurt most, because nearly every file read is one that also has deletes.
-- NOTE: partition column differs by format — order_date_str (Paimon) vs
-- order_dt (Iceberg). bench-hot-data.sh substitutes the right one; the
-- predicate below is on updated_at so the query itself stays portable.
SELECT
    order_status,
    COUNT(*)              AS orders,
    SUM(total_amount)     AS revenue,
    AVG(total_amount)     AS avg_order_value
FROM orders
WHERE updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE)
GROUP BY order_status
ORDER BY orders DESC;

-- ---------------------------------------------------------------------------
-- Q10: Hot join — recent orders joined to their items
-- ---------------------------------------------------------------------------
-- Two hot tables at once, so both sides pay the merge-on-read cost. Closest
-- query here to a real-time operational dashboard.
SELECT
    o.order_status,
    COUNT(DISTINCT o.order_id) AS orders,
    SUM(oi.quantity)           AS units,
    SUM(oi.subtotal)           AS revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE)
GROUP BY o.order_status
ORDER BY revenue DESC;
