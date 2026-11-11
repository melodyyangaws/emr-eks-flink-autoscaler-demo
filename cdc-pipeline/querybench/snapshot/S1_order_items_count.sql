-- The headline CDC read: every live row of order_items in the closed partition,
-- i.e. the cost of resolving the upsert history to its current state.
--   Iceberg (merge-on-read): the reader must build the partition's equality-delete
--     set (365M equality deletes over 124 delete files on 2026-11-01) and apply it,
--     plus the position deletes / deletion vectors, before it can emit one row.
--     Uncompacted, this OOMed in 2026-09 (DuckDB SIGKILLed past its
--     memory_limit). An ERROR here is the result.
--   Paimon (LSM + deletion vectors): one bitmap per data file, no key join.
-- SUM(quantity) is there to force a data read: on a delete-free Iceberg table
-- (after rewrite_data_files) an engine can answer a bare COUNT from manifest
-- record counts without reading a row (DuckDB does). That would time metadata,
-- not the merge. units is also a checksum: it must match across engines on the
-- same pin, and between formats when both pipelines drained to the same binlog
-- position before the writers stopped.
-- Compaction state decides this query: record per table, per arm, the data,
-- delete-file and equality-delete counts of the pinned snapshot beside the time.
-- Today the table has one partition, so this is also the full-table count.
-- Runs unchanged on DuckDB and Trino; units is cast to BIGINT (DuckDB's
-- SUM(INTEGER) is HUGEINT) so both print the same text.
SELECT COUNT(*)                      AS live_rows,
       CAST(SUM(quantity) AS BIGINT) AS units
FROM ${ORDER_ITEMS}
WHERE ${ORDER_ITEMS_PART} = '${SNAPSHOT_DT}';
