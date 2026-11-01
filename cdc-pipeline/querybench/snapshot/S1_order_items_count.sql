-- The headline CDC read: every live row of order_items in the closed partition,
-- i.e. the cost of resolving the upsert history to its current state.
--   Iceberg (merge-on-read): the reader must build the partition's equality-delete
--     set (365M equality deletes over 124 delete files on 2026-11-01) and apply it,
--     plus the position deletes, before it can emit one row. Uncompacted, this
--     OOMed on both engines in 2026-09 (StarRocks MEM_LIMIT_EXCEEDED on a bare
--     COUNT, DuckDB SIGKILLed past its memory_limit). An ERROR here is the result.
--   Paimon (LSM + deletion vectors): one bitmap per data file, no key join.
-- SUM(quantity) is there to force a data read: on a delete-free Iceberg table
-- (after rewrite_data_files) StarRocks answers a bare COUNT from manifest
-- record counts with ScanRows=0, and DuckDB does the same on Paimon's Iceberg
-- export. That would time metadata, not the merge. units is also a checksum:
-- both values must match between paimon and pmn_as_iceberg (same writer) on
-- every engine, and between formats when both pipelines drained to the same
-- binlog position before the writers stopped.
-- Compaction state decides this query: record per table, per arm, the data,
-- delete-file and equality-delete counts of the pinned snapshot beside the time.
-- Today the table has one partition, so this is also the full-table count.
SELECT COUNT(*)      AS live_rows,
       SUM(quantity) AS units
FROM ${ORDER_ITEMS}
WHERE ${ORDER_ITEMS_PART} = '${SNAPSHOT_DT}';
