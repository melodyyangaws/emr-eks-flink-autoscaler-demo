-- Full-partition COUNT(*) over one closed dt; the other queries are normalised by it.
-- A full scan: there is no COUNT(*) metadata shortcut on an external catalog,
-- and the data caches are off.
SELECT COUNT(*) AS partition_rows
FROM ${TABLE}
WHERE dt = '${SNAPSHOT_DT}';
