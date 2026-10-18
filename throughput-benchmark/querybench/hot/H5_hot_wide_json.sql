-- Fat-column decode: sum the bytes of the ~3.3 KB/row JSON payload. The most
-- sensitive to file count and size, i.e. to compaction state.
-- payload_bytes is the cross-engine check that both read the same rows;
-- ${BYTES_FN} counts bytes on both engines (DuckDB's LENGTH counts characters).
SELECT COUNT(*)                        AS n,
       SUM(${BYTES_FN}(event_json))    AS payload_bytes
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM}'
  AND ingestionTime <  '${HOT_TO}';
