-- Fat-column decode: sum the bytes of the ~3.3 KB/row JSON payload.
--
-- STEP 5 OF 6, HOT SUITE. The I/O and decompression bound of the hot suite, and
-- the query most sensitive to file count and file size — i.e. to compaction
-- state. Measured 57-86 s on StarRocks against H4's fraction of a second on the
-- same window.
--
-- payload_bytes IS THE CROSS-ENGINE EQUALITY CHECK. It is the one value in the
-- suite that proves both engines read the SAME rows rather than merely ran the
-- same text: two arms agreeing on COUNT(*) but not on payload_bytes have read
-- different data. That is why ${BYTES_FN} exists instead of a hardcoded
-- function name — DuckDB's LENGTH counts CHARACTERS while StarRocks' counts
-- BYTES, so the two agree only on pure ASCII. The generator's padding is ASCII
-- today, but the check must not depend on that staying true, so DuckDB renders
-- STRLEN and StarRocks LENGTH. The decode cost is identical either way; only
-- the number printed changes.
--
-- NOT octet_length(): in DuckDB that is BLOB/BIT-only and a VARCHAR argument is
-- a BinderException, which cost a whole H5/S4 slot on this file's first run.
SELECT COUNT(*)                        AS n,
       SUM(${BYTES_FN}(event_json))    AS payload_bytes
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM_5M}'
  AND ingestionTime <  '${HOT_TO}';
