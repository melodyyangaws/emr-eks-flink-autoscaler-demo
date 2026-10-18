-- Needle: a uuid predicate that cannot match (zero rows by design).
-- bench: allow-zero-rows
-- uuid is a UUIDv7: its leading digits are the event millisecond, so files
-- have narrow uuid min/max and a literal `uuid = '...'` lets every engine skip
-- almost every file from stats alone. Wrapping the column in SUBSTR defeats
-- min/max pruning, and the variant group (chars 20-23) is always 8xxx-bxxx,
-- never '0000'. So this reads the uuid column of the whole window. This is the
-- control: an arm much faster here than on H4 is winning on aggregation, not
-- on the scan.
SELECT uuid, eventTimeDT
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM}'
  AND ingestionTime <  '${HOT_TO}'
  AND SUBSTR(uuid, 20, 4) = '0000';
