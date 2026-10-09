-- Needle: point lookup on a uuid that cannot exist (zero rows by design).
-- bench: allow-zero-rows
-- No key, index or sort on uuid, and uuid is MD5-random, so min/max prunes
-- nothing: a full scan of the window. This is the control. An arm much faster here
-- than on H4 is winning on aggregation, not on the scan.
SELECT uuid, eventTimeDT
FROM ${TABLE}
WHERE dt = '${HOT_DT}'
  AND ingestionTime >= '${HOT_FROM_5M}'
  AND ingestionTime <  '${HOT_TO}'
  AND uuid = '00000000-0000-4000-0000-000000000000';
