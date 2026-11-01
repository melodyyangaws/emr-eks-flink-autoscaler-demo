-- Point lookup by primary key on an order that was UPDATED in the hot window
-- (the harness picks it before the passes, the same key for every arm; see
-- QUERIES.md). Lake primary key = (creation-date partition, order_id), so the
-- partition value comes with the key and every arm prunes to one partition.
-- The cost after that is how many files might hold the key and what must be
-- merged to decide which version is live:
--   Iceberg: the key's newest version is in a recent file, its older versions
--     in earlier ones. Equality-delete files carry keys from the generator's
--     cold tail (uniform over all ids), so their key bounds span the whole range
--     and cannot be skipped: the reader loads the partition's equality deletes
--     newer than every candidate file. That grows with the time since the
--     partition was last compacted - the merge-on-read worst case, by design.
--     An ERROR / TIMEOUT here on an uncompacted table is the result.
--   Paimon: files are sorted by key within a bucket, so key min/max skips most
--     of them; deletion vectors mark superseded versions.
-- Expect exactly one row. No row = the arm cannot see the key (Paimon's Iceberg
-- export lags: top LSM level only), reported as ZERO_SCAN, never as a win.
-- updated_at can legitimately differ between formats: each arm is pinned at its
-- own snapshot, and the key may be updated again after HOT_TO.
SELECT o.order_id,
       o.order_status,
       o.total_amount,
       o.payment_method,
       o.updated_at
FROM ${ORDERS} AS o
WHERE o.${ORDERS_PART} = '${LOOKUP_ORDER_DT}'
  AND o.order_id = ${LOOKUP_ORDER_ID};
