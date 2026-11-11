-- Needle: a predicate on order_items that cannot match (zero rows by design).
-- bench: allow-zero-rows
-- order_id is drawn from the generator's sample of existing orders, so every
-- file's order_id min/max is a narrow, positive range: a literal like
-- order_id = -1 would let an engine skip every file from stats alone. The
-- modulo hides the column from min/max pruning, and a positive id modulo a
-- positive number is never -1 (DuckDB and Trino both truncate toward zero, so
-- the result takes the dividend's sign), so the reader must decode order_id
-- for every row of the window. This is the scan-only control for H2/H6/H7: an
-- arm much faster there than here is winning on aggregation, not on the read.
-- Merge-on-read note: an engine may evaluate this filter BEFORE applying
-- deletes (no row survives, so there is nothing to delete). If H3 is far
-- cheaper than H2 on the Iceberg arm only, the gap is the equality-delete
-- join, not the scan.
-- Runs unchanged on DuckDB and Trino (TIMESTAMP literals: see H1).
SELECT oi.order_item_id, oi.order_id
FROM ${ORDER_ITEMS} AS oi
WHERE oi.${ORDER_ITEMS_PART} = '${HOT_DT}'
  AND oi.created_at >= TIMESTAMP '${HOT_FROM}'
  AND oi.created_at <  TIMESTAMP '${HOT_TO}'
  AND oi.order_id % 1000003 = -1;
