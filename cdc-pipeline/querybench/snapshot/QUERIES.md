# CDC snapshot suite (S1–S7)

Analytics on a pinned snapshot of the four CDC tables (MySQL `ecommerce` →
Flink CDC → Paimon / Iceberg), writers stopped. Modelled on
`throughput-benchmark/querybench/snapshot/`: one query per file, file name =
query name, name order = run order. Every query runs unchanged on **DuckDB 2.0**
(`iceberg_scan`) and **Trino 484** (Iceberg connector) against the native
Iceberg v3 tables, the arm that matters now (neither engine reads Paimon's v3
tables or their export). The SQL is the subset both engines accept and print
alike; the one difference SQL cannot hide is a dialect key. The rules and the
key are the same as the hot suite's: see `../hot/QUERIES.md`, "Dialect keys"
and "Writing for both engines".

## Queries

| Id | Tables | Measures | Flags | Runtime class |
|---|---|---|---|---|
| `S1_order_items_count` | order_items (partition) | `COUNT(*)` + `SUM(quantity)` of the live rows: the cost of resolving the upsert history. Iceberg builds and applies the equality-delete set, Paimon applies deletion vectors. The headline CDC read. `SUM` forces a data read, so neither engine can answer it from manifest counts. | — | Paimon: seconds. Iceberg compacted: seconds. Iceberg uncompacted: OOM (2026-09: StarRocks and DuckDB). |
| `S2_items_narrow_agg` | order_items (partition) | `GROUP BY quantity` (10 groups), `SUM(subtotal)`. S2 − S1 = the aggregate's cost with the scan and merge held constant. | `leading-col-is-label` | as S1, plus a few seconds |
| `S3_orders_distinct_customers` | orders (partition) | `COUNT(DISTINCT customer_id)` per `payment_method` (5 × ~17M): memory pressure, on the upsert-heaviest table. Its compaction state can differ from order_items'. | `leading-col-is-label` | seconds to tens of seconds |
| `S4_orders_wide_slice` | orders (partition, `order_date` slice) | Whole-row decode: bytes of every string column plus sums of the numeric ones, over `[S_FROM, S_TO)`. The only time-sliced query. | — | seconds (depends on the slice width) |
| `S5_star_join_revenue` | all four | order_items ⋈ orders ⋈ products ⋈ customers, revenue by order day × category × state for the items created on the closed day. Every table's merge at once, plus three join hash tables. | `leading-col-is-label` | Paimon: ~5 s (2026-09 Q6). Iceberg uncompacted order_items: OOM, expect it first. |
| `S6_topn_customers` | orders (partition), customers | Top 20 customers by spend: aggregate → TopN → lookup. The spend uses each order's current `total_amount`, so an unapplied delete reorders it. | — | seconds |
| `S7_dedup_keys` | all four (facts: partition, dimensions: whole) | Correctness: per table, live rows, distinct source PKs, and `dup_rows` (must be 0). Compare every row across arms. | `leading-col-is-label` | tens of seconds (`COUNT(DISTINCT)` on ~145–380M keys) |

Sizes on 2026-11-01: each fact table has a single partition (`2026-09-27`),
so the partition-bounded queries currently read the whole table.

| Table | Iceberg (current snapshot) | Paimon native, 2026-09-30 |
|---|---|---|
| order_items | 381.7M records, 137 data + 124 delete files, 365.2M equality + 16.5M position deletes. **Not compacted.** | 144.3M rows |
| orders | 95.2M, 2 data files, 0 deletes (Athena OPTIMIZE 2026-09-28) | 95.2M |
| products | 31.1M, 10 data files, 0 deletes; 10 category partitions | 31.1M |
| customers | 16.9M, 14 data files, 0 deletes; unpartitioned | 16.9M |

## Placeholders

| Placeholder | Value |
|---|---|
| `${CUSTOMERS}` `${PRODUCTS}` `${ORDERS}` `${ORDER_ITEMS}` | the qualified, version-pinned table expression per arm. S5 and S6 follow them with a bare alias (`${ORDERS} o`), so the expression must accept one after it (on Trino, after `FOR VERSION AS OF ...`). |
| `${ORDERS_PART}` | `order_dt` (Iceberg; STRING partition column, `yyyy-MM-dd`) / `order_date_str` (Paimon) |
| `${ORDER_ITEMS_PART}` | `created_dt` (Iceberg, STRING) / `created_date_str` (Paimon) |
| `${SNAPSHOT_DT}` | the closed `yyyy-MM-dd` partition, compared as a string literal with the STRING partition column |
| `${S_FROM}` `${S_TO}` | S4 only: `yyyy-MM-dd HH:mm:ss` bounds on `orders.order_date`, inside `SNAPSHOT_DT`, on whole minutes; the SQL writes them as `TIMESTAMP '…'` literals (Trino does not coerce VARCHAR to TIMESTAMP in a comparison) |
| `${BYTE_LEN_OPEN}` `${BYTE_LEN_CLOSE}` | S4 only, from `querybench/dialect-<engine>.env`: wrap one VARCHAR expression to get its UTF-8 byte length. DuckDB `STRLEN(` … `)`, Trino `LENGTH(TO_UTF8(` … `))`. Replaces `BYTES_FN`. |

Products is partitioned by `category` in both formats under the same name, so
it needs no partition placeholder.

## What can make a format look artificially fast or slow

- **Compaction state decides S1, S2, S5 and S7.** Uncompacted Iceberg
  order_items is a merge-on-read table with 365M equality deletes. Compacted, it
  is a plain Parquet scan. Record per table and arm: data files, delete files,
  equality and position deletes, and the snapshot id the run pinned. The
  "compacted" and "uncompacted" Iceberg results are separate results, not one.
- **Paimon's Iceberg export is complete only after a full compaction.** On
  2026-09-30 it showed 0.4–9.2% fewer rows than the native Paimon reader at the
  same snapshot id (rows still in un-compacted LSM levels). An export reader
  then does less work, and its time looks like a speed-up. (Not read on the v3
  set: neither DuckDB 2.0 nor Trino 484 reads it.)
- **Cross-format counts need both pipelines drained to one binlog position.**
  The two formats are written by separate Flink jobs. Unless both caught up
  before the writers were suspended, they hold different data (orders differed
  by 8 rows on 2026-09-28). This is pipeline state, not a reader result, but
  the formats then read different data.
- **Metadata shortcuts.** A bare `COUNT(*)` on a delete-free Iceberg table can
  be answered from manifests: StarRocks did it in 22 ms with ScanRows=0 (2026-09),
  and DuckDB in 43 ms on Paimon's export. No query here has a bare count. S1 adds
  `SUM(quantity)`, and S7 adds `COUNT(DISTINCT)`.
- **Stats mode.** Iceberg tables write `write.metadata.metrics.default=full`
  (whole min/max for every column, strings included). Paimon truncates string
  stats. Pruning compares equally on numbers and timestamps, but not on long
  strings. That is why S4 slices on `order_date` and no query filters on a
  string other than the partition columns.
- **Delete key columns are always read.** On Iceberg with live equality
  deletes, `(created_dt, order_item_id)` / `(order_dt, order_id)` are read
  whatever the query projects. That makes Iceberg's narrow scans (S1, S2) wider
  than Paimon's. This is a real format cost, so keep it.
- **File layout after compaction.** Athena's bin-pack produced 2 files of
  ~800 MB for orders. Paimon keeps 32 buckets per partition, sorted by key.
  S4's slice pruning depends on this layout, so record it with the result.
- **Delete-set memory.** The 2026-09 order_items OOMs (then on StarRocks and
  DuckDB) came from building the equality-delete set, not from scan width, so
  no query carries a scan-width flag. Record each engine's peak query memory
  beside the time.
- **DuckDB memory.** On 1.5.6 the equality-delete set sits outside
  `memory_limit` and the spill. It was SIGKILLed, not errored, at 52 Gi and at
  72 Gi. On uncompacted Iceberg order_items, expect a pod restart (cache lost)
  rather than an ERROR row.

## Comparing results

Group keys are strings or integers. Decimal sums are cast to `DECIMAL(38,2)`
and integer sums to `BIGINT`, so both engines print the same scale and type. No
AVG, float or timestamp is in any output, and every ORDER BY is a total order,
so results compare as text across engines and arms. DuckDB and Trino on the
same Iceberg pin must agree on every row of every query; S2, S3, S5, S6 and S7
should also match between formats that read the same data (see above), not
only on the first row.
