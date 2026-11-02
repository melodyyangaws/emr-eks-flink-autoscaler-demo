# CDC hot suite (H1–H9)

Run while the CDC writers and the upsert generator are live. Each file holds one
query; file name = query name; name order = run order. Every query runs
unchanged on **DuckDB 2.0** (`iceberg_scan`) and **Trino 484** (Iceberg
connector) against the native Iceberg v3 tables; the Iceberg arm is the one
that matters now (neither engine reads Paimon's v3 tables or their export).
Every table reference has an alias (`AS o`, `AS oi`), so a placeholder can
render as a plain name, `name FOR VERSION AS OF n`, an `iceberg_scan('…')` call
or a parenthesised subquery. The few engine differences that SQL cannot paper
over are dialect keys (see Placeholders); the rest is written in the subset
both engines accept and print alike (see Writing for both engines).

## The hot window, per table

| Table | Window column | Partition filter | Why |
|---|---|---|---|
| `orders` | `updated_at` | none (except H8) | MySQL bumps `updated_at` on every insert and real update (the generator always changes `order_status`/`total_amount`). `order_date` never changes, so an updated order stays in its creation-date partition: filtering on `HOT_DT` would drop every update to an older order. |
| `order_items` | `created_at` | `= HOT_DT` | The table's only timestamp. Upserts change `quantity`/`unit_price` but not `created_at`, so the window holds the rows inserted in it. `created_at` decides the partition, so the filter only prunes. |

Fact tables only: `customers` and `products` are not read (see Open points).

## Queries

| Id | Table(s) | Measures | Flags |
|---|---|---|---|
| `H1_freshness` | orders, order_items | Rows changed in the window on each table, plus the newest one. `min_rows_in_window` = 0 means a writer is down, and every hot number is void. Compare across arms first. | — |
| `H2_hot_count` | order_items | `COUNT(*)` of the window. On an upsert table this is planning, pruning and resolving deletes, not metadata only. | — |
| `H3_hot_needle` | order_items | A filter on `order_id` that never matches (a modulo blocks min/max pruning). Scan-only control for H2, H6 and H7. | `allow-zero-rows` |
| `H4_hot_narrow_agg` | orders | Groups the change set by `order_status`, using 2 of the 9 columns: projection pushdown. Needs `updated_at` stats pruning across partitions. | `leading-col-is-label` |
| `H5_hot_wide_row` | orders | All 9 columns of the same rows as H4, as a checksum (UTF-8 bytes of the text columns via `BYTE_LEN_OPEN`/`BYTE_LEN_CLOSE`). H5/H4 is the all-columns tax. | — |
| `H6_hot_product_agg` | order_items | Per-product `COUNT(DISTINCT order_id)`, up to ~20k groups: the hash table. | `leading-col-is-label` |
| `H7_hot_mor_dedup` | order_items | Merge-on-read: key plus the columns upserts change, resolved to the current versions. Iceberg equality deletes vs Paimon deletion vectors. `dup_rows` must be 0. | — |
| `H8_hot_pk_lookup` | orders | Primary-key point lookup of an order updated in the window. The merge-on-read worst case on Iceberg: it grows with the equality deletes in that key's partition since the last compaction. | — |
| `H9_hot_join` | order_items ⋈ orders | Items inserted in the window joined to orders changed in the window, by status. Both sides merge at once; dynamic join filters prune the orders side. | `leading-col-is-label` |

## Placeholders

| Placeholder | Value |
|---|---|
| `${ORDERS}`, `${ORDER_ITEMS}` | table expression for the arm, pinned to the run's version (`${CUSTOMERS}`, `${PRODUCTS}` are unused here) |
| `${ORDERS_PART}` | `order_dt` (Iceberg; a STRING partition column holding `yyyy-MM-dd`) / `order_date_str` (Paimon) |
| `${ORDER_ITEMS_PART}` | `created_dt` (Iceberg, STRING) / `created_date_str` (Paimon) |
| `${HOT_DT}` | `yyyy-MM-dd`, the date of the window; compared as a string literal with the STRING partition column |
| `${HOT_FROM}`, `${HOT_TO}` | `yyyy-MM-dd HH:mm:ss` UTC, half-open `[FROM, TO)`; the SQL writes them as `TIMESTAMP '…'` literals |
| **`${LOOKUP_ORDER_ID}`** (new) | integer `order_id` for H8, picked once per run, the same for every arm and pass |
| **`${LOOKUP_ORDER_DT}`** (new) | that order's partition value (`yyyy-MM-dd` of its `order_date`) |

Comments contain no placeholders, because `qb_render` fails on any `${…}` it
cannot resolve, including ones in comments.

### Dialect keys (`querybench/dialect-duckdb.env`, `querybench/dialect-trino.env`)

Every `KEY=VALUE` line of `dialect-<engine>.env` is a placeholder too, its
value inserted verbatim. Both files define the same keys. There is one pair:

| Key | DuckDB | Trino | Why |
|---|---|---|---|
| `${BYTE_LEN_OPEN}` … `${BYTE_LEN_CLOSE}` | `STRLEN(` … `)` | `LENGTH(TO_UTF8(` … `))` | UTF-8 byte length of one VARCHAR expression (BIGINT), written `${BYTE_LEN_OPEN}expr${BYTE_LEN_CLOSE}`. `LENGTH` counts characters on both engines, and Trino has no byte-length function on VARCHAR, so the Trino form is two nested calls: an open/close pair rather than one function name. Both are O(1) per value (DuckDB keeps the length in the string header; `TO_UTF8` is a zero-copy cast). Used by H5 and S4; replaces the old `BYTES_FN`. |

### Writing for both engines

Rules every query here follows, so that DuckDB and Trino accept it and print
the same text (the summary compares rows as text):

- **Timestamps in predicates are `TIMESTAMP '…'` literals.** Trino has no
  implicit VARCHAR → TIMESTAMP coercion in a comparison
  (`Cannot apply operator: timestamp(6) >= varchar(19)`); DuckDB accepts both.
  The Iceberg columns are `timestamp` (no zone): Trino `timestamp(6)`, DuckDB
  `TIMESTAMP`.
- **Partition columns are STRING** (`order_dt`, `created_dt`: Flink
  `DATE_FORMAT` output, `03-iceberg-sinks.sql`), so they compare with plain
  `'yyyy-MM-dd'` string literals on both engines; no DATE cast.
- **Timestamps in results print as `SUBSTR(CAST(x AS VARCHAR), 1, 19)`**
  (`yyyy-MM-dd HH:mm:ss`). DuckDB drops a zero fraction, Trino always prints
  six digits for `timestamp(6)`. The source (MySQL `TIMESTAMP`) is whole
  seconds, so nothing is lost.
- **Sums are cast**: decimal sums to `DECIMAL(38,2)` (the type both give
  `SUM(DECIMAL(p,2))`, pinned so the scale prints the same), integer sums to
  `BIGINT` (DuckDB's `SUM(INTEGER)` is `HUGEINT`). Counts are `BIGINT` on both.
  No AVG, no float.
- **Every ORDER BY is a total order** over the output (group key, or a tie
  break on the key), and LIMIT only follows such an ORDER BY. NULLs sort last
  on both engines by default.
- **No engine-only functions** (`STRLEN` only through the dialect pair). `%`
  on integers truncates toward zero on both. `LEAST` is only applied to
  non-NULL counts (Trino returns NULL if any argument is NULL, DuckDB skips
  NULLs).

### Picking the H8 key (harness, untimed, after pinning)

Pick a real update (an order created before the window and changed inside it)
on the native Iceberg arm at the pinned version:

```sql
SELECT o.order_id, o.<orders part col>
FROM <orders, pinned> AS o
WHERE o.updated_at >= TIMESTAMP '<HOT_FROM>' AND o.updated_at < TIMESTAMP '<HOT_TO>'
  AND o.order_date <  TIMESTAMP '<HOT_FROM>'
ORDER BY o.updated_at DESC, o.order_id
LIMIT 1;
```

(`TIMESTAMP` literals, so the same text runs on DuckDB and Trino.) Where a
Paimon arm is run, check the key there with H8; if that returns no row, take
the next candidate (`LIMIT 1 OFFSET n`). Write the key, its partition, and whether
the partition is `HOT_DT` or older to the `.pins` file. An older partition
holds far more equality deletes, so H8's Iceberg time depends on it. This query
has no partition filter, so it reads `updated_at` across the whole table (same
read as H4; not yet timed).

## Things that can make an arm look fast or slow

- **Paimon's Iceberg export lags.** On the v3 default (Paimon 2.1, format 3,
  64-bit deletion vectors) the export lists every file above LSM level 0 plus
  its deletion vectors, so it trails native Paimon by about one checkpoint; only
  readers that support it could read it (neither DuckDB 2.0 nor Trino 484
  reads the v3 export today). On the v2 set the export holds only the top LSM
  level and no deletion vectors
  (`IcebergCommitCallback.shouldAddFileToIceberg`), so an export reader sees an
  older, fully merged view. In the hot window they can see fewer rows (H1) or none, or miss the H8
  key, and they do no merge work. Compare their H1 row with native `paimon`'s;
  where it differs, the export arms' times describe lagged data, not the
  reader. The snapshot suite saw 0.4–9.2 % fewer rows in the export on 2026-09-30.
- **Iceberg upsert writes an equality delete for every row**, inserts included.
  So even fresh-insert windows (order_items) pay a key anti-join on Iceberg,
  even though the deletes mostly match nothing. Paimon resolved the key at
  commit time (`changelog-producer = lookup`) and reads bitmaps.
- **Layout vs `updated_at` (H4, H5, H8, H9 orders side).** Iceberg files are
  written in commit order and prune well on `updated_at`. Paimon files are
  sorted by key, and compaction mixes old and new rows, so on Paimon the window
  is close to a full read of `updated_at`. That is a real property of the
  formats.
- **Delete-set size is set by compaction state, not by the window.** H8 above
  all. Record each pinned snapshot's data-file, delete-file and
  equality-delete counts (Iceberg), and its levels (Paimon), next to the times.
  An uncompacted Iceberg order_items OOMed both engines on a bare `COUNT(*)` in
  2026-09; the window filters keep H1–H7 and H9 bounded by the deletes
  committed after the window, but H8 is not bounded that way.
- **Different versions are fine; different rows are not.** Each format is
  pinned at its own snapshot, so `updated_at` in H8 and the sums in H5, H7 and
  H9 can differ between formats if a pipeline lags. Within one writer
  (DuckDB vs Trino on the same Iceberg pin) every row must match.
