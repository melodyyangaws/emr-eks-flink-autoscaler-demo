-- ============================================================================
-- Iceberg Sink Tables — Streaming CDC Optimized (Iceberg Flink runtime 1.12.0)
-- ============================================================================
-- Creates Apache Iceberg sink tables (format v3 by default on this branch, v2
-- via ICEBERG_FORMAT_VERSION=2)
-- optimized for low-latency CDC:
--   - Merge-on-read row-level deletes (no data-file rewrite per commit)
--   - UPSERT enabled with hash distribution for PK-based dedup
--   - ZSTD compression for better ratio on CDC payloads
--   - Tuned snapshot retention and metadata cleanup for 60s checkpoints
--
-- ----------------------------------------------------------------------------
-- 'format-version' = ICEBERG_FORMAT_VERSION: 3 (default) or 2
-- ----------------------------------------------------------------------------
-- The same variable drives Paimon's 'metadata.iceberg.format-version'
-- (../paimon/03-paimon-sinks.sql), so the native Iceberg tables and the
-- Paimon Iceberg export are always the same version — a fair comparison.
--
-- What the 1.12.0 Flink upsert writer emits per checkpoint
-- (RowDataTaskWriterFactory: useDv = formatVersion > 2):
--   v2: data files + equality-delete files (one per upsert / delete of a key
--       not written in this checkpoint) + positional-delete Parquet files (for
--       keys rewritten within the same checkpoint).
--   v3: data files + equality-delete files (unchanged — upserts of rows from
--       earlier checkpoints still use equality deletes) + deletion vectors in
--       Puffin files instead of positional-delete files for the within-
--       checkpoint case (BaseTaskWriter's PartitioningDVWriter,
--       DeleteGranularity.FILE).
-- Not enabled: the sink's equality-delete -> DV converter
-- (flink-maintenance.convert-equality-deletes.enabled /
-- IcebergSink.Builder#convertEqualityDeletes). It runs as a post-commit task of
-- the v2 IcebergSink (table.exec.iceberg.use-v2-sink) and is untested here; it
-- is the future option for removing equality deletes on v3 without compaction.
--
-- Reader support for v3 (relevant to the benchmark):
--   - StarRocks 4.1.x: FAILS FAST on any table that has a DV —
--     IcebergConnectorScanRangeSource throws "Iceberg V3 Deletion Vectors are
--     not supported". A v3 table with only equality deletes still plans.
--   - DuckDB 2.0 (iceberg extension): reads v3 DVs.
--   - Athena engine v3: Iceberg v2 only.
-- v3 is the default on this branch; its default table set is a fresh
-- CDC_DB_SUFFIX=_v3 database. Use ICEBERG_FORMAT_VERSION=2 (with a matching
-- database) where StarRocks' Iceberg reader or Athena must read these tables.
--
-- format-version applies at table creation only. Changing it here does not alter
-- tables that already exist; use a fresh database (CDC_DB_SUFFIX, e.g. _v3 ->
-- flink_iceberg_db_v3) or drop the four tables (and their S3 warehouse prefix
-- + Glue entries) before this DDL takes effect.
--
-- Streaming best practices applied:
--   1. Smaller target file sizes (32-64MB) to match checkpoint-driven writes
--   2. Hash distribution ensures same-PK rows land in the same file writer
--   3. Aggressive metadata cleanup (delete-after-commit) to prevent S3 bloat
--   4. Manifest merging to keep read planning fast as snapshots accumulate
--   5. Full column metrics for predicate pushdown in query engines
--
-- The write.{delete,update,merge}.mode = merge-on-read properties are read by
-- Spark row-level DML (on v3 Spark writes DVs for them); the Flink upsert sink
-- is merge-on-read regardless. All properties below are valid for v2 and v3.
--
-- Partitioning: Explicit DATE STRING partition columns computed via
-- DATE_FORMAT() in the pipeline INSERT statements (04-cdc-pipelines.sql).
-- Partition columns MUST be in PRIMARY KEY for hash distribution + upsert.
--
-- Environment variables:
--   GLUE_DATABASE: default flink_iceberg_db; the manifest sets
--     flink_iceberg_db + CDC_DB_SUFFIX
--   ICEBERG_FORMAT_VERSION: 3 (default) or 2
-- ============================================================================

-- Switch to Iceberg catalog
USE CATALOG iceberg_catalog;
USE ${GLUE_DATABASE:flink_iceberg_db};

-- ============================================================================
-- Table: customers (Dimension Table — unpartitioned)
-- ============================================================================
-- Low cardinality, frequent updates (address/phone changes).
-- Unpartitioned: small enough that partition overhead isn't justified.
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    customer_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    zip_code STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    -- ICEBERG_FORMAT_VERSION (3 default, 2 opt-out): v3 replaces positional-delete
    -- files with DVs, which StarRocks 4.1.x cannot read — see the header.
    'format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    'write.upsert.enabled' = 'true',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.distribution-mode' = 'hash',
    'write.target-file-size-bytes' = '33554432',
    'write.parquet.compression-codec' = 'zstd',
    -- Level 1, to match Paimon's 'file.compression.zstd-level' = '1'. Both sides
    -- write ZSTD Parquet, but Iceberg sets no level, so parquet-mr defaults to 3 —
    -- a different CPU-vs-ratio tradeoff on the write path, which shows up as both a
    -- throughput and a stored-bytes difference that is a config artifact rather than
    -- a property of either table format.
    'write.parquet.compression-level' = '1',
    'write.parquet.row-group-size-bytes' = '8388608',
    'history.expire.max-snapshot-age-ms' = '3600000',
    'history.expire.min-snapshots-to-keep' = '5',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '3',
    'commit.manifest.target-size-bytes' = '8388608',
    'commit.manifest-merge.enabled' = 'true',
    'write.metadata.metrics.default' = 'full'
);

-- ============================================================================
-- Table: products (Dimension Table — partitioned by category)
-- ============================================================================
-- Medium cardinality, updates on stock_quantity and price.
-- Partitioned by category for query pruning (WHERE category = 'Electronics').
CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock_quantity INT,
    description STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (category, product_id) NOT ENFORCED
) PARTITIONED BY (category) WITH (
    -- ICEBERG_FORMAT_VERSION (3 default, 2 opt-out): v3 replaces positional-delete
    -- files with DVs, which StarRocks 4.1.x cannot read — see the header.
    'format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    'write.upsert.enabled' = 'true',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.distribution-mode' = 'hash',
    'write.target-file-size-bytes' = '33554432',
    'write.parquet.compression-codec' = 'zstd',
    -- Level 1, to match Paimon's 'file.compression.zstd-level' = '1'. Both sides
    -- write ZSTD Parquet, but Iceberg sets no level, so parquet-mr defaults to 3 —
    -- a different CPU-vs-ratio tradeoff on the write path, which shows up as both a
    -- throughput and a stored-bytes difference that is a config artifact rather than
    -- a property of either table format.
    'write.parquet.compression-level' = '1',
    'write.parquet.row-group-size-bytes' = '8388608',
    'history.expire.max-snapshot-age-ms' = '3600000',
    'history.expire.min-snapshots-to-keep' = '5',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '3',
    'commit.manifest.target-size-bytes' = '8388608',
    'commit.manifest-merge.enabled' = 'true',
    'write.metadata.metrics.default' = 'full'
);

-- ============================================================================
-- Table: orders (Fact Table — partitioned by order_dt)
-- ============================================================================
-- High write volume, mostly inserts + status updates.
-- Explicit order_dt STRING column computed from order_date via DATE_FORMAT().
CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    customer_id INT,
    order_date TIMESTAMP(3),
    order_dt STRING,
    total_amount DECIMAL(12, 2),
    order_status STRING,
    payment_method STRING,
    shipping_address STRING,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_dt, order_id) NOT ENFORCED
) PARTITIONED BY (order_dt) WITH (
    -- ICEBERG_FORMAT_VERSION (3 default, 2 opt-out): v3 replaces positional-delete
    -- files with DVs, which StarRocks 4.1.x cannot read — see the header.
    'format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    'write.upsert.enabled' = 'true',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.distribution-mode' = 'hash',
    'write.target-file-size-bytes' = '67108864',
    'write.parquet.compression-codec' = 'zstd',
    -- Level 1, to match Paimon's 'file.compression.zstd-level' = '1'. Both sides
    -- write ZSTD Parquet, but Iceberg sets no level, so parquet-mr defaults to 3 —
    -- a different CPU-vs-ratio tradeoff on the write path, which shows up as both a
    -- throughput and a stored-bytes difference that is a config artifact rather than
    -- a property of either table format.
    'write.parquet.compression-level' = '1',
    'write.parquet.row-group-size-bytes' = '16777216',
    'history.expire.max-snapshot-age-ms' = '3600000',
    'history.expire.min-snapshots-to-keep' = '5',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '3',
    'commit.manifest.target-size-bytes' = '8388608',
    'commit.manifest-merge.enabled' = 'true',
    'write.metadata.metrics.default' = 'full'
);

-- ============================================================================
-- Table: order_items (Fact Table — partitioned by created_dt)
-- ============================================================================
-- Highest write volume (multiple items per order), append-heavy.
-- Explicit created_dt STRING column computed from created_at via DATE_FORMAT().
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10, 2),
    subtotal DECIMAL(12, 2),
    created_at TIMESTAMP(3),
    created_dt STRING,
    PRIMARY KEY (created_dt, order_item_id) NOT ENFORCED
) PARTITIONED BY (created_dt) WITH (
    -- ICEBERG_FORMAT_VERSION (3 default, 2 opt-out): v3 replaces positional-delete
    -- files with DVs, which StarRocks 4.1.x cannot read — see the header.
    'format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    'write.upsert.enabled' = 'true',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.distribution-mode' = 'hash',
    'write.target-file-size-bytes' = '67108864',
    'write.parquet.compression-codec' = 'zstd',
    -- Level 1, to match Paimon's 'file.compression.zstd-level' = '1'. Both sides
    -- write ZSTD Parquet, but Iceberg sets no level, so parquet-mr defaults to 3 —
    -- a different CPU-vs-ratio tradeoff on the write path, which shows up as both a
    -- throughput and a stored-bytes difference that is a config artifact rather than
    -- a property of either table format.
    'write.parquet.compression-level' = '1',
    'write.parquet.row-group-size-bytes' = '16777216',
    'history.expire.max-snapshot-age-ms' = '3600000',
    'history.expire.min-snapshots-to-keep' = '5',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '3',
    'commit.manifest.target-size-bytes' = '8388608',
    'commit.manifest-merge.enabled' = 'true',
    'write.metadata.metrics.default' = 'full'
);
