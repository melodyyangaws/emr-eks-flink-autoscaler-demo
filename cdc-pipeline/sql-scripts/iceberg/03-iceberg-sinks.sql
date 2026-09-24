-- ============================================================================
-- Iceberg V2 Sink Tables — Streaming CDC Optimized
-- ============================================================================
-- Creates Apache Iceberg V2 sink tables optimized for low-latency CDC:
--   - Positional delete files for row-level deletes (merge-on-read)
--   - UPSERT enabled with hash distribution for PK-based dedup
--   - ZSTD compression for better ratio on CDC payloads
--   - Tuned snapshot retention and metadata cleanup for 60s checkpoints
--   - Compatible with Athena engine v3
--
-- Keep 'format-version' = '2': the bundled iceberg-flink-runtime.jar sink writes
-- positional-delete Parquet files, which is what V2 expects. Raising it rejects
-- every checkpoint commit and silently freezes the table.
--
-- format-version applies at table creation only. Changing it here does not alter
-- tables that already exist; the four tables must be dropped (and their S3
-- warehouse prefix + Glue entries removed) before this DDL takes effect.
--
-- Streaming best practices applied:
--   1. Smaller target file sizes (32-64MB) to match checkpoint-driven writes
--   2. Hash distribution ensures same-PK rows land in the same file writer
--   3. Aggressive metadata cleanup (delete-after-commit) to prevent S3 bloat
--   4. Manifest merging to keep read planning fast as snapshots accumulate
--   5. Full column metrics for predicate pushdown in query engines
--
-- Partitioning: Explicit DATE STRING partition columns computed via
-- DATE_FORMAT() in the pipeline INSERT statements (04-cdc-pipelines.sql).
-- Partition columns MUST be in PRIMARY KEY for hash distribution + upsert.
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
    -- Matches the positional deletes the Flink sink writes — see the header.
    'format-version' = '2',
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
    -- Matches the positional deletes the Flink sink writes — see the header.
    'format-version' = '2',
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
    -- Matches the positional deletes the Flink sink writes — see the header.
    'format-version' = '2',
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
    -- Matches the positional deletes the Flink sink writes — see the header.
    'format-version' = '2',
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
