-- ============================================================================
-- Paimon Sink Tables — Streaming CDC Optimized
-- ============================================================================
-- Creates Apache Paimon sink tables optimized for low-latency CDC:
--   - Deletion vectors: bitmap-based row deletes without rewriting data files
--   - Deduplicate merge engine (default): last-write-wins upsert via PK
--   - ZSTD compression for better ratio on CDC payloads
--   - Tuned compaction, write buffers, and snapshot retention for 60s checkpoints
--   - Iceberg compatibility: IcebergHadoopMetadataCommitter writes read-only
--     Iceberg V3 metadata (deletion vectors) to S3 for Athena/Spark queries
--   - Input changelog producer for downstream CDC consumers
--
-- Streaming best practices applied:
--   1. Spillable write buffers to prevent backpressure during burst writes
--   2. Smaller target file sizes (32-64MB) to match checkpoint-driven writes
--   3. Tuned compaction triggers to balance write amp vs read performance
--   4. Manifest merging to keep read planning fast
--   5. Async snapshot expiration to avoid checkpoint latency spikes
--
-- Valid metadata.iceberg.* options (Paimon 1.3.0):
--   storage, uri, hive-conf-dir, hadoop-conf-dir, format-version,
--   manifest-compression
--
-- Required: 'aws.glue.enabled: "true"' in flinkConfiguration
-- Required environment variables:
--   GLUE_DATABASE: Glue database name (default: flink_paimonv3_db)
-- ============================================================================

-- Switch to Paimon catalog
USE CATALOG paimon_catalogv3;
USE ${GLUE_DATABASE:flink_paimonv3_db};

-- ============================================================================
-- Table: customers (Dimension Table — unpartitioned)
-- ============================================================================
-- Low cardinality, frequent updates (address/phone changes).
-- Fewer buckets (2) since total data volume is small.
-- Smaller file target (32MB) and write buffer to save memory.
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    customer_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    zip_code VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'bucket' = '-1',
    'changelog-producer' = 'lookup',
    'deletion-vectors.enabled' = 'true',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    'file.compression.zstd-level' = '1',
    'target-file-size' = '32mb',
    'write-buffer-size' = '128mb',
    'write-buffer-spillable' = 'true',
    'num-sorted-run.compaction-trigger' = '4',
    'compaction.max.file-num' = '8',
    'snapshot.time-retained' = '1h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '10',
    'snapshot.expire.execution-mode' = 'async',
    'manifest.target-file-size' = '8mb',
    'manifest.merge-min-count' = '5',
    -- Iceberg compatibility via EMR Glue conf
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    'metadata.iceberg.format-version' = '3',
    'metadata.iceberg.manifest-compression' = 'zstd',
    'sink.writer-coordinator.enabled' = 'true'
);

-- ============================================================================
-- Table: products (Dimension Table — partitioned by category)
-- ============================================================================
-- Medium cardinality, frequent stock_quantity and price updates.
-- Partitioned by category for query pruning (WHERE category = 'Electronics').
CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name VARCHAR,
    category VARCHAR,
    price DECIMAL(10, 2),
    stock_quantity INT,
    description VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    PRIMARY KEY (category, product_id) NOT ENFORCED
) PARTITIONED BY (category) WITH (
    'bucket' = '-1',
    'changelog-producer' = 'lookup',
    'deletion-vectors.enabled' = 'true',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    'file.compression.zstd-level' = '1',
    'target-file-size' = '32mb',
    'write-buffer-size' = '128mb',
    'write-buffer-spillable' = 'true',
    'num-sorted-run.compaction-trigger' = '4',
    'compaction.max.file-num' = '8',
    'snapshot.time-retained' = '1h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '10',
    'snapshot.expire.execution-mode' = 'async',
    'manifest.target-file-size' = '8mb',
    'manifest.merge-min-count' = '5',
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    'metadata.iceberg.format-version' = '3',
    'metadata.iceberg.manifest-compression' = 'zstd',
    'sink.writer-coordinator.enabled' = 'true'
);

-- ============================================================================
-- Table: orders (Fact Table — partitioned by date)
-- ============================================================================
-- High write volume, mostly inserts + status updates.
-- 4 buckets to handle higher throughput per partition.
-- Larger file target (64MB) and write buffer for sustained throughput.
CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    customer_id INT,
    order_date TIMESTAMP(6),
    order_date_str VARCHAR,
    total_amount DECIMAL(12, 2),
    order_status VARCHAR,
    payment_method VARCHAR,
    shipping_address VARCHAR,
    updated_at TIMESTAMP(6),
    PRIMARY KEY (order_date_str, order_id) NOT ENFORCED
) PARTITIONED BY (order_date_str) WITH (
    'bucket' = '-1',
    'changelog-producer' = 'lookup',
    'deletion-vectors.enabled' = 'true',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    'file.compression.zstd-level' = '1',
    'target-file-size' = '64mb',
    'write-buffer-size' = '256mb',
    'write-buffer-spillable' = 'true',
    'num-sorted-run.compaction-trigger' = '4',
    'compaction.max.file-num' = '10',
    'snapshot.time-retained' = '1h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '10',
    'snapshot.expire.execution-mode' = 'async',
    'manifest.target-file-size' = '8mb',
    'manifest.merge-min-count' = '5',
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    'metadata.iceberg.format-version' = '3',
    'metadata.iceberg.manifest-compression' = 'zstd',
    'sink.writer-coordinator.enabled' = 'true'
);

-- ============================================================================
-- Table: order_items (Fact Table — partitioned by date)
-- ============================================================================
-- Highest write volume (multiple items per order), append-heavy.
-- 4 buckets for parallelism. Largest write buffer.
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10, 2),
    subtotal DECIMAL(12, 2),
    created_at TIMESTAMP(6),
    created_date_str VARCHAR,
    PRIMARY KEY (created_date_str, order_item_id) NOT ENFORCED
) PARTITIONED BY (created_date_str) WITH (
    'bucket' = '-1',
    'changelog-producer' = 'lookup',
    'deletion-vectors.enabled' = 'true',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    'file.compression.zstd-level' = '1',
    'target-file-size' = '64mb',
    'write-buffer-size' = '256mb',
    'write-buffer-spillable' = 'true',
    'num-sorted-run.compaction-trigger' = '4',
    'compaction.max.file-num' = '10',
    'snapshot.time-retained' = '1h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '10',
    'snapshot.expire.execution-mode' = 'async',
    'manifest.target-file-size' = '8mb',
    'manifest.merge-min-count' = '5',
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    'metadata.iceberg.format-version' = '3',
    'metadata.iceberg.manifest-compression' = 'zstd',
    'sink.writer-coordinator.enabled' = 'true'
);
