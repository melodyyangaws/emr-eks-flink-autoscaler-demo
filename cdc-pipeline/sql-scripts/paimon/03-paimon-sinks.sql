-- ============================================================================
-- Paimon Sink Tables — Streaming CDC Optimized
-- ============================================================================
-- Creates Apache Paimon sink tables optimized for low-latency CDC:
--   - Deletion vectors: bitmap-based row deletes without rewriting data files
--   - Deduplicate merge engine (default): last-write-wins upsert via PK
--   - ZSTD compression for the Parquet DATA files (better ratio on CDC payloads);
--     the Iceberg-compat Avro MANIFESTS use snappy instead, because StarRocks BE
--     has no zstd-jni on its Iceberg reader classpath — see the
--     metadata.iceberg.manifest-compression note on `customers` below
--   - Tuned compaction, write buffers, and snapshot retention for 60s checkpoints
--   - Iceberg compatibility: IcebergHadoopMetadataCommitter writes read-only
--     Iceberg V2 metadata to S3 for Athena/Spark queries
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
--   GLUE_DATABASE: Glue database name (default: flink_paimon_db)
-- ============================================================================

-- Switch to Paimon catalog
USE CATALOG paimon_catalog;
USE ${GLUE_DATABASE:flink_paimon_db};

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
    -- Matches Iceberg's 'write.parquet.row-group-size-bytes' = '8388608'. Without
    -- this Paimon inherits parquet-mr's 128MB default block size and emits ONE row
    -- group for a whole 32MB file (measured: 1 x 84MB row group, vs Iceberg's
    -- 5 x 16MB). Row group is the unit of predicate pushdown and scan splitting, so
    -- a single one means the reader can neither skip within the file nor parallelize
    -- across it — that penalizes Paimon on the read benchmark for a reason that has
    -- nothing to do with the LSM-vs-merge-on-read question being measured.
    'parquet.block.size' = '8388608',
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
    -- '2', not '3'. This is the format version of the Iceberg-compatible
    -- metadata Paimon exports under <table>/iceberg/, and Athena engine v3 only
    -- reads Iceberg V2 — see README "Paimon Tables" and 4-STARROCKS-OLAP-ENGINE.md
    'metadata.iceberg.format-version' = '2',
    -- 'snappy', not 'zstd'. This compresses the Avro MANIFEST files of the
    -- Iceberg-compatible metadata Paimon exports under <table>/iceberg/, and it
    -- has to be a codec the READER has on its classpath, not just the best
    -- ratio. StarRocks BE ships avro-1.12.0.jar and snappy-java in
    -- be/lib/iceberg-reader-lib/ but NO zstd-jni (hudi-reader-lib,
    -- kudu-reader-lib and odps-reader-lib each bundle it; the Iceberg one does
    -- not). Avro's ZstandardCodec class itself IS present, so the codec is
    -- selected and then dies on its missing native binding:
    --
    --   java.lang.NoClassDefFoundError: com/github/luben/zstd/ZstdInputStreamNoFinalizer
    --     at org.apache.avro.file.ZstandardCodec.decompress(ZstandardCodec.java:84)
    --     at org.apache.iceberg.avro.AvroIterable$AvroReuseIterator.hasNext(...)
    --
    -- Fixing it BE-side is not durable: /opt/starrocks/be/lib is a container
    -- layer (a copied zstd-jni jar is lost on restart, and the classloader is
    -- built at BE start so a live copy has no effect anyway), and
    -- /etc/starrocks/be/conf is a read-only ConfigMap mount. Writing a codec
    -- the reader already supports is the fix that survives a pod restart.
    -- Costs a little manifest size; manifests are tiny next to the data files.
    'metadata.iceberg.manifest-compression' = 'snappy',
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
    -- Matches Iceberg's 'write.parquet.row-group-size-bytes' = '8388608'. Without
    -- this Paimon inherits parquet-mr's 128MB default block size and emits ONE row
    -- group for a whole 32MB file (measured: 1 x 84MB row group, vs Iceberg's
    -- 5 x 16MB). Row group is the unit of predicate pushdown and scan splitting, so
    -- a single one means the reader can neither skip within the file nor parallelize
    -- across it — that penalizes Paimon on the read benchmark for a reason that has
    -- nothing to do with the LSM-vs-merge-on-read question being measured.
    'parquet.block.size' = '8388608',
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
    -- '2', not '3'. This is the format version of the Iceberg-compatible
    -- metadata Paimon exports under <table>/iceberg/, and Athena engine v3 only
    -- reads Iceberg V2 — see README "Paimon Tables" and 4-STARROCKS-OLAP-ENGINE.md
    'metadata.iceberg.format-version' = '2',
    -- snappy, not zstd — see the manifest-compression note on `customers`.
    'metadata.iceberg.manifest-compression' = 'snappy',
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
    -- 16MB to match Iceberg's row-group-size on the fact tables — see the
    -- parquet.block.size note on `customers`.
    'parquet.block.size' = '16777216',
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
    -- '2', not '3'. This is the format version of the Iceberg-compatible
    -- metadata Paimon exports under <table>/iceberg/, and Athena engine v3 only
    -- reads Iceberg V2 — see README "Paimon Tables" and 4-STARROCKS-OLAP-ENGINE.md
    'metadata.iceberg.format-version' = '2',
    -- snappy, not zstd — see the manifest-compression note on `customers`.
    'metadata.iceberg.manifest-compression' = 'snappy',
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
    -- 16MB to match Iceberg's row-group-size on the fact tables — see the
    -- parquet.block.size note on `customers`.
    'parquet.block.size' = '16777216',
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
    -- '2', not '3'. This is the format version of the Iceberg-compatible
    -- metadata Paimon exports under <table>/iceberg/, and Athena engine v3 only
    -- reads Iceberg V2 — see README "Paimon Tables" and 4-STARROCKS-OLAP-ENGINE.md
    'metadata.iceberg.format-version' = '2',
    -- snappy, not zstd — see the manifest-compression note on `customers`.
    'metadata.iceberg.manifest-compression' = 'snappy',
    'sink.writer-coordinator.enabled' = 'true'
);
