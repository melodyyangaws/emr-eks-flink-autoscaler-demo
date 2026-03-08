-- ============================================================================
-- Paimon Sink Tables — Glue Catalog + Iceberg Compat + Deletion Vectors
-- ============================================================================
-- Creates Apache Paimon sink tables with:
--   - Deletion vectors: marks deleted rows in a bitmap instead of rewriting
--     entire data files — reduces write amplification for CDC workloads
--   - Deduplicate merge engine (default): last-write-wins upsert via PK
--   - Iceberg compatibility: IcebergHiveMetadataCommitter registers read-only
--     Iceberg metadata in the same Glue catalog via EMR's /glue/confs/ dirs.
--     Iceberg readers (Athena/Trino/Spark) see clean data after DV resolution.
--   - Input changelog producer for downstream CDC consumers
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
-- Table: customers (Dimension Table)
-- ============================================================================
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
    'bucket' = '4',
    'changelog-producer' = 'input',
    -- 'deletion-vectors.enabled' = 'true',
    -- Iceberg compatibility via EMR Glue conf
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.manifest-legacy-version' = 'true',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'fs.s3.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    -- 'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    -- 'metadata.iceberg.hadoop-conf-dir' = '/glue/confs/hadoop/conf',
    'metadata.iceberg.format-version' = '3',
    -- Compaction
    'num-sorted-run.compaction-trigger' = '4',
    -- Snapshot retention
    'snapshot.time-retained' = '1h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '10',
    -- register table as Iceberg table in Glue to enable Athena query
    'table_type' = 'ICEBERG'
);

-- ============================================================================
-- Table: products (Dimension Table — partitioned by category)
-- ============================================================================
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
    'bucket' = '4',
    'changelog-producer' = 'input',
    -- 'deletion-vectors.enabled' = 'true',
    -- Iceberg compatibility via EMR Glue conf
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.manifest-legacy-version' = 'true',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'fs.s3.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    -- 'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    -- 'metadata.iceberg.hadoop-conf-dir' = '/glue/confs/hadoop/conf',
    'metadata.iceberg.format-version' = '3',
    -- Compaction
    'num-sorted-run.compaction-trigger' = '4',
    -- Snapshot retention
    'snapshot.time-retained' = '1h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '10',
    -- register table as Iceberg table in Glue to enable Athena query
    'table_type' = 'ICEBERG'
);

-- ============================================================================
-- Table: orders (Fact Table — partitioned by date)
-- ============================================================================
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
    'bucket' = '8',
    'changelog-producer' = 'input',
    -- 'deletion-vectors.enabled' = 'true',
    -- Iceberg compatibility via EMR Glue conf
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.manifest-legacy-version' = 'true',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'fs.s3.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    -- 'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    -- 'metadata.iceberg.hadoop-conf-dir' = '/glue/confs/hadoop/conf',
    'metadata.iceberg.format-version' = '3',
    -- Compaction
    'num-sorted-run.compaction-trigger' = '4',
    -- Snapshot retention
    'snapshot.time-retained' = '1h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '10',
    -- register table as Iceberg table in Glue to enable Athena query
    'table_type' = 'ICEBERG'
);

-- ============================================================================
-- Table: order_items (Fact Table — partitioned by date)
-- ============================================================================
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
    'bucket' = '8',
    'changelog-producer' = 'input',
    -- 'deletion-vectors.enabled' = 'true',
    -- Iceberg compatibility via EMR Glue conf
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.manifest-legacy-version' = 'true',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'fs.s3.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    -- 'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    -- 'metadata.iceberg.hadoop-conf-dir' = '/glue/confs/hadoop/conf',
    'metadata.iceberg.format-version' = '3',
    -- Compaction
    'num-sorted-run.compaction-trigger' = '4',
    -- Snapshot retention
    'snapshot.time-retained' = '1h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '10',
    -- register table as Iceberg table in Glue to enable Athena query
    'table_type' = 'ICEBERG'
);
