-- ============================================================================
-- Iceberg V3 Sink Tables with Deletion Vectors
-- ============================================================================
-- Creates Apache Iceberg V3 sink tables with:
--   - Deletion vectors (DV) for efficient row-level deletes/updates
--   - Merge-on-read mode for low-latency CDC writes
--   - UPSERT enabled for CDC streaming
--
-- Partitioning: Flink SQL does NOT support Iceberg hidden partition transforms
-- (days(), months(), bucket()). Those require Spark SQL or the Java catalog API.
-- We use explicit DATE partition columns computed via DATE_FORMAT() in the
-- pipeline INSERT statements (04-cdc-pipelines.sql).
--
-- Required environment variables:
--   GLUE_DATABASE: AWS Glue database name (default: flink_iceberg_db)
-- ============================================================================

-- Switch to Iceberg catalog
USE CATALOG iceberg_catalog;
USE ${GLUE_DATABASE:flink_iceberg_db};

-- ============================================================================
-- Table: customers (Dimension Table — unpartitioned)
-- ============================================================================
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
    'format-version' = '3',
    'write.upsert.enabled' = 'true',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '5',
    'write.metadata.metrics.default' = 'full',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'hash'
);

-- ============================================================================
-- Table: products (Dimension Table — partitioned by category)
-- ============================================================================
CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock_quantity INT,
    description STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (category,product_id) NOT ENFORCED
) PARTITIONED BY (category) WITH (
    'format-version' = '3',
    'write.upsert.enabled' = 'true',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '5',
    'write.metadata.metrics.default' = 'full',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'hash'
);

-- ============================================================================
-- Table: orders (Fact Table — partitioned by order_dt)
-- ============================================================================
-- Explicit DATE STRING column computed from order_date in 04-cdc-pipelines.sql.
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
    'format-version' = '3',
    'write.upsert.enabled' = 'true',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '5',
    'write.metadata.metrics.default' = 'full',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'hash'
);

-- ============================================================================
-- Table: order_items (Fact Table — partitioned by created_dt)
-- ============================================================================
-- Explicit DATE STRING column computed from created_at in 04-cdc-pipelines.sql.
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
    'format-version' = '3',
    'write.upsert.enabled' = 'true',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '5',
    'write.metadata.metrics.default' = 'full',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'hash'
);
