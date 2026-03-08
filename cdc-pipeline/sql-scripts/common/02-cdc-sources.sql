-- ============================================================================
-- MySQL CDC Source Catalog & Tables
-- ============================================================================
-- Creates a dedicated catalog for MySQL CDC streaming sources.
-- Uses mysql-cdc connector for continuous binlog streaming (not batch JDBC).
--
-- Required environment variables:
--   MYSQL_HOST: MySQL hostname
--   MYSQL_PORT: MySQL port (default: 3306)
--   MYSQL_USER: MySQL username
--   MYSQL_PASSWORD: MySQL password
--   MYSQL_DATABASE: MySQL database name (default: ecommerce)
--
-- Features:
--   - Full snapshot on startup, then continuous binlog CDC
--   - Incremental chunked snapshot for parallel reads
--   - Non-blocking snapshot (no table locks on RDS)
--   - Heartbeat to prevent connection timeout during idle periods
--   - JDBC-level timeouts to prevent hung connections
-- ============================================================================

-- Create dedicated catalog for CDC sources (instead of default_catalog)
CREATE CATALOG mysql_catalog WITH ('type' = 'generic_in_memory');
USE CATALOG mysql_catalog;
CREATE DATABASE IF NOT EXISTS ${MYSQL_DATABASE:ecommerce};
USE ${MYSQL_DATABASE:ecommerce};

-- ============================================================================
-- Table: customers
-- ============================================================================
CREATE TEMPORARY TABLE IF NOT EXISTS mysql_src_customers (
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
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT:3306}',
    'username' = '${MYSQL_USER}',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = '${MYSQL_DATABASE:ecommerce}',
    'table-name' = 'customers',
    'server-time-zone' = 'UTC',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.key-column' = 'customer_id'
);

-- ============================================================================
-- Table: products
-- ============================================================================
CREATE TEMPORARY TABLE IF NOT EXISTS mysql_src_products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock_quantity INT,
    description STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT:3306}',
    'username' = '${MYSQL_USER}',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = '${MYSQL_DATABASE:ecommerce}',
    'table-name' = 'products',
    'server-time-zone' = 'UTC',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.key-column' = 'product_id'
);

-- ============================================================================
-- Table: orders
-- ============================================================================
CREATE TEMPORARY TABLE IF NOT EXISTS mysql_src_orders (
    order_id INT,
    customer_id INT,
    order_date TIMESTAMP(3),
    total_amount DECIMAL(12, 2),
    order_status STRING,
    payment_method STRING,
    shipping_address STRING,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT:3306}',
    'username' = '${MYSQL_USER}',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = '${MYSQL_DATABASE:ecommerce}',
    'table-name' = 'orders',
    'server-time-zone' = 'UTC',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.key-column' = 'order_id'
);

-- ============================================================================
-- Table: order_items
-- ============================================================================
CREATE TEMPORARY TABLE IF NOT EXISTS mysql_src_order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10, 2),
    subtotal DECIMAL(12, 2),
    created_at TIMESTAMP(3),
    PRIMARY KEY (order_item_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${MYSQL_HOST}',
    'port' = '${MYSQL_PORT:3306}',
    'username' = '${MYSQL_USER}',
    'password' = '${MYSQL_PASSWORD}',
    'database-name' = '${MYSQL_DATABASE:ecommerce}',
    'table-name' = 'order_items',
    'server-time-zone' = 'UTC',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.key-column' = 'order_item_id'
);
