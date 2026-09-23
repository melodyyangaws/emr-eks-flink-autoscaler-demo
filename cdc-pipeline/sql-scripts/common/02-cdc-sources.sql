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
--   CDC_SERVER_ID_CUSTOMERS, CDC_SERVER_ID_PRODUCTS, CDC_SERVER_ID_ORDERS,
--   CDC_SERVER_ID_ORDER_ITEMS: binlog client id ranges (see below)
--
-- Features:
--   - Full snapshot on startup, then continuous binlog CDC
--   - Incremental chunked snapshot for parallel reads
--   - Non-blocking snapshot (no table locks on RDS)
--   - Heartbeat to prevent connection timeout during idle periods
--   - JDBC-level timeouts to prevent hung connections
--
-- ----------------------------------------------------------------------------
-- server-id: every source below MUST get an explicit, globally unique range
-- ----------------------------------------------------------------------------
-- Each source subtask opens its own binlog client, and MySQL treats a client's
-- server id as an exclusive identity: when a second client connects with an id
-- already in use, the server evicts the first one. The evicted subtask dies with
--
--   io.debezium.DebeziumException: A replica with the same server_uuid/server_id
--   as this replica has connected to the source
--
-- Leaving 'server-id' unset lets the connector pick ids on its own, and this
-- deployment opens 128 binlog clients at once (16 subtasks x 4 tables x 2 jobs,
-- Paimon and Iceberg reading the same MySQL), so ids collided constantly. The
-- sources then failed in a loop, no checkpoint ever completed ("Not all required
-- tasks are currently running"), and neither sink committed a snapshot — the
-- lakehouse tables sat frozen while /jobs still reported RUNNING.
--
-- The range must be at least as wide as the source parallelism; 32 each leaves
-- room to raise parallelism without re-planning the map. These are injected per
-- job from the FlinkDeployment manifests so the two pipelines never overlap:
--
--             customers    products     orders       order_items
--   Paimon    5401-5432    5441-5472    5481-5512    5521-5552
--   Iceberg   6401-6432    6441-6472    6481-6512    6521-6552
--
-- They are intentionally declared with no default: a missing value should fail
-- the job loudly at SQL-parse time rather than silently re-collide at runtime.
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
    -- Unique binlog client id range — see the server-id note in the header.
    'server-id' = '${CDC_SERVER_ID_CUSTOMERS}',
    'server-time-zone' = 'UTC',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.key-column' = 'customer_id',
    -- Bound the snapshot chunk explicitly instead of trusting MySQL's row
    -- estimate. information_schema.tables reported 3k rows for a 2.4M-row table
    -- (stale InnoDB stats), so the connector emitted ONE unchunked split
    -- (splitStart=null, splitEnd=null) and pulled whole tables into heap ->
    -- "OutOfMemoryError: Java heap space" in the TaskManager. A fixed chunk size
    -- caps each split regardless of how wrong the estimate is.
    'scan.incremental.snapshot.chunk.size' = '8096',
    -- Cap rows held in memory per fetch within a chunk.
    'scan.snapshot.fetch.size' = '1024'
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
    -- Unique binlog client id range — see the server-id note in the header.
    'server-id' = '${CDC_SERVER_ID_PRODUCTS}',
    'server-time-zone' = 'UTC',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.key-column' = 'product_id',
    -- Bound the snapshot chunk explicitly instead of trusting MySQL's row
    -- estimate. information_schema.tables reported 3k rows for a 2.4M-row table
    -- (stale InnoDB stats), so the connector emitted ONE unchunked split
    -- (splitStart=null, splitEnd=null) and pulled whole tables into heap ->
    -- "OutOfMemoryError: Java heap space" in the TaskManager. A fixed chunk size
    -- caps each split regardless of how wrong the estimate is.
    'scan.incremental.snapshot.chunk.size' = '8096',
    -- Cap rows held in memory per fetch within a chunk.
    'scan.snapshot.fetch.size' = '1024'
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
    -- Unique binlog client id range — see the server-id note in the header.
    'server-id' = '${CDC_SERVER_ID_ORDERS}',
    'server-time-zone' = 'UTC',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.key-column' = 'order_id',
    -- Bound the snapshot chunk explicitly instead of trusting MySQL's row
    -- estimate. information_schema.tables reported 3k rows for a 2.4M-row table
    -- (stale InnoDB stats), so the connector emitted ONE unchunked split
    -- (splitStart=null, splitEnd=null) and pulled whole tables into heap ->
    -- "OutOfMemoryError: Java heap space" in the TaskManager. A fixed chunk size
    -- caps each split regardless of how wrong the estimate is.
    'scan.incremental.snapshot.chunk.size' = '8096',
    -- Cap rows held in memory per fetch within a chunk.
    'scan.snapshot.fetch.size' = '1024'
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
    -- Unique binlog client id range — see the server-id note in the header.
    'server-id' = '${CDC_SERVER_ID_ORDER_ITEMS}',
    'server-time-zone' = 'UTC',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.key-column' = 'order_item_id',
    -- Bound the snapshot chunk explicitly instead of trusting MySQL's row
    -- estimate. information_schema.tables reported 3k rows for a 2.4M-row table
    -- (stale InnoDB stats), so the connector emitted ONE unchunked split
    -- (splitStart=null, splitEnd=null) and pulled whole tables into heap ->
    -- "OutOfMemoryError: Java heap space" in the TaskManager. A fixed chunk size
    -- caps each split regardless of how wrong the estimate is.
    'scan.incremental.snapshot.chunk.size' = '8096',
    -- Cap rows held in memory per fetch within a chunk.
    'scan.snapshot.fetch.size' = '1024'
);
