-- ============================================================================
-- Paimon CDC Streaming Pipelines
-- ============================================================================
-- Reads from MySQL JDBC catalog and writes to Paimon tables.
--
-- Required environment variables:
--   MYSQL_DATABASE: Database name (default: ecommerce)
--
-- Note: These are long-running streaming INSERT statements
-- ============================================================================
-- SET 'execution.checkpointing.interval' = '10s';
-- SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- SET 'table.exec.sink.upsert-materialize' = 'NONE';
-- ============================================================================
-- Pipeline 1: Customers (Dimension Table)
-- ============================================================================
INSERT INTO paimon_catalog.${GLUE_DATABASE:flink_paimon_db}.customers
SELECT
    customer_id,
    customer_name,
    email,
    phone,
    address,
    city,
    state,
    country,
    zip_code,
    created_at,
    updated_at
FROM mysql_catalog.${MYSQL_DATABASE:ecommerce}.mysql_src_customers;

-- ============================================================================
-- Pipeline 2: Products (Dimension Table)
-- ============================================================================
INSERT INTO paimon_catalog.${GLUE_DATABASE:flink_paimon_db}.products
SELECT
    product_id,
    product_name,
    category,
    price,
    stock_quantity,
    description,
    created_at,
    updated_at
FROM mysql_catalog.${MYSQL_DATABASE:ecommerce}.mysql_src_products;

-- ============================================================================
-- Pipeline 3: Orders (Fact Table with Date Partitioning)
-- ============================================================================
INSERT INTO paimon_catalog.${GLUE_DATABASE:flink_paimon_db}.orders
SELECT
    order_id,
    customer_id,
    order_date,
    DATE_FORMAT(order_date, 'yyyy-MM-dd') as order_date_str,
    total_amount,
    order_status,
    payment_method,
    shipping_address,
    updated_at
FROM mysql_catalog.${MYSQL_DATABASE:ecommerce}.mysql_src_orders;

-- ============================================================================
-- Pipeline 4: Order Items (Fact Table with Date Partitioning)
-- ============================================================================
INSERT INTO paimon_catalog.${GLUE_DATABASE:flink_paimon_db}.order_items
SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    subtotal,
    created_at,
    DATE_FORMAT(created_at, 'yyyy-MM-dd') as created_date_str
FROM mysql_catalog.${MYSQL_DATABASE:ecommerce}.mysql_src_order_items;
