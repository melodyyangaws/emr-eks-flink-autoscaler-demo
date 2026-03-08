-- ============================================================================
-- Iceberg CDC Streaming Pipelines
-- ============================================================================
-- Reads from MySQL CDC sources and writes to Iceberg V3 tables.
-- Computes explicit partition columns via DATE_FORMAT() because Flink SQL
-- does NOT support Iceberg hidden partition transforms (days(), months()).
--
-- Required environment variables:
--   GLUE_DATABASE: AWS Glue database name (default: flink_iceberg_db)
--   MYSQL_DATABASE: MySQL database name (default: ecommerce)
-- ============================================================================

-- ============================================================================
-- Pipeline 1: Customers (Dimension Table — unpartitioned)
-- ============================================================================
INSERT INTO iceberg_catalog.${GLUE_DATABASE:flink_iceberg_db}.customers
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
-- Pipeline 2: Products (Dimension Table — partitioned by category)
-- ============================================================================
INSERT INTO iceberg_catalog.${GLUE_DATABASE:flink_iceberg_db}.products
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
-- Pipeline 3: Orders (Fact Table — partitioned by order_dt)
-- ============================================================================
-- DATE_FORMAT computes the explicit partition column order_dt from order_date.
INSERT INTO iceberg_catalog.${GLUE_DATABASE:flink_iceberg_db}.orders
SELECT
    order_id,
    customer_id,
    order_date,
    DATE_FORMAT(order_date, 'yyyy-MM-dd') AS order_dt,
    total_amount,
    order_status,
    payment_method,
    shipping_address,
    updated_at
FROM mysql_catalog.${MYSQL_DATABASE:ecommerce}.mysql_src_orders;

-- ============================================================================
-- Pipeline 4: Order Items (Fact Table — partitioned by created_dt)
-- ============================================================================
-- DATE_FORMAT computes the explicit partition column created_dt from created_at.
INSERT INTO iceberg_catalog.${GLUE_DATABASE:flink_iceberg_db}.order_items
SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    subtotal,
    created_at,
    DATE_FORMAT(created_at, 'yyyy-MM-dd') AS created_dt
FROM mysql_catalog.${MYSQL_DATABASE:ecommerce}.mysql_src_order_items;
