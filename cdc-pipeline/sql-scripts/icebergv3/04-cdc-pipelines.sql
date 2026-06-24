-- ============================================================================
-- Iceberg CDC Streaming Pipelines
-- ============================================================================
-- Reads from MySQL CDC sources and writes to Iceberg V3 tables.
-- Explicit DATE STRING partition columns computed via DATE_FORMAT().
-- ============================================================================

-- ============================================================================
-- Pipeline 1: Customers (Dimension Table — unpartitioned)
-- ============================================================================
INSERT INTO icebergv3_catalog.${GLUE_DATABASE:flink_icebergv3_db}.customers
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
INSERT INTO icebergv3_catalog.${GLUE_DATABASE:flink_icebergv3_db}.products
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
INSERT INTO icebergv3_catalog.${GLUE_DATABASE:flink_icebergv3_db}.orders
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
INSERT INTO icebergv3_catalog.${GLUE_DATABASE:flink_icebergv3_db}.order_items
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
