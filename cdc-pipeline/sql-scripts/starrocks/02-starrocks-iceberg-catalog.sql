-- ============================================================================
-- StarRocks Catalog Configuration for Iceberg
-- ============================================================================
-- Creates external catalog in StarRocks to query Iceberg tables via AWS Glue
--
-- Prerequisites:
--   - Iceberg tables created by Flink CDC
--   - StarRocks cluster running on EKS with AWS IAM role
--   - AWS Glue database: flink_icebergv3_db
-- ============================================================================

-- Connect to StarRocks
-- mysql -h <STARROCKS_FE_LB> -P 9030 -u root

-- Create Iceberg catalog with AWS Glue
CREATE EXTERNAL CATALOG icebergv3_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "glue",
    "aws.glue.region" = "us-west-2",
    "aws.s3.region" = "us-west-2",
    "aws.s3.use_instance_profile" = "true",
    "client.factory" = "com.starrocks.connector.iceberg.glue.IcebergGlueCatalogFactory"
);

-- Switch to Iceberg catalog
SET CATALOG icebergv3_catalog;

-- Show databases
SHOW DATABASES;

-- Use Glue database
USE flink_icebergv3_db;

-- Show tables
SHOW TABLES;

-- Verify table schema
DESC customers;

-- Test query
SELECT COUNT(*) FROM customers;

-- Query with filters
SELECT
    city,
    state,
    COUNT(*) as customer_count
FROM customers
GROUP BY city, state
ORDER BY customer_count DESC
LIMIT 10;

-- ============================================================================
-- Performance Test Queries for Iceberg
-- ============================================================================

-- Q1: Simple aggregation
SELECT COUNT(*) as total_customers FROM customers;

-- Q2: Group by with aggregation
SELECT
    state,
    COUNT(*) as customer_count,
    COUNT(DISTINCT city) as city_count
FROM customers
GROUP BY state
ORDER BY customer_count DESC;

-- Q3: Join query
SELECT
    c.customer_name,
    c.city,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.city
ORDER BY total_spent DESC
LIMIT 20;

-- Q4: Product sales analysis
SELECT
    p.product_name,
    p.category,
    SUM(oi.quantity) as units_sold,
    SUM(oi.subtotal) as revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue DESC
LIMIT 20;

-- Q5: Time-based analysis (Iceberg hidden partitioning)
-- Note: Iceberg uses DAYS(order_date) partitioning automatically
SELECT
    DATE(order_date) as order_day,
    COUNT(*) as order_count,
    SUM(total_amount) as daily_revenue,
    AVG(total_amount) as avg_order_value
FROM orders
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY DATE(order_date)
ORDER BY order_day DESC;

-- Q6: Complex multi-table join
SELECT
    DATE(o.order_date) as order_day,
    c.state,
    p.category,
    COUNT(DISTINCT o.order_id) as orders,
    SUM(oi.quantity) as units_sold,
    SUM(oi.subtotal) as revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'DELIVERED'
GROUP BY DATE(o.order_date), c.state, p.category
ORDER BY order_day DESC, revenue DESC
LIMIT 100;

-- ============================================================================
-- Iceberg Time Travel Queries (StarRocks 3.2+)
-- ============================================================================

-- Query as of specific timestamp
SELECT * FROM customers
FOR VERSION AS OF '2026-02-20 00:00:00';

-- Query specific snapshot ID
SELECT * FROM customers
FOR VERSION AS OF SNAPSHOT_ID;

-- View snapshot history
SELECT * FROM customers FOR SYSTEM_VERSION AS OF CURRENT_SNAPSHOT();
