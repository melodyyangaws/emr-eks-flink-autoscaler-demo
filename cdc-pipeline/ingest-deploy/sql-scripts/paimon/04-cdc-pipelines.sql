-- ============================================================================
-- Paimon CDC Streaming Pipelines
-- ============================================================================
-- Reads from MySQL JDBC catalog and writes to Paimon tables.
--
-- Required environment variables:
--   MYSQL_DATABASE: Database name (default: ecommerce)
--   GLUE_DATABASE: target Paimon database (default: flink_paimon_db; the
--     manifest sets flink_paimon_db + CDC_DB_SUFFIX)
--
-- Note: These are long-running streaming INSERT statements
--
-- Write-path options are INSERT hints, not table properties: the sinks are
-- CREATE TABLE IF NOT EXISTS, so a hint is the only way to change them on an
-- existing table (none is an immutable option). Buffer: 64mb dimension, 128mb
-- fact tables, the same as Iceberg's write.target-file-size-bytes per table:
--   write-only=true            no inline compaction AND no snapshot expiry in the
--                              writer; small files and snapshots accumulate until a
--                              separate compaction runs (state it with any result)
--   write-buffer-spillable     true: a full buffer spills a sorted run and the
--                              checkpoint flush merges them; false: flushes an L0
--                              file in the record path
-- Set via PAIMON_WRITE_ONLY / PAIMON_WRITE_BUFFER_SPILLABLE / PAIMON_WRITE_BUFFER_SIZE
-- (build-deploy-generic.sh; needs table.dynamic-table-options.enabled, which the
-- executor sets).
-- ============================================================================
-- SET 'execution.checkpointing.interval' = '60s';
-- SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- SET 'table.exec.sink.upsert-materialize' = 'NONE';
-- ============================================================================
-- Pipeline 1: Customers (Dimension Table)
-- ============================================================================
INSERT INTO paimon_catalog.${GLUE_DATABASE:flink_paimon_db}.customers /*+ OPTIONS(
    'write-only' = '${PAIMON_WRITE_ONLY:false}',
    'write-buffer-spillable' = '${PAIMON_WRITE_BUFFER_SPILLABLE:true}',
    'write-buffer-size' = '${PAIMON_WRITE_BUFFER_SIZE_DIM:64mb}'
) */
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
INSERT INTO paimon_catalog.${GLUE_DATABASE:flink_paimon_db}.products /*+ OPTIONS(
    'write-only' = '${PAIMON_WRITE_ONLY:false}',
    'write-buffer-spillable' = '${PAIMON_WRITE_BUFFER_SPILLABLE:true}',
    'write-buffer-size' = '${PAIMON_WRITE_BUFFER_SIZE_DIM:64mb}'
) */
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
INSERT INTO paimon_catalog.${GLUE_DATABASE:flink_paimon_db}.orders /*+ OPTIONS(
    'write-only' = '${PAIMON_WRITE_ONLY:false}',
    'write-buffer-spillable' = '${PAIMON_WRITE_BUFFER_SPILLABLE:true}',
    'write-buffer-size' = '${PAIMON_WRITE_BUFFER_SIZE_FACT:128mb}'
) */
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
INSERT INTO paimon_catalog.${GLUE_DATABASE:flink_paimon_db}.order_items /*+ OPTIONS(
    'write-only' = '${PAIMON_WRITE_ONLY:false}',
    'write-buffer-spillable' = '${PAIMON_WRITE_BUFFER_SPILLABLE:true}',
    'write-buffer-size' = '${PAIMON_WRITE_BUFFER_SIZE_FACT:128mb}'
) */
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
