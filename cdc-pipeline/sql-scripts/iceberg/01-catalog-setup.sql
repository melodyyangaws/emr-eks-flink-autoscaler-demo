
-- ============================================================================
-- Iceberg Catalog Setup with AWS Glue (Iceberg Flink runtime 1.12.0)
-- ============================================================================
-- Creates an Apache Iceberg catalog using AWS Glue as metadata service.
--
-- Environment variables:
--   ICEBERG_WAREHOUSE: S3 path for Iceberg warehouse (e.g., s3://bucket/iceberg/)
--   GLUE_DATABASE: AWS Glue database name (default: flink_iceberg_db; the
--     manifest sets flink_iceberg_db + CDC_DB_SUFFIX, e.g. flink_iceberg_db_v3)
--   AWS_REGION: AWS region
--   ICEBERG_FORMAT_VERSION: 3 (default on this branch) or 2
--
-- Iceberg features used by this pipeline:
--   - Upserts via equality deletes + sequence numbers (both v2 and v3)
--   - Deletes of rows written earlier in the same checkpoint: positional delete
--     files on v2, deletion vectors (Puffin) on v3
--   - Min/max column statistics for predicate pushdown
--
-- The 'format-version' that matters is the per-table one in
-- 03-iceberg-sinks.sql. Iceberg catalogs take table defaults only from
-- 'table-default.*' catalog properties (CatalogProperties.TABLE_DEFAULT_PREFIX),
-- so a bare catalog-level 'format-version' does not set the tables' version.
-- It is kept here only so the two files visibly agree, driven by the same
-- ICEBERG_FORMAT_VERSION. See 03-iceberg-sinks.sql for what v3 changes and
-- which readers can handle it.
-- ============================================================================

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    'warehouse' = '${ICEBERG_WAREHOUSE}',
    's3.region' = '${AWS_REGION}',
    'catalog-name' = 'iceberg_glue_catalog',
    'catalog-database' = '${GLUE_DATABASE:flink_iceberg_db}',
    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
    -- urlconnection, not Iceberg 1.12's default 'apache': that type builds the
    -- SDK's Apache5HttpClient, whose relocated httpcore5 classes are not in
    -- EMR 7.14's AWS SDK v2 bundle (2.44.5), and GlueCatalog.initialize then
    -- fails with NoClassDefFoundError ...thirdparty/org/apache/hc/core5/http/
    -- ClassicHttpRequest (first v3 deploy, 2026-11-02).
    'http-client.type' = 'urlconnection',
    'format-version' = '${ICEBERG_FORMAT_VERSION:3}'
);

-- Switch to Iceberg catalog
USE CATALOG iceberg_catalog;
-- Create Glue database if not exists
CREATE DATABASE IF NOT EXISTS ${GLUE_DATABASE:flink_iceberg_db};
