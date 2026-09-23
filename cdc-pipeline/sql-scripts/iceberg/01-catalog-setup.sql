
-- ============================================================================
-- Iceberg V2 Catalog Setup with AWS Glue
-- ============================================================================
-- Creates an Apache Iceberg catalog using AWS Glue as metadata service.
--
-- Required environment variables:
--   ICEBERG_WAREHOUSE: S3 path for Iceberg warehouse (e.g., s3://bucket/iceberg/)
--   GLUE_DATABASE: AWS Glue database name (e.g., flink_iceberg_db)
--   AWS_REGION: AWS region
--
-- Iceberg V2 features used by this pipeline:
--   - Row-level deletes via positional delete files (merge-on-read)
--   - Equality deletes and sequence numbers for CDC upserts
--   - Min/max column statistics for predicate pushdown
--
-- Keep format-version at 2 here and in 03-iceberg-sinks.sql; the bundled
-- iceberg-flink-runtime.jar sink writes positional-delete files, which is what
-- V2 expects. Raising it stops the sink from committing.
-- ============================================================================

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    'warehouse' = '${ICEBERG_WAREHOUSE}',
    's3.region' = '${AWS_REGION}',
    'catalog-name' = 'iceberg_glue_catalog',
    'catalog-database' = '${GLUE_DATABASE:flink_iceberg_db}',
    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
    'format-version' = '2'
);

-- Switch to Iceberg catalog
USE CATALOG iceberg_catalog;
-- Create Glue database if not exists
CREATE DATABASE IF NOT EXISTS ${GLUE_DATABASE:flink_iceberg_db};
