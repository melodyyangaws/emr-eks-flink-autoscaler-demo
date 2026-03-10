
-- ============================================================================
-- Iceberg V3 Catalog Setup with AWS Glue
-- ============================================================================
-- Creates Apache Iceberg V3 catalog using AWS Glue as metadata service
--
-- Required environment variables:
--   ICEBERG_WAREHOUSE: S3 path for Iceberg warehouse (e.g., s3://bucket/iceberg/)
--   GLUE_DATABASE: AWS Glue database name (e.g., flink_iceberg_db)
--   AWS_REGION: AWS region
--
-- Iceberg V3 Features (on top of V2):
--   - All V2 features: row-level deletes, equality deletes, sequence numbers
--   - Multi-argument transforms
--   - Default values for columns
--   - Nanosecond timestamp types (timestamp_ns, timestamptz_ns)
--   - Enhanced metadata statistics
--   - Data skipping with bloom filters and min/max indexes
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
