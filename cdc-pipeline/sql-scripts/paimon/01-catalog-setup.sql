-- ============================================================================
-- Paimon Catalog Setup with EMR on EKS Glue Integration
-- ============================================================================
-- Creates Apache Paimon catalog using EMR's built-in Glue Data Catalog
-- integration. Requires 'aws.glue.enabled: "true"' in flinkConfiguration.
--
-- EMR provides pre-configured Hive/Hadoop conf dirs at /glue/confs/ that
-- route the Hive metastore client to AWS Glue automatically.
--
-- Required environment variables:
--   PAIMON_WAREHOUSE: S3 path for Paimon warehouse (e.g., s3://bucket/paimon/)
--   AWS_REGION: AWS region (e.g., us-west-2)
--   GLUE_DATABASE: Glue database name (default: flink_paimon_db)
-- ============================================================================

CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'lock.enabled' = 'false',
    'metastore' = 'hive',
    's3.region' = '${AWS_REGION}',
    'hive-conf-dir' = '/glue/confs/hive/conf',
    'hadoop-conf-dir' = '/glue/confs/hadoop/conf',
    'warehouse' = '${PAIMON_WAREHOUSE}',
    'metastore.client.class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient'
);

-- Switch to Paimon catalog
USE CATALOG paimon_catalog;
-- Create Glue database if not exists
CREATE DATABASE IF NOT EXISTS ${GLUE_DATABASE:flink_paimon_db};