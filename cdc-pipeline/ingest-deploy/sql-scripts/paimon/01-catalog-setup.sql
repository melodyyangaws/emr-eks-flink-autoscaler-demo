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
--   GLUE_DATABASE: Glue database name (default: flink_paimon_db). The manifest
--     sets flink_paimon_db + CDC_DB_SUFFIX (e.g. flink_paimon_db_v3), so a
--     suffixed run creates and writes a separate, fresh set of tables.
-- ============================================================================

CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'lock.enabled' = 'false',
    'metastore' = 'hive',
    's3.region' = '${AWS_REGION}',
    's3.aws.credentials.provider' = 'software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider',
    -- S3A upload shape. Paimon's S3FileIO builds its S3AFileSystem from a bare
    -- Configuration (EMR's core-site.xml never applies), so these are passed as
    -- catalog options: S3FileIO maps 's3.*' to 'fs.s3a.*'. multipart.size is the
    -- block cut point (S3A floor 5M, clamped up silently); 'disk' buffering keeps
    -- queued blocks off the task heap. Iceberg's catalog gets the same shape
    -- (01-catalog-setup.sql there). Values: build-deploy-generic.sh.
    's3.multipart.size' = '${S3A_MULTIPART_SIZE:16M}',
    's3.multipart.threshold' = '${S3A_MULTIPART_THRESHOLD:16M}',
    's3.fast.upload.active.blocks' = '${S3A_ACTIVE_BLOCKS:6}',
    's3.fast.upload.buffer' = 'disk',
    's3.buffer.dir' = '/tmp/s3a-paimon',
    'hive-conf-dir' = '/glue/confs/hive/conf',
    'hadoop-conf-dir' = '/glue/confs/hadoop/conf',
    'warehouse' = '${PAIMON_WAREHOUSE}',
    'metastore.client.class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient'
);

-- Switch to Paimon catalog
USE CATALOG paimon_catalog;
-- Create Glue database if not exists
CREATE DATABASE IF NOT EXISTS ${GLUE_DATABASE:flink_paimon_db};