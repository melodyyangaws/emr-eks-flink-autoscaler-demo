-- ============================================================================
-- Paimon Catalog Setup (Glue-backed via EMR hive conf)
-- ============================================================================
-- Same catalog shape as cdc-pipeline/sql-scripts/paimon/: EMR's built-in Glue
-- Data Catalog integration through the Hive metastore client. Requires
-- 'aws.glue.enabled: "true"' in flinkConfiguration (the manifest sets it) —
-- that is what materializes /glue/confs/ inside the pods.
--
-- Required environment variables:
--   PAIMON_WAREHOUSE: S3 path for the warehouse
--   GLUE_DATABASE: Glue database name (default flink_cid_paimon_db)
--   AWS_REGION: AWS region
-- ============================================================================

CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'lock.enabled' = 'false',
    'metastore' = 'hive',
    's3.region' = '${AWS_REGION}',
    -- Rewritten by S3FileIO to fs.s3a.multipart.size. This is the ONE option
    -- that changes the write path: S3ABlockOutputStream cuts a block on
    -- blockSize alone and never reads any threshold, and close() does a single
    -- putObject() exactly when no block ever filled. It MUST stay below the
    -- smallest data file we expect or uploads silently fall back to that single
    -- PUT: at 32M a 60 MB file is 2 parts and a 118 MB file is 4 (at the earlier
    -- 16M default, 4 and 8), whereas hadoop-aws's own 64M left every ~60 MB file
    -- as 1 PUT. Floor is 5 MiB — S3AUtils.getMultipartSizeProperty() clamps
    -- anything smaller UP to 5242880 and only logs a warning, so a too-small
    -- value fails silently rather than loudly.
    --
    -- These are hadoop-aws keys and reach THIS catalog only. ../iceberg/
    -- 02-catalog-setup.sql tunes the same upload shape through Iceberg's own
    -- S3FileIO properties, in bytes rather than size strings; build-deploy-cid.sh
    -- derives those from these so the two cannot drift. Not interchangeable.
    's3.multipart.size' = '${S3A_MULTIPART_SIZE:32M}',
    's3.multipart.threshold' = '${S3A_MULTIPART_THRESHOLD:32M}',
    's3.fast.upload.active.blocks' = '${S3A_ACTIVE_BLOCKS:6}',
    's3.fast.upload.buffer' = 'disk',
    's3.buffer.dir' = '/tmp/s3a-paimon',
    'hive-conf-dir' = '/glue/confs/hive/conf',
    'hadoop-conf-dir' = '/glue/confs/hadoop/conf',
    'warehouse' = '${PAIMON_WAREHOUSE}',
    'metastore.client.class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient'
);

USE CATALOG paimon_catalog;
CREATE DATABASE IF NOT EXISTS ${GLUE_DATABASE:flink_cid_paimon_db};
USE ${GLUE_DATABASE:flink_cid_paimon_db};
