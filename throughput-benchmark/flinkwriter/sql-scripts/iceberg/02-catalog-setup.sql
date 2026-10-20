-- ============================================================================
-- Iceberg Catalog Setup (Glue-backed, format v2)
-- ============================================================================
-- Same Glue-backed catalog shape as cdc-pipeline/sql-scripts/iceberg/ and the
-- same emr-flink-cdc-paimon image, but a dedicated Glue database so benchmark
-- tables never mix with the CDC tables.
--
-- Required environment variables:
--   ICEBERG_WAREHOUSE: S3 path for the warehouse
--   GLUE_DATABASE: Glue database name (default flink_cid_iceberg_db)
--   AWS_REGION: AWS region
--   ICEBERG_S3_MULTIPART_SIZE_BYTES / ICEBERG_S3_MULTIPART_THREADS: upload
--     tuning, DERIVED by flinkwriter/run.sh from the Paimon job's
--     S3A_MULTIPART_SIZE / S3A_ACTIVE_BLOCKS — see the long note below for why
--     they are not the same keys.
-- ============================================================================

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    'warehouse' = '${ICEBERG_WAREHOUSE}',
    's3.region' = '${AWS_REGION}',
    -- UPLOAD TUNING — ICEBERG'S OWN KEYS, NOT HADOOP-AWS'S.
    -- The 'io-impl' line above is the whole reason the next three options look
    -- nothing like ../paimon/02-catalog-setup.sql's: S3FileIO drives the AWS
    -- SDK v2 S3 client itself and never constructs an S3AFileSystem, so not one
    -- fs.s3a.* key reaches this write path from anywhere — not this manifest's
    -- flinkConfiguration, not EMR's core-site.xml, not these catalog options.
    -- The s3:// warehouse and the s3p:// checkpoint directory are the visible
    -- tell: no s3a:// URI is in play on the Iceberg side at all. Paimon's knobs
    -- are read by hadoop-aws through its shaded S3FileIO; these are read by
    -- org.apache.iceberg.aws.s3.S3FileIOProperties. The two sets are
    -- EQUIVALENT IN EFFECT but differ in NAME and in UNIT, so they are not
    -- copy-pasteable in either direction.
    --
    -- part-size-bytes is Iceberg's block cut point — the same role Paimon's
    -- 's3.multipart.size' plays, but a long in BYTES rather than a Hadoop size
    -- string, so the 32 MiB both jobs now use is spelled 33554432 here.
    -- flinkwriter/run.sh converts S3A_MULTIPART_SIZE and passes the result in
    -- as ICEBERG_S3_MULTIPART_SIZE_BYTES, so the two sides cannot drift when
    -- that knob moves. 32 MiB is also S3FileIOProperties' own default: pinning
    -- it keeps the derivation honest, it does not change today's value.
    's3.multipart.part-size-bytes' = '${ICEBERG_S3_MULTIPART_SIZE_BYTES:33554432}',
    -- num-threads is the nearest analog of Paimon's
    -- 's3.fast.upload.active.blocks' = 6, NOT a copy of it: S3A caps queued
    -- blocks PER OUTPUT STREAM, while Iceberg has no per-stream cap and bounds
    -- in-flight parts with ONE shared pool per TaskManager JVM. The derived
    -- value is therefore blocks x slots (6 x 4 = 24), which puts both sides on
    -- the same per-node staging budget: 24 x 32 MiB x 4 TMs = 3 GiB/node,
    -- exactly the product flinkwriter/run.sh's disk guard checks for Paimon.
    's3.multipart.num-threads' = '${ICEBERG_S3_MULTIPART_THREADS:24}',
    -- Where those 3 GiB/node land. Iceberg's S3OutputStream ALWAYS stages parts
    -- to local files — there is no heap-buffer mode to pick, unlike Paimon's
    -- 's3.fast.upload.buffer' = 'disk' — and it mkdirs() this path when absent,
    -- so no volume or initContainer is needed. Pinned beside Paimon's
    -- '/tmp/s3a-paimon' instead of left at java.io.tmpdir so both jobs' upload
    -- buffers are findable on the 50 GiB root volume they share with the Paimon
    -- write-buffer spill files and the container logs.
    's3.staging-dir' = '/tmp/s3fileio-iceberg',
    -- NO threshold key here, deliberately. Iceberg's 's3.multipart.threshold'
    -- is a dimensionless FACTOR of the part size (default 1.5, validated >= 1),
    -- not a byte count, so handing it the Paimon job's S3A_MULTIPART_THRESHOLD
    -- of 32M would be read as a 33-million-fold multiplier and switch multipart
    -- upload off outright. Paimon's threshold has no byte-equivalent on this
    -- side; left at the default.
    'catalog-name' = 'iceberg_glue_catalog',
    'catalog-database' = '${GLUE_DATABASE:flink_cid_iceberg_db}',
    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog'
);

USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS ${GLUE_DATABASE:flink_cid_iceberg_db};
USE ${GLUE_DATABASE:flink_cid_iceberg_db};
