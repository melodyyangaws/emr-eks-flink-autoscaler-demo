-- ============================================================================
-- Paimon Sink Tables — Streaming CDC Optimized (Paimon 2.1.0)
-- ============================================================================
-- Creates Apache Paimon sink tables optimized for low-latency CDC:
--   - Deletion vectors: bitmap-based row deletes without rewriting data files
--   - Deduplicate merge engine (default): last-write-wins upsert via PK
--   - ZSTD compression for the Parquet DATA files (better ratio on CDC payloads);
--     the Iceberg-compat Avro MANIFESTS use snappy instead, because StarRocks BE
--     has no zstd-jni on its Iceberg reader classpath — see the
--     metadata.iceberg.manifest-compression note on `customers` below
--   - Tuned compaction, write buffers, and snapshot retention for 60s checkpoints
--   - Iceberg compatibility: Paimon writes read-only Iceberg metadata (format
--     version ICEBERG_FORMAT_VERSION, 3 by default on this branch) for Iceberg
--     readers, and
--     registers it in Glue (metadata.iceberg.storage = hive-catalog)
--   - No changelog producer (changelog-producer = none): nothing reads one, and
--     Iceberg writes none, so it would be a Paimon-only write cost
--
-- Streaming best practices applied:
--   1. Spillable write buffers to prevent backpressure during burst writes
--   2. Target data files: 64 MB on customers/products, 128 MB on orders/order_items (same as Iceberg)
--   3. Tuned compaction triggers to balance write amp vs read performance
--   4. Manifest merging to keep read planning fast
--   5. Async snapshot expiration to avoid checkpoint latency spikes
--
-- ----------------------------------------------------------------------------
-- Iceberg format version of the exported metadata: v3 (default) vs v2
-- ----------------------------------------------------------------------------
-- ICEBERG_FORMAT_VERSION drives 'metadata.iceberg.format-version' here AND
-- 'format-version' on the native Iceberg tables (../iceberg/03-iceberg-sinks.sql),
-- so the Paimon export and the native Iceberg table are always the same version.
-- v3 is the default on this branch, and its default table set is a fresh
-- CDC_DB_SUFFIX=_v3 database, so nothing here has to migrate an existing table.
-- ICEBERG_FORMAT_VERSION=2 (+ PAIMON_DV_BITMAP64=false) gives the v2 setup.
--
--   v2: an Iceberg v2 reader cannot see Paimon deletion vectors, so for a
--       primary-key table the export lists ONLY files at the max LSM level
--       (IcebergCommitCallback.shouldAddFileToIceberg: level == num-levels - 1).
--       Rows still in lower levels are invisible to Iceberg readers until a full
--       compaction pushes them down; the native Paimon reader is unaffected.
--   v3: the export lists every file at level > 0 plus the deletion vectors as
--       Iceberg v3 DVs (Puffin). Paimon does this only when ALL THREE hold
--       (IcebergCommitCallback.needAddDvToIceberg):
--         'deletion-vectors.enabled'        = 'true'
--         'deletion-vectors.bitmap64'       = 'true'
--         'metadata.iceberg.format-version' = '3'
--       With DVs on, lookup-compact = radical (the default) compacts level 0
--       up at every checkpoint, so the v3 export trails the table by about one
--       checkpoint without any forced full compaction.
--
-- deletion-vectors.enabled is 'true' on every table regardless of version.
-- deletion-vectors.bitmap64 is PAIMON_DV_BITMAP64 (default 'true', matching the
-- v3 default), NOT hard-wired, because it is wrong for the v2 setup:
--   - It must be set at CREATE TABLE time (existing 32-bit DV index files are
--     not rewritten), so it cannot be flipped on later for the v3 case.
--   - It is NOT harmless for v2. StarRocks 4.1.x reads Paimon DVs in
--     its native C++ reader (PaimonScanNode passes the DeletionFile of every
--     raw-convertible split to BE), and BE's PaimonDeleteFileBuilder
--     (be/src/exec/paimon/paimon_delete_file_builder.cpp, branch-4.1) only knows
--     the 32-bit layout: big-endian magic 1581511376 + a 32-bit roaring bitmap.
--     The 64-bit layout (Bitmap64DeletionVector: little-endian magic
--     1681511377 + roaring64 + CRC) is not decoded there. The JNI reader
--     (Paimon 1.3.1 Java, DeletionVector.read) does handle both magics, so on
--     v3 tables StarRocks' paimon_catalog needs SET paimon_force_jni_reader =
--     true (untested). DuckDB and Iceberg readers of the v3 export need 64-bit.
--   So: PAIMON_DV_BITMAP64 = 'true' (the default) exactly when
--   ICEBERG_FORMAT_VERSION = 3 (the default); set it to 'false' whenever
--   ICEBERG_FORMAT_VERSION = 2. The SQL substitution cannot derive one from the
--   other, so the manifest/build script sets both together.
--
-- Valid metadata.iceberg.* options used here (Paimon 2.1.0 IcebergOptions):
--   storage, hive-client-class, hive-conf-dir, format-version (2 or 3),
--   manifest-compression
--
-- Table options are applied at CREATE TABLE only (CREATE TABLE IF NOT EXISTS
-- is a no-op for an existing table). The live flink_paimon_db tables have
-- drifted from this file (they are 'bucket' = '-1', and several sizes differ);
-- this DDL is deliberately NOT changed to match them. New tables — e.g. a
-- fresh CDC_DB_SUFFIX database — get exactly what is written here.
--
-- Required: 'aws.glue.enabled: "true"' in flinkConfiguration
-- Environment variables:
--   GLUE_DATABASE: Glue database name (default: flink_paimon_db; the manifest
--     sets flink_paimon_db + CDC_DB_SUFFIX)
--   ICEBERG_FORMAT_VERSION: 3 (default) or 2 — Iceberg export format version
--   PAIMON_DV_BITMAP64: true or false, derived by build-deploy-generic.sh from
--     the version (true iff 3); not a setting of its own
-- ============================================================================

-- Switch to Paimon catalog
USE CATALOG paimon_catalog;
USE ${GLUE_DATABASE:flink_paimon_db};

-- ============================================================================
-- Table: customers (Dimension Table — unpartitioned)
-- ============================================================================
-- Low cardinality, frequent updates (address/phone changes).
-- 32 buckets, like every table here. Smaller write buffer (128mb) than the fact
-- tables to save memory.
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    customer_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    zip_code VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    -- 8 buckets, not 32 (2026-11-04): at 32 the _v3r3 table held 1.71 GiB live in
    -- ~55 MiB buckets, each a writer, a DV index file and an LSM per checkpoint;
    -- 8 gives ~220 MiB buckets (Paimon's guidance: ~200 MiB-1 GiB). Fixed at CREATE
    -- TABLE: takes a new CDC_DB_SUFFIX. Iceberg has no buckets (hash distribution).
    'bucket' = '8',
    -- none, not lookup: the benchmark reads no changelog, and Iceberg writes none,
    -- so lookup's changelog files were a Paimon-only write cost. Deletion vectors
    -- still use lookup compaction (lookup-compact), independent of this option.
    'changelog-producer' = 'none',
    'deletion-vectors.enabled' = 'true',
    -- 64-bit DV index format (default on, for the v3 default) — required for the
    -- v3 DV export, breaks StarRocks' native Paimon DV reader; 'false' for v2.
    -- See the header. CREATE-time only.
    'deletion-vectors.bitmap64' = '${PAIMON_DV_BITMAP64:true}',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    'file.compression.zstd-level' = '1',
    -- Matches Iceberg's 'write.parquet.row-group-size-bytes' = '8388608'. Without
    -- this Paimon inherits parquet-mr's 128MB default block size and emits ONE row
    -- group for a whole data file (measured on a 32MB file: 1 x 84MB row group, vs Iceberg's
    -- 5 x 16MB). Row group is the unit of predicate pushdown and scan splitting, so
    -- a single one means the reader can neither skip within the file nor parallelize
    -- across it — that penalizes Paimon on the read benchmark for a reason that has
    -- nothing to do with the LSM-vs-merge-on-read question being measured.
    'parquet.block.size' = '8388608',
    -- 64 MB on the dimension tables (128 MB on orders/order_items), paired with
    -- Iceberg's write.target-file-size-bytes.
    'target-file-size' = '64mb',
    'write-buffer-size' = '64mb',
    'write-buffer-spillable' = 'true',
    'num-sorted-run.compaction-trigger' = '4',
    -- 'compaction.max.file-num' was removed from all four tables: no such option
    -- exists in Paimon 1.3.x or 2.1.0 CoreOptions (only compaction.min.file-num,
    -- for append tables), so Paimon stored it and ignored it. Dropping it changes
    -- no behaviour; it only stops implying a cap that never applied.
    -- Parity with the Iceberg tables (history.expire.*): retention by AGE, 12 h.
    -- max 1000 (> 720 = 12 h of 60 s commits) so the age rule binds, not the
    -- count; at 10 it expired after ~10 min, shorter than a hot pass's pin.
    'snapshot.time-retained' = '12h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '1000',
    -- Explicit Paimon default; pairs with Iceberg's write.metadata.metrics.default.
    'metadata.stats-mode' = 'truncate(16)',
    'snapshot.expire.execution-mode' = 'async',
    'manifest.target-file-size' = '8mb',
    'manifest.merge-min-count' = '5',
    -- Iceberg compatibility via EMR Glue conf
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    -- ICEBERG_FORMAT_VERSION (3 default on this branch, 2 opt-out) — the format
    -- version of the Iceberg-compatible metadata Paimon exports; always equal to
    -- the native Iceberg tables' 'format-version'. 3 exports DVs only together
    -- with PAIMON_DV_BITMAP64 = true (its default) — see the header. 2 is for
    -- Iceberg-v2-only readers (Athena engine v3) and needs PAIMON_DV_BITMAP64 =
    -- false.
    'metadata.iceberg.format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    -- 'snappy', not 'zstd'. This compresses the Avro MANIFEST files of the
    -- Iceberg-compatible metadata Paimon exports under <table>/iceberg/, and it
    -- has to be a codec the READER has on its classpath, not just the best
    -- ratio. StarRocks BE ships avro-1.12.0.jar and snappy-java in
    -- be/lib/iceberg-reader-lib/ but NO zstd-jni (hudi-reader-lib,
    -- kudu-reader-lib and odps-reader-lib each bundle it; the Iceberg one does
    -- not). Avro's ZstandardCodec class itself IS present, so the codec is
    -- selected and then dies on its missing native binding:
    --
    --   java.lang.NoClassDefFoundError: com/github/luben/zstd/ZstdInputStreamNoFinalizer
    --     at org.apache.avro.file.ZstandardCodec.decompress(ZstandardCodec.java:84)
    --     at org.apache.iceberg.avro.AvroIterable$AvroReuseIterator.hasNext(...)
    --
    -- Fixing it BE-side is not durable: /opt/starrocks/be/lib is a container
    -- layer (a copied zstd-jni jar is lost on restart, and the classloader is
    -- built at BE start so a live copy has no effect anyway), and
    -- /etc/starrocks/be/conf is a read-only ConfigMap mount. Writing a codec
    -- the reader already supports is the fix that survives a pod restart.
    -- Costs a little manifest size; manifests are tiny next to the data files.
    'metadata.iceberg.manifest-compression' = 'snappy',
    -- Export metadata retention, paired with the Iceberg tables'
    -- write.metadata.previous-versions-max = 3 / delete-after-commit. Paimon's
    -- default (0) deleted the previous metadata.json on every commit, so a path
    -- read from Glue 404'd within one checkpoint (dry run 2026-11-02).
    'metadata.iceberg.previous-versions-max' = '3',
    'metadata.iceberg.delete-after-commit.enabled' = 'true',
    -- Export manifest merging, paired with commit.manifest.min-count-to-merge = 5.
    'metadata.iceberg.compaction.min.file-num' = '5',
    'metadata.iceberg.compaction.max.file-num' = '50',
    'sink.writer-coordinator.enabled' = 'true'
);

-- ============================================================================
-- Table: products (Dimension Table — partitioned by category)
-- ============================================================================
-- Medium cardinality, frequent stock_quantity and price updates.
-- Partitioned by category for query pruning (WHERE category = 'Electronics').
CREATE TABLE IF NOT EXISTS products (
    product_id INT,
    product_name VARCHAR,
    category VARCHAR,
    price DECIMAL(10, 2),
    stock_quantity INT,
    description VARCHAR,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    PRIMARY KEY (category, product_id) NOT ENFORCED
) PARTITIONED BY (category) WITH (
    -- 4 buckets per category, not 32 (2026-11-04): 8 categories x 32 = 256 buckets of
    -- ~4 MiB live each on _v3r3 (1.10 GiB; ~44K small files on S3). 4 gives ~35 MiB
    -- buckets and still 32 bucket writers in all. New CDC_DB_SUFFIX to take effect.
    'bucket' = '4',
    -- none, not lookup: the benchmark reads no changelog, and Iceberg writes none,
    -- so lookup's changelog files were a Paimon-only write cost. Deletion vectors
    -- still use lookup compaction (lookup-compact), independent of this option.
    'changelog-producer' = 'none',
    'deletion-vectors.enabled' = 'true',
    -- 64-bit DV index format (default on, for the v3 default) — required for the
    -- v3 DV export, breaks StarRocks' native Paimon DV reader; 'false' for v2.
    -- See the header. CREATE-time only.
    'deletion-vectors.bitmap64' = '${PAIMON_DV_BITMAP64:true}',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    'file.compression.zstd-level' = '1',
    -- Matches Iceberg's 'write.parquet.row-group-size-bytes' = '8388608'. Without
    -- this Paimon inherits parquet-mr's 128MB default block size and emits ONE row
    -- group for a whole data file (measured on a 32MB file: 1 x 84MB row group, vs Iceberg's
    -- 5 x 16MB). Row group is the unit of predicate pushdown and scan splitting, so
    -- a single one means the reader can neither skip within the file nor parallelize
    -- across it — that penalizes Paimon on the read benchmark for a reason that has
    -- nothing to do with the LSM-vs-merge-on-read question being measured.
    'parquet.block.size' = '8388608',
    -- 64 MB on the dimension tables (128 MB on orders/order_items), paired with
    -- Iceberg's write.target-file-size-bytes.
    'target-file-size' = '64mb',
    'write-buffer-size' = '64mb',
    'write-buffer-spillable' = 'true',
    'num-sorted-run.compaction-trigger' = '4',
    -- Parity with the Iceberg tables (history.expire.*): retention by AGE, 12 h.
    -- max 1000 (> 720 = 12 h of 60 s commits) so the age rule binds, not the
    -- count; at 10 it expired after ~10 min, shorter than a hot pass's pin.
    'snapshot.time-retained' = '12h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '1000',
    -- Explicit Paimon default; pairs with Iceberg's write.metadata.metrics.default.
    'metadata.stats-mode' = 'truncate(16)',
    'snapshot.expire.execution-mode' = 'async',
    'manifest.target-file-size' = '8mb',
    'manifest.merge-min-count' = '5',
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    -- ICEBERG_FORMAT_VERSION — see the format-version note on `customers`.
    'metadata.iceberg.format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    -- snappy, not zstd — see the manifest-compression note on `customers`.
    'metadata.iceberg.manifest-compression' = 'snappy',
    -- Export metadata retention, paired with the Iceberg tables'
    -- write.metadata.previous-versions-max = 3 / delete-after-commit. Paimon's
    -- default (0) deleted the previous metadata.json on every commit, so a path
    -- read from Glue 404'd within one checkpoint (dry run 2026-11-02).
    'metadata.iceberg.previous-versions-max' = '3',
    'metadata.iceberg.delete-after-commit.enabled' = 'true',
    -- Export manifest merging, paired with commit.manifest.min-count-to-merge = 5.
    'metadata.iceberg.compaction.min.file-num' = '5',
    'metadata.iceberg.compaction.max.file-num' = '50',
    'sink.writer-coordinator.enabled' = 'true'
);

-- ============================================================================
-- Table: orders (Fact Table — partitioned by date)
-- ============================================================================
-- High write volume, mostly inserts + status updates.
-- 32 buckets, like every table here. Larger write buffer (256mb) for sustained
-- throughput.
CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    customer_id INT,
    order_date TIMESTAMP(6),
    order_date_str VARCHAR,
    total_amount DECIMAL(12, 2),
    order_status VARCHAR,
    payment_method VARCHAR,
    shipping_address VARCHAR,
    updated_at TIMESTAMP(6),
    PRIMARY KEY (order_date_str, order_id) NOT ENFORCED
) PARTITIONED BY (order_date_str) WITH (
    'bucket' = '32',
    -- none, not lookup: the benchmark reads no changelog, and Iceberg writes none,
    -- so lookup's changelog files were a Paimon-only write cost. Deletion vectors
    -- still use lookup compaction (lookup-compact), independent of this option.
    'changelog-producer' = 'none',
    'deletion-vectors.enabled' = 'true',
    -- 64-bit DV index format (default on, for the v3 default) — required for the
    -- v3 DV export, breaks StarRocks' native Paimon DV reader; 'false' for v2.
    -- See the header. CREATE-time only.
    'deletion-vectors.bitmap64' = '${PAIMON_DV_BITMAP64:true}',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    'file.compression.zstd-level' = '1',
    -- 16MB to match Iceberg's row-group-size on the fact tables — see the
    -- parquet.block.size note on `customers`.
    'parquet.block.size' = '16777216',
    -- 128 MB on the fact tables (64 MB on customers/products), paired with
    -- Iceberg's write.target-file-size-bytes.
    'target-file-size' = '128mb',
    'write-buffer-size' = '128mb',
    'write-buffer-spillable' = 'true',
    'num-sorted-run.compaction-trigger' = '4',
    -- Parity with the Iceberg tables (history.expire.*): retention by AGE, 12 h.
    -- max 1000 (> 720 = 12 h of 60 s commits) so the age rule binds, not the
    -- count; at 10 it expired after ~10 min, shorter than a hot pass's pin.
    'snapshot.time-retained' = '12h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '1000',
    -- Explicit Paimon default; pairs with Iceberg's write.metadata.metrics.default.
    'metadata.stats-mode' = 'truncate(16)',
    'snapshot.expire.execution-mode' = 'async',
    'manifest.target-file-size' = '8mb',
    'manifest.merge-min-count' = '5',
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    -- ICEBERG_FORMAT_VERSION — see the format-version note on `customers`.
    'metadata.iceberg.format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    -- snappy, not zstd — see the manifest-compression note on `customers`.
    'metadata.iceberg.manifest-compression' = 'snappy',
    -- Export metadata retention, paired with the Iceberg tables'
    -- write.metadata.previous-versions-max = 3 / delete-after-commit. Paimon's
    -- default (0) deleted the previous metadata.json on every commit, so a path
    -- read from Glue 404'd within one checkpoint (dry run 2026-11-02).
    'metadata.iceberg.previous-versions-max' = '3',
    'metadata.iceberg.delete-after-commit.enabled' = 'true',
    -- Export manifest merging, paired with commit.manifest.min-count-to-merge = 5.
    'metadata.iceberg.compaction.min.file-num' = '5',
    'metadata.iceberg.compaction.max.file-num' = '50',
    'sink.writer-coordinator.enabled' = 'true'
);

-- ============================================================================
-- Table: order_items (Fact Table — partitioned by date)
-- ============================================================================
-- Highest write volume (multiple items per order), append-heavy.
-- 4 buckets for parallelism. Largest write buffer.
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10, 2),
    subtotal DECIMAL(12, 2),
    created_at TIMESTAMP(6),
    created_date_str VARCHAR,
    PRIMARY KEY (created_date_str, order_item_id) NOT ENFORCED
) PARTITIONED BY (created_date_str) WITH (
    'bucket' = '32',
    -- none, not lookup: the benchmark reads no changelog, and Iceberg writes none,
    -- so lookup's changelog files were a Paimon-only write cost. Deletion vectors
    -- still use lookup compaction (lookup-compact), independent of this option.
    'changelog-producer' = 'none',
    'deletion-vectors.enabled' = 'true',
    -- 64-bit DV index format (default on, for the v3 default) — required for the
    -- v3 DV export, breaks StarRocks' native Paimon DV reader; 'false' for v2.
    -- See the header. CREATE-time only.
    'deletion-vectors.bitmap64' = '${PAIMON_DV_BITMAP64:true}',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    'file.compression.zstd-level' = '1',
    -- 16MB to match Iceberg's row-group-size on the fact tables — see the
    -- parquet.block.size note on `customers`.
    'parquet.block.size' = '16777216',
    -- 128 MB on the fact tables (64 MB on customers/products), paired with
    -- Iceberg's write.target-file-size-bytes.
    'target-file-size' = '128mb',
    'write-buffer-size' = '128mb',
    'write-buffer-spillable' = 'true',
    'num-sorted-run.compaction-trigger' = '4',
    -- Parity with the Iceberg tables (history.expire.*): retention by AGE, 12 h.
    -- max 1000 (> 720 = 12 h of 60 s commits) so the age rule binds, not the
    -- count; at 10 it expired after ~10 min, shorter than a hot pass's pin.
    'snapshot.time-retained' = '12h',
    'snapshot.num-retained.min' = '5',
    'snapshot.num-retained.max' = '1000',
    -- Explicit Paimon default; pairs with Iceberg's write.metadata.metrics.default.
    'metadata.stats-mode' = 'truncate(16)',
    'snapshot.expire.execution-mode' = 'async',
    'manifest.target-file-size' = '8mb',
    'manifest.merge-min-count' = '5',
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    -- ICEBERG_FORMAT_VERSION — see the format-version note on `customers`.
    'metadata.iceberg.format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    -- snappy, not zstd — see the manifest-compression note on `customers`.
    'metadata.iceberg.manifest-compression' = 'snappy',
    -- Export metadata retention, paired with the Iceberg tables'
    -- write.metadata.previous-versions-max = 3 / delete-after-commit. Paimon's
    -- default (0) deleted the previous metadata.json on every commit, so a path
    -- read from Glue 404'd within one checkpoint (dry run 2026-11-02).
    'metadata.iceberg.previous-versions-max' = '3',
    'metadata.iceberg.delete-after-commit.enabled' = 'true',
    -- Export manifest merging, paired with commit.manifest.min-count-to-merge = 5.
    'metadata.iceberg.compaction.min.file-num' = '5',
    'metadata.iceberg.compaction.max.file-num' = '50',
    'sink.writer-coordinator.enabled' = 'true'
);
