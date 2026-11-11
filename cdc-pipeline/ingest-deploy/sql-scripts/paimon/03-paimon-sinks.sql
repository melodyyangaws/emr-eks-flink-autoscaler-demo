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
-- SESSION_SUFFIX=_v3 database, so nothing here has to migrate an existing table.
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
-- Iceberg export ON again (2026-11-05; off 2026-11-04): Paimon writes
-- Iceberg-compatible metadata (v3: files at level > 0 plus DVs, see above) and
-- registers it in Glue. The benchmark reads Paimon only through it
-- (Paimon-as-iceberg; native readers removed 2026-11-09). With write-only = true nothing leaves
-- level 0 until a compaction, so the export lists no files until then.
--
-- Buckets (2026-11-04): FIXED, 8 on the dimension tables (customers; products
-- per category) and 16 per date partition on the fact tables (orders,
-- order_items): one bucket per writer subtask at parallelism 16, so the
-- current date partition, which takes nearly all new rows, uses every writer. Fixed, not dynamic ('bucket' = '-1'): DuckDB's paimon extension
-- (paimon-cpp) refuses dynamic-bucket primary-key tables ("do not support pk
-- table bucket=-1 in scan process", _v3r2). One writer per bucket. Iceberg gets the
-- same N and key as bucket(N, <id>) partitions (2026-11-09, 03-iceberg-sinks.sql).
-- Like every option here: new tables only.
-- bucket-key = the table's id column: the primary key minus the partition column,
-- which is Paimon's default anyway (a primary-key table's bucket key must be a
-- subset of its primary key); explicit so the high-cardinality choice is visible.
--
-- Required: 'aws.glue.enabled: "true"' in flinkConfiguration
-- Environment variables:
--   GLUE_DATABASE: Glue database name (default: flink_paimon_db; the manifest
--     sets flink_paimon_db + SESSION_SUFFIX)
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
    'bucket' = '8',
    'bucket-key' = 'customer_id',
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
    -- 32 MB on the dimension tables (64 MB on orders/order_items; 2026-11-04,
    -- was 64 / 128), paired with Iceberg's write.target-file-size-bytes.
    -- Write buffer = 2 x the target file size.
    'target-file-size' = '32mb',
    'write-buffer-size' = '64mb',
    -- false, as PAIMON_WRITE_BUFFER_SPILLABLE in .bench-env (the INSERT hint in
    -- 04-cdc-pipelines.sql sets the effective value; this is the table default).
    'write-buffer-spillable' = 'false',
    'num-sorted-run.compaction-trigger' = '8',
    -- Write-path tuning (2026-11-11, _v3): checkpoints no longer wait for the lookup
    -- compaction (lookup-wait; default true held every checkpoint 10-14 s on _v3bucket
    -- and backpressured the sources ~200 ms/s). gentle mode + max-interval 3 still
    -- forces L0 up every 3 checkpoints, bounding how far Paimon-as-iceberg (level > 0
    -- only) trails. stop-trigger 16 (default trigger + 3 = 11) avoids write stalls in
    -- the post-snapshot catch-up. L0 files are rewritten by the next compaction, so
    -- lz4 there; the levels readers see stay zstd-1.
    'lookup-wait' = 'false',
    'lookup-compact' = 'gentle',
    'lookup-compact.max-interval' = '3',
    'num-sorted-run.stop-trigger' = '16',
    'file.compression.per.level' = '0:lz4',
    -- Lookup cache of the deletion-vector lookup compaction: the local disk cache
    -- capped at 8gb (default unlimited) on the TaskManager volume. The in-memory
    -- cache stays at its 256mb default: 512mb per writer task (16 per TaskManager)
    -- ran a TaskManager out of heap on _v3r3 (2026-11-04).
    'lookup.cache-max-disk-size' = '8gb',
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
    -- Iceberg export (header: on again 2026-11-05)
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'metadata.iceberg.format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    'metadata.iceberg.manifest-compression' = 'snappy',
    'metadata.iceberg.previous-versions-max' = '100',
    'metadata.iceberg.delete-after-commit.enabled' = 'true',
    'metadata.iceberg.compaction.min.file-num' = '5',
    'metadata.iceberg.compaction.max.file-num' = '50',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
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
    'bucket' = '8',
    'bucket-key' = 'product_id',
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
    -- 32 MB on the dimension tables (64 MB on orders/order_items; 2026-11-04,
    -- was 64 / 128), paired with Iceberg's write.target-file-size-bytes.
    -- Write buffer = 2 x the target file size.
    'target-file-size' = '32mb',
    'write-buffer-size' = '64mb',
    -- false, as PAIMON_WRITE_BUFFER_SPILLABLE in .bench-env (the INSERT hint in
    -- 04-cdc-pipelines.sql sets the effective value; this is the table default).
    'write-buffer-spillable' = 'false',
    'num-sorted-run.compaction-trigger' = '8',
    -- Write-path tuning (2026-11-11, _v3): checkpoints no longer wait for the lookup
    -- compaction (lookup-wait; default true held every checkpoint 10-14 s on _v3bucket
    -- and backpressured the sources ~200 ms/s). gentle mode + max-interval 3 still
    -- forces L0 up every 3 checkpoints, bounding how far Paimon-as-iceberg (level > 0
    -- only) trails. stop-trigger 16 (default trigger + 3 = 11) avoids write stalls in
    -- the post-snapshot catch-up. L0 files are rewritten by the next compaction, so
    -- lz4 there; the levels readers see stay zstd-1.
    'lookup-wait' = 'false',
    'lookup-compact' = 'gentle',
    'lookup-compact.max-interval' = '3',
    'num-sorted-run.stop-trigger' = '16',
    'file.compression.per.level' = '0:lz4',
    -- Lookup cache of the deletion-vector lookup compaction: the local disk cache
    -- capped at 8gb (default unlimited) on the TaskManager volume. The in-memory
    -- cache stays at its 256mb default: 512mb per writer task (16 per TaskManager)
    -- ran a TaskManager out of heap on _v3r3 (2026-11-04).
    'lookup.cache-max-disk-size' = '8gb',
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
    -- Iceberg export (header: on again 2026-11-05)
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'metadata.iceberg.format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    'metadata.iceberg.manifest-compression' = 'snappy',
    'metadata.iceberg.previous-versions-max' = '100',
    'metadata.iceberg.delete-after-commit.enabled' = 'true',
    'metadata.iceberg.compaction.min.file-num' = '5',
    'metadata.iceberg.compaction.max.file-num' = '50',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    'sink.writer-coordinator.enabled' = 'true'
);

-- ============================================================================
-- Table: orders (Fact Table — partitioned by date)
-- ============================================================================
-- High write volume, mostly inserts + status updates.
-- Larger write buffer (128mb, = Iceberg's fact-table target file size).
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT,
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
    'bucket' = '16',
    'bucket-key' = 'order_id',
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
    -- 64 MB on the fact tables (32 MB on customers/products; 2026-11-04,
    -- was 128 / 64), paired with Iceberg's write.target-file-size-bytes.
    -- Write buffer = 2 x the target file size.
    'target-file-size' = '64mb',
    'write-buffer-size' = '128mb',
    -- false, as PAIMON_WRITE_BUFFER_SPILLABLE in .bench-env (the INSERT hint in
    -- 04-cdc-pipelines.sql sets the effective value; this is the table default).
    'write-buffer-spillable' = 'false',
    'num-sorted-run.compaction-trigger' = '8',
    -- Write-path tuning (2026-11-11, _v3): checkpoints no longer wait for the lookup
    -- compaction (lookup-wait; default true held every checkpoint 10-14 s on _v3bucket
    -- and backpressured the sources ~200 ms/s). gentle mode + max-interval 3 still
    -- forces L0 up every 3 checkpoints, bounding how far Paimon-as-iceberg (level > 0
    -- only) trails. stop-trigger 16 (default trigger + 3 = 11) avoids write stalls in
    -- the post-snapshot catch-up. L0 files are rewritten by the next compaction, so
    -- lz4 there; the levels readers see stay zstd-1.
    'lookup-wait' = 'false',
    'lookup-compact' = 'gentle',
    'lookup-compact.max-interval' = '3',
    'num-sorted-run.stop-trigger' = '16',
    'file.compression.per.level' = '0:lz4',
    -- Lookup cache of the deletion-vector lookup compaction: the local disk cache
    -- capped at 8gb (default unlimited) on the TaskManager volume. The in-memory
    -- cache stays at its 256mb default: 512mb per writer task (16 per TaskManager)
    -- ran a TaskManager out of heap on _v3r3 (2026-11-04).
    'lookup.cache-max-disk-size' = '8gb',
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
    -- Iceberg export (header: on again 2026-11-05)
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'metadata.iceberg.format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    'metadata.iceberg.manifest-compression' = 'snappy',
    'metadata.iceberg.previous-versions-max' = '100',
    'metadata.iceberg.delete-after-commit.enabled' = 'true',
    'metadata.iceberg.compaction.min.file-num' = '5',
    'metadata.iceberg.compaction.max.file-num' = '50',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    'sink.writer-coordinator.enabled' = 'true'
);

-- ============================================================================
-- Table: order_items (Fact Table — partitioned by date)
-- ============================================================================
-- Highest write volume (multiple items per order), append-heavy.
-- Write buffer 128mb, as orders.
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id BIGINT,
    order_id BIGINT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10, 2),
    subtotal DECIMAL(12, 2),
    created_at TIMESTAMP(6),
    created_date_str VARCHAR,
    PRIMARY KEY (created_date_str, order_item_id) NOT ENFORCED
) PARTITIONED BY (created_date_str) WITH (
    'bucket' = '16',
    'bucket-key' = 'order_item_id',
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
    -- 64 MB on the fact tables (32 MB on customers/products; 2026-11-04,
    -- was 128 / 64), paired with Iceberg's write.target-file-size-bytes.
    -- Write buffer = 2 x the target file size.
    'target-file-size' = '64mb',
    'write-buffer-size' = '128mb',
    -- false, as PAIMON_WRITE_BUFFER_SPILLABLE in .bench-env (the INSERT hint in
    -- 04-cdc-pipelines.sql sets the effective value; this is the table default).
    'write-buffer-spillable' = 'false',
    'num-sorted-run.compaction-trigger' = '8',
    -- Write-path tuning (2026-11-11, _v3): checkpoints no longer wait for the lookup
    -- compaction (lookup-wait; default true held every checkpoint 10-14 s on _v3bucket
    -- and backpressured the sources ~200 ms/s). gentle mode + max-interval 3 still
    -- forces L0 up every 3 checkpoints, bounding how far Paimon-as-iceberg (level > 0
    -- only) trails. stop-trigger 16 (default trigger + 3 = 11) avoids write stalls in
    -- the post-snapshot catch-up. L0 files are rewritten by the next compaction, so
    -- lz4 there; the levels readers see stay zstd-1.
    'lookup-wait' = 'false',
    'lookup-compact' = 'gentle',
    'lookup-compact.max-interval' = '3',
    'num-sorted-run.stop-trigger' = '16',
    'file.compression.per.level' = '0:lz4',
    -- Lookup cache of the deletion-vector lookup compaction: the local disk cache
    -- capped at 8gb (default unlimited) on the TaskManager volume. The in-memory
    -- cache stays at its 256mb default: 512mb per writer task (16 per TaskManager)
    -- ran a TaskManager out of heap on _v3r3 (2026-11-04).
    'lookup.cache-max-disk-size' = '8gb',
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
    -- Iceberg export (header: on again 2026-11-05)
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'metadata.iceberg.format-version' = '${ICEBERG_FORMAT_VERSION:3}',
    'metadata.iceberg.manifest-compression' = 'snappy',
    'metadata.iceberg.previous-versions-max' = '100',
    'metadata.iceberg.delete-after-commit.enabled' = 'true',
    'metadata.iceberg.compaction.min.file-num' = '5',
    'metadata.iceberg.compaction.max.file-num' = '50',
    'fs.s3.impl' ='org.apache.hadoop.fs.s3a.S3AFileSystem',
    'sink.writer-coordinator.enabled' = 'true'
);