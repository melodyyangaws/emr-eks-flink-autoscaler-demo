-- ============================================================================
-- Sink Table — Iceberg format v2, APPEND-ONLY, partitioned by dt
-- ============================================================================
-- The same 34 columns in the same order as ../paimon/03-sink.sql, taken from
-- schema_sample.sql: 33 extracted columns + event_json, PARTITIONED BY (dt), no
-- primary key. Both formats are given one table shape so the only remaining
-- difference is the write path.
--
-- WHAT CHANGED FROM THE PREVIOUS REVISION, AND WHY IT CHANGES THE MEASUREMENT
-- ---------------------------------------------------------------------------
-- This table used to be `PRIMARY KEY (id) NOT ENFORCED` with
-- write.upsert.enabled = true. Every repeated key made the writer emit a
-- row-level delete, which landed as a positional-delete Parquet file, and
-- merge-on-read was the thing being measured.
--
-- schema_sample.sql is append-only, so upsert is off and Iceberg writes NO
-- delete files. Its merge-on-read penalty is gone from both the write and the
-- read side, and so is Paimon's key-resolving LSM merge. The earlier runs'
-- Paimon-vs-Iceberg gap was largely an upsert gap and does NOT carry over —
-- numbers from this table are comparable only with other append-mode runs.
-- write.upsert.enabled and the three merge-on-read mode properties are removed
-- below rather than set to false, because with no equality fields they have
-- nothing to apply to.
--
-- ----------------------------------------------------------------------------
-- PARTITIONING — THIS IS NOW EXPRESSIBLE, BUT READ THE DISTRIBUTION NOTE
-- ----------------------------------------------------------------------------
-- The previous revision was deliberately UNPARTITIONED and carried a long
-- argument for why. Two of its three reasons no longer apply, and one still
-- does:
--
--   * RESOLVED — DDL expressiveness. FlinkCatalog.toPartitionSpec maps every
--     PARTITIONED BY column through PartitionSpec$Builder.identity, so only
--     identity transforms are declarable from SQL. dt is a STRING already
--     bucketed to a day by the generator, so identity(dt) is exactly the spec
--     we want; nothing needs bucket(32, x).
--   * RESOLVED — upsert equality fields. appendWriter enforces "In UPSERT mode,
--     source column '%s' of partition field '%s', should be included in
--     equality fields". There is no UPSERT mode and no equality fields now, so
--     partitioning by dt costs nothing semantically.
--   * STILL LIVE — writer distribution. With a partition spec in place,
--     FlinkSink$Builder.distributeDataStream switches from
--     EqualityFieldKeySelector to PartitionKeySelector, whose getKey returns
--     PartitionKey.toPath() — a STRING like "dt=2026-09-21" — which Flink runs
--     through MathUtils.murmurHash into key groups. The generator stamps rows
--     with wall-clock UTC time, so a run has ONE distinct dt value (two across
--     midnight) and hash distribution would route EVERY row to ONE
--     subtask. That is not a skew tax, it is a total serialization of the
--     writer.
--
-- Hence 'write.distribution-mode' = 'none' below, not 'hash'. With none,
-- IcebergStreamWriter keeps each subtask's rows local and every subtask writes
-- into the same dt partition directory in parallel. This is the right choice
-- for an append table partitioned on a low-cardinality column, and it is also
-- the closest analog to what Paimon does: Paimon routes on
-- hash(bucket-key=uuid) % bucket, i.e. it distributes by a UNIQUE column, not
-- by the partition column. Iceberg has no bucket-key equivalent, so 'none'
-- (local, uniform by construction) is the nearest equivalent to Paimon's
-- uniform bucket fan-out. Setting 'hash' here would compare one serialized
-- Iceberg writer against 64 parallel Paimon writers.
--
-- Cost of 'none': each subtask produces its own files in the dt partition, so
-- the file count per commit is parallelism x (bytes / target-file-size) rather
-- than the theoretical minimum. That is the same cost Paimon pays per bucket,
-- and target-file-size is 256 MB on both sides, so it does not favour either.
--
-- The source S3 layout is seven levels deep
-- (x/year/month/day/hour/customerId/productCode); only dt is reproduced as a
-- partition, matching schema_sample.sql. The finer levels remain ordinary
-- columns (partition_cust, productCode) and are still available for pushdown.
-- Adding them to the spec would multiply open files per writer by the product
-- of their cardinalities — the small-file failure mode this benchmark is
-- specifically trying not to measure.
--
-- FAIR-COMPARISON CONTRACT with ../paimon/03-sink.sql, re-pointed at
-- schema_sample.sql's values:
--   snappy data compression             <-> file.compression = snappy
--   32 MiB parquet row groups           <-> parquet.block.size = 33554432
--   256 MiB target data file            <-> target-file-size = 256mb
--   5-snapshot retention floor          <-> snapshot.num-retained.min = 5
--   1 h snapshot age window             <-> snapshot.time-retained = 1h
--   8 MiB manifest targets + merging    <-> manifest.target-file-size / merge-min-count
--   write.metadata.previous-versions-max = 3
--                                       <-> metadata.iceberg.previous-versions-max
--   write.metadata.delete-after-commit.enabled
--                                       <-> metadata.iceberg.delete-after-commit.enabled
--   commit.manifest.min-count-to-merge = 5
--                                       <-> metadata.iceberg.compaction.min.file-num
--
-- The last three pair this table against Paimon's ICEBERG-COMPAT EXPORT tree,
-- not against Paimon's own metadata. They were unpaired until 2026-10-03: every
-- metadata.iceberg.* knob sat at a Paimon default that differed from the value
-- set here — most consequentially previous-versions-max, whose default is 0
-- (keep no old metadata.json at all) against the 3 set here. Paimon stores those
-- in its schema at create time, so the 2026-10-03 wipe is what lets the paired
-- values take effect on that side.
--
-- RETENTION IS PAIRED BY AGE. This table has no snapshot count ceiling to set;
-- the pairing was made on the Paimon side by lifting its
-- 'snapshot.num-retained.max' from 20 to 240 so that its 1 h age rule binds
-- first, as this table's does. Measured before the fix: 20 Paimon snapshots vs
-- 84 here, i.e. ~20 min of history against 1 h.
-- Row-group and target-file sizes moved from the previous revision's 16 MiB /
-- 128 MB on BOTH sides, so the comparison holds, but absolute file counts and
-- commit rates are not comparable with the earlier runs.
--
-- NOTE ON DATA-FILE COMPACTION. Iceberg's Flink sink never rewrites data files;
-- 'commit.manifest-merge.enabled' merges MANIFESTS only. Compacting this
-- table's data files requires an external Spark rewrite_data_files call — there
-- is no Iceberg-side equivalent of flink-cid-paimon-compact.yaml running in
-- this project. Factor that in before comparing file counts against Paimon's.
-- ============================================================================

CREATE TABLE IF NOT EXISTS cid_events (
    ingestionTime    TIMESTAMP(6),
    dt               STRING,
    partition_cust   STRING,
    customerId       STRING,
    uuid             STRING,
    productCode      STRING,
    -- TIMESTAMP(6) to stay byte-for-byte schema-identical with the Paimon side,
    -- which REQUIRES precision 4..9 once ICEBERG_COMPAT=true (Paimon's Iceberg
    -- exporter rejects TIMESTAMP(3) at commit time — see ../paimon/03-sink.sql).
    -- Iceberg's timestamp type is microsecond-precision natively, so this costs
    -- the Iceberg side nothing and changes no bytes it would otherwise write;
    -- the source value is millisecond ISO-8601, so no precision is invented.
    eventTimeDT      TIMESTAMP(6),
    eventTime        BIGINT,
    -- The whole raw JSON line, verbatim (the VARIANT-equivalent). ~95 of the
    -- source's 170 fields: whatever ../../jsongz-gen/cid_jsongz_gen.py wrote.
    event_json       STRING,
    filterRiskLevel  STRING,
    eventId          STRING,
    -- STRING per schema_sample.sql, though the source sends an int — see the
    -- MISMATCHES block at the bottom of ../paimon/03-sink.sql.
    eventSubId       STRING,
    endpointGuid     STRING,
    endpointHostName STRING,
    endpointIp       ARRAY<STRING>,
    hostName         STRING,
    pname            STRING,
    processName      STRING,
    processHashId    STRING,
    processFilePath  STRING,
    processCmd       STRING,
    request          STRING,
    objectHostName   STRING,
    objectHashId     STRING,
    objectFilePath   STRING,
    objectUser       STRING,
    objectCmd        ARRAY<STRING>,
    providerName     STRING,
    tags             ARRAY<STRING>,
    dst              ARRAY<STRING>,
    src              ARRAY<STRING>,
    dpt              INT,
    spt              INT,
    winEventId       INT
) PARTITIONED BY (dt) WITH (
    'format-version' = '2',
    -- 'none', NOT 'hash' — see the PARTITIONING note above. With one distinct
    -- dt value, hash on the partition key would serialize all writes into a
    -- single subtask.
    'write.distribution-mode' = 'none',
    -- Matches Paimon's 'target-file-size' = '256mb'.
    'write.target-file-size-bytes' = '268435456',
    -- snappy, matching Paimon's 'file.compression' = 'snappy'. The
    -- compression-level option is REMOVED, not set: snappy has no levels, and a
    -- stale level line would be ignored while reading as though it applied.
    'write.parquet.compression-codec' = 'snappy',
    -- Matches Paimon's 'parquet.block.size' = '33554432'.
    'write.parquet.row-group-size-bytes' = '33554432',
    'history.expire.max-snapshot-age-ms' = '3600000',
    'history.expire.min-snapshots-to-keep' = '5',
    -- Age (1 h) is the BINDING retention rule on both sides. Iceberg has no
    -- snapshot COUNT ceiling at all, so the pairing is made by moving Paimon's
    -- 'snapshot.num-retained.max' far enough out (20 -> 240 = 4 h of commits)
    -- that its age rule binds first, rather than by inventing a count cap here.
    -- Before that change Paimon expired at 20 snapshots (~20 min) while this
    -- table kept a full hour: measured 20 vs 84 snapshots on the 2026-10-03 run.
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '3',
    'commit.manifest.target-size-bytes' = '8388608',
    'commit.manifest-merge.enabled' = 'true',
    -- 5, not the Iceberg default of 100 — pairs with Paimon's
    -- 'manifest.merge-min-count' = '5'. Without this, Paimon rewrites its
    -- manifests every 5 commits while Iceberg lets 100 pile up: cheaper
    -- Iceberg commits (unfair write-side), slower Iceberg planning between
    -- merges (unfair read-side).
    'commit.manifest.min-count-to-merge' = '5',
    -- truncate(16), not 'none': the exact analog of Paimon's default stats
    -- mode. With 'none' StarRocks gets NO file-level min/max on the Iceberg
    -- side and cannot prune files, while Paimon keeps truncate(16) — that
    -- biased the READ benchmark toward Paimon for a pure config reason.
    -- It matters more now: event_json is a multi-KB STRING per row, and
    -- truncating its stats to 16 bytes is what keeps manifest size sane.
    'write.metadata.metrics.default' = 'truncate(16)'
);
