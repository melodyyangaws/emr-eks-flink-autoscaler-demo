-- ============================================================================
-- Sink Table — Paimon APPEND-ONLY table, partitioned by dt
-- ============================================================================
-- Schema, partitioning and the write properties below are taken from
-- schema_sample.sql (the CID ingestion PoC definition): 33 extracted columns +
-- event_json, PARTITIONED BY (dt), no primary key, bucketed on uuid.
-- ../iceberg/03-sink.sql declares the SAME 34 columns in the SAME order with
-- the SAME identity partitioning, so the two formats are given one table shape
-- and the only remaining difference is the write path itself.
--
-- ============================================================================

CREATE TABLE IF NOT EXISTS cid_events (
    ingestionTime    TIMESTAMP(6),
    dt               STRING,
    partition_cust   STRING,
    customerId       STRING,
    uuid             STRING,
    productCode      STRING,
    -- TIMESTAMP(6), not (3), and the precision is LOAD-BEARING when
    -- ICEBERG_COMPAT=true. Paimon's Iceberg metadata exporter rejects any
    -- precision outside 4..9 outright:
    --   java.lang.IllegalArgumentException: Paimon Iceberg compatibility only
    --   support timestamp type with precision from 4 to 9
    --     at IcebergDataField.toTypeObject(IcebergDataField.java:176)
    eventTimeDT      TIMESTAMP(6),
    eventTime        BIGINT,
    -- The whole raw JSON line, verbatim (the VARIANT-equivalent). ~95 of the
    -- source's 170 fields; see ../common/01-datagen-source.sql for which.
    event_json       STRING,
    filterRiskLevel  STRING,
    eventId          STRING,
    -- STRING per schema_sample.sql, though the source sends an int
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
    -- = the job parallelism, as PAIMON_BUCKETS from build-deploy-cid.sh: each
    -- bucket has exactly one writer, so fewer buckets than subtasks idles
    -- writers (JOB_PARALLELISM / 2 from 2026-10-06 to 2026-10-09 left 80 of 160
    -- with no rows). The default here is the script's default parallelism —
    -- a FALLBACK ONLY: build-deploy-cid.sh always ships PAIMON_BUCKETS as pod
    -- env, so this value is reached only if the JobManager env is missing.
    'bucket' = '${PAIMON_BUCKETS:128}',
    'bucket-key' = 'uuid',
    'file.format' = 'parquet',
    -- snappy, matching Iceberg's 'write.parquet.compression-codec' = 'snappy'.
    'file.compression' = 'snappy',
    -- 32 MiB row groups, matching Iceberg's
    -- 'write.parquet.row-group-size-bytes' = '33554432'.
    'parquet.block.size' = '33554432',
    -- Matches Iceberg's 'write.target-file-size-bytes' = '268435456'.
    'target-file-size' = '256mb',
    -- Spillable, and it matters more here than it did for the PK table:
    -- event_json is a single wide STRING column, so the write buffer fills on
    -- far fewer rows than the old 8-column layout did, and spilling is cheaper
    -- than stalling the writer.
    'write-buffer-size' = '128mb',
    'write-buffer-spillable' = 'true',
    'snapshot.num-retained.min' = '5',
    -- Iceberg expresses snapshot retention by AGE ONLY
    -- (history.expire.max-snapshot-age-ms); it has no count ceiling to pair a
    -- count ceiling with.
    -- 240 = 4 h at the observed ~60 s commit interval, i.e. 4x the 1 h age
    -- window, so 'snapshot.time-retained' is always the binding rule and the two
    -- sides agree on the MECHANISM (age) rather than disagreeing via a knob only
    -- one format has. It is kept finite deliberately to avoid unbounded growth of the snapshot tree,
    -- which is the only thing that can ever trigger a compaction on this table (no compactor ships with this project).
    'snapshot.num-retained.max' = '240',
    'snapshot.time-retained' = '1h',
    'snapshot.expire.execution-mode' = 'async',
    'metadata.stats-mode' = 'truncate(16)',
    'manifest.target-file-size' = '8mb',
    'manifest.merge-min-count' = '5',
    'compaction.min.file-num' = '5',
    'compaction.max.file-num' = '50',
    -- Iceberg-compatibility TOGGLE.
    'metadata.iceberg.storage' = '${PAIMON_ICEBERG_STORAGE:disabled}',
    'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient',
    'metadata.iceberg.hive-conf-dir' = '/glue/confs/hive/conf',
    'metadata.iceberg.format-version' = '2',
    'metadata.iceberg.manifest-compression' = 'snappy',
    -- ------------------------------------------------------------------------
    -- previous-versions-max: Paimon's default is 0 — keep ZERO old vN.metadata.json.
    -- Paimon rewrites the whole metadata.json every commit, so at 0 the previous
    -- version is deleted IMMEDIATELY and the retention window is ONE commit
    -- interval (~60 s). The native Iceberg side sets
    -- 'write.metadata.previous-versions-max' = '3'. Set to 3 to match.
    -- Confirmed empirically: the export tree held exactly ONE metadata.json
    -- (v85, 12808 B) against the native table's 4.
    'metadata.iceberg.previous-versions-max' = '3',
    -- Matches the native side's 'write.metadata.delete-after-commit.enabled' =
    -- 'true'. Paimon already defaults to true; stated explicitly so the pairing
    -- is visible and a future default flip cannot silently unpair it.
    'metadata.iceberg.delete-after-commit.enabled' = 'true',
    -- Export-manifest compaction. Paimon's default min.file-num is 10 while the
    -- native side sets 'commit.manifest.min-count-to-merge' = '5', so the export
    -- let twice as many small manifests accumulate before merging — cheaper
    -- commits on the Paimon side, slower planning between merges. 5/50 pairs the
    -- merge trigger with the native side and with this table's own
    -- 'compaction.min.file-num'/'compaction.max.file-num' above.
    'metadata.iceberg.compaction.min.file-num' = '5',
    'metadata.iceberg.compaction.max.file-num' = '50',
    'fs.s3.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'sink.writer-coordinator.enabled' = 'true'
);

-- ============================================================================
-- MISMATCHES between schema_sample.sql and the real sample extract
-- ============================================================================
-- Verified against all 189,013 records of cid_9011075d_sample_masked. The
-- columns above follow schema_sample.sql, so the generator bridges each gap;
-- recorded here because anyone comparing this table against the raw files will
-- hit all five.
--
-- 1. request, providerName, winEventId do not occur in the extract at all
--    (0 of 189,013 records). Kept as columns. request is emitted NULL;
--    providerName is populated from objectBmData's embedded "provider" key at
--    its real 1.9% rate; winEventId is gated to Windows rows (8.28%).
-- 2. objectCmd, dst, src are declared ARRAY<STRING> but are always scalar
--    STRINGs in the data (93,141 / 62,899 / 62,970 records, zero arrays). The
--    generator wraps each in a 1-element array for these columns while
--    event_json keeps the scalar form, so the raw line stays faithful.
--    endpointIp and tags ARE genuinely arrays and need no wrapping.
-- 3. eventSubId is declared STRING but is an int in the data (all 189,013
--    records). Carried as STRING here; event_json carries the int.
-- 4. eventTime is declared BIGINT but is a string-encoded epoch-millis in the
--    data ('1789948607072'), and eventTimeDT is declared TIMESTAMP(6) but is an
--    ISO-8601 string with MILLISECOND precision ('2026-09-20T23:56:47.072Z').
--    Both are parsed into their declared types for these columns and kept in
--    their original string form inside event_json. The declared (6) is wider
--    than the data's (3) because Paimon's Iceberg export requires 4..9; the
--    extra digits are always zero, so nothing is fabricated.
-- 5. ingestionTime, dt and partition_cust do not exist in the source at all —
--    they are derived. ingestionTime is wall-clock ingest time (so
--    ingestionTime - eventTimeDT is end-to-end latency), dt is the event date,
--    partition_cust is customerId.
--
-- Columns that are legitimately near-empty rather than mismatched:
-- filterRiskLevel 2.3%, tags 2.8%, hostName 0.1% (106 records),
-- objectHostName 3 records. Their null density is reproduced, not filled in.
-- ============================================================================
