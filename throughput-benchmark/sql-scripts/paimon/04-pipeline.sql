-- ============================================================================
-- Pipeline — datagen source into the sink
-- ============================================================================
-- The source is a TEMPORARY table living in default_catalog (it is created
-- before the USE CATALOG switch), so it is referenced fully qualified here.
--
-- The three write-path options below are applied as dynamic table option hints
-- rather than table properties because the sink DDL is CREATE TABLE IF NOT
-- EXISTS and Paimon bakes table properties into the schema at create time — a
-- property would need a table drop to change, while a hint takes effect against
-- the existing dataset. Requires table.dynamic-table-options.enabled=true
-- (flink-cid-executor.py sets it unconditionally). None of the three is in
-- Paimon's IMMUTABLE_OPTIONS: that set is built by reflection over the
-- @Immutable-annotated CoreOptions fields (verified with javap against
-- paimon-flink-1.20-1.3.x — 15 fields, none of these), so the hints are legal.
--
-- write-only=false and write-buffer-spillable=true are FIXED, and
-- build-deploy-cid.sh hardcodes them — the ${VAR:default} syntax here is just
-- the substitution mechanism, not an invitation to vary them. Paimon's own
-- defaults, and the only configuration this project measures; the exploratory
-- write-only=true / spillable=false arms, and the dedicated
-- flink-cid-paimon-compact job that write-only=true required, were removed on
-- 2026-10-05 and 2026-10-03 respectively. Iceberg's writer has no equivalent of
-- either option, so a non-default value on this side would make the two formats
-- structurally different write paths rather than a format comparison.
--
-- What they do: write-buffer-spillable=true means
-- BinaryExternalSortBuffer.write() spills a sorted run and RETRIES rather than
-- rejecting the record, so the buffer accumulates the whole checkpoint interval
-- and the barrier flush external-merges all of it. write-only=false leaves
-- inline compaction and snapshot expiry with the writer — on this APPEND-ONLY
-- table (no primary key) that is AppendOnlyFileStoreWrite coalescing small
-- files, not KeyValueFileStoreWrite merging sorted runs, so it costs far less
-- than it did in the upsert era.
--
-- PAIMON_WRITE_BUFFER_SIZE overrides the 'write-buffer-size' table property in
-- 03-sink.sql for THIS JOB only. Because the path is spillable, it only moves
-- bytes between RAM and spill files — the bytes the barrier external-merges are
-- set by arrival rate x checkpoint interval.
--
-- The column list below is SELECT * in spirit: datagen_source projects exactly
-- the 34 columns of 03-sink.sql in the same order (see
-- ../common/01-datagen-source.sql stage 4). They are written out explicitly so
-- a schema drift on either side fails at plan time with a named column rather
-- than silently mis-assigning positionally.
-- ============================================================================

INSERT INTO cid_events /*+ OPTIONS(
    'write-only' = '${PAIMON_WRITE_ONLY:false}',
    'write-buffer-spillable' = '${PAIMON_WRITE_BUFFER_SPILLABLE:true}',
    'write-buffer-size' = '${PAIMON_WRITE_BUFFER_SIZE:128mb}'
) */
SELECT
    ingestionTime,
    dt,
    partition_cust,
    customerId,
    uuid,
    productCode,
    eventTimeDT,
    eventTime,
    event_json,
    filterRiskLevel,
    eventId,
    eventSubId,
    endpointGuid,
    endpointHostName,
    endpointIp,
    hostName,
    pname,
    processName,
    processHashId,
    processFilePath,
    processCmd,
    request,
    objectHostName,
    objectHashId,
    objectFilePath,
    objectUser,
    objectCmd,
    providerName,
    tags,
    dst,
    src,
    dpt,
    spt,
    winEventId
FROM default_catalog.default_database.datagen_source;
