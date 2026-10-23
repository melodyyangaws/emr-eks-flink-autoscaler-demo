-- ============================================================================
-- Pipeline — the SQS-fed source view into the sink
-- ============================================================================
-- The source is a TEMPORARY table living in default_catalog (it is created
-- before the USE CATALOG switch), so it is referenced fully qualified here.
--
-- The column list is SELECT * in spirit: datagen_source projects exactly the 34
-- columns of 03-sink.sql in the same order (see
-- ../common/01-parse-s3json.sql stage 2), identical to
-- ../paimon/04-pipeline.sql. They are written out explicitly so a schema drift
-- on either side fails at plan time with a named column rather than silently
-- mis-assigning positionally.
--
-- No OPTIONS hint here. Iceberg's Flink sink takes its write configuration from
-- the table properties set in 03-sink.sql, and unlike Paimon there is no
-- writer-vs-compactor split that needs a job-scoped override: data-file
-- compaction for this table would be an external Spark rewrite_data_files call,
-- not a second Flink job sharing the table.
-- ============================================================================

INSERT INTO cid_events
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
