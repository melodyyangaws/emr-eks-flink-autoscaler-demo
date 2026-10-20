-- ============================================================================
-- Parse stage — gzip NDJSON lines into the 34-column source view
-- ============================================================================
-- NOT a runnable file on its own. upload_scripts() in flinkwriter/run.sh
-- concatenates 01-raw-s3sqs.sql and THIS file into each format's 00-source.sql.
-- The raw fragment defines s3json_raw(line STRING), one gzip NDJSON line per
-- row; everything from here on reads only that. The text below is therefore
-- byte-identical between the Paimon and Iceberg jobs. Edit the parse here,
-- never in the raw fragment.
--
-- It defines the view datagen_source with the 34 columns of both sink tables,
-- in the same order and types, which ../paimon/04-pipeline.sql and
-- ../iceberg/04-pipeline.sql read. The name is historical (the jobs were once
-- fed by Flink's datagen connector) and kept so the pipelines need no edit.
--
-- THE SOURCE IS NOT FREE. Per row it does an S3 GET share, an inflate and a
-- JSON parse of the 4 KB line. If a run shows backpressure with idle sinks,
-- the read side — not the table format — is the wall. There is also no rate
-- knob: the job reads as fast as it can, so the rate is whatever the
-- generator (JSONGZ_REPLICAS x JSONGZ_ROWS_PER_POD) lands.
--
-- PARSE ONCE. Stage 1 uses Hive's json_tuple (HiveModule; the jars ship in
-- the EMR image's /opt/flink/lib), which parses the line ONCE and returns
-- every requested top-level key. The first version called JSON_VALUE /
-- JSON_QUERY ~30 times per row, and each call re-parsed the whole 4 KB line:
-- with 160 subtasks that Calc ran at 97% busy and capped each job at ~184K
-- rows/s while the writers sat ~80% idle (2026-10-08). json_tuple returns
-- STRINGs only: scalars as their text (numbers too, hence the CASTs), and
-- arrays/objects re-serialized as JSON text, which stage 2 parses again. Those
-- strings are a few dozen bytes, not 4 KB. An absent key or a JSON null gives
-- NULL, exactly as JSON_VALUE did.
-- ============================================================================

-- Core stays first in the module order, but that does NOT protect every
-- Flink function: those outside the core module (DATE_FORMAT, at least) lose
-- to Hive's same-named ones. Keep this file free of them; see dt below.
-- HiveModule's static init also needs commons-codec, which is not on the
-- image's classpath. Both manifests append it via HADOOP_CLASSPATH.
LOAD MODULE hive WITH ('hive-version' = '3.1.3');

-- Stage 1: one parse per row. The key list and the alias list are
-- positional and must stay the same length, in the same order.
CREATE TEMPORARY VIEW s3json_fields AS
SELECT
    r.line, j.*
FROM s3json_raw AS r,
LATERAL TABLE(json_tuple(r.line,
    'customerId', 'uuid', 'productCode', 'eventTimeDT', 'eventTime',
    'filterRiskLevel', 'eventId', 'eventSubId', 'endpointGuid',
    'endpointHostName', 'endpointIp', 'hostName', 'pname', 'processName',
    'processHashId', 'processFilePath', 'processCmd', 'objectHostName',
    'objectHashId', 'objectFilePath', 'objectUser', 'objectCmd',
    'objectBmData', 'tags', 'dst', 'src', 'dpt', 'spt', 'winEventId'
)) AS j(
    customerId, uuid, productCode, eventTimeDT_s, eventTime_s,
    filterRiskLevel, eventId, eventSubId, endpointGuid,
    endpointHostName, endpointIp_j, hostName, pname, processName,
    processHashId, processFilePath, processCmd, objectHostName,
    objectHashId, objectFilePath, objectUser, objectCmd_s,
    objectBmData_s, tags_j, dst_s, src_s, dpt_s, spt_s, winEventId_s
);

-- Stage 2: the public view, column-for-column with the sink tables.
-- eventTimeDT is parsed from the ISO string rather than converted from the
-- epoch millis, so no session time zone is involved; the source writes UTC 'Z'.
CREATE TEMPORARY VIEW datagen_source AS
SELECT
    CAST(LOCALTIMESTAMP AS TIMESTAMP(6))                    AS ingestionTime,
    -- NOT DATE_FORMAT: with the hive module loaded, Hive's date_format
    -- shadows Flink's (Flink's is not a core-module function) and throws on
    -- every row. json_tuple then swallows that error and emits an all-NULL
    -- row. The ISO string is already UTC, so its first 10 chars ARE the date.
    SUBSTRING(eventTimeDT_s, 1, 10)                         AS dt,
    customerId                                              AS partition_cust,
    customerId,
    uuid,
    productCode,
    CAST(eventTs AS TIMESTAMP(6))                           AS eventTimeDT,
    -- eventTime is an epoch-millis STRING in the source.
    TRY_CAST(eventTime_s AS BIGINT)                     AS eventTime,
    line                                                    AS event_json,
    filterRiskLevel,
    eventId,
    -- eventSubId is a JSON NUMBER in the source and a STRING column in the
    -- schema; json_tuple already returns its text form.
    eventSubId,
    endpointGuid,
    endpointHostName,
    -- A JSON array, re-serialized by json_tuple: a short string to re-parse.
    JSON_QUERY(endpointIp_j, '$' RETURNING ARRAY<STRING>)   AS endpointIp,
    hostName,
    pname,
    processName,
    processHashId,
    processFilePath,
    processCmd,
    -- Never present in the source; always NULL.
    CAST(NULL AS STRING)                                    AS request,
    objectHostName,
    objectHashId,
    objectFilePath,
    objectUser,
    -- Scalar in the data, ARRAY<STRING> in the schema: wrapped as a 1-element
    -- array. Likewise dst and src below.
    CASE WHEN objectCmd_s IS NOT NULL THEN ARRAY[objectCmd_s] END AS objectCmd,
    -- providerName is the "provider" key inside objectBmData, which is itself a
    -- JSON-encoded STRING (a short one) — hence the second, small parse.
    JSON_VALUE(objectBmData_s, '$.provider')                AS providerName,
    JSON_QUERY(tags_j, '$' RETURNING ARRAY<STRING>)         AS tags,
    CASE WHEN dst_s IS NOT NULL THEN ARRAY[dst_s] END       AS dst,
    CASE WHEN src_s IS NOT NULL THEN ARRAY[src_s] END       AS src,
    TRY_CAST(dpt_s AS INT)                              AS dpt,
    TRY_CAST(spt_s AS INT)                              AS spt,
    -- Not in the raw data (the sample extract has no winEventId field), so
    -- this reads NULL. The column is kept so the sinks see 34 columns.
    TRY_CAST(winEventId_s AS INT)                       AS winEventId
FROM (
    SELECT *,
        TO_TIMESTAMP(REPLACE(SUBSTRING(eventTimeDT_s, 1, 23), 'T', ' '),
                     'yyyy-MM-dd HH:mm:ss.SSS')             AS eventTs
    FROM s3json_fields
);
