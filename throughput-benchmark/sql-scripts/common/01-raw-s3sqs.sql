-- ============================================================================
-- RAW fragment — S3 event notifications via SQS (the only source)
-- ============================================================================
-- upload_scripts() in build-deploy-cid.sh publishes THIS file followed by
-- 01-parse-s3json.sql as each format's 00-source.sql. It defines only
-- s3json_raw(line STRING); the json_tuple parse is in the parse fragment.
--
-- WHAT IT READS. The same .json.gz objects jsongz-gen/ lands, but it never
-- lists the bucket. Each PUT under cid-landing-jsongz-r4/*.json.gz fires an S3
-- event to SNS cid-landing-jsongz-events, which fans out to one queue per
-- format (<SQS_QUEUE_PREFIX>-paimon, <SQS_QUEUE_PREFIX>-iceberg), so the two
-- jobs each see every file. The connector long-polls its queue and turns each
-- event's object key into a split that it reads line by line; how and when it
-- deletes the message is the connector's business (sqs-delta-filename/).
-- Nothing is enumerated at job start, so a large backlog under the prefix
-- costs the JobMaster nothing.
--
-- THE QUEUE IS THE STATE. A message left over from an earlier run is a file
-- this run will read. Purge both queues between runs (README), and use
-- sqs-delta-filename/backfill/ to enqueue an existing prefix on purpose.
--
-- ADD JAR runs through the executor like any other statement (not SET, not
-- INSERT, so table_env.execute_sql). Flink downloads the jar to the JM,
-- adds it to the session classloader that resolves 'connector' below, and
-- ships it to the TaskManagers with the job graph. s3a:// because that is the
-- scheme the image's Flink FileSystem resolves (Hadoop S3A).
--
-- Knobs (env on the JobManager, threaded through by build-deploy-cid.sh):
--   SQS_SOURCE_JAR            s3a://<bucket>/flink/jars/sqs-delta-filename.jar
--   SQS_QUEUE_URL             per format, <SQS_QUEUE_PREFIX>-<fmt>
--   SQS_POLL_THREADS          default 8 — concurrent ReceiveMessage loops
--   SQS_MAX_PENDING_OPT       rendered from SQS_MAX_PENDING: a whole option
--                             line, or empty for the connector's default
--   SQS_VISIBILITY_TIMEOUT    default 15min — must exceed the checkpoint
--                             interval plus a file's read time, or a message
--                             reappears and its file is read twice
-- 'wait-time' and 'dedup-grace' are left at the connector's defaults.
-- ============================================================================

ADD JAR '${SQS_SOURCE_JAR}';

CREATE TEMPORARY TABLE s3json_raw (
    line STRING
) WITH (
    'connector'          = 'sqs-delta-filename',
    'queue-url'          = '${SQS_QUEUE_URL}',
    'region'             = '${AWS_REGION}',
    -- The events carry bucket + key; this is the scheme the connector puts in
    -- front of them to open the object through Flink's FileSystem.
    'path-scheme'        = 's3a',
    'key-suffix'         = '.json.gz',
    'poll-threads'       = '${SQS_POLL_THREADS:8}',
    ${SQS_MAX_PENDING_OPT}
    'visibility-timeout' = '${SQS_VISIBILITY_TIMEOUT:15min}'
);

