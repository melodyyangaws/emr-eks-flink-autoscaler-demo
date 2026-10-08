-- ============================================================================
-- Datagen Stream Source — CID security-telemetry mockup
-- ============================================================================
-- Reproduces the shape of the real source extract (cid_9011075d_sample_masked:
-- 100 x .json.gz, 189,013 NDJSON records, 170 distinct fields) and emits the
-- 33 extracted columns + event_json defined by schema_sample.sql, so Paimon and
-- Iceberg can be given one identical table definition.
--
-- Still the built-in datagen connector (in-memory, no Kafka/RDS) so the source
-- can never be the bottleneck. The measured variable is the sink write path.
--
-- MEASURED PROFILE OF THE REAL SAMPLE (all figures from its 189,013 records):
--   mean JSON line          3,817 B   (median 3,041, p95 8,625, max 111,445)
--   top byte contributors   processCmd 8.45%, processCmdNormalized 6.19%,
--                           parentCmd 3.59%, endpointMacAddress 3.01%,
--                           parentCmdNormalized 2.77%, objectCmd 2.75%,
--                           processFileHashSha256 2.30%, packageTraceId 2.09%
--   source S3 layout        x=NN/year=/month=/day=/hour=/customerId=/productCode=
--                           where x = first 2 hex chars of coupledTraceId
--
-- ----------------------------------------------------------------------------
-- ROW SIZE IS CALIBRATED TO 4KB MEAN — and that is a DELIBERATE DEVIATION
-- ----------------------------------------------------------------------------
-- The real sample's mean line is 3,817 B. The benchmark requires 4 KB/row, so
-- the five command fields are padded 1.233x their measured means:
--   processCmd 322 -> 295 B pad   processCmdNormalized 236 -> 221 B pad
--   parentCmd  137 -> 135 B pad   parentCmdNormalized   105 -> 117 B pad
--   objectCmd  105 ->  98 B pad
-- Measured by Monte-Carlo over this exact probability model (the gates, the
-- value ladders, the field widths reimplemented offline and sampled 60,000x,
-- because the SQL cannot report its own mean until it runs):
--   event_json mean 3,345 B, full-row mean 4,099 B (+0.07% of 4,096),
--   p50 4,163, p95 5,123, max 6,001
-- "Full row" is the 34 sink columns' unescaped byte sum, which is what the
-- Parquet writer is handed; event_json is 82% of it.
--
-- WHY THE PAD GOES ON THE COMMAND FIELDS and not a new filler column: they are
-- where the real bytes already are (the top 4 command fields are 21% of the
-- sample's bytes), they are variable-width and sparsely present, so widening
-- them preserves the null pattern and the compression behaviour. A dedicated
-- 280-B filler column would be dense, uniform-width and perfectly
-- dictionary-hostile — it would hit the byte target while destroying exactly
-- the properties this mockup exists to reproduce.
--
-- WHY THE SCALE IS UNIFORM: scaling all five by one factor preserves their
-- relative byte contributions, so the ranking of fields by size — which is what
-- column-chunk sizes and therefore row-group layout follow — is unchanged.
--
-- CONSEQUENCE: the p95 and max are wider than the sample's (5,123 vs 8,625 and
-- 6,001 vs 111,445). This model has no heavy tail: the real data's 111 KB
-- outliers come from a handful of pathological command lines, and reproducing
-- them would need a rare-event branch the benchmark does not have. So row size
-- here is tighter around its mean than reality. That is fine for a throughput
-- measurement (which is driven by the mean) and worth knowing for anything
-- that depends on the tail — e.g. per-row buffer high-water marks.
--
-- The previous synthetic row was 8 x 512 B random STRING = 4 KiB, so the MEAN
-- row size is unchanged from the earlier runs while the DATA now has real field
-- cardinalities, real skew and real null density. Note that the earlier figure
-- was a CONSTANT and this one is a mean over a distribution — equal means do
-- not make the two series interchangeable (the write path is also append-only
-- now; see ../paimon/03-sink.sql).
--
-- WHY THAT MATTERS: the old payload_0..7 columns were 8 incompressible blobs of
-- identical width. Parquet could neither dictionary-encode them nor exploit
-- nulls, so compressed size was a constant and column stats were useless. The
-- mockup makes compression, dictionary encoding, min/max pruning and null
-- counts behave the way they do on the real table — which is what the
-- downstream StarRocks/DuckDB read benchmarks actually exercise.
--
-- VALUE POOLS ARE THE REAL VALUES AT THE REAL FREQUENCIES. Every CASE ladder
-- below was generated from the sample's own distribution, e.g.
--   eventId        58.47% '1', 33.31% '3', 5.64% '2', 1.93% '14', ...
--   osVer          32.9% 'Amazon Linux 2', 17.7% 'Ubuntu 24.04', 13.2% 'CentOS 7'
--   cloudProvider  69.8% AWS / 30.1% Azure / 0.2% GCP (plus 11.7% empty)
--   processUser    72.6% root, then a long tail of user-xxxxxx
--   dpt            22.1% 443, 11.3% 53, 2.6% 80, 1.5% 8080, ...
-- Correlated groups are driven by ONE draw so they stay consistent: the
-- (eventId, eventSubId, eventCaseId) triple from r_event, and
-- (osName, osVer, osDescription, osType, pplat) from r_os. In the real data
-- these move together; independent draws would destroy the joint distribution
-- that predicate pushdown sees.
--
-- FIELD PRESENCE IS MODELLED, NOT UNIFORM. Most of the 170 fields are sparse:
-- objectFilePath 64.1%, objectUser 57.2%, the whole parent_* block 38.3%,
-- src/dst 33.3%, dpt 32.2%, processCmdNormalized 20.1%, tags 2.8%,
-- filterRiskLevel 2.3%. Sparsity is reproduced with per-BLOCK presence gates
-- (g_proc, g_parent, g_obj, g_net, g_rare, g_cloud) rather than one global
-- gate: in the real data a block appears or is absent as a unit (if there is no
-- parent process, none of the seven parent_* fields exist), but the blocks are
-- independent of each other. One shared gate would make every sparse field
-- present in exactly the same records — a null pattern Parquet compresses far
-- better than reality.
--
-- Within g_rare the ultra-sparse detection fields ARE nested rather than
-- independent (every row with tags also has filterRiskLevel, and so on). That
-- is deliberate: those fields co-occur in the real data too, because they are
-- all written by the same detection path.
--
-- ABSENT ON NULL on the JSON_OBJECT makes a gated-off scalar field genuinely
-- missing from the raw line, as in the source — not "k":null.
--
-- KNOWN DEVIATION — GATED-OFF ARRAYS RENDER AS [] RATHER THAN BEING ABSENT.
-- Flink inlines a nested JSON_ARRAY as real JSON only when the array call is
-- the DIRECT argument of the enclosing JSON_OBJECT; its codegen inspects the
-- operand's operator. Wrapping it in CASE WHEN ... THEN JSON_ARRAY(...) END
-- defeats that detection and the array would be emitted as an ESCAPED STRING
-- ("dstIsp":"[\"Amazon.com\"]"), which is visibly not what the source sends.
-- So the arrays are called unconditionally and their ELEMENTS are gated, which
-- yields "dstIsp":[] instead of no dstIsp key. Six fields are affected
-- (processSigner, processSignerValid, dstIsp, dstCountry, dstCity, tags);
-- worst case — a row where all six are gated off — adds ~85 B to a 3,817 B
-- line, i.e. 2.2%, and those bytes are highly compressible. logonUser and
-- userDomain are also gated this way but are 87.2% present, so the effect is
-- negligible. endpointIp and endpointMacAddress are 100% present and
-- unaffected.
--
-- IDENTITY COLUMNS use the source's exact formats so string widths and join
-- selectivity match:
--   uuid             8-4-4-4-12 hex, version nibble 7 (unique per record)
--   coupledTraceId   32 hex with a '00' prefix — the sample's x=00 shard
--   *HashId          signed 64-bit rendered as a decimal STRING
--   *FileHashSha256  64 hex / Sha1 40 hex / Md5 32 hex
--   endpointHostName 'host-' + 8 hex   (card 1,047 in the sample)
--   vpcId            'vpc-' + 8 hex    (card 133)
--
-- Knobs (env, threaded through the manifests by build-deploy-cid.sh):
--   SOURCE_ROWS_PER_SECOND  default 2000000 in build-deploy-cid.sh, which always
--                           ships it; the literal fallback below is only reached
--                           if the JobManager env is missing
--   HOT_KEY_FRACTION        default 0 (uniform; set 80 for 80/20)
--   CID_CUSTOMER_COUNT      default 16    — customerId fan-out
--   CID_HOST_COUNT          default 1024  — endpoint fan-out (sample: 1,047)
--   CID_DAY_SPREAD          default 1     — number of distinct dt partitions
--
-- WHAT HOT_KEY_FRACTION MEANS NOW. The target table is append-only (see
-- ../paimon/03-sink.sql), so there is no primary key for a key to be "hot" on
-- and skew no longer drives LSM key-merge work. The selector therefore routes
-- the ENDPOINT identity instead: HOT_KEY_FRACTION% of events are attributed to
-- a hot 5% slice of the endpoint space (CID_HOST_COUNT / 20), the rest spread
-- over all of it. That is the realistic shape — a few noisy hosts produce most
-- telemetry — and it still changes real work: the endpoint columns
-- dictionary-encode better when a few hosts dominate. HOT_KEY_FRACTION=0
-- (uniform) remains the default so runs stay comparable with the earlier series.
--
-- It does NOT change how rows spread over Paimon's bucket-key, which an earlier
-- version of this comment claimed: 'bucket-key' = 'uuid', and uuid is derived
-- from r_uuid, an independent full-range draw that no skew knob touches.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Raw datagen table. Every column is a cheap primitive; all shaping happens in
-- the views below.
--
-- Only ONE wide random STRING is drawn (cmd_pad), then sliced to pad the
-- command-line fields that dominate record size. Drawing a separate 512 B
-- string per command field would make the source CPU-bound on random-string
-- generation rather than on the sink, which would invalidate the benchmark.
-- The slices overlap; that only creates redundancy WITHIN a row, while Parquet
-- compresses per column chunk ACROSS rows, so the compression ratio is still
-- driven by genuine cross-row randomness.
--
-- The r_* columns are value-selection draws in [0,9999] so a frequency can be
-- written with 4 decimals directly in the CASE ladders (r_event < 5847 for
-- 58.47%). The g_* columns are independent presence gates on the same scale.
-- ----------------------------------------------------------------------------
CREATE TEMPORARY TABLE datagen_raw (
    -- The endpoint-identity draw. ONE full-range BIGINT, folded to the hot or
    -- the cold slice of the endpoint space by the MOD in cid_ids below.
    host_draw BIGINT,
    selector TINYINT,
    -- value-selection draws
    r_event  INT,
    r_os     INT,
    r_region INT,
    r_cloud  INT,
    r_risk   INT,
    r_ver    INT,
    r_tags   INT,
    r_tz     INT,
    r_proc   INT,
    r_obj    INT,
    r_user   INT,
    r_port   INT,
    r_net    INT,
    -- independent per-block presence gates
    g_proc   INT,
    g_parent INT,
    g_obj    INT,
    g_net    INT,
    g_rare   INT,
    g_cloud  INT,
    -- identity / entropy sources
    r_cust   INT,
    r_uuid   BIGINT,
    r_trace  BIGINT,
    r_phash  BIGINT,
    r_ohash  BIGINT,
    r_ehash  BIGINT,
    r_pid    INT,
    r_day    INT,
    cmd_pad  STRING,
    event_ts TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    -- The literal is a FALLBACK ONLY and is NOT the project's target rate:
    -- build-deploy-cid.sh always ships SOURCE_ROWS_PER_SECOND as pod env
    -- (default 2000000 ≈ 7.63 GiB/s, .bench-env 2097152 = 8 GiB/s), so this is
    -- reached only if the JobManager env is missing. 1572864 is the retired
    -- 6 GiB/s target, kept so a fallback run is recognisable as one.
    'rows-per-second'      = '${SOURCE_ROWS_PER_SECOND:1572864}',
    -- UNBOUNDED, always. This source had a DATAGEN_ROW_LIMIT hook that let the
    -- removed jsongen job reuse this file as a bounded generator; both it and
    -- burst mode were deleted on 2026-10-03, so there is no bounded mode left.
    -- Adding 'number-of-rows' here would make the benchmark jobs terminate.
    -- host_draw feeds the endpoint identity, not a primary key. Full range: the
    -- consumer MODs it down to the endpoint space, so any narrower bound would
    -- only add residue bias.
    'fields.host_draw.kind' = 'random',
    'fields.host_draw.min'  = '0',
    'fields.host_draw.max'  = '9223372036854775807',
    'fields.selector.kind' = 'random',
    'fields.selector.min'  = '0',
    'fields.selector.max'  = '99',
    'fields.r_event.kind'  = 'random',
    'fields.r_event.min'   = '0',
    'fields.r_event.max'   = '9999',
    'fields.r_os.kind'     = 'random',
    'fields.r_os.min'      = '0',
    'fields.r_os.max'      = '9999',
    'fields.r_region.kind' = 'random',
    'fields.r_region.min'  = '0',
    'fields.r_region.max'  = '9999',
    'fields.r_cloud.kind'  = 'random',
    'fields.r_cloud.min'   = '0',
    'fields.r_cloud.max'   = '9999',
    'fields.r_risk.kind'   = 'random',
    'fields.r_risk.min'    = '0',
    'fields.r_risk.max'    = '9999',
    'fields.r_ver.kind'    = 'random',
    'fields.r_ver.min'     = '0',
    'fields.r_ver.max'     = '9999',
    'fields.r_tags.kind'   = 'random',
    'fields.r_tags.min'    = '0',
    'fields.r_tags.max'    = '9999',
    'fields.r_tz.kind'     = 'random',
    'fields.r_tz.min'      = '0',
    'fields.r_tz.max'      = '9999',
    'fields.r_proc.kind'   = 'random',
    'fields.r_proc.min'    = '0',
    'fields.r_proc.max'    = '9999',
    'fields.r_obj.kind'    = 'random',
    'fields.r_obj.min'     = '0',
    'fields.r_obj.max'     = '9999',
    'fields.r_user.kind'   = 'random',
    'fields.r_user.min'    = '0',
    'fields.r_user.max'    = '9999',
    'fields.r_port.kind'   = 'random',
    'fields.r_port.min'    = '0',
    'fields.r_port.max'    = '9999',
    'fields.r_net.kind'    = 'random',
    'fields.r_net.min'     = '0',
    'fields.r_net.max'     = '9999',
    'fields.g_proc.kind'   = 'random',
    'fields.g_proc.min'    = '0',
    'fields.g_proc.max'    = '9999',
    'fields.g_parent.kind' = 'random',
    'fields.g_parent.min'  = '0',
    'fields.g_parent.max'  = '9999',
    'fields.g_obj.kind'    = 'random',
    'fields.g_obj.min'     = '0',
    'fields.g_obj.max'     = '9999',
    'fields.g_net.kind'    = 'random',
    'fields.g_net.min'     = '0',
    'fields.g_net.max'     = '9999',
    'fields.g_rare.kind'   = 'random',
    'fields.g_rare.min'    = '0',
    'fields.g_rare.max'    = '9999',
    'fields.g_cloud.kind'  = 'random',
    'fields.g_cloud.min'   = '0',
    'fields.g_cloud.max'   = '9999',
    'fields.r_cust.kind'   = 'random',
    'fields.r_cust.min'    = '0',
    'fields.r_cust.max'    = '${CID_CUSTOMER_COUNT:16}',
    'fields.r_uuid.kind'   = 'random',
    'fields.r_uuid.min'    = '0',
    'fields.r_uuid.max'    = '9223372036854775807',
    'fields.r_trace.kind'  = 'random',
    'fields.r_trace.min'   = '0',
    'fields.r_trace.max'   = '9223372036854775807',
    'fields.r_phash.kind'  = 'random',
    'fields.r_phash.min'   = '-9223372036854775808',
    'fields.r_phash.max'   = '9223372036854775807',
    'fields.r_ohash.kind'  = 'random',
    'fields.r_ohash.min'   = '-9223372036854775808',
    'fields.r_ohash.max'   = '9223372036854775807',
    'fields.r_ehash.kind'  = 'random',
    'fields.r_ehash.min'   = '-9223372036854775808',
    'fields.r_ehash.max'   = '9223372036854775807',
    'fields.r_pid.kind'    = 'random',
    'fields.r_pid.min'     = '1',
    'fields.r_pid.max'     = '4194304',
    'fields.r_day.kind'    = 'random',
    'fields.r_day.min'     = '0',
    'fields.r_day.max'     = '${CID_DAY_SPREAD:1}',
    -- Sliced below into the cmd fields. The real sample's command-text budget
    -- is processCmd 322 B + processCmdNormalized 236 B + parentCmd 137 B +
    -- parentCmdNormalized 105 B + objectCmd 105 B = 905 B, and the slice widths
    -- below are those figures scaled by 1.233 to hit the required 4 KB mean —
    -- see the ROW SIZE IS CALIBRATED TO 4096 B note in the header. 768 B of
    -- draw covers the widest slice (offset 200 + 98) with headroom; it is one
    -- draw rather than five so the source never becomes CPU-bound on random
    -- string generation.
    'fields.cmd_pad.length' = '768'
);

-- ----------------------------------------------------------------------------
-- Stage 1 — identities. Each digest is computed ONCE here and sliced later,
-- instead of being recomputed in every column that needs a slice of it.
--
-- host_idx is where HOT_KEY_FRACTION lands: hot rows fold into a 5% slice of
-- the endpoint space, cold rows into all of it. The integer division
-- CID_HOST_COUNT / 20 is the 5% slice; GREATEST keeps it >= 1 for tiny
-- CID_HOST_COUNT values.
-- ----------------------------------------------------------------------------
CREATE TEMPORARY VIEW cid_ids AS
SELECT
    r_event, r_os, r_region, r_cloud, r_risk, r_ver, r_tags, r_tz,
    r_proc, r_obj, r_user, r_port, r_net,
    g_proc, g_parent, g_obj, g_net, g_rare, g_cloud,
    r_pid, r_day, cmd_pad, event_ts,
    MD5(CAST(r_uuid  AS STRING))                           AS h_uuid,
    MD5(CAST(r_trace AS STRING))                           AS h_trace,
    MD5(CAST(r_cust  AS STRING))                           AS h_cust,
    MD5(CAST(
        CASE WHEN selector < CAST('${HOT_KEY_FRACTION:0}' AS TINYINT)
             THEN MOD(host_draw, GREATEST(CAST(1 AS BIGINT),
                                          CAST('${CID_HOST_COUNT:1024}' AS BIGINT) / 20))
             ELSE MOD(host_draw, CAST('${CID_HOST_COUNT:1024}' AS BIGINT))
        END AS STRING))                                    AS h_host,
    SHA256(CAST(r_phash AS STRING))                        AS h_p256,
    SHA256(CAST(r_ohash AS STRING))                        AS h_o256,
    SHA1(CAST(r_phash AS STRING))                          AS h_p1,
    SHA1(CAST(r_ohash AS STRING))                          AS h_o1,
    MD5(CAST(r_phash AS STRING))                           AS h_pmd5,
    MD5(CAST(r_ohash AS STRING))                           AS h_omd5,
    CAST(r_phash AS STRING)                                AS processHashId,
    CAST(r_ohash AS STRING)                                AS objectHashId_raw,
    CAST(r_ehash AS STRING)                                AS eventHashId,
    r_pid                                                  AS processPid
FROM datagen_raw;

-- ----------------------------------------------------------------------------
-- Stage 2 — dimension values. Every threshold is a real cumulative frequency
-- measured from the sample.
-- ----------------------------------------------------------------------------
CREATE TEMPORARY VIEW cid_dims AS
SELECT
    event_ts, cmd_pad, r_pid, r_day,
    g_proc, g_parent, g_obj, g_net, g_rare, g_cloud,
    r_net, r_port, r_event,
    h_uuid, h_trace, h_host, h_cust, h_p256, h_o256, h_p1, h_o1, h_pmd5, h_omd5,
    processHashId, objectHashId_raw, eventHashId, processPid,

    -- ---- identities in the source's exact formats ----
    SUBSTRING(h_uuid, 1, 8) || '-' || SUBSTRING(h_uuid, 9, 4) || '-7'
        || SUBSTRING(h_uuid, 14, 3) || '-' || SUBSTRING(h_uuid, 17, 4) || '-'
        || SUBSTRING(h_uuid, 21, 12)                        AS uuid,
    -- '00' prefix mirrors the sample's x=00 shard: in the real data x is the
    -- first two hex chars of coupledTraceId, and 100% of the extract is x=00.
    '00' || SUBSTRING(h_trace, 3, 30)                       AS coupledTraceId,
    SUBSTRING(h_cust, 1, 8) || '-' || SUBSTRING(h_cust, 9, 4) || '-4'
        || SUBSTRING(h_cust, 14, 3) || '-' || SUBSTRING(h_cust, 17, 4) || '-'
        || SUBSTRING(h_cust, 21, 12)                        AS customerId,
    SUBSTRING(h_host, 1, 8) || '-' || SUBSTRING(h_host, 9, 4) || '-4'
        || SUBSTRING(h_host, 14, 3) || '-' || SUBSTRING(h_host, 17, 4) || '-'
        || SUBSTRING(h_host, 21, 12)                        AS endpointGuid,
    SUBSTRING(h_trace, 1, 8) || '-' || SUBSTRING(h_trace, 9, 4) || '-4'
        || SUBSTRING(h_trace, 14, 3) || '-' || SUBSTRING(h_trace, 17, 4) || '-'
        || SUBSTRING(h_trace, 21, 12)                       AS tenantGuid,
    'host-' || SUBSTRING(h_host, 1, 8)                      AS endpointHostName,
    'vpc-'  || SUBSTRING(h_host, 5, 8)                      AS vpcId,

    -- ---- event classification. The triple is CORRELATED in the source, so
    -- all three come from the single r_event draw. ----
    CASE WHEN r_event < 5847 THEN '1'
         WHEN r_event < 9178 THEN '3'
         WHEN r_event < 9742 THEN '2'
         WHEN r_event < 9935 THEN '14'
         WHEN r_event < 9967 THEN '11'
         WHEN r_event < 9989 THEN '5'
         WHEN r_event < 9995 THEN '4'
         WHEN r_event < 9999 THEN '8'
         ELSE '19' END                                      AS eventId,
    CASE WHEN r_event < 5611 THEN '2'
         WHEN r_event < 7334 THEN '204'
         WHEN r_event < 8938 THEN '203'
         WHEN r_event < 9502 THEN '101'
         WHEN r_event < 9695 THEN '1101'
         WHEN r_event < 9834 THEN '4'
         WHEN r_event < 9931 THEN '3'
         WHEN r_event < 9963 THEN '901'
         WHEN r_event < 9985 THEN '402'
         WHEN r_event < 9991 THEN '301'
         WHEN r_event < 9995 THEN '703'
         WHEN r_event < 9999 THEN '202'
         ELSE '1704' END                                    AS eventSubId,
    CASE WHEN r_event < 5847 THEN 7
         WHEN r_event < 9178 THEN 3
         WHEN r_event < 9742 THEN 5
         WHEN r_event < 9935 THEN 14
         WHEN r_event < 9967 THEN 2
         WHEN r_event < 9989 THEN 8
         WHEN r_event < 9995 THEN 4
         WHEN r_event < 9999 THEN 9
         ELSE 19 END                                        AS eventCaseId,
    -- sds 99.996% / ddr 0.004%; pname tracks productCode exactly.
    CASE WHEN r_event < 9999 THEN 'sds' ELSE 'ddr' END      AS productCode,
    CASE WHEN r_event < 9999 THEN 'Server & Workload Protection'
         ELSE 'Data Detection and Response' END             AS pname,

    -- ---- agent version: 8 real values cover 66%, then a synthetic tail that
    -- keeps cardinality near the sample's 43. ----
    CASE WHEN r_ver < 2193 THEN '1.0.422'
         WHEN r_ver < 3589 THEN '1.0.413'
         WHEN r_ver < 4837 THEN '1.0.428'
         WHEN r_ver < 5269 THEN '1.0.402'
         WHEN r_ver < 5653 THEN '1.0.412'
         WHEN r_ver < 6022 THEN '1.0.376'
         WHEN r_ver < 6340 THEN '1.0.316'
         WHEN r_ver < 6654 THEN '1.0.416'
         ELSE '1.0.' || CAST(300 + MOD(r_ver, 35) AS STRING) END AS pver,

    -- ---- platform: 91.72% Linux / 8.28% Windows, with osVer, osDescription,
    -- osType and pplat all following the SAME r_os draw. ----
    CASE WHEN r_os < 9172 THEN 'Linux' ELSE 'Windows' END   AS osName,
    CASE WHEN r_os < 3293 THEN 'Amazon Linux 2'
         WHEN r_os < 5068 THEN 'Ubuntu 24.04'
         WHEN r_os < 6392 THEN 'CentOS 7'
         WHEN r_os < 7566 THEN '"Rocky Linux 9.6 (Blue Onyx)"'
         WHEN r_os < 8005 THEN 'Ubuntu 22.04'
         WHEN r_os < 8343 THEN 'Ubuntu Linux 16'
         WHEN r_os < 8615 THEN 'Ubuntu Linux 20'
         WHEN r_os < 8836 THEN 'Debian 9'
         WHEN r_os < 9009 THEN 'CentOS 6'
         WHEN r_os < 9172 THEN 'Amazon Linux 2023'
         WHEN r_os < 9692 THEN '10.0.14393'
         WHEN r_os < 9856 THEN '10.0.20348'
         ELSE '10.0.26100' END                              AS osVer,
    CASE WHEN r_os < 1379 THEN 'Ubuntu 24.04 (64 bit) (6.8.0-1067-azure)'
         WHEN r_os < 2699 THEN '"Rocky Linux 9.6 (Blue Onyx)" (64 bit) (5.14.0-570.17.1.el9_6.x86_64)'
         WHEN r_os < 3718 THEN 'Amazon Linux 2 (64 bit) (6.12.77-99.140.amzn2023.x86_64)'
         WHEN r_os < 4226 THEN 'Ubuntu 24.04 (64 bit) (6.8.0-1065-azure)'
         WHEN r_os < 4604 THEN 'CentOS 7 (64 bit) (3.10.0-1127.19.1.el7.x86_64)'
         WHEN r_os < 4913 THEN 'Amazon Linux 2 (64 bit) (6.12.68-92.122.amzn2023.x86_64)'
         WHEN r_os < 5169 THEN 'CentOS 7 (64 bit) (3.10.0-1160.66.1.el7.x86_64)'
         WHEN r_os < 5417 THEN 'Debian 9 (64 bit) (4.9.0-9-amd64)'
         WHEN r_os < 9172 THEN 'CentOS 7 (64 bit) (3.10.0-1160.119.1.el7.x86_64)'
         ELSE 'Microsoft Windows Server 2016 (64 bit) build 14393' END AS osDescription,
    CASE WHEN r_os < 9172 THEN 9217 ELSE 5889 END           AS pplat,
    -- osType is the empty string on Linux in the sample and numeric on Windows.
    CASE WHEN r_os < 9172 THEN ''
         WHEN r_os < 9981 THEN '8'
         WHEN r_os < 9992 THEN '4'
         ELSE '7' END                                       AS osType,
    CASE WHEN r_tz < 9555 THEN 'UTC+00:00'
         WHEN r_tz < 9781 THEN 'UTC+09:00'
         WHEN r_tz < 9891 THEN 'UTC+08:00'
         WHEN r_tz < 9944 THEN 'UTC-08:00'
         WHEN r_tz < 9963 THEN 'UTC+05:30'
         ELSE 'UTC-07:00' END                               AS timezone,

    -- ---- cloud placement. cloudProvider is the empty string for 11.7% of the
    -- sample; regionId/vpcId (50.6%) and instanceId (72.5%) are gated
    -- separately on g_cloud. ----
    CASE WHEN r_cloud < 6158 THEN 'AWS'
         WHEN r_cloud < 8812 THEN 'Azure'
         WHEN r_cloud < 9983 THEN ''
         ELSE 'GCP' END                                     AS cloudProvider,
    CASE WHEN r_region < 2399 THEN 'Europe (Frankfurt)'
         WHEN r_region < 4066 THEN 'US West (Oregon)'
         WHEN r_region < 5504 THEN 'Asia Pacific (Tokyo)'
         WHEN r_region < 6883 THEN 'US East (N. Virginia)'
         WHEN r_region < 7425 THEN 'Asia Pacific (Singapore)'
         WHEN r_region < 7892 THEN 'Asia Pacific (Mumbai)'
         WHEN r_region < 8321 THEN 'Europe (Ireland)'
         WHEN r_region < 8686 THEN 'Africa (Cape Town)'
         WHEN r_region < 8960 THEN 'US East (Ohio)'
         ELSE 'Asia Pacific (Sydney)' END                   AS regionId,

    -- ---- risk. Present in only 2.3% of records. ----
    CASE WHEN r_risk < 8369 THEN 'info'
         WHEN r_risk < 9826 THEN 'low'
         ELSE 'medium' END                                  AS filterRiskLevel,

    -- ---- process / object paths. The head of each distribution is the real
    -- value set; the tail is rebuilt from a hash slice so cardinality stays in
    -- the range the real data has (processName 831, objectFilePath 11,515). ----
    CASE WHEN r_proc < 1735 THEN '/usr/bin/bash'
         WHEN r_proc < 2310 THEN '/usr/bin/node-problem-detector'
         WHEN r_proc < 2727 THEN '/var/lang/bin/python3.12'
         WHEN r_proc < 3080 THEN '/var/lang/bin/python3.10'
         WHEN r_proc < 3432 THEN '/usr/bin/python2.7'
         WHEN r_proc < 3747 THEN 'c:\windows\system32\windowspowershell\v1.0\powershell.exe'
         WHEN r_proc < 4047 THEN '/runc'
         WHEN r_proc < 4334 THEN '/usr/sbin/runc'
         WHEN r_proc < 4606 THEN '/usr/bin/python3.9'
         WHEN r_proc < 4783 THEN '/usr/sbin/sshd'
         WHEN r_proc < 4948 THEN '/usr/bin/perl'
         WHEN r_proc < 5112 THEN '/hyperkube'
         ELSE '/usr/bin/' || SUBSTRING(h_p1, 1, 6) END      AS processName,
    CASE WHEN r_obj < 788  THEN '/usr/bin/bash'
         WHEN r_obj < 1286 THEN '/usr/bin/node-problem-detector'
         WHEN r_obj < 1688 THEN '/runc'
         WHEN r_obj < 2004 THEN '/usr/sbin/runc'
         WHEN r_obj < 2224 THEN '/usr/sbin/xtables-nft-multi'
         WHEN r_obj < 2439 THEN '/usr/bin/cat'
         WHEN r_obj < 2652 THEN '/usr/bin/dash'
         WHEN r_obj < 2861 THEN '/usr/sbin/sshd'
         WHEN r_obj < 3069 THEN '/usr/bin/grep'
         WHEN r_obj < 3265 THEN '/usr/bin/timeout'
         ELSE '/usr/bin/' || SUBSTRING(h_o1, 1, 6) END      AS objectFilePathVal,
    CASE WHEN r_user < 7264 THEN 'root'
         WHEN r_user < 7598 THEN 'SYSTEM'
         WHEN r_user < 7730 THEN 'www-data'
         ELSE 'user-' || SUBSTRING(h_p1, 3, 6) END          AS processUser,
    CASE WHEN r_user < 8665 THEN 'root'
         WHEN r_user < 8863 THEN 'www-data'
         WHEN r_user < 8951 THEN 'SYSTEM'
         ELSE 'user-' || SUBSTRING(h_o1, 3, 6) END          AS objectUserVal,

    -- ---- network ----
    '10.' || CAST(MOD(r_net,  256) AS STRING) || '.'
          || CAST(MOD(r_port, 256) AS STRING) || '.'
          || CAST(MOD(r_pid,  256) AS STRING)               AS src_ip,
    '10.' || CAST(MOD(r_port,  256) AS STRING) || '.'
          || CAST(MOD(r_net,   256) AS STRING) || '.'
          || CAST(MOD(r_event, 256) AS STRING)              AS dst_ip,
    CASE WHEN r_port < 2208 THEN 443
         WHEN r_port < 3340 THEN 53
         WHEN r_port < 3596 THEN 80
         WHEN r_port < 3743 THEN 8080
         WHEN r_port < 3878 THEN 9
         WHEN r_port < 4006 THEN 5666
         WHEN r_port < 4096 THEN 123
         WHEN r_port < 4176 THEN 1433
         ELSE 1024 + MOD(r_port * 7, 64000) END             AS dptVal,
    -- spt is 53 for 46.6% of the sample, otherwise ephemeral.
    CASE WHEN r_net < 4659 THEN 53
         ELSE 1024 + MOD(r_net * 13, 64000) END             AS sptVal,
    CASE WHEN r_net < 8227 THEN 2 ELSE 1 END                AS proto,

    -- ---- MITRE tag sets. Variable-length in the source, so the elements are
    -- carried as four nullable slots: unused slots are NULL and get dropped,
    -- which is how the variable length is reproduced. ----
    CASE WHEN r_tags < 1311 THEN 'MITRE.T1057'
         WHEN r_tags < 2509 THEN 'MITRE.T1124'
         WHEN r_tags < 3145 THEN 'MITRE.T1112'
         WHEN r_tags < 3701 THEN 'MITRE.T1560.001'
         WHEN r_tags < 4196 THEN 'MITRE.T1082'
         ELSE 'XSAE.F12793' END                             AS tag_1,
    CASE WHEN r_tags < 1311 THEN CAST(NULL AS STRING)
         WHEN r_tags < 2509 THEN 'XSAE.F2451'
         WHEN r_tags < 3145 THEN 'MITRE.T1543.003'
         WHEN r_tags < 3701 THEN 'XSAE.F2307'
         WHEN r_tags < 4196 THEN 'XSAE.F13030'
         ELSE 'MITRE.T1059.001' END                         AS tag_2,
    CASE WHEN r_tags >= 2509 AND r_tags < 3145 THEN 'XSAE.F17784'
         WHEN r_tags >= 3145 AND r_tags < 3701 THEN 'MITRE.T1560'
         ELSE CAST(NULL AS STRING) END                      AS tag_3,
    CASE WHEN r_tags >= 2509 AND r_tags < 3145 THEN 'MITRE.T1036.004'
         ELSE CAST(NULL AS STRING) END                      AS tag_4,

    -- ---- event time. CID_DAY_SPREAD fans the event time back over N days so
    -- dt (the partition key) takes N values; the sample itself is a single-hour
    -- extract, hence the default of 1. TIMESTAMPADD, not
    -- INTERVAL '1' DAY * r_day — multiplying an interval literal by a column
    -- is not portable across Flink planner versions. ----
    TIMESTAMPADD(DAY, -r_day, event_ts)                     AS eventTimeDT
FROM cid_ids;

-- ----------------------------------------------------------------------------
-- Stage 3 — derived strings that depend on stage-2 aliases: the command lines
-- (the dominant byte contributors) and the epoch-millis renderings. Computed
-- once here so event_json and the extracted columns share one value instead of
-- recomputing and diverging.
-- ----------------------------------------------------------------------------
CREATE TEMPORARY VIEW cid_rows AS
SELECT
    *,
    CAST(UNIX_TIMESTAMP(DATE_FORMAT(eventTimeDT, 'yyyy-MM-dd HH:mm:ss')) AS BIGINT) * 1000
        + MOD(CAST(processPid AS BIGINT), 1000)                        AS eventTimeMs,
    DATE_FORMAT(eventTimeDT, 'yyyy-MM-dd''T''HH:mm:ss.SSS') || 'Z'     AS eventTimeIso,
    DATE_FORMAT(eventTimeDT, 'yyyy-MM-dd')                             AS dt,
    -- The five command fields below carry the 4 KB calibration. Each slice
    -- width is the sample's measured mean for that field x 1.233; the presence
    -- GATES are untouched, so what changes is per-field width, not which rows
    -- have the field. See the header note for why the scale factor is uniform.
    --
    -- processCmd: present 92.5%, sample mean 322 B -> 295 B of pad. The single
    -- largest field, so it absorbs most of the calibration in absolute terms.
    CASE WHEN g_proc < 9250 THEN
        processName || ' -c3 nice -n 19 du -s /var/lib/docker/overlay2/'
        || SUBSTRING(cmd_pad, 1, 295) || '/diff' END                   AS processCmd,
    -- processCmdNormalized: present 20.1%, sample mean 236 B -> 221 B of pad.
    -- The long options are built with ' -' || '-' because the executor's comment
    -- stripper would truncate this line at a literal double dash.
    CASE WHEN g_proc < 2010 THEN
        processName || ' -' || '-node-ip=' || src_ip
        || ' -' || '-config=/etc/kubernetes/' || SUBSTRING(cmd_pad, 40, 221) END
                                                                       AS processCmdNormalized,
    -- parentCmd: present 35.2%, sample mean 137 B -> 135 B of pad.
    CASE WHEN g_parent < 3520 THEN
        '/bin/sh -c bash -c cd /data/ && ' || SUBSTRING(cmd_pad, 90, 135) END AS parentCmd,
    -- parentCmdNormalized: present 8.8%, sample mean 105 B -> 117 B of pad.
    CASE WHEN g_parent < 880 THEN
        '/bin/sh -c ' || SUBSTRING(cmd_pad, 150, 117) END              AS parentCmdNormalized,
    -- objectCmd: present 49.3%, sample mean 105 B -> 98 B of pad. Scalar STRING
    -- in the source.
    CASE WHEN g_obj < 4930 THEN
        'nice -n 19 du -s /var/lib/docker/overlay2/' || SUBSTRING(cmd_pad, 200, 98) END AS objectCmd,
    CASE WHEN g_obj < 670 THEN
        'ps -o %cpu %mem ' || CAST(processPid AS STRING) END           AS objectCmdNormalized
FROM cid_dims;

-- ----------------------------------------------------------------------------
-- Stage 4 — the public view.
--
-- The column list and types are exactly schema_sample.sql's 33 columns +
-- event_json, in the same order, so ../paimon/04-pipeline.sql and
-- ../iceberg/04-pipeline.sql are a straight projection and both sinks receive
-- an identical row.
--
-- event_json carries 95 of the sample's 170 fields — every field that is 100%
-- present plus every field in the measured top-45 by byte contribution, i.e.
-- the fields that determine record size, compression and column statistics.
-- The omitted 75 are the sub-1%-presence registry / posix-permission / raw-data
-- tail, which together account for under 1% of bytes.
-- ----------------------------------------------------------------------------
CREATE TEMPORARY VIEW datagen_source AS
SELECT
    -- Wall-clock ingest time, NOT event time: this is what makes
    -- ingestionTime - eventTimeDT a usable end-to-end latency measure.
    CAST(LOCALTIMESTAMP AS TIMESTAMP(6))                    AS ingestionTime,
    dt,
    -- partition_cust mirrors the source S3 layout's customerId= level. It is a
    -- separate column from customerId because schema_sample.sql declares both,
    -- and because it is the column a second partition level would use if
    -- PARTITIONED BY is ever widened to (dt, partition_cust).
    customerId                                              AS partition_cust,
    customerId,
    uuid,
    productCode,
    -- CAST to (6) explicitly, mirroring ingestionTime above. event_ts is
    -- declared TIMESTAMP(3) in the datagen connector (the source data really is
    -- millisecond-precision ISO-8601), but BOTH sinks declare eventTimeDT as
    -- TIMESTAMP(6) because Paimon's Iceberg-compat export rejects any precision
    -- outside 4..9 at commit time. Flink would widen this implicitly on INSERT;
    -- the explicit cast makes the sinks' requirement visible at the source, so
    -- the two cannot drift apart silently. jsongen is unaffected: it renders
    -- this column through DATE_FORMAT(..., 'SSS'), which is precision-agnostic.
    CAST(eventTimeDT AS TIMESTAMP(6))                       AS eventTimeDT,
    eventTimeMs                                             AS eventTime,

    -- ---- event_json: the whole raw line ----
    -- ABSENT ON NULL reproduces genuine field absence rather than "k":null.
    -- Nested JSON_ARRAY calls are inlined as real JSON arrays (not re-escaped)
    -- because they are direct arguments here — see the KNOWN DEVIATION note in
    -- the header for why they are not wrapped in CASE.
    JSON_OBJECT(
        -- identity / envelope (all 100% present in the sample)
        'uuid'                  VALUE uuid,
        'customerId'            VALUE customerId,
        'productCode'           VALUE productCode,
        'pname'                 VALUE pname,
        'pver'                  VALUE pver,
        'plang'                 VALUE 1,
        'pplat'                 VALUE pplat,
        'version'               VALUE '1.0',
        'eventId'               VALUE eventId,
        'eventSubId'            VALUE CAST(eventSubId AS INT),
        'eventCaseId'           VALUE eventCaseId,
        'eventSourceType'       VALUE 1,
        'eventHashId'           VALUE eventHashId,
        'coupledTraceId'        VALUE coupledTraceId,
        'partitionKey'          VALUE tenantGuid,
        'tenantGuid'            VALUE tenantGuid,
        'mgmtInstanceId'        VALUE tenantGuid,
        'groupId'               VALUE endpointGuid,
        'protectionManager'     VALUE 'PM-' || SUBSTRING(h_pmd5, 1, 6),
        'fastTrack'             VALUE FALSE,
        'dataReductionRuntime'  VALUE 'java',
        'filterMatchingRuntime' VALUE 'java',
        'engType'               VALUE '',
        'productAgent'          VALUE '',
        'packagePath'           VALUE '',
        -- timestamps: the source carries epoch-millis as STRINGS alongside ISO
        -- DT twins, and both forms are kept so the raw line is faithful.
        'eventTime'             VALUE CAST(eventTimeMs AS STRING),
        'eventTimeDT'           VALUE eventTimeIso,
        'firstSeen'             VALUE CAST(eventTimeMs AS STRING),
        'firstSeenDT'           VALUE eventTimeIso,
        'lastSeen'              VALUE CAST(eventTimeMs AS STRING),
        'lastSeenDT'            VALUE eventTimeIso,
        'receivedTime'          VALUE CAST(eventTimeMs + 274000 AS STRING),
        'logReceivedTime'       VALUE CAST(eventTimeMs + 274000 AS STRING),
        'logReceivedTimeDT'     VALUE DATE_FORMAT(TIMESTAMPADD(SECOND, 274, eventTimeDT),
                                                  'yyyy-MM-dd''T''HH:mm:ss.SSS') || 'Z',
        'timezone'              VALUE timezone,
        -- packageTraceId is 2.09% of bytes on its own (mean 79.9 B).
        'packageTraceId'        VALUE endpointGuid || '-'
                                      || DATE_FORMAT(eventTimeDT, 'yyyy-MM-dd_HH-mm-ss-SSS'),
        -- platform
        'osName'                VALUE osName,
        'osVer'                 VALUE osVer,
        'osType'                VALUE osType,
        'osDescription'         VALUE osDescription,
        -- endpoint
        'endpointGuid'          VALUE endpointGuid,
        'endpointHostName'      VALUE endpointHostName,
        'endpointIp'            VALUE JSON_ARRAY(src_ip, dst_ip ABSENT ON NULL),
        -- endpointMacAddress is 3.01% of bytes (mean 114.9 B) despite being a
        -- MAC list — the sample's entries are long. Two slots reproduce it.
        'endpointMacAddress'    VALUE JSON_ARRAY(SUBSTRING(h_host, 1, 12),
                                                 SUBSTRING(h_host, 13, 12) ABSENT ON NULL),
        'hostName'              VALUE CASE WHEN g_rare < 6 THEN
                                     'host-' || SUBSTRING(h_omd5, 1, 8) END,
        -- cloud
        'cloudProvider'         VALUE cloudProvider,
        'regionId'              VALUE CASE WHEN g_cloud < 5060 THEN regionId END,
        'vpcId'                 VALUE CASE WHEN g_cloud < 5060 THEN vpcId END,
        'instanceId'            VALUE CASE WHEN g_cloud < 7250 THEN
                                     'i-' || SUBSTRING(h_host, 1, 17) END,
        -- process block
        'processName'           VALUE processName,
        'processFilePath'       VALUE processName,
        'processHashId'         VALUE processHashId,
        'processFileHashId'     VALUE processHashId,
        'processPid'            VALUE processPid,
        'processLaunchTime'     VALUE CAST(eventTimeMs - 584 AS STRING),
        'processLaunchTimeDT'   VALUE eventTimeIso,
        'processUser'           VALUE CASE WHEN g_proc < 9120 THEN processUser END,
        'processUserDomain'     VALUE CASE WHEN g_proc < 830 THEN
                                     'ad-' || SUBSTRING(h_pmd5, 2, 4) END,
        'processFileHashSha256' VALUE CASE WHEN g_proc < 9630 THEN h_p256 END,
        'processFileHashSha1'   VALUE CASE WHEN g_proc < 9630 THEN h_p1 END,
        'processFileHashMd5'    VALUE CASE WHEN g_proc < 9630 THEN h_pmd5 END,
        'processCmd'            VALUE processCmd,
        'processCmdNormalized'  VALUE processCmdNormalized,
        'processSigner'         VALUE JSON_ARRAY(
                                     CASE WHEN g_proc < 750 THEN
                                         'Microsoft Windows Publisher' END ABSENT ON NULL),
        'processSignerValid'    VALUE JSON_ARRAY(
                                     CASE WHEN g_proc < 750 THEN TRUE END ABSENT ON NULL),
        'logonUser'             VALUE JSON_ARRAY(
                                     CASE WHEN g_proc < 8720 THEN processUser END ABSENT ON NULL),
        'userDomain'            VALUE JSON_ARRAY(
                                     CASE WHEN g_proc < 8720 THEN '' END ABSENT ON NULL),
        -- parent block: 38.3% present, and present as a UNIT.
        'parentHashId'          VALUE CASE WHEN g_parent < 3830 THEN processHashId END,
        'parentFileHashId'      VALUE CASE WHEN g_parent < 3830 THEN processHashId END,
        'parentName'            VALUE CASE WHEN g_parent < 3830 THEN processName END,
        'parentFilePath'        VALUE CASE WHEN g_parent < 3830 THEN processName END,
        'parentPid'             VALUE CASE WHEN g_parent < 3830 THEN processPid END,
        'parentUser'            VALUE CASE WHEN g_parent < 3820 THEN processUser END,
        'parentLaunchTime'      VALUE CASE WHEN g_parent < 3830 THEN
                                     CAST(eventTimeMs - 1584 AS STRING) END,
        'parentLaunchTimeDT'    VALUE CASE WHEN g_parent < 3830 THEN eventTimeIso END,
        'parentFileHashSha256'  VALUE CASE WHEN g_parent < 3720 THEN h_p256 END,
        'parentFileHashSha1'    VALUE CASE WHEN g_parent < 3720 THEN h_p1 END,
        'parentFileHashMd5'     VALUE CASE WHEN g_parent < 3720 THEN h_pmd5 END,
        'parentCmd'             VALUE parentCmd,
        'parentCmdNormalized'   VALUE parentCmdNormalized,
        'parentUserDomain'      VALUE CASE WHEN g_parent < 390 THEN
                                     'ad-' || SUBSTRING(h_pmd5, 6, 4) END,
        -- object block
        'objectHashId'          VALUE CASE WHEN g_obj < 9860 THEN objectHashId_raw END,
        'objectFileHashId'      VALUE CASE WHEN g_obj < 5850 THEN objectHashId_raw END,
        'objectName'            VALUE CASE WHEN g_obj < 5710 THEN objectFilePathVal END,
        'objectFilePath'        VALUE CASE WHEN g_obj < 6410 THEN objectFilePathVal END,
        'objectPid'             VALUE CASE WHEN g_obj < 5710 THEN processPid END,
        'objectUser'            VALUE CASE WHEN g_obj < 5720 THEN objectUserVal END,
        'objectUserDomain'      VALUE CASE WHEN g_obj < 100 THEN
                                     'ad-' || SUBSTRING(h_omd5, 2, 4) END,
        'objectRunAsLocalAccount' VALUE CASE WHEN g_obj < 5850 THEN FALSE END,
        'objectLaunchTime'      VALUE CASE WHEN g_obj < 5710 THEN
                                     CAST(eventTimeMs - 584 AS STRING) END,
        'objectLaunchTimeDT'    VALUE CASE WHEN g_obj < 5710 THEN eventTimeIso END,
        'objectFirstSeen'       VALUE CASE WHEN g_obj < 3960 THEN
                                     CAST(eventTimeMs AS STRING) END,
        'objectFirstSeenDT'     VALUE CASE WHEN g_obj < 3960 THEN eventTimeIso END,
        'objectLastSeen'        VALUE CASE WHEN g_obj < 3960 THEN
                                     CAST(eventTimeMs AS STRING) END,
        'objectLastSeenDT'      VALUE CASE WHEN g_obj < 3960 THEN eventTimeIso END,
        'objectFileHashSha256'  VALUE CASE WHEN g_obj < 5700 THEN h_o256 END,
        'objectFileHashSha1'    VALUE CASE WHEN g_obj < 5700 THEN h_o1 END,
        'objectFileHashMd5'     VALUE CASE WHEN g_obj < 5700 THEN h_omd5 END,
        'objectCmd'             VALUE objectCmd,
        'objectCmdNormalized'   VALUE objectCmdNormalized,
        'objectSubTrueType'     VALUE CASE WHEN g_obj < 560 THEN 1 END,
        -- objectBmData is a JSON-ENCODED STRING in the source, not a struct, so
        -- it is emitted as an ordinary STRING and correctly comes out escaped —
        -- which is what the real line contains.
        'objectBmData'          VALUE CASE WHEN g_rare < 190 THEN
                                     '{"provider":"ORCA","schema_version":1,"severity":"low"}' END,
        -- network block
        'src'                   VALUE CASE WHEN g_net < 3330 THEN src_ip END,
        'dst'                   VALUE CASE WHEN g_net < 3330 THEN dst_ip END,
        'spt'                   VALUE CASE WHEN g_net < 3330 THEN sptVal END,
        'dpt'                   VALUE CASE WHEN g_net < 3220 THEN dptVal END,
        'proto'                 VALUE CASE WHEN g_net < 3200 THEN proto END,
        'srcHashId'             VALUE CASE WHEN g_net < 3330 THEN objectHashId_raw END,
        'dstIsp'                VALUE JSON_ARRAY(
                                     CASE WHEN g_net < 1150 THEN 'Amazon.com' END ABSENT ON NULL),
        'dstCountry'            VALUE JSON_ARRAY(
                                     CASE WHEN g_net < 1110 THEN 'US' END ABSENT ON NULL),
        'dstCity'               VALUE JSON_ARRAY(
                                     CASE WHEN g_net < 1050 THEN 'Boardman' END ABSENT ON NULL),
        -- detection block (all rare, and co-occurring — see the header)
        'tags'                  VALUE JSON_ARRAY(
                                     CASE WHEN g_rare < 280 THEN tag_1 END,
                                     CASE WHEN g_rare < 280 THEN tag_2 END,
                                     CASE WHEN g_rare < 280 THEN tag_3 END,
                                     CASE WHEN g_rare < 280 THEN tag_4 END ABSENT ON NULL),
        'filterRiskLevel'       VALUE CASE WHEN g_rare < 230 THEN filterRiskLevel END,
        'tmFilterRiskLevel'     VALUE CASE WHEN g_rare < 230 THEN filterRiskLevel END,
        'bitwiseFilterRiskLevel' VALUE CASE WHEN g_rare < 230 THEN 1 END,
        -- these two are JSON-encoded STRINGS in the source as well
        'platformAssetTags'     VALUE CASE WHEN g_rare < 130 THEN
                                     '{"Asset group": ["DC+Cloud", "TrendIQ"]}' END,
        'customAssetTags'       VALUE CASE WHEN g_rare < 130 THEN '{}' END
        ABSENT ON NULL
    )                                                       AS event_json,

    -- ---- the 25 remaining extracted columns, in schema_sample.sql order ----
    CASE WHEN g_rare < 230 THEN filterRiskLevel END         AS filterRiskLevel,
    eventId,
    eventSubId,
    endpointGuid,
    endpointHostName,
    ARRAY[src_ip, dst_ip]                                   AS endpointIp,
    -- hostName is the rare alternate host field (106 of 189,013 records), not
    -- a duplicate of endpointHostName.
    CASE WHEN g_rare < 6 THEN 'host-' || SUBSTRING(h_omd5, 1, 8) END AS hostName,
    pname,
    processName,
    processHashId,
    processName                                             AS processFilePath,
    processCmd,
    -- MISMATCH vs the sample: 'request' does not occur in any of the 189,013
    -- records. schema_sample.sql declares it, so the column is kept, but it is
    -- emitted NULL rather than invented — an all-null column is the honest
    -- representation of a field the source never sends.
    CAST(NULL AS STRING)                                    AS request,
    -- objectHostName: 3 of 189,013 records.
    CASE WHEN g_rare < 2 THEN 'host-' || SUBSTRING(h_o1, 1, 8) END AS objectHostName,
    CASE WHEN g_obj < 9860 THEN objectHashId_raw END        AS objectHashId,
    CASE WHEN g_obj < 6410 THEN objectFilePathVal END       AS objectFilePath,
    CASE WHEN g_obj < 5720 THEN objectUserVal END           AS objectUser,
    -- MISMATCH vs the sample: objectCmd is a scalar STRING in the data but
    -- ARRAY<STRING> in schema_sample.sql. Wrapped in a 1-element array to
    -- honour the declared type; event_json keeps the scalar form so the raw
    -- line stays faithful. Same treatment for dst and src below.
    CASE WHEN objectCmd IS NOT NULL THEN ARRAY[objectCmd] END AS objectCmd,
    -- MISMATCH vs the sample: 'providerName' does not occur either. The nearest
    -- real field is the "provider" key embedded in objectBmData's JSON (1.9% of
    -- records, e.g. 'ORCA'), so the column is populated from that at the same
    -- rate instead of being left entirely null.
    CASE WHEN g_rare < 190 THEN 'ORCA' END                  AS providerName,
    -- Variable-length tag array: the unused slots are dropped rather than
    -- emitted as NULL elements.
    CASE WHEN g_rare >= 280 THEN CAST(NULL AS ARRAY<STRING>)
         WHEN tag_4 IS NOT NULL THEN ARRAY[tag_1, tag_2, tag_3, tag_4]
         WHEN tag_3 IS NOT NULL THEN ARRAY[tag_1, tag_2, tag_3]
         WHEN tag_2 IS NOT NULL THEN ARRAY[tag_1, tag_2]
         ELSE ARRAY[tag_1] END                              AS tags,
    CASE WHEN g_net < 3330 THEN ARRAY[dst_ip] END           AS dst,
    CASE WHEN g_net < 3330 THEN ARRAY[src_ip] END           AS src,
    CASE WHEN g_net < 3220 THEN dptVal END                  AS dpt,
    CASE WHEN g_net < 3330 THEN sptVal END                  AS spt,
    -- MISMATCH vs the sample: 'winEventId' does not occur. Only Windows
    -- records (8.28%) could carry it, so it is gated to exactly that share
    -- rather than left wholly null — this keeps the column's null density
    -- plausible without fabricating it on Linux rows.
    CASE WHEN osName = 'Windows' THEN 4688 END              AS winEventId
FROM cid_rows;
