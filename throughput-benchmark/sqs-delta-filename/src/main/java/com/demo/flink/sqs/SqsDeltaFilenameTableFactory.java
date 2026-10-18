package com.demo.flink.sqs;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.time.Duration;
import java.util.Set;

/**
 * {@code 'connector' = 'sqs-delta-filename'}: an unbounded {@code (line STRING)} table over the
 * S3 files announced by S3 ObjectCreated events on an SQS queue.
 */
public final class SqsDeltaFilenameTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "sqs-delta-filename";

    public static final ConfigOption<String> QUEUE_URL = ConfigOptions.key("queue-url")
            .stringType().noDefaultValue().withDescription("SQS queue URL.");
    public static final ConfigOption<String> REGION = ConfigOptions.key("region")
            .stringType().defaultValue("us-west-2");
    public static final ConfigOption<String> PATH_SCHEME = ConfigOptions.key("path-scheme")
            .stringType().defaultValue("s3a").withDescription("Files are read as <scheme>://<bucket>/<key>.");
    public static final ConfigOption<String> KEY_PREFIX = ConfigOptions.key("key-prefix")
            .stringType().noDefaultValue();
    public static final ConfigOption<String> KEY_SUFFIX = ConfigOptions.key("key-suffix")
            .stringType().defaultValue(".json.gz");
    public static final ConfigOption<Integer> POLL_THREADS = ConfigOptions.key("poll-threads")
            .intType().defaultValue(8);
    public static final ConfigOption<Integer> MAX_PENDING_SPLITS = ConfigOptions.key("max-pending-splits")
            .intType().noDefaultValue().withDescription("Pause polling at this many unassigned splits. Default 4 x parallelism.");
    public static final ConfigOption<Duration> VISIBILITY_TIMEOUT = ConfigOptions.key("visibility-timeout")
            .durationType().defaultValue(Duration.ofMinutes(15));
    public static final ConfigOption<Duration> WAIT_TIME = ConfigOptions.key("wait-time")
            .durationType().defaultValue(Duration.ofSeconds(20));
    public static final ConfigOption<Duration> DEDUP_GRACE = ConfigOptions.key("dedup-grace")
            .durationType().defaultValue(Duration.ofMinutes(30));

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(QUEUE_URL);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(REGION, PATH_SCHEME, KEY_PREFIX, KEY_SUFFIX, POLL_THREADS, MAX_PENDING_SPLITS,
                VISIBILITY_TIMEOUT, WAIT_TIME, DEDUP_GRACE);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig o = helper.getOptions();

        DataType physical = context.getPhysicalRowDataType();
        RowType row = (RowType) physical.getLogicalType();
        if (row.getFieldCount() != 1 || !"line".equals(row.getFieldNames().get(0))
                || row.getTypeAt(0).getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            throw new ValidationException(IDENTIFIER + " tables have exactly one column: line STRING (got "
                    + row.asSummaryString() + ")");
        }
        long visibility = o.get(VISIBILITY_TIMEOUT).getSeconds();
        long wait = o.get(WAIT_TIME).getSeconds();
        int pollThreads = o.get(POLL_THREADS);
        if (visibility < 1 || visibility > 43_200) {
            throw new ValidationException("'visibility-timeout' must be 1 s .. 12 h");
        }
        if (wait < 0 || wait > 20) {
            throw new ValidationException("'wait-time' must be 0 .. 20 s");
        }
        if (pollThreads < 1) {
            throw new ValidationException("'poll-threads' must be >= 1");
        }
        SqsSourceConfig cfg = new SqsSourceConfig(
                o.get(QUEUE_URL), o.get(REGION), o.get(PATH_SCHEME),
                o.getOptional(KEY_PREFIX).orElse(""), o.get(KEY_SUFFIX), pollThreads,
                o.getOptional(MAX_PENDING_SPLITS).orElse(0),
                (int) visibility, (int) wait, o.get(DEDUP_GRACE).toMillis());
        return new Source(cfg, physical);
    }

    /** The scan source; one per table. */
    static final class Source implements ScanTableSource {
        private final SqsSourceConfig cfg;
        private final DataType physical;

        Source(SqsSourceConfig cfg, DataType physical) {
            this.cfg = cfg;
            this.physical = physical;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
            return SourceProvider.of(new SqsFileSource(
                    cfg, AwsSqsQueue.factory(cfg), context.<RowData>createTypeInformation(physical)));
        }

        @Override
        public DynamicTableSource copy() {
            return new Source(cfg, physical);
        }

        @Override
        public String asSummaryString() {
            return IDENTIFIER + "(" + cfg.queueUrl + ")";
        }
    }
}
