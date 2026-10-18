package com.demo.flink.sqs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.FileSourceSplitSerializer;
import org.apache.flink.connector.file.src.impl.FileSourceReader;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

/** FLIP-27 source: SQS-driven enumerator, Flink's own file reader. */
public final class SqsFileSource implements Source<RowData, FileSourceSplit, EnumState> {
    private static final long serialVersionUID = 1L;

    private final SqsSourceConfig config;
    private final SqsQueue.Factory queueFactory;
    private final TypeInformation<RowData> producedType;

    SqsFileSource(SqsSourceConfig config, SqsQueue.Factory queueFactory,
                  TypeInformation<RowData> producedType) {
        this.config = config;
        this.queueFactory = queueFactory;
        this.producedType = producedType;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, FileSourceSplit> createReader(SourceReaderContext ctx) {
        return new FileSourceReader<>(
                ctx, new StreamFormatAdapter<>(new LineRowFormat(producedType)), ctx.getConfiguration());
    }

    @Override
    public SplitEnumerator<FileSourceSplit, EnumState> createEnumerator(
            SplitEnumeratorContext<FileSourceSplit> ctx) {
        return new SqsSplitEnumerator(ctx, config, queueFactory, null, null);
    }

    @Override
    public SplitEnumerator<FileSourceSplit, EnumState> restoreEnumerator(
            SplitEnumeratorContext<FileSourceSplit> ctx, EnumState state) {
        return new SqsSplitEnumerator(ctx, config, queueFactory, state, null);
    }

    @Override
    public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
        return FileSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<EnumState> getEnumeratorCheckpointSerializer() {
        return new EnumStateSerializer();
    }
}
