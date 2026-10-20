package com.demo.flink.sqs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.io.IOException;

/**
 * {@link TextLineInputFormat} with each line wrapped as a one-column {@link RowData}. Wrapped
 * by {@code StreamFormatAdapter}, which picks the decompressor from the file extension and
 * tracks restart offsets.
 */
final class LineRowFormat implements StreamFormat<RowData> {
    private static final long serialVersionUID = 1L;

    private final TextLineInputFormat lines = new TextLineInputFormat();
    private final TypeInformation<RowData> producedType;

    LineRowFormat(TypeInformation<RowData> producedType) {
        this.producedType = producedType;
    }

    @Override
    public Reader<RowData> createReader(Configuration config, FSDataInputStream stream,
                                        long fileLen, long splitEnd) throws IOException {
        return wrap(lines.createReader(config, stream, fileLen, splitEnd));
    }

    @Override
    public Reader<RowData> restoreReader(Configuration config, FSDataInputStream stream,
                                         long restoredOffset, long fileLen, long splitEnd)
            throws IOException {
        return wrap(lines.restoreReader(config, stream, restoredOffset, fileLen, splitEnd));
    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    private static Reader<RowData> wrap(Reader<String> in) {
        return new Reader<RowData>() {
            @Override
            public RowData read() throws IOException {
                String line = in.read();
                return line == null ? null : GenericRowData.of(StringData.fromString(line));
            }

            @Override
            public CheckpointedPosition getCheckpointedPosition() {
                return in.getCheckpointedPosition();
            }

            @Override
            public void close() throws IOException {
                in.close();
            }
        };
    }
}
