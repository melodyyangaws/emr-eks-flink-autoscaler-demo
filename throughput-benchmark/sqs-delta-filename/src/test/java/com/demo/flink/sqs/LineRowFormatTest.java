package com.demo.flink.sqs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LineRowFormatTest {

    @Test
    void readsGzipLinesAsRows(@TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path file = dir.resolve("a_001_new.json.gz");
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(file))) {
            out.write("{\"a\":1}\n{\"b\":\"é\"}\n".getBytes(StandardCharsets.UTF_8));
        }
        long size = Files.size(file);
        Path path = new Path(file.toUri());
        FileSourceSplit split = new FileSourceSplit("s", path, 0, size, 0, size);

        List<String> lines = new ArrayList<>();
        try (BulkFormat.Reader<RowData> reader =
                     new StreamFormatAdapter<>(new LineRowFormat(null)).createReader(new Configuration(), split)) {
            BulkFormat.RecordIterator<RowData> batch;
            while ((batch = reader.readBatch()) != null) {
                RecordAndPosition<RowData> r;
                while ((r = batch.next()) != null) {
                    lines.add(r.getRecord().getString(0).toString());
                }
                batch.releaseBatch();
            }
        }
        assertEquals(List.of("{\"a\":1}", "{\"b\":\"é\"}"), lines);
    }
}
