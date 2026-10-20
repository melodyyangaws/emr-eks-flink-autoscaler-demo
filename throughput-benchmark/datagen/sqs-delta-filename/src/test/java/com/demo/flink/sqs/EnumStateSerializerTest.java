package com.demo.flink.sqs;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EnumStateSerializerTest {

    @Test
    void roundTrip() throws Exception {
        String p1 = "s3a://b/year=2026/a b.json.gz";
        String p2 = "s3a://b/year=2026/c.json.gz";
        FileSourceSplit s1 = new FileSourceSplit(p1, new Path(p1), 0, 100, 1234L, 100);
        FileSourceSplit s2 = new FileSourceSplit(p2, new Path(p2), 0, 200, 5678L, 200,
                new String[0], new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, 42));
        Map<String, String> assigned = new LinkedHashMap<>();
        assigned.put("s3a://b/x.json.gz", "m2");
        Map<String, String> handles = new LinkedHashMap<>();
        handles.put("m1", "handle-1");
        handles.put("m2", "handle-2");
        EnumState state = new EnumState(
                List.of(new EnumState.PendingSplit(s1, "m1"), new EnumState.PendingSplit(s2, "m1")),
                assigned, handles);

        EnumStateSerializer ser = new EnumStateSerializer();
        EnumState back = ser.deserialize(ser.getVersion(), ser.serialize(state));

        assertEquals(2, back.pending.size());
        FileSourceSplit r1 = back.pending.get(0).split;
        assertEquals(p1, r1.splitId());
        assertEquals(new Path(p1), r1.path());
        assertEquals(100, r1.length());
        assertEquals(100, r1.fileSize());
        assertEquals(1234L, r1.fileModificationTime());
        assertEquals("m1", back.pending.get(0).messageId);
        assertEquals(42, back.pending.get(1).split.getReaderPosition().get().getRecordsAfterOffset());
        assertEquals(assigned, back.assigned);
        assertEquals(handles, back.receiptHandles);

        EnumState empty = ser.deserialize(1, ser.serialize(new EnumState(List.of(), Map.of(), Map.of())));
        assertEquals(0, empty.pending.size());
        assertThrows(java.io.IOException.class, () -> ser.deserialize(99, new byte[0]));
    }
}
