package com.demo.flink.sqs;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.FileSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Version 1: pending splits (with their message ids), assigned splits, receipt handles. */
public final class EnumStateSerializer implements SimpleVersionedSerializer<EnumState> {

    private static final int VERSION = 1;
    private static final FileSourceSplitSerializer SPLITS = FileSourceSplitSerializer.INSTANCE;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(EnumState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(4096);
        out.writeInt(SPLITS.getVersion());
        out.writeInt(state.pending.size());
        for (EnumState.PendingSplit p : state.pending) {
            out.writeUTF(p.messageId);
            byte[] split = SPLITS.serialize(p.split);
            out.writeInt(split.length);
            out.write(split);
        }
        writeMap(out, state.assigned);
        writeMap(out, state.receiptHandles);
        return out.getCopyOfBuffer();
    }

    @Override
    public EnumState deserialize(int version, byte[] bytes) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown sqs-delta-filename enumerator state version " + version);
        }
        DataInputDeserializer in = new DataInputDeserializer(bytes);
        int splitVersion = in.readInt();
        int n = in.readInt();
        List<EnumState.PendingSplit> pending = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String messageId = in.readUTF();
            byte[] split = new byte[in.readInt()];
            in.readFully(split);
            FileSourceSplit s = SPLITS.deserialize(splitVersion, split);
            pending.add(new EnumState.PendingSplit(s, messageId));
        }
        return new EnumState(pending, readMap(in), readMap(in));
    }

    private static void writeMap(DataOutputSerializer out, Map<String, String> map) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<String, String> e : map.entrySet()) {
            out.writeUTF(e.getKey());
            out.writeUTF(e.getValue());
        }
    }

    private static Map<String, String> readMap(DataInputDeserializer in) throws IOException {
        int n = in.readInt();
        Map<String, String> map = new LinkedHashMap<>(n * 2);
        for (int i = 0; i < n; i++) {
            map.put(in.readUTF(), in.readUTF());
        }
        return map;
    }
}
