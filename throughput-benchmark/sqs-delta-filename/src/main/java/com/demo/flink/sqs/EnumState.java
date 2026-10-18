package com.demo.flink.sqs;

import org.apache.flink.connector.file.src.FileSourceSplit;

import java.util.List;
import java.util.Map;

/**
 * Enumerator checkpoint. Splits assigned after the checkpoint are absent on purpose: their SQS
 * messages are undeleted and come back after the visibility timeout.
 */
public final class EnumState {

    /** An unassigned split and the SQS message that announced it. */
    static final class PendingSplit {
        final FileSourceSplit split;
        final String messageId;

        PendingSplit(FileSourceSplit split, String messageId) {
            this.split = split;
            this.messageId = messageId;
        }
    }

    final List<PendingSplit> pending;
    /** Split id (path) -> message id, for splits assigned at or before this checkpoint. */
    final Map<String, String> assigned;
    /** Message id -> newest receipt handle, for every message referenced above. */
    final Map<String, String> receiptHandles;

    EnumState(List<PendingSplit> pending, Map<String, String> assigned,
              Map<String, String> receiptHandles) {
        this.pending = pending;
        this.assigned = assigned;
        this.receiptHandles = receiptHandles;
    }
}
