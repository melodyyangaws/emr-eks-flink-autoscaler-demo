package com.demo.flink.sqs;

import java.io.Serializable;
import java.util.List;

/** The few SQS calls the enumerator makes, behind an interface so tests can fake them. */
public interface SqsQueue extends AutoCloseable {

    /** One received message. */
    final class Message {
        public final String id;
        public final String receiptHandle;
        public final String body;

        public Message(String id, String receiptHandle, String body) {
            this.id = id;
            this.receiptHandle = receiptHandle;
            this.body = body;
        }
    }

    /** Creates the queue client where it runs (the JobManager), not where the job is built. */
    @FunctionalInterface
    interface Factory extends Serializable {
        SqsQueue create();
    }

    /** One (long) poll. Blocks up to {@code waitSeconds}. */
    List<Message> receive(int maxMessages, int waitSeconds, int visibilitySeconds) throws Exception;

    /** Deletes up to 10 messages; returns how many entries failed. */
    int delete(List<String> receiptHandles) throws Exception;

    /** Sets the visibility timeout of up to 10 messages; returns how many entries failed. */
    int changeVisibility(List<String> receiptHandles, int visibilitySeconds) throws Exception;

    @Override
    void close();
}
