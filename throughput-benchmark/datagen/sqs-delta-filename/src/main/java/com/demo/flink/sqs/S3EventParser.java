package com.demo.flink.sqs;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parses an SQS body holding an S3 event notification: the raw S3 event JSON (raw message
 * delivery), or the same wrapped in an SNS envelope ({@code "Message": "<json string>"}).
 */
final class S3EventParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** One ObjectCreated record. {@code size} is -1 when the event omits it. */
    static final class S3Object {
        final String bucket;
        final String key;
        final long size;
        final long eventTimeMillis;

        S3Object(String bucket, String key, long size, long eventTimeMillis) {
            this.bucket = bucket;
            this.key = key;
            this.size = size;
            this.eventTimeMillis = eventTimeMillis;
        }
    }

    private S3EventParser() {}

    /**
     * Returns the ObjectCreated records with URL-decoded keys. An {@code s3:TestEvent}, any
     * other JSON object and records of other event types yield nothing.
     *
     * @throws IOException if the body is not JSON
     */
    static List<S3Object> parse(String body) throws IOException {
        JsonNode root = MAPPER.readTree(body);
        if (root == null || !root.isObject()) {
            return Collections.emptyList();
        }
        JsonNode records = root.get("Records");
        JsonNode snsMessage = root.get("Message");
        if (records == null && snsMessage != null && snsMessage.isTextual()) {
            return parse(snsMessage.asText());
        }
        if (records == null || !records.isArray()) {
            return Collections.emptyList(); // s3:TestEvent and anything else
        }
        List<S3Object> out = new ArrayList<>(records.size());
        for (JsonNode r : records) {
            if (!r.path("eventName").asText("").startsWith("ObjectCreated")) {
                continue;
            }
            JsonNode s3 = r.path("s3");
            String bucket = s3.path("bucket").path("name").asText(null);
            String key = s3.path("object").path("key").asText(null);
            if (bucket == null || key == null) {
                continue;
            }
            JsonNode size = s3.path("object").path("size");
            out.add(new S3Object(
                    bucket,
                    decodeKey(key),
                    size.canConvertToLong() ? size.asLong() : -1L,
                    eventTime(r.path("eventTime").asText(null))));
        }
        return out;
    }

    /** S3 event keys are form-encoded: {@code %3D} is '=', '+' is a space. */
    static String decodeKey(String key) {
        return URLDecoder.decode(key, StandardCharsets.UTF_8);
    }

    private static long eventTime(String iso) {
        if (iso == null) {
            return 0L;
        }
        try {
            return Instant.parse(iso).toEpochMilli();
        } catch (RuntimeException e) {
            return 0L;
        }
    }
}
