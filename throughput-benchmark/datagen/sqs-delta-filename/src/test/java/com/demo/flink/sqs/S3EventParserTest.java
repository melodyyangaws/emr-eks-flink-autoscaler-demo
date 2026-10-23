package com.demo.flink.sqs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3EventParserTest {

    static String record(String eventName, String bucket, String key, long size) {
        return "{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"eventTime\":\"2026-10-09T06:21:03.123Z\","
                + "\"eventName\":\"" + eventName + "\",\"s3\":{\"bucket\":{\"name\":\"" + bucket + "\"},"
                + "\"object\":{\"key\":\"" + key + "\",\"size\":" + size
                + ",\"eTag\":\"abc\",\"sequencer\":\"0066\"}}}";
    }

    static String event(String... records) {
        return "{\"Records\":[" + String.join(",", records) + "]}";
    }

    @Test
    void decodesFormEncodedKeys() throws IOException {
        List<S3EventParser.S3Object> out = S3EventParser.parse(event(record("ObjectCreated:Put", "b",
                "cid-landing-jsongz/year%3D2026/month%3D10/a+b%2Bc_001_new.json.gz", 1234)));
        assertEquals(1, out.size());
        S3EventParser.S3Object o = out.get(0);
        assertEquals("b", o.bucket);
        assertEquals("cid-landing-jsongz/year=2026/month=10/a b+c_001_new.json.gz", o.key);
        assertEquals(1234, o.size);
        assertEquals(java.time.Instant.parse("2026-10-09T06:21:03.123Z").toEpochMilli(), o.eventTimeMillis);
    }

    @Test
    void testEventAndUnknownJsonYieldNothing() throws IOException {
        assertTrue(S3EventParser.parse("{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\","
                + "\"Time\":\"2026-10-09T06:00:00.000Z\",\"Bucket\":\"b\"}").isEmpty());
        assertTrue(S3EventParser.parse("{\"hello\":1}").isEmpty());
        assertTrue(S3EventParser.parse("[]").isEmpty());
    }

    @Test
    void unwrapsSnsEnvelope() throws IOException {
        String inner = event(record("ObjectCreated:CompleteMultipartUpload", "b", "k%3D1.json.gz", 7));
        String sns = "{\"Type\":\"Notification\",\"MessageId\":\"m\",\"TopicArn\":\"arn:aws:sns:us-west-2:0:t\","
                + "\"Message\":" + quote(inner) + "}";
        List<S3EventParser.S3Object> out = S3EventParser.parse(sns);
        assertEquals(1, out.size());
        assertEquals("k=1.json.gz", out.get(0).key);
        assertEquals(7, out.get(0).size);
    }

    @Test
    void multipleRecordsKeepOnlyObjectCreated() throws IOException {
        List<S3EventParser.S3Object> out = S3EventParser.parse(event(
                record("ObjectCreated:Put", "b", "a.json.gz", 1),
                record("ObjectRemoved:Delete", "b", "gone.json.gz", 0),
                record("ObjectCreated:Copy", "b2", "c.json.gz", 3)));
        assertEquals(2, out.size());
        assertEquals("a.json.gz", out.get(0).key);
        assertEquals("b2", out.get(1).bucket);
    }

    @Test
    void missingSizeIsMinusOne() throws IOException {
        String body = "{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"bucket\":{\"name\":\"b\"},"
                + "\"object\":{\"key\":\"k\"}}}]}";
        assertEquals(-1, S3EventParser.parse(body).get(0).size);
    }

    @Test
    void nonJsonThrows() {
        assertThrows(IOException.class, () -> S3EventParser.parse("not json {"));
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
