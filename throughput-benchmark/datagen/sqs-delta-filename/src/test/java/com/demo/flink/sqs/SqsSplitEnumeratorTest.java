package com.demo.flink.sqs;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.demo.flink.sqs.S3EventParserTest.event;
import static com.demo.flink.sqs.S3EventParserTest.record;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqsSplitEnumeratorTest {

    // ---------------------------------------------------------------- fakes

    /** In-memory queue: receive hides messages (as visibility does); tests re-enqueue to redeliver. */
    static final class FakeQueue implements SqsQueue {
        final ArrayDeque<Message> visible = new ArrayDeque<>();
        final List<String> deleted = new ArrayList<>();
        final List<String> released = new ArrayList<>();

        @Override
        public List<Message> receive(int max, int waitSeconds, int visibilitySeconds) {
            List<Message> out = new ArrayList<>();
            while (out.size() < max && !visible.isEmpty()) {
                out.add(visible.poll());
            }
            return out;
        }

        @Override
        public int delete(List<String> handles) {
            assertTrue(handles.size() <= 10);
            deleted.addAll(handles);
            return 0;
        }

        @Override
        public int changeVisibility(List<String> handles, int seconds) {
            released.addAll(handles);
            return 0;
        }

        @Override
        public void close() {}
    }

    /** Runs nothing by itself: the test pumps the io queue and the coordinator queue. */
    static final class FakeContext implements SplitEnumeratorContext<FileSourceSplit> {
        final ArrayDeque<Runnable> coordinator = new ArrayDeque<>();
        final ArrayDeque<Runnable> io = new ArrayDeque<>();
        final Map<Integer, List<FileSourceSplit>> assigned = new HashMap<>();
        final Map<Integer, ReaderInfo> readers = new HashMap<>();

        FakeContext(int parallelism) {
            for (int i = 0; i < parallelism; i++) {
                readers.put(i, new ReaderInfo(i, "localhost"));
            }
        }

        @Override
        public SplitEnumeratorMetricGroup metricGroup() {
            return UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public int currentParallelism() {
            return readers.size();
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            return readers;
        }

        @Override
        public void assignSplits(SplitsAssignment<FileSourceSplit> a) {
            a.assignment().forEach((r, s) -> assigned.computeIfAbsent(r, k -> new ArrayList<>()).addAll(s));
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public <T> void callAsync(Callable<T> c, BiConsumer<T, Throwable> h) {}

        @Override
        public <T> void callAsync(Callable<T> c, BiConsumer<T, Throwable> h, long delay, long period) {}

        @Override
        public void runInCoordinatorThread(Runnable r) {
            coordinator.add(r);
        }

        /** A few rounds of io-then-coordinator work (an idle poller would re-poll forever). */
        void pump() {
            for (int round = 0; round < 4; round++) {
                for (int n = io.size(); n > 0; n--) {
                    io.poll().run();
                }
                for (int n = coordinator.size(); n > 0; n--) {
                    coordinator.poll().run();
                }
            }
        }

        List<String> assignedPaths(int reader) {
            return assigned.getOrDefault(reader, List.of()).stream()
                    .map(FileSourceSplit::splitId).collect(Collectors.toList());
        }
    }

    // ---------------------------------------------------------------- helpers

    private FakeQueue queue;
    private FakeContext ctx;
    private final AtomicLong clock = new AtomicLong(1_000_000);

    @BeforeEach
    void setUp() {
        queue = new FakeQueue();
        ctx = new FakeContext(2);
    }

    private static SqsSourceConfig config(int maxPending) {
        return new SqsSourceConfig("https://sqs/q", "us-west-2", "s3a", "landing/", ".json.gz",
                1, maxPending, 900, 20, 30 * 60_000L);
    }

    private SqsSplitEnumerator enumerator(EnumState state) {
        return enumerator(state, 100);
    }

    private SqsSplitEnumerator enumerator(EnumState state, int maxPending) {
        SqsSplitEnumerator e = new SqsSplitEnumerator(
                ctx, config(maxPending), () -> queue, state, ctx.io::add, clock::get);
        e.start();
        ctx.pump();
        return e;
    }

    /** Enqueue a message announcing landing/<key> files. */
    private void send(String id, String handle, String... keys) {
        String[] recs = Arrays.stream(keys)
                .map(k -> record("ObjectCreated:Put", "bkt", "landing/" + k, 10))
                .toArray(String[]::new);
        queue.visible.add(new SqsQueue.Message(id, handle, event(recs)));
    }

    private static String path(String key) {
        return "s3a://bkt/landing/" + key;
    }

    private void request(SqsSplitEnumerator e, int reader) {
        e.handleSplitRequest(reader, "localhost");
        ctx.pump();
    }

    private void checkpoint(SqsSplitEnumerator e, long id) throws Exception {
        e.snapshotState(id);
        e.notifyCheckpointComplete(id);
        ctx.pump();
    }

    // ---------------------------------------------------------------- tests

    @Test
    void deletesOnlyAfterTheAssigningCheckpointCompletes() throws Exception {
        send("m1", "h1", "a.json.gz");
        SqsSplitEnumerator e = enumerator(null);
        assertEquals(1, e.pendingCount());

        request(e, 0);
        assertEquals(List.of(path("a.json.gz")), ctx.assignedPaths(0));
        EnumState s = e.snapshotState(1);
        assertEquals(Map.of(path("a.json.gz"), "m1"), s.assigned);
        assertEquals("h1", s.receiptHandles.get("m1"));
        ctx.pump();
        assertTrue(queue.deleted.isEmpty(), "not before the checkpoint completes");

        e.notifyCheckpointComplete(1);
        ctx.pump();
        assertEquals(List.of("h1"), queue.deleted);
        assertEquals(0, e.inflightMessages());
    }

    @Test
    void assignmentAfterSnapshotWaitsForTheNextCheckpoint() throws Exception {
        send("m1", "h1", "a.json.gz");
        SqsSplitEnumerator e = enumerator(null);
        e.snapshotState(1);
        request(e, 0); // assigned after checkpoint 1's snapshot
        e.notifyCheckpointComplete(1);
        ctx.pump();
        assertTrue(queue.deleted.isEmpty());

        checkpoint(e, 2);
        assertEquals(List.of("h1"), queue.deleted);
    }

    @Test
    void multiRecordMessageIsDeletedWhenItsLastFileIsCheckpointed() throws Exception {
        send("m1", "h1", "a.json.gz", "b.json.gz", "c.json.gz");
        SqsSplitEnumerator e = enumerator(null);
        assertEquals(3, e.pendingCount());

        request(e, 0);
        request(e, 1);
        checkpoint(e, 1); // a, b confirmed; c still pending
        assertTrue(queue.deleted.isEmpty());
        assertEquals(1, e.inflightMessages());

        request(e, 0); // c, after checkpoint 1
        e.snapshotState(2);
        assertTrue(queue.deleted.isEmpty());
        e.notifyCheckpointComplete(2);
        ctx.pump();
        assertEquals(List.of("h1"), queue.deleted, "deleted once, with the one handle");
    }

    @Test
    void redeliveryReplacesTheHandleAndDedupsPerKey() throws Exception {
        send("m1", "h1", "a.json.gz", "b.json.gz");
        SqsSplitEnumerator e = enumerator(null);
        request(e, 0); // a
        request(e, 1); // b

        // Visibility timeout expired: the same message comes back with a new handle.
        send("m1", "h1-new", "a.json.gz", "b.json.gz");
        // A different message re-announcing b (tracked) plus a new file d.
        send("m2", "h2", "b.json.gz", "d.json.gz");
        ctx.pump();
        assertEquals(1, e.pendingCount(), "only d is new");

        checkpoint(e, 1);
        assertEquals(List.of("h1-new"), queue.deleted, "m1 deleted with its newest handle");

        // A later message re-sends a, b (done) and nothing new: deleted at once, no splits.
        send("m3", "h3", "a.json.gz", "b.json.gz");
        ctx.pump();
        assertEquals(List.of("h1-new", "h3"), queue.deleted);
        assertEquals(1, e.pendingCount());

        request(e, 0); // d
        checkpoint(e, 2);
        assertEquals(List.of("h1-new", "h3", "h2"), queue.deleted);
        assertEquals(List.of(path("a.json.gz"), path("d.json.gz")), ctx.assignedPaths(0));
        assertEquals(List.of(path("b.json.gz")), ctx.assignedPaths(1));
    }

    @Test
    void doneKeysExpireAfterTheGrace() throws Exception {
        send("m1", "h1", "a.json.gz");
        SqsSplitEnumerator e = enumerator(null);
        request(e, 0);
        checkpoint(e, 1);

        clock.addAndGet(31 * 60_000L);
        send("m2", "h2", "a.json.gz");
        ctx.pump();
        assertEquals(1, e.pendingCount(), "beyond dedup-grace a re-announced key is read again");
    }

    @Test
    void testEventsAndFilteredKeysAreDeletedAtOnceMalformedIsLeft() {
        queue.visible.add(new SqsQueue.Message("t", "ht",
                "{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\",\"Bucket\":\"bkt\"}"));
        queue.visible.add(new SqsQueue.Message("w", "hw",
                event(record("ObjectCreated:Put", "bkt", "landing/x.txt", 1),
                      record("ObjectCreated:Put", "bkt", "other/y.json.gz", 1),
                      record("ObjectRemoved:Delete", "bkt", "landing/z.json.gz", 0))));
        queue.visible.add(new SqsQueue.Message("bad", "hbad", "not json"));
        SqsSplitEnumerator e = enumerator(null);
        assertEquals(0, e.pendingCount());
        assertEquals(List.of("ht", "hw"), queue.deleted);
    }

    @Test
    void restoreConfirmsAssignedSplitsAndKeepsPendingOnes() throws Exception {
        send("m1", "h1", "a.json.gz", "b.json.gz");
        send("m2", "h2", "c.json.gz");
        SqsSplitEnumerator e = enumerator(null);
        request(e, 0); // a (m1)
        request(e, 1); // b (m1)
        e.addSplitsBack(List.of(ctx.assigned.get(1).get(0)), 1); // b back to pending
        request(e, 1); // a split added back is re-assigned before newer ones
        assertEquals(List.of(path("b.json.gz")), ctx.assignedPaths(1).subList(1, 2));
        e.addSplitsBack(List.of(ctx.assigned.get(1).get(1)), 1); // b pending again
        EnumState snap = e.snapshotState(5); // a assigned; b, c pending
        assertTrue(queue.deleted.isEmpty());

        EnumStateSerializer ser = new EnumStateSerializer();
        EnumState state = ser.deserialize(ser.getVersion(), ser.serialize(snap));

        // Global failover: new enumerator from checkpoint 5. m1 also comes back from SQS.
        ctx = new FakeContext(2);
        send("m1", "h1-new", "a.json.gz", "b.json.gz");
        SqsSplitEnumerator r = enumerator(state);
        assertEquals(2, r.pendingCount(), "b and c, no duplicate for the redelivered m1");
        assertTrue(queue.deleted.isEmpty(), "m1 still has b pending, m2 has c pending");

        request(r, 0);
        request(r, 1);
        assertEquals(List.of(path("b.json.gz")), ctx.assignedPaths(0));
        assertEquals(List.of(path("c.json.gz")), ctx.assignedPaths(1));
        checkpoint(r, 6);
        assertEquals(2, queue.deleted.size());
        assertTrue(queue.deleted.containsAll(List.of("h1-new", "h2")));
    }

    @Test
    void restoredAssignedOnlyMessageIsDeletedOnStart() throws Exception {
        send("m1", "h1", "a.json.gz");
        SqsSplitEnumerator e = enumerator(null);
        request(e, 0);
        EnumState snap = e.snapshotState(3); // completes, but the notification never arrives

        ctx = new FakeContext(2);
        SqsSplitEnumerator r = enumerator(snap);
        assertEquals(List.of("h1"), queue.deleted);

        // A late redelivery of a is a duplicate now.
        send("m1", "h1-late", "a.json.gz");
        ctx.pump();
        assertEquals(0, r.pendingCount());
        assertEquals(List.of("h1", "h1-late"), queue.deleted);
    }

    @Test
    void splitsAddedBackAreConfirmedByTheirNewAssignment() throws Exception {
        send("m1", "h1", "a.json.gz");
        SqsSplitEnumerator e = enumerator(null);
        request(e, 0);
        e.addSplitsBack(new ArrayList<>(ctx.assigned.get(0)), 0);
        assertEquals(1, e.pendingCount());
        checkpoint(e, 1);
        assertTrue(queue.deleted.isEmpty(), "a is unassigned again at checkpoint 1");

        request(e, 1);
        checkpoint(e, 2);
        assertEquals(List.of("h1"), queue.deleted);
    }

    @Test
    void pausesPollingAtMaxPendingSplits() {
        for (int i = 0; i < 25; i++) {
            send("m" + i, "h" + i, i + ".json.gz");
        }
        SqsSplitEnumerator e = enumerator(null, 2);
        assertEquals(10, e.pendingCount(), "one poll (10 messages) then paused");
        assertEquals(15, queue.visible.size());

        for (int i = 0; i < 9; i++) {
            request(e, i % 2);
        }
        assertEquals(11, e.pendingCount(), "resumed once below the cap: 1 left + 10 polled");
        assertEquals(5, queue.visible.size());
    }

    @Test
    void closeReleasesHeldMessages() throws Exception {
        send("m1", "h1", "a.json.gz");
        send("m2", "h2", "b.json.gz");
        SqsSplitEnumerator e = enumerator(null);
        request(e, 0);
        checkpoint(e, 1); // m1 deleted
        e.close();
        assertEquals(List.of("h2"), queue.released);
    }
}
