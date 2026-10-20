package com.demo.flink.sqs;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Turns S3 ObjectCreated events from SQS into {@link FileSourceSplit}s and deletes each SQS
 * message once every file it announced was assigned at or before a completed checkpoint.
 *
 * <p>Threading: all fields except the counters are touched only on the coordinator thread.
 * SQS calls run on a private pool ({@code poll-threads} long polls + 2 for deletes); their
 * results come back through {@link SplitEnumeratorContext#runInCoordinatorThread}.
 *
 * <p>Bookkeeping is per message with a refcount: one message may announce many files.
 */
final class SqsSplitEnumerator implements SplitEnumerator<FileSourceSplit, EnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(SqsSplitEnumerator.class);
    private static final int SQS_BATCH = 10;
    private static final long STATS_INTERVAL_MS = 60_000;
    private static final long POLL_ERROR_BACKOFF_MS = 5_000;

    /** An SQS message whose files are not all checkpoint-confirmed yet. */
    private static final class Msg {
        String receiptHandle;
        int open; // files of this message still pending or awaiting confirmation

        Msg(String receiptHandle) {
            this.receiptHandle = receiptHandle;
        }
    }

    /** A received message after parsing and filtering (done on the poll thread). */
    private static final class Received {
        final SqsQueue.Message message;
        final List<FileSourceSplit> files; // null = body is not JSON

        Received(SqsQueue.Message message, List<FileSourceSplit> files) {
            this.message = message;
            this.files = files;
        }
    }

    private final SplitEnumeratorContext<FileSourceSplit> ctx;
    private final SqsSourceConfig cfg;
    private final SqsQueue.Factory queueFactory;
    private final LongSupplier clock;
    private final int maxPending;
    private final DoneSet done;

    private final ArrayDeque<FileSourceSplit> pending = new ArrayDeque<>();
    /** Every tracked split id (pending or assigned, not yet confirmed) -> message id. */
    private final Map<String, String> splitToMsg = new HashMap<>();
    private final Map<String, Msg> messages = new HashMap<>();
    private final List<String> assignedSinceSnapshot = new ArrayList<>();
    private final TreeMap<Long, List<String>> assignedByCheckpoint = new TreeMap<>();
    private final LinkedHashSet<Integer> waitingReaders = new LinkedHashSet<>();
    /** Restored as assigned: the checkpoint we restored from completed, so they are confirmed. */
    private final List<String> restoredAssigned = new ArrayList<>();

    private Executor io;
    private ExecutorService ownedIo;
    private SqsQueue queue;
    private int pollsInFlight;
    private boolean closed;

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong splitsCreated = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong ignored = new AtomicLong();
    private final AtomicLong malformed = new AtomicLong();
    private final AtomicLong redelivered = new AtomicLong();
    private final AtomicLong deleted = new AtomicLong();
    private final AtomicLong deleteFailures = new AtomicLong();
    private final AtomicLong pollErrors = new AtomicLong();
    private long lastPollErrorLog;

    SqsSplitEnumerator(SplitEnumeratorContext<FileSourceSplit> ctx, SqsSourceConfig cfg,
                       SqsQueue.Factory queueFactory, EnumState restored, Executor io) {
        this(ctx, cfg, queueFactory, restored, io, System::currentTimeMillis);
    }

    SqsSplitEnumerator(SplitEnumeratorContext<FileSourceSplit> ctx, SqsSourceConfig cfg,
                       SqsQueue.Factory queueFactory, EnumState restored, Executor io,
                       LongSupplier clock) {
        this.ctx = ctx;
        this.cfg = cfg;
        this.queueFactory = queueFactory;
        this.io = io;
        this.clock = clock;
        this.maxPending = cfg.maxPendingSplits > 0
                ? cfg.maxPendingSplits : 4 * Math.max(1, ctx.currentParallelism());
        this.done = new DoneSet(cfg.dedupGraceMillis);
        if (restored != null) {
            for (EnumState.PendingSplit p : restored.pending) {
                track(p.split.splitId(), p.messageId, restored.receiptHandles);
                pending.add(p.split);
            }
            for (Map.Entry<String, String> a : restored.assigned.entrySet()) {
                track(a.getKey(), a.getValue(), restored.receiptHandles);
                restoredAssigned.add(a.getKey());
            }
        }
    }

    private void track(String splitId, String messageId, Map<String, String> handles) {
        splitToMsg.put(splitId, messageId);
        messages.computeIfAbsent(messageId, id -> new Msg(handles.get(id))).open++;
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public void start() {
        queue = queueFactory.create();
        if (io == null) {
            ownedIo = Executors.newFixedThreadPool(cfg.pollThreads + 2, r -> {
                Thread t = new Thread(r, "sqs-delta-filename-io");
                t.setDaemon(true);
                return t;
            });
            io = ownedIo;
        }
        ctx.metricGroup().gauge("pendingSplits", pending::size);
        ctx.metricGroup().gauge("inflightHandles", messages::size);
        ctx.callAsync(() -> null, (v, t) -> logStats(), STATS_INTERVAL_MS, STATS_INTERVAL_MS);
        LOG.info("sqs-delta-filename: queue={} pollThreads={} maxPendingSplits={} restored {} pending, {} assigned",
                cfg.queueUrl, cfg.pollThreads, maxPending, pending.size(), restoredAssigned.size());
        confirm(restoredAssigned);
        restoredAssigned.clear();
        maybePoll();
    }

    /**
     * Best effort: makes every still-held message visible again, so a restarted job gets the
     * files whose assignment was not checkpointed now rather than after the visibility timeout.
     * Messages also in the restored state are recognised by id and just get the new handle.
     */
    @Override
    public void close() {
        closed = true;
        List<String> handles = new ArrayList<>();
        messages.values().forEach(m -> handles.add(m.receiptHandle));
        if (queue != null) {
            for (List<String> batch : batches(handles)) {
                try {
                    queue.changeVisibility(batch, 0);
                } catch (Exception e) {
                    LOG.debug("Releasing messages on close failed", e);
                    break;
                }
            }
        }
        if (ownedIo != null) {
            ownedIo.shutdown();
            try {
                ownedIo.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ownedIo.shutdownNow();
        }
        if (queue != null) {
            queue.close();
        }
    }

    // ------------------------------------------------------------------ assignment

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        waitingReaders.add(subtaskId);
        assignPending();
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        // Splits assigned after the last completed checkpoint of a failed reader.
        for (int i = splits.size() - 1; i >= 0; i--) {
            FileSourceSplit s = splits.get(i);
            if (!splitToMsg.containsKey(s.splitId())) {
                continue;
            }
            assignedSinceSnapshot.remove(s.splitId());
            assignedByCheckpoint.values().forEach(l -> l.remove(s.splitId()));
            pending.addFirst(s);
        }
        LOG.info("Re-queued {} splits from failed reader {}", splits.size(), subtaskId);
        assignPending();
    }

    @Override
    public void addReader(int subtaskId) {
        // FileSourceReader asks for a split on start; nothing to push.
    }

    private void assignPending() {
        Iterator<Integer> it = waitingReaders.iterator();
        while (it.hasNext() && !pending.isEmpty()) {
            int reader = it.next();
            it.remove();
            if (!ctx.registeredReaders().containsKey(reader)) {
                continue; // reader went away while waiting
            }
            FileSourceSplit split = pending.poll();
            ctx.assignSplit(split, reader);
            assignedSinceSnapshot.add(split.splitId());
        }
        maybePoll();
    }

    // ------------------------------------------------------------------ checkpoints

    @Override
    public EnumState snapshotState(long checkpointId) {
        if (!assignedSinceSnapshot.isEmpty()) {
            assignedByCheckpoint.computeIfAbsent(checkpointId, id -> new ArrayList<>())
                    .addAll(assignedSinceSnapshot);
            assignedSinceSnapshot.clear();
        }
        List<EnumState.PendingSplit> p = new ArrayList<>(pending.size());
        for (FileSourceSplit s : pending) {
            p.add(new EnumState.PendingSplit(s, splitToMsg.get(s.splitId())));
        }
        Map<String, String> assigned = new LinkedHashMap<>();
        assignedByCheckpoint.values().forEach(l -> l.forEach(id -> assigned.put(id, splitToMsg.get(id))));
        Map<String, String> handles = new HashMap<>(messages.size() * 2);
        messages.forEach((id, m) -> handles.put(id, m.receiptHandle));
        return new EnumState(p, assigned, handles);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        NavigableMap<Long, List<String>> confirmed = assignedByCheckpoint.headMap(checkpointId, true);
        if (confirmed.isEmpty()) {
            return;
        }
        List<String> ids = new ArrayList<>();
        confirmed.values().forEach(ids::addAll);
        confirmed.clear();
        confirm(ids);
    }

    /** The files' reads are now in Flink state: release their refs, delete finished messages. */
    private void confirm(Collection<String> splitIds) {
        long now = clock.getAsLong();
        List<String> handles = new ArrayList<>();
        for (String id : splitIds) {
            done.add(id, now);
            String messageId = splitToMsg.remove(id);
            Msg m = messageId == null ? null : messages.get(messageId);
            if (m != null && --m.open == 0) {
                messages.remove(messageId);
                handles.add(m.receiptHandle);
            }
        }
        deleteAsync(handles);
    }

    // ------------------------------------------------------------------ SQS

    private void maybePoll() {
        // Soft cap: one more poll may add up to 10 messages' worth of splits.
        while (!closed && pollsInFlight < cfg.pollThreads && pending.size() < maxPending) {
            pollsInFlight++;
            io.execute(this::pollOnce);
        }
    }

    /** Runs on the io pool. */
    private void pollOnce() {
        List<Received> batch = Collections.emptyList();
        Throwable error = null;
        try {
            List<SqsQueue.Message> msgs =
                    queue.receive(SQS_BATCH, cfg.waitSeconds, cfg.visibilitySeconds);
            batch = new ArrayList<>(msgs.size());
            for (SqsQueue.Message m : msgs) {
                batch.add(new Received(m, toSplits(m)));
            }
        } catch (Throwable t) {
            error = t;
            pollErrors.incrementAndGet();
            try {
                Thread.sleep(POLL_ERROR_BACKOFF_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        List<Received> result = batch;
        Throwable err = error;
        try {
            ctx.runInCoordinatorThread(() -> onPolled(result, err));
        } catch (RuntimeException e) {
            // coordinator shut down; the messages come back after the visibility timeout
        }
    }

    /** Parse + filter on the io thread; also the only place that may do a HEAD. */
    private List<FileSourceSplit> toSplits(SqsQueue.Message m) throws Exception {
        List<S3EventParser.S3Object> objects;
        try {
            objects = S3EventParser.parse(m.body);
        } catch (Exception e) {
            return null;
        }
        List<FileSourceSplit> splits = new ArrayList<>(objects.size());
        for (S3EventParser.S3Object o : objects) {
            if (!cfg.accepts(o.key)) {
                ignored.incrementAndGet();
                continue;
            }
            String id = cfg.pathOf(o.bucket, o.key);
            Path path = new Path(id);
            long size = o.size >= 0 ? o.size : path.getFileSystem().getFileStatus(path).getLen();
            splits.add(new FileSourceSplit(id, path, 0, size, o.eventTimeMillis, size));
        }
        return splits;
    }

    private void onPolled(List<Received> batch, Throwable error) {
        pollsInFlight--;
        if (error != null) {
            long now = clock.getAsLong();
            if (now - lastPollErrorLog >= STATS_INTERVAL_MS) {
                lastPollErrorLog = now;
                LOG.warn("SQS ReceiveMessage failed ({} failures so far)", pollErrors.get(), error);
            }
        }
        if (closed) {
            return;
        }
        long now = clock.getAsLong();
        List<String> deleteNow = new ArrayList<>();
        for (Received r : batch) {
            received.incrementAndGet();
            SqsQueue.Message m = r.message;
            Msg known = messages.get(m.id);
            if (known != null) {
                // Same message again (visibility timeout expired): only the newest handle deletes.
                known.receiptHandle = m.receiptHandle;
                redelivered.incrementAndGet();
                continue;
            }
            if (r.files == null) {
                malformed.incrementAndGet();
                LOG.warn("Leaving non-JSON SQS message {} for the DLQ", m.id);
                continue;
            }
            int open = 0;
            for (FileSourceSplit s : r.files) {
                if (splitToMsg.containsKey(s.splitId()) || done.contains(s.splitId(), now)) {
                    duplicates.incrementAndGet(); // tracked by another message, or already committed
                    continue;
                }
                splitToMsg.put(s.splitId(), m.id);
                pending.add(s);
                open++;
            }
            if (open == 0) {
                deleteNow.add(m.receiptHandle); // test event, filtered, or all duplicates
            } else {
                Msg msg = new Msg(m.receiptHandle);
                msg.open = open;
                messages.put(m.id, msg);
                splitsCreated.addAndGet(open);
            }
        }
        deleteAsync(deleteNow);
        assignPending();
    }

    private void deleteAsync(List<String> handles) {
        for (List<String> batch : batches(handles)) {
            io.execute(() -> {
                try {
                    int failed = queue.delete(batch);
                    deleted.addAndGet(batch.size() - failed);
                    deleteFailures.addAndGet(failed);
                } catch (Exception e) {
                    deleteFailures.addAndGet(batch.size());
                    LOG.warn("SQS DeleteMessageBatch failed; the messages will be redelivered and dropped as duplicates", e);
                }
            });
        }
    }

    private static List<List<String>> batches(List<String> handles) {
        List<List<String>> out = new ArrayList<>();
        for (int i = 0; i < handles.size(); i += SQS_BATCH) {
            out.add(new ArrayList<>(handles.subList(i, Math.min(handles.size(), i + SQS_BATCH))));
        }
        return out;
    }

    private void logStats() {
        int awaiting = assignedSinceSnapshot.size();
        for (List<String> l : assignedByCheckpoint.values()) {
            awaiting += l.size();
        }
        LOG.info("sqs-delta-filename: received={} redelivered={} splits={} duplicates={} ignored={} malformed={} "
                        + "deleted={} deleteFailures={} pollErrors={} | pending={} assignedUnconfirmed={} "
                        + "inflightMessages={} done={}",
                received.get(), redelivered.get(), splitsCreated.get(), duplicates.get(), ignored.get(),
                malformed.get(), deleted.get(), deleteFailures.get(), pollErrors.get(),
                pending.size(), awaiting, messages.size(), done.size());
    }

    // ------------------------------------------------------------------ test hooks

    int pendingCount() {
        return pending.size();
    }

    int inflightMessages() {
        return messages.size();
    }
}
