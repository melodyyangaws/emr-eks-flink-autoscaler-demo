package com.demo.flink.sqs;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

/**
 * Keys whose data is committed, remembered for at least {@code grace} to drop duplicate S3
 * events. Memory only. Keys are kept as 64-bit hashes in time buckets (1/30 of the grace), so
 * ~1M keys (600 files/s for 30 min) cost tens of MB, not hundreds.
 */
final class DoneSet {

    private static final class Bucket {
        final long start;
        final Set<Long> hashes = new HashSet<>();

        Bucket(long start) {
            this.start = start;
        }
    }

    private final long graceMillis;
    private final long bucketMillis;
    private final ArrayDeque<Bucket> buckets = new ArrayDeque<>();
    private int size;

    DoneSet(long graceMillis) {
        this.graceMillis = Math.max(0, graceMillis);
        this.bucketMillis = Math.max(1000, this.graceMillis / 30);
    }

    void add(String key, long now) {
        expire(now);
        Bucket last = buckets.peekLast();
        if (last == null || now >= last.start + bucketMillis) {
            last = new Bucket(now);
            buckets.addLast(last);
        }
        if (last.hashes.add(hash(key))) {
            size++;
        }
    }

    boolean contains(String key, long now) {
        expire(now);
        long h = hash(key);
        for (Bucket b : buckets) {
            if (b.hashes.contains(h)) {
                return true;
            }
        }
        return false;
    }

    int size() {
        return size;
    }

    private void expire(long now) {
        // A bucket's newest key was added before start + bucketMillis; drop it once that is
        // older than the grace.
        while (!buckets.isEmpty() && buckets.peekFirst().start + bucketMillis + graceMillis <= now) {
            size -= buckets.pollFirst().hashes.size();
        }
    }

    /** 64-bit FNV-1a with a final avalanche; collisions are negligible at millions of keys. */
    static long hash(String s) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < s.length(); i++) {
            h ^= s.charAt(i);
            h *= 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        return h;
    }
}
