package com.demo.flink.sqs;

import java.io.Serializable;

/** Resolved table options, shipped with the source to the JobManager. */
final class SqsSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final String queueUrl;
    final String region;
    final String pathScheme;
    final String keyPrefix;
    final String keySuffix;
    final int pollThreads;
    /** Values below 1 mean 4 x source parallelism. */
    final int maxPendingSplits;
    final int visibilitySeconds;
    final int waitSeconds;
    final long dedupGraceMillis;

    SqsSourceConfig(String queueUrl, String region, String pathScheme, String keyPrefix,
                    String keySuffix, int pollThreads, int maxPendingSplits,
                    int visibilitySeconds, int waitSeconds, long dedupGraceMillis) {
        this.queueUrl = queueUrl;
        this.region = region;
        this.pathScheme = pathScheme;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.keySuffix = keySuffix == null ? "" : keySuffix;
        this.pollThreads = pollThreads;
        this.maxPendingSplits = maxPendingSplits;
        this.visibilitySeconds = visibilitySeconds;
        this.waitSeconds = waitSeconds;
        this.dedupGraceMillis = dedupGraceMillis;
    }

    boolean accepts(String key) {
        return key.startsWith(keyPrefix) && key.endsWith(keySuffix);
    }

    String pathOf(String bucket, String key) {
        return pathScheme + "://" + bucket + "/" + key;
    }
}
