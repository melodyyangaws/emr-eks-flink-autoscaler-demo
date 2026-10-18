package com.demo.flink.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** {@link SqsQueue} on the (shaded) AWS SDK v2 sync client with the URLConnection HTTP client. */
final class AwsSqsQueue implements SqsQueue {

    private static final Logger LOG = LoggerFactory.getLogger(AwsSqsQueue.class);

    private final String queueUrl;
    private final SqsClient client;

    private AwsSqsQueue(String queueUrl, String region, Duration waitTime) {
        this.queueUrl = queueUrl;
        this.client =
                SqsClient.builder()
                        .region(Region.of(region))
                        // On EKS this resolves the IRSA web identity token (needs the sts module).
                        .credentialsProvider(DefaultCredentialsProvider.builder().build())
                        .httpClientBuilder(
                                UrlConnectionHttpClient.builder()
                                        .connectionTimeout(Duration.ofSeconds(10))
                                        // must outlast the long poll
                                        .socketTimeout(waitTime.plusSeconds(15)))
                        .build();
    }

    static Factory factory(SqsSourceConfig cfg) {
        String url = cfg.queueUrl;
        String region = cfg.region;
        Duration wait = Duration.ofSeconds(cfg.waitSeconds);
        return () -> new AwsSqsQueue(url, region, wait);
    }

    @Override
    public List<Message> receive(int maxMessages, int waitSeconds, int visibilitySeconds) {
        return client
                .receiveMessage(r -> r.queueUrl(queueUrl)
                        .maxNumberOfMessages(maxMessages)
                        .waitTimeSeconds(waitSeconds)
                        .visibilityTimeout(visibilitySeconds))
                .messages().stream()
                .map(m -> new Message(m.messageId(), m.receiptHandle(), m.body()))
                .collect(Collectors.toList());
    }

    @Override
    public int delete(List<String> handles) {
        List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>(handles.size());
        for (int i = 0; i < handles.size(); i++) {
            entries.add(DeleteMessageBatchRequestEntry.builder()
                    .id(Integer.toString(i)).receiptHandle(handles.get(i)).build());
        }
        return failures("DeleteMessageBatch",
                client.deleteMessageBatch(r -> r.queueUrl(queueUrl).entries(entries)).failed());
    }

    @Override
    public int changeVisibility(List<String> handles, int visibilitySeconds) {
        List<ChangeMessageVisibilityBatchRequestEntry> entries = new ArrayList<>(handles.size());
        for (int i = 0; i < handles.size(); i++) {
            entries.add(ChangeMessageVisibilityBatchRequestEntry.builder()
                    .id(Integer.toString(i)).receiptHandle(handles.get(i))
                    .visibilityTimeout(visibilitySeconds).build());
        }
        return failures("ChangeMessageVisibilityBatch",
                client.changeMessageVisibilityBatch(r -> r.queueUrl(queueUrl).entries(entries)).failed());
    }

    private static int failures(String op, List<BatchResultErrorEntry> failed) {
        if (!failed.isEmpty()) {
            BatchResultErrorEntry e = failed.get(0);
            LOG.debug("{}: {} entries failed, first: {} {}", op, failed.size(), e.code(), e.message());
        }
        return failed.size();
    }

    @Override
    public void close() {
        client.close();
    }
}
