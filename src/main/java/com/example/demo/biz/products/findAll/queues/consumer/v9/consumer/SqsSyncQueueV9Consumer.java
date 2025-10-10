package com.example.demo.biz.products.findAll.queues.consumer.v9.consumer;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.utils.QueueAttributeUtils;
import com.example.demo.biz.products.findAll.queues.consumer.v9.cache.LockingV9CacheService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.StringUtils;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.stream.Collectors.toList;

@Component
@Slf4j
public class SqsSyncQueueV9Consumer implements AutoCloseable {

    private static final int MAX_MESSAGES_PER_POLL = 10;
    private static final int WAIT_TIME_SECONDS = 20;
    private static final int VISIBILITY_TIMEOUT_SECONDS = 30;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final SqsClient sqsClient;
    private final String queueUrl;

    private final ExecutorService pollingExecutor =
            Executors.newSingleThreadExecutor(r -> Thread.ofPlatform().name("find-all-consumer-v9-poller-", 0).daemon(true).unstarted(r));

    private final UnresolvedMessagesStrategy unresolvedMessagesStrategy = UnresolvedMessagesStrategy.ADD_TO_CACHE;

    private enum UnresolvedMessagesStrategy {
        RELEASE,
        ADD_TO_CACHE
    }

    @Getter
    @Setter
    private volatile boolean running = false;

    public SqsSyncQueueV9Consumer() {
        this("http://localhost:9324/000000000000/product-find-web",
                "http://localhost:9324",
                Region.US_EAST_1,
                "test",
                "test"
        );
    }

    public SqsSyncQueueV9Consumer(String queueUrl, String endpoint, Region region, String accessKey, String secretKey) {
        this.queueUrl = queueUrl;
        this.sqsClient = SqsClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
    }

    @PostConstruct
    public void init() {
        if (running) return;
        running = true;
        pollingExecutor.submit(this::startPolling);
        log.info("start - Poller submitted");
    }

    @PreDestroy
    public void destroy() {
        close(); // close sqs connection
        pollingExecutor.shutdown(); // stop Thread
        log.info("stop - Poller stopped");
    }

    public void startPolling() {
        log.info("startPolling - Starting polling");

        while (running) {
            log.info("StartPolling - >>>>> Polling... <<<<<");
            List<Message> messages = getMessages();
            if (!messages.isEmpty()) {
                log.info("startPolling - Received {} messages", messages.size());
                processMessages(messages);
            } else {
                log.debug("startPolling - No messages received");
            }
            log.info("StartPolling - >>>> Polling completed <<<<< ");
        }

        log.info("startPolling - Polling completed");
    }

    private void processMessages(List<Message> messages) {
        List<Message> toDelete = new ArrayList<>();
        List<Message> toRelease = new ArrayList<>();
        try {
            for (Message m : messages) {
                try {
                    if (handleMessage(m)) {
                        toDelete.add(m);
                    } else {
                        toRelease.add(m);
                    }
                } catch (Exception e) {
                    log.error("processMessages - Failed to process messageId={}: {}", m.messageId(), e.getMessage(), e);
                    toRelease.add(m);
                }
            }
        } finally {
            if (!toDelete.isEmpty()) {
                log.debug("processMessages - Deleting {} messages", toDelete.size());
                safeDeleteBatch(toDelete);
            }
            if (!toRelease.isEmpty()) {
                log.debug("processMessages - Releasing {} messages", toRelease.size());
                handleUnresolvedStrategies(toRelease);
            }
        }
    }

    public void handleUnresolvedStrategies(List<Message> messages) {
        log.info("handleUnresolvedStrategies - Handling unresolved messages, size: {}", messages.size());
        switch (unresolvedMessagesStrategy) {
            case RELEASE -> {
                log.info("handleUnresolvedStrategies - Releasing messages");
                releaseMessagesBatch(messages);
            }
            case ADD_TO_CACHE -> {
                log.info("handleUnresolvedStrategies - Adding to cache");
                for (Message m : messages) {
                    String correlationId = QueueAttributeUtils.extractCorrelationId(m);
                    var products = parseProductList(m.body());
                    LockingV9CacheService.INSTANCE.putIfAbsent(correlationId, products.orElse(List.of()));
                }
            }
            default -> throw new IllegalStateException("Unexpected value: " + unresolvedMessagesStrategy);
        }
        log.info("handleUnresolvedStrategies - Deleting messages");
        safeDeleteBatch(messages);
        log.info("handleUnresolvedStrategies - Done");
    }

    private boolean handleMessage(Message m) {
        String correlationId = QueueAttributeUtils.extractCorrelationId(m);

        if (StringUtils.isBlank(correlationId)) {
            log.warn("handleMessage - Missing CORRELATION_ID for messageId={}", m.messageId());
            return false;
        }
        if (StringUtils.isBlank(m.body())) {
            log.warn("handleMessage - Missing BODY for messageId={}", m.messageId());
            return false;
        }

        Optional<List<ProductResponseDto>> productsOpt = parseProductList(m.body());
        if (productsOpt.isEmpty() || productsOpt.get().isEmpty()) {
            log.warn("handleMessage - Unparseable or empty list for messageId: {} with correlationId:{}", m.messageId(), correlationId);
            return false;
        }

        List<ProductResponseDto> products = productsOpt.get();
        if (LockingV9CacheService.INSTANCE.hasCorrelationId(correlationId)) {
            log.debug("handleMessage - Caching {} products for correlationId={}", products.size(), correlationId);
            LockingV9CacheService.setProducts(correlationId, products);
            return true;
        } else {
            log.debug("handleMessage - CorrelationId {} not registered, releasing messageId={}", correlationId, m.messageId());
            return false;
        }
    }

    private List<Message> getMessages() {
        log.debug("getMessages - Getting messages from queue {}, perPoll={}, wait={}, visibility={}", queueUrl, MAX_MESSAGES_PER_POLL, WAIT_TIME_SECONDS, VISIBILITY_TIMEOUT_SECONDS);

        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(MAX_MESSAGES_PER_POLL)
                .waitTimeSeconds(WAIT_TIME_SECONDS)
                .visibilityTimeout(VISIBILITY_TIMEOUT_SECONDS)
                .messageAttributeNames(QueueAttributeName.ALL.toString())
                .build();

        try {
            ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);
            if (!response.hasMessages()) {
                return Collections.emptyList();
            }
            log.debug("getMessages - Received {} messages", response.messages().size());
            return response.messages();
        } catch (Exception e) {
            log.error("getMessages - Failed to receive messages: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    private Optional<List<ProductResponseDto>> parseProductList(String messageBody) {
        try {
            List<ProductResponseDto> products = OBJECT_MAPPER.readValue(
                    messageBody,
                    new TypeReference<List<ProductResponseDto>>() {
                    }
            );
            return Optional.ofNullable(products);
        } catch (Exception e) {
            log.warn("parseProductList - Failed to parse message body. {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    private void safeDeleteBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) return;

        List<DeleteMessageBatchRequestEntry> entries = buildDeleteEntries(messages);
        try {
            DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build();

            DeleteMessageBatchResponse response = sqsClient.deleteMessageBatch(deleteRequest);
            if (response.hasFailed() && !response.failed().isEmpty()) {
                log.warn("safeDeleteBatch - {} messages failed to delete", response.failed().size());
                response.failed().forEach(failure ->
                        log.warn("safeDeleteBatch - Failed to delete messageId={}: {}", failure.id(), failure.message())
                );
            }
        } catch (SdkException e) {
            log.error("safeDeleteBatch - Batch delete failed: {}", e.getMessage(), e);
        }
    }

    private List<DeleteMessageBatchRequestEntry> buildDeleteEntries(List<Message> messages) {
        return messages.stream()
                .map(m -> DeleteMessageBatchRequestEntry.builder()
                        .id(m.messageId() != null ? m.messageId() : UUID.randomUUID().toString())
                        .receiptHandle(m.receiptHandle())
                        .build())
                .collect(toList());
    }

    private void releaseMessagesBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) return;

        List<ChangeMessageVisibilityBatchRequestEntry> entries = buildReleaseEntries(messages);
        try {
            ChangeMessageVisibilityBatchRequest releaseRequest = ChangeMessageVisibilityBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build();

            ChangeMessageVisibilityBatchResponse response = sqsClient.changeMessageVisibilityBatch(releaseRequest);
            if (response.hasFailed() && !response.failed().isEmpty()) {
                log.warn("releaseMessagesBatch - {} messages failed to release", response.failed().size());
                response.failed().forEach(failure ->
                        log.warn("releaseMessagesBatch - Failed to release messageId={}: {}", failure.id(), failure.message())
                );
            }
        } catch (SdkException e) {
            log.error("releaseMessagesBatch - Batch visibility change failed: {}", e.getMessage(), e);
        }
    }

    private List<ChangeMessageVisibilityBatchRequestEntry> buildReleaseEntries(List<Message> messages) {
        return messages.stream()
                .map(m -> ChangeMessageVisibilityBatchRequestEntry.builder()
                        .id(m.messageId() != null ? m.messageId() : UUID.randomUUID().toString())
                        .receiptHandle(m.receiptHandle())
                        .visibilityTimeout(Math.max(0, 5))
                        .build())
                .collect(toList());
    }

    public void close() {
        if (sqsClient != null) {
            try {
                sqsClient.close();
                log.info("close - SQS client closed successfully");
            } catch (Exception e) {
                log.error("close - Failed to close SQS client: {}", e.getMessage(), e);
            }
        }
    }
}