package com.example.demo.biz.products.findAll.queues.consumer.v8.consumer;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.utils.QueueAttributeUtils;
import com.example.demo.biz.products.findAll.queues.consumer.v8.caches.LockingV8CacheService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.StringUtils;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.stream.Collectors.toList;

@Slf4j
public class SqsSyncQueueV8Consumer implements AutoCloseable {

    private final SqsClient sqsClient;
    private final String queueUrl;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public SqsSyncQueueV8Consumer() {
        this("http://localhost:9324/000000000000/product-find-web",
                "http://localhost:9324",
                Region.US_EAST_1,
                "test",
                "test"
        );
    }

    public SqsSyncQueueV8Consumer(String queueUrl, String endpoint, Region region, String accessKey, String secretKey) {
        this.queueUrl = queueUrl;
        this.sqsClient = SqsClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
    }


    public CompletableFuture<LockingV8CacheService.LockStateVariables> consume(String correlationId) {

        log.info("consume - Starting polling for correlationId={}", correlationId);

        final int MAX_ATTEMPTS = 3;
        int currentAttempt = 1;

        List<ProductResponseDto> productList = null;

        do {

            log.debug("consume - Polling attempt {}/{} for correlationId={}", currentAttempt, MAX_ATTEMPTS, correlationId);

            List<Message> messages = getMessages();
            if (!messages.isEmpty()) {
                log.debug("consume - Received {} messages on attempt {}", messages.size(), currentAttempt);
                productList = getProductList(messages, correlationId);
                if (productList != null && !productList.isEmpty()) {
                    log.info("consume - Successfully retrieved {} products for correlationId={}", productList.size(), correlationId);
                    break;
                }
            }

            log.debug("consume - No messages received on attempt {}", currentAttempt);
            sleepWithBackoff(currentAttempt);

        } while (currentAttempt++ < MAX_ATTEMPTS);

        log.info("consume - Polling completed for correlationId={}, total-products={}, attempts={}",
                correlationId,
                productList == null ? 0 : productList.size(),
                currentAttempt
        );

        var operationResult = new LockingV8CacheService.LockStateVariables();
        operationResult.setCompleted(true);
        operationResult.setSuccess(productList != null);
        operationResult.setProducts(productList);

        return CompletableFuture.completedFuture(operationResult);
    }

    private List<ProductResponseDto> getProductList(List<Message> messages, String correlationId) {
        List<Message> toDelete = new ArrayList<>();
        List<Message> toRelease = new ArrayList<>();

        try {
            for (Message m : messages) {
                try {
                    String messageCorrelationId = QueueAttributeUtils.extractCorrelationId(m);

                    if (StringUtils.isBlank(messageCorrelationId)) {
                        log.warn("getProductList - Missing CORRELATION_ID for messageId={}", m.messageId());
                        toDelete.add(m);
                        continue;
                    }

                    if (!correlationId.equals(messageCorrelationId)) {
                        log.debug("getProductList - Skipping message with different correlationId: {} (expected: {})", messageCorrelationId, correlationId);
                        toRelease.add(m);
                        continue;
                    }

                    if (StringUtils.isBlank(m.body())) {
                        log.warn("getProductList - Missing BODY for correlationId={}, messageId={}", correlationId, m.messageId());
                        toDelete.add(m);
                        continue;
                    }

                    Optional<List<ProductResponseDto>> productsOpt = parseProducts(m.body());
                    if (productsOpt.isEmpty() || productsOpt.get().isEmpty()) {
                        log.warn("getProductList - Unparseable or empty body for messageId={}, releasing for retry", m.messageId());
                        toRelease.add(m);
                        continue;
                    }

                    List<ProductResponseDto> productList = productsOpt.get();
                    log.info("getProductList - Found {} products for correlationId={}",
                            productList.size(), correlationId);

                    toDelete.add(m);
                    return productList;

                } catch (Exception e) {
                    log.error("getProductList - Failed to process messageId={}: {}", m.messageId(), e.getMessage(), e);
                    toRelease.add(m);
                }
            }
        } finally {
            if (!toDelete.isEmpty()) {
                log.debug("getProductList - Deleting {} messages for correlationId={}",
                        toDelete.size(), correlationId);
                safeDeleteBatch(toDelete);
            }
            if (!toRelease.isEmpty()) {
                log.debug("getProductList - Releasing {} messages for correlationId={}",
                        toRelease.size(), correlationId);
                releaseMessagesBatch(toRelease);
            }
        }

        return null;
    }

    private List<Message> getMessages() {

        final int MAX_MESSAGES_PER_POLL = 10;
        final int WAIT_TIME_SECONDS = 20;
        final int VISIBILITY_TIMEOUT_SECONDS = 30;

        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(MAX_MESSAGES_PER_POLL)
                .waitTimeSeconds(WAIT_TIME_SECONDS)
                .visibilityTimeout(VISIBILITY_TIMEOUT_SECONDS)
                .messageAttributeNames("All")
                .build();

        try {
            log.debug("getMessages - Polling SQS queue {}", queueUrl);
            ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);

            if (!response.hasMessages()) {
                log.debug("getMessages - No messages available");
                return Collections.emptyList();
            }

            log.debug("getMessages - Received {} messages", response.messages().size());
            return response.messages();
        } catch (Exception e) {
            log.error("getMessages - Failed to receive messages: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    private Optional<List<ProductResponseDto>> parseProducts(String messageBody) {
        try {
            List<ProductResponseDto> products = objectMapper.readValue(
                    messageBody,
                    new TypeReference<List<ProductResponseDto>>() {
                    }
            );
            return Optional.ofNullable(products);
        } catch (Exception e) {
            log.error("parseProducts - Failed to parse message body: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private void safeDeleteBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) return;

        log.debug("safeDeleteBatch - Deleting {} messages", messages.size());

        List<DeleteMessageBatchRequestEntry> entries = messages.stream()
                .map(m -> DeleteMessageBatchRequestEntry.builder()
                        .id(m.messageId() != null ? m.messageId() : UUID.randomUUID().toString())
                        .receiptHandle(m.receiptHandle())
                        .build())
                .collect(toList());

        try {
            DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build();

            DeleteMessageBatchResponse response = sqsClient.deleteMessageBatch(deleteRequest);

            if (response.hasFailed() && !response.failed().isEmpty()) {
                log.warn("safeDeleteBatch - {} messages failed to delete", response.failed().size());
                response.failed().forEach(failure ->
                        log.warn("safeDeleteBatch - Failed to delete messageId={}: {}",
                                failure.id(), failure.message())
                );
            }
        } catch (SdkException e) {
            log.error("safeDeleteBatch - Batch delete failed: {}", e.getMessage(), e);
        }
    }

    private void releaseMessagesBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) return;

        log.debug("releaseMessagesBatch - Releasing {} messages", messages.size());

        List<ChangeMessageVisibilityBatchRequestEntry> entries = messages.stream()
                .map(m -> ChangeMessageVisibilityBatchRequestEntry.builder()
                        .id(m.messageId() != null ? m.messageId() : UUID.randomUUID().toString())
                        .receiptHandle(m.receiptHandle())
                        .visibilityTimeout(0)
                        .build())
                .collect(toList());

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

    private void sleepWithBackoff(int attemptNumber) {
        final long BASE_BACKOFF_MS = 2_000L;
        final int MIN_JITTER_MS = 100;
        final int MAX_JITTER_MS = 301;

        try {
            long base = BASE_BACKOFF_MS * attemptNumber;
            long jitter = ThreadLocalRandom.current().nextInt(MIN_JITTER_MS, MAX_JITTER_MS);
            long sleepTime = base + jitter;
            log.debug("sleepWithBackoff - Backing off for {} ms before attempt {}", sleepTime, attemptNumber);
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("sleepWithBackoff - Sleep interrupted");
        }
    }

    @Override
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