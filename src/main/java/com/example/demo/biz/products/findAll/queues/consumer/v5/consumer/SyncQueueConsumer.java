package com.example.demo.biz.products.findAll.queues.consumer.v5.consumer;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.utils.QueueAttributeUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.stream.Collectors.toList;

@Slf4j
public class SyncQueueConsumer implements ISyncQueueConsumer<List<ProductResponseDto>>, AutoCloseable {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final SqsClient sqsClient;

    private static final String QUEUE_URL = "http://localhost:9324/000000000000/product-find-web";
    private static final String LOCALSTACK_ENDPOINT = "http://localhost:9324";

    public SyncQueueConsumer() {
        this.sqsClient = SqsClient.builder()
                .endpointOverride(URI.create(LOCALSTACK_ENDPOINT))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
    }

    @Override
    public CompletableFuture<List<ProductResponseDto>> consume(String correlationId) {

        final int MAX_ATTEMPTS = 3;
        int currentAttempt = 0;

        log.info("SyncQueueConsumer::consume - Polling SQS queue {} for correlationId={}", QUEUE_URL, correlationId);
        List<ProductResponseDto> productList = null;

        do {
            log.info("###############################################################################################");

            sleepQuietly(currentAttempt);

            log.debug("SyncQueueConsumer::consume - Polling attempt {}/{} for correlationId={}",
                    currentAttempt,
                    MAX_ATTEMPTS,
                    correlationId
            );

            List<Message> messages = getMessages();
            if (messages.isEmpty()) {
                log.debug("SyncQueueConsumer::consume - No messages received for {} on attempt {}", QUEUE_URL, currentAttempt);
                continue;
            }

            log.debug("SyncQueueConsumer::consume - {} messages received on attempt {} for correlationId={}",
                    messages.size(),
                    currentAttempt,
                    correlationId
            );

            log.info("SyncQueueConsumer::consume - Polling products for expected correlationId={}", correlationId);
            productList = getProductList(messages, correlationId);
            log.info("SyncQueueConsumer::consume - Polling products completed for correlationId={}, products:{}", correlationId, productList);

            if (productList != null) {
                log.info("SyncQueueConsumer::consume - Polling completed - break! - {} products for correlationId={}", productList.size(), correlationId);
                break;
            }

        } while (currentAttempt++ < MAX_ATTEMPTS);

        log.info("SyncQueueConsumer::consume - Polling completed for correlationId={}, resultCount={}, currentAttempt={}",
                correlationId,
                productList == null ? 0 : productList.size(),
                currentAttempt
        );

        return CompletableFuture.completedFuture(productList);
    }

    private List<ProductResponseDto> getProductList(List<Message> messages, String correlationId) {
        List<Message> toDelete = new ArrayList<>();
        List<Message> toRelease = new ArrayList<>();

        try {

            for (Message m : messages) {
                try {
                    String messageCorrelationId = QueueAttributeUtils.extractCorrelationId(m);

                    if (StringUtils.isBlank(messageCorrelationId)) {
                        log.warn("SyncQueueConsumer::getProductList - Missing CORRELATION_ID for messageId={}", m.messageId());
                        continue;
                    }

                    if (!correlationId.equals(messageCorrelationId)) {
                        log.warn("SyncQueueConsumer::getProductList - Skipping message with different correlationId: {} (expected: {})", messageCorrelationId, correlationId);
                        toRelease.add(m);
                        continue;
                    }

                    if (StringUtils.isBlank(m.body())) {
                        log.warn("SyncQueueConsumer::getProductList - Missing BODY for correlationId={}, messageId={}", correlationId, m.messageId());
                        toDelete.add(m);
                        continue;
                    }

                    var productsOpt = parseProducts(m.body()).orElse(List.of());
                    if (productsOpt.isEmpty()) {
                        log.warn("SyncQueueConsumer::getProductList - Unparseable body for messageId={}, leaving for retry/DLQ", m.messageId());
                        continue;
                    }

                    log.info("SyncQueueConsumer::getProductList - Received {} products for correlationId={}", productsOpt.size(), correlationId);
                    List<ProductResponseDto> productList = new ArrayList<>(productsOpt);
                    log.info("SyncQueueConsumer::getProductList - Returning {} products for correlationId={}", productList.size(), correlationId);

                    toDelete.add(m);

                    return productList;
                } catch (Exception e) {
                    log.error("SyncQueueConsumer::getProductList - Failed to process messageId={}: {}", m.messageId(), e.getMessage(), e);
                }
            }
        } finally {
            log.info("SyncQueueConsumer::getProductList - Deleting {} messages for correlationId={}", toDelete.size(), correlationId);
            if (!toDelete.isEmpty()) safeDeleteBatch(toDelete);
            log.info("SyncQueueConsumer::getProductList - Releasing {} messages for correlationId={}", toRelease.size(), correlationId);
            if (!toRelease.isEmpty()) releaseMessagesBatch(toRelease);
        }

        return null;
    }

    private List<Message> getMessages() {
        var receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(QUEUE_URL)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                .visibilityTimeout(10)
                .messageAttributeNames("All")
                .build();
        try {
            log.info("SyncQueueConsumer::getMessages - Polling SQS queue {}", QUEUE_URL);
            var response = sqsClient.receiveMessage(receiveRequest);
            if (!response.hasMessages()) {
                log.info("SyncQueueConsumer::getMessages - SQS receive has no messages");
                return List.of();
            }
            log.info("SyncQueueConsumer::getMessages - SQS receive has messages: {}", response.messages().size());
            return response.messages();
        } catch (Exception e) {
            log.error("SyncQueueConsumer::getMessages - SQS receive failed: {}", e.getMessage(), e);
            return List.of();
        }
    }

    private Optional<List<ProductResponseDto>> parseProducts(String messageBody) {
        try {
            return Optional.ofNullable(objectMapper.readValue(messageBody, new TypeReference<List<ProductResponseDto>>() {
            }));
        } catch (Exception e) {
            log.error("SyncQueueConsumer::parseProducts - Failed to parse message body: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private void safeDeleteBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) return;
        log.debug("SyncQueueConsumer::safeDeleteBatch - Deleting {} messages", messages.size());
        List<DeleteMessageBatchRequestEntry> entries = messages.stream()
                .map(m -> DeleteMessageBatchRequestEntry.builder()
                        .id(m.messageId() != null ? m.messageId() : UUID.randomUUID().toString())
                        .receiptHandle(m.receiptHandle())
                        .build())
                .collect(toList());
        try {
            DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder().queueUrl(QUEUE_URL).entries(entries).build();
            sqsClient.deleteMessageBatch(deleteRequest);
        } catch (SdkException e) {
            log.error("SyncQueueConsumer::safeDeleteBatch - SQS delete failed: {}", e.getMessage(), e);
        }
    }

    private void releaseMessagesBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) return;
        log.debug("SyncQueueConsumer::releaseMessagesBatch - Releasing {} messages", messages.size());
        List<ChangeMessageVisibilityBatchRequestEntry> entries = messages.stream()
                .map(m -> ChangeMessageVisibilityBatchRequestEntry.builder()
                        .id(m.messageId() != null ? m.messageId() : UUID.randomUUID().toString())
                        .receiptHandle(m.receiptHandle())
                        .visibilityTimeout(0)
                        .build())
                .collect(toList());
        try {
            ChangeMessageVisibilityBatchRequest releaseRequest = ChangeMessageVisibilityBatchRequest.builder().queueUrl(QUEUE_URL).entries(entries).build();
            sqsClient.changeMessageVisibilityBatch(releaseRequest);
        } catch (SdkException e) {
            log.error("SyncQueueConsumer::releaseMessagesBatch - SQS change visibility failed: {}", e.getMessage(), e);
        }
    }

    private static void sleepQuietly(long retry) {
        try {
            if (retry > 0) {
                long base = 3_000L * retry;
                long jitter = ThreadLocalRandom.current().nextInt(100, 501);
                long time = base + jitter;
                log.info("SyncQueueConsumer::sleepQuietly - Sleeping for {} milliseconds", time);
                Thread.sleep(time);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws Exception {

    }
}

