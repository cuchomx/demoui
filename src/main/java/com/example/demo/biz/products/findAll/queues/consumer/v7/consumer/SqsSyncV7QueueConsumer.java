package com.example.demo.biz.products.findAll.queues.consumer.v7.consumer;

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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.stream.Collectors.toList;

@Slf4j
public class SqsSyncV7QueueConsumer implements ISqsSyncV7QueueConsumer<List<ProductResponseDto>>, AutoCloseable {

    private final SqsClient sqsClient;
    private final String queueUrl;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public record OperationResult(
            String correlationId,
            List<ProductResponseDto> products,
            boolean success,
            String message
    ) {
    }

    ;

    public SqsSyncV7QueueConsumer() {
        this("http://localhost:9324/000000000000/product-find-web",
                "http://localhost:9324",
                Region.US_EAST_1,
                "test",
                "test"
        );
    }

    public SqsSyncV7QueueConsumer(String queueUrl, String endpoint, Region region, String accessKey, String secretKey) {
        this.queueUrl = queueUrl;
        this.sqsClient = SqsClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
    }

    @Override
    public CompletableFuture<List<OperationResult>> consume() {

        log.debug("SyncQueueConsumer::consume - Starting polling");

        final int MAX_ATTEMPTS = 3;
        int currentAttempt = 1;

        List<OperationResult> results = null;

        do {
            log.debug("SyncQueueConsumer::consume - Polling attempt {}/{}", currentAttempt, MAX_ATTEMPTS);

            List<Message> messages = getMessages();
            if (!messages.isEmpty()) {
                log.info("SyncQueueConsumer::consume - Received {} messages on attempt {}", messages.size(), currentAttempt);
                results = getProductList(messages);
                if (results != null && !results.isEmpty()) {
                    log.info("SyncQueueConsumer::consume - consumption retrieved {} polling requests", results.size());
                    break;
                }
            }

            log.debug("SyncQueueConsumer::consume - No messages received on attempt {}", currentAttempt);
            sleepWithBackoff(currentAttempt);

        } while (currentAttempt++ <= MAX_ATTEMPTS);

        log.info("SyncQueueConsumer::consume - Polling completed for resultCount={}, attempts={}",
                results == null ? 0 : results.size(),
                currentAttempt
        );


        return CompletableFuture.completedFuture(results);
    }

    private List<OperationResult> getProductList(List<Message> messages) {
        List<Message> toDelete = new ArrayList<>();
        List<OperationResult> operationResults = new ArrayList<>();


        try {
            for (Message m : messages) {
                try {
                    String messageCorrelationId = QueueAttributeUtils.extractCorrelationId(m);

                    if (StringUtils.isBlank(messageCorrelationId)) {
                        log.warn("SyncQueueConsumer::getProductList - Missing CORRELATION_ID for messageId={}", m.messageId());
                        toDelete.add(m);
                        continue;
                    }

                    if (StringUtils.isBlank(m.body())) {
                        log.warn("SyncQueueConsumer::getProductList - Missing BODY for messageId={}", m.messageId());
                        toDelete.add(m);
                        continue;
                    }

                    Optional<List<ProductResponseDto>> productsOpt = parseProducts(m.body());
                    if (productsOpt.isEmpty() || productsOpt.get().isEmpty()) {
                        log.warn("SyncQueueConsumer::getProductList - Unparseable or empty body for messageId={}", m.messageId());
                        continue;
                    }

                    List<ProductResponseDto> productList = productsOpt.get();
                    log.info("SyncQueueConsumer::getProductList - Found {} products", productList.size());

                    toDelete.add(m);

                    operationResults.add(new OperationResult(
                            messageCorrelationId,
                            productList,
                            true,
                            "Success"
                    ));

                    return operationResults;

                } catch (Exception e) {
                    log.error("SyncQueueConsumer::getProductList - Failed to process messageId={}: {}", m.messageId(), e.getMessage(), e);
                }
            }
        } finally {
            if (!toDelete.isEmpty()) {
                log.debug("SyncQueueConsumer::getProductList - Deleting {} messages", toDelete.size());
                safeDeleteBatch(toDelete);
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
            log.debug("SyncQueueConsumer::getMessages - Polling SQS queue {}", queueUrl);
            ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);

            if (!response.hasMessages()) {
                log.debug("SyncQueueConsumer::getMessages - No messages available");
                return Collections.emptyList();
            }

            log.debug("SyncQueueConsumer::getMessages - Received {} messages", response.messages().size());
            return response.messages();
        } catch (Exception e) {
            log.error("SyncQueueConsumer::getMessages - Failed to receive messages: {}", e.getMessage(), e);
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
            DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build();

            DeleteMessageBatchResponse response = sqsClient.deleteMessageBatch(deleteRequest);

            if (response.hasFailed() && !response.failed().isEmpty()) {
                log.warn("SyncQueueConsumer::safeDeleteBatch - {} messages failed to delete", response.failed().size());
                response.failed().forEach(failure ->
                        log.warn("SyncQueueConsumer::safeDeleteBatch - Failed to delete messageId={}: {}",
                                failure.id(), failure.message())
                );
            }
        } catch (SdkException e) {
            log.error("SyncQueueConsumer::safeDeleteBatch - Batch delete failed: {}", e.getMessage(), e);
        }
    }

    private void sleepWithBackoff(int attemptNumber) {
        final long BASE_BACKOFF_MS = 2_000L;
        final int MIN_JITTER_MS = 100;
        final int MAX_JITTER_MS = 201;

        try {
            long base = BASE_BACKOFF_MS * attemptNumber;
            long jitter = ThreadLocalRandom.current().nextInt(MIN_JITTER_MS, MAX_JITTER_MS);
            long sleepTime = base + jitter;
            log.debug("SyncQueueConsumer::sleepWithBackoff - Backing off for {} ms before attempt {}", sleepTime, attemptNumber);
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("SyncQueueConsumer::sleepWithBackoff - Sleep interrupted");
        }
    }

    @Override
    public void close() {
        if (sqsClient != null) {
            try {
                sqsClient.close();
                log.info("SyncQueueConsumer::close - SQS client closed successfully");
            } catch (Exception e) {
                log.error("SyncQueueConsumer::close - Failed to close SQS client: {}", e.getMessage(), e);
            }
        }
    }
}