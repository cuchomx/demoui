package com.example.demo.biz.products.findAll.queues.consumer.v4.consumer;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.utils.QueueAttributeUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
@Slf4j
@Service
public class AsyncQueueV4Consumer implements IAsyncQueueV4Consumer<List<ProductResponseDto>, String> {

    private final ObjectMapper objectMapper;
    private final SqsClient sqsClient;

    @Value("${aws.sqs.queue.find.web.consumer.url}")
    private String queueUrl;

    @Value("${products.findAll.v4.consumer.maxPollAttempts:3}")
    private int maxPollAttempts;

    @Value("${products.findAll.v4.consumer.longPollingWaitSeconds:20}")
    private int longPollingWaitSeconds;

    @Value("${products.findAll.v4.consumer.visibilityTimeoutSeconds:60}")
    private int visibilityTimeoutSeconds;

    @Value("${products.findAll.v4.consumer.maxMessagesPerPoll:10}")
    private int maxMessagesPerPoll;

    @Override
    public List<ProductResponseDto> consume(String correlationId) {
        log.info("AsyncQueueV4Consumer::consume - Polling SQS queue {} for correlationId={}", queueUrl, correlationId);
        List<ProductResponseDto> accumulated = new ArrayList<>();

        for (int attempt = 1; attempt <= maxPollAttempts; attempt++) {
            if (pollAndProcessMessages(correlationId, attempt, accumulated)) {
                break; // Found messages, exit loop
            }
        }

        if (accumulated.isEmpty()) {
            log.info("AsyncQueueV4Consumer::consume - No matching messages after {} attempts for correlationId={}", maxPollAttempts, correlationId);
        }

        return accumulated;
    }

    private boolean pollAndProcessMessages(String correlationId, int attempt, List<ProductResponseDto> accumulated) {
        log.debug("AsyncQueueV4Consumer::pollAndProcessMessages - Polling attempt {}/{} for correlationId={}", attempt, maxPollAttempts, correlationId);
        try {
            List<Message> messages = getMessages();
            if (messages.isEmpty()) {
                log.debug("AsyncQueueV4Consumer::pollAndProcessMessages - No messages received on attempt {}", attempt);
                return false;
            }

            log.debug("AsyncQueueV4Consumer::pollAndProcessMessages - {} messages received on attempt {} for correlationId={}", messages.size(), attempt, correlationId);
            processMessages(messages, correlationId, accumulated);

            if (!accumulated.isEmpty()) {
                log.info("AsyncQueueV4Consumer::pollAndProcessMessages - Returning {} products for correlationId={}", accumulated.size(), correlationId);
                return true;
            }
        } catch (SdkException e) {
            log.error("AsyncQueueV4Consumer::pollAndProcessMessages - AWS SDK exception on attempt {}: {}", attempt, e.getMessage(), e);
            sleepQuietly(Duration.ofSeconds(attempt));
        } catch (Exception e) {
            log.error("AsyncQueueV4Consumer::pollAndProcessMessages - Unexpected exception on attempt {}: {}", attempt, e.getMessage(), e);
        }
        return false;
    }

    private void processMessages(List<Message> messages, String correlationId, List<ProductResponseDto> accumulated) {
        List<Message> toDelete = new ArrayList<>();
        List<Message> toRelease = new ArrayList<>();

//        var message = messages.stream()
//                .filter(Message::hasMessageAttributes)
//                .filter(m -> m.messageAttributes().containsKey(CORRELATION_ID))
//                .filter(m -> QueueAttributeUtils.extractCorrelationId(m).equals(correlationId))
//                .filter(m -> StringUtils.isNotBlank(m.body()))
//                .findFirst()
//                .orElse(null);
//
//        if (message != null) {
//            QueueAttributeUtils.logMessageSummary(message);
//            toDelete.add(message);
//        }


        for (Message m : messages) {
            try {
                String messageCorrelationId = QueueAttributeUtils.extractCorrelationId(m);
                if (StringUtils.isBlank(messageCorrelationId)) {
                    log.warn("AsyncQueueV4Consumer::processMessages - Missing CORRELATION_ID for messageId={}", m.messageId());
                    continue;
                }

                if (!correlationId.equals(messageCorrelationId)) {
                    toRelease.add(m);
                    continue;
                }

                if (StringUtils.isBlank(m.body())) {
                    log.warn("AsyncQueueV4Consumer::processMessages - Missing BODY for correlationId={}, messageId={}", correlationId, m.messageId());
                    toDelete.add(m);
                    continue;
                }

                parseProducts(m.body()).ifPresentOrElse(productList -> {
                    QueueAttributeUtils.logMessageSummary(m);
                    accumulated.addAll(productList);
                    toDelete.add(m);
                    log.debug("AsyncQueueV4Consumer::processMessages - Added {} products, accumulated={}", productList.size(), accumulated.size());
                }, () -> {
                    log.warn("AsyncQueueV4Consumer::processMessages - Unparseable body for messageId={}, leaving for retry/DLQ", m.messageId());
                });
            } catch (Exception e) {
                log.error("AsyncQueueV4Consumer::processMessages - Failed to process messageId={}: {}", m.messageId(), e.getMessage(), e);
            }
        }

        if (!toDelete.isEmpty()) safeDeleteBatch(toDelete);
        if (!toRelease.isEmpty()) releaseMessagesBatch(toRelease);
    }

    private List<Message> getMessages() {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(longPollingWaitSeconds)
                .visibilityTimeout(visibilityTimeoutSeconds)
                .maxNumberOfMessages(maxMessagesPerPoll)
                .messageAttributeNames("All")
                .build();
        try {
            var response = sqsClient.receiveMessage(receiveRequest);
            return (response != null && response.hasMessages()) ? response.messages() : List.of();
        } catch (SdkException e) {
            log.error("AsyncQueueV4Consumer::getMessages - SQS receive failed: {}", e.getMessage(), e);
            return List.of();
        }
    }

    private Optional<List<ProductResponseDto>> parseProducts(String messageBody) {
        try {
            return Optional.ofNullable(objectMapper.readValue(messageBody, new TypeReference<>() {
            }));
        } catch (Exception e) {
            log.error("AsyncQueueV4Consumer::parseProducts - Failed to parse message body: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private void safeDeleteBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) return;
        log.debug("AsyncQueueV4Consumer::safeDeleteBatch - Deleting {} messages", messages.size());
        List<DeleteMessageBatchRequestEntry> entries = messages.stream()
                .map(m -> DeleteMessageBatchRequestEntry.builder().id(UUID.randomUUID().toString()).receiptHandle(m.receiptHandle()).build())
                .collect(toList());
        try {
            DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder().queueUrl(queueUrl).entries(entries).build();
            sqsClient.deleteMessageBatch(deleteRequest);
        } catch (SdkException e) {
            log.error("AsyncQueueV4Consumer::safeDeleteBatch - SQS delete failed: {}", e.getMessage(), e);
        }
    }

    private void releaseMessagesBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) return;
        log.debug("AsyncQueueV4Consumer::releaseMessagesBatch - Releasing {} messages", messages.size());
        List<ChangeMessageVisibilityBatchRequestEntry> entries = messages.stream()
                .map(m -> ChangeMessageVisibilityBatchRequestEntry.builder().id(UUID.randomUUID().toString()).receiptHandle(m.receiptHandle()).visibilityTimeout(0).build())
                .collect(toList());
        try {
            ChangeMessageVisibilityBatchRequest releaseRequest = ChangeMessageVisibilityBatchRequest.builder().queueUrl(queueUrl).entries(entries).build();
            sqsClient.changeMessageVisibilityBatch(releaseRequest);
        } catch (SdkException e) {
            log.error("AsyncQueueV4Consumer::releaseMessagesBatch - SQS change visibility failed: {}", e.getMessage(), e);
        }
    }

    private static void sleepQuietly(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
