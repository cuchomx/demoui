package com.example.demo.biz.products.findAll.queues.consumer.sync;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.commons.utils.QueueAttributeUtils;
import com.example.commons.utils.ReceiveMessageQueueUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
@Component
public class ProductFindAllSyncQueueConsumer implements IProductFindAllSyncQueueConsumer {

    private final ObjectMapper objectMapper;

    private final SqsClient sqsClient;

    @Value("${aws.sqs.queue.find.web.consumer.url}")
    private String queueUrl;

    private static final int MAX_POLL_ATTEMPTS = 5;

    @PostConstruct
    public void validateConfiguration() {

        if (queueUrl == null || queueUrl.isBlank()) {
            throw new IllegalStateException("aws.sqs.queue.create.url must be configured");
        }
        try {
            var uri = new URI(queueUrl);
            if (!"http".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(uri.getScheme())) {
                throw new IllegalStateException("Invalid SQS queue URL scheme: " + uri.getScheme());
            }
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Invalid SQS queue URL: " + queueUrl, e);
        }
    }

    @Async
    @Override
    public CompletableFuture<List<ProductResponseDto>> consume(String correlationId) {
        log.info("=================================================================================================");

        log.debug("ProductFindAllSyncQueueConsumer::consume - Polling SQS queue {} for correlationId: {}", queueUrl, correlationId);

        if (!ParameterValidationUtils.isValidCorrelationIdValue(correlationId)) {
            log.error("ProductFindAllSyncQueueConsumer::consume - Invalid correlationId");
            return CompletableFuture.completedFuture(List.of());
        }

        for (int attempt = 1; attempt <= MAX_POLL_ATTEMPTS; attempt++) {
            log.debug("ProductFindAllSyncQueueConsumer::consume - Polling attempt {}/{} for correlationId: {}",
                    attempt,
                    MAX_POLL_ATTEMPTS,
                    correlationId
            );

            List<Message> messages = sqsClient.receiveMessage(ReceiveMessageQueueUtils.buildReceiveRequest(queueUrl)).messages();

            if (messages == null || messages.isEmpty()) {
                log.trace("ProductFindAllSyncQueueConsumer::consume - No messages received on attempt {}", attempt);
                continue;
            }

            List<ProductResponseDto> allProducts = new ArrayList<>();

            for (Message m : messages) {
                try {
                    QueueAttributeUtils.logMessageSummary(m);

                    String messageCorrelationId = QueueAttributeUtils.extractCorrelationId(m);
                    if (messageCorrelationId == null || messageCorrelationId.isBlank()) {
                        log.warn("ProductFindAllSyncQueueConsumer::consume - Missing CORRELATION_ID for messageId={}",
                                m.messageId());
                        continue;
                    }

                    // Only process messages matching the requested correlationId
                    if (!correlationId.equals(messageCorrelationId)) {
                        log.debug("ProductFindAllSyncQueueConsumer::consume - Skipping message with different correlationId: {} (expected: {})",
                                messageCorrelationId, correlationId);
                        continue;
                    }

                    String messageBody = m.body();
                    if (messageBody == null || messageBody.isBlank()) {
                        log.warn("ProductFindAllSyncQueueConsumer::consume - Missing BODY for messageId={}, correlationId={}",
                                m.messageId(), messageCorrelationId);
                        continue;
                    }

                    log.info("ProductFindAllSyncQueueConsumer::consume - Response received - messageId={}, correlationId={}, body={}",
                            m.messageId(), messageCorrelationId, messageBody);

                    List<ProductResponseDto> products = parseProducts(messageBody);
                    if (products != null && !products.isEmpty()) {
                        allProducts.addAll(products);
                        products.stream()
                                .filter(Objects::nonNull)
                                .forEach(product -> log.info("ProductFindAllSyncQueueConsumer::consume - product: {}", product));
                    }

                    delete(m.receiptHandle());

                    log.info("ProductFindAllSyncQueueConsumer::consume - Successfully processed message for correlationId: {}, total products: {}",
                            correlationId, allProducts.size());

                    return CompletableFuture.completedFuture(allProducts);

                } catch (Exception e) {
                    log.error("ProductFindAllSyncQueueConsumer::consume - Failed to process message: {}", e.getMessage(), e);
                }
            }

            // If we found matching messages, return the accumulated products
            if (!allProducts.isEmpty()) {
                log.debug("ProductFindAllSyncQueueConsumer::consume - Returning {} products for correlationId: {}",
                        allProducts.size(),
                        correlationId
                );
                return CompletableFuture.completedFuture(allProducts);
            }
        }

        log.info("ProductFindAllSyncQueueConsumer::consume - No matching messages found for correlationId: {} after {} attempts",
                correlationId,
                MAX_POLL_ATTEMPTS
        );

        return CompletableFuture.completedFuture(List.of());
    }

    private List<ProductResponseDto> parseProducts(String messageBody) {
        try {
            return objectMapper.readValue(messageBody, new TypeReference<List<ProductResponseDto>>() {
            });
        } catch (Exception e) {
            log.error("ProductFindAllSyncQueueConsumer::parseProducts - Failed to parse message body: {}", e.getMessage(), e);
            return List.of();
        }
    }

    @Override
    public void delete(String receiptHandle) {
        try {
            var deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();
            sqsClient.deleteMessage(deleteRequest);
            log.info("ProductFindAllSyncQueueConsumer::delete - Deleted message with receiptHandle={}", receiptHandle);
        } catch (Exception e) {
            log.error("ProductFindAllSyncQueueConsumer::delete - Exception: ", e);
        }
    }
}
