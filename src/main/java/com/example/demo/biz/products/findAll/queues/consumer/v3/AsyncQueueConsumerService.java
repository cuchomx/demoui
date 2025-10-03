package com.example.demo.biz.products.findAll.queues.consumer.v3;


import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.utils.QueueAttributeUtils;
import com.example.commons.utils.ReceiveMessageQueueUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
@Slf4j
@Service
public class AsyncQueueConsumerService implements AsyncQueueCall<List<ProductResponseDto>> {


    private final ObjectMapper objectMapper;

    private final SqsClient sqsClient;

    @Value("${aws.sqs.queue.find.web.consumer.url}")
    private String queueUrl;

    @Override
    public CompletableFuture<List<ProductResponseDto>> call(String correlationId) {

        try {
            var receiveRequest = ReceiveMessageQueueUtils.buildReceiveRequest(queueUrl);

            List<Message> messages = getMessages(receiveRequest);
            if (messages == null || messages.isEmpty()) {
                log.trace("AsyncQueueConsumerService::consume - No messages received");
                return CompletableFuture.completedFuture(List.of());
            }

            List<ProductResponseDto> accumulated = new ArrayList<>();

            for (Message m : messages) {
                try {
                    QueueAttributeUtils.logMessageSummary(m);

                    String messageCorrelationId = QueueAttributeUtils.extractCorrelationId(m);

                    String messageBody = validateAndExtractMessageBody(correlationId, m, messageCorrelationId);
                    if (messageBody == null) continue;

                    log.debug("AsyncQueueConsumerService::consume - Response received - messageId={}, correlationId={}, body.size={}",
                            m.messageId(),
                            messageCorrelationId,
                            messageBody.length()
                    );

                    var products = parseProducts(messageBody);
                    products.ifPresentOrElse(productList -> {
                        log.info("AsyncQueueConsumerService::consume - products.size={}", productList.size());
                        accumulated.addAll(productList);
                        delete(m.receiptHandle());
                    }, () -> log.warn("AsyncQueueConsumerService::consume - Parsed empty products for messageId={}, keeping message for retry", m.messageId()));

                    log.info("AsyncQueueConsumerService::consume - Processed message for correlationId: {}, accumulated total: {}",
                            correlationId,
                            accumulated.size()
                    );
                } catch (Exception e) {
                    log.error("AsyncQueueConsumerService::consume - Failed to process messageId={}: {}", m.messageId(), e.getMessage(), e);
                }
            }

            return CompletableFuture.completedFuture(accumulated);

        } catch (Exception e) {
            log.error("AsyncQueueConsumerService::call - Unexpected exception: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(List.of());
        }
    }

    private static String validateAndExtractMessageBody(String correlationId, Message m, String messageCorrelationId) {

        if (StringUtils.isBlank(messageCorrelationId)) {
            log.warn("AsyncQueueConsumerService::consume - Missing CORRELATION_ID for messageId={}", m.messageId());
            return null;
        }

        if (!correlationId.equals(messageCorrelationId)) {
            log.debug("AsyncQueueConsumerService::consume - Skipping message with different correlationId: {} (expected: {})", messageCorrelationId, correlationId);
            return null;
        }

        String messageBody = m.body();
        if (StringUtils.isBlank(messageBody)) {
            log.warn("AsyncQueueConsumerService::consume - Missing BODY for messageId={}, correlationId={}", m.messageId(), messageCorrelationId);
            return null;
        }
        return messageBody;
    }

    private List<Message> getMessages(ReceiveMessageRequest receiveRequest) {
        try {
            var response = sqsClient.receiveMessage(receiveRequest);
            return response != null ? response.messages() : List.of();
        } catch (Exception e) {
            log.error("AsyncQueueConsumerService::consume - SQS receive failed {}", e.getMessage(), e);
            return List.of();
        }
    }

    private Optional<List<ProductResponseDto>> parseProducts(String messageBody) {
        try {
            return Optional.ofNullable(
                    objectMapper.readValue(messageBody, new TypeReference<>() {
                    })
            );
        } catch (Exception e) {
            log.error("AsyncQueueConsumerService::parseProducts - Failed to parse message body: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    public void delete(String receiptHandle) {
        try {
            var deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();
            sqsClient.deleteMessage(deleteRequest);
            log.info("AsyncQueueConsumerService::delete - Deleted message with receiptHandle={}", receiptHandle);
        } catch (Exception e) {
            log.error("AsyncQueueConsumerService::delete - Exception: ", e);
        }
    }
}
