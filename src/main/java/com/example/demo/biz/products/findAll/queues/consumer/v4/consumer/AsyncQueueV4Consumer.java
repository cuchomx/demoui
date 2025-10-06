package com.example.demo.biz.products.findAll.queues.consumer.v4.consumer;


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

@RequiredArgsConstructor
@Slf4j
@Service
public class AsyncQueueV4Consumer implements IAsyncQueueV4Consumer<List<ProductResponseDto>, String> {

    private final ObjectMapper objectMapper;

    private final SqsClient sqsClient;

    @Value("${aws.sqs.queue.find.web.consumer.url}")
    private String queueUrl;


    @Override
    public List<ProductResponseDto> consume(String correlationId) {

        log.info("AsyncQueueConsumerService::consume - Polling SQS queue {} for correlationId: {}", queueUrl, correlationId);

        try {
            var receiveRequest = ReceiveMessageQueueUtils.buildReceiveRequest(queueUrl, null);

            log.info("AsyncQueueV4Consumer::consume - Calling sqsClient.receiveMessage() for correlationId={}, attributeNames={}, messageAttributeNames={}:",
                    correlationId,
                    receiveRequest.attributeNamesAsStrings(),
                    receiveRequest.messageAttributeNames()
            );

            List<Message> messages = getMessages(receiveRequest);
            if (messages == null || messages.isEmpty()) {
                log.trace("AsyncQueueV4Consumer::consume - No messages received");
                return List.of();
            }

            List<ProductResponseDto> accumulated = new ArrayList<>();

            log.info("----------------------------------------------------------------------------------------------");
            log.info("AsyncQueueV4Consumer::consume - polling SQS queue {} to find correlationId: {} - {} messages received",
                    queueUrl,
                    correlationId,
                    messages.size()
            );

            for (Message m : messages) {
                try {
                    String messageCorrelationId = QueueAttributeUtils.extractCorrelationId(m);

                    if (!correlationId.equals(messageCorrelationId)) {
                        log.info("AsyncQueueV4Consumer::consume - Skipping message with different correlationId: {} (expected: {})",
                                messageCorrelationId,
                                correlationId
                        );
                        continue;
                    }

                    log.info("AsyncQueueV4Consumer::consume - Processing message for correlationId: {} with message.Id={} and message.correlationId:{}",
                            correlationId,
                            m.messageId(),
                            messageCorrelationId
                    );

                    QueueAttributeUtils.logMessageSummary(m);

                    String messageBody = validateAndExtractMessageBody(correlationId, m, messageCorrelationId);
                    if (messageBody == null) {
                        log.warn("AsyncQueueV4Consumer::consume - Missing BODY for messageId={}, correlationId={}", m.messageId(), correlationId);
                        continue;
                    }

                    log.info("AsyncQueueV4Consumer::consume - Response received - correlationId: {}, messageId={}, correlationId={}, body.size={}",
                            correlationId,
                            m.messageId(),
                            messageCorrelationId,
                            messageBody.length()
                    );

                    log.info("AsyncQueueV4Consumer::consume - Processing message for correlationId: {} with message.correlationId:{}",
                            correlationId,
                            messageCorrelationId
                    );

                    var products = parseProducts(messageBody);
                    products.ifPresentOrElse(productList -> {
                        log.info("AsyncQueueV4Consumer::consume - products.size={}", productList.size());
                        accumulated.addAll(productList);
                        productList.forEach(p -> log.info("AsyncQueueV4Consumer::consume - product: {}", p));
                        log.info("AsyncQueueV4Consumer::consume - Deleting message for correlationId: {}, messageId={}",
                                correlationId,
                                m.messageId()
                        );
                        delete(m.receiptHandle());
                    }, () -> log.warn("AsyncQueueV4Consumer::consume - Parsed empty products for messageId={}, keeping message for retry", m.messageId()));

                    log.info("AsyncQueueV4Consumer::consume - Processed message for correlationId: {}, accumulated total: {}",
                            correlationId,
                            accumulated.size()
                    );
                } catch (Exception e) {
                    log.error("AsyncQueueV4Consumer::consume - Failed to process messageId={}: {}", m.messageId(), e.getMessage(), e);
                }
            }

            return accumulated;

        } catch (Exception e) {
            log.error("AsyncQueueV4Consumer::call - Unexpected exception: {}", e.getMessage(), e);
            return List.of();
        }
    }

    private static String validateAndExtractMessageBody(String correlationId, Message m, String messageCorrelationId) {

        if (StringUtils.isBlank(messageCorrelationId)) {
            log.warn("AsyncQueueV4Consumer::validateAndExtractMessageBody - Missing CORRELATION_ID for messageId={}", m.messageId());
            return null;
        }

        if (!correlationId.equals(messageCorrelationId)) {
            log.debug("AsyncQueueV4Consumer::validateAndExtractMessageBody - Skipping message with different correlationId: {} (expected: {})", messageCorrelationId, correlationId);
            return null;
        }

        String messageBody = m.body();
        if (StringUtils.isBlank(messageBody)) {
            log.warn("AsyncQueueV4Consumer::validateAndExtractMessageBody - Missing BODY - expected correlationId={}, messagesCorrelationId:{}, messageId={}, ",
                    correlationId,
                    messageCorrelationId,
                    m.messageId()
            );
            return null;
        }
        return messageBody;
    }

    private List<Message> getMessages(ReceiveMessageRequest receiveRequest) {
        try {
            var response = sqsClient.receiveMessage(receiveRequest);
            return response != null ? response.messages() : List.of();
        } catch (Exception e) {
            log.error("AsyncQueueV4Consumer::consume - SQS receive failed {}", e.getMessage(), e);
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
            log.error("AsyncQueueV4Consumer::parseProducts - Failed to parse message body: {}", e.getMessage(), e);
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
            log.info("AsyncQueueV4Consumer::delete - Deleted message with receiptHandle={}", receiptHandle);
        } catch (Exception e) {
            log.error("AsyncQueueV4Consumer::delete - Exception: ", e);
        }
    }

}
