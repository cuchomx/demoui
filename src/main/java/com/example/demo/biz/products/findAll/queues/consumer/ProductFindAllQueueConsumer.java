package com.example.demo.biz.products.findAll.queues.consumer;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.commons.utils.QueueAttributeUtils;
import com.example.commons.utils.ReceiveMessageQueueUtils;
import com.example.demo.biz.products.findAll.cache.ProductFindAllCacheService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.common.util.StringUtils;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;

import static com.example.commons.constants.AppConstants.CORRELATION_ID;

@Slf4j
@RequiredArgsConstructor
@Component
public class ProductFindAllQueueConsumer implements IProductFindAllQueueConsumer {

    private final ObjectMapper objectMapper;

    private final SqsClient sqsClient;

    @Value("${aws.sqs.queue.find.web.consumer.url}")
    private String queueUrl;

    @PostConstruct
    public void validateConfiguration() {
        if (StringUtils.isBlank(queueUrl)) {
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

    //@Scheduled(fixedDelayString = "${sqs.poll.fixedDelay.ms:1000}")
    @Override
    public void consume() {
        log.info("=================================================================================================");

        log.debug("ProductFindAllQueueConsumer::consume - Polling SQS queue {}", queueUrl);

        var receiveRequest = ReceiveMessageQueueUtils.buildReceiveRequest(queueUrl);
        List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

        if (messages == null || messages.isEmpty()) {
            log.trace("ProductFindAllQueueConsumer::consume - No messages received");
            return;
        }

        for (Message m : messages) {

            QueueAttributeUtils.logMessageSummary(m);

            String correlationId = m.messageAttributes().get(CORRELATION_ID).stringValue();
            if (!ParameterValidationUtils.isValidCorrelationIdValue(correlationId)) {
                log.warn("ProductFindAllQueueConsumer::consume - Missing CORRELATION_ID for messageId={}", m.messageId());
                continue;
            }

            String messageBody = m.body();
            if (StringUtils.isBlank(messageBody)) {
                log.warn("ProductFindAllQueueConsumer::consume - Missing BODY for messageId={}, correlationId={}", m.messageId(), correlationId);
                continue;
            }

            log.info("ProductFindAllQueueConsumer::consume - Response received - messageId={}, correlationId={}, body={}",
                    m.messageId(),
                    correlationId,
                    messageBody
            );

            try {
                List<ProductResponseDto> products = objectMapper.readValue(
                        messageBody,
                        new TypeReference<>() {
                        }
                );

                products.stream()
                        .filter(Objects::nonNull)
                        .forEach(product -> {
                            log.info("ProductFindAllQueueConsumer::consume - product: {}", product);
                        });

                ProductFindAllCacheService.update(correlationId, products);
                ProductFindAllCacheService.display();
                delete(m.receiptHandle());

            } catch (Exception e) {
                log.error("ProductFindAllQueueConsumer::consume - Failed to parse message body: {}", e.getMessage());
            }
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
            log.info("ProductFindAllQueueConsumer::delete - Deleted message with receiptHandle={}", receiptHandle);
        } catch (Exception x) {
            log.error("ProductFindAllQueueConsumer::delete - Exception: ", x);
        }
    }

}
