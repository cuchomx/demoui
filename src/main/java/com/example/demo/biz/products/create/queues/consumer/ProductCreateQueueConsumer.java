package com.example.demo.biz.products.create.queues.consumer;

import com.example.commons.utils.QueueAttributeUtils;
import com.example.demo.biz.products.create.cache.ProductCacheService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static com.example.commons.constants.AppConstants.CORRELATION_ID;

@Slf4j
@RequiredArgsConstructor
@Component
public class ProductCreateQueueConsumer implements IProductCreateQueueConsumer {

    private final SqsClient sqsClient;

    @Value("${aws.sqs.queue.create.web.consumer.url}")
    private String queueUrl;

    private static final int MAX_NUMBER_OF_MESSAGES = 5;
    private static final int WAIT_TIME_SECONDS = 10;

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

    @Scheduled(fixedDelayString = "${sqs.poll.fixedDelay.ms:1000}")
    @Override
    public void consume() {
        log.debug("=================================================================================================");
        log.debug("ProductCreateQueueConsumer::consume - Polling SQS queue {}", queueUrl);

        ReceiveMessageRequest receiveRequest = buildReceiveRequest();
        List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

        if (messages == null || messages.isEmpty()) {
            log.trace("ProductCreateQueueConsumer::consume - No messages received");
            return;
        }

        for (Message m : messages) {
            QueueAttributeUtils.logMessageSummary(m);

            String correlationId = m.messageAttributes().get(CORRELATION_ID).stringValue();
            if (correlationId == null || correlationId.isBlank()) {
                log.warn("ProductCreateQueueConsumer::consume - Missing CORRELATION_ID for messageId={}", m.messageId());
                continue;
            }
            String messageBody = m.body();
            if (messageBody == null || messageBody.isBlank()) {
                log.warn("ProductCreateQueueConsumer::consume - Missing BODY for messageId={}, correlationId={}", m.messageId(), correlationId);
                continue;
            }
            log.info("ProductCreateQueueConsumer::consume - Response received - messageId={}, correlationId={}, body={}",
                    m.messageId(),
                    correlationId,
                    messageBody
            );
            ProductCacheService.update(correlationId, messageBody);
            ProductCacheService.display();
            delete(m.receiptHandle());
        }
    }

    private ReceiveMessageRequest buildReceiveRequest() {
        return ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(MAX_NUMBER_OF_MESSAGES)
                .waitTimeSeconds(WAIT_TIME_SECONDS)
                .messageAttributeNames(CORRELATION_ID)
                .build();
    }

    @Override
    public void delete(String receiptHandle) {
        try {
            var deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();
            sqsClient.deleteMessage(deleteRequest);
            log.info("ProductCreateQueueConsumer::delete - Deleted message with receiptHandle={}", receiptHandle);
        } catch (Exception e) {
            log.error("ProductCreateQueueConsumer::delete - Exception: ", e);
        }
    }

}
