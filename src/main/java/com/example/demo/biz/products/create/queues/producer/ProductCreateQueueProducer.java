package com.example.demo.biz.products.create.queues.producer;

import com.example.commons.dto.create.ProductRequestDto;
import com.example.commons.utils.SendMessageQueueUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;
import java.net.URISyntaxException;

@RequiredArgsConstructor
@Slf4j
@Component
public class ProductCreateQueueProducer implements IProductCreateQueueProducer {

    private final ObjectMapper objectMapper;

    private final SqsClient sqsClient;

    @Value("${aws.sqs.queue.create.web.producer.url}")
    private String queueUrl;

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

    @Override
    public void produce(String correlationId, ProductRequestDto product) {
        log.info("=================================================================================================");

        var message = getMessage(product);

        log.info("ProductCreateQueueProducer::produce - producing - id: {} message: {}", correlationId, message);

        try {
            var sendRequest = SendMessageQueueUtils.buildSendMessageRequest(queueUrl, correlationId, message);
            var response = sqsClient.sendMessage(sendRequest);
            log.info("ProductCreateQueueProducer::produce - Message sent successfully. queue: {}, messageId={}, sequenceNumber={}",
                    queueUrl,
                    response.messageId(),
                    response.sequenceNumber()
            );
        } catch (AwsServiceException e) {
            log.error("ProductCreateQueueProducer::produce - AWS service error. statusCode={}, awsErrorCode={}, requestId={}, message={}",
                    e.statusCode(), e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "n/a",
                    e.requestId(), e.getMessage(), e);
            throw e;
        } catch (SdkClientException e) {
            log.error("ProductCreateQueueProducer::produce - SDK client error: {}", e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            log.error("ProductCreateQueueProducer::produce - Unexpected error", e);
            throw e;
        }
    }

    private String getMessage(ProductRequestDto product) {
        String message = null;
        try {
            message = objectMapper.writeValueAsString(product);
        } catch (JsonProcessingException e) {
            log.error("ProductCreateQueueProducer::produce - Error serializing message: {}", e.getMessage(), e);
        }
        return message;
    }

}
