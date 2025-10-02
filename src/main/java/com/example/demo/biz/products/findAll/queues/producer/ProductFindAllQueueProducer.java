package com.example.demo.biz.products.findAll.queues.producer;

import com.example.commons.dto.find.ProductFindAllRequestDto;
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
import software.amazon.awssdk.utils.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;

@RequiredArgsConstructor
@Slf4j
@Component
public class ProductFindAllQueueProducer implements IProductFindAllQueueProducer {

    private final ObjectMapper objectMapper;

    private final SqsClient sqsClient;

    @Value("${aws.sqs.queue.find.web.producer.url}")
    private String queueUrl;

    @PostConstruct
    public void validateConfiguration() {
        if (StringUtils.isBlank(queueUrl)) {
            throw new IllegalStateException("aws.sqs.queue.find.web.producer.url must be configured");
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
    public void produce(String correlationId, ProductFindAllRequestDto dto) {

        log.info("=================================================================================================");

        String message = getMessage(dto);
        if (StringUtils.isBlank(message)) {
            throw new IllegalStateException("Failed to serialize ProductFindAllRequestDto for SQS message");
        }

        log.info("ProductFindAllQueueProducer::produce - producing - \nqueue:{} \nid: {} \nmessage:{}",
                queueUrl,
                correlationId,
                message
        );

        try {
            var sendRequest = SendMessageQueueUtils.buildSendMessageRequest(queueUrl, correlationId, message);
            var response = sqsClient.sendMessage(sendRequest);
            log.info("ProductFindAllQueueProducer::produce - Message sent successfully. queue: {}, correlationId: {}, messageId={}",
                    queueUrl,
                    correlationId,
                    response.messageId()
            );
        } catch (AwsServiceException e) {
            log.error("ProductFindAllQueueProducer::produce - AWS service error. statusCode={}, awsErrorCode={}, requestId={}, message={}",
                    e.statusCode(), e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "n/a",
                    e.requestId(), e.getMessage(), e);
            throw e;
        } catch (SdkClientException e) {
            log.error("ProductFindAllQueueProducer::produce - SDK client error: {}", e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            log.error("ProductFindAllQueueProducer::produce - Unexpected error", e);
            throw e;
        }
    }

    private String getMessage(ProductFindAllRequestDto dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            log.error("ProductFindAllQueueProducer::produce - Error serializing message: {}", e.getMessage(), e);
            return null;
        }
    }

}
