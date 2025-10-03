package com.example.demo.biz.products.findAll.queues.consumer.v3;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.controllers.v3.ThreadMapCacheService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@Service
@Slf4j
public class ProductFindAllV3QueueConsumer implements IProductFindAllV3QueueConsumer {

    private final ObjectMapper objectMapper;

    private final SqsClient sqsClient;

    private static final Executor VIRTUAL_EXECUTOR =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("find-all-v3-", 0).factory());

    @Value("${aws.sqs.queue.find.web.consumer.url}")
    private String queueUrl;

    @Override
    public List<ProductResponseDto> consume(String correlationId) {

        AsyncQueueConsumerService consumerService = new AsyncQueueConsumerService(objectMapper, sqsClient);

        try {
            CompletableFuture<List<ProductResponseDto>> future = CompletableFuture
                    .supplyAsync(() -> consumerService.call(correlationId), VIRTUAL_EXECUTOR)
                    .orTimeout(10, TimeUnit.of(ChronoUnit.SECONDS))
                    .thenCompose(cf -> cf);

            future.whenComplete((products, throwable) -> {
                if (throwable != null) {
                    log.error("ProductFindAllV3QueueConsumer::consume - Failed for correlationId={}: {}", correlationId, throwable.getMessage(), throwable);
                    ThreadMapCacheService.completeExceptionally(correlationId, throwable);
                } else {
                    if (products == null) {
                        var npe = new NullPointerException("Null products result");
                        log.error("ProductFindAllV3QueueConsumer::consume - Null result for correlationId={}", correlationId, npe);
                        ThreadMapCacheService.completeExceptionally(correlationId, npe);
                    } else {
                        ThreadMapCacheService.complete(correlationId, products);
                    }
                }
            });

            return List.of();
        } catch (Exception e) {
            log.error("ProductFindAllV3QueueConsumer::consume - Unexpected exception for correlationId={}: {}", correlationId, e.getMessage(), e);
            ThreadMapCacheService.completeExceptionally(correlationId, e);
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
            log.info("ProductFindAllV3QueueConsumer::delete - Deleted message with receiptHandle={}", receiptHandle);
        } catch (Exception e) {
            log.error("ProductFindAllV3QueueConsumer::delete - Exception deleting receiptHandle={}: {}",
                    receiptHandle, e.getMessage(), e);
        }
    }
}
