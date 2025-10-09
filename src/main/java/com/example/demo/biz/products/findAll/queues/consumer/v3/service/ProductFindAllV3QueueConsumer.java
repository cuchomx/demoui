package com.example.demo.biz.products.findAll.queues.consumer.v3.service;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.products.findAll.queues.consumer.v3.consumer.AsyncQueueConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@Service
@Slf4j
public class ProductFindAllV3QueueConsumer implements IProductFindAllV3QueueConsumer {

    private final AsyncQueueConsumerService consumerService;

    private static final Executor VIRTUAL_EXECUTOR =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("find-all-v3-", 0).factory());

    @Value("${aws.sqs.queue.find.web.consumer.url}")
    private String queueUrl;

    private static final long TIMEOUT_SECONDS = 10L;

    @Override
    public List<ProductResponseDto> consume(String correlationId) {

        log.info("ProductFindAllV3QueueConsumer::consume - Polling SQS queue {} for correlationId: {}", queueUrl, correlationId);

        if (!ParameterValidationUtils.isValidCorrelationIdValue(correlationId)) {
            log.warn("ProductFindAllV3QueueConsumer::consume - Invalid correlationId");
            return List.of();
        }

        log.info("ProductFindAllV3QueueConsumer::consume - Calling consumerService.call() for correlationId={}", correlationId);

        try {
            // Ensure a waiting future exists for the correlationId

            CompletableFuture<List<ProductResponseDto>> pipeline =
                    CompletableFuture
                            .supplyAsync(() -> consumerService.call(correlationId), VIRTUAL_EXECUTOR)
                            .thenCompose(f -> f)
                            .orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            .whenComplete((products, throwable) -> {
                                if (throwable != null) {
                                    log.error("ProductFindAllV3QueueConsumer::consume - Failed for correlationId={}: {}", correlationId, throwable.getMessage(), throwable);
                                    // CompletableCacheService.INSTANCE.completeExceptionally(correlationId, throwable);
                                } else {
                                    int size = products != null ? products.size() : 0;
                                    log.info("ProductFindAllV3QueueConsumer::consume - Processed {} products for correlationId={}", size, correlationId);
                                    if (products != null && !products.isEmpty()) {
                                        products.stream().limit(5).forEach(p -> log.debug("ProductFindAllV3QueueConsumer::consume - product: {}", p));
                                    }
                                    //CompletableCacheService.INSTANCE.complete(correlationId, products != null ? products : List.of());
                                }
                            });

            log.info("ProductFindAllV3QueueConsumer::consume - Waiting up to {}s for future for correlationId={}", TIMEOUT_SECONDS, correlationId);

            // Synchronous contract: wait and return results (or empty on timeout/error)
            try {
                return pipeline.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("ProductFindAllV3QueueConsumer::consume - Timeout or error awaiting pipeline for correlationId={}: {}", correlationId, e.getMessage(), e);
                return List.of();
            }

        } catch (Exception e) {
            log.error("ProductFindAllV3QueueConsumer::consume - Unexpected exception for correlationId={}: {}", correlationId, e.getMessage(), e);
            //CompletableCacheService.INSTANCE.completeExceptionally(correlationId, e);
            return List.of();
        }
    }

}
