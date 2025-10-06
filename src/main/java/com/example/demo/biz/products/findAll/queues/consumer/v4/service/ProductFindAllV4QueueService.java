package com.example.demo.biz.products.findAll.queues.consumer.v4.service;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.products.findAll.queues.consumer.shared.CallableFutureCacheService;
import com.example.demo.biz.products.findAll.queues.consumer.v4.consumer.IAsyncQueueV4Consumer;
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
public class ProductFindAllV4QueueService implements IProductFindAllV4QueueService {

    private final IAsyncQueueV4Consumer iAsyncQueueV4Consumer;

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
            CallableFutureCacheService.INSTANCE.createIfAbsent(correlationId);

            CompletableFuture<List<ProductResponseDto>> pipeline = CompletableFuture
                    .supplyAsync(() -> consumerService.call(correlationId), VIRTUAL_EXECUTOR)
                    .thenCompose(f -> f)
                    .orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .whenComplete((products, throwable) -> {
                        if (throwable != null) {
                            log.error("ProductFindAllV3QueueConsumer::consume - Failed for correlationId={}: {}", correlationId, throwable.getMessage(), throwable);
                            CallableFutureCacheService.INSTANCE.completeExceptionally(correlationId, throwable);
                        } else {
                            int size = products != null ? products.size() : 0;
                            log.info("ProductFindAllV3QueueConsumer::consume - Processed {} products for correlationId={}", size, correlationId);
                            if (products != null && !products.isEmpty()) {
                                products.stream().limit(5).forEach(p -> log.debug("ProductFindAllV3QueueConsumer::consume - product: {}", p));
                            }
                            CallableFutureCacheService.INSTANCE.complete(correlationId, products != null ? products : List.of());
                        }
                    });

            log.info("ProductFindAllV3QueueConsumer::consume - Waiting up to {}s for future for correlationId={}", TIMEOUT_SECONDS, correlationId);

            try {
                return pipeline.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("ProductFindAllV3QueueConsumer::consume - Timeout or error awaiting pipeline for correlationId={}: {}", correlationId, e.getMessage(), e);
                return List.of();
            }

        } catch (Exception e) {
            log.error("ProductFindAllV3QueueConsumer::consume - Unexpected exception for correlationId={}: {}", correlationId, e.getMessage(), e);
            CallableFutureCacheService.INSTANCE.completeExceptionally(correlationId, e);
            return List.of();
        }
    }

}
