package com.example.demo.biz.products.findAll.queues.consumer.v7.service;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.v7.caches.LockingV7CacheService;
import com.example.demo.biz.products.findAll.queues.consumer.v7.consumer.SqsSyncV7QueueConsumer;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor

@Service
public class ProductFindAllSqsQueueV7Service {

    private final ExecutorService virtualExecutor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("find-all-consumer-v7-", 0).factory());

    @Value("${products.findAll.v5.timeout.seconds:20}")
    private int timeoutSeconds;

    //    @Scheduled(fixedRate = 1_000)
    public void consume() {
        log.debug("queue::consume - Polling SQS queue");

        CompletableFuture
                .supplyAsync(() -> {
                    try (var sqsConsumer = new SqsSyncV7QueueConsumer()) {
                        return sqsConsumer.consume();
                    }
                }, virtualExecutor)
                .thenCompose(future -> future)
                .whenComplete((operationResults, throwable) -> {
                    if (throwable != null) {
                        log.error("queue::consume - Timeout or error consuming, returning empty list for products", throwable);
                        return;
                    }
                    if (operationResults != null) {
                        log.info("queue::consume - Returning {} results", operationResults.size());
                        operationResults.forEach(e -> {
                            var correlationId = e.correlationId();

                            if (!LockingV7CacheService.INSTANCE.hasCorrelationId(correlationId)) {
                                log.warn("queue::consume - Received result for inactive correlationId: {}", correlationId);
                                return;
                            }

                            var products = e.products();
                            var success = e.success();
                            var message = e.message();

                            log.info("queue::consume - correlationId: {}, products:{} success: {}, message: {}", correlationId, products, success, message);
                            try {
                                if (success) {
                                    log.info("queue::consume - complete for correlationId: {}", correlationId);
                                    logEachProduct(correlationId, products);
                                    LockingV7CacheService.INSTANCE.complete(correlationId, products);
                                } else {
                                    log.error("queue::consume - fail for correlationId: {}", correlationId);
                                    LockingV7CacheService.INSTANCE.fail(correlationId, message);
                                }
                            } finally {
                                log.info("queue::consume - unlock for correlationId: {}", correlationId);
                                LockingV7CacheService.INSTANCE.unlock(correlationId);
                            }
                        });
                    }
                })
                .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    log.error("queue::consume - Error consuming, returning empty list for products", e);
                    return null;
                })
        ;
    }

    private void logEachProduct(String correlationId, List<ProductResponseDto> products) {
        log.info("___________________________________________________________________________________________");
        log.info("queue::logEachProduct - logging - correlationId: {}, products.size(): {}", correlationId, products.size());
        products.stream()
                .filter(Objects::nonNull)
                .forEach(product -> log.info("product: {}", product));
        log.info("___________________________________________________________________________________________");
    }

    @PreDestroy
    void shutdownExecutor() {
        try {
            virtualExecutor.shutdown();
            if (!virtualExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                virtualExecutor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            virtualExecutor.shutdownNow();
        }
    }
}
