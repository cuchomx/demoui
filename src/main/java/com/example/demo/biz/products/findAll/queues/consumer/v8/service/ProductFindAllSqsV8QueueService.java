package com.example.demo.biz.products.findAll.queues.consumer.v8.service;

import com.example.demo.biz.products.findAll.queues.consumer.v8.caches.LockingV8CacheService;
import com.example.demo.biz.products.findAll.queues.consumer.v8.consumer.SqsSyncQueueV8Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class ProductFindAllSqsV8QueueService {

    final ExecutorService virtualExecutor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("find-all-consumer-v8-", 0).factory());

    public CompletableFuture<LockingV8CacheService.LockStateVariables> consume(String correlationId) {

        log.info("queue::consume - building completable future - consuming products");

        return CompletableFuture
                .supplyAsync(() -> {
                    try (var sqsConsumer = new SqsSyncQueueV8Consumer()) {
                        return sqsConsumer.consume(correlationId);
                    } catch (Exception e) {
                        log.error("queue::consume - Error consuming, returning null list for completable future", e);
                        return null;
                    }
                }, virtualExecutor)
                .thenCompose(future -> future)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("queue::consume - Timeout or error consuming, returning empty list for products", throwable);
                        return;
                    }

                    if (!LockingV8CacheService.INSTANCE.hasCorrelationId(correlationId)) {
                        log.warn("queue::consume - Received result for inactive correlationId: {}", correlationId);
                        return;
                    }

                    var success = result.isSuccess();
                    var message = result.getErrorMessage();
                    var products = result.getProducts();

                    log.info("queue::consume - correlationId: {}, success: {}, message: {}, products:{}", correlationId, success, message, products);

                    if (success) {
                        LockingV8CacheService.INSTANCE.complete(correlationId, products);
                    } else {
                        log.error("queue::consume - fail for correlationId: {}", correlationId);
                        LockingV8CacheService.INSTANCE.fail(correlationId, message);
                    }
                })
                .orTimeout(10, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    log.error("queue::consume - Error consuming, returning null list for completable future", e);
                    return null;
                });
    }

    void shutdownExecutor(ExecutorService virtualExecutor) {
        try {
            virtualExecutor.shutdown();
        } catch (Exception ie) {
            Thread.currentThread().interrupt();
            virtualExecutor.shutdownNow();
        }
    }
}
