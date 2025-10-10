package com.example.demo.biz.products.findAll.queues.consumer.v9.service;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.v9.cache.LockingV9CacheService;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
@Service
public class ProductFindAllSqsQueueV9Service {

    private final ExecutorService virtualExecutor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("find-all-consumer-v9-", 0).factory());

    public CompletableFuture<List<ProductResponseDto>> waitForResult(String correlationId, Long timeout) {
        try {
            CompletableFuture<List<ProductResponseDto>> completableFuture = CompletableFuture
                    .supplyAsync(() -> LockingV9CacheService.INSTANCE.getProducts(correlationId), virtualExecutor);

            completableFuture.orTimeout(timeout == null ? 20 : timeout, TimeUnit.SECONDS);

            completableFuture.exceptionally(e -> {
                log.error("consume - Error consuming, returning empty list for products", e);
                LockingV9CacheService.INSTANCE.fail(correlationId, e.getMessage());
                return null;
            });

            return completableFuture;
        } catch (Exception e) {
            log.error("consume - Error consuming, returning empty list for products", e);
            LockingV9CacheService.INSTANCE.fail(correlationId, e.getMessage());
            return null;
        }
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
