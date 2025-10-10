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

    public List<ProductResponseDto> waitForResult(String correlationId, Long timeout) {
        log.info("waitForResult - correlationId: {}, timeout: {}", correlationId, timeout);

        try {
            LockingV9CacheService.INSTANCE.lock(correlationId);

            CompletableFuture<List<ProductResponseDto>> completableFuture = CompletableFuture
                    .supplyAsync(() -> {
                        log.info("waitForResult - Getting products for correlationId: {}", correlationId);
                        return LockingV9CacheService.INSTANCE.getProducts(correlationId);
                    }, virtualExecutor)
                    .thenApply(products -> {
                        if (products == null) {
                            log.warn("waitForResult - No products found for correlationId: {}", correlationId);
                            return List.of();
                        }
                        log.info("waitForResult - Returning {} products for correlationId: {}", products.size(), correlationId);
                        LockingV9CacheService.INSTANCE.complete(correlationId);
                        return products;
                    });


            completableFuture
                    .orTimeout(timeout == null ? 20 : timeout, TimeUnit.SECONDS)
                    .exceptionally(e -> {
                        log.error("waitForResult - Error consuming, returning empty list for products", e);
                        return null;
                    });

            return completableFuture.get();

        } catch (Exception e) {
            log.error("waitForResult - Error consuming, returning empty list for products", e);
            return null;
        } finally {
            log.info("waitForResult - Unlocking for correlationId: {}", correlationId);
            LockingV9CacheService.INSTANCE.complete(correlationId);
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
