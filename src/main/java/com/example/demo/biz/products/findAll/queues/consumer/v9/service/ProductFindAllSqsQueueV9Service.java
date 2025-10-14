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
            Executors.newVirtualThreadPerTaskExecutor();

    public List<ProductResponseDto> waitForResult(String correlationId, Long timeout) {
        log.info("waitForResult - correlationId: {}, timeout: {}", correlationId, timeout);

        long effectiveTimeoutSec = (timeout == null || timeout <= 0) ? 20L : timeout;

        try {
            LockingV9CacheService.INSTANCE.lock(correlationId);

            CompletableFuture<List<ProductResponseDto>> cf = CompletableFuture
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
                        return products;
                    });

            cf = cf
                    .completeOnTimeout(List.of(), effectiveTimeoutSec, TimeUnit.SECONDS)
                    .exceptionally(e -> {
                        log.error("waitForResult - Error consuming, returning empty list for products", e);
                        return List.of();
                    });

            List<ProductResponseDto> result = cf.join();

            LockingV9CacheService.INSTANCE.complete(correlationId);

            return result;

        } catch (Exception e) {
            log.error("waitForResult - Error consuming, returning empty list for products", e);
            return List.of();
        } finally {
            log.info("waitForResult - Unlocking for correlationId: {}", correlationId);
            try {
                LockingV9CacheService.INSTANCE.unlock(correlationId);
            } catch (Exception ex) {
                log.warn("waitForResult - Unlock failed or unnecessary for correlationId: {}", correlationId, ex);
            }
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
