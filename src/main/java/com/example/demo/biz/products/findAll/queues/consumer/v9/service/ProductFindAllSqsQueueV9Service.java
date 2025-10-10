package com.example.demo.biz.products.findAll.queues.consumer.v9.service;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.v9.cache.LockingV9CacheService;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
public class ProductFindAllSqsQueueV9Service {

    private final ExecutorService virtualExecutor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("find-all-consumer-v9-", 0).factory());

    public List<ProductResponseDto> waitForResult(String correlationId, Long timeout) {
        var response =
                CompletableFuture
                        .supplyAsync(() -> LockingV9CacheService.INSTANCE.getProducts(correlationId), virtualExecutor)
                        .whenComplete((products, throwable) -> {
                            if (throwable != null) {
                                log.error("consume - Timeout or error consuming, returning empty list for products", throwable);
                                LockingV9CacheService.INSTANCE.fail(correlationId, throwable.getMessage());
                                return;
                            }
                            if (products != null) {
                                log.info("consume - Returning {} results", products.size());
                                LockingV9CacheService.INSTANCE.complete(correlationId);
                            }
                        })
                        .orTimeout(timeout == null ? 20 : timeout, TimeUnit.SECONDS)
                        .exceptionally(e -> {
                            log.error("consume - Error consuming, returning empty list for products", e);
                            LockingV9CacheService.INSTANCE.fail(correlationId, e.getMessage());
                            return null;
                        });
        try {
            var list = response.get();
            if (list == null) {
                log.info("consume - Returning empty list for products");
                return null;
            }
            logEachProduct(correlationId, list);
            return list;
        } catch (Exception e) {
            log.error("consume - Error consuming, returning empty list for products", e);
            LockingV9CacheService.INSTANCE.fail(correlationId, e.getMessage());
            return null;
        }
    }

    private void logEachProduct(String correlationId, List<ProductResponseDto> products) {
        log.info("___________________________________________________________________________________________");
        log.info("logEachProduct - logging - correlationId: {}, products.size(): {}", correlationId, products.size());
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
