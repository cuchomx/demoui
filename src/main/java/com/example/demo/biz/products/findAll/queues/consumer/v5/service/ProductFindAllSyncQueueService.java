package com.example.demo.biz.products.findAll.queues.consumer.v5.service;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.v5.consumer.SyncQueueConsumer;
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

@RequiredArgsConstructor
@Service
@Slf4j
public class ProductFindAllSyncQueueService implements IProductFindAllSyncQueueService {

    private final ExecutorService virtualExecutor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("find-all-v5-", 0).factory());

    @Value("${products.findAll.v5.timeout.seconds:10}")
    private int timeoutSeconds;

    @Override
    public List<ProductResponseDto> consume(String correlationId) {
        log.info("ProductFindAllSyncQueueService::consume - Polling SQS queue for correlationId={}", correlationId);

        var futureResponse = CompletableFuture
                .supplyAsync(() -> new SyncQueueConsumer().consume(correlationId), virtualExecutor)
                .thenCompose(f -> f)
                .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    log.error("ProductFindAllSyncQueueService::consume - Timeout or error waiting for response for correlationId={}", correlationId, e);
                    return List.of();
                })
                .thenApply(this::ensureProductsOrEmpty);

        try {
            var products = futureResponse.get();
            if (products.isEmpty()) {
                log.warn("ProductFindAllSyncQueueService::consume - No products yet for correlationId={}", correlationId);
                return List.of();
            }
            log.info("ProductFindAllSyncQueueService::consume - Returning {} products for correlationId={}", products.size(), correlationId);
            return products;
        } catch (Exception e) {
            log.error("ProductFindAllSyncQueueService::consume - Unexpected error for correlationId={}", correlationId, e);
            return List.of();
        }
    }

    private List<ProductResponseDto> ensureProductsOrEmpty(List<ProductResponseDto> products) {
        if (products == null || products.isEmpty()) {
            return List.of();
        }
        logEachProduct(products);
        return products;
    }

    private void logEachProduct(List<ProductResponseDto> products) {
        products.stream()
                .filter(Objects::nonNull)
                .forEach(product -> log.info("ProductFindAllSyncQueueService::consume - product: {}", product));
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
