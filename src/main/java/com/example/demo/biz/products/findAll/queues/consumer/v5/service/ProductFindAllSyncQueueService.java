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

    @Value("${products.findAll.v5.timeout.seconds:20}")
    private int timeoutSeconds;

    @Override
    public List<ProductResponseDto> consume(String correlationId) {
        log.info("ProductFindAllSyncQueueService::consume - Polling SQS queue for correlationId={}", correlationId);

        var futureResponse = CompletableFuture
                .supplyAsync(() -> new SyncQueueConsumer().consume(correlationId), virtualExecutor)
                .thenApply(CompletableFuture::join)
                .exceptionally(e -> {
                    log.error("ProductFindAllSyncQueueService::consume - Error consuming for correlationId={}", correlationId, e);
                    return List.of();
                })
                .completeOnTimeout(List.of(), timeoutSeconds, TimeUnit.SECONDS);

        log.info("ProductFindAllSyncQueueService::consume - Get ! - collecting products for correlationId: {}", correlationId);

        try {
            var products = futureResponse.get();
            log.info("ProductFindAllSyncQueueService::consume - Returning {} products for correlationId={}", products.size(), correlationId);
            logEachProduct(products);
            return products;
        } catch (Exception e) {
            log.error("ProductFindAllSyncQueueService::consume - Unexpected error for correlationId={}", correlationId, e);
            return List.of();
        }
    }

    private void logEachProduct(List<ProductResponseDto> products) {
        log.info("ProductFindAllSyncQueueService::logEachProduct - logging each product, total: {}", products.size());
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
