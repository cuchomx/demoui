package com.example.demo.biz.products.findAll.queues.consumer.v6.service;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.v5.consumer.SyncQueueConsumer;
import com.example.demo.biz.products.findAll.queues.consumer.v6.caches.LockCacheService;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
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
public class ProductFindAllSqsQueueService {

    private final ExecutorService virtualExecutor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("find-all-consumer-", 0).factory());

    @Value("${products.findAll.v5.timeout.seconds:20}")
    private int timeoutSeconds;

    @Scheduled(fixedRate = 1_000)
    public void collectProducts() {

        var correlations = LockCacheService.INSTANCE.getCorrelationsList();
        for (String correlationId : correlations) {
            virtualExecutor.execute(() -> {
                var products = consume(correlationId);
                if (!products.isEmpty()) {
                    LockCacheService.INSTANCE.unlock(correlationId);
                }
            });
        }

    }

    public List<ProductResponseDto> consume(String correlationId) {
        log.info("queue::consume - Polling SQS queue for correlationId={}", correlationId);

        var futureResponse = CompletableFuture
                .supplyAsync(() -> {
                    try (var consumer = new SyncQueueConsumer()) {
                        return consumer.consume(correlationId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, virtualExecutor)
                .thenApply(CompletableFuture::join)
                .exceptionally(e -> {
                    log.error("queue::consume - Error consuming for correlationId: {}", correlationId, e);
                    return List.of();
                })
                .completeOnTimeout(List.of(), timeoutSeconds, TimeUnit.SECONDS);

        log.info("queue::consume - Get ! - collecting products for correlationId: {}", correlationId);

        try {
            var products = futureResponse.get();
            log.info("queue::consume - Returning {} products for correlationId: {}", products.size(), correlationId);
            logEachProduct(products);
            return products;
        } catch (Exception e) {
            log.error("queue::consume - Unexpected error for correlationId: {}", correlationId, e);
            return List.of();
        }
    }

    private void logEachProduct(List<ProductResponseDto> products) {
        log.info("queue::logEachProduct - logging each product, total: {}", products.size());
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
