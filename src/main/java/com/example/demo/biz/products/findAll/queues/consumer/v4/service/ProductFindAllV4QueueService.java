package com.example.demo.biz.products.findAll.queues.consumer.v4.service;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.v4.consumer.IAsyncQueueV4Consumer;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@Service
@Slf4j
public class ProductFindAllV4QueueService implements IProductFindAllV4QueueService {

    private static final String LOG_CTX = "ProductFindAllV4QueueService::consume - ";

    private final IAsyncQueueV4Consumer<List<ProductResponseDto>, String> iAsyncQueueV4Consumer;

    private static final ExecutorService VIRTUAL_EXECUTOR =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("find-all-v4-", 0).factory());

    @Value("${aws.sqs.queue.find.web.consumer.url}")
    private String queueUrl;

    @Override
    public List<ProductResponseDto> consume(String correlationId) {

        log.info("{} - Polling SQS queue {} for correlationId={}", LOG_CTX, queueUrl, correlationId);

        try {
            var futureResponse = CompletableFuture
                    .supplyAsync(() -> iAsyncQueueV4Consumer.consume(correlationId), VIRTUAL_EXECUTOR)
                    .thenApply(this::getProcessingResult)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .exceptionally(x -> {
                        log.error("{} - Error while consuming for correlationId={}: {}", LOG_CTX, correlationId, x.getMessage(), x);
                        return List.of();
                    });

            futureResponse.whenComplete((response, throwable) -> {
                if (throwable != null) {
                    log.error("{} - Error while consuming for correlationId={}: {}", LOG_CTX, correlationId, throwable.getMessage(), throwable);
                } else {
                    log.info("{} - Completed for correlationId={}", LOG_CTX, correlationId);
                }
            });

            log.info("{} - Waiting for response for correlationId={}", LOG_CTX, correlationId);
            return futureResponse.get();

        } catch (Exception e) {
            log.error("{} - Unexpected error for correlationId={}: {}", LOG_CTX, correlationId, e.getMessage(), e);
            return List.of();
        } finally {
            log.info("{} - Removing from cache for correlationId={}", LOG_CTX, correlationId);
        }

    }

    private List<ProductResponseDto> getProcessingResult(List<ProductResponseDto> products) {
        if (products == null || products.isEmpty()) {
            return List.of();
        }
        logProducts(products);
        return products;
    }

    private void logProducts(List<ProductResponseDto> products) {
        if (products == null || products.isEmpty()) return;
        products.forEach(p -> log.debug("{}product: {}", LOG_CTX, p));
    }

    @PreDestroy
    void shutdownExecutor() {
        try {
            VIRTUAL_EXECUTOR.shutdown();
            if (!VIRTUAL_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                VIRTUAL_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            VIRTUAL_EXECUTOR.shutdownNow();
        }
    }
}