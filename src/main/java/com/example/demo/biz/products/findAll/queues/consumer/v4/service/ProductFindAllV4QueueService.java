package com.example.demo.biz.products.findAll.queues.consumer.v4.service;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.v4.consumer.IAsyncQueueV4Consumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@Service
@Slf4j
public class ProductFindAllV4QueueService implements IProductFindAllV4QueueService {

    private final IAsyncQueueV4Consumer<List<ProductResponseDto>, String> iAsyncQueueV4Consumer;
    private final ExecutorService virtualExecutor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("find-all-v4-", 0).factory());

    @Value("${products.findAll.v4.timeout.seconds:10}")
    private int timeoutSeconds;

    @Value("${aws.sqs.queue.find.web.consumer.url}")
    private String queueUrl;

    @PostConstruct
    void validateConfiguration() {
        if (queueUrl == null || queueUrl.isBlank()) {
            throw new IllegalStateException("aws.sqs.queue.find.web.consumer.url must be configured");
        }
        try {
            var uri = new URI(queueUrl);
            var scheme = uri.getScheme();
            if (!Objects.equals("http", scheme) && !Objects.equals("https", scheme)) {
                throw new IllegalStateException("Invalid SQS queue URL scheme: " + scheme);
            }
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Invalid SQS queue URL: " + queueUrl, e);
        }
        if (timeoutSeconds < 1) {
            log.warn("ProductFindAllV4QueueService::validateConfiguration - timeoutSeconds {} is too low; defaulting to 35", timeoutSeconds);
            timeoutSeconds = 35;
        }
    }

    @Override
    public List<ProductResponseDto> consume(String correlationId) {
        log.info("ProductFindAllV4QueueService::consume - Polling SQS queue {} for correlationId={}", queueUrl, correlationId);

        var futureResponse = CompletableFuture
                .supplyAsync(() -> iAsyncQueueV4Consumer.consume(correlationId), virtualExecutor)
                .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    log.error("ProductFindAllV4QueueService::consume - Timeout waiting for response for correlationId={}", correlationId, e);
                    return List.of();
                })
                .thenApply(this::getProcessingResult);

        try {
            var products = futureResponse.get();
            if (products == null || products.isEmpty()) {
                log.warn("ProductFindAllV4QueueService::consume - No products yet for correlationId={}", correlationId);
                return List.of();
            }
            log.info("ProductFindAllV4QueueService::consume - Returning {} products for correlationId={}", products.size(), correlationId);
            return products;
        } catch (Exception e) {
            log.error("ProductFindAllV4QueueService::consume - Unexpected error for correlationId={}", correlationId, e);
            return List.of();
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
        products.stream()
                .filter(Objects::nonNull)
                .forEach(product -> log.info("ProductFindAllV4QueueService::consume - product: {}", product));
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
