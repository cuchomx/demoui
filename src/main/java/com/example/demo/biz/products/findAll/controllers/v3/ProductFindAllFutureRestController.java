package com.example.demo.biz.products.findAll.controllers.v3;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.commons.cache.IdempotentRequestCache;
import com.example.demo.biz.products.findAll.queues.consumer.v3.IProductFindAllV3QueueConsumer;
import com.example.demo.biz.products.findAll.queues.producer.IProductFindAllQueueProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v3/products")
public class ProductFindAllFutureRestController {

    private final IProductFindAllQueueProducer productFindAllQueueProducer;

    private final IProductFindAllV3QueueConsumer productFindAllV3QueueConsumer;

    private static final int DEFAULT_LIMIT = 10;
    private static final int DEFAULT_OFFSET = 0;
    private static final int MIN_LIMIT = 1;
    private static final int MIN_OFFSET = 0;
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(12);

    @GetMapping
    public HttpEntity<?> findAll(
            @RequestParam("uuid") String correlationId,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset
    ) {
        log.info("ProductFindAllFutureRestController::findAll - Request UUID: {}", correlationId);

        if (ParameterValidationUtils.isNotValidCorrelationId(correlationId)) {
            log.error("ProductFindAllFutureRestController::findAll - Invalid correlation id value - {}", correlationId);
            return ResponseEntity.badRequest().body("Invalid correlationId");
        }

        int safeLimit = normalizeLimit(limit);
        int safeOffset = normalizeOffset(offset);

        if (IdempotentRequestCache.INSTANCE.isInProgress(correlationId)) {
            log.info("ProductFindAllFutureRestController::findAll - correlationId {} is in progress", correlationId);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("Processing");
        }

        log.info("ProductFindAllFutureRestController::findAll - Generated from product UUID: {}", correlationId);
        IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.RECEIVED);

        ProductFindAllRequestDto requestDto = buildRequestDto(correlationId, safeLimit, safeOffset);
        try {
            log.info("ProductFindAllFutureRestController::findAll - Producing for correlationId: {}", correlationId);
            productFindAllQueueProducer.produce(correlationId, requestDto);
            log.info("ProductFindAllFutureRestController::findAll - Consuming for correlationId: {}", correlationId);
            productFindAllV3QueueConsumer.consume(correlationId);
        } catch (Exception e) {
            log.error("ProductFindAllFutureRestController::findAll - Error producing/consuming for {}", correlationId, e);
            IdempotentRequestCache.INSTANCE.remove(correlationId);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to enqueue request");
        }

        log.info("ProductFindAllFutureRestController::findAll - Waiting for response for correlationId: {}", correlationId);
        IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.PROCESSING);

        try {
            CompletableFuture<List<ProductResponseDto>> responseFuture = ThreadMapCacheService.createIfAbsent(correlationId);
            List<ProductResponseDto> response = responseFuture
                    .orTimeout(RESPONSE_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                    .join();
            log.info("ProductFindAllFutureRestController::findAll - Response UUID: {} - response.size={}", correlationId, response.size());
            IdempotentRequestCache.INSTANCE.remove(correlationId);
            return ResponseEntity.ok(response);
        } catch (Exception ex) {
            log.error("ProductFindAllFutureRestController::findAll - Timeout/exception for {}", correlationId, ex);
            IdempotentRequestCache.INSTANCE.remove(correlationId);
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).body("Timed out waiting for response");
        } finally {
            log.info("ProductFindAllFutureRestController::findAll - Removing from cache for correlationId: {}", correlationId);
            ThreadMapCacheService.remove(correlationId);
        }
    }

    private int normalizeLimit(Integer limit) {
        int value = (limit == null) ? DEFAULT_LIMIT : limit;
        return Math.max(value, MIN_LIMIT);
    }

    private int normalizeOffset(Integer offset) {
        int value = (offset == null) ? DEFAULT_OFFSET : offset;
        return Math.max(value, MIN_OFFSET);
    }

    private ProductFindAllRequestDto buildRequestDto(String id, Integer limit, Integer offset) {
        return new ProductFindAllRequestDto(
                id,
                limit == null ? DEFAULT_LIMIT : limit,
                offset == null ? DEFAULT_OFFSET : offset
        );
    }
}
