package com.example.demo.biz.products.findAll.queues.consumer.v8.controller;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.commons.cache.IdempotentRequestCache;
import com.example.demo.biz.products.findAll.queues.consumer.v8.caches.LockingV8CacheService;
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

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v8/products")
public class ProductFindAllSqsV8RestController {

    private final IProductFindAllQueueProducer productFindAllQueueProducer;

    @GetMapping
    public HttpEntity<List<ProductResponseDto>> findAllRequestThread(
            @RequestParam("uuid") String correlationId,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset
    ) {
        log.info("findAll - New request with correlationId: {}", correlationId);

        if (ParameterValidationUtils.isNotValidCorrelationId(correlationId)) {
            log.error("findAll - Invalid correlation id value - {}", correlationId);
            return ResponseEntity.badRequest().body(List.of());
        }

        if (IdempotentRequestCache.INSTANCE.isInProgress(correlationId)) {
            log.info("findAll - correlationId {} is in progress", correlationId);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
        }

        IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.RECEIVED);

        try {
            log.info("findAll - Producing for correlationId: {}", correlationId);
            ProductFindAllRequestDto requestDto = buildRequestDto(correlationId, limit, offset);
            productFindAllQueueProducer.produce(correlationId, requestDto);

            // lock !!!
            log.info("findAll - Locking for correlationId: {}", correlationId);
            LockingV8CacheService.INSTANCE.lock(correlationId);

            IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.COMPLETED);

            // get products
            log.info("findAll - Getting products for correlationId: {}", correlationId);
            List<ProductResponseDto> products = LockingV8CacheService.INSTANCE.getProducts(correlationId);

            if (products == null) {
                log.warn("findAll - No products found for correlationId: {} returning accepted", correlationId);
                return ResponseEntity.status(HttpStatus.NO_CONTENT).body(List.of());
            }

            log.info("findAll - Received {} products for correlationId: {}",
                    products.size(),
                    correlationId
            );

            log.info("findAll - Returning {} products for correlationId: {}", products.size(), correlationId);
            return ResponseEntity.ok(products);
        } catch (Exception e) {
            log.error("findAll - exception - Error producing/consuming for {}", correlationId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(List.of());
        } finally {
            log.info("findAll - finally | done - Removing from cache for correlationId={}", correlationId);
            IdempotentRequestCache.INSTANCE.remove(correlationId);
            LockingV8CacheService.INSTANCE.remove(correlationId);
        }
    }

    private ProductFindAllRequestDto buildRequestDto(String correlationId, Integer limit, Integer offset) {
        final int DEFAULT_LIMIT = 10;
        final int DEFAULT_OFFSET = 0;
        final int MIN_LIMIT = 1;
        final int MIN_OFFSET = 0;

        int normalizedLimit = (limit == null || limit < MIN_LIMIT) ? DEFAULT_LIMIT : limit;
        int normalizedOffset = (offset == null || offset < MIN_OFFSET) ? DEFAULT_OFFSET : offset;
        return new ProductFindAllRequestDto(correlationId, normalizedLimit, normalizedOffset);
    }

}
