package com.example.demo.biz.products.findAll.queues.consumer.v9.controller;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.commons.cache.IdempotentRequestCache;
import com.example.demo.biz.products.findAll.queues.consumer.v9.cache.LockingV9CacheService;
import com.example.demo.biz.products.findAll.queues.consumer.v9.service.ProductFindAllSqsQueueV9Service;
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
import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v9/products")
public class ProductFindAllSqsV9RestController {

    private final IProductFindAllQueueProducer productFindAllQueueProducer;

    private final ProductFindAllSqsQueueV9Service productFindAllSqsQueueV9Service;

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

            // mark completed only when we have a definitive result
            IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.COMPLETED);


            // get products
            log.info("findAll - Getting products for correlationId: {}", correlationId);
            List<ProductResponseDto> products = productFindAllSqsQueueV9Service.waitForResult(correlationId, 10L);
            if (products == null) {
                log.warn("findAll - null products - No products found for correlationId: {} returning no content", correlationId);
                return ResponseEntity.status(HttpStatus.NO_CONTENT).body(List.of());
            }

            logEachProduct(correlationId, products);

            log.info("findAll - Returning {} products for correlationId: {}", products.size(), correlationId);
            return ResponseEntity.ok(products);
        } catch (Exception e) {
            log.error("findAll - exception - Error producing/consuming for {}", correlationId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(List.of());
        } finally {
            log.info("findAll - finally | done - Removing from cache for correlationId={}", correlationId);
            IdempotentRequestCache.INSTANCE.remove(correlationId);
            LockingV9CacheService.INSTANCE.complete(correlationId);
        }
    }

    private ProductFindAllRequestDto buildRequestDto(String correlationId, Integer limit, Integer offset) {
        int normalizedLimit = (limit == null || limit < 1) ? 10 : limit;
        int normalizedOffset = (offset == null || offset < 0) ? 0 : offset;
        return new ProductFindAllRequestDto(correlationId, normalizedLimit, normalizedOffset);
    }

    private void logEachProduct(String correlationId, List<ProductResponseDto> products) {
        log.info("___________________________________________________________________________________________");
        log.info("logEachProduct - logging - correlationId: {}, products.size(): {}", correlationId, products.size());
        products.stream()
                .filter(Objects::nonNull)
                .forEach(product -> log.info("product: {}", product));
        log.info("___________________________________________________________________________________________");
    }

}
