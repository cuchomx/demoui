package com.example.demo.biz.products.findAll.queues.consumer.v4;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.commons.cache.IdempotentRequestCache;
import com.example.demo.biz.products.findAll.queues.consumer.v4.service.IProductFindAllV4QueueService;
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
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v6/products")
public class ProductFindAllV6Controller {

    private static final int DEFAULT_LIMIT = 10;
    private static final int DEFAULT_OFFSET = 0;
    private static final int MIN_LIMIT = 1;
    private static final int MIN_OFFSET = 0;

    private final IProductFindAllQueueProducer productFindAllQueueProducer;

    private final IProductFindAllV4QueueService productFindAllV4QueueService;

    private final Object lock = new Object();
    private volatile boolean completed = true;

    @GetMapping
    public HttpEntity<List<ProductResponseDto>> findAllRequestThread(
            @RequestParam("uuid") String correlationId,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset
    ) {
        log.info("ProductFindAllV6Controller::findAll - Request UUID: {}", correlationId);

        ResponseEntity<List<ProductResponseDto>> validationError = validateRequest(correlationId);
        if (validationError != null) {
            return validationError;
        }

        IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.RECEIVED);

        try {
            // produce
            log.info("ProductFindAllV6Controller::findAll - Producing for correlationId: {}", correlationId);
            ProductFindAllRequestDto requestDto = buildRequestDto(correlationId, limit, offset);
            productFindAllQueueProducer.produce(correlationId, requestDto);
            log.info("ProductFindAllV6Controller::findAll - Produced for correlationId: {}", correlationId);

            // consume
            log.info("ProductFindAllV6Controller::findAll - Consuming for correlationId: {}", correlationId);
            AtomicReference<List<ProductResponseDto>> products = fetchProductsAsync(correlationId);

            log.info("ProductFindAllV6Controller::findAll - Waiting for response for correlationId: {}", correlationId);
            waitForResponse(correlationId);

            log.info("ProductFindAllV6Controller::findAll - Completed for correlationId: {}", correlationId);
            IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.COMPLETED);

            if (products.get() == null || products.get().isEmpty()) {
                log.warn("ProductFindAllV6Controller::findAll - No products yet for correlationId: {} returning accepted", correlationId);
                return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
            }

            log.info("ProductFindAllV6Controller::findAll - Returning {} products for correlationId: {}", products.get().size(), correlationId);
            return ResponseEntity.ok(products.get());
        } catch (Exception e) {
            log.error("ProductFindAllV6Controller::findAll - exception - Error producing/consuming for {}", correlationId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(List.of());
        } finally {
            log.info("ProductFindAllV6Controller::findAll -  finally - Removing from cache for correlationId={}", correlationId);
            IdempotentRequestCache.INSTANCE.remove(correlationId);
        }
    }

    private AtomicReference<List<ProductResponseDto>> fetchProductsAsync(String correlationId) {
        completed = false;
        AtomicReference<List<ProductResponseDto>> products = new AtomicReference<>();
        //new Thread(() -> {
        try {
            List<ProductResponseDto> result = productFindAllV4QueueService.consume(correlationId);
            log.info("ProductFindAllV6Controller::findAll - Received {} products for correlationId: {}", result.size(), correlationId);
            products.set(result);
        } catch (Exception e) {
            log.error("ProductFindAllV6Controller::findAll - Error consuming for correlationId: {}", correlationId, e);
        } finally {
            synchronized (lock) {
                completed = true;
                lock.notifyAll();
            }
        }
        //}, "find-all-consumer-thread").start();
        return products;
    }

    private void waitForResponse(String correlationId) {
        try {
            synchronized (lock) {
                while (!completed) {
                    log.info("ProductFindAllV6Controller::waitForResponse - Waiting for response for correlationId: {}", correlationId);
                    try {
                        lock.wait();
                        log.info("ProductFindAllV6Controller::waitForResponse - Completed for correlationId: {}", correlationId);
                        if (!completed) {
                            log.warn("ProductFindAllV6Controller::waitForResponse - Timeout waiting for response for correlationId: {}", correlationId);
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("ProductFindAllV6Controller::waitForResponse - Thread Interrupted", e);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("ProductFindAllV6Controller::waitForResponse - Error waiting for response for correlationId: {}", correlationId, e);
            throw new RuntimeException(e);
        }
    }

    private ResponseEntity<List<ProductResponseDto>> validateRequest(String correlationId) {
        if (ParameterValidationUtils.isNotValidCorrelationId(correlationId)) {
            log.error("ProductFindAllV6Controller::findAll - Invalid correlation id value - {}", correlationId);
            return ResponseEntity.badRequest().body(List.of());
        }
        if (IdempotentRequestCache.INSTANCE.isInProgress(correlationId)) {
            log.info("ProductFindAllV6Controller::findAll - correlationId {} is in progress", correlationId);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
        }
        return null;
    }

    private ProductFindAllRequestDto buildRequestDto(String correlationId, Integer limit, Integer offset) {
        int normalizedLimit = (limit == null || limit < MIN_LIMIT) ? DEFAULT_LIMIT : limit;
        int normalizedOffset = (offset == null || offset < MIN_OFFSET) ? DEFAULT_OFFSET : offset;
        return new ProductFindAllRequestDto(correlationId, normalizedLimit, normalizedOffset);
    }

}
