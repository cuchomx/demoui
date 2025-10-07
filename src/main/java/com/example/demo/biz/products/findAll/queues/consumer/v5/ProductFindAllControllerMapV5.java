package com.example.demo.biz.products.findAll.queues.consumer.v5;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.commons.cache.IdempotentRequestCache;
import com.example.demo.biz.products.findAll.queues.consumer.v5.service.IProductFindAllSyncQueueService;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v5/products/map")
public class ProductFindAllControllerMapV5 {

    private static final int DEFAULT_LIMIT = 10;
    private static final int DEFAULT_OFFSET = 0;
    private static final int MIN_LIMIT = 1;
    private static final int MIN_OFFSET = 0;

    private final IProductFindAllQueueProducer productFindAllQueueProducer;

    private final IProductFindAllSyncQueueService iProductFindAllSyncQueueService;

    private final Map<String, Object> locks = new ConcurrentHashMap<>();
    private final Map<String, Boolean> completedStates = new ConcurrentHashMap<>();

    @GetMapping
    public HttpEntity<List<ProductResponseDto>> findAllRequestThread(
            @RequestParam("uuid") String correlationId,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset
    ) {
        log.info("ProductFindAllControllerV5::findAll - New request with correlationId: {}", correlationId);

        if (ParameterValidationUtils.isNotValidCorrelationId(correlationId)) {
            log.error("ProductFindAllControllerV5::findAll - Invalid correlation id value - {}", correlationId);
            return ResponseEntity.badRequest().body(List.of());
        }

        if (IdempotentRequestCache.INSTANCE.isInProgress(correlationId)) {
            log.info("ProductFindAllControllerV5::findAll - correlationId {} is in progress", correlationId);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
        }

        IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.RECEIVED);

        try {
            // produce
            log.info("ProductFindAllControllerV5::findAll - Producing for correlationId: {}", correlationId);
            ProductFindAllRequestDto requestDto = buildRequestDto(correlationId, limit, offset);
            productFindAllQueueProducer.produce(correlationId, requestDto);

            // consume
            log.info("ProductFindAllControllerV5::findAll - Consuming for correlationId: {}", correlationId);
//            List<ProductResponseDto> products = iProductFindAllSyncQueueService.consume(correlationId);

            List<ProductResponseDto> products = fetchProductsAsync(correlationId);

            IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.COMPLETED);

            if (products == null || products.isEmpty()) {
                log.warn("ProductFindAllControllerV5::findAll - No products found for correlationId: {} returning accepted", correlationId);
                return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
            }

            log.info("ProductFindAllControllerV5::findAll - Received {} products for correlationId: {}",
                    products.size(),
                    correlationId
            );

            log.info("ProductFindAllControllerV5::findAll - Returning {} products for correlationId: {}", products.size(), correlationId);
            return ResponseEntity.ok(products);
        } catch (Exception e) {
            log.error("ProductFindAllControllerV5::findAll - exception - Error producing/consuming for {}", correlationId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(List.of());
        } finally {
            log.info("ProductFindAllControllerV5::findAll - finally | done - Removing from cache for correlationId={}", correlationId);
            IdempotentRequestCache.INSTANCE.remove(correlationId);
        }
    }

    private List<ProductResponseDto> fetchProductsAsync(String correlationId) throws TimeoutException {
        AtomicReference<List<ProductResponseDto>> products = new AtomicReference<>();
        new Thread(() -> {
            try {
                List<ProductResponseDto> result = iProductFindAllSyncQueueService.consume(correlationId);
                products.set(result);
            } catch (Exception e) {
                log.error("ProductFindAllControllerV5::fetchProductsAsync - Error consuming for correlationId: {}", correlationId, e);
            } finally {
                unlock(correlationId);
            }
        }, "find-all-consumer-thread").start();
        lock(correlationId);
        return products.get();
    }

    private void unlock(String correlationId) {
        log.info("ProductFindAllControllerV5::unlock - correlationId: {}, Unlocking for thread: {}", correlationId, Thread.currentThread());
        Object lock = locks.computeIfAbsent(correlationId, k -> new Object());
        synchronized (lock) {
            completedStates.put(correlationId, true);
            lock.notifyAll();
        }
    }

    private void lock(String correlationId) throws TimeoutException {
        log.info("ProductFindAllControllerV5::lock - correlationId: {}, Locking for thread: {}", correlationId, Thread.currentThread());

        Object lock = locks.computeIfAbsent(correlationId, k -> new Object());
        completedStates.put(correlationId, false);

        synchronized (lock) {
            long timeoutMillis = 30_000;
            long startTime = System.currentTimeMillis();
            while (!completedStates.getOrDefault(correlationId, false)) {
                log.info("ProductFindAllControllerV5::lock - Waiting for response - correlationId: {}", correlationId);
                try {
                    lock.wait(timeoutMillis);
                    if (!completedStates.getOrDefault(correlationId, false) && (System.currentTimeMillis() - startTime) >= timeoutMillis) {
                        log.error("ProductFindAllControllerV5::lock - Timeout waiting for response for correlationId: {}", correlationId);
                        throw new TimeoutException("Lock wait timed out for correlationId: " + correlationId);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("ProductFindAllControllerV5::lock - Thread Interrupted while waiting for correlationId: {}", correlationId, e);
                    throw new RuntimeException("Interrupted while waiting for lock", e);
                }
            }
        }
    }

    private ProductFindAllRequestDto buildRequestDto(String correlationId, Integer limit, Integer offset) {
        int normalizedLimit = (limit == null || limit < MIN_LIMIT) ? DEFAULT_LIMIT : limit;
        int normalizedOffset = (offset == null || offset < MIN_OFFSET) ? DEFAULT_OFFSET : offset;
        return new ProductFindAllRequestDto(correlationId, normalizedLimit, normalizedOffset);
    }

}
