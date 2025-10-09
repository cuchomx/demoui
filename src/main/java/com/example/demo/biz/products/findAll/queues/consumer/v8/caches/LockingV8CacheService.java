package com.example.demo.biz.products.findAll.queues.consumer.v8.caches;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.v8.service.ProductFindAllSqsV8QueueService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@Slf4j
public enum LockingV8CacheService {

    INSTANCE;

    private final Map<String, LockStateVariables> lockStates = new ConcurrentHashMap<>();

    @Data
    public static class LockStateVariables {

        final Object monitor = new Object();
        volatile boolean completed;

        List<ProductResponseDto> products;

        boolean success;
        String errorMessage;

        CompletableFuture<?> completableFuture;
    }

    public List<ProductResponseDto> lock(
            String correlationId
    ) throws TimeoutException, InterruptedException {

        log.info("lock - correlationId: {}, Locking for thread: {}", correlationId, Thread.currentThread().getName());

        LockStateVariables lockStateVariables = lockStates.computeIfAbsent(correlationId, k -> new LockStateVariables());

        lockStateVariables.completableFuture = new ProductFindAllSqsV8QueueService().consume(correlationId);

        synchronized (lockStateVariables.monitor) {
            long timeoutMillis = 20_000;
            long startTime = System.currentTimeMillis();
            long remainingTime = timeoutMillis;

            while (!lockStateVariables.completed && remainingTime > 0) {
                log.debug("lock - Waiting for response - correlationId: {}, remaining time: {} ms", correlationId, remainingTime);
                lockStateVariables.monitor.wait(remainingTime);
                long elapsed = System.currentTimeMillis() - startTime;
                remainingTime = timeoutMillis - elapsed;
            }

            log.info("lock - Received signal for correlationId: {}", correlationId);

            var products = lockStateVariables.products;
            log.info("lock - Returning products for correlationId: {}, products: {}", correlationId, products);
            return products;
        }

    }

    public void unlock(String correlationId) {
        log.info("unlock - correlationId: {}, Unlocking for thread: {}", correlationId, Thread.currentThread().getName());

        LockStateVariables lockState = lockStates.get(correlationId);

        if (lockState == null) {
            log.warn("unlock - No lock found for correlationId: {}. It may have already timed out or been cleaned up.", correlationId);
            return;
        }

        if (lockState.completed) {
            log.warn("unlock - Lock already completed for correlationId: {}", correlationId);
            return;
        }

        log.info("unlock - Notifying all waiting threads for correlationId: {}", correlationId);

        synchronized (lockState.monitor) {
            lockState.completed = true;
            lockState.monitor.notifyAll();
            log.info("unlock - Notified all waiting threads for correlationId: {}", correlationId);
        }
    }

    public List<ProductResponseDto> getProducts(String correlationId) {
        log.info("getProducts - correlationId: {}, Getting products for thread: {}", correlationId, Thread.currentThread().getName());
        LockStateVariables lockState = lockStates.get(correlationId);
        if (lockState == null || lockState.products == null) {
            log.warn("getProducts - No products found for correlationId: {}", correlationId);
            return null;
        }
        log.info("getProducts - Returning products for correlationId: {}", correlationId);
        return lockState.products;
    }

    public void setProducts(String correlationId, List<ProductResponseDto> products) {
        log.info("setProducts - correlationId: {}, Setting products for thread: {}", correlationId, Thread.currentThread().getName());

        if (products == null) {
            log.warn("setProducts - No products found for correlationId: {}", correlationId);
            return;
        }

        LockStateVariables lockState = lockStates.get(correlationId);
        if (lockState == null) {
            log.warn("setProducts - No lock found for correlationId: {}", correlationId);
            return;
        }

        log.info("setProducts - Setting products {} for correlationId: {}", products.size(), correlationId);
        lockState.products = products;
    }

    public void complete(String correlationId, List<ProductResponseDto> products) {
        log.info("complete - correlationId: {}, with {} products, thread: {}",
                correlationId,
                products == null ? 0 : products.size(),
                Thread.currentThread().getName()
        );
        setProducts(correlationId, products);
        unlock(correlationId);
    }

    public void fail(String correlationId, String errorMessage) {
        LockStateVariables lockState = lockStates.computeIfAbsent(correlationId, k -> new LockStateVariables());
        synchronized (lockState.monitor) {
            lockState.success = false;
            lockState.errorMessage = errorMessage;
            lockState.completed = true;
            lockState.monitor.notifyAll();
        }
    }

    public void remove(String correlationId) {
        log.info("remove - correlationId: {}, Removing from cache for thread: {}", correlationId, Thread.currentThread().getName());
        lockStates.remove(correlationId);
    }

    public boolean hasCorrelationId(String correlationId) {
        return lockStates.containsKey(correlationId);
    }

}