package com.example.demo.biz.products.findAll.queues.consumer.v7.caches;

import com.example.commons.dto.create.ProductResponseDto;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@Slf4j
public enum LockingV7CacheService {

    INSTANCE;

    private final Map<String, LockState> lockStates = new ConcurrentHashMap<>();

    private final Set<String> correlations = new HashSet<>();

    public void addCompletableFuture(CompletableFuture<List<ProductResponseDto>> futureResponse) {
    }


    @Data
    private static class LockState {
        final Object lock = new Object();
        volatile boolean completed = false;
        volatile boolean failed = false;
        volatile String errorMessage;
        volatile List<ProductResponseDto> products;
    }

    public List<ProductResponseDto> lock(String correlationId) throws TimeoutException, InterruptedException {
        log.info("LockCacheService::lock - correlationId: {}, Locking for thread: {}", correlationId, Thread.currentThread().getName());

        LockState lockState = lockStates.computeIfAbsent(correlationId, k -> new LockState());

        synchronized (lockState.lock) {
            long timeoutMillis = 30_000;
            long startTime = System.currentTimeMillis();
            long remainingTime = timeoutMillis;

            while (!lockState.completed && remainingTime > 0) {
                log.debug("LockCacheService::lock - Waiting for response - correlationId: {}, remaining time: {}ms", correlationId, remainingTime);
                lockState.lock.wait(remainingTime);
                long elapsed = System.currentTimeMillis() - startTime;
                remainingTime = timeoutMillis - elapsed;
            }

            if (!lockState.completed) {
                log.error("LockCacheService::lock - Timeout waiting for response for correlationId: {}", correlationId);
                throw new TimeoutException("Lock wait timed out for correlationId: " + correlationId);
            }

            if (lockState.failed) {
                throw new TimeoutException("Request failed for correlationId: " + correlationId + " error=" + lockState.errorMessage);
            }

            log.info("LockCacheService::lock - Received signal for correlationId: {}", correlationId);
            // Return the products directly to avoid race with external cache
            return lockState.products;
        }
    }

    public void unlock(String correlationId) {
        log.info("LockCacheService::unlock - correlationId: {}, Unlocking for thread: {}", correlationId, Thread.currentThread().getName());

        LockState lockState = lockStates.get(correlationId);

        if (lockState == null) {
            log.warn("LockCacheService::unlock - No lock found for correlationId: {}. It may have already timed out or been cleaned up.", correlationId);
            return;
        }

        if (lockState.completed) {
            log.warn("LockCacheService::unlock - Lock already completed for correlationId: {}", correlationId);
            return;
        }

        log.info("LockCacheService::unlock - Notifying all waiting threads for correlationId: {}", correlationId);

        synchronized (lockState.lock) {
            lockState.completed = true;
            lockState.lock.notifyAll();
            log.debug("LockCacheService::unlock - Notified all waiting threads for correlationId: {}", correlationId);
            updateCorrelationList(correlationId);
        }
    }

    public List<ProductResponseDto> getProducts(String correlationId) {
        log.info("LockCacheService::getProducts - correlationId: {}, Getting products for thread: {}", correlationId, Thread.currentThread().getName());
        LockState lockState = lockStates.get(correlationId);
        if (lockState == null || lockState.products == null) {
            log.warn("LockCacheService::getProducts - No products found for correlationId: {}", correlationId);
            return null;
        }
        log.info("LockCacheService::getProducts - Returning products for correlationId: {}", correlationId);
        return lockState.products;
    }

    public void setProducts(String correlationId, List<ProductResponseDto> products) {
        log.info("LockCacheService::setProducts - correlationId: {}, Setting products for thread: {}", correlationId, Thread.currentThread().getName());

        if (products == null) {
            log.warn("LockCacheService::setProducts - No products found for correlationId: {}", correlationId);
            return;
        }

        LockState lockState = lockStates.get(correlationId);
        if (lockState == null) {
            log.warn("LockCacheService::setProducts - No lock found for correlationId: {}", correlationId);
            return;
        }

        log.info("LockCacheService::setProducts - Setting products {} for correlationId: {}", products.size(), correlationId);
        lockState.products = products;
    }

    public void complete(String correlationId, List<ProductResponseDto> products) {
        log.info("LockCacheService::complete - correlationId: {}, with {} products, thread: {}",
                correlationId,
                products == null ? 0 : products.size(),
                Thread.currentThread().getName()
        );
        setProducts(correlationId, products);
        unlock(correlationId);
    }

    public void fail(String correlationId, String errorMessage) {
        LockState lockState = lockStates.computeIfAbsent(correlationId, k -> new LockState());
        synchronized (lockState.lock) {
            lockState.failed = true;
            lockState.errorMessage = errorMessage;
            lockState.completed = true;
            lockState.lock.notifyAll();
        }
        updateCorrelationList(correlationId);
    }

    public void remove(String correlationId) {
        log.info("LockCacheService::remove - correlationId: {}, Removing from cache for thread: {}", correlationId, Thread.currentThread().getName());
        lockStates.remove(correlationId);
        updateCorrelationList(correlationId);
    }

    public boolean hasCorrelationId(String correlationId) {
        return lockStates.containsKey(correlationId);
    }

    public void updateCorrelationList(String correlation) {
        correlations.remove(correlation);
    }

}