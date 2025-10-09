package com.example.demo.biz.products.findAll.queues.consumer.v6.caches;

import com.example.commons.dto.create.ProductResponseDto;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@Slf4j
public enum LockCacheService {

    INSTANCE;

    private final Map<String, LockState> lockStates = new ConcurrentHashMap<>();

    private final Set<String> correlations = new HashSet<>();


    private static class LockState {
        final Object lock = new Object();
        volatile boolean completed = false;
        List<ProductResponseDto> products;

        private boolean failed;
        private String errorMessage;
    }

    public void lock(String correlationId) throws TimeoutException, InterruptedException {
        log.info("LockCacheService::lock - correlationId: {}, Locking for thread: {}", correlationId, Thread.currentThread().getName());

        LockState lockState = lockStates.computeIfAbsent(correlationId, k -> new LockState());

        try {
            synchronized (lockState.lock) {
                long timeoutMillis = 30_000;
                long startTime = System.currentTimeMillis();
                long remainingTime = timeoutMillis;

                while (!lockState.completed) {
                    if (remainingTime <= 0) {
                        log.error("LockCacheService::lock - Timeout waiting for response for correlationId: {}", correlationId);
                        throw new TimeoutException("Lock wait timed out for correlationId: " + correlationId);
                    }

                    log.debug("LockCacheService::lock - Waiting for response - correlationId: {}, remaining time: {}ms", correlationId, remainingTime);

                    lockState.lock.wait(remainingTime);

                    long elapsed = System.currentTimeMillis() - startTime;
                    remainingTime = timeoutMillis - elapsed;
                }

                log.info("LockCacheService::lock - Received signal for correlationId: {}", correlationId);
            }
        } finally {
            lockStates.remove(correlationId);
            log.debug("LockCacheService::lock - Cleaned up lock state for correlationId: {}", correlationId);
        }
    }

    public void unlock(String correlationId) {
        log.info("LockCacheService::unlock - correlationId: {}, Unlocking for thread: {}", correlationId, Thread.currentThread().getName());

        LockState lockState = lockStates.get(correlationId);

        if (lockState == null) {
            log.warn("LockCacheService::unlock - No lock found for correlationId: {}. It may have already timed out or been cleaned up.", correlationId);
            return;
        }

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
        if (lockState == null) {
            log.warn("LockCacheService::getProducts - No lock found for correlationId: {}", correlationId);
            return null;
        }
        if (lockState.products == null) {
            log.warn("LockCacheService::getProducts - No products found for correlationId: {}", correlationId);
            return null;
        }
        log.info("LockCacheService::getProducts - Returning {} products for correlationId: {}", lockState.products.size(), correlationId);
        return lockState.products;
    }

    public Set<String> getCorrelationsList() {
        lockStates.entrySet().stream()
                .filter(entry -> !entry.getValue().completed)
                .map(Map.Entry::getKey)
                .filter(id -> !correlations.contains(id))
                .peek(id -> log.info("getUnprocessedRequestList - Unprocessed request: {}", id))
                .forEach(correlations::add);
        return correlations;
    }

    public void updateCorrelationList(String correlation) {
        correlations.remove(correlation);
    }

    public void remove(String correlationId) {
        log.info("LockCacheService::remove - correlationId: {}, Removing from cache for thread: {}", correlationId, Thread.currentThread().getName());
        lockStates.remove(correlationId);
    }

}