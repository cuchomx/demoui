package com.example.demo.biz.products.findAll.queues.consumer.v9.cache;

import com.example.commons.dto.create.ProductResponseDto;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@Slf4j
public enum LockingV9CacheService {

    INSTANCE;

    private final Map<String, LockStateVariables> cache = new ConcurrentHashMap<>();

    @Data
    public static class LockStateVariables {

        final Object monitor = new Object();
        volatile boolean completed;

        List<ProductResponseDto> products;
        boolean success;
        String message;

        LocalDateTime unlockTime;
        LocalDateTime lockTime = LocalDateTime.now();
    }

    public void putIfAbsent(String correlationId, List<ProductResponseDto> products) {
        log.info("putIfAbsent - correlationId: {}, adding {} products to cache for thread: {}",
                correlationId,
                products == null ? 0 : products.size(),
                Thread.currentThread().getName());
        var entry = new LockStateVariables();
        entry.setProducts(products);
        entry.setMessage("added-to-cache-by-thread");
        cache.put(correlationId, entry);
    }

    public void complete(String correlationId) {
        log.info("complete - correlationId: {}, thread: {}",
                correlationId,
                Thread.currentThread().getName()
        );

        LockStateVariables lockState = cache.get(correlationId);

        if (lockState == null) {
            log.warn("complete - No lock found for correlationId: {}", correlationId);
            return;
        }
        if (lockState.completed) {
            log.warn("complete - Lock already completed for correlationId: {}", correlationId);
            return;
        }

        lockState.completed = true;
        lockState.success = true;
        lockState.message = "completed-by-thread";

        release(correlationId);
    }

    public void fail(String correlationId, String errorMessage) {
        log.info("fail - correlationId: {}, with error: {}, thread: {}", correlationId, errorMessage, Thread.currentThread().getName());
        LockStateVariables lockState = cache.get(correlationId);

        if (lockState == null) {
            log.warn("fail - No lock found for correlationId: {}", correlationId);
            return;
        }

        lockState.completed = true;
        lockState.success = false;
        lockState.message = errorMessage;

        release(correlationId);
    }

    private void release(String correlationId) {
        unlock(correlationId);
        remove(correlationId);
    }

    private void remove(String correlationId) {
        log.info("remove - correlationId: {}, Removing from cache for thread: {}", correlationId, Thread.currentThread().getName());
        cache.remove(correlationId);
    }

    @Scheduled(fixedDelay = 5 * 60 * 1_000)
    private void clean() {
        log.info("clean - Cleaning cache for thread: {}", Thread.currentThread().getName());
        cache.entrySet().removeIf(entry -> entry.getValue().completed);
        cache.entrySet().removeIf(entry -> entry.getValue().unlockTime.isBefore(LocalDateTime.now().minusMinutes(5)));
    }

    public boolean hasCorrelationId(String correlationId) {
        boolean exists = cache.containsKey(correlationId);
        log.debug("hasCorrelationId - correlationId: {}, in cache:{},  for thread: {}", correlationId, exists, Thread.currentThread().getName());
        return exists;
    }

    public List<ProductResponseDto> getProducts(String correlationId) {
        log.info("getProducts - correlationId: {}, Getting products for thread: {}", correlationId, Thread.currentThread().getName());
        LockStateVariables lockState = cache.get(correlationId);
        if (lockState == null || lockState.products == null) {
            log.warn("getProducts - No products found for correlationId: {}", correlationId);
            return null;
        }
        log.info("getProducts - Returning products for correlationId: {}", correlationId);
        return lockState.products;
    }

    public static void setProducts(String correlationId, List<ProductResponseDto> list) {
        log.info("setProducts - correlationId: {}, Setting products {} for thread: {}",
                correlationId,
                list == null ? 0 : list.size(),
                Thread.currentThread().getName()
        );
        LockStateVariables lockState = LockingV9CacheService.INSTANCE.cache.get(correlationId);
        lockState.setProducts(list);
    }

    public void lock(String correlationId) throws TimeoutException, InterruptedException {
        log.info("lock - correlationId: {}, Locking for thread: {}", correlationId, Thread.currentThread().getName());

        var lockState = cache.computeIfAbsent(correlationId, k -> new LockStateVariables());

        int attempt = 1;

        do {
            log.info("lock - attempt:{}", attempt);

            synchronized (lockState.monitor) {
                log.debug("lock - Waiting for response - correlationId: {}", correlationId);
                lockState.monitor.wait(1_500);
            }

            var products = lockState.products;

            if (products == null) {
                log.warn("lock - No products found for correlationId: {}, attempt:{}", correlationId, attempt);
            } else {
                log.info("lock - Received signal for correlationId: {}, products size : {}", correlationId, products.size());
                break;
            }

        } while (attempt++ < 3 && !lockState.completed);

        if (!lockState.completed) {
            log.error("lock - not completed - a timeout waiting problem for correlationId: {}", correlationId);
        }

        if (!lockState.success) {
            log.error("lock - Request failed for correlationId: {} error={}", correlationId, lockState.message);
        }

    }

    public void unlock(String correlationId) {
        log.info("unlock - correlationId: {}, Unlocking for thread: {}", correlationId, Thread.currentThread().getName());

        var lockState = cache.computeIfAbsent(correlationId, k -> new LockStateVariables());
        lockState.unlockTime = LocalDateTime.now();

        synchronized (lockState.monitor) {
            lockState.monitor.notifyAll();
            log.info("unlock - Notified all waiting threads for correlationId: {}", correlationId);
        }

    }
}