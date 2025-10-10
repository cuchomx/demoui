package com.example.demo.biz.products.findAll.queues.consumer.v9.cache;

import com.example.commons.dto.create.ProductResponseDto;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    }

    public void putIfAbsent(String correlationId, List<ProductResponseDto> products) {
        log.info("putIfAbsent - correlationId: {}, Adding to cache for thread: {}", correlationId, Thread.currentThread().getName());
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
        remove(correlationId);
    }

    public void fail(String correlationId, String errorMessage) {
        log.info("fail - correlationId: {}, with error: {}, thread: {}", correlationId, errorMessage, Thread.currentThread().getName());
        remove(correlationId);
    }

    private void remove(String correlationId) {
        log.info("remove - correlationId: {}, Removing from cache for thread: {}", correlationId, Thread.currentThread().getName());
        cache.remove(correlationId);
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
        log.info("setProducts - correlationId: {}, Setting products for thread: {}", correlationId, Thread.currentThread().getName());
        LockStateVariables lockState = LockingV9CacheService.INSTANCE.cache.get(correlationId);
        lockState.setProducts(list);
    }

}