package com.example.demo.biz.products.findAll.queues.consumer.shared;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public enum CallableFutureCacheService {

    INSTANCE;

    private static final Map<String, CompletableFuture<?>> cache = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> get(String key) {
        return (CompletableFuture<T>) cache.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> createIfAbsent(String key) {
        return (CompletableFuture<T>) cache.computeIfAbsent(key, k -> new CompletableFuture<>());
    }

    @SuppressWarnings("unchecked")
    public void complete(String key, Object value) {
        CompletableFuture<Object> future = (CompletableFuture<Object>) cache.remove(key);
        if (future != null) {
            future.complete(value);
        }
    }

    @SuppressWarnings("unchecked")
    public void completeExceptionally(String key, Throwable t) {
        CompletableFuture<Object> future = (CompletableFuture<Object>) cache.remove(key);
        if (future != null) {
            future.completeExceptionally(t);
        }
    }

    public void remove(String key) {
        cache.remove(key);
    }
}
