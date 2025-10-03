package com.example.demo.biz.products.findAll.controllers.v3;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public enum ThreadMapCacheService {

    INSTANCE;

    private static final Map<String, CompletableFuture<?>> cache = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public static <T> CompletableFuture<T> get(String key) {
        return (CompletableFuture<T>) cache.get(key);
    }

    @SuppressWarnings("unchecked")
    public static <T> CompletableFuture<T> createIfAbsent(String key) {
        return (CompletableFuture<T>) cache.computeIfAbsent(key, k -> new CompletableFuture<>());
    }

    @SuppressWarnings("unchecked")
    public static void complete(String key, Object value) {
        CompletableFuture<Object> future = (CompletableFuture<Object>) cache.remove(key);
        if (future != null) {
            future.complete(value);
        }
    }

    @SuppressWarnings("unchecked")
    public static void completeExceptionally(String key, Throwable t) {
        CompletableFuture<Object> future = (CompletableFuture<Object>) cache.remove(key);
        if (future != null) {
            future.completeExceptionally(t);
        }
    }

    public static void remove(String key) {
        cache.remove(key);
    }
}
