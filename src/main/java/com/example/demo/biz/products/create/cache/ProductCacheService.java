package com.example.demo.biz.products.create.cache;

import com.example.commons.constants.RequestStatus;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public enum ProductCacheService {

    INSTANCE;

    private static final int DEFAULT_CAPACITY = 128;
    
    private static final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>(DEFAULT_CAPACITY);

    public static void add(String key, RequestStatus value) {
        log.info("Adding key: {} with value: {}", key, value);
        validateKey(key);
        validateValue(value);
        var result = cache.putIfAbsent(key, value);
        log.info("Added key: {} with value: {}, previous value: {}", key, value, result);
    }

    public static Object get(String key) {
        log.info("Getting value for key: {}", key);
        validateKey(key);
        var result = cache.get(key);
        log.info("Got value: {} for key: {}", result, key);
        return result;
    }

    public static void update(String key, Object value) {
        log.info("Updating key: {} with value: {}", key, value);
        validateKey(key);
        validateValue(value);
        cache.put(key, value);
        log.info("Updated key: {} with value: {}", key, value);
    }

    public static Object remove(String key) {
        log.info("Removing key: {}", key);
        validateKey(key);
        var removed = cache.remove(key);
        log.info("Removed value: {} for key: {}", removed, key);
        return removed;
    }

    public static void clear() {
        cache.clear();
        log.info("Cache cleared successfully");
    }

    public static Map<String, Object> getCache() {
        log.info("Getting cache");
        display();
        return cache;
    }

    public static void display() {
        log.info("Displaying cache content size {}:", cache.size());
        if (!cache.isEmpty())
            cache.forEach((k, v) -> log.info("key: {}, value: {}", k, v));
    }

    private static void validateKey(String key) {
        try {
            Objects.requireNonNull(key, "key must not be null");
        } catch (NullPointerException e) {
            log.error("Validation failed: key is null");
            throw e;
        }
    }

    private static void validateValue(Object value) {
        try {
            Objects.requireNonNull(value, "value must not be null");
        } catch (NullPointerException e) {
            log.error("Validation failed: value is null");
            throw e;
        }
    }
}
