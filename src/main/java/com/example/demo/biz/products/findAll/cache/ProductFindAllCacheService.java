package com.example.demo.biz.products.findAll.cache;

import com.example.commons.dto.create.ProductResponseDto;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public enum ProductFindAllCacheService {

    INSTANCE;

    private static final int DEFAULT_CAPACITY = 128;
    private static final ConcurrentHashMap<String, List<ProductResponseDto>> cache = new ConcurrentHashMap<>(DEFAULT_CAPACITY);

    public static List<ProductResponseDto> add(String key, List<ProductResponseDto> value) {
        log.info("Adding key: {} with value: {}", key, value);
        validateKey(key);
        validateValue(value);
        List<ProductResponseDto> result = cache.putIfAbsent(key, value);
        log.info("Added key: {} with value: {}, previous value: {}", key, value, result);
        return result;
    }

    public static List<ProductResponseDto> get(String key) {
        log.info("Getting value for key: {}", key);
        validateKey(key);
        List<ProductResponseDto> result = cache.get(key);
        log.info("Got value: {} for key: {}", result, key);
        return result;
    }

    public static void update(String key, List<ProductResponseDto> value) {
        log.info("Updating key: {} with value: {}", key, value);
        validateKey(key);
        validateValue(value);
        cache.put(key, value);
        log.info("Updated key: {} with value: {}", key, value);
    }

    public static List<ProductResponseDto> remove(String key) {
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

    public static Map<String, List<ProductResponseDto>> getCache() {
        log.info("Getting cache");
        display();
        return cache;
    }

    public static boolean containsKey(String key) {
        log.info("Checking if key: {}", key);
        var exists = cache.containsKey(key);
        if (!exists) {
            log.info("Key does not exist: {} actual content is:", key);
            display();
        }
        log.info("Key: {} exists: {}", key, exists);
        return exists;
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

    private static void validateValue(List<ProductResponseDto> value) {
        try {
            Objects.requireNonNull(value, "value must not be null");
        } catch (NullPointerException e) {
            log.error("Validation failed: value is null");
            throw e;
        }
    }
}
