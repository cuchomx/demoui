package com.example.demo.biz.commons.cache;

import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public enum IdempotentRequestCache {

    INSTANCE;

    private static final int DEFAULT_CAPACITY = 128;

    private static final ConcurrentHashMap<String, Status> cache = new ConcurrentHashMap<>(DEFAULT_CAPACITY);

    public void putIfAbsent(String key, Status value) {
        validateKey(key);
        validateValue(value);
        log.debug("Putting key: {} with value: {}", key, value);
        cache.putIfAbsent(key, value);
    }

    public Status get(String key) {
        validateKey(key);
        log.debug("Getting value for key: {}", key);
        return cache.get(key);
    }

    public void remove(String key) {
        validateKey(key);
        log.debug("Removing key: {}", key);
        cache.remove(key);
    }

    public void clearCompleted() {
        log.info("Clearing COMPLETED entries from cache");
        cache.entrySet().removeIf(e -> e.getValue() == Status.COMPLETED);
        log.info("Completed entries cleared");
    }

    public boolean containsKey(String key) {
        validateKey(key);
        boolean present = cache.containsKey(key);
        log.debug("Contains key {}: {}", key, present);
        return present;
    }

    public boolean isInProgress(String key) {
        validateKey(key);
        var value = cache.get(key);
        boolean inProgress = value == Status.RECEIVED || value == Status.PROCESSING;
        log.debug("Key {} is in progress: {}", key, inProgress);
        return inProgress;
    }

    public void display() {
        int size = cache.size();
        log.debug("Displaying cache content size {}:", size);
        if (size > 0) {
            cache.forEach((k, v) -> log.debug("Key: {} Value: {}", k, v));
        }
    }

    private void validateKey(String key) {
        try {
            Objects.requireNonNull(key, "key must not be null");
        } catch (NullPointerException e) {
            log.error("Validation failed: key is null");
            throw e;
        }
    }

    private void validateValue(Status value) {
        try {
            Objects.requireNonNull(value, "value must not be null");
        } catch (NullPointerException e) {
            log.error("Validation failed: value is null");
            throw e;
        }
    }

    public enum Status {
        RECEIVED,
        PROCESSING,
        COMPLETED,
        ERROR
    }
}
