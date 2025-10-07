package com.example.demo.biz.products.findAll.controllers.v4.dep;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public enum HttpRequestThreadMapCache {

    INSTANCE;

    private static final ConcurrentHashMap<String, Thread> cache = new ConcurrentHashMap<>();

    public void putIfAbsent(String key, Thread value) {
        log.info("Adding key: {} with value: {}", key, value);
        cache.putIfAbsent(key, value);
    }

    public Thread get(String key) {
        log.info("Getting value for key: {}", key);
        return cache.get(key);
    }

    public void remove(String key) {
        log.info("Removing key: {}", key);
        cache.remove(key);
    }

    public void clear() {
        log.info("Clearing cache");
        cache.clear();
    }

    public boolean containsKey(String key) {
        log.info("Checking if key: {}", key);
        return cache.containsKey(key);
    }

    public void display() {
        log.info("Displaying cache content size {}:", cache.size());
        cache.forEach((k, v) -> System.out.println("key: " + k + ", value: " + v));
    }

}
