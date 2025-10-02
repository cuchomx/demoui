package com.example.demo.biz.products.findAll.cache;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Slf4j
@Controller
@RequiredArgsConstructor
@RequestMapping("/api/products")
public class ProductFindAllCacheController {

    @GetMapping("/cache")
    public HttpEntity<?> getData() {
        var cache = ProductFindAllCacheService.getCache();
        log.info("products - cache - size: {}", cache.size());
        cache.forEach((k, v) -> log.info(">>>> key: {}, value: {}", k, v));
        return ResponseEntity.ok(cache);
    }

}
