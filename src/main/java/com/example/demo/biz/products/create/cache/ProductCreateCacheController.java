package com.example.demo.biz.products.create.cache;

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
@RequestMapping("/api/product")
public class ProductCreateCacheController {

    @GetMapping("/cache")
    public HttpEntity<?> getCacheData() {
        var cache = ProductCacheService.getCache();
        log.info("products - cache - size: {}", cache.size());
        cache.forEach((k, v) -> log.info(">>>> key: {}, value: {}", k, v));
        return ResponseEntity.ok(cache);
    }

}
