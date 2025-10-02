package com.example.demo.biz.unit.products.create.cache;

import com.example.demo.biz.products.create.cache.ProductCacheService;
import com.example.demo.biz.products.create.cache.ProductCreateCacheController;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProductCreateCacheControllerTests {

    @AfterEach
    void cleanup() {
        ProductCacheService.clear();
    }

    @Test
    void shouldReturnUnderlyingCacheMap() {

        ProductCacheService.add("k1", com.example.commons.constants.RequestStatus.IN_PROGRESS);
        ProductCacheService.add("k2", com.example.commons.constants.RequestStatus.IN_PROGRESS);
        ProductCacheService.update("k2", "v2");

        ProductCreateCacheController controller = new ProductCreateCacheController();
        ResponseEntity<?> entity = (ResponseEntity<?>) controller.getCacheData();

        assertEquals(200, entity.getStatusCode().value());
        assertTrue(entity.getBody() instanceof Map);


        Map<String, Object> map = (Map<String, Object>) entity.getBody();
        assertSame(ProductCacheService.getCache(), map, "Controller should expose exact cache map reference");
        assertEquals(2, map.size());
        assertEquals(com.example.commons.constants.RequestStatus.IN_PROGRESS, map.get("k1"));
        assertEquals("v2", map.get("k2"));
    }
}
