package com.example.demo.biz.unit.products.create.cache;

import com.example.commons.constants.RequestStatus;
import com.example.demo.biz.products.create.cache.ProductCacheService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProductCacheServiceTests {

    @AfterEach
    void cleanup() {
        ProductCacheService.clear();
    }

    @Test
    void addGetUpdateRemoveAndClear() {
        ProductCacheService.add("k1", RequestStatus.IN_PROGRESS);
        assertEquals(RequestStatus.IN_PROGRESS, ProductCacheService.get("k1"));

        ProductCacheService.update("k1", "done");
        assertEquals("done", ProductCacheService.get("k1"));

        assertEquals("done", ProductCacheService.remove("k1"));
        assertNull(ProductCacheService.getCache().get("k1"));

        ProductCacheService.add("k2", RequestStatus.IN_PROGRESS);
        assertFalse(ProductCacheService.getCache().isEmpty());
        ProductCacheService.clear();
        assertTrue(ProductCacheService.getCache().isEmpty());
    }

    @Test
    void nullKeyOrValueThrows() {
        assertThrows(NullPointerException.class, () -> ProductCacheService.add(null, RequestStatus.IN_PROGRESS));
        assertThrows(NullPointerException.class, () -> ProductCacheService.add("k", null));
        assertThrows(NullPointerException.class, () -> ProductCacheService.get(null));
        assertThrows(NullPointerException.class, () -> ProductCacheService.update(null, "v"));
        assertThrows(NullPointerException.class, () -> ProductCacheService.update("k", null));
        assertThrows(NullPointerException.class, () -> ProductCacheService.remove(null));
    }
}
