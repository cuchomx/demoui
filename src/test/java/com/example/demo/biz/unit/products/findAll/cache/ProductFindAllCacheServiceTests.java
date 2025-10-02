package com.example.demo.biz.unit.products.findAll.cache;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.cache.ProductFindAllCacheService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ProductFindAllCacheServiceTests {

    @AfterEach
    void cleanup() {
        ProductFindAllCacheService.clear();
    }

    @Test
    void addGetUpdateRemoveClearAndContainsKey() {
        ProductResponseDto r1 = mock(ProductResponseDto.class);
        ProductResponseDto r2 = mock(ProductResponseDto.class);

        // add
        ProductFindAllCacheService.add("k1", List.of(r1));
        assertTrue(ProductFindAllCacheService.containsKey("k1"));
        assertEquals(List.of(r1), ProductFindAllCacheService.get("k1"));

        // update
        ProductFindAllCacheService.update("k1", List.of(r1, r2));
        assertEquals(List.of(r1, r2), ProductFindAllCacheService.get("k1"));

        // remove
        assertEquals(List.of(r1, r2), ProductFindAllCacheService.remove("k1"));
        assertFalse(ProductFindAllCacheService.containsKey("k1"));

        // clear
        ProductFindAllCacheService.add("k2", List.of(r2));
        assertFalse(ProductFindAllCacheService.getCache().isEmpty());
        ProductFindAllCacheService.clear();
        assertTrue(ProductFindAllCacheService.getCache().isEmpty());
    }

    @Test
    void nullKeyOrValueThrows() {
        ProductResponseDto r1 = mock(ProductResponseDto.class);

        assertThrows(NullPointerException.class, () -> ProductFindAllCacheService.add(null, List.of(r1)));
        assertThrows(NullPointerException.class, () -> ProductFindAllCacheService.add("k", null));
        assertThrows(NullPointerException.class, () -> ProductFindAllCacheService.get(null));
        assertThrows(NullPointerException.class, () -> ProductFindAllCacheService.update(null, List.of(r1)));
        assertThrows(NullPointerException.class, () -> ProductFindAllCacheService.update("k", null));
        assertThrows(NullPointerException.class, () -> ProductFindAllCacheService.remove(null));
        assertThrows(NullPointerException.class, () -> ProductFindAllCacheService.containsKey(null));
    }
}
