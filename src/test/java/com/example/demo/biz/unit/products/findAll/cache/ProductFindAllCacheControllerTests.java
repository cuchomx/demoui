package com.example.demo.biz.unit.products.findAll.cache;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.cache.ProductFindAllCacheController;
import com.example.demo.biz.products.findAll.cache.ProductFindAllCacheService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ProductFindAllCacheControllerTests {

    @AfterEach
    void cleanup() {
        ProductFindAllCacheService.clear();
    }

    @Test
    void shouldReturnUnderlyingCacheMap() {
        ProductResponseDto r1 = mock(ProductResponseDto.class);
        ProductResponseDto r2 = mock(ProductResponseDto.class);

        ProductFindAllCacheService.add("k1", List.of(r1));
        ProductFindAllCacheService.add("k2", List.of(r1));
        ProductFindAllCacheService.update("k2", List.of(r1, r2));

        ProductFindAllCacheController controller = new ProductFindAllCacheController();
        ResponseEntity<?> entity = (ResponseEntity<?>) controller.getData();

        assertEquals(200, entity.getStatusCode().value());
        assertTrue(entity.getBody() instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, List<ProductResponseDto>> map = (Map<String, List<ProductResponseDto>>) entity.getBody();
        assertSame(ProductFindAllCacheService.getCache(), map, "Controller should expose exact cache map reference");
        assertEquals(2, map.size());
        assertEquals(List.of(r1), map.get("k1"));
        assertEquals(List.of(r1, r2), map.get("k2"));
    }
}
