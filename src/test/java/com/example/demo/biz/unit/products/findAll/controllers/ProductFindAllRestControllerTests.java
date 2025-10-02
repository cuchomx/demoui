package com.example.demo.biz.unit.products.findAll.controllers;

import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.demo.biz.commons.dto.IdResponse;
import com.example.demo.biz.products.findAll.cache.ProductFindAllCacheService;
import com.example.demo.biz.products.findAll.controllers.ProductFindAllRestController;
import com.example.demo.biz.products.findAll.queues.producer.IProductFindAllQueueProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProductFindAllRestControllerTests {

    @AfterEach
    void cleanup() {
        ProductFindAllCacheService.clear();
    }

    @Test
    void shouldThrowOnInvalidCorrelationId() {
        IProductFindAllQueueProducer producer = mock(IProductFindAllQueueProducer.class);
        ProductFindAllRestController controller = new ProductFindAllRestController(producer);

        assertThrows(IllegalArgumentException.class, () -> controller.findAll(null, null, null));
        assertThrows(IllegalArgumentException.class, () -> controller.findAll("", null, null));
        verifyNoInteractions(producer);
    }

    @Test
    void shouldProduceOnFirstRequestAndReturnIdResponse() {
        IProductFindAllQueueProducer producer = mock(IProductFindAllQueueProducer.class);
        ProductFindAllRestController controller = new ProductFindAllRestController(producer);

        String correlationId = "123e4567-e89b-12d3-a456-426614174000";

        ResponseEntity<?> entity = controller.findAll(correlationId, null, null);

        // verify produce was called with built DTO containing defaults
        ArgumentCaptor<String> idCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductFindAllRequestDto> dtoCap = ArgumentCaptor.forClass(ProductFindAllRequestDto.class);
        verify(producer, times(1)).produce(idCap.capture(), dtoCap.capture());
        assertEquals(correlationId, idCap.getValue());
        ProductFindAllRequestDto dto = dtoCap.getValue();
        assertNotNull(dto, "DTO passed to producer should not be null");

        // cache should have been seeded with empty list
        assertTrue(ProductFindAllCacheService.containsKey(correlationId));
        assertEquals(List.of(), ProductFindAllCacheService.get(correlationId));

        // response should be 200 with IdResponse{id}
        assertEquals(200, entity.getStatusCode().value());
        assertTrue(entity.getBody() instanceof IdResponse);
        assertEquals(correlationId, ((IdResponse) entity.getBody()).id());
    }

    @Test
    void shouldReturnListFromCacheWhenPresent() {
        IProductFindAllQueueProducer producer = mock(IProductFindAllQueueProducer.class);
        ProductFindAllRestController controller = new ProductFindAllRestController(producer);

        String id = "123e4567-e89b-12d3-a456-426614174111";
        // Seed cache with an empty list marker; verify cached path returns list and producer is not called
        ProductFindAllCacheService.add(id, List.of());

        ResponseEntity<?> entity = controller.findAll(id, 5, 2);
        assertEquals(200, entity.getStatusCode().value());
        assertTrue(entity.getBody() instanceof List);
        assertTrue(((List<?>) entity.getBody()).isEmpty());
        verifyNoInteractions(producer);
    }
}
