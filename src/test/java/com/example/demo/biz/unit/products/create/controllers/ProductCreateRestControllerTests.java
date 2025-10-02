package com.example.demo.biz.unit.products.create.controllers;

import com.example.commons.dto.create.ProductRequestDto;
import com.example.demo.biz.commons.dto.IdResponse;
import com.example.demo.biz.products.create.cache.ProductCacheService;
import com.example.demo.biz.products.create.controllers.ProductCreateRestController;
import com.example.demo.biz.products.create.queues.producer.IProductCreateQueueProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;

import static com.example.commons.constants.RequestStatus.IN_PROGRESS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProductCreateRestControllerTests {

    @AfterEach
    void cleanup() {
        ProductCacheService.clear();
    }

    @Test
    void shouldThrowOnInvalidBindingResult() {
        IProductCreateQueueProducer producer = mock(IProductCreateQueueProducer.class);
        BindingResult bindingResult = mock(BindingResult.class);
        when(bindingResult.hasErrors()).thenReturn(true);

        ProductCreateRestController controller = new ProductCreateRestController(producer);

        assertThrows(IllegalStateException.class, () ->
                controller.create("abc", mock(ProductRequestDto.class), bindingResult)
        );
        verifyNoInteractions(producer);
    }

    @Test
    void shouldThrowOnInvalidCorrelationId() {
        IProductCreateQueueProducer producer = mock(IProductCreateQueueProducer.class);
        BindingResult bindingResult = mock(BindingResult.class);
        when(bindingResult.hasErrors()).thenReturn(false);

        ProductCreateRestController controller = new ProductCreateRestController(producer);

        // invalid IDs (null/blank)
        assertThrows(IllegalArgumentException.class, () ->
                controller.create(null, mock(ProductRequestDto.class), bindingResult)
        );
        assertThrows(IllegalArgumentException.class, () ->
                controller.create("", mock(ProductRequestDto.class), bindingResult)
        );
        verifyNoInteractions(producer);
    }

    @Test
    void shouldEnqueueAndReturnIdResponseOnHappyPath() {
        IProductCreateQueueProducer producer = mock(IProductCreateQueueProducer.class);
        BindingResult bindingResult = mock(BindingResult.class);
        when(bindingResult.hasErrors()).thenReturn(false);

        ProductCreateRestController controller = new ProductCreateRestController(producer);

        String correlationId = "123e4567-e89b-12d3-a456-426614174000";
        ProductRequestDto product = mock(ProductRequestDto.class);

        ResponseEntity<?> response = (ResponseEntity<?>) controller.create(correlationId, product, bindingResult);

        // verify queue called
        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductRequestDto> dtoCaptor = ArgumentCaptor.forClass(ProductRequestDto.class);
        verify(producer, times(1)).produce(idCaptor.capture(), dtoCaptor.capture());
        assertEquals(correlationId, idCaptor.getValue());
        assertSame(product, dtoCaptor.getValue());

        // verify cache updated to IN_PROGRESS
        assertEquals(IN_PROGRESS, ProductCacheService.get(correlationId));

        // verify response DTO
        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody() instanceof IdResponse);
        IdResponse idResponse = (IdResponse) response.getBody();
        assertEquals(correlationId, idResponse.id());
    }

    @Test
    void shouldGetCreatedIdReturnEmptyWhenNullOrInProgressAnd201WhenReady() {
        IProductCreateQueueProducer producer = mock(IProductCreateQueueProducer.class);
        ProductCreateRestController controller = new ProductCreateRestController(producer);

        // nothing in cache -> empty OK
        HttpEntity<?> emptyResp = controller.getCreatedId("k1");
        assertTrue(((ResponseEntity<?>) emptyResp).getStatusCode().is2xxSuccessful());
        assertNull(((ResponseEntity<?>) emptyResp).getBody());

        // IN_PROGRESS -> still empty OK
        ProductCacheService.add("k2", IN_PROGRESS);
        HttpEntity<?> inProgResp = controller.getCreatedId("k2");
        assertTrue(((ResponseEntity<?>) inProgResp).getStatusCode().is2xxSuccessful());
        assertNull(((ResponseEntity<?>) inProgResp).getBody());

        // set a concrete id -> 201 with body
        ProductCacheService.update("k2", "new-id-001");
        HttpEntity<?> created = controller.getCreatedId("k2");
        ResponseEntity<?> createdResp = (ResponseEntity<?>) created;
        assertEquals(201, createdResp.getStatusCode().value());
        assertEquals("new-id-001", createdResp.getBody());
    }
}
