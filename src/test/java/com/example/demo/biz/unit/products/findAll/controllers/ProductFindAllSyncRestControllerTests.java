package com.example.demo.biz.unit.products.findAll.controllers;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.demo.biz.products.findAll.controllers.v2.ProductFindAllSyncRestController;
import com.example.demo.biz.products.findAll.queues.consumer.v2.IProductFindAllSyncQueueConsumer;
import com.example.demo.biz.products.findAll.queues.producer.IProductFindAllQueueProducer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ProductFindAllSyncRestControllerTests {

    @Test
    void shouldReturnProductsOnHappyPath() {
        IProductFindAllSyncQueueConsumer consumer = mock(IProductFindAllSyncQueueConsumer.class);
        IProductFindAllQueueProducer producer = mock(IProductFindAllQueueProducer.class);
        ProductFindAllSyncRestController controller = new ProductFindAllSyncRestController(consumer, producer);

        String correlationId = "id-001";
        @SuppressWarnings("unchecked")
        List<ProductResponseDto> products = (List<ProductResponseDto>) (List<?>) List.of(mock(ProductResponseDto.class), mock(ProductResponseDto.class));

        when(consumer.consume(correlationId)).thenReturn(CompletableFuture.completedFuture(products));

        List<ProductResponseDto> result = controller.findAll(correlationId, null, null).join();

        // verify produce called once with built DTO of some sort
        verify(producer, times(1)).produce(eq(correlationId), any(ProductFindAllRequestDto.class));
        assertSame(products, result);
    }

    @Test
    void shouldPropagateTimeoutAsResponseStatusException() {
        IProductFindAllSyncQueueConsumer consumer = mock(IProductFindAllSyncQueueConsumer.class);
        IProductFindAllQueueProducer producer = mock(IProductFindAllQueueProducer.class);
        ProductFindAllSyncRestController controller = new ProductFindAllSyncRestController(consumer, producer);

        String correlationId = "id-timeout";
        CompletableFuture<List<ProductResponseDto>> failed = CompletableFuture.failedFuture(new CompletionException(new TimeoutException("t")));
        when(consumer.consume(correlationId)).thenReturn(failed);

        CompletableFuture<List<ProductResponseDto>> fut = controller.findAll(correlationId, null, null);
        CompletionException ex = assertThrows(CompletionException.class, fut::join);
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof org.springframework.web.server.ResponseStatusException);
        org.springframework.web.server.ResponseStatusException rse = (org.springframework.web.server.ResponseStatusException) ex.getCause();
        assertEquals(408, rse.getStatusCode().value());
    }

    @Test
    void shouldReturnEmptyListOnGenericError() {
        IProductFindAllSyncQueueConsumer consumer = mock(IProductFindAllSyncQueueConsumer.class);
        IProductFindAllQueueProducer producer = mock(IProductFindAllQueueProducer.class);
        ProductFindAllSyncRestController controller = new ProductFindAllSyncRestController(consumer, producer);

        String correlationId = "id-error";
        when(consumer.consume(correlationId)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));

        List<ProductResponseDto> result = controller.findAll(correlationId, null, null).join();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}
