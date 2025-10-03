package com.example.demo.biz.unit.products.findAll.queues.consumer.sync;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.v2.IProductFindAllSyncQueueConsumer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IProductFindAllSyncQueueConsumerTests {

    @Test
    void shouldAllowMockingAsyncSignature() {
        IProductFindAllSyncQueueConsumer consumer = mock(IProductFindAllSyncQueueConsumer.class);
        when(consumer.consume("id-1")).thenReturn(CompletableFuture.completedFuture(List.of()));

        assertDoesNotThrow(() -> consumer.delete("rh-1"));
        CompletableFuture<List<ProductResponseDto>> fut = consumer.consume("id-1");
        assertNotNull(fut);
        assertTrue(fut.join().isEmpty());
    }
}
