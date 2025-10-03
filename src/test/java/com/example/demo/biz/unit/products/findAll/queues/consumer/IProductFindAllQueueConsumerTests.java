package com.example.demo.biz.unit.products.findAll.queues.consumer;

import com.example.demo.biz.products.findAll.queues.consumer.v1.IProductFindAllQueueConsumer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

class IProductFindAllQueueConsumerTests {

    @Test
    void shouldAllowMockingAndInvocation() {
        IProductFindAllQueueConsumer consumer = mock(IProductFindAllQueueConsumer.class);
        assertDoesNotThrow(() -> {
            consumer.consume();
            consumer.delete("rh-123");
        });
    }
}
