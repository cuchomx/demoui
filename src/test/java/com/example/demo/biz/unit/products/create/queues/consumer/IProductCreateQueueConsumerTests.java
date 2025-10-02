package com.example.demo.biz.unit.products.create.queues.consumer;

import com.example.demo.biz.products.create.queues.consumer.IProductCreateQueueConsumer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

class IProductCreateQueueConsumerTests {

    @Test
    void shouldAllowMockingAndInvocation() {
        IProductCreateQueueConsumer consumer = mock(IProductCreateQueueConsumer.class);
        // Just ensure the signatures are invocable without throwing
        assertDoesNotThrow(() -> {
            consumer.consume();
            consumer.delete("rh-123");
        });
    }
}
