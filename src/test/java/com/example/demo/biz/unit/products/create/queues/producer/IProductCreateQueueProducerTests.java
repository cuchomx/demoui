package com.example.demo.biz.unit.products.create.queues.producer;

import com.example.commons.dto.create.ProductRequestDto;
import com.example.demo.biz.products.create.queues.producer.IProductCreateQueueProducer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class IProductCreateQueueProducerTests {

    @Test
    void shouldInvokeFunctionalInterfaceWithLambda() {
        AtomicReference<String> idRef = new AtomicReference<>();
        AtomicReference<ProductRequestDto> dtoRef = new AtomicReference<>();

        IProductCreateQueueProducer producer = (correlationId, product) -> {
            idRef.set(correlationId);
            dtoRef.set(product);
        };

        ProductRequestDto dto = org.mockito.Mockito.mock(ProductRequestDto.class);
        producer.produce("id-123", dto);

        assertEquals("id-123", idRef.get());
        assertSame(dto, dtoRef.get());
    }

    @Test
    void shouldInvokeViaMethodReference() {
        class Target {
            String lastId;
            ProductRequestDto lastDto;

            void handle(String id, ProductRequestDto p) {
                this.lastId = id;
                this.lastDto = p;
            }
        }
        Target t = new Target();
        IProductCreateQueueProducer producer = t::handle;

        ProductRequestDto dto = org.mockito.Mockito.mock(ProductRequestDto.class);
        producer.produce("abc", dto);

        assertEquals("abc", t.lastId);
        assertSame(dto, t.lastDto);
    }
}
