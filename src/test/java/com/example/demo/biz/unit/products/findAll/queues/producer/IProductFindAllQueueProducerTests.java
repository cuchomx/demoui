package com.example.demo.biz.unit.products.findAll.queues.producer;

import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.demo.biz.products.findAll.queues.producer.IProductFindAllQueueProducer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class IProductFindAllQueueProducerTests {

    @Test
    void shouldInvokeFunctionalInterfaceWithLambda() {
        AtomicReference<String> idRef = new AtomicReference<>();
        AtomicReference<ProductFindAllRequestDto> dtoRef = new AtomicReference<>();

        IProductFindAllQueueProducer producer = (correlationId, dto) -> {
            idRef.set(correlationId);
            dtoRef.set(dto);
        };

        ProductFindAllRequestDto dto = new ProductFindAllRequestDto("id-1", 10, 0);
        producer.produce("id-1", dto);

        assertEquals("id-1", idRef.get());
        assertSame(dto, dtoRef.get());
    }

    @Test
    void shouldInvokeViaMethodReference() {
        class Target {
            String lastId;
            ProductFindAllRequestDto lastDto;

            void handle(String id, ProductFindAllRequestDto p) {
                this.lastId = id;
                this.lastDto = p;
            }
        }
        Target t = new Target();
        IProductFindAllQueueProducer producer = t::handle;

        ProductFindAllRequestDto dto = new ProductFindAllRequestDto("abc", 5, 2);
        producer.produce("abc", dto);

        assertEquals("abc", t.lastId);
        assertSame(dto, t.lastDto);
    }
}
