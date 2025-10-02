package com.example.demo.biz.products.create.queues.producer;

import com.example.commons.dto.create.ProductRequestDto;

@FunctionalInterface
public interface IProductCreateQueueProducer {

    void produce(String correlationId, ProductRequestDto product);

}
