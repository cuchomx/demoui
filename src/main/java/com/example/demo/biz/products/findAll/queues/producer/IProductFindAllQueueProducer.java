package com.example.demo.biz.products.findAll.queues.producer;

import com.example.commons.dto.find.ProductFindAllRequestDto;

@FunctionalInterface
public interface IProductFindAllQueueProducer {

    void produce(String correlationId, ProductFindAllRequestDto dto);

}
