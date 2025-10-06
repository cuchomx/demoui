package com.example.demo.biz.products.findAll.queues.consumer.v3.service;

import com.example.commons.dto.create.ProductResponseDto;

import java.util.List;

public interface IProductFindAllV3QueueConsumer {

    List<ProductResponseDto> consume(String correlationId);

}
