package com.example.demo.biz.products.findAll.queues.consumer.v4.service;

import com.example.commons.dto.create.ProductResponseDto;

import java.util.List;

public interface IProductFindAllV4QueueService {

    List<ProductResponseDto> consume(String correlationId);

}
