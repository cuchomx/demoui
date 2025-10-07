package com.example.demo.biz.products.findAll.queues.consumer.v5.service;

import com.example.commons.dto.create.ProductResponseDto;

import java.util.List;

public interface IProductFindAllSyncQueueService {

    List<ProductResponseDto> consume(String correlationId);

}
