package com.example.demo.biz.products.findAll.queues.consumer.sync;

import com.example.commons.dto.create.ProductResponseDto;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface IProductFindAllSyncQueueConsumer {

    CompletableFuture<List<ProductResponseDto>> consume(String correlationId);

    void delete(String receiptHandle);

}
