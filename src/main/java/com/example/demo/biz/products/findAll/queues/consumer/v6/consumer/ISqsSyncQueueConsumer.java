package com.example.demo.biz.products.findAll.queues.consumer.v6.consumer;

import java.util.concurrent.CompletableFuture;

public interface ISqsSyncQueueConsumer<T> {

    CompletableFuture<T> consume(String correlationId);

}
