package com.example.demo.biz.products.findAll.queues.consumer.v4.consumer;

import java.util.concurrent.CompletableFuture;

public interface IAsyncQueueV4Consumer<T> {

    CompletableFuture<T> call(String correlationId);

}
