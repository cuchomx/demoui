package com.example.demo.biz.products.findAll.queues.consumer.v5.consumer;

import java.util.concurrent.CompletableFuture;

public interface ISyncQueueConsumer<T> {

    CompletableFuture<T> consume(String id);

}
