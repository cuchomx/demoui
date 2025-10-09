package com.example.demo.biz.products.findAll.queues.consumer.v7.consumer;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ISqsSyncV7QueueConsumer<T> {

    CompletableFuture<List<SqsSyncV7QueueConsumer.OperationResult>> consume();

}
