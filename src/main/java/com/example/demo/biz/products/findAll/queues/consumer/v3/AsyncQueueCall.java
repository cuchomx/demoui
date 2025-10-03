package com.example.demo.biz.products.findAll.queues.consumer.v3;

import java.util.concurrent.CompletableFuture;

public interface AsyncQueueCall<T> {

    CompletableFuture<T> call(String correlationId);

}
