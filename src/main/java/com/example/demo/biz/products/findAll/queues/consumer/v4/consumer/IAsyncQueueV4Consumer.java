package com.example.demo.biz.products.findAll.queues.consumer.v4.consumer;

public interface IAsyncQueueV4Consumer<T, ID> {

    T consume(String ID);

}
