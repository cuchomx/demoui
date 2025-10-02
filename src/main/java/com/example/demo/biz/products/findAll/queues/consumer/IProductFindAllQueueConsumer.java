package com.example.demo.biz.products.findAll.queues.consumer;

public interface IProductFindAllQueueConsumer {

    void consume();

    void delete(String receiptHandle);

}
