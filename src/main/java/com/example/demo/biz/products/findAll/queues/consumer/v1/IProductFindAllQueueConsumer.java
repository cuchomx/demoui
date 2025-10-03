package com.example.demo.biz.products.findAll.queues.consumer.v1;

public interface IProductFindAllQueueConsumer {

    void consume();

    void delete(String receiptHandle);

}
