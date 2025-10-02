package com.example.demo.biz.products.create.queues.consumer;

public interface IProductCreateQueueConsumer {

    void consume();

    void delete(String receiptHandle);

}
