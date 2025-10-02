package com.example.demo.biz.unit.products.create.queues.consumer;

import com.example.demo.biz.products.create.cache.ProductCacheService;
import com.example.demo.biz.products.create.queues.consumer.ProductCreateQueueConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ProductCreateQueueConsumerTests {

    private static final String QUEUE_URL = "http://localhost/queue";

    @AfterEach
    void cleanup() {
        ProductCacheService.clear();
    }

    @Test
    void validateConfigurationShouldRejectNullBlankAndInvalidUrl() throws Exception {
        SqsClient sqs = mock(SqsClient.class);
        ProductCreateQueueConsumer consumer = new ProductCreateQueueConsumer(sqs);

        // null
        setQueueUrl(consumer, null);
        IllegalStateException ex1 = assertThrows(IllegalStateException.class, consumer::validateConfiguration);
        assertTrue(ex1.getMessage().contains("must be configured"));

        // blank
        setQueueUrl(consumer, "  ");
        IllegalStateException ex2 = assertThrows(IllegalStateException.class, consumer::validateConfiguration);
        assertTrue(ex2.getMessage().contains("must be configured"));

        // invalid scheme
        setQueueUrl(consumer, "ftp://localhost/queue");
        IllegalStateException ex3 = assertThrows(IllegalStateException.class, consumer::validateConfiguration);
        assertTrue(ex3.getMessage().toLowerCase().contains("invalid sqs queue url scheme"));

        // bad uri
        setQueueUrl(consumer, "not a url");
        IllegalStateException ex4 = assertThrows(IllegalStateException.class, consumer::validateConfiguration);
        assertTrue(ex4.getMessage().toLowerCase().contains("invalid sqs queue url"));
    }

    @Test
    void consumeShouldReturnSafelyWhenNoMessages() throws Exception {
        SqsClient sqs = mock(SqsClient.class);
        ProductCreateQueueConsumer consumer = new ProductCreateQueueConsumer(sqs);
        setQueueUrl(consumer, QUEUE_URL);

        ReceiveMessageResponse empty = ReceiveMessageResponse.builder()
                .messages(Collections.emptyList())
                .build();
        when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(empty);

        assertDoesNotThrow(consumer::consume);
        // ensure delete is never called when there are no messages
        verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void deleteShouldInvokeSqsDeleteMessage() throws Exception {

        SqsClient sqs = mock(SqsClient.class);
        ProductCreateQueueConsumer consumer = new ProductCreateQueueConsumer(sqs);
        setQueueUrl(consumer, QUEUE_URL);

        String rh = "rh-001";
        consumer.delete(rh);

        ArgumentCaptor<DeleteMessageRequest> captor = ArgumentCaptor.forClass(DeleteMessageRequest.class);
        verify(sqs, times(1)).deleteMessage(captor.capture());
        DeleteMessageRequest req = captor.getValue();

        assertEquals(QUEUE_URL, req.queueUrl());
        assertEquals(rh, req.receiptHandle());
    }

    private static void setQueueUrl(ProductCreateQueueConsumer consumer, String value) throws Exception {
        Field f = ProductCreateQueueConsumer.class.getDeclaredField("queueUrl");
        f.setAccessible(true);
        f.set(consumer, value);
    }
}
