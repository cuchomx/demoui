package com.example.demo.biz.unit.products.findAll.queues.consumer;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.ProductFindAllQueueConsumer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static com.example.commons.constants.AppConstants.CORRELATION_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ProductFindAllQueueConsumerTests {

    private static final String QUEUE_URL = "http://localhost/queue";

    private static void setField(Object target, String fieldName, Object value) {
        try {
            Field f = target.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void cleanup() {
        com.example.demo.biz.products.findAll.cache.ProductFindAllCacheService.clear();
    }

    @Test
    void validateConfigurationShouldThrowOnNullBlankBadSchemeOrMalformed() {
        ObjectMapper mapper = mock(ObjectMapper.class);
        SqsClient sqs = mock(SqsClient.class);
        ProductFindAllQueueConsumer consumer = new ProductFindAllQueueConsumer(mapper, sqs);

        setField(consumer, "queueUrl", null);
        assertThrows(IllegalStateException.class, consumer::validateConfiguration);

        setField(consumer, "queueUrl", "   ");
        assertThrows(IllegalStateException.class, consumer::validateConfiguration);

        setField(consumer, "queueUrl", "ftp://localhost/queue");
        assertThrows(IllegalStateException.class, consumer::validateConfiguration);

        setField(consumer, "queueUrl", "http://[bad-uri]");
        assertThrows(IllegalStateException.class, consumer::validateConfiguration);
    }

    @Test
    void consumeShouldParseJsonUpdateCacheAndDelete() throws Exception {
        ObjectMapper mapper = mock(ObjectMapper.class);
        SqsClient sqs = mock(SqsClient.class);
        ProductFindAllQueueConsumer consumer = new ProductFindAllQueueConsumer(mapper, sqs);
        setField(consumer, "queueUrl", QUEUE_URL);

        // Prepare a JSON payload -> parsed into 2 products
        ProductResponseDto p1 = mock(ProductResponseDto.class);
        ProductResponseDto p2 = mock(ProductResponseDto.class);
        when(mapper.readValue(any(String.class), any(TypeReference.class)))
                .thenReturn(List.of(p1, p2));

        String correlationId = "123e4567-e89b-12d3-a456-426614174000";
        Message m = Message.builder()
                .messageId("m-1")
                .receiptHandle("rh-1")
                .messageAttributes(Map.of(
                        CORRELATION_ID, MessageAttributeValue.builder().dataType("String").stringValue(correlationId).build()
                ))
                .body("[{}]")
                .build();

        ReceiveMessageResponse resp = ReceiveMessageResponse.builder()
                .messages(m)
                .build();
        when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(resp);

        consumer.consume();

        // Verify cache was updated
        List<com.example.commons.dto.create.ProductResponseDto> list = com.example.demo.biz.products.findAll.cache.ProductFindAllCacheService.get(correlationId);
        assertNotNull(list);
        assertEquals(2, list.size());

        // Verify delete invoked with proper handle
        ArgumentCaptor<DeleteMessageRequest> captor = ArgumentCaptor.forClass(DeleteMessageRequest.class);
        verify(sqs, times(1)).deleteMessage(captor.capture());
        DeleteMessageRequest del = captor.getValue();
        assertEquals(QUEUE_URL, del.queueUrl());
        assertEquals("rh-1", del.receiptHandle());
    }

    @Test
    void consumeShouldSkipWhenMissingCorrelationIdOrBody() {
        ObjectMapper mapper = mock(ObjectMapper.class);
        SqsClient sqs = mock(SqsClient.class);
        ProductFindAllQueueConsumer consumer = new ProductFindAllQueueConsumer(mapper, sqs);
        setField(consumer, "queueUrl", QUEUE_URL);

        Message blankAttr = Message.builder()
                .messageId("m-1")
                .receiptHandle("rh-1")
                .messageAttributes(Map.of(
                        CORRELATION_ID, MessageAttributeValue.builder().dataType("String").stringValue("").build()
                ))
                .body("[{}]")
                .build();
        Message noBody = Message.builder()
                .messageId("m-2")
                .receiptHandle("rh-2")
                .messageAttributes(Map.of(
                        CORRELATION_ID, MessageAttributeValue.builder().dataType("String").stringValue("id-2").build()
                ))
                .body("")
                .build();

        ReceiveMessageResponse resp = ReceiveMessageResponse.builder()
                .messages(blankAttr, noBody)
                .build();
        when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(resp);

        consumer.consume();

        // delete should not be called for skipped messages
        verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
    }
}
