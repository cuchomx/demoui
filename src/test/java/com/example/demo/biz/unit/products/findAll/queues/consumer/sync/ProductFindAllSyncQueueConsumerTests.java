package com.example.demo.biz.unit.products.findAll.queues.consumer.sync;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.products.findAll.queues.consumer.v2.ProductFindAllSyncQueueConsumer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.example.commons.constants.AppConstants.CORRELATION_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ProductFindAllSyncQueueConsumerTests {

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

    @Test
    void validateConfigurationShouldRejectNullBlankAndInvalidUrl() {
        ObjectMapper mapper = mock(ObjectMapper.class);
        SqsClient sqs = mock(SqsClient.class);
        ProductFindAllSyncQueueConsumer consumer = new ProductFindAllSyncQueueConsumer(mapper, sqs);

        setField(consumer, "queueUrl", null);
        assertThrows(IllegalStateException.class, consumer::validateConfiguration);

        setField(consumer, "queueUrl", "  ");
        assertThrows(IllegalStateException.class, consumer::validateConfiguration);

        setField(consumer, "queueUrl", "ftp://localhost/queue");
        assertThrows(IllegalStateException.class, consumer::validateConfiguration);

        setField(consumer, "queueUrl", "http://[bad-uri]");
        assertThrows(IllegalStateException.class, consumer::validateConfiguration);
    }

    @Test
    void consumeShouldReturnParsedListOnlyForMatchingCorrelationIdAndDeleteOnce() throws Exception {
        ObjectMapper mapper = mock(ObjectMapper.class);
        SqsClient sqs = mock(SqsClient.class);
        ProductFindAllSyncQueueConsumer consumer = new ProductFindAllSyncQueueConsumer(mapper, sqs);
        setField(consumer, "queueUrl", QUEUE_URL);

        ProductResponseDto r1 = mock(ProductResponseDto.class);
        ProductResponseDto r2 = mock(ProductResponseDto.class);
        when(mapper.readValue(any(String.class), any(TypeReference.class))).thenReturn(List.of(r1, r2));

        String wantedId = "123e4567-e89b-12d3-a456-426614174000";

        Message nonMatching = Message.builder()
                .messageId("m-0")
                .receiptHandle("rh-0")
                .messageAttributes(Map.of(
                        CORRELATION_ID, MessageAttributeValue.builder().dataType("String").stringValue("other-id").build()
                ))
                .body("[{}]")
                .build();

        Message matching = Message.builder()
                .messageId("m-1")
                .receiptHandle("rh-1")
                .messageAttributes(Map.of(
                        CORRELATION_ID, MessageAttributeValue.builder().dataType("String").stringValue(wantedId).build()
                ))
                .body("[{\"name\":\"p\"}]")
                .build();

        ReceiveMessageResponse response = ReceiveMessageResponse.builder()
                .messages(nonMatching, matching)
                .build();
        when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

        CompletableFuture<List<ProductResponseDto>> fut = consumer.consume(wantedId);
        List<ProductResponseDto> result = fut.join();

        assertNotNull(result);
        assertEquals(2, result.size());

        ArgumentCaptor<DeleteMessageRequest> captor = ArgumentCaptor.forClass(DeleteMessageRequest.class);
        verify(sqs, times(1)).deleteMessage(captor.capture());
        DeleteMessageRequest del = captor.getValue();
        assertEquals(QUEUE_URL, del.queueUrl());
        assertEquals("rh-1", del.receiptHandle());
    }

    @Test
    void consumeShouldReturnEmptyOnInvalidCorrelationId() {
        ObjectMapper mapper = mock(ObjectMapper.class);
        SqsClient sqs = mock(SqsClient.class);
        ProductFindAllSyncQueueConsumer consumer = new ProductFindAllSyncQueueConsumer(mapper, sqs);
        setField(consumer, "queueUrl", QUEUE_URL);

        List<ProductResponseDto> list = consumer.consume("").join();
        assertTrue(list.isEmpty());
        verify(sqs, never()).receiveMessage(any(ReceiveMessageRequest.class));
    }
}
