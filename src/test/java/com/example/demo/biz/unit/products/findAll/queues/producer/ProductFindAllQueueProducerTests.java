package com.example.demo.biz.unit.products.findAll.queues.producer;

import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.demo.biz.products.findAll.queues.producer.ProductFindAllQueueProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ProductFindAllQueueProducerTests {

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
    void validateConfigurationShouldThrowOnNullBlankBadSchemeAndMalformed() {
        ObjectMapper mapper = mock(ObjectMapper.class);
        SqsClient sqs = mock(SqsClient.class);
        ProductFindAllQueueProducer producer = new ProductFindAllQueueProducer(mapper, sqs);

        setField(producer, "queueUrl", null);
        assertThrows(IllegalStateException.class, producer::validateConfiguration);

        setField(producer, "queueUrl", "   ");
        assertThrows(IllegalStateException.class, producer::validateConfiguration);

        setField(producer, "queueUrl", "ftp://localhost/queue");
        assertThrows(IllegalStateException.class, producer::validateConfiguration);

        setField(producer, "queueUrl", "http://[bad-uri]");
        assertThrows(IllegalStateException.class, producer::validateConfiguration);
    }

    @Test
    void produceShouldSendMessageViaSqsClient() throws JsonProcessingException {
        ObjectMapper mapper = mock(ObjectMapper.class);
        when(mapper.writeValueAsString(any())).thenReturn("{}\n");

        SqsClient sqs = mock(SqsClient.class);
        SendMessageResponse resp = SendMessageResponse.builder().messageId("mid-2").build();
        doReturn(resp).when(sqs).sendMessage(any(SendMessageRequest.class));

        ProductFindAllQueueProducer producer = new ProductFindAllQueueProducer(mapper, sqs);
        setField(producer, "queueUrl", "http://localhost/queue");
        producer.validateConfiguration();

        ProductFindAllRequestDto dto = new ProductFindAllRequestDto("id-1", 10, 0);
        producer.produce("id-1", dto);

        verify(sqs, times(1)).sendMessage(any(SendMessageRequest.class));
    }
}
