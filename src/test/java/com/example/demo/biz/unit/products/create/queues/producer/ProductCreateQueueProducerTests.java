package com.example.demo.biz.unit.products.create.queues.producer;

import com.example.commons.dto.create.ProductRequestDto;
import com.example.demo.biz.products.create.queues.producer.ProductCreateQueueProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ProductCreateQueueProducerTests {

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
    void validateConfigurationShouldThrowOnNullOrBlank() {
        ObjectMapper mapper = mock(ObjectMapper.class);
        SqsClient sqs = mock(SqsClient.class);
        ProductCreateQueueProducer producer = new ProductCreateQueueProducer(mapper, sqs);

        // null
        setField(producer, "queueUrl", null);
        assertThrows(IllegalStateException.class, () -> producer.validateConfiguration());

        // blank
        setField(producer, "queueUrl", "   ");
        assertThrows(IllegalStateException.class, () -> producer.validateConfiguration());
    }

    @Test
    void validateConfigurationShouldThrowOnBadSchemeOrMalformed() {
        ObjectMapper mapper = mock(ObjectMapper.class);
        SqsClient sqs = mock(SqsClient.class);
        ProductCreateQueueProducer producer = new ProductCreateQueueProducer(mapper, sqs);

        // bad scheme
        setField(producer, "queueUrl", "ftp://localhost/queue");
        assertThrows(IllegalStateException.class, producer::validateConfiguration);

        // malformed URI
        setField(producer, "queueUrl", "http://[bad-uri]");
        assertThrows(IllegalStateException.class, producer::validateConfiguration);
    }

    @Test
    void produceShouldSendMessageViaSqsClient() throws JsonProcessingException {
        ObjectMapper mapper = mock(ObjectMapper.class);
        when(mapper.writeValueAsString(any())).thenReturn("{}\n");

        SqsClient sqs = mock(SqsClient.class);
        SendMessageResponse resp = SendMessageResponse.builder()
                .messageId("mid-1")
                .sequenceNumber("seq-1")
                .build();
        doReturn(resp).when(sqs).sendMessage(any(SendMessageRequest.class));

        ProductCreateQueueProducer producer = new ProductCreateQueueProducer(mapper, sqs);
        setField(producer, "queueUrl", "http://localhost/queue");
        // validate config
        producer.validateConfiguration();

        String correlationId = "123e4567-e89b-12d3-a456-426614174000";
        ProductRequestDto dto = Mockito.mock(ProductRequestDto.class);
        producer.produce(correlationId, dto);

        verify(sqs, times(1)).sendMessage(any(SendMessageRequest.class));
    }
}
