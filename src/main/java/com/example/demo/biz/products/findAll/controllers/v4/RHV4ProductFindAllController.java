package com.example.demo.biz.products.findAll.controllers.v4;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.commons.cache.IdempotentRequestCache;
import com.example.demo.biz.products.findAll.queues.consumer.v4.service.IProductFindAllV4QueueService;
import com.example.demo.biz.products.findAll.queues.producer.IProductFindAllQueueProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v4/products")
public class RHV4ProductFindAllController {

    private static final int DEFAULT_LIMIT = 10;
    private static final int DEFAULT_OFFSET = 0;

    private final IProductFindAllQueueProducer productFindAllQueueProducer;
    private final IProductFindAllV4QueueService iProductFindAllV4QueueService;

    @GetMapping
    public HttpEntity<List<ProductResponseDto>> findAll(
            @RequestParam("uuid") String correlationId,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset
    ) {
        log.info("ProductFindAllV4Controller::findAll - Request UUID: {}", correlationId);

        if (ParameterValidationUtils.isNotValidCorrelationId(correlationId)) {
            log.error("ProductFindAllV4Controller::findAll - Invalid correlation id value - {}", correlationId);
            return ResponseEntity.badRequest().body(List.of());
        }

        if (IdempotentRequestCache.INSTANCE.isInProgress(correlationId)) {
            log.info("ProductFindAllV4Controller::findAll - correlationId {} is in progress", correlationId);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
        }

        log.info("ProductFindAllV4Controller::findAll - Generated from product correlationId: {}", correlationId);
        IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.RECEIVED);

        ProductFindAllRequestDto requestDto = new ProductFindAllRequestDto(
                correlationId,
                limit == null ? DEFAULT_LIMIT : limit,
                offset == null ? DEFAULT_OFFSET : offset
        );

        try {

            log.info("ProductFindAllV4Controller::findAll - Producing for correlationId: {}", correlationId);
            productFindAllQueueProducer.produce(correlationId, requestDto);

            log.info("ProductFindAllV4Controller::findAll - Consuming for correlationId: {}", correlationId);
            List<ProductResponseDto> products = iProductFindAllV4QueueService.consume(correlationId);

            if (products == null || products.isEmpty()) {
                log.warn("ProductFindAllV4Controller::findAll - No products yet for correlationId: {} returning accepted", correlationId);
                return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
            }

            log.info("ProductFindAllV4Controller::findAll - Returning {} products for correlationId: {}",
                    products.size(),
                    correlationId
            );
            return ResponseEntity.ok(products);

        } catch (Exception e) {
            log.error("ProductFindAllV4Controller::findAll - Error producing/consuming for {}", correlationId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(List.of());
        } finally {
            IdempotentRequestCache.INSTANCE.remove(correlationId);
        }
    }


}
