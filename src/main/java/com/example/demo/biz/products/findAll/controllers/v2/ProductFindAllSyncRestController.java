package com.example.demo.biz.products.findAll.controllers.v2;

import com.example.commons.dto.create.ProductResponseDto;
import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.products.findAll.queues.consumer.v2.IProductFindAllSyncQueueConsumer;
import com.example.demo.biz.products.findAll.queues.producer.IProductFindAllQueueProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v2/products")
public class ProductFindAllSyncRestController {

    private final IProductFindAllSyncQueueConsumer iProductFindAllSyncQueueConsumer;
    private final IProductFindAllQueueProducer productFindAllQueueProducer;

    private static final int TIMEOUT_SECONDS = 30;
    private static final int DEFAULT_LIMIT = 10;
    private static final int DEFAULT_OFFSET = 0;

    @GetMapping
    public CompletableFuture<List<ProductResponseDto>> findAll(
            @RequestParam("uuid") String correlationId,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset
    ) {
        log.info("=================================================================================================");
        log.info("ProductFindAllSyncRestController::findAll - Request UUID: {} - consuming from queue", correlationId);

        ParameterValidationUtils.isValidCorrelationIdValue(correlationId);

        ProductFindAllRequestDto requestDto = buildRequestDto(correlationId, limit, offset);
        productFindAllQueueProducer.produce(correlationId, requestDto);

        return iProductFindAllSyncQueueConsumer
                .consume(correlationId)
                .orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .exceptionally(throwable -> handleConsumptionError(correlationId, throwable))
                .thenApply(products -> transformResponse(correlationId, products));
    }

    private List<ProductResponseDto> handleConsumptionError(String correlationId, Throwable throwable) {
        log.error("ProductFindAllSyncRestController::handleConsumptionError - Failed to consume for UUID: {}. Error: {}",
                correlationId, throwable.getMessage(), throwable);

        if (throwable.getCause() instanceof TimeoutException) {
            throw new ResponseStatusException(
                    HttpStatus.REQUEST_TIMEOUT,
                    "Request timed out waiting for products",
                    throwable
            );
        }

        return List.of();
    }

    private ProductFindAllRequestDto buildRequestDto(String id, Integer limit, Integer offset) {
        return new ProductFindAllRequestDto(
                id,
                limit == null ? DEFAULT_LIMIT : limit,
                offset == null ? DEFAULT_OFFSET : offset
        );
    }

    private List<ProductResponseDto> transformResponse(String correlationId, List<ProductResponseDto> products) {
        if (products == null || products.isEmpty()) {
            log.info("ProductFindAllSyncRestController::findAll - No data found for request UUID: {}", correlationId);
            return List.of();
        }

        log.info("ProductFindAllSyncRestController::findAll - Found {} products for request UUID: {}",
                products.size(),
                correlationId
        );
        return products;
    }
}
