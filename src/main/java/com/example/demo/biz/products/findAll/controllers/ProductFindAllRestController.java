package com.example.demo.biz.products.findAll.controllers;

import com.example.commons.dto.find.ProductFindAllRequestDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.commons.dto.IdResponse;
import com.example.demo.biz.products.findAll.cache.ProductFindAllCacheService;
import com.example.demo.biz.products.findAll.queues.producer.IProductFindAllQueueProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/products")
public class ProductFindAllRestController {

    private static final int DEFAULT_LIMIT = 10;
    private static final int DEFAULT_OFFSET = 0;

    private final IProductFindAllQueueProducer productFindAllQueueProducer;

    @GetMapping
    public ResponseEntity<?> findAll(
            @RequestParam("uuid") String correlationId,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset
    ) {

        log.info("=================================================================================================");
        log.info("ProductFindAllRestController::findAll - correlationId: {}", correlationId);

        if (!ParameterValidationUtils.isValidCorrelationIdValue(correlationId)) {
            log.error("ProductFindAllRestController::findAll - Correlation id has an invalid value - {}", correlationId);
            throw new IllegalArgumentException("No correlation Id - correlation id must be provided");
        }

        if (!ProductFindAllCacheService.containsKey(correlationId)) {
            log.info("ProductFindAllRestController::findAll - Request UUID: {} is not in cache - producing message", correlationId);
            try {
                ProductFindAllRequestDto dto = buildRequestDto(correlationId, limit, offset);
                productFindAllQueueProducer.produce(correlationId, dto);
                ProductFindAllCacheService.add(correlationId, List.of());
                ProductFindAllCacheService.display();
            } catch (Exception e) {
                log.error("ProductFindAllRestController::findAll - ProductFindAllRestController::findAll - Exception: ", e);
            }
            return ResponseEntity.ok(new IdResponse(correlationId));
        }

        log.info("ProductFindAllRestController::findAll - Request UUID: {} is not in cache", correlationId);
        var list = ProductFindAllCacheService.get(correlationId);
        if (list.isEmpty()) {
            log.info("ProductFindAllRestController::findAll - Find all - data for request UUID: {} is empty", correlationId);
        } else {
            log.info("ProductFindAllRestController::findAll - Data is found for request UUID: {}", correlationId);
        }
        return ResponseEntity.ok(list);
    }

    private ProductFindAllRequestDto buildRequestDto(String id, Integer limit, Integer offset) {
        return new ProductFindAllRequestDto(
                id,
                limit == null ? DEFAULT_LIMIT : limit,
                offset == null ? DEFAULT_OFFSET : offset
        );
    }

}
