package com.example.demo.biz.products.create.controllers;

import com.example.commons.dto.create.ProductRequestDto;
import com.example.commons.utils.ParameterValidationUtils;
import com.example.demo.biz.commons.cache.IdempotentRequestCache;
import com.example.demo.biz.commons.dto.IdResponse;
import com.example.demo.biz.products.create.cache.ProductCacheService;
import com.example.demo.biz.products.create.queues.producer.IProductCreateQueueProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import static com.example.commons.constants.RequestStatus.IN_PROGRESS;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/product")
public class ProductCreateRestController {

    private final IProductCreateQueueProducer productCreateQueueProducer;

    @GetMapping(path = "/{id}", produces = "application/json")
    public HttpEntity<?> getCreatedId(@PathVariable String id) {
        log.info("=================================================================================================");

        if (!ParameterValidationUtils.isValidCorrelationIdValue(id)) {
            log.error("ProductCreateRestController::getCreatedId - Invalid correlation id value - {}", id);
            return ResponseEntity.badRequest().build();
        }

        if (IdempotentRequestCache.INSTANCE.isInProgress(id)) {
            log.info("ProductCreateRestController::getCreatedId - correlation id is in progress");
            return ResponseEntity.ok().build();
        }

        log.info("ProductCreateRestController::getCreatedId - correlation id is not in cache");
        IdempotentRequestCache.INSTANCE.putIfAbsent(id, IdempotentRequestCache.Status.RECEIVED);

        Object productId = ProductCacheService.get(id);

        log.info("ProductCreateRestController::getCreatedId - product - id: {} value is: {}", id, productId);

        if (productId == null || productId.equals(IN_PROGRESS)) {
            log.info("ProductCreateRestController::getCreatedId - product id is null or IN_PROGRESS");
            return ResponseEntity.ok().build();
        }

        log.info("ProductCreateRestController::getCreatedId - product id: {}", productId);
        IdempotentRequestCache.INSTANCE.remove(id);
        return new ResponseEntity<>(productId, HttpStatus.CREATED);
    }

    @PostMapping
    public ResponseEntity<?> create(
            @RequestParam("uuid") String correlationId,
            @Valid ProductRequestDto product,
            BindingResult bindingResult
    ) {
        log.info("=================================================================================================");

        if (bindingResult.hasErrors()) {
            log.error("ProductCreateRestController::create - Invalid product request");
            throw new IllegalStateException("Invalid product request");
        }

        if (!ParameterValidationUtils.isValidCorrelationIdValue(correlationId)) {
            log.error("ProductCreateRestController::create product id is null");
            throw new IllegalArgumentException("No correlation Id - Product id must be provided");
        }

        if (IdempotentRequestCache.INSTANCE.isInProgress(correlationId)) {
            log.info("ProductCreateRestController::create - correlation id is in progress");
            return ResponseEntity.ok().build();
        }

        log.info("ProductCreateRestController::create - Generated from product UUID: {}", correlationId);

        IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.RECEIVED);

        try {
            productCreateQueueProducer.produce(correlationId, product);
            ProductCacheService.add(correlationId, IN_PROGRESS);
            ProductCacheService.display();
        } catch (Exception e) {
            log.error("ProductCreateRestController::create - Exception: ", e);
        } finally {
            IdempotentRequestCache.INSTANCE.remove(correlationId);
        }

        return ResponseEntity.ok(new IdResponse(correlationId));
    }

}
