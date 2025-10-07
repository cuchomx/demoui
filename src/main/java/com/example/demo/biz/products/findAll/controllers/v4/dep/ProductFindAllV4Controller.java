package com.example.demo.biz.products.findAll.controllers.v4.dep;

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
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v4/products")
public class ProductFindAllV4Controller {

    private static final int DEFAULT_LIMIT = 10;
    private static final int DEFAULT_OFFSET = 0;
    private static final int MIN_LIMIT = 1;
    private static final int MIN_OFFSET = 0;

    private final IProductFindAllQueueProducer productFindAllQueueProducer;
    private final IProductFindAllV4QueueService productFindAllV4QueueService;

    private final Object lock = new Object();

    @GetMapping("/future")
    public HttpEntity<List<ProductResponseDto>> findAll(
            @RequestParam("uuid") String correlationId,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset
    ) {
        log.info("ProductFindAllV4Controller::findAll - Request UUID: {}", correlationId);

        ResponseEntity<List<ProductResponseDto>> validationError = validateRequest(correlationId);
        if (validationError != null) {
            return validationError;
        }

        IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.RECEIVED);
        ProductFindAllRequestDto requestDto = buildRequestDto(correlationId, limit, offset);

        try {
            log.info("ProductFindAllV4Controller::findAll - Producing for correlationId: {}", correlationId);
            productFindAllQueueProducer.produce(correlationId, requestDto);

            log.info("ProductFindAllV4Controller::findAll - Consuming for correlationId: {}", correlationId);
            List<ProductResponseDto> products = products = productFindAllV4QueueService.consume(correlationId);

            if (products == null || products.isEmpty()) {
                log.warn("ProductFindAllV4Controller::findAll - No products yet for correlationId: {} returning accepted", correlationId);
                return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
            }

            log.info("ProductFindAllV4Controller::findAll - Returning {} products for correlationId: {}", products.size(), correlationId);

            return ResponseEntity.ok(products);
        } catch (Exception e) {
            log.error("ProductFindAllV4Controller::findAll - Error producing/consuming for {}", correlationId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(List.of());
        } finally {
            IdempotentRequestCache.INSTANCE.remove(correlationId);
        }
    }

    private ResponseEntity<List<ProductResponseDto>> validateRequest(String correlationId) {
        if (ParameterValidationUtils.isNotValidCorrelationId(correlationId)) {
            log.error("ProductFindAllV4Controller::findAll - Invalid correlation id value - {}", correlationId);
            return ResponseEntity.badRequest().body(List.of());
        }
        if (IdempotentRequestCache.INSTANCE.isInProgress(correlationId)) {
            log.info("ProductFindAllV4Controller::findAll - correlationId {} is in progress", correlationId);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
        }
        log.info("ProductFindAllV4Controller::findAll - Generated from product correlationId: {}", correlationId);
        return null;
    }

    private ProductFindAllRequestDto buildRequestDto(String correlationId, Integer limit, Integer offset) {
        int normalizedLimit = (limit == null || limit < MIN_LIMIT) ? DEFAULT_LIMIT : limit;
        int normalizedOffset = (offset == null || offset < MIN_OFFSET) ? DEFAULT_OFFSET : offset;
        return new ProductFindAllRequestDto(correlationId, normalizedLimit, normalizedOffset);
    }

    //----

    private boolean completed = true;

    @GetMapping("/monitor")
    public HttpEntity<List<ProductResponseDto>> findAllRequest(
            @RequestParam("uuid") String correlationId,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset
    ) {
        log.info("ProductFindAllV4Controller::findAll - Request UUID: {}", correlationId);

        ResponseEntity<List<ProductResponseDto>> validationError = validateRequest(correlationId);
        if (validationError != null) {
            return validationError;
        }

        IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.RECEIVED);
        ProductFindAllRequestDto requestDto = buildRequestDto(correlationId, limit, offset);

        try {
            log.info("ProductFindAllV4Controller::findAll - Producing for correlationId: {}", correlationId);
            productFindAllQueueProducer.produce(correlationId, requestDto);

            Thread thread = Thread.currentThread();
            log.info("ProductFindAllV4Controller::findAll - current thread: {}", thread);

            AtomicReference<List<ProductResponseDto>> products = new AtomicReference<>();
            completed = false;

            new Thread(() -> {
                log.info("ProductFindAllV4Controller::findAll - new thread: {}", Thread.currentThread());
                try {
                    log.info("ProductFindAllV4Controller::findAll - Consuming for correlationId: {}", correlationId);
                    List<ProductResponseDto> result = productFindAllV4QueueService.consume(correlationId);
                    products.set(result);
                    synchronized (lock) {
                        log.info("ProductFindAllV4Controller::findAll - Completed for correlationId: {}", correlationId);
                        completed = true;
                        lock.notifyAll();
                    }
                } catch (Exception e) {
                    log.error("ProductFindAllV4Controller::findAll - Error consuming for correlationId: {}", correlationId, e);
                    synchronized (lock) {
                        completed = true;
                        lock.notifyAll();
                    }
                    throw new RuntimeException(e);
                }
            }).start();

            synchronized (lock) {
                while (!completed) {
                    try {
                        log.info("ProductFindAllV4Controller::findAll - Waiting for response for correlationId: {}", correlationId);
                        lock.wait(10000); // Add timeout of 10 seconds
                        if (!completed) {
                            log.warn("ProductFindAllV4Controller::findAll - Timeout waiting for response for correlationId: {}", correlationId);
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("ProductFindAllV4Controller::findAll - Thread Interrupted", e);
                        break;
                    }
                }
            }

            log.info("ProductFindAllV4Controller::findAll - Completed for correlationId: {}", correlationId);

            if (products.get() == null || products.get().isEmpty()) {
                log.warn("ProductFindAllV4Controller::findAll - No products yet for correlationId: {} returning accepted", correlationId);
                return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
            }

            log.info("ProductFindAllV4Controller::findAll - Returning {} products for correlationId: {}", products.get().size(), correlationId);

            return ResponseEntity.ok(products.get());
        } catch (Exception e) {
            log.error("ProductFindAllV4Controller::findAll - Error producing/consuming for {}", correlationId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(List.of());
        } finally {
            IdempotentRequestCache.INSTANCE.remove(correlationId);
        }
    }


    @GetMapping("mv1")
    public HttpEntity<List<ProductResponseDto>> findAllRequestThread(
            @RequestParam("uuid") String correlationId,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset
    ) {
        log.info("ProductFindAllV4Controller::findAll - Request UUID: {}", correlationId);

        ResponseEntity<List<ProductResponseDto>> validationError = validateRequest(correlationId);
        if (validationError != null) {
            return validationError;
        }

        IdempotentRequestCache.INSTANCE.putIfAbsent(correlationId, IdempotentRequestCache.Status.RECEIVED);
        ProductFindAllRequestDto requestDto = buildRequestDto(correlationId, limit, offset);

        try {
            log.info("ProductFindAllV4Controller::findAll - Producing for correlationId: {}", correlationId);
            productFindAllQueueProducer.produce(correlationId, requestDto);

            completed = false;
            AtomicReference<List<ProductResponseDto>> products = new AtomicReference<>();

            new Thread(() -> {
                try {
                    log.info("ProductFindAllV4Controller::findAll - Consuming for correlationId: {}", correlationId);
                    List<ProductResponseDto> result = productFindAllV4QueueService.consume(correlationId);
                    products.set(result);

                    synchronized (this) {
                        log.info("ProductFindAllV4Controller::findAll - Completed for correlationId: {}", correlationId);
                        completed = true;
                        notifyAll();
                    }
                } catch (Exception e) {
                    log.error("ProductFindAllV4Controller::findAll - Error consuming for correlationId: {}", correlationId, e);
                    synchronized (this) {
                        completed = true;
                        notifyAll();
                    }
                    throw new RuntimeException(e);
                }
            }).start();

            try {
                synchronized (this) {
                    while (!completed) {
                        log.info("ProductFindAllV4Controller::findAll - Waiting for response for correlationId: {}", correlationId);
                        try {
                            wait(10000);
                            if (!completed) {
                                log.warn("ProductFindAllV4Controller::findAll - Timeout waiting for response for correlationId: {}", correlationId);
                                break;
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            log.error("ProductFindAllV4Controller::findAll - Thread Interrupted", e);
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                log.error("ProductFindAllV4Controller::findAll - Error waiting for response for correlationId: {}", correlationId, e);
                throw new RuntimeException(e);
            }

            log.info("ProductFindAllV4Controller::findAll - Completed for correlationId: {}", correlationId);

            if (products.get() == null || products.get().isEmpty()) {
                log.warn("ProductFindAllV4Controller::findAll - No products yet for correlationId: {} returning accepted", correlationId);
                return ResponseEntity.status(HttpStatus.ACCEPTED).body(List.of());
            }

            log.info("ProductFindAllV4Controller::findAll - Returning {} products for correlationId: {}", products.get().size(), correlationId);

            return ResponseEntity.ok(products.get());
        } catch (Exception e) {
            log.error("ProductFindAllV4Controller::findAll - Error producing/consuming for {}", correlationId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(List.of());
        } finally {
            IdempotentRequestCache.INSTANCE.remove(correlationId);
        }
    }
}
