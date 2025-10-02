package com.example.demo.biz.commons.memory.services;

import com.example.commons.dto.create.ProductRequestDto;
import com.example.commons.dto.create.ProductResponseDto;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class InMemoryProductService implements IInMemoryProductService {

    private final Map<Long, ProductResponseDto> store = new LinkedHashMap<>();
    private final AtomicLong idGen = new AtomicLong(0);

    @PostConstruct
    public void initMockData() {
        // Seed some mock products for testing
        create(ProductRequestDto.builder()
                .name("Wireless Mouse")
                .description("Ergonomic wireless mouse with 2.4GHz receiver")
                .price(new BigDecimal("19.99"))
                .quantity(150)
                .category("Electronics")
                .build());

        create(ProductRequestDto.builder()
                .name("Running Shoes")
                .description("Lightweight shoes for daily training")
                .price(new BigDecimal("59.90"))
                .quantity(80)
                .category("Sports")
                .build());

        create(ProductRequestDto.builder()
                .name("Cookbook")
                .description("100 easy recipes for busy people")
                .price(new BigDecimal("24.50"))
                .quantity(40)
                .category("Books")
                .build());
    }

    @Override
    public List<ProductResponseDto> findAll() {
        return Collections.unmodifiableList(new ArrayList<>(store.values()));
    }

    @Override
    public ProductResponseDto findById(Long id) {
        ProductResponseDto dto = store.get(id);
        if (dto == null) {
            throw new RuntimeException("Product with id=" + id + " not found");
        }
        return dto;
    }

    @Override
    public ProductResponseDto create(ProductRequestDto request) {
        long id = idGen.incrementAndGet();
        LocalDateTime now = LocalDateTime.now();
        ProductResponseDto response = ProductResponseDto.builder()
                .id(id)
                .name(request.getName())
                .description(request.getDescription())
                .price(request.getPrice())
                .quantity(request.getQuantity())
                .category(request.getCategory())
                .active(true)
                .createdAt(now)
                .updatedAt(now)
                .build();
        store.put(id, response);
        return response;
    }
}
