package com.example.demo.biz.commons.memory.services;


import com.example.commons.dto.create.ProductRequestDto;
import com.example.commons.dto.create.ProductResponseDto;

import java.util.List;

public interface IInMemoryProductService {

    List<ProductResponseDto> findAll();

    ProductResponseDto findById(Long id);

    ProductResponseDto create(ProductRequestDto request);

}
