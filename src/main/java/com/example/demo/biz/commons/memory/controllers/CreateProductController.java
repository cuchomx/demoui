package com.example.demo.biz.commons.memory.controllers;

import com.example.commons.constants.RequestStatus;
import com.example.commons.dto.create.ProductRequestDto;
import com.example.commons.dto.create.ProductResponseDto;
import com.example.demo.biz.commons.memory.services.IInMemoryProductService;
import com.example.demo.biz.products.create.cache.ProductCacheService;
import com.example.demo.biz.products.create.queues.producer.IProductCreateQueueProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Slf4j
@RequiredArgsConstructor
@Controller
@RequestMapping("/products")
class CreateProductController {

    private final IInMemoryProductService inMemoryProductService;
    private final IProductCreateQueueProducer productCreateQueueProducer;

    @Value("${app.create.memory.enabled:false}")
    private Boolean useInMemoryEnabled;

    @PostMapping
    public String create(
            @Valid @ModelAttribute("product") ProductRequestDto product,
            BindingResult bindingResult,
            RedirectAttributes redirectAttributes
    ) {

        if (bindingResult.hasErrors()) {
            redirectAttributes.addFlashAttribute("org.springframework.validation.BindingResult.product", bindingResult);
            redirectAttributes.addFlashAttribute("product", product);
            return "redirect:/products/new";
        }

        String id;
        if (useInMemoryEnabled) {
            id = processCreateProductInMemory(product, redirectAttributes);
            return "redirect:/products/" + id;
        } else {
            id = processCreateProductToQueue(product, redirectAttributes);
            log.info("Temporary from controller - Product Id: {}", id);
            return id;
        }
    }

    private String processCreateProductInMemory(
            ProductRequestDto product,
            RedirectAttributes redirectAttributes
    ) {
        ProductResponseDto created = inMemoryProductService.create(product);
        redirectAttributes.addFlashAttribute("message", "Product created successfully");
        return created.getId().toString();
    }

    private String processCreateProductToQueue(
            ProductRequestDto product,
            RedirectAttributes redirectAttributes
    ) {
        var uuid = product.getId();
        if (uuid == null) {
            throw new IllegalArgumentException("No correlation Id - Product id must be provided");
        }
        log.info("Create product - Generated UUID: {}", uuid);
        productCreateQueueProducer.produce(uuid, product);
        ProductCacheService.add(uuid, RequestStatus.IN_PROGRESS);
        ProductCacheService.display();
        redirectAttributes.addFlashAttribute("message", "Product request pending");
        return uuid;
    }
}
