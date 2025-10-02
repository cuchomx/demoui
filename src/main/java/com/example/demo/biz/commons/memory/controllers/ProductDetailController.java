package com.example.demo.biz.commons.memory.controllers;

import com.example.demo.biz.commons.memory.services.IInMemoryProductService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/products")
public class ProductDetailController {

    private final IInMemoryProductService IInMemoryProductService;

    public ProductDetailController(IInMemoryProductService IInMemoryProductService) {
        this.IInMemoryProductService = IInMemoryProductService;
    }

    @GetMapping("/{id}")
    public String detail(@PathVariable("id") Long id, Model model) {
        model.addAttribute("product", IInMemoryProductService.findById(id));
        return "products/detail";
    }
}
