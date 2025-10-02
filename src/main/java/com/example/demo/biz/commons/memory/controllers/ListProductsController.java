package com.example.demo.biz.commons.memory.controllers;

import com.example.demo.biz.commons.memory.services.IInMemoryProductService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/products")
public class ListProductsController {

    private final IInMemoryProductService IInMemoryProductService;

    public ListProductsController(IInMemoryProductService IInMemoryProductService) {
        this.IInMemoryProductService = IInMemoryProductService;
    }

    @GetMapping
    public String list(Model model) {
        model.addAttribute("products", IInMemoryProductService.findAll());
        return "products/list";
    }
}
