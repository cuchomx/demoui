package com.example.demo.biz.commons.memory.controllers;

import com.example.commons.dto.create.ProductRequestDto;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.UUID;

@Controller
@RequestMapping("/products")
public class ShowCreateProductFormController {

    @GetMapping("/new")
    public String showCreateForm(Model model) {
        if (!model.containsAttribute("product")) {
            var productRequestDto = new ProductRequestDto();
            productRequestDto.setId(UUID.randomUUID().toString());
            model.addAttribute("product", productRequestDto);
        }
        return "products/create";
    }
}
