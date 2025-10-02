package com.example.demo.biz.unit.commons.exceptions;

import com.example.demo.biz.commons.exceptions.GlobalExceptionHandler;
import org.junit.jupiter.api.Test;
import org.springframework.ui.ConcurrentModel;
import org.springframework.ui.Model;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GlobalExceptionHandlerTests {

    @Test
    void handleGenericPopulatesModelAndReturnsErrorView() {
        GlobalExceptionHandler handler = new GlobalExceptionHandler();
        Model model = new ConcurrentModel();
        String view = handler.handleGeneric(new RuntimeException("boom"), model);
        assertEquals("error", view);
        assertEquals(500, model.getAttribute("status"));
        assertEquals("Internal Server Error", model.getAttribute("error"));
        assertEquals("boom", model.getAttribute("message"));
    }
}
