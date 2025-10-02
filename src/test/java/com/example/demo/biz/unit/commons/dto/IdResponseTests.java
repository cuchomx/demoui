package com.example.demo.biz.unit.commons.dto;

import com.example.demo.biz.commons.dto.IdResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class IdResponseTests {

    @Test
    void recordAccessorsAndEqualityWork() {
        IdResponse a = new IdResponse("abc");
        IdResponse b = new IdResponse("abc");
        IdResponse c = new IdResponse("xyz");

        assertEquals("abc", a.id());
        assertEquals(a, b);
        assertNotEquals(a, c);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
