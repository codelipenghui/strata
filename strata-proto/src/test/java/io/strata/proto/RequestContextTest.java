package io.strata.proto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestContextTest {

    @Test
    void takeReturnsSetValueThenClears() {
        RequestContext.setNamespace("orders");
        assertEquals("orders", RequestContext.takeNamespace());
        assertEquals("-", RequestContext.takeNamespace(), "take must clear; default is \"-\"");
    }

    @Test
    void takeDefaultsToDashWhenUnset() {
        // Ensure no leakage from a prior test on this thread.
        RequestContext.takeNamespace();
        assertEquals("-", RequestContext.takeNamespace());
    }
}
