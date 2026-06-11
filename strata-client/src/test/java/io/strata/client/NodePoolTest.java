package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NodePoolTest {

    @Test
    void rejectsMalformedStorageEndpointsAsScpErrors() {
        try (NodePool pool = new NodePool()) {
            for (String endpoint : List.of("missing-port", "localhost:", ":1234",
                    " localhost:1234", "localhost :1234", "localhost:not-a-port",
                    "localhost:0", "localhost:65536", "[::1:1234")) {
                ScpException e = assertThrows(ScpException.class, () -> pool.get(endpoint), endpoint);
                assertEquals(ErrorCode.INTERNAL, e.code());
            }
        }
    }

    @Test
    void rejectsNullStorageEndpointAsScpError() {
        try (NodePool pool = new NodePool()) {
            ScpException e = assertThrows(ScpException.class, () -> pool.get(null));
            assertEquals(ErrorCode.INTERNAL, e.code());
        }
    }

    @Test
    void endpointParserHandlesBracketedIpv6AndNullGuard() throws Exception {
        Object parsed = parseEndpoint("[::1]:1234");
        Method host = parsed.getClass().getDeclaredMethod("host");
        Method port = parsed.getClass().getDeclaredMethod("port");
        host.setAccessible(true);
        port.setAccessible(true);
        assertEquals("::1", host.invoke(parsed));
        assertEquals(1234, port.invoke(parsed));

        ScpException e = parseFailure(null);
        assertEquals(ErrorCode.INTERNAL, e.code());
    }

    private static Object parseEndpoint(String endpoint) throws Exception {
        Method method = NodePool.class.getDeclaredMethod("parseEndpoint", String.class);
        method.setAccessible(true);
        return method.invoke(null, endpoint);
    }

    private static ScpException parseFailure(String endpoint) throws Exception {
        try {
            parseEndpoint(endpoint);
            throw new AssertionError("expected parser failure");
        } catch (InvocationTargetException e) {
            return (ScpException) e.getCause();
        }
    }
}
