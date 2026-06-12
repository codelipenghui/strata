package io.strata.client;

import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;

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
    void endpointParserHandlesBracketedIpv6AndNullGuard() {
        Endpoint parsed = Endpoint.parse("[::1]:1234", "storage endpoint", ErrorCode.INTERNAL);
        assertEquals("::1", parsed.host());
        assertEquals(1234, parsed.port());

        ScpException e = assertThrows(ScpException.class,
                () -> Endpoint.parse(null, "storage endpoint", ErrorCode.INTERNAL));
        assertEquals(ErrorCode.INTERNAL, e.code());
    }
}
