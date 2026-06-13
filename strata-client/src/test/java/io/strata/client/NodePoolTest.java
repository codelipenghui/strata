package io.strata.client;

import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.proto.BufWriter;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void defaultPoolKeepsOneConnectionPerEndpoint() {
        try (NodePool pool = new NodePool(new ClientConfig(List.of("127.0.0.1:1"), 1024, 500))) {
            ManagedScpConnection first = pool.get("127.0.0.1:1234");
            ManagedScpConnection second = pool.get("127.0.0.1:1234");

            assertSame(first, second);
        }
    }

    @Test
    void configuredPoolRoundRobinsWithinEndpoint() {
        ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500)
                .withStorageConnectionsPerEndpoint(3);
        try (NodePool pool = new NodePool(config)) {
            ManagedScpConnection first = pool.get("127.0.0.1:1234");
            ManagedScpConnection second = pool.get("127.0.0.1:1234");
            ManagedScpConnection third = pool.get("127.0.0.1:1234");

            assertNotSame(first, second);
            assertNotSame(second, third);
            assertNotSame(first, third);
            assertSame(first, pool.get("127.0.0.1:1234"));
            assertSame(second, pool.get("127.0.0.1:1234"));
        }
    }

    @Test
    void closeShutsDownEveryPooledConnection() {
        ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500)
                .withStorageConnectionsPerEndpoint(2);
        ManagedScpConnection first;
        ManagedScpConnection second;
        NodePool pool = new NodePool(config);
        try {
            first = pool.get("127.0.0.1:1234");
            second = pool.get("127.0.0.1:1234");
        } finally {
            pool.close();
        }

        assertThrows(ScpException.class, () -> first.call(Opcode.PING, emptyHeader(), null, 500));
        assertThrows(ScpException.class, () -> second.call(Opcode.PING, emptyHeader(), null, 500));
    }

    @Test
    void closeStopsEveryPooledConnectionMonitorThread() throws Exception {
        ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500)
                .withStorageConnectionsPerEndpoint(3);
        String endpoint = "127.0.0.1:1234";
        ManagedScpConnection[] connections;
        NodePool pool = new NodePool(config);
        try {
            pool.get(endpoint);
            pool.get(endpoint);
            pool.get(endpoint);
            connections = pooledConnections(pool, endpoint);
            assertEquals(3, connections.length);
        } finally {
            pool.close();
        }

        waitFor(() -> {
            for (ManagedScpConnection connection : connections) {
                if (monitorAlive(connection)) {
                    return false;
                }
            }
            return true;
        });
    }

    @Test
    void replacesClosedStorageConnectionOnNextUse() throws Exception {
        AtomicInteger pings = new AtomicInteger();
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                    pings.incrementAndGet();
                    return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
             });
             NodePool pool = new NodePool(new ClientConfig(List.of("127.0.0.1:1"), 1024, 500,
                     new ConnectionPolicy(500, 1_000, 100, 10_000, 50, 200)))) {
            ManagedScpConnection conn = pool.get(endpoint(server));
            conn.call(Opcode.PING, emptyHeader(), null, 500);
            long firstGeneration = conn.generation();

            conn.disconnect();
            conn.call(Opcode.PING, emptyHeader(), null, 500);

            assertNotEquals(firstGeneration, conn.generation());
            assertEquals(2, pings.get());
        }
    }

    private static byte[] emptyHeader() {
        return new BufWriter(4).noTags().toBytes();
    }

    private static String endpoint(ScpServer server) {
        return "127.0.0.1:" + server.port();
    }

    @SuppressWarnings("unchecked")
    private static ManagedScpConnection[] pooledConnections(NodePool pool, String endpoint) throws Exception {
        Field connsField = NodePool.class.getDeclaredField("conns");
        connsField.setAccessible(true);
        Map<String, ?> conns = (Map<String, ?>) connsField.get(pool);
        Object endpointPool = conns.get(endpoint);
        Field connectionsField = endpointPool.getClass().getDeclaredField("connections");
        connectionsField.setAccessible(true);
        return (ManagedScpConnection[]) connectionsField.get(endpointPool);
    }

    private static boolean monitorAlive(ManagedScpConnection connection) throws Exception {
        Method method = ManagedScpConnection.class.getDeclaredMethod("monitorAliveForTests");
        method.setAccessible(true);
        return (Boolean) method.invoke(connection);
    }

    private static void waitFor(CheckedBoolean condition) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "condition not met before deadline");
    }

    private interface CheckedBoolean {
        boolean getAsBoolean() throws Exception;
    }
}
