package io.strata.client;

import io.strata.common.Endpoint;
import io.strata.common.FileId;
import io.strata.common.ErrorCode;
import io.strata.common.ConnectionPolicy;
import io.strata.common.ScpException;
import io.strata.proto.Messages;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetaClientTest {

    @Test
    void endpointParserCoversValidAndInvalidBoundaries() {
        Endpoint host = parseEndpoint("meta.local:2181");
        assertEquals("meta.local", host.host());
        assertEquals(2181, host.port());

        Endpoint ipv6 = parseEndpoint("[::1]:2181");
        assertEquals("::1", ipv6.host());
        assertEquals(2181, ipv6.port());

        assertEquals(ErrorCode.INTERNAL, parseFailure(null).code());
        assertEquals(ErrorCode.INTERNAL, parseFailure("host").code());
        assertEquals(ErrorCode.INTERNAL, parseFailure("host:").code());
        ScpException e = parseFailure("[::1:2181");
        assertEquals(ErrorCode.INTERNAL, e.code());
        assertEquals(ErrorCode.INTERNAL, parseFailure("host:not-a-port").code());
        assertEquals(ErrorCode.INTERNAL, parseFailure(" host:2181").code());
        assertEquals(ErrorCode.INTERNAL, parseFailure("[]:2181").code());
        assertEquals(ErrorCode.INTERNAL, parseFailure("host:0").code());
        assertEquals(ErrorCode.INTERNAL, parseFailure("host:-1").code());
        assertEquals(ErrorCode.INTERNAL, parseFailure("host:65536").code());
    }

    @Test
    void decodePropagatesTypedExceptionsWithoutWrapping() throws Exception {
        MetaClient meta = new MetaClient(new ClientConfig(List.of("127.0.0.1:1"), 1024, 100));
        ScpException expected = new ScpException(ErrorCode.NOT_LEADER, "standby");

        Throwable thrown = decodeFailure(meta, Opcode.CREATE_FILE, ignored -> {
            throw expected;
        });

        assertSame(expected, thrown);
    }

    @Test
    void malformedMetadataResponseDisconnectsManagedConnection() throws Exception {
        FileId id = FileId.random();
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                Opcode op = Opcode.fromCode(req.opcode());
                if (op == Opcode.CREATE_FILE) {
                    return ScpServer.ok(req, new Messages.CreateFileResp(id).encode(), null);
                }
                if (op == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             MetaClient meta = new MetaClient(new ClientConfig(List.of(endpoint(server)), 1024, 100))) {
            assertEquals(id, meta.createFile(StrataClient.FileSpec.log("test", "/test-file")));
            ManagedScpConnection connection = connectionField(meta);
            long before = connection.generation();

            Throwable thrown = decodeFailure(meta, Opcode.CREATE_FILE, ignored -> {
                throw new IllegalArgumentException("bad response");
            });

            assertEquals(ErrorCode.INTERNAL, ((ScpException) thrown).code());
            assertNotEquals(before, connection.generation());
        }
    }

    @Test
    void metadataCallRotatesOffNotLeaderEndpoint() throws Exception {
        AtomicInteger standbyCalls = new AtomicInteger();
        FileId id = FileId.random();
        try (ScpServer standby = new ScpServer(0, 1, 0, 0, req -> {
                standbyCalls.incrementAndGet();
                throw new ScpException(ErrorCode.NOT_LEADER, "standby");
             });
             ScpServer leader = new ScpServer(0, 1, 0, 0,
                     req -> ScpServer.ok(req, new Messages.CreateFileResp(id).encode(), null));
             MetaClient meta = new MetaClient(new ClientConfig(
                     List.of(endpoint(standby), endpoint(leader)), 1024, 100))) {

            assertEquals(id, meta.createFile(StrataClient.FileSpec.log("test", "/test-file")));

            assertEquals(1, standbyCalls.get());
        }
    }

    @Test
    void metadataCallUsesNextEndpointAfterConnectFailure() throws Exception {
        int deadPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            deadPort = socket.getLocalPort();
        }
        FileId id = FileId.random();
        try (ScpServer leader = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, new Messages.CreateFileResp(id).encode(), null))) {
            ConnectionPolicy policy = ConnectionPolicy.DEFAULT
                    .withConnectTimeoutMs(100)
                    .withHeartbeatIntervalMs(1_000);
            try (MetaClient meta = new MetaClient(new ClientConfig(
                    List.of("127.0.0.1:" + deadPort, endpoint(leader)), 1024, 200, policy))) {

                assertEquals(id, meta.createFile(StrataClient.FileSpec.log("test", "/test-file")));
            }
        }
    }

    @Test
    void metadataCallRetriesRetriableErrorsWithoutRotating() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        FileId id = FileId.random();
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                if (calls.getAndIncrement() == 0) {
                    throw new ScpException(ErrorCode.NO_CAPACITY, "warming up");
                }
                return ScpServer.ok(req, new Messages.CreateFileResp(id).encode(), null);
             });
             MetaClient meta = new MetaClient(new ClientConfig(List.of(endpoint(server)), 1024, 100))) {

            assertEquals(id, meta.createFile(StrataClient.FileSpec.log("test", "/test-file")));

            assertEquals(2, calls.get());
        }
    }

    @Test
    void metadataHeartbeatReconnectsToNextEndpointAfterLeaderConnectionDies() throws Exception {
        FileId firstId = FileId.random();
        FileId secondId = FileId.random();
        AtomicInteger secondPings = new AtomicInteger();
        try (ScpServer first = new ScpServer(0, 1, 0, 0, req -> {
                Opcode op = Opcode.fromCode(req.opcode());
                if (op == Opcode.CREATE_FILE) {
                    return ScpServer.ok(req, new Messages.CreateFileResp(firstId).encode(), null);
                }
                if (op == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer second = new ScpServer(0, 1, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_FILE) {
                     return ScpServer.ok(req, new Messages.CreateFileResp(secondId).encode(), null);
                 }
                 if (op == Opcode.PING) {
                     secondPings.incrementAndGet();
                     return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ConnectionPolicy policy = new ConnectionPolicy(500, 50, 50, 10_000, 50, 200);
            ClientConfig config = new ClientConfig(List.of(endpoint(first), endpoint(second)), 1024, 500, policy);
            try (MetaClient meta = new MetaClient(config)) {
                assertEquals(firstId, meta.createFile(StrataClient.FileSpec.log("test", "/first")));
                first.close();

                waitFor(() -> secondPings.get() > 0);

                assertEquals(secondId, meta.createFile(StrataClient.FileSpec.log("test", "/second")));
            }
        }
    }

    @Test
    void metadataCallDoesNotRetryPermanentErrors() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                calls.incrementAndGet();
                throw new ScpException(ErrorCode.FILE_NOT_FOUND, "missing");
             });
             MetaClient meta = new MetaClient(new ClientConfig(List.of(endpoint(server)), 1024, 100))) {

            ScpException e = assertThrows(ScpException.class,
                    () -> meta.lookupFile(FileId.random()));

            assertEquals(ErrorCode.FILE_NOT_FOUND, e.code());
            assertEquals(1, calls.get());
        }
    }

    @Test
    void interruptedRetryRestoresInterruptAndRethrowsLastError() throws Exception {
        int unusedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            unusedPort = socket.getLocalPort();
        }
        MetaClient meta = new MetaClient(new ClientConfig(List.of("127.0.0.1:" + unusedPort), 1024, 100));
        Thread.currentThread().interrupt();
        try {
            ScpException e = assertThrows(ScpException.class,
                    () -> meta.createFile(StrataClient.FileSpec.log("test", "/test-file")));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            meta.close();
        }
    }

    private static ScpException parseFailure(String endpoint) {
        return assertThrows(ScpException.class, () -> parseEndpoint(endpoint));
    }

    private static Endpoint parseEndpoint(String endpoint) {
        return Endpoint.parse(endpoint, "metadata endpoint", ErrorCode.INTERNAL);
    }

    private static Throwable decodeFailure(MetaClient meta, Opcode op,
                                           Function<ByteBuffer, ?> decoder) throws Exception {
        Method method = MetaClient.class.getDeclaredMethod("decode", Opcode.class, ByteBuffer.class, Function.class);
        method.setAccessible(true);
        try {
            method.invoke(meta, op, ByteBuffer.allocate(0), decoder);
            throw new AssertionError("expected decoder failure");
        } catch (InvocationTargetException e) {
            return e.getCause();
        }
    }

    private static ManagedScpConnection connectionField(MetaClient meta) throws ReflectiveOperationException {
        Field field = MetaClient.class.getDeclaredField("connection");
        field.setAccessible(true);
        return (ManagedScpConnection) field.get(meta);
    }

    private static String endpoint(ScpServer server) {
        return "127.0.0.1:" + server.port();
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("condition not met before deadline");
    }
}
