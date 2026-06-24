package io.strata.client;

import io.strata.common.Endpoint;
import io.strata.common.FileId;
import io.strata.common.ErrorCode;
import io.strata.common.ConnectionPolicy;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControllerClientTest {

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
        ControllerClient meta = new ControllerClient(new ClientConfig(List.of("127.0.0.1:1"), 1024, 100));
        ScpException expected = new ScpException(ErrorCode.NOT_LEADER, "standby");

        Throwable thrown = decodeFailure(meta, Opcode.CREATE_FILE, ignored -> {
            throw expected;
        });

        assertSame(expected, thrown);
    }

    @Test
    void malformedMetadataResponseSurfacesAsInternal() throws Exception {
        FileId id = FileId.of(1);
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
             ControllerClient meta = new ControllerClient(new ClientConfig(List.of(endpoint(server)), 1024, 100))) {
            assertEquals(id, meta.createFile(StrataClient.FileSpec.log("test", "/test-file")));

            Throwable thrown = decodeFailure(meta, Opcode.CREATE_FILE, ignored -> {
                throw new IllegalArgumentException("bad response");
            });

            assertEquals(ErrorCode.INTERNAL, ((ScpException) thrown).code());
        }
    }

    @Test
    void metadataCallRotatesOffNotLeaderEndpoint() throws Exception {
        AtomicInteger standbyCalls = new AtomicInteger();
        FileId id = FileId.of(2);
        try (ScpServer standby = new ScpServer(0, 1, 0, 0, req -> {
                standbyCalls.incrementAndGet();
                throw new ScpException(ErrorCode.NOT_LEADER, "standby");
             });
             ScpServer leader = new ScpServer(0, 1, 0, 0,
                     req -> ScpServer.ok(req, new Messages.CreateFileResp(id).encode(), null));
             ControllerClient meta = new ControllerClient(new ClientConfig(
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
        FileId id = FileId.of(3);
        try (ScpServer leader = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, new Messages.CreateFileResp(id).encode(), null))) {
            ConnectionPolicy policy = ConnectionPolicy.DEFAULT
                    .withConnectTimeoutMs(100)
                    .withHeartbeatIntervalMs(1_000);
            try (ControllerClient meta = new ControllerClient(new ClientConfig(
                    List.of("127.0.0.1:" + deadPort, endpoint(leader)), 1024, 200, policy))) {

                assertEquals(id, meta.createFile(StrataClient.FileSpec.log("test", "/test-file")));
            }
        }
    }

    @Test
    void metadataCallRetriesRetriableErrorsWithoutRotating() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        FileId id = FileId.of(4);
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                if (calls.getAndIncrement() == 0) {
                    throw new ScpException(ErrorCode.NO_CAPACITY, "warming up");
                }
                return ScpServer.ok(req, new Messages.CreateFileResp(id).encode(), null);
             });
             ControllerClient meta = new ControllerClient(new ClientConfig(List.of(endpoint(server)), 1024, 100))) {

            assertEquals(id, meta.createFile(StrataClient.FileSpec.log("test", "/test-file")));

            assertEquals(2, calls.get());
        }
    }

    @Test
    void clientFailsOverToAnotherControllerWhenOneDies() throws Exception {
        // Owner-aware client: the first call goes to the first seed; once that controller dies, a retriable
        // transport failure advances to the next controller, so the second call still succeeds.
        FileId firstId = FileId.of(5);
        FileId secondId = FileId.of(6);
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
                     return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ConnectionPolicy policy = new ConnectionPolicy(500, 50, 50, 10_000, 50, 200);
            ClientConfig config = new ClientConfig(List.of(endpoint(first), endpoint(second)), 1024, 500, policy);
            try (ControllerClient meta = new ControllerClient(config)) {
                assertEquals(firstId, meta.createFile(StrataClient.FileSpec.log("test", "/first")));
                first.close();
                assertEquals(secondId, meta.createFile(StrataClient.FileSpec.log("test", "/second")),
                        "after the first controller dies, the client fails over to the second");
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
             ControllerClient meta = new ControllerClient(new ClientConfig(List.of(endpoint(server)), 1024, 100))) {

            ScpException e = assertThrows(ScpException.class,
                    () -> meta.lookupFile(StrataNamespace.of("test"), FileId.of(7)));

            assertEquals(ErrorCode.FILE_NOT_FOUND, e.code());
            assertEquals(1, calls.get());
        }
    }

    @Test
    void lookupFileCarriesNamespaceAsFirstWireField() throws Exception {
        // Routing test: the namespace passed to lookupFile must appear as the first decoded field
        // in the LOOKUP_FILE request that arrives at the controller — the server uses it to route
        // to the correct namespace owner.
        StrataNamespace ns = StrataNamespace.of("tenant-x");
        FileId fileId = FileId.of(8);
        Messages.LookupFileResp stubResp = new Messages.LookupFileResp(
                ns.toString(), "/some/path", Messages.WritePolicy.DEFAULT, (byte) 0, List.of());

        AtomicInteger calls = new AtomicInteger();
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                Opcode op = Opcode.fromCode(req.opcode());
                if (op == Opcode.LOOKUP_FILE) {
                    calls.incrementAndGet();
                    Messages.LookupFile decoded = Messages.LookupFile.decode(req.headerSlice());
                    assertEquals(ns, decoded.namespace(), "namespace must be first wire field");
                    assertEquals(fileId, decoded.fileId(), "fileId must follow namespace");
                    return ScpServer.ok(req, stubResp.encode(), null);
                }
                if (op == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ControllerClient meta = new ControllerClient(new ClientConfig(List.of(endpoint(server)), 1024, 200))) {

            Messages.LookupFileResp result = meta.lookupFile(ns, fileId);

            assertEquals(1, calls.get(), "server must have received exactly one LOOKUP_FILE request");
            assertEquals(ns, result.namespace());
        }
    }

    @Test
    void fileScopedOpRoutesByNamespaceAndFollowsRedirect() throws Exception {
        StrataNamespace ns = StrataNamespace.of("tenant-x");
        FileId id = FileId.of(9);
        AtomicInteger standbyCalls = new AtomicInteger();
        try (ScpServer standby = new ScpServer(0, 1, 0, 0, req -> {
                standbyCalls.incrementAndGet();
                throw new ScpException(ErrorCode.NOT_LEADER, "standby"); // null hint -> client rotates to next seed
             });
             ScpServer owner = new ScpServer(0, 1, 0, 0,
                     req -> ScpServer.ok(req, new Messages.AllocateWriterEpochResp(7).encode(), null));
             ControllerClient cc = new ControllerClient(new ClientConfig(
                     List.of(endpoint(standby), endpoint(owner)), 1024, 100))) {
            assertEquals(7, cc.allocateWriterEpochForAppend(ns, id));
            assertEquals(1, standbyCalls.get());
        }
    }

    @Test
    void interruptedRetryRestoresInterruptAndRethrowsLastError() throws Exception {
        int unusedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            unusedPort = socket.getLocalPort();
        }
        ControllerClient meta = new ControllerClient(new ClientConfig(List.of("127.0.0.1:" + unusedPort), 1024, 100));
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
        return Endpoint.parse(endpoint, "controller endpoint", ErrorCode.INTERNAL);
    }

    private static Throwable decodeFailure(ControllerClient meta, Opcode op,
                                           Function<ByteBuffer, ?> decoder) throws Exception {
        Method method = ControllerClient.class.getDeclaredMethod("decode", Opcode.class, ByteBuffer.class, Function.class);
        method.setAccessible(true);
        try {
            method.invoke(meta, op, ByteBuffer.allocate(0), decoder);
            throw new AssertionError("expected decoder failure");
        } catch (InvocationTargetException e) {
            return e.getCause();
        }
    }

    private static String endpoint(ScpServer server) {
        return "127.0.0.1:" + server.port();
    }
}
