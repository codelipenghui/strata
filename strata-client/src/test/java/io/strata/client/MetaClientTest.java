package io.strata.client;

import io.strata.common.FileId;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetaClientTest {

    @Test
    void endpointParserCoversValidAndInvalidBoundaries() throws Exception {
        Object host = parseEndpoint("meta.local:2181");
        assertEquals("meta.local", endpointHost(host));
        assertEquals(2181, endpointPort(host));

        Object ipv6 = parseEndpoint("[::1]:2181");
        assertEquals("::1", endpointHost(ipv6));
        assertEquals(2181, endpointPort(ipv6));

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
    void malformedMetadataResponseClosesExistingConnectionAndClearsClient() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, Messages.okHeader(), null));
             ScpClient scp = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_BROKER, "test");
             MetaClient meta = new MetaClient(new ClientConfig(List.of(endpoint(server)), 1024, 100))) {
            setClient(meta, scp);

            Throwable thrown = decodeFailure(meta, Opcode.CREATE_FILE, ignored -> {
                throw new IllegalArgumentException("bad response");
            });

            assertEquals(ErrorCode.INTERNAL, ((ScpException) thrown).code());
            assertTrue(scp.isClosed());
            assertNull(clientField(meta));
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

            assertEquals(id, meta.createFile(StrataClient.FileSpec.log("owner")));

            assertEquals(1, standbyCalls.get());
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

            assertEquals(id, meta.createFile(StrataClient.FileSpec.log("owner")));

            assertEquals(2, calls.get());
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
                    () -> meta.createFile(StrataClient.FileSpec.log("owner")));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            meta.close();
        }
    }

    private static ScpException parseFailure(String endpoint) throws Exception {
        try {
            parseEndpoint(endpoint);
            throw new AssertionError("expected parser failure");
        } catch (InvocationTargetException e) {
            return (ScpException) e.getCause();
        }
    }

    private static Object parseEndpoint(String endpoint) throws Exception {
        Method method = MetaClient.class.getDeclaredMethod("parseEndpoint", String.class);
        method.setAccessible(true);
        return method.invoke(null, endpoint);
    }

    private static String endpointHost(Object endpoint) throws Exception {
        Method host = endpoint.getClass().getDeclaredMethod("host");
        host.setAccessible(true);
        return (String) host.invoke(endpoint);
    }

    private static int endpointPort(Object endpoint) throws Exception {
        Method port = endpoint.getClass().getDeclaredMethod("port");
        port.setAccessible(true);
        return (int) port.invoke(endpoint);
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

    private static void setClient(MetaClient meta, ScpClient client) throws ReflectiveOperationException {
        Field field = MetaClient.class.getDeclaredField("client");
        field.setAccessible(true);
        field.set(meta, client);
    }

    private static Object clientField(MetaClient meta) throws ReflectiveOperationException {
        Field field = MetaClient.class.getDeclaredField("client");
        field.setAccessible(true);
        return field.get(meta);
    }

    private static String endpoint(ScpServer server) {
        return "127.0.0.1:" + server.port();
    }
}
