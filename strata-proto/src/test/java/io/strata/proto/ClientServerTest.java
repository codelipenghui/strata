package io.strata.proto;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientServerTest {

    @Test
    void pipelinedEchoAndErrors() throws Exception {
        ScpServer.Handler handler = req -> {
            if (req.opcode() == Opcode.PING.code) {
                // echo payload back
                return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "nope");
        };
        try (ScpServer server = new ScpServer(0, 1, 0xA, 0xB, handler)) {
            try (ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
                assertEquals(1, client.serverHello().nodeId());

                // pipeline 100 pings with distinct payloads
                List<CompletableFuture<Frame>> futures = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    byte[] p = ("payload-" + i).getBytes();
                    futures.add(client.send(Opcode.PING, emptyHeader(), ByteBuffer.wrap(p)));
                }
                for (int i = 0; i < 100; i++) {
                    Frame resp = futures.get(i).get();
                    ByteBuffer hb = resp.headerSlice();
                    Resp.check(hb);
                    byte[] p = new byte[resp.payloadLength()];
                    resp.payloadSlice().get(p);
                    assertArrayEquals(("payload-" + i).getBytes(), p);
                }

                // server-side ScpException becomes a typed client-side exception
                ScpException e = assertThrows(ScpException.class,
                        () -> client.call(Opcode.READ, new Messages.Read(
                                new io.strata.common.ChunkId(io.strata.common.FileId.random(), 0), 0, 1).encode(), null, 2000));
                assertEquals(ErrorCode.UNKNOWN_OPCODE, e.code());
            }
        }
    }

    private static byte[] emptyHeader() {
        BufWriter w = new BufWriter(4);
        w.noTags();
        return w.toBytes();
    }

    @Test
    void timedOutCallCleansUpItsPendingCorrelation() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        ScpServer.Handler hang = req -> {
            release.await(); // never answers until released
            return ScpServer.ok(req, Messages.okHeader(), null);
        };
        try (ScpServer server = new ScpServer(0, 1, 0, 0, hang);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            try {
                assertThrows(ScpException.class,
                        () -> client.callFrame(Opcode.PING, emptyHeader(), null, 200));
                // the correlation entry must not leak after the timeout — on a long-lived connection
                // (appends pipeline for hours) leaked entries accumulate unboundedly
                assertEquals(0, client.pendingCount(),
                        "timed-out request left its correlation entry behind");
                assertEquals(true, client.isClosed(),
                        "timed-out call must poison the connection so pools replace it");
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void timedOutPipelinedSendClosesConnection() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        ScpServer.Handler hang = req -> {
            release.await();
            return ScpServer.ok(req, Messages.okHeader(), null);
        };
        try (ScpServer server = new ScpServer(0, 1, 0, 0, hang);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            try {
                CompletableFuture<Frame> future = client.sendWithTimeout(Opcode.PING, emptyHeader(), null, 200);
                var e = assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> future.get(1, java.util.concurrent.TimeUnit.SECONDS));
                assertEquals(java.util.concurrent.TimeoutException.class, e.getCause().getClass());
                assertEquals(0, client.pendingCount(),
                        "timed-out pipelined request left its correlation entry behind");
                assertEquals(true, client.isClosed(),
                        "timed-out pipelined send must poison the connection so pools replace it");
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void nullHandlerResponsesBecomeTypedErrors() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> null);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            ScpException e = assertThrows(ScpException.class,
                    () -> client.call(Opcode.PING, emptyHeader(), null, 2000));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(e.getMessage().contains("null response"));
        }

        ScpServer.Handler nullFuture = new ScpServer.Handler() {
            @Override
            public Frame handle(Frame request) {
                return ScpServer.ok(request, Messages.okHeader(), null);
            }

            @Override
            public CompletableFuture<Frame> handleAsync(Frame request) {
                return null;
            }
        };
        try (ScpServer server = new ScpServer(0, 1, 0, 0, nullFuture);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            ScpException e = assertThrows(ScpException.class,
                    () -> client.call(Opcode.PING, emptyHeader(), null, 2000));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(e.getMessage().contains("null future"));
        }
    }

    @Test
    void asyncHandlerFailuresBecomeTypedErrors() throws Exception {
        ScpServer.Handler handler = new ScpServer.Handler() {
            @Override
            public Frame handle(Frame request) {
                throw new AssertionError("handleAsync should be used");
            }

            @Override
            public CompletableFuture<Frame> handleAsync(Frame request) {
                Opcode op = Opcode.fromCode(request.opcode());
                if (op == Opcode.PING) {
                    return CompletableFuture.failedFuture(new CompletionException(
                            new ScpException(ErrorCode.FENCED_EPOCH, "async fenced", 44)));
                }
                return CompletableFuture.failedFuture(new IllegalStateException("async boom"));
            }
        };

        try (ScpServer server = new ScpServer(0, 1, 0, 0, handler);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            ScpException fenced = assertThrows(ScpException.class,
                    () -> client.call(Opcode.PING, emptyHeader(), null, 2000));
            assertEquals(ErrorCode.FENCED_EPOCH, fenced.code());
            assertEquals(44, fenced.detail());

            ScpException internal = assertThrows(ScpException.class,
                    () -> client.call(Opcode.READ, emptyHeader(), null, 2000));
            assertEquals(ErrorCode.INTERNAL, internal.code());
            assertTrue(internal.getMessage().contains("async boom"));
        }
    }

    @Test
    void serverCloseSkipsDeferredAsyncResponses() throws Exception {
        CompletableFuture<Frame> delayed = new CompletableFuture<>();
        CompletableFuture<Frame> seenRequest = new CompletableFuture<>();
        ScpServer.Handler handler = new ScpServer.Handler() {
            @Override
            public Frame handle(Frame request) {
                throw new AssertionError("handleAsync should be used");
            }

            @Override
            public CompletableFuture<Frame> handleAsync(Frame request) {
                seenRequest.complete(request);
                return delayed;
            }
        };

        try (ScpServer server = new ScpServer(0, 1, 0, 0, handler);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            CompletableFuture<Frame> pending = client.send(Opcode.PING, emptyHeader(), null);
            Frame request = seenRequest.get(3, TimeUnit.SECONDS);

            server.close();
            var e = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> pending.get(3, TimeUnit.SECONDS));
            assertEquals(IOException.class, e.getCause().getClass());

            delayed.complete(ScpServer.ok(request, Messages.okHeader(), null));
            assertTrue(delayed.isDone());
        }
    }

    @Test
    void clientReaderClosesLocalSocketWhenPeerCloses() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, Messages.okHeader(), null));
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            assertFalse(client.isClosed());

            server.close();
            waitFor(client::isClosed);
        }
    }

    @Test
    void invalidSendInputDoesNotLeakPendingCorrelation() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, Messages.okHeader(), null));
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            CompletableFuture<Frame> invalid = client.send(null, null, null);
            var e = assertThrows(java.util.concurrent.ExecutionException.class, invalid::get);
            assertEquals(ErrorCode.UNKNOWN_OPCODE, ((ScpException) e.getCause()).code());
            assertEquals(0, client.pendingCount());
            assertEquals(false, client.isClosed());
        }
    }

    @Test
    void callFrameInvalidOpcodeReturnsTypedErrorWithoutLeakingPendingCorrelation() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, Messages.okHeader(), null));
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            ScpException e = assertThrows(ScpException.class,
                    () -> client.callFrame(null, null, null, 2000));
            assertEquals(ErrorCode.UNKNOWN_OPCODE, e.code());
            assertEquals(0, client.pendingCount());
            assertFalse(client.isClosed());
        }
    }

    @Test
    void sendAfterCloseFailsImmediatelyWithoutLeakingPendingCorrelation() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, Messages.okHeader(), null));
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            client.close();

            CompletableFuture<Frame> failed = client.send(Opcode.PING, emptyHeader(), null);

            var e = assertThrows(java.util.concurrent.ExecutionException.class, failed::get);
            assertEquals(IOException.class, e.getCause().getClass());
            assertEquals(0, client.pendingCount());
        }
    }

    @Test
    void writeFailureFailsPendingFutureAndClosesConnection() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, Messages.okHeader(), null));
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            CompletableFuture<Frame> failed = client.send(Opcode.PING, new byte[0x1_0000], null);

            var e = assertThrows(java.util.concurrent.ExecutionException.class, failed::get);
            assertEquals(IOException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("header too large"));
            assertEquals(0, client.pendingCount());
            assertTrue(client.isClosed());
        }
    }

    @Test
    void sendFailsPendingFutureIfConnectionClosesAfterWrite() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<Void> peer = CompletableFuture.runAsync(() -> {
                try (Socket socket = server.accept()) {
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    Frame hello = FrameIO.read(in);
                    FrameIO.write(out, Frame.response(hello,
                            new Messages.HelloResp(0, 7, 0, 0, FrameIO.MAX_FRAME_BYTES, 1024).encode(), null));
                    FrameIO.read(in); // request was written; close without responding
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            try (ScpClient client = new ScpClient("127.0.0.1", server.getLocalPort(),
                    ScpClient.KIND_TOOL, "t")) {
                CompletableFuture<Frame> failed = client.send(Opcode.PING, emptyHeader(), null);

                var e = assertThrows(java.util.concurrent.ExecutionException.class, failed::get);
                assertEquals(IOException.class, e.getCause().getClass());
                assertTrue(e.getCause().getMessage().contains("connection closed"));
                assertEquals(0, client.pendingCount());
            }
            peer.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void interruptedCallFrameRestoresInterruptAndCleansPendingCorrelation() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        ScpServer.Handler hang = req -> {
            release.await();
            return ScpServer.ok(req, Messages.okHeader(), null);
        };
        try (ScpServer server = new ScpServer(0, 1, 0, 0, hang);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            Thread.currentThread().interrupt();
            try {
                ScpException e = assertThrows(ScpException.class,
                        () -> client.callFrame(Opcode.PING, emptyHeader(), null, 2000));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(Thread.currentThread().isInterrupted());
                assertEquals(0, client.pendingCount());
            } finally {
                Thread.interrupted();
                release.countDown();
            }
        }
    }

    @Test
    void malformedResponsePrefixBecomesTypedErrorAndClosesConnection() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, new byte[] {0}, null));
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            ScpException e = assertThrows(ScpException.class,
                    () -> client.call(Opcode.PING, emptyHeader(), null, 2000));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertEquals(true, client.isClosed());
        }
    }

    @Test
    void serverRejectsNonHelloFirstFrameAndMalformedHello() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, Messages.okHeader(), null));
             Socket socket = new Socket("127.0.0.1", server.port())) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            FrameIO.write(out, Frame.request(Opcode.PING, emptyHeader(), null, 99));
            Frame response = FrameIO.read(in);
            ScpException e = assertThrows(ScpException.class, () -> Resp.check(response.headerSlice()));
            assertEquals(ErrorCode.UNKNOWN_OPCODE, e.code());
            assertTrue(e.getMessage().contains("HELLO"));
        }

        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, Messages.okHeader(), null));
             Socket socket = new Socket("127.0.0.1", server.port())) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            FrameIO.write(out, Frame.request(Opcode.HELLO, malformedHelloHeader(), null, 100));
            Frame response = FrameIO.read(in);
            ScpException e = assertThrows(ScpException.class, () -> Resp.check(response.headerSlice()));
            assertEquals(ErrorCode.UNSUPPORTED_VERSION, e.code());
        }
    }

    @Test
    void serverClosesQuietlyWhenPeerDisconnectsBeforeHello() throws Exception {
        AtomicBoolean handlerCalled = new AtomicBoolean();
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
            handlerCalled.set(true);
            return ScpServer.ok(req, Messages.okHeader(), null);
        })) {
            try (Socket socket = new Socket("127.0.0.1", server.port())) {
                socket.shutdownOutput();
            }
            Thread.sleep(50);
            assertEquals(false, handlerCalled.get());
        }
    }

    @Test
    void serverDropsConnectionAfterMalformedPostHandshakeFrame() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, Messages.okHeader(), null));
             Socket socket = new Socket("127.0.0.1", server.port())) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            FrameIO.write(out, Frame.request(Opcode.HELLO,
                    new Messages.Hello(ScpClient.KIND_TOOL, 0, "raw").encode(), null, 101));
            Resp.check(FrameIO.read(in).headerSlice());

            out.writeInt(FrameIO.MAX_FRAME_BYTES + 1);
            out.flush();

            assertNull(FrameIO.read(in));
        }
    }

    @Test
    void failedHandshakeClosesSocketBeforeThrowing() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<Boolean> sawClientClose = CompletableFuture.supplyAsync(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(2_000);
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    Frame hello = FrameIO.read(in);
                    FrameIO.write(out, Frame.response(hello,
                            Resp.error(ErrorCode.UNSUPPORTED_VERSION, "bad hello", 0), null));
                    try {
                        return socket.getInputStream().read() < 0;
                    } catch (SocketTimeoutException e) {
                        return false;
                    }
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            ScpException e = assertThrows(ScpException.class,
                    () -> new ScpClient("127.0.0.1", server.getLocalPort(), ScpClient.KIND_TOOL, "bad"));
            assertEquals(ErrorCode.UNSUPPORTED_VERSION, e.code());
            assertEquals(true, sawClientClose.get(3, TimeUnit.SECONDS),
                    "failed ScpClient constructor left the socket open");
        }
    }

    @Test
    void silentHandshakePeerTimesOutAndClosesSocket() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<Boolean> sawClientClose = CompletableFuture.supplyAsync(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(2_000);
                    FrameIO.read(new DataInputStream(socket.getInputStream()));
                    try {
                        return socket.getInputStream().read() < 0;
                    } catch (SocketTimeoutException e) {
                        return false;
                    }
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            assertThrows(SocketTimeoutException.class,
                    () -> new ScpClient("127.0.0.1", server.getLocalPort(), ScpClient.KIND_TOOL, "silent", 200));
            assertTrue(sawClientClose.get(3, TimeUnit.SECONDS),
                    "timed-out ScpClient constructor left the socket open");
        }
    }

    @Test
    void wrongOpcodeHandshakeClosesSocketAndThrowsIOException() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<Boolean> sawClientClose = CompletableFuture.supplyAsync(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(2_000);
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    Frame hello = FrameIO.read(in);
                    Frame wrongOpcode = new Frame(Opcode.PING.code, (short) 1, Frame.FLAG_RESPONSE,
                            hello.correlationId(), ByteBuffer.wrap(Messages.okHeader()), null);
                    FrameIO.write(out, wrongOpcode);
                    try {
                        return socket.getInputStream().read() < 0;
                    } catch (SocketTimeoutException e) {
                        return false;
                    }
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            IOException e = assertThrows(IOException.class,
                    () -> new ScpClient("127.0.0.1", server.getLocalPort(), ScpClient.KIND_TOOL, "bad"));
            assertTrue(e.getMessage().contains("handshake failed"));
            assertTrue(sawClientClose.get(3, TimeUnit.SECONDS),
                    "failed ScpClient constructor left the socket open");
        }
    }

    @Test
    void nonResponseHelloFrameClosesSocketAndThrowsIOException() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<Boolean> sawClientClose = CompletableFuture.supplyAsync(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(2_000);
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    Frame hello = FrameIO.read(in);
                    Frame requestShapedHello = new Frame(Opcode.HELLO.code, (short) 1, (short) 0,
                            hello.correlationId(), ByteBuffer.wrap(Messages.okHeader()), null);
                    FrameIO.write(out, requestShapedHello);
                    try {
                        return socket.getInputStream().read() < 0;
                    } catch (SocketTimeoutException e) {
                        return false;
                    }
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            IOException e = assertThrows(IOException.class,
                    () -> new ScpClient("127.0.0.1", server.getLocalPort(), ScpClient.KIND_TOOL, "bad"));
            assertTrue(e.getMessage().contains("handshake failed"));
            assertTrue(sawClientClose.get(3, TimeUnit.SECONDS),
                    "failed ScpClient constructor left the socket open");
        }
    }

    @Test
    void nullHandshakeResponseClosesSocketAndThrowsIOException() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<Void> accepted = CompletableFuture.runAsync(() -> {
                try (Socket socket = server.accept()) {
                    FrameIO.read(new DataInputStream(socket.getInputStream()));
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            IOException e = assertThrows(IOException.class,
                    () -> new ScpClient("127.0.0.1", server.getLocalPort(), ScpClient.KIND_TOOL, "bad"));
            assertTrue(e.getMessage().contains("handshake failed")
                    || e.getMessage().contains("connection closed"), "got: " + e.getMessage());
            accepted.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void malformedSuccessfulHandshakeClosesSocketAndThrowsIOException() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<Boolean> sawClientClose = CompletableFuture.supplyAsync(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(2_000);
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    Frame hello = FrameIO.read(in);
                    FrameIO.write(out, Frame.response(hello, Messages.okHeader(), null));
                    try {
                        return socket.getInputStream().read() < 0;
                    } catch (SocketTimeoutException e) {
                        return false;
                    }
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            IOException e = assertThrows(IOException.class,
                    () -> new ScpClient("127.0.0.1", server.getLocalPort(), ScpClient.KIND_TOOL, "bad"));
            assertTrue(e.getMessage().contains("malformed handshake response"));
            assertEquals(true, sawClientClose.get(3, TimeUnit.SECONDS),
                    "malformed ScpClient constructor left the socket open");
        }
    }

    @Test
    void clientIgnoresResponsesForUnknownCorrelationIds() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<Void> serving = CompletableFuture.runAsync(() -> {
                try (Socket socket = server.accept()) {
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    Frame hello = FrameIO.read(in);
                    FrameIO.write(out, Frame.response(hello,
                            new Messages.HelloResp(0, 7, 0, 0, FrameIO.MAX_FRAME_BYTES, 1024).encode(), null));
                    Frame unsolicited = new Frame(Opcode.PING.code, (short) 1, Frame.FLAG_RESPONSE,
                            999, ByteBuffer.wrap(Messages.okHeader()), null);
                    FrameIO.write(out, unsolicited);

                    Frame request = FrameIO.read(in);
                    FrameIO.write(out, Frame.response(request, Messages.okHeader(), request.payloadSlice()));
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            try (ScpClient client = new ScpClient("127.0.0.1", server.getLocalPort(), ScpClient.KIND_TOOL, "t")) {
                Frame response = client.callFrame(Opcode.PING, emptyHeader(),
                        ByteBuffer.wrap(new byte[] {1, 2, 3}), 2000);
                Resp.check(response.headerSlice());
                byte[] payload = new byte[response.payloadLength()];
                response.payloadSlice().get(payload);
                assertArrayEquals(new byte[] {1, 2, 3}, payload);
            }
            serving.get(3, TimeUnit.SECONDS);
        }
    }

    private static byte[] malformedHelloHeader() {
        BufWriter w = new BufWriter();
        w.u16(0).u16(0).u8(ScpClient.KIND_TOOL).u64(0).string("old").noTags();
        return w.toBytes();
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }
}
