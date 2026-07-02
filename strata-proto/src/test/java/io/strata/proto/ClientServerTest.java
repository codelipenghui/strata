package io.strata.proto;

import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.strata.common.ChunkId;
import io.strata.common.ConnectionPolicy;
import io.strata.common.ErrorCode;
import io.strata.common.FailureInjector;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
        List<Frame> serverRequests = new CopyOnWriteArrayList<>();
        ScpServer.Handler handler = req -> {
            if (req.opcode() == Opcode.PING.code) {
                // echo payload back
                serverRequests.add(req);
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
                    assertFalse(resp.ownsBuffer(), "client responses must not expose retained Netty buffers");
                    ByteBuffer hb = resp.headerSlice();
                    Resp.check(hb);
                    byte[] p = new byte[resp.payloadLength()];
                    resp.payloadSlice().get(p);
                    assertArrayEquals(("payload-" + i).getBytes(), p);
                }
                waitFor(() -> serverRequests.stream().allMatch(req -> req.ownerRefCnt() == 0));

                // server-side ScpException becomes a typed client-side exception
                ScpException e = assertThrows(ScpException.class,
                        () -> client.call(Opcode.READ, new Messages.Read(
                                new ChunkId(FileId.of(1), 0), 0, 1,
                                StrataNamespace.of("test")).encode(), null, 2000));
                assertEquals(ErrorCode.UNKNOWN_OPCODE, e.code());
            }
        }
    }

    @Test
    void sendCopiesHeaderAndPayloadBeforeAsyncEncoding() throws Exception {
        byte[] header = new byte[] {5, 6, 7};
        byte[] payload = new byte[] {1, 2, 3, 4};
        byte[] originalHeader = header.clone();
        byte[] original = payload.clone();
        CountDownLatch encoderPaused = new CountDownLatch(1);
        CountDownLatch releaseEncoder = new CountDownLatch(1);

        try (ScpServer server = new ScpServer(0, 0, 0, 0,
                req -> {
                    ByteBuffer requestHeader = req.headerSlice();
                    ByteBuffer requestPayload = req.payloadSlice();
                    ByteBuffer echoed = ByteBuffer.allocate(requestHeader.remaining() + requestPayload.remaining());
                    echoed.put(requestHeader).put(requestPayload).flip();
                    return ScpServer.ok(req, Messages.okHeader(), echoed);
                });
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "copy-test")) {
            FailureInjector.arm("scp.encoder.beforeHeader", point -> {
                encoderPaused.countDown();
                releaseEncoder.await(5, TimeUnit.SECONDS);
            });

            CompletableFuture<Frame> future = client.send(Opcode.PING, header, ByteBuffer.wrap(payload));
            assertTrue(encoderPaused.await(30, TimeUnit.SECONDS), "encoder did not reach request write seam");
            Arrays.fill(header, (byte) 8);
            Arrays.fill(payload, (byte) 9);
            releaseEncoder.countDown();

            Frame response = future.get(30, TimeUnit.SECONDS);
            Resp.check(response.headerSlice());
            byte[] echoed = new byte[response.payloadLength()];
            response.payloadSlice().get(echoed);
            byte[] expected = ByteBuffer.allocate(originalHeader.length + original.length)
                    .put(originalHeader)
                    .put(original)
                    .array();
            assertArrayEquals(expected, echoed,
                    "send must snapshot header and payload before returning to the caller");
        } finally {
            FailureInjector.reset();
        }
    }

    private static byte[] emptyHeader() {
        BufWriter w = new BufWriter(4);
        w.noTags();
        return w.toBytes();
    }

    @Test
    void managedConnectionHeartbeatsIdleHealthyConnection() throws Exception {
        AtomicInteger pings = new AtomicInteger();
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                    pings.incrementAndGet();
                    return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
             });
             ManagedScpConnection conn = managed(endpoint(server), 50, 100, 10_000)) {
            conn.call(Opcode.PING, emptyHeader(), null, 500);

            waitFor(() -> pings.get() >= 2);
        }
    }

    @Test
    void managedConnectionHeartbeatTimeoutClosesConnection() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger pings = new AtomicInteger();
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                    if (pings.incrementAndGet() > 1) {
                        release.await();
                    }
                    return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
             });
             ManagedScpConnection conn = managed(endpoint(server), 50, 50, 10_000)) {
            conn.call(Opcode.PING, emptyHeader(), null, 500);
            long generation = conn.generation();

            waitFor(() -> pings.get() > 1 && conn.generation() > generation);
        } finally {
            release.countDown();
        }
    }

    @Test
    void managedConnectionIdleEvictionAllowsFutureReconnect() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
             });
             ManagedScpConnection conn = managed(endpoint(server), 50, 100, 120)) {
            conn.call(Opcode.PING, emptyHeader(), null, 500);
            long connectedGeneration = conn.generation();

            waitFor(() -> conn.generation() > connectedGeneration);
            long evictedGeneration = conn.generation();

            conn.call(Opcode.PING, emptyHeader(), null, 500);
            assertTrue(conn.generation() > evictedGeneration);
        }
    }

    @Test
    void managedConnectionDoesNotEvictPendingRequestAsIdle() throws Exception {
        CountDownLatch requestStarted = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                Opcode op = Opcode.fromCode(req.opcode());
                if (op == Opcode.READ) {
                    requestStarted.countDown();
                    releaseRequest.await();
                    return ScpServer.ok(req, Messages.okHeader(), null);
                }
                if (op == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), null);
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
             });
             ManagedScpConnection conn = managed(endpoint(server), 30, 100, 80)) {
            CompletableFuture<Frame> future = conn.sendWithTimeout(Opcode.READ, emptyHeader(), null, 1_000);
            assertTrue(requestStarted.await(30, TimeUnit.SECONDS));
            long generation = conn.generation();

            TimeUnit.MILLISECONDS.sleep(180);
            assertFalse(future.isDone());
            assertEquals(generation, conn.generation());

            releaseRequest.countDown();
            Resp.check(future.get(30, TimeUnit.SECONDS).headerSlice());
        } finally {
            releaseRequest.countDown();
        }
    }

    @Test
    void managedConnectionCloseStopsMonitorThread() throws Exception {
        ManagedScpConnection conn = managed("127.0.0.1:1", 50, 50, 120);
        assertTrue(conn.monitorAliveForTests());

        conn.close();

        waitFor(() -> !conn.monitorAliveForTests());
    }

    @Test
    void fileRegionResponseStreamsPayloadWithoutPayloadCrc() throws Exception {
        var file = Files.createTempFile("strata-file-region", ".bin");
        Files.writeString(file, "0123456789", StandardCharsets.UTF_8);
        AtomicReference<FileChannel> sentChannel = new AtomicReference<>();
        ScpServer.Handler handler = req -> {
            FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
            sentChannel.set(channel);
            return ScpServer.okFileRegion(req, Messages.okHeader(), channel, 2, 5, () -> {
                try { channel.close(); } catch (IOException ignored) {}
            });
        };

        try (ScpServer server = new ScpServer(0, 1, 0xA, 0xB, handler);
             Socket socket = new Socket("127.0.0.1", server.port());
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            FrameIO.write(out, Frame.request(Opcode.HELLO,
                    new Messages.Hello(ScpClient.KIND_TOOL, 0, "file-region-test").encode(), null, 1));
            Resp.check(FrameIO.read(in).headerSlice());

            FrameIO.write(out, Frame.request(Opcode.PING, emptyHeader(), null, 2));
            Frame response = FrameIO.read(in);
            Resp.check(response.headerSlice());
            assertEquals(5, response.payloadLength());
            assertEquals(0, response.flags() & Frame.FLAG_PAYLOAD_CRC);
            byte[] payload = new byte[response.payloadLength()];
            response.payloadSlice().get(payload);
            assertArrayEquals("23456".getBytes(StandardCharsets.UTF_8), payload);
            waitFor(() -> sentChannel.get() != null && !sentChannel.get().isOpen());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void timedOutCallCleansUpItsPendingCorrelation() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
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
        CountDownLatch release = new CountDownLatch(1);
        ScpServer.Handler hang = req -> {
            release.await();
            return ScpServer.ok(req, Messages.okHeader(), null);
        };
        try (ScpServer server = new ScpServer(0, 1, 0, 0, hang);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            try {
                CompletableFuture<Frame> future = client.sendWithTimeout(Opcode.PING, emptyHeader(), null, 200);
                var e = assertThrows(ExecutionException.class,
                        () -> future.get(30, TimeUnit.SECONDS));
                assertEquals(TimeoutException.class, e.getCause().getClass());
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
    void closingConnectionDoesNotInterruptActiveServerRequest() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        ScpServer.Handler blocked = req -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
            return ScpServer.ok(req, Messages.okHeader(), null);
        };
        try (ScpServer server = new ScpServer(0, 1, 0, 0, blocked);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            CompletableFuture<Frame> pending = client.send(Opcode.PING, emptyHeader(), null);
            assertTrue(started.await(30, TimeUnit.SECONDS), "server request did not start");

            client.close();
            Thread.sleep(200);
            assertFalse(interrupted.get(), "connection close must not interrupt active server storage work");

            release.countDown();
            assertThrows(ExecutionException.class,
                    () -> pending.get(30, TimeUnit.SECONDS));
        } finally {
            release.countDown();
        }
    }

    @Test
    void pipelinedSendAppliesAdmissionControlBeforeTimeoutWindowExplodes() throws Exception {
        int maxExpectedOutstanding = 1024;
        CountDownLatch release = new CountDownLatch(1);
        ScpServer.Handler blocked = req -> {
            release.await();
            return ScpServer.ok(req, Messages.okHeader(), null);
        };
        try (ScpServer server = new ScpServer(0, 1, 0, 0, blocked);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            List<CompletableFuture<Frame>> futures = new ArrayList<>();
            try {
                for (int i = 0; i < maxExpectedOutstanding + 1; i++) {
                    futures.add(client.send(Opcode.PING, emptyHeader(), null));
                }

                assertTrue(client.pendingCount() <= maxExpectedOutstanding,
                        "a blackholed peer must not allow unbounded outstanding correlations");
                assertTrue(futures.stream().anyMatch(CompletableFuture::isCompletedExceptionally),
                        "requests above the connection window should fail fast or apply backpressure");
                CompletableFuture<Frame> failed = futures.stream()
                        .filter(CompletableFuture::isCompletedExceptionally)
                        .findFirst()
                        .orElseThrow();
                CompletionException e = assertThrows(CompletionException.class, failed::join);
                assertTrue(ScpException.rootCause(e) instanceof ScpException);
                assertEquals(ErrorCode.THROTTLED, ((ScpException) ScpException.rootCause(e)).code());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void serverRejectsRequestsBeyondConnectionAdmissionWindow() throws Exception {
        CompletableFuture<Frame> firstResponse = new CompletableFuture<>();
        CountDownLatch firstStarted = new CountDownLatch(1);
        AtomicReference<Frame> firstRequest = new AtomicReference<>();
        ScpServer.Handler blocked = new ScpServer.Handler() {
            @Override
            public Frame handle(Frame request) {
                throw new AssertionError("async path expected");
            }

            @Override
            public CompletableFuture<Frame> handleAsync(Frame request) {
                firstRequest.set(request);
                firstStarted.countDown();
                return firstResponse;
            }
        };
        try (ScpServer server = new ScpServer(0, 1, 0, 0, blocked, 1, 1 << 20);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "admission")) {
            CompletableFuture<Frame> first = client.send(Opcode.PING, emptyHeader(), null);
            // Readiness wait — block until the async handler runs for the first request. This is NOT a
            // latency deadline (the admission deadline under test is asserted below via THROTTLED), so give
            // it a generous budget: on a slow/loaded CI runner the first connect + round-trip + async
            // dispatch can exceed a few seconds and a tight budget times out spuriously.
            assertTrue(firstStarted.await(30, TimeUnit.SECONDS));

            CompletableFuture<Frame> rejected = client.send(Opcode.PING, emptyHeader(), null);
            Frame rejectedFrame = rejected.get(30, TimeUnit.SECONDS);
            ScpException e = assertThrows(ScpException.class, () -> Resp.check(rejectedFrame.headerSlice()));
            assertEquals(ErrorCode.THROTTLED, e.code());

            firstResponse.complete(ScpServer.ok(firstRequest.get(), Messages.okHeader(), null));
            assertThrows(Exception.class, () -> first.get(1, TimeUnit.SECONDS),
                    "server closes the over-admitted connection after returning THROTTLED");
        }
    }

    @Test
    void serverRejectsResponsesBeyondConnectionAdmissionByteWindow() throws Exception {
        ScpServer.Handler largeResponse = req -> ScpServer.ok(req, Messages.okHeader(),
                ByteBuffer.wrap(new byte[2048]));
        try (ScpServer server = new ScpServer(0, 1, 0, 0, largeResponse, 8, 1024);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "admission")) {
            ScpException e = assertThrows(ScpException.class,
                    () -> client.call(Opcode.PING, emptyHeader(), null, 5_000));
            assertEquals(ErrorCode.THROTTLED, e.code());
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
        List<Frame> serverRequests = new CopyOnWriteArrayList<>();
        ScpServer.Handler handler = new ScpServer.Handler() {
            @Override
            public Frame handle(Frame request) {
                throw new AssertionError("handleAsync should be used");
            }

            @Override
            public CompletableFuture<Frame> handleAsync(Frame request) {
                serverRequests.add(request);
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
            waitFor(() -> serverRequests.stream().allMatch(req -> req.ownerRefCnt() == 0));
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
            Frame request = seenRequest.get(30, TimeUnit.SECONDS);
            assertTrue(request.ownsBuffer());
            assertEquals(1, request.ownerRefCnt());

            server.close();
            var e = assertThrows(ExecutionException.class,
                    () -> pending.get(30, TimeUnit.SECONDS));
            assertEquals(IOException.class, e.getCause().getClass());
            waitFor(() -> request.ownerRefCnt() == 0);

            delayed.complete(ScpServer.ok(request, Messages.okHeader(), null));
            assertTrue(delayed.isDone());
        }
    }

    @Test
    void serverCloseDuringAsyncDispatchReleasesOrphanedRequestBuffer() throws Exception {
        // Deterministic regression for the close-vs-register race: handleAsync returns a deferred future,
        // then the connection closes BEFORE handleRequest adds the request to inFlightAsyncRequests, so
        // channelInactive drains the still-empty set. The request buffer must still be released, not
        // orphaned until the (possibly-never-completed) future fires. A seam forces that ordering.
        CountDownLatch reachedSeam = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        CompletableFuture<Frame> delayed = new CompletableFuture<>(); // intentionally never completed
        AtomicReference<Frame> serverReq = new AtomicReference<>();
        ScpServer.Handler handler = new ScpServer.Handler() {
            @Override
            public Frame handle(Frame request) {
                throw new AssertionError("handleAsync should be used");
            }

            @Override
            public CompletableFuture<Frame> handleAsync(Frame request) {
                serverReq.set(request);
                return delayed;
            }
        };
        FailureInjector.arm("scp.handleRequest.beforeInflightAdd", p -> {
            reachedSeam.countDown();
            proceed.await();
        });
        try (ScpServer server = new ScpServer(0, 1, 0, 0, handler);
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            CompletableFuture<Frame> pending = client.send(Opcode.PING, emptyHeader(), null);
            assertTrue(reachedSeam.await(30, TimeUnit.SECONDS), "request thread never reached the seam");
            Frame req = serverReq.get();
            assertEquals(1, req.ownerRefCnt());

            // Close while the request thread is paused BEFORE the inflight-add: channelInactive drains the
            // still-empty inFlightAsyncRequests set, then we let the add proceed (registering after the drain).
            server.close();
            proceed.countDown();

            // The orphaned request buffer must still be released (claimed by the post-add re-check).
            waitFor(() -> req.ownerRefCnt() == 0);
            assertThrows(ExecutionException.class,
                    () -> pending.get(30, TimeUnit.SECONDS));
        } finally {
            FailureInjector.reset();
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
            var e = assertThrows(ExecutionException.class, invalid::get);
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

            var e = assertThrows(ExecutionException.class, failed::get);
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

            var e = assertThrows(ExecutionException.class, failed::get);
            assertEquals(IOException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("header too large"));
            assertEquals(0, client.pendingCount());
            assertTrue(client.isClosed());
        }
    }

    @Test
    void synchronousWriteFailureReleasesPendingPermit() throws Exception {
        try (ScpClient client = new ScpClient(new ThrowingWriteChannel(),
                new Messages.HelloResp(0, 1, 0, 0, FrameIO.MAX_FRAME_BYTES, 1024))) {
            int permitsBefore = availablePendingPermits(client);

            CompletableFuture<Frame> failed = client.send(Opcode.PING, emptyHeader(), null);

            var e = assertThrows(ExecutionException.class, failed::get);
            assertEquals(IOException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("sync write failed"));
            assertEquals(0, client.pendingCount());
            assertEquals(permitsBefore, availablePendingPermits(client));
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

                var e = assertThrows(ExecutionException.class, failed::get);
                assertEquals(IOException.class, e.getCause().getClass());
                assertTrue(e.getCause().getMessage().contains("connection closed"));
                assertEquals(0, client.pendingCount());
            }
            peer.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void interruptedCallFrameRestoresInterruptAndCleansPendingCorrelation() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
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

    @Test
    void callFrameBorrowedRetainsBufferUntilClosed() throws Exception {
        byte[] body = "borrowed-payload".getBytes();
        try (ScpServer server = new ScpServer(0, 1, 0xA, 0xB, req -> {
                if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), ByteBuffer.wrap(body));
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "nope");
             });
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {

            Frame borrowed = client.callFrameBorrowed(Opcode.PING, emptyHeader(), ByteBuffer.wrap(body), 2000);
            try {
                assertTrue(borrowed.ownsBuffer(), "borrowed response must retain the pooled buffer");
                assertTrue(borrowed.ownerRefCnt() > 0, "buffer must be live before close");
                byte[] got = new byte[borrowed.payloadLength()];
                borrowed.payloadSlice().get(got);
                assertArrayEquals(body, got);
            } finally {
                borrowed.close();
            }
            assertEquals(0, borrowed.ownerRefCnt(), "close() must release the pooled buffer");
        }
    }

    @Test
    void managedCallFrameBorrowedRetainsBufferUntilClosed() throws Exception {
        byte[] body = "managed-borrow".getBytes();
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), ByteBuffer.wrap(body));
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
             });
             ManagedScpConnection conn = managed(endpoint(server), 1_000, 1_000, 10_000)) {
            Frame borrowed = conn.callFrameBorrowed(Opcode.PING, emptyHeader(), ByteBuffer.wrap(body), 2000);
            try {
                assertTrue(borrowed.ownsBuffer());
                byte[] got = new byte[borrowed.payloadLength()];
                borrowed.payloadSlice().get(got);
                assertArrayEquals(body, got);
            } finally {
                borrowed.close();
            }
            assertEquals(0, borrowed.ownerRefCnt());
        }
    }

    @Test
    void nonBorrowResponsesStillCopyToHeap() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0xA, 0xB, req -> {
                if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "nope");
             });
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            Frame resp = client.callFrame(Opcode.PING, emptyHeader(), ByteBuffer.wrap("x".getBytes()), 2000);
            assertFalse(resp.ownsBuffer(), "non-borrow client responses must not retain Netty buffers");
        }
    }

    private static int availablePendingPermits(ScpClient client) throws Exception {
        Field field = ScpClient.class.getDeclaredField("pendingPermits");
        field.setAccessible(true);
        return ((Semaphore) field.get(client)).availablePermits();
    }

    private static final class ThrowingWriteChannel extends EmbeddedChannel {
        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            throw new IllegalStateException("sync write failed");
        }
    }

    private static byte[] malformedHelloHeader() {
        BufWriter w = new BufWriter();
        w.u16(0).u16(0).u8(ScpClient.KIND_TOOL).u64(0).string("old").noTags();
        return w.toBytes();
    }

    private static ManagedScpConnection managed(String endpoint, int heartbeatIntervalMs,
                                                int heartbeatTimeoutMs, int idleTimeoutMs) {
        return new ManagedScpConnection(List.of(endpoint),
                new ConnectionPolicy(500, heartbeatIntervalMs, heartbeatTimeoutMs, idleTimeoutMs, 50, 200),
                ScpClient.KIND_TOOL, "managed-test", "test endpoint", false, true);
    }

    private static String endpoint(ScpServer server) {
        return "127.0.0.1:" + server.port();
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        // Readiness poll, not a latency deadline — give a generous budget so a loaded CI runner can't time
        // out spuriously while waiting for an expected condition (refcount drained, generation advanced, …).
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }
}
