package io.strata.proto;

import io.strata.common.ErrorCode;
import io.strata.common.FailureInjector;
import io.strata.common.ScpException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A file-region (READ) response is written to the channel as a frame prefix followed by a
 * {@link io.netty.channel.DefaultFileRegion}. If those two writes are not atomic relative to other
 * writes on the same connection, an async response completing on a DIFFERENT thread (e.g. the
 * group-commit flusher writing an APPEND ack) can slot a whole frame between the prefix and the
 * region — corrupting the wire stream for every subsequent frame.
 *
 * Uses {@link FailureInjector} to park the file-region write between its two pieces and a
 * concurrent async response to drive the interleave deterministically, then verifies the bytes on
 * the wire still parse into two well-formed frames with the file payload intact.
 */
class ServerWriteRaceInjectionTest {

    @AfterEach
    void clearInjector() {
        FailureInjector.reset();
    }

    @Test
    void fileRegionResponseIsNotSplitByConcurrentResponseWrite() throws Exception {
        Path file = Files.createTempFile("strata-split-write", ".bin");
        byte[] fileContent = "FILE-REGION-PAYLOAD-CONTENT-0123456789".getBytes(StandardCharsets.UTF_8);
        Files.write(file, fileContent);

        CountDownLatch seamReached = new CountDownLatch(1);
        CountDownLatch seamRelease = new CountDownLatch(1);
        FailureInjector.arm("scp.writeFileResponse.betweenPrefixAndRegion", p -> {
            seamReached.countDown();
            seamRelease.await();
        });

        CompletableFuture<Frame> asyncResponse = new CompletableFuture<>();
        AtomicReference<Frame> pingRequest = new AtomicReference<>();
        ScpServer.Handler handler = new ScpServer.Handler() {
            @Override
            public Frame handle(Frame req) {
                throw new AssertionError("handleAsync routes everything in this test");
            }

            @Override
            public CompletableFuture<Frame> handleAsync(Frame req) throws IOException {
                Opcode op = Opcode.fromCode(req.opcode());
                if (op == Opcode.READ) {
                    FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
                    return CompletableFuture.completedFuture(
                            ScpServer.okFileRegion(req, Messages.okHeader(), channel, 0, fileContent.length));
                }
                if (op == Opcode.PING) {
                    // an async response the test completes on its own thread while the file-region
                    // write is parked between its prefix and its region
                    pingRequest.set(req);
                    return asyncResponse;
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
            }
        };

        try (ScpServer server = new ScpServer(0, 1, 0xA, 0xB, handler);
             Socket socket = new Socket("127.0.0.1", server.port());
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            socket.setSoTimeout(5_000);
            FrameIO.write(out, Frame.request(Opcode.HELLO,
                    new Messages.Hello(ScpClient.KIND_TOOL, 0, "split-write-test").encode(), null, 1));
            Resp.check(FrameIO.read(in).headerSlice());

            long pingId = 100;
            long readId = 200;
            // async PING first (its response is parked), then the file-region READ
            FrameIO.write(out, Frame.request(Opcode.PING, emptyHeader(), null, pingId));
            FrameIO.write(out, Frame.request(Opcode.READ, emptyHeader(), null, readId));

            // the READ response is now parked between its prefix and its region write
            assertTrue(seamReached.await(3, TimeUnit.SECONDS), "file-region write never reached the seam");
            assertNotNull(pingRequest.get(), "async PING was never received");

            // complete the async PING on the test thread: its single-write response now races the
            // parked file-region write. With the split-write bug it lands between prefix and region.
            asyncResponse.complete(ScpServer.ok(pingRequest.get(), Messages.okHeader(), null));
            // let the (buggy) interleaved write reach the wire before the region is released
            TimeUnit.MILLISECONDS.sleep(200);
            seamRelease.countDown();

            // The wire must be two well-formed frames. FrameIO.read validates framing; a split
            // stream yields a READ frame whose payload is the interleaved frame's bytes.
            Map<Long, Frame> byId = new HashMap<>();
            Frame f1 = FrameIO.read(in);
            Frame f2 = FrameIO.read(in);
            assertNotNull(f1, "missing first response frame");
            assertNotNull(f2, "missing second response frame");
            byId.put(f1.correlationId(), f1);
            byId.put(f2.correlationId(), f2);

            Frame read = byId.get(readId);
            assertNotNull(read, "missing READ response");
            byte[] got = new byte[read.payloadLength()];
            read.payloadSlice().get(got);
            assertArrayEquals(fileContent, got,
                    "file-region READ payload was corrupted by an interleaved concurrent write");
            assertNotNull(byId.get(pingId), "missing async PING response");
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static byte[] emptyHeader() {
        BufWriter w = new BufWriter(4);
        w.noTags();
        return w.toBytes();
    }
}
