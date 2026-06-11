package io.strata.proto;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SCP server: virtual thread per connection, frames processed serially per connection
 * (preserves per-chunk append ordering — tech design §10.2 connection rules).
 */
public final class ScpServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ScpServer.class);

    /**
     * Handles one request frame; returns the response frame. Throw ScpException for protocol errors.
     *
     * handleAsync is the dispatch entry point: its synchronous portion runs on the connection
     * thread IN ORDER (validation/writes keep per-chunk contiguity semantics), but the response
     * may complete later (e.g. group commit) — the connection keeps processing subsequent frames
     * meanwhile. Out-of-order responses are protocol-legal: clients correlate by id.
     */
    public interface Handler {
        Frame handle(Frame request) throws Exception;

        default java.util.concurrent.CompletableFuture<Frame> handleAsync(Frame request) throws Exception {
            return java.util.concurrent.CompletableFuture.completedFuture(handle(request));
        }
    }

    private final ServerSocket serverSocket;
    private final Handler handler;
    private final int nodeId;
    private final long incMsb;
    private final long incLsb;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();

    public ScpServer(int port, int nodeId, long incMsb, long incLsb, Handler handler) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.handler = handler;
        this.nodeId = nodeId;
        this.incMsb = incMsb;
        this.incLsb = incLsb;
        Thread.ofVirtual().name("scp-accept-" + port()).start(this::acceptLoop);
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (!closed.get()) {
            try {
                Socket s = serverSocket.accept();
                s.setTcpNoDelay(true);
                connections.add(s);
                Thread.ofVirtual().name("scp-conn-" + s.getRemoteSocketAddress()).start(() -> serve(s));
            } catch (IOException e) {
                if (!closed.get()) log.warn("accept failed", e);
                return;
            }
        }
    }

    private void serve(Socket s) {
        // ReentrantLock (not synchronized): deferred completions write responses from virtual
        // threads; blocking socket writes must not pin carriers
        java.util.concurrent.locks.ReentrantLock writeLock = new java.util.concurrent.locks.ReentrantLock();
        try (s) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 1 << 16));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 1 << 16));

            Frame hello = FrameIO.read(in);
            if (hello == null) return;
            if (hello.opcode() != Opcode.HELLO.code) {
                writeResponse(s, out, writeLock, Frame.response(hello,
                        Resp.error(ErrorCode.UNKNOWN_OPCODE, "first frame must be HELLO", 0), null));
                return;
            }
            Messages.Hello.decode(hello.headerSlice()); // validates frame-version overlap
            writeResponse(s, out, writeLock, Frame.response(hello,
                    new Messages.HelloResp(0, nodeId, incMsb, incLsb, FrameIO.MAX_FRAME_BYTES, 1L << 30).encode(), null));

            while (!closed.get()) {
                Frame req = FrameIO.read(in);
                if (req == null) return;
                java.util.concurrent.CompletableFuture<Frame> respF;
                try {
                    respF = handler.handleAsync(req);
                } catch (ScpException e) {
                    respF = java.util.concurrent.CompletableFuture.completedFuture(
                            Frame.response(req, Resp.error(e.code(), e.getMessage(), e.detail()), null));
                } catch (Exception e) {
                    log.warn("handler error for opcode 0x{}", Integer.toHexString(req.opcode()), e);
                    respF = java.util.concurrent.CompletableFuture.completedFuture(
                            Frame.response(req, Resp.error(ErrorCode.INTERNAL, String.valueOf(e), 0), null));
                }
                if (respF.isDone() && !respF.isCompletedExceptionally()) {
                    writeResponse(s, out, writeLock, respF.join()); // fast path, no extra hop
                } else {
                    respF.whenComplete((resp, err) -> {
                        Frame frame = resp;
                        if (err != null) {
                            Throwable cause = err instanceof java.util.concurrent.CompletionException
                                    ? err.getCause() : err;
                            frame = cause instanceof ScpException se
                                    ? Frame.response(req, Resp.error(se.code(), se.getMessage(), se.detail()), null)
                                    : Frame.response(req, Resp.error(ErrorCode.INTERNAL, String.valueOf(cause), 0), null);
                        }
                        writeResponse(s, out, writeLock, frame);
                    });
                }
            }
        } catch (IOException e) {
            // connection dropped — normal in failure tests
        } finally {
            connections.remove(s);
        }
    }

    private void writeResponse(Socket s, DataOutputStream out,
                               java.util.concurrent.locks.ReentrantLock writeLock, Frame frame) {
        writeLock.lock();
        try {
            FrameIO.write(out, frame);
        } catch (IOException e) {
            try {
                s.close(); // unblocks the reader loop; connection teardown is the recovery path
            } catch (IOException ignored) {
            }
        } finally {
            writeLock.unlock();
        }
    }

    /** Convenience for handlers: success response with header bytes and optional payload. */
    public static Frame ok(Frame req, byte[] header, ByteBuffer payload) {
        return Frame.response(req, header, payload);
    }

    @Override
    public void close() {
        closed.set(true);
        try {
            serverSocket.close();
        } catch (IOException ignored) {
        }
        for (Socket s : connections) {
            try {
                s.close();
            } catch (IOException ignored) {
            }
        }
    }
}
