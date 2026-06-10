package io.strata.proto;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One SCP connection: HELLO handshake, then pipelined request/response correlated by id.
 * Thread-safe; writes are serialized, responses dispatched by a reader thread (virtual).
 */
public final class ScpClient implements AutoCloseable {
    public static final byte KIND_BROKER = 1;
    public static final byte KIND_STORAGE_NODE = 2;
    public static final byte KIND_METADATA = 3;
    public static final byte KIND_TOOL = 4;

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final Object writeLock = new Object();
    private final AtomicLong correlation = new AtomicLong(1);
    private final Map<Long, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Messages.HelloResp serverHello;

    public ScpClient(String host, int port, byte clientKind, String clientId) throws IOException {
        this(host, port, clientKind, clientId, 5_000);
    }

    public ScpClient(String host, int port, byte clientKind, String clientId, int connectTimeoutMs) throws IOException {
        socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 1 << 16));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 1 << 16));

        FrameIO.write(out, Frame.request(Opcode.HELLO, new Messages.Hello(clientKind, 0, clientId).encode(), null, 0));
        Frame helloResp = FrameIO.read(in);
        if (helloResp == null || helloResp.opcode() != Opcode.HELLO.code || !helloResp.isResponse()) {
            socket.close();
            throw new IOException("handshake failed");
        }
        ByteBuffer hb = helloResp.headerSlice();
        Resp.check(hb);
        this.serverHello = Messages.HelloResp.decode(hb);

        Thread.ofVirtual().name("scp-reader-" + host + ":" + port).start(this::readLoop);
    }

    public Messages.HelloResp serverHello() {
        return serverHello;
    }

    private void readLoop() {
        try {
            while (!closed.get()) {
                Frame f = FrameIO.read(in);
                if (f == null) break;
                CompletableFuture<Frame> fut = pending.remove(f.correlationId());
                if (fut != null) fut.complete(f);
            }
        } catch (IOException e) {
            // fall through to failure propagation
        } finally {
            failAll(new IOException("connection closed"));
        }
    }

    private void failAll(Exception e) {
        closed.set(true);
        pending.keySet().forEach(id -> {
            CompletableFuture<Frame> fut = pending.remove(id);
            if (fut != null) fut.completeExceptionally(e);
        });
    }

    public CompletableFuture<Frame> send(Opcode op, byte[] header, ByteBuffer payload) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IOException("connection closed"));
        }
        long id = correlation.getAndIncrement();
        CompletableFuture<Frame> fut = new CompletableFuture<>();
        pending.put(id, fut);
        try {
            synchronized (writeLock) {
                FrameIO.write(out, Frame.request(op, header, payload, id));
            }
        } catch (IOException e) {
            pending.remove(id);
            failAll(e);
            fut.completeExceptionally(e);
        }
        return fut;
    }

    /** Synchronous call; returns the response header positioned AFTER the error check. */
    public ByteBuffer call(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        Frame resp = callFrame(op, header, payload, timeoutMs);
        ByteBuffer hb = resp.headerSlice();
        Resp.check(hb);
        return hb;
    }

    /** Synchronous call returning the whole frame (for payload-carrying responses); error NOT yet checked. */
    public Frame callFrame(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        try {
            return send(op, header, payload).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ScpException se) throw se;
            throw new ScpException(ErrorCode.INTERNAL, String.valueOf(e.getCause()));
        } catch (TimeoutException e) {
            throw new ScpException(ErrorCode.INTERNAL, "timeout after " + timeoutMs + "ms for " + op);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ScpException(ErrorCode.INTERNAL, "interrupted");
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
