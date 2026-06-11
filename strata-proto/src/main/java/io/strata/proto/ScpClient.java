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
import java.util.concurrent.CompletionException;
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
    // ReentrantLock, not synchronized: blocking socket writes under a monitor pin virtual threads
    private final java.util.concurrent.locks.ReentrantLock writeLock =
            new java.util.concurrent.locks.ReentrantLock();
    private final AtomicLong correlation = new AtomicLong(1);
    private final Map<Long, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Messages.HelloResp serverHello;

    public ScpClient(String host, int port, byte clientKind, String clientId) throws IOException {
        this(host, port, clientKind, clientId, 5_000);
    }

    public ScpClient(String host, int port, byte clientKind, String clientId, int connectTimeoutMs) throws IOException {
        Socket connected = new Socket();
        DataInputStream input;
        DataOutputStream output;
        Messages.HelloResp hello;
        try {
            connected.setTcpNoDelay(true);
            connected.connect(new InetSocketAddress(host, port), connectTimeoutMs);
            connected.setSoTimeout(connectTimeoutMs);
            input = new DataInputStream(new BufferedInputStream(connected.getInputStream(), 1 << 16));
            output = new DataOutputStream(new BufferedOutputStream(connected.getOutputStream(), 1 << 16));

            FrameIO.write(output,
                    Frame.request(Opcode.HELLO, new Messages.Hello(clientKind, 0, clientId).encode(), null, 0));
            Frame helloResp = FrameIO.read(input);
            if (helloResp == null || helloResp.opcode() != Opcode.HELLO.code || !helloResp.isResponse()) {
                throw new IOException("handshake failed");
            }
            ByteBuffer hb = helloResp.headerSlice();
            try {
                Resp.check(hb);
                hello = Messages.HelloResp.decode(hb);
            } catch (ScpException e) {
                throw e;
            } catch (RuntimeException e) {
                throw new IOException("malformed handshake response: " + e, e);
            }
            connected.setSoTimeout(0);
        } catch (IOException | RuntimeException e) {
            try {
                connected.close();
            } catch (IOException ignored) {
            }
            throw e;
        }
        socket = connected;
        in = input;
        out = output;
        this.serverHello = hello;

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
            try {
                socket.close();
            } catch (IOException ignored) {
            }
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
        Frame request;
        try {
            if (op == null) {
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "opcode is null");
            }
            request = Frame.request(op, header, payload, id);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<Frame> fut = new CompletableFuture<>();
        pending.put(id, fut);
        // whatever completes this future — response, connection failure, caller-side timeout or
        // orTimeout/cancel — the correlation entry must go with it, or long-lived pipelined
        // connections leak one entry per unanswered request
        fut.whenComplete((r, e) -> pending.remove(id, fut));
        try {
            writeLock.lock();
            try {
                FrameIO.write(out, request);
            } finally {
                writeLock.unlock();
            }
        } catch (IOException e) {
            pending.remove(id);
            failAll(e);
            fut.completeExceptionally(e);
            return fut;
        }
        // failAll() may have swept the map BETWEEN the closed-check above and our put — in that
        // window nobody will ever complete this future; fail it ourselves rather than orphan it
        if (closed.get() && pending.remove(id) != null) {
            fut.completeExceptionally(new IOException("connection closed"));
        }
        return fut;
    }

    /**
     * Pipelined send with a caller-side deadline. A timeout means this connection can no longer
     * be trusted for future pipelined traffic: the server may answer later, but the correlation
     * has been abandoned and pools must reconnect rather than reuse a stuck reader path.
     */
    public CompletableFuture<Frame> sendWithTimeout(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        CompletableFuture<Frame> fut = send(op, header, payload);
        fut.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
        fut.whenComplete((frame, err) -> {
            if (isTimeout(err)) {
                close();
            }
        });
        return fut;
    }

    private static boolean isTimeout(Throwable err) {
        Throwable t = err;
        while (t instanceof CompletionException && t.getCause() != null) {
            t = t.getCause();
        }
        return t instanceof TimeoutException;
    }

    /** Synchronous call; returns the response header positioned AFTER the error check. */
    public ByteBuffer call(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        Frame resp = callFrame(op, header, payload, timeoutMs);
        ByteBuffer hb = resp.headerSlice();
        try {
            Resp.check(hb);
        } catch (ScpException e) {
            throw e;
        } catch (RuntimeException e) {
            close();
            throw new ScpException(ErrorCode.INTERNAL, "malformed response for " + op + ": " + e);
        }
        return hb;
    }

    /** Synchronous call returning the whole frame (for payload-carrying responses); error NOT yet checked. */
    public Frame callFrame(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        CompletableFuture<Frame> fut = send(op, header, payload);
        try {
            return fut.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ScpException se) throw se;
            throw new ScpException(ErrorCode.INTERNAL, String.valueOf(e.getCause()));
        } catch (TimeoutException e) {
            ScpException timeout = new ScpException(ErrorCode.INTERNAL,
                    "timeout after " + timeoutMs + "ms for " + op);
            fut.completeExceptionally(timeout); // releases the correlation entry (cleanup hook)
            close();
            throw timeout;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ScpException interrupted = new ScpException(ErrorCode.INTERNAL, "interrupted");
            fut.completeExceptionally(interrupted);
            throw interrupted;
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    /** Outstanding correlation entries — observability and leak tests. */
    public int pendingCount() {
        return pending.size();
    }

    @Override
    public void close() {
        // fail pending futures synchronously: a caller observing isClosed()==true must be able
        // to assume no future from this connection is still silently pending
        failAll(new IOException("connection closed"));
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
