package io.strata.proto;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.strata.common.EnvConfig;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One SCP connection over Netty: HELLO handshake, then pipelined request/response correlated by id.
 * Thread-safe; Netty serializes channel writes, responses are dispatched by the channel pipeline.
 */
public final class ScpClient implements AutoCloseable {
    public static final byte KIND_BROKER = 1;
    public static final byte KIND_DATA_NODE = 2;
    public static final byte KIND_METADATA = 3;
    public static final byte KIND_TOOL = 4;
    private static final int MAX_PENDING_REQUESTS =
            EnvConfig.intEnv("STRATA_SCP_MAX_PENDING_REQUESTS", 1024);

    private final Channel channel;
    private final AtomicLong correlation = new AtomicLong(1);
    private record Pending(CompletableFuture<Frame> future, boolean borrow) {}

    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
    private final Semaphore pendingPermits = new Semaphore(MAX_PENDING_REQUESTS);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CompletableFuture<Frame> handshake = new CompletableFuture<>();
    private final Messages.HelloResp serverHello;

    public ScpClient(String host, int port, byte clientKind, String clientId) throws IOException {
        this(host, port, clientKind, clientId, io.strata.common.ConnectionPolicy.DEFAULT.connectTimeoutMs());
    }

    public ScpClient(String host, int port, byte clientKind, String clientId, int connectTimeoutMs) throws IOException {
        Channel connected = null;
        Messages.HelloResp hello;
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(NettyEventLoops.CLIENT_GROUP)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new NettyFrameCodec.Decoder())
                                    .addLast(new NettyFrameCodec.Encoder())
                                    .addLast(new ClientHandler());
                        }
                    });

            ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(host, port));
            if (!connectFuture.await(connectTimeoutMs, TimeUnit.MILLISECONDS)) {
                connectFuture.cancel(true);
                throw new SocketTimeoutException("connect timed out after " + connectTimeoutMs + "ms");
            }
            if (!connectFuture.isSuccess()) {
                throw asIOException(connectFuture.cause());
            }
            connected = connectFuture.channel();

            Frame helloFrame = Frame.request(Opcode.HELLO,
                    new Messages.Hello(clientKind, 0, clientId).encode(), null, 0);
            ChannelFuture writeHello = connected.writeAndFlush(helloFrame);
            if (!writeHello.await(connectTimeoutMs, TimeUnit.MILLISECONDS)) {
                writeHello.cancel(true);
                throw new SocketTimeoutException("handshake write timed out after " + connectTimeoutMs + "ms");
            }
            if (!writeHello.isSuccess()) {
                throw asIOException(writeHello.cause());
            }

            Frame helloResp = awaitHandshake(connectTimeoutMs);
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
        } catch (IOException | RuntimeException e) {
            closeChannel(connected);
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closeChannel(connected);
            throw new IOException("interrupted", e);
        }
        channel = connected;
        serverHello = hello;
    }

    ScpClient(Channel channel, Messages.HelloResp serverHello) {
        this.channel = channel;
        this.serverHello = serverHello;
    }

    private Frame awaitHandshake(int timeoutMs) throws IOException, InterruptedException {
        try {
            return handshake.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("handshake timed out after " + timeoutMs + "ms");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IOException(String.valueOf(cause), cause);
        }
    }

    public Messages.HelloResp serverHello() {
        return serverHello;
    }

    private final class ClientHandler extends SimpleChannelInboundHandler<Frame> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            if (!handshake.isDone()) {
                Frame response;
                try {
                    response = frame.copyToHeap();
                } finally {
                    frame.close();
                }
                handshake.complete(response);
                return;
            }
            Pending holder = pending.remove(frame.correlationId());
            if (holder == null) {
                frame.close();   // abandoned/timed-out: release the retained pooled buffer
                return;
            }
            pendingPermits.release();
            if (holder.borrow()) {
                holder.future().complete(frame);  // ownership transfers to caller; no copy, no close
            } else {
                Frame response;
                try {
                    response = frame.copyToHeap();
                } finally {
                    frame.close();
                }
                holder.future().complete(response);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            failAll(new IOException("connection closed"));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            failAll(asIOException(cause));
            ctx.close();
        }
    }

    private void failAll(Exception e) {
        closed.set(true);
        handshake.completeExceptionally(e);
        pending.keySet().forEach(id -> {
            Pending holder = pending.remove(id);
            if (holder != null) {
                pendingPermits.release();
                holder.future().completeExceptionally(e);
            }
        });
    }

    public CompletableFuture<Frame> send(Opcode op, byte[] header, ByteBuffer payload) {
        return send(op, header, payload, false);
    }

    /**
     * Package-private: callers outside this package should use {@link #send(Opcode, byte[], ByteBuffer)}
     * or {@link #callFrameBorrowed}. When {@code borrow=true} the returned future yields a retained
     * Netty frame whose pooled buffer the caller is responsible for releasing via {@link Frame#close()}.
     */
    CompletableFuture<Frame> send(Opcode op, byte[] header, ByteBuffer payload, boolean borrow) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IOException("connection closed"));
        }
        if (!pendingPermits.tryAcquire()) {
            return CompletableFuture.failedFuture(new ScpException(ErrorCode.THROTTLED,
                    "too many pending requests on connection: " + pending.size()
                            + " >= " + MAX_PENDING_REQUESTS));
        }
        long id = correlation.getAndIncrement();
        Frame request;
        try {
            if (op == null) {
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "opcode is null");
            }
            request = Frame.request(op, copyHeader(header), copyPayload(payload), id);
        } catch (RuntimeException e) {
            pendingPermits.release();
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Frame> fut = new CompletableFuture<>();
        Pending holder = new Pending(fut, borrow);
        pending.put(id, holder);
        fut.whenComplete((r, e) -> removePending(id, holder));

        ChannelFuture write;
        try {
            write = channel.writeAndFlush(request);
        } catch (RuntimeException e) {
            IOException failure = asIOException(e);
            removePending(id, holder);
            failAll(failure);
            fut.completeExceptionally(failure);
            return fut;
        }
        write.addListener(f -> {
            if (!f.isSuccess()) {
                removePending(id, holder);
                IOException failure = asIOException(f.cause());
                failAll(failure);
                fut.completeExceptionally(failure);
            }
        });
        if (closed.get() && removePending(id, holder)) {
            fut.completeExceptionally(new IOException("connection closed"));
        }
        return fut;
    }

    private boolean removePending(long id, Pending holder) {
        boolean removed = pending.remove(id, holder);
        if (removed) {
            pendingPermits.release();
        }
        return removed;
    }

    private static byte[] copyHeader(byte[] header) {
        return header == null || header.length == 0 ? header : header.clone();
    }

    private static ByteBuffer copyPayload(ByteBuffer payload) {
        if (payload == null || !payload.hasRemaining()) {
            return payload;
        }
        ByteBuffer src = payload.duplicate();
        byte[] bytes = new byte[src.remaining()];
        src.get(bytes);
        return ByteBuffer.wrap(bytes);
    }

    /**
     * Pipelined send with a caller-side deadline. A timeout means this connection can no longer
     * be trusted for future pipelined traffic: the server may answer later, but the correlation
     * has been abandoned and pools must reconnect rather than reuse a stuck reader path.
     */
    public CompletableFuture<Frame> sendWithTimeout(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        CompletableFuture<Frame> fut = send(op, header, payload);
        // Schedule the deadline on THIS connection's Netty event loop rather than
        // CompletableFuture.orTimeout, which routes every call through one process-wide Delayer
        // executor. Under a deep append pipeline (responses arrive fast, so each timeout is
        // scheduled then immediately cancelled) that single delay-queue heap is a top CPU cost
        // (schedule + heap-remove per request, ~135k/s); per-event-loop timers spread the load
        // across the I/O threads and Netty's scheduled-task cancellation is cheap.
        io.netty.util.concurrent.ScheduledFuture<?> deadline = channel.eventLoop().schedule(
                () -> fut.completeExceptionally(new TimeoutException("timed out after " + timeoutMs + "ms")),
                timeoutMs, TimeUnit.MILLISECONDS);
        fut.whenComplete((frame, err) -> {
            deadline.cancel(false);
            if (isTimeout(err)) {
                close();
            }
        });
        return fut;
    }

    private static boolean isTimeout(Throwable err) {
        return ScpException.rootCause(err) instanceof TimeoutException;
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

    /**
     * Synchronous call returning a response frame whose pooled buffer is retained; the caller MUST
     * close it (try-with-resources). Until it is closed the frame pins a pooled direct buffer (up to
     * the 64 MiB max frame), so a slow consumer raises direct-pool residency — close promptly.
     */
    public Frame callFrameBorrowed(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        CompletableFuture<Frame> fut = send(op, header, payload, true);
        try {
            return fut.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ScpException se) throw se;
            throw new ScpException(ErrorCode.INTERNAL, String.valueOf(e.getCause()));
        } catch (TimeoutException e) {
            ScpException timeout = new ScpException(ErrorCode.INTERNAL,
                    "timeout after " + timeoutMs + "ms for " + op);
            fut.completeExceptionally(timeout);
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
        return closed.get() || !channel.isOpen();
    }

    /** Outstanding correlation entries — observability and leak tests. */
    public int pendingCount() {
        return pending.size();
    }

    /** Maximum outstanding requests admitted on one connection. */
    public static int maxPendingRequests() {
        return MAX_PENDING_REQUESTS;
    }

    /** Available request slots on this connection. */
    public int pendingCapacity() {
        return pendingPermits.availablePermits();
    }

    @Override
    public void close() {
        failAll(new IOException("connection closed"));
        if (channel.eventLoop().inEventLoop()) {
            channel.close();
        } else {
            channel.close().awaitUninterruptibly(1, TimeUnit.SECONDS);
        }
    }

    private static void closeChannel(Channel channel) {
        if (channel != null) {
            channel.close().awaitUninterruptibly(1, TimeUnit.SECONDS);
        }
    }

    private static IOException asIOException(Throwable cause) {
        if (cause instanceof IOException io) {
            return io;
        }
        return new IOException(String.valueOf(cause), cause);
    }
}
