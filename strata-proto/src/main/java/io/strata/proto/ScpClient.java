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
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
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
 * One SCP connection over Netty: HELLO handshake, then pipelined request/response correlated by id.
 * Thread-safe; Netty serializes channel writes, responses are dispatched by the channel pipeline.
 */
public final class ScpClient implements AutoCloseable {
    public static final byte KIND_BROKER = 1;
    public static final byte KIND_STORAGE_NODE = 2;
    public static final byte KIND_METADATA = 3;
    public static final byte KIND_TOOL = 4;

    private final Channel channel;
    private final AtomicLong correlation = new AtomicLong(1);
    private final Map<Long, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CompletableFuture<Frame> handshake = new CompletableFuture<>();
    private final Messages.HelloResp serverHello;

    public ScpClient(String host, int port, byte clientKind, String clientId) throws IOException {
        this(host, port, clientKind, clientId, 5_000);
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
            Frame response;
            try {
                response = frame.copyToHeap();
            } finally {
                frame.close();
            }
            if (!handshake.isDone()) {
                handshake.complete(response);
                return;
            }
            CompletableFuture<Frame> fut = pending.remove(response.correlationId());
            if (fut != null) {
                fut.complete(response);
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
            CompletableFuture<Frame> fut = pending.remove(id);
            if (fut != null) {
                fut.completeExceptionally(e);
            }
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

        ChannelFuture write = channel.writeAndFlush(request);
        write.addListener(f -> {
            if (!f.isSuccess()) {
                pending.remove(id);
                IOException failure = asIOException(f.cause());
                failAll(failure);
                fut.completeExceptionally(failure);
            }
        });
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
        return closed.get() || !channel.isOpen();
    }

    /** Outstanding correlation entries — observability and leak tests. */
    public int pendingCount() {
        return pending.size();
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
