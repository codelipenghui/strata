package io.strata.proto;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.strata.common.EnvConfig;
import io.strata.common.ErrorCode;
import io.strata.common.FailureInjector;
import io.strata.common.ScpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SCP server over Netty. Handler invocation is serialized per connection on a virtual-thread
 * executor so blocking storage/metadata code never runs on a Netty event-loop thread.
 */
public final class ScpServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ScpServer.class);
    private static final int DEFAULT_MAX_INFLIGHT_REQUESTS =
            EnvConfig.intEnv("STRATA_SCP_MAX_INFLIGHT_REQUESTS", 1024);
    private static final long DEFAULT_MAX_INFLIGHT_BYTES =
            EnvConfig.longEnv("STRATA_SCP_MAX_INFLIGHT_BYTES", 1L << 30);

    /**
     * Handles one request frame; returns the response frame. Throw ScpException for protocol errors.
     *
     * handleAsync is the dispatch entry point: its synchronous portion runs on the connection
     * handler executor IN ORDER (validation/writes keep per-chunk contiguity semantics), but the
     * response may complete later (e.g. group commit) — the connection keeps processing subsequent
     * frames meanwhile. Out-of-order responses are protocol-legal: clients correlate by id.
     *
     * Request header/payload slices are valid during the synchronous handleAsync call. Async
     * continuations that need request bytes after handleAsync returns must copy them first; the
     * server may release retained transport buffers if the connection closes before completion.
     */
    public interface Handler {
        Frame handle(Frame request) throws Exception;

        default CompletableFuture<Frame> handleAsync(Frame request) throws Exception {
            return CompletableFuture.completedFuture(handle(request));
        }

        /**
         * Composes two handlers onto one listener: opcodes &gt;= 0x0100 (control plane — data-node
         * registration + client metadata RPCs) route to {@code controlPlane}; everything else (data
         * plane; PING too) to {@code dataPlane}. HELLO never reaches a handler (the server answers it).
         * Lets a combined node serve data + metadata on a single SCP port.
         */
        static Handler route(Handler dataPlane, Handler controlPlane) {
            return new Handler() {
                @Override
                public Frame handle(Frame request) throws Exception {
                    return pick(request).handle(request);
                }

                @Override
                public CompletableFuture<Frame> handleAsync(Frame request) throws Exception {
                    return pick(request).handleAsync(request);
                }

                private Handler pick(Frame request) {
                    return (request.opcode() & 0xFFFF) >= 0x0100 ? controlPlane : dataPlane;
                }
            };
        }
    }

    private final Channel serverChannel;
    private final ChannelGroup connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final Handler handler;
    private final int nodeId; // fixed at construction from the volume-bound identity (STRATA_NODE_ID)
    private final long incMsb;
    private final long incLsb;
    private final int maxInflightRequests;
    private final long maxInflightBytes;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile RequestObserver requestObserver; // optional; set by the metrics layer

    /** Installs (or clears) the per-request latency observer. Safe to set after the server starts. */
    public void setRequestObserver(RequestObserver observer) {
        this.requestObserver = observer;
    }

    public ScpServer(int port, int nodeId, long incMsb, long incLsb, Handler handler) throws IOException {
        this(port, nodeId, incMsb, incLsb, handler,
                DEFAULT_MAX_INFLIGHT_REQUESTS, DEFAULT_MAX_INFLIGHT_BYTES);
    }

    ScpServer(int port, int nodeId, long incMsb, long incLsb, Handler handler,
              int maxInflightRequests, long maxInflightBytes) throws IOException {
        this.handler = handler;
        this.nodeId = nodeId;
        this.incMsb = incMsb;
        this.incLsb = incLsb;
        this.maxInflightRequests = Math.max(1, maxInflightRequests);
        this.maxInflightBytes = Math.max(Frame.PREAMBLE_AFTER_LEN, maxInflightBytes);

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(NettyEventLoops.SERVER_BOSS_GROUP, NettyEventLoops.SERVER_WORKER_GROUP)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new NettyFrameCodec.Decoder())
                                .addLast(new NettyFrameCodec.Encoder())
                                .addLast(new ConnectionHandler(ch));
                    }
                });

        try {
            ChannelFuture bind = bootstrap.bind(new InetSocketAddress(port)).sync();
            this.serverChannel = bind.channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        } catch (RuntimeException e) {
            throw new IOException("failed to bind SCP server", e);
        }
    }

    public int port() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    private final class ConnectionHandler extends SimpleChannelInboundHandler<Frame> {
        private final ExecutorService requestExecutor;
        private final Set<Frame> inFlightAsyncRequests = ConcurrentHashMap.newKeySet();
        private final ConcurrentHashMap<Frame, Integer> reservedBytesByFrame = new ConcurrentHashMap<>();
        private final AtomicInteger inflightRequests = new AtomicInteger();
        private final AtomicLong inflightBytes = new AtomicLong();
        private final AtomicBoolean connectionOpen = new AtomicBoolean(true);
        private boolean helloComplete;

        ConnectionHandler(Channel channel) {
            this.requestExecutor = Executors.newSingleThreadExecutor(
                    Thread.ofVirtual().name("scp-conn-" + channel.remoteAddress() + "-", 0).factory());
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            connections.add(ctx.channel());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            if (!reserveInbound(frame)) {
                writeResponse(ctx, Frame.response(frame,
                        Resp.error(ErrorCode.THROTTLED, "too many in-flight requests", maxInflightRequests),
                        null), true, frame);
                return;
            }
            FrameTask task = new FrameTask(ctx, frame);
            try {
                requestExecutor.execute(task);
            } catch (RuntimeException | Error e) {
                task.closeIfPending();
                throw e;
            }
        }

        private boolean reserveInbound(Frame frame) {
            int frameBytes = Frame.PREAMBLE_AFTER_LEN + frame.headerSlice().remaining() + frame.payloadLength();
            int requests = inflightRequests.incrementAndGet();
            long bytes = inflightBytes.addAndGet(frameBytes);
            if (requests <= maxInflightRequests && bytes <= maxInflightBytes) {
                reservedBytesByFrame.put(frame, frameBytes);
                return true;
            }
            inflightRequests.decrementAndGet();
            inflightBytes.addAndGet(-frameBytes);
            return false;
        }

        private void releaseInbound(Frame frame) {
            Integer bytes = reservedBytesByFrame.remove(frame);
            if (bytes != null) {
                inflightRequests.decrementAndGet();
                inflightBytes.addAndGet(-bytes);
            }
        }

        private final class FrameTask implements Runnable {
            private final ChannelHandlerContext ctx;
            private final Frame frame;
            private final AtomicBoolean started = new AtomicBoolean(false);

            private FrameTask(ChannelHandlerContext ctx, Frame frame) {
                this.ctx = ctx;
                this.frame = frame;
            }

            @Override
            public void run() {
                started.set(true);
                try {
                    processFrame(ctx, frame);
                } catch (RuntimeException | Error e) {
                    releaseInbound(frame);
                    frame.close();
                    throw e;
                }
            }

            private void closeIfPending() {
                if (started.compareAndSet(false, true)) {
                    releaseInbound(frame);
                    frame.close();
                }
            }
        }

        private void processFrame(ChannelHandlerContext ctx, Frame frame) {
            if (closed.get() || !connectionOpen.get() || !ctx.channel().isActive()) {
                releaseInbound(frame);
                frame.close();
                return;
            }
            if (!helloComplete) {
                handleHello(ctx, frame);
                return;
            }
            handleRequest(ctx, frame);
        }

        private void handleHello(ChannelHandlerContext ctx, Frame hello) {
            if (hello.opcode() != Opcode.HELLO.code) {
                writeResponse(ctx, Frame.response(hello,
                        Resp.error(ErrorCode.UNKNOWN_OPCODE, "first frame must be HELLO", 0), null), true, hello);
                return;
            }
            try {
                Messages.Hello.decode(hello.headerSlice()); // validates frame-version overlap
            } catch (RuntimeException e) {
                // incompatible version range or malformed HELLO header: answer with a typed
                // error instead of silently dropping the connection
                writeResponse(ctx, Frame.response(hello,
                        Resp.error(ErrorCode.UNSUPPORTED_VERSION, String.valueOf(e.getMessage()), 0), null), true,
                        hello);
                return;
            }
            helloComplete = true;
            writeResponse(ctx, Frame.response(hello,
                    new Messages.HelloResp(0, nodeId, incMsb, incLsb, FrameIO.MAX_FRAME_BYTES,
                            maxInflightBytes).encode(),
                    null), false, hello);
        }

        private void handleRequest(ChannelHandlerContext ctx, Frame req) {
            long startNanos = System.nanoTime();
            CompletableFuture<Frame> respF;
            boolean handlerFailed = false;
            try {
                respF = handler.handleAsync(req);
                if (respF == null) {
                    respF = CompletableFuture.completedFuture(internalError(req, "handler returned null future"));
                    handlerFailed = true;
                }
            } catch (ScpException e) {
                respF = CompletableFuture.completedFuture(
                        Frame.response(req, Resp.error(e.code(), e.getMessage(), e.detail(), e.leaderHint()), null));
                handlerFailed = true;
            } catch (Exception e) {
                log.warn("handler error for opcode 0x{}", Integer.toHexString(req.opcode()), e);
                respF = CompletableFuture.completedFuture(
                        Frame.response(req, Resp.error(ErrorCode.INTERNAL, String.valueOf(e), 0), null));
                handlerFailed = true;
            }
            // The handler set the request's namespace (if any) into RequestContext during its synchronous
            // decode, on this same connection-handler thread — read it now, before any async completion,
            // and carry it into both the sync and async observe paths.
            String ns = RequestContext.takeNamespace();
            if (respF.isDone() && !respF.isCompletedExceptionally()) {
                observeRequest(req, startNanos, !handlerFailed, ns);
                writeResponse(ctx, requireResponse(req, respF.join()), false, req); // fast path, no extra hop
            } else {
                // Test seam: lets a test pause the request thread here to drive the close-vs-register
                // race deterministically (connection closes after handleAsync returns but before the add).
                FailureInjector.point("scp.handleRequest.beforeInflightAdd");
                inFlightAsyncRequests.add(req);
                respF.whenComplete((resp, err) -> {
                    if (!inFlightAsyncRequests.remove(req)) {
                        // channelInactive (connection closed) or the close-race guard below already claimed
                        // and released req — there is no open connection to answer, so stop here.
                        return;
                    }
                    observeRequest(req, startNanos, err == null, ns);
                    Frame frame = resp;
                    if (err != null) {
                        Throwable cause = err instanceof java.util.concurrent.CompletionException
                                ? err.getCause() : err;
                        frame = cause instanceof ScpException se
                                ? Frame.response(req, Resp.error(se.code(), se.getMessage(), se.detail(), se.leaderHint()), null)
                                : Frame.response(req, Resp.error(ErrorCode.INTERNAL, String.valueOf(cause), 0), null);
                    }
                    writeResponse(ctx, requireResponse(req, frame), false, req);
                });
                // The connection can close between handleAsync returning and the add above; channelInactive
                // would then drain inFlightAsyncRequests before req was in it, orphaning the request buffer.
                // Re-check and claim it ourselves — remove() is the single-owner handoff across this path,
                // channelInactive, and the whenComplete callback, so req is released exactly once.
                if (!connectionOpen.get() && inFlightAsyncRequests.remove(req)) {
                    releaseInbound(req);
                    req.close();
                }
            }
        }

        private void observeRequest(Frame req, long startNanos, boolean success, String namespace) {
            RequestObserver obs = requestObserver;
            if (obs == null) {
                return;
            }
            Opcode op = Opcode.fromCode(req.opcode());
            obs.observe(op != null ? op.name() : "unknown", namespace, System.nanoTime() - startNanos, success);
        }

        private void writeResponse(ChannelHandlerContext ctx, Frame frame, boolean closeAfterWrite,
                                   Frame releaseAfterWrite) {
            if (closed.get() || !connectionOpen.get() || !ctx.channel().isActive()) {
                closeFrames(frame, releaseAfterWrite);
                return;
            }
            if (frame.hasFilePayload()) {
                writeFileResponse(ctx, frame, closeAfterWrite, releaseAfterWrite);
                return;
            }
            ChannelFuture write;
            try {
                write = ctx.writeAndFlush(frame);
            } catch (RuntimeException e) {
                closeFrames(frame, releaseAfterWrite);
                throw e;
            }
            finishWrite(ctx, write, closeAfterWrite, frame, releaseAfterWrite);
        }

        private void writeFileResponse(ChannelHandlerContext ctx, Frame frame, boolean closeAfterWrite,
                                       Frame releaseAfterWrite) {
            // The prefix and the file region are two separate writes that MUST land in the channel's
            // outbound buffer with nothing between them. Async handlers complete on other threads
            // (e.g. the group-commit flusher writing an APPEND ack), and their single writeAndFlush
            // can otherwise be marshalled onto the event loop BETWEEN this prefix and region —
            // slotting a whole frame mid-response and corrupting the stream. Issuing both from one
            // event-loop task makes the pair atomic relative to every other write on this channel.
            if (ctx.channel().eventLoop().inEventLoop()) {
                writeFileResponseOnEventLoop(ctx, frame, closeAfterWrite, releaseAfterWrite);
            } else {
                ctx.channel().eventLoop().execute(
                        () -> writeFileResponseOnEventLoop(ctx, frame, closeAfterWrite, releaseAfterWrite));
            }
        }

        private void writeFileResponseOnEventLoop(ChannelHandlerContext ctx, Frame frame,
                                                  boolean closeAfterWrite, Frame releaseAfterWrite) {
            Frame.FilePayload file = frame.filePayload();
            ByteBuf prefix;
            DefaultFileRegion region;
            try {
                prefix = NettyFrameCodec.encodeFilePrefix(ctx.alloc(), frame);
                region = new DefaultFileRegion(file.channel(), file.position(), file.length());
            } catch (IOException | RuntimeException e) {
                closeFrames(frame, releaseAfterWrite);
                ctx.close();
                return;
            }
            ctx.write(prefix);
            FailureInjector.point("scp.writeFileResponse.betweenPrefixAndRegion");
            ChannelFuture write = ctx.writeAndFlush(region);
            finishWrite(ctx, write, closeAfterWrite, frame, releaseAfterWrite);
        }

        private void finishWrite(ChannelHandlerContext ctx, ChannelFuture write, boolean closeAfterWrite,
                                 Frame frame, Frame releaseAfterWrite) {
            write.addListener(f -> closeFrames(frame, releaseAfterWrite));
            if (closeAfterWrite) {
                write.addListener(f -> ctx.close());
            }
            write.addListener(f -> {
                if (!f.isSuccess()) {
                    ctx.close();
                }
            });
        }

        private void closeFrames(Frame frame, Frame releaseAfterWrite) {
            frame.close();
            if (releaseAfterWrite != frame) {
                releaseInbound(releaseAfterWrite);
                releaseAfterWrite.close();
            } else {
                releaseInbound(frame);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            connectionOpen.set(false);
            connections.remove(ctx.channel());
            for (Frame frame : inFlightAsyncRequests) {
                if (inFlightAsyncRequests.remove(frame)) {
                    releaseInbound(frame);
                    frame.close();
                }
            }
            // Do not shutdownNow(): interrupting the active request thread while it is in a
            // FileChannel operation can close the shared chunk channel (InterruptibleChannel),
            // corrupting the in-process storage handle for unrelated future requests. Let the
            // running request finish; queued tasks will observe connectionOpen=false and release
            // their frames without invoking the handler.
            requestExecutor.shutdown();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private static Frame requireResponse(Frame req, Frame frame) {
        return frame != null ? frame : internalError(req, "handler returned null response");
    }

    private static Frame internalError(Frame req, String message) {
        return Frame.response(req, Resp.error(ErrorCode.INTERNAL, message, 0), null);
    }

    /** Convenience for handlers: success response with header bytes and optional payload. */
    public static Frame ok(Frame req, byte[] header, ByteBuffer payload) {
        return Frame.response(req, header, payload);
    }

    /** Convenience for handlers: success response whose payload is streamed from a file region. */
    public static Frame okFileRegion(Frame req, byte[] header, FileChannel channel, long position,
                                     int length, Runnable releaser) {
        return Frame.fileResponse(req, header, new Frame.FilePayload(channel, position, length, releaser));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        serverChannel.close().awaitUninterruptibly(1, java.util.concurrent.TimeUnit.SECONDS);
        connections.close().awaitUninterruptibly(1, java.util.concurrent.TimeUnit.SECONDS);
    }
}
