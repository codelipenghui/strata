package io.strata.proto;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultFileRegion;
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
import java.util.ArrayDeque;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SCP server over Netty. Handler invocation is serialized per connection on a virtual-thread
 * drain so blocking storage/metadata code never runs on a Netty event-loop thread.
 */
public final class ScpServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ScpServer.class);
    private static final int DEFAULT_MAX_INFLIGHT_REQUESTS =
            EnvConfig.intEnv("STRATA_SCP_MAX_INFLIGHT_REQUESTS", 1024);
    private static final long DEFAULT_MAX_INFLIGHT_BYTES =
            EnvConfig.longEnv("STRATA_SCP_MAX_INFLIGHT_BYTES", 1L << 30);
    private static final int MAX_POOLED_FRAME_TASKS =
            EnvConfig.intEnv("STRATA_SCP_FRAME_TASK_POOL_SIZE", 256);
    private static final int MAX_POOLED_RESPONSE_WRITE_LISTENERS =
            EnvConfig.intEnv("STRATA_SCP_RESPONSE_WRITE_LISTENER_POOL_SIZE", 256);
    private static final int MAX_POOLED_OK_U64_WRITE_TASKS =
            EnvConfig.intEnv("STRATA_SCP_OK_U64_WRITE_TASK_POOL_SIZE", 256);

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

        default boolean requiresAsyncHandling(Frame request) {
            return true;
        }

        default CompletableFuture<Frame> handleAsync(Frame request) throws Exception {
            return CompletableFuture.completedFuture(handle(request));
        }

        /**
         * Async dispatch result for handlers that can often complete synchronously. Return either a
         * {@link Frame}, an {@link OkU64Response}, or a {@code CompletableFuture} that completes with
         * one of those when the response must complete later.
         */
        default Object handleAsyncResult(Frame request) throws Exception {
            return handleAsync(request);
        }

        /**
         * Allocation-free dispatch result hook for hot handlers. Implementations must not retain
         * {@code sink}; fill it during the call and return.
         */
        default void handleAsyncResult(Frame request, ResponseSink sink) throws Exception {
            sink.result(handleAsyncResult(request));
        }

        static Handler sync(Handler handler) {
            return new Handler() {
                @Override
                public Frame handle(Frame request) throws Exception {
                    return handler.handle(request);
                }

                @Override
                public boolean requiresAsyncHandling(Frame request) {
                    return false;
                }
            };
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

                @Override
                public Object handleAsyncResult(Frame request) throws Exception {
                    return pick(request).handleAsyncResult(request);
                }

                @Override
                public void handleAsyncResult(Frame request, ResponseSink sink) throws Exception {
                    pick(request).handleAsyncResult(request, sink);
                }

                @Override
                public boolean requiresAsyncHandling(Frame request) {
                    return pick(request).requiresAsyncHandling(request);
                }

                private Handler pick(Frame request) {
                    return (request.opcode() & 0xFFFF) >= 0x0100 ? controlPlane : dataPlane;
                }
            };
        }
    }

    public static final class ResponseSink {
        private static final int EMPTY = 0;
        private static final int OBJECT = 1;
        private static final int FUTURE = 2;
        private static final int OK_U64 = 3;
        private static final int DEFERRED_OK_U64 = 4;

        private int kind;
        private Object response;
        private CompletableFuture<?> future;
        private long okU64Value;

        private ResponseSink() {}

        public void frame(Frame frame) {
            result(frame);
        }

        public void result(Object result) {
            if (result instanceof OkU64Response okU64) {
                okU64(okU64.value(), okU64.waitFor());
                return;
            }
            if (result instanceof CompletableFuture<?> responseFuture) {
                future(responseFuture);
                return;
            }
            kind = OBJECT;
            response = result;
            future = null;
            okU64Value = 0;
        }

        public void future(CompletableFuture<?> responseFuture) {
            if (responseFuture == null) {
                result(null);
                return;
            }
            kind = FUTURE;
            response = null;
            future = responseFuture;
            okU64Value = 0;
        }

        public void okU64(long value) {
            kind = OK_U64;
            response = null;
            future = null;
            okU64Value = value;
        }

        public void okU64(long value, CompletableFuture<Void> waitFor) {
            if (waitFor == null) {
                okU64(value);
                return;
            }
            kind = DEFERRED_OK_U64;
            response = null;
            future = waitFor;
            okU64Value = value;
        }

        private void reset() {
            kind = EMPTY;
            response = null;
            future = null;
            okU64Value = 0;
        }
    }

    /**
     * Lightweight success response for hot u64-ack paths. The server writes it directly as a small
     * ByteBuf, avoiding a full {@link Frame} allocation for APPEND acknowledgements.
     */
    public static final class OkU64Response {
        private final long value;
        private final CompletableFuture<Void> waitFor;

        private OkU64Response(long value, CompletableFuture<Void> waitFor) {
            this.value = value;
            this.waitFor = waitFor;
        }

        private long value() {
            return value;
        }

        private CompletableFuture<Void> waitFor() {
            return waitFor;
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
        private final SerialRequestExecutor requestExecutor;
        private final Set<Frame> inFlightAsyncRequests = ConcurrentHashMap.newKeySet();
        private final ArrayDeque<FrameTask> frameTasks = new ArrayDeque<>();
        private final ArrayDeque<ResponseWriteListener> responseWriteListeners = new ArrayDeque<>();
        private final ArrayDeque<OkU64WriteTask> okU64WriteTasks = new ArrayDeque<>();
        private final ResponseSink responseSink = new ResponseSink();
        private final AtomicInteger inflightRequests = new AtomicInteger();
        private final AtomicLong inflightBytes = new AtomicLong();
        private final AtomicBoolean connectionOpen = new AtomicBoolean(true);
        private boolean helloComplete;

        ConnectionHandler(Channel channel) {
            this.requestExecutor = new SerialRequestExecutor(channel);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            connections.add(ctx.channel());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            if (!reserveInbound(frame)) {
                // This request was never admitted into reservedBytesByFrame. Send the small
                // close response without reserving it against request accounting.
                writeUnreservedResponse(ctx, Frame.response(frame,
                        Resp.error(ErrorCode.THROTTLED, "too many in-flight requests", maxInflightRequests),
                        null), true, frame);
                return;
            }
            FrameTask task = frameTask(ctx, frame);
            try {
                requestExecutor.execute(task);
            } catch (RuntimeException | Error e) {
                task.closeRejected();
                throw e;
            }
        }

        private FrameTask frameTask(ChannelHandlerContext ctx, Frame frame) {
            FrameTask task;
            synchronized (frameTasks) {
                task = frameTasks.pollFirst();
            }
            if (task == null) {
                task = new FrameTask();
            }
            task.reset(ctx, frame);
            return task;
        }

        private void recycleFrameTask(FrameTask task) {
            if (MAX_POOLED_FRAME_TASKS <= 0) {
                return;
            }
            synchronized (frameTasks) {
                if (frameTasks.size() < MAX_POOLED_FRAME_TASKS) {
                    frameTasks.addFirst(task);
                }
            }
        }

        private boolean reserveInbound(Frame frame) {
            long frameBytes = frameWireBytes(frame);
            int requests = inflightRequests.incrementAndGet();
            long bytes = inflightBytes.addAndGet(frameBytes);
            if (requests <= maxInflightRequests && bytes <= maxInflightBytes) {
                frame.reserveWireBytes(frameBytes);
                return true;
            }
            inflightRequests.decrementAndGet();
            inflightBytes.addAndGet(-frameBytes);
            return false;
        }

        private boolean reserveOutbound(Frame response, Frame request) {
            long frameBytes = frameWireBytes(response);
            long bytes = inflightBytes.addAndGet(frameBytes);
            if (bytes <= maxInflightBytes) {
                request.reserveWireBytes(frameBytes);
                return true;
            }
            inflightBytes.addAndGet(-frameBytes);
            return false;
        }

        private long frameWireBytes(Frame frame) {
            long payloadBytes = frame.hasFilePayload() ? frame.filePayload().length() : frame.payloadLength();
            return Frame.PREAMBLE_AFTER_LEN + frame.headerLength() + payloadBytes;
        }

        private void releaseInbound(Frame frame) {
            long bytes = frame.drainReservedWireBytes();
            if (bytes != 0) {
                inflightRequests.decrementAndGet();
                inflightBytes.addAndGet(-bytes);
            }
        }

        private final class SerialRequestExecutor {
            private final ArrayDeque<FrameTask> queue = new ArrayDeque<>();
            private final Thread worker;
            private boolean shutdown;

            private SerialRequestExecutor(Channel channel) {
                this.worker = Thread.ofVirtual()
                        .name("scp-conn-" + channel.remoteAddress() + "-", 0)
                        .start(this::drain);
            }

            private void execute(FrameTask task) {
                synchronized (queue) {
                    if (shutdown) {
                        throw new RejectedExecutionException("connection request executor is shut down");
                    }
                    queue.addLast(task);
                    queue.notify();
                }
            }

            private void drain() {
                while (true) {
                    FrameTask task;
                    synchronized (queue) {
                        while ((task = queue.pollFirst()) == null) {
                            if (shutdown) {
                                return;
                            }
                            try {
                                queue.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                    try {
                        task.run();
                    } catch (RuntimeException | Error e) {
                        log.warn("scp request task failed", e);
                    }
                }
            }

            private void shutdown() {
                synchronized (queue) {
                    shutdown = true;
                    queue.notify();
                }
            }
        }

        private final class FrameTask implements Runnable {
            private ChannelHandlerContext ctx;
            private Frame frame;

            private void reset(ChannelHandlerContext ctx, Frame frame) {
                this.ctx = ctx;
                this.frame = frame;
            }

            @Override
            public void run() {
                ChannelHandlerContext localCtx = ctx;
                Frame localFrame = frame;
                ctx = null;
                frame = null;
                try {
                    processFrame(localCtx, localFrame);
                } catch (RuntimeException | Error e) {
                    releaseInbound(localFrame);
                    localFrame.close();
                    throw e;
                } finally {
                    recycleFrameTask(this);
                }
            }

            private void closeRejected() {
                Frame localFrame = frame;
                ctx = null;
                frame = null;
                try {
                    releaseInbound(localFrame);
                    localFrame.close();
                } finally {
                    recycleFrameTask(this);
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
                Messages.Hello.decode(hello.headerReadBuffer()); // validates frame-version overlap
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
            CompletableFuture<?> respF;
            Object immediateResp = null;
            boolean immediateOkU64 = false;
            long immediateOkU64Value = 0;
            boolean deferredOkU64 = false;
            long deferredOkU64Value = 0;
            boolean handlerFailed = false;
            try {
                if (handler.requiresAsyncHandling(req)) {
                    responseSink.reset();
                    try {
                        handler.handleAsyncResult(req, responseSink);
                        switch (responseSink.kind) {
                            case ResponseSink.OBJECT -> {
                                Object result = responseSink.response;
                                if (result instanceof Frame frame) {
                                    respF = null;
                                    immediateResp = frame;
                                } else if (result == null) {
                                    respF = null;
                                    immediateResp = internalError(req, "handler returned null future");
                                    handlerFailed = true;
                                } else {
                                    respF = null;
                                    immediateResp = internalError(req, "handler returned unsupported async response");
                                    handlerFailed = true;
                                }
                            }
                            case ResponseSink.FUTURE -> {
                                respF = responseSink.future;
                            }
                            case ResponseSink.OK_U64 -> {
                                respF = null;
                                immediateOkU64 = true;
                                immediateOkU64Value = responseSink.okU64Value;
                            }
                            case ResponseSink.DEFERRED_OK_U64 -> {
                                respF = responseSink.future;
                                deferredOkU64 = true;
                                deferredOkU64Value = responseSink.okU64Value;
                            }
                            default -> {
                                respF = null;
                                immediateResp = internalError(req, "handler returned null future");
                                handlerFailed = true;
                            }
                        }
                    } finally {
                        responseSink.reset();
                    }
                } else {
                    respF = null;
                    immediateResp = handler.handle(req);
                    if (immediateResp == null) {
                        immediateResp = internalError(req, "handler returned null response");
                        handlerFailed = true;
                    }
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
            if (respF == null) {
                observeRequest(req, startNanos, !handlerFailed, ns);
                if (immediateOkU64) {
                    writeOkU64Response(ctx, req, immediateOkU64Value);
                    return;
                }
                writeResponseObject(ctx, req, requireResponse(req, immediateResp));
                return;
            }
            boolean asyncOkU64 = deferredOkU64;
            long asyncOkU64Value = deferredOkU64Value;
            if (respF.isDone() && !respF.isCompletedExceptionally()) {
                observeRequest(req, startNanos, !handlerFailed, ns);
                if (asyncOkU64) {
                    writeOkU64Response(ctx, req, asyncOkU64Value);
                } else {
                    writeResponseObject(ctx, req, requireResponse(req, respF.join())); // fast path
                }
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
                    if (err != null) {
                        Throwable cause = err instanceof CompletionException
                                ? err.getCause() : err;
                        Frame frame = cause instanceof ScpException se
                                ? Frame.response(req, Resp.error(se.code(), se.getMessage(), se.detail(), se.leaderHint()), null)
                                : Frame.response(req, Resp.error(ErrorCode.INTERNAL, String.valueOf(cause), 0), null);
                        writeResponse(ctx, frame, false, req);
                        return;
                    }
                    if (asyncOkU64) {
                        writeOkU64Response(ctx, req, asyncOkU64Value);
                    } else {
                        writeResponseObject(ctx, req, requireResponse(req, resp));
                    }
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
            if (!reserveOutbound(frame, releaseAfterWrite)) {
                frame.close();
                writeUnreservedResponse(ctx, Frame.response(releaseAfterWrite,
                        Resp.error(ErrorCode.THROTTLED, "too many in-flight response bytes", maxInflightBytes),
                        null), true, releaseAfterWrite);
                return;
            }
            writeUnreservedResponse(ctx, frame, closeAfterWrite, releaseAfterWrite);
        }

        private boolean reserveOutboundBytes(long frameBytes, Frame request) {
            long bytes = inflightBytes.addAndGet(frameBytes);
            if (bytes <= maxInflightBytes) {
                request.reserveWireBytes(frameBytes);
                return true;
            }
            inflightBytes.addAndGet(-frameBytes);
            return false;
        }

        private void writeResponseObject(ChannelHandlerContext ctx, Frame req, Object response) {
            if (response instanceof Frame frame) {
                writeResponse(ctx, frame, false, req);
                return;
            }
            if (response instanceof OkU64Response okU64) {
                writeOkU64Response(ctx, req, okU64.value());
                return;
            }
            writeResponse(ctx, internalError(req, "handler returned unsupported response"), false, req);
        }

        private void writeOkU64Response(ChannelHandlerContext ctx, Frame req, long value) {
            if (closed.get() || !connectionOpen.get() || !ctx.channel().isActive()) {
                closeFrames(null, req);
                return;
            }
            if (!reserveOutboundBytes(Frame.PREAMBLE_AFTER_LEN + Frame.OK_U64_HEADER_LENGTH, req)) {
                writeUnreservedResponse(ctx, Frame.response(req,
                        Resp.error(ErrorCode.THROTTLED, "too many in-flight response bytes", maxInflightBytes),
                        null), true, req);
                return;
            }
            if (ctx.channel().eventLoop().inEventLoop()) {
                writeOkU64ResponseOnEventLoop(ctx, req, value);
                return;
            }
            OkU64WriteTask task = okU64WriteTask(ctx, req, value);
            try {
                ctx.channel().eventLoop().execute(task);
            } catch (RuntimeException e) {
                task.closeRejected();
                throw e;
            }
        }

        private OkU64WriteTask okU64WriteTask(ChannelHandlerContext ctx, Frame req, long value) {
            OkU64WriteTask task;
            synchronized (okU64WriteTasks) {
                task = okU64WriteTasks.pollFirst();
            }
            if (task == null) {
                task = new OkU64WriteTask();
            }
            task.reset(ctx, req, value);
            return task;
        }

        private void recycleOkU64WriteTask(OkU64WriteTask task) {
            if (MAX_POOLED_OK_U64_WRITE_TASKS <= 0) {
                return;
            }
            synchronized (okU64WriteTasks) {
                if (okU64WriteTasks.size() < MAX_POOLED_OK_U64_WRITE_TASKS) {
                    okU64WriteTasks.addFirst(task);
                }
            }
        }

        private final class OkU64WriteTask implements Runnable {
            private ChannelHandlerContext ctx;
            private Frame req;
            private long value;

            private void reset(ChannelHandlerContext ctx, Frame req, long value) {
                this.ctx = ctx;
                this.req = req;
                this.value = value;
            }

            @Override
            public void run() {
                ChannelHandlerContext localCtx = ctx;
                Frame localReq = req;
                long localValue = value;
                ctx = null;
                req = null;
                value = 0;
                try {
                    writeOkU64ResponseOnEventLoop(localCtx, localReq, localValue);
                } finally {
                    recycleOkU64WriteTask(this);
                }
            }

            private void closeRejected() {
                Frame localReq = req;
                ctx = null;
                req = null;
                value = 0;
                try {
                    closeFrames(null, localReq);
                } finally {
                    recycleOkU64WriteTask(this);
                }
            }
        }

        private void writeOkU64ResponseOnEventLoop(ChannelHandlerContext ctx, Frame req, long value) {
            if (closed.get() || !connectionOpen.get() || !ctx.channel().isActive()) {
                closeFrames(null, req);
                return;
            }
            ByteBuf out;
            try {
                out = NettyFrameCodec.encodeOkU64Response(ctx.alloc(), req, value);
            } catch (IOException | RuntimeException e) {
                closeFrames(null, req);
                ctx.close();
                return;
            }
            try {
                ctx.writeAndFlush(out, ctx.voidPromise());
            } catch (RuntimeException e) {
                out.release();
                closeFrames(null, req);
                throw e;
            }
            // OK_U64 responses own only the encoded ByteBuf now queued in Netty; the request payload
            // was consumed before this point, so no write listener is needed just to release it.
            closeFrames(null, req);
        }

        private void writeUnreservedResponse(ChannelHandlerContext ctx, Frame frame, boolean closeAfterWrite,
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
            write.addListener(responseWriteListener(ctx, frame, releaseAfterWrite, closeAfterWrite));
        }

        private ResponseWriteListener responseWriteListener(ChannelHandlerContext ctx, Frame frame,
                                                            Frame releaseAfterWrite, boolean closeAfterWrite) {
            ResponseWriteListener listener;
            synchronized (responseWriteListeners) {
                listener = responseWriteListeners.pollFirst();
            }
            if (listener == null) {
                listener = new ResponseWriteListener();
            }
            listener.reset(ctx, frame, releaseAfterWrite, closeAfterWrite);
            return listener;
        }

        private void recycleResponseWriteListener(ResponseWriteListener listener) {
            if (MAX_POOLED_RESPONSE_WRITE_LISTENERS <= 0) {
                return;
            }
            synchronized (responseWriteListeners) {
                if (responseWriteListeners.size() < MAX_POOLED_RESPONSE_WRITE_LISTENERS) {
                    responseWriteListeners.addFirst(listener);
                }
            }
        }

        private final class ResponseWriteListener implements ChannelFutureListener {
            private ChannelHandlerContext ctx;
            private Frame frame;
            private Frame releaseAfterWrite;
            private boolean closeAfterWrite;

            private void reset(ChannelHandlerContext ctx, Frame frame, Frame releaseAfterWrite,
                               boolean closeAfterWrite) {
                this.ctx = ctx;
                this.frame = frame;
                this.releaseAfterWrite = releaseAfterWrite;
                this.closeAfterWrite = closeAfterWrite;
            }

            @Override
            public void operationComplete(ChannelFuture future) {
                ChannelHandlerContext localCtx = ctx;
                Frame localFrame = frame;
                Frame localReleaseAfterWrite = releaseAfterWrite;
                boolean localCloseAfterWrite = closeAfterWrite;
                ctx = null;
                frame = null;
                releaseAfterWrite = null;
                closeAfterWrite = false;
                try {
                    try {
                        closeFrames(localFrame, localReleaseAfterWrite);
                    } finally {
                        if (localCloseAfterWrite || !future.isSuccess()) {
                            localCtx.close();
                        }
                    }
                } finally {
                    recycleResponseWriteListener(this);
                }
            }
        }

        private void closeFrames(Frame frame, Frame releaseAfterWrite) {
            if (frame != null) {
                frame.close();
            }
            if (releaseAfterWrite == null) {
                return;
            }
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

    private static Object requireResponse(Frame req, Object response) {
        return response != null ? response : internalError(req, "handler returned null response");
    }

    private static Frame internalError(Frame req, String message) {
        return Frame.response(req, Resp.error(ErrorCode.INTERNAL, message, 0), null);
    }

    /** Convenience for handlers: success response with header bytes and optional payload. */
    public static Frame ok(Frame req, byte[] header, ByteBuffer payload) {
        return Frame.response(req, header, payload);
    }

    /** Convenience for handlers: success response with one u64 field and no tagged fields. */
    public static Frame okU64(Frame req, long value) {
        return Frame.okU64Response(req, value);
    }

    /** Hot-path handler result: success response with one u64 field and no tagged fields. */
    public static OkU64Response okU64Result(long value) {
        return new OkU64Response(value, null);
    }

    /** Hot-path handler result: write the u64 success response after the supplied future completes. */
    public static OkU64Response okU64Result(long value, CompletableFuture<Void> waitFor) {
        return new OkU64Response(value, waitFor);
    }

    /** Convenience for handlers: success response that owns the materialized payload until write close. */
    public static Frame ok(Frame req, byte[] header, ByteBuffer payload, Runnable payloadReleaser) {
        return Frame.response(req, header, payload, payloadReleaser);
    }

    /** Convenience for handlers: success response that borrows a heap payload until write close. */
    public static Frame okBytes(Frame req, byte[] header, byte[] payload, int payloadLen, Runnable payloadReleaser) {
        return Frame.responseBytes(req, header, payload, payloadLen, payloadReleaser);
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
        serverChannel.close().awaitUninterruptibly(1, TimeUnit.SECONDS);
        connections.close().awaitUninterruptibly(1, TimeUnit.SECONDS);
    }
}
