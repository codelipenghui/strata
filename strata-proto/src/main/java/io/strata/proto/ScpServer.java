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

    /** Handles one request frame; returns the response frame. Throw ScpException for protocol errors. */
    public interface Handler {
        Frame handle(Frame request) throws Exception;
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
        try (s) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 1 << 16));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 1 << 16));

            Frame hello = FrameIO.read(in);
            if (hello == null) return;
            if (hello.opcode() != Opcode.HELLO.code) {
                FrameIO.write(out, Frame.response(hello,
                        Resp.error(ErrorCode.UNKNOWN_OPCODE, "first frame must be HELLO", 0), null));
                return;
            }
            Messages.Hello.decode(hello.headerSlice()); // validates frame-version overlap
            FrameIO.write(out, Frame.response(hello,
                    new Messages.HelloResp(0, nodeId, incMsb, incLsb, FrameIO.MAX_FRAME_BYTES, 1L << 30).encode(), null));

            while (!closed.get()) {
                Frame req = FrameIO.read(in);
                if (req == null) return;
                Frame resp;
                try {
                    resp = handler.handle(req);
                } catch (ScpException e) {
                    resp = Frame.response(req, Resp.error(e.code(), e.getMessage(), e.detail()), null);
                } catch (Exception e) {
                    log.warn("handler error for opcode 0x{}", Integer.toHexString(req.opcode()), e);
                    resp = Frame.response(req, Resp.error(ErrorCode.INTERNAL, String.valueOf(e), 0), null);
                }
                FrameIO.write(out, resp);
            }
        } catch (IOException e) {
            // connection dropped — normal in failure tests
        } finally {
            connections.remove(s);
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
