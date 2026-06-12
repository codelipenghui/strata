package io.strata.proto;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One SCP frame (tech design §10.2). Header and payload are exposed as read-only
 * {@link ByteBuffer} slices.
 *
 * <p>Frames constructed from ordinary {@link ByteBuffer}s do not own memory and
 * {@link #close()} is a no-op. Netty-decoded frames may own a retained {@link ByteBuf};
 * transport code must close those frames after the handler no longer needs the slices.</p>
 */
public final class Frame implements AutoCloseable {
    public static final byte MAGIC = 0x5C;
    public static final byte FRAME_VERSION = 1;
    public static final short FLAG_RESPONSE = 0x0001;
    public static final short FLAG_PAYLOAD_CRC = 0x0002;
    /** Fixed bytes after the leading u32 length field. */
    public static final int PREAMBLE_AFTER_LEN = 26;

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0).asReadOnlyBuffer();

    private final short opcode;
    private final short apiVersion;
    private final short flags;
    private final long correlationId;
    private final ByteBuffer header;
    private final ByteBuffer payload;
    private final FilePayload filePayload;
    private final ByteBuf owner;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public Frame(short opcode, short apiVersion, short flags, long correlationId,
                 ByteBuffer header, ByteBuffer payload) {
        this(opcode, apiVersion, flags, correlationId, readOnlySlice(header), readOnlySlice(payload), null, null);
    }

    private Frame(short opcode, short apiVersion, short flags, long correlationId,
                  ByteBuffer header, ByteBuffer payload, FilePayload filePayload, ByteBuf owner) {
        this.opcode = opcode;
        this.apiVersion = apiVersion;
        this.flags = flags;
        this.correlationId = correlationId;
        this.header = header;
        this.payload = payload;
        this.filePayload = filePayload;
        this.owner = owner;
    }

    static Frame fromOwnedBuffer(short opcode, short apiVersion, short flags, long correlationId,
                                 ByteBuf owner, int headerIndex, int headerLen, int payloadIndex, int payloadLen) {
        ByteBuffer header = owner.nioBuffer(headerIndex, headerLen).asReadOnlyBuffer();
        ByteBuffer payload = owner.nioBuffer(payloadIndex, payloadLen).asReadOnlyBuffer();
        return new Frame(opcode, apiVersion, flags, correlationId, header, payload, null, owner);
    }

    public record FilePayload(FileChannel channel, long position, int length) implements AutoCloseable {
        public FilePayload {
            if (channel == null) {
                throw new IllegalArgumentException("channel must not be null");
            }
            if (position < 0) {
                throw new IllegalArgumentException("position must be non-negative: " + position);
            }
            if (length < 0) {
                throw new IllegalArgumentException("length must be non-negative: " + length);
            }
        }

        @Override
        public void close() {
            try {
                channel.close();
            } catch (java.io.IOException ignored) {
                // Closing a response frame is best-effort cleanup.
            }
        }
    }

    public short opcode() {
        return opcode;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public short flags() {
        return flags;
    }

    public long correlationId() {
        return correlationId;
    }

    public ByteBuffer header() {
        return headerSlice();
    }

    public ByteBuffer payload() {
        return payloadSlice();
    }

    public boolean isResponse() {
        return (flags & FLAG_RESPONSE) != 0;
    }

    public ByteBuffer headerSlice() {
        return header.duplicate();
    }

    public ByteBuffer payloadSlice() {
        if (filePayload != null) {
            throw new IllegalStateException("file payload is not materialized as a ByteBuffer");
        }
        return payload.duplicate();
    }

    public int payloadLength() {
        return filePayload != null ? filePayload.length() : payload.remaining();
    }

    public boolean hasFilePayload() {
        return filePayload != null;
    }

    public FilePayload filePayload() {
        if (filePayload == null) {
            throw new IllegalStateException("frame has no file payload");
        }
        return filePayload;
    }

    Frame copyToHeap() {
        if (filePayload != null) {
            throw new IllegalStateException("file payload cannot be copied to heap");
        }
        return new Frame(opcode, apiVersion, flags, correlationId, copy(header), copy(payload));
    }

    boolean ownsBuffer() {
        return owner != null;
    }

    int ownerRefCnt() {
        return owner == null ? -1 : owner.refCnt();
    }

    @Override
    public void close() {
        if (owner != null && closed.compareAndSet(false, true)) {
            owner.release();
        }
        if (filePayload != null && closed.compareAndSet(false, true)) {
            filePayload.close();
        }
    }

    private static ByteBuffer readOnlySlice(ByteBuffer buffer) {
        return buffer == null ? EMPTY.duplicate() : buffer.slice().asReadOnlyBuffer();
    }

    private static ByteBuffer copy(ByteBuffer source) {
        ByteBuffer duplicate = source.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private static ByteBuffer headerBuffer(byte[] header) {
        return header != null ? ByteBuffer.wrap(header) : EMPTY.duplicate();
    }

    public static Frame request(Opcode op, byte[] header, ByteBuffer payload, long correlationId) {
        return new Frame(op.code, (short) 1, (short) 0, correlationId,
                headerBuffer(header), payload != null ? payload : EMPTY.duplicate());
    }

    public static Frame response(Frame req, byte[] header, ByteBuffer payload) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                headerBuffer(header), payload != null ? payload : EMPTY.duplicate());
    }

    public static Frame fileResponse(Frame req, byte[] header, FilePayload filePayload) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                headerBuffer(header), EMPTY.duplicate(), filePayload, null);
    }
}
