package io.strata.proto;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * One SCP frame (tech design §10.2). Header and payload are exposed as read-only
 * {@link ByteBuffer} slices.
 *
 * <p>Frames constructed from ordinary {@link ByteBuffer}s do not own memory unless a payload
 * releaser is supplied. Netty-decoded frames may own a retained {@link ByteBuf}; transport code
 * must close owned frames after the handler no longer needs the slices.</p>
 */
public final class Frame implements AutoCloseable {
    public static final byte MAGIC = 0x5C;
    public static final byte FRAME_VERSION = 1;
    public static final short FLAG_RESPONSE = 0x0001;
    public static final short FLAG_PAYLOAD_CRC = 0x0002;
    /** Fixed bytes after the leading u32 length field. */
    public static final int PREAMBLE_AFTER_LEN = 26;

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0).asReadOnlyBuffer();
    private static final AtomicIntegerFieldUpdater<Frame> CLOSED =
            AtomicIntegerFieldUpdater.newUpdater(Frame.class, "closed");

    private final short opcode;
    private final short apiVersion;
    private final short flags;
    private final long correlationId;
    private final ByteBuffer header;
    private final ByteBuffer payload;
    private final FilePayload filePayload;
    private final ByteBuf owner;
    private final int ownerHeaderIndex;
    private final int ownerHeaderLen;
    private final int ownerPayloadIndex;
    private final int ownerPayloadLen;
    private final Runnable payloadReleaser;
    private final int payloadCrc;
    @SuppressWarnings("unused") // updated through CLOSED
    private volatile int closed;

    public Frame(short opcode, short apiVersion, short flags, long correlationId,
                 ByteBuffer header, ByteBuffer payload) {
        this(opcode, apiVersion, flags, correlationId, readOnlySlice(header), readOnlySlice(payload),
                null, null, null, 0);
    }

    private Frame(short opcode, short apiVersion, short flags, long correlationId,
                  ByteBuffer header, ByteBuffer payload, FilePayload filePayload, ByteBuf owner,
                  Runnable payloadReleaser, int payloadCrc) {
        this(opcode, apiVersion, flags, correlationId, header, payload, filePayload, owner,
                -1, -1, -1, -1, payloadReleaser, payloadCrc);
    }

    private Frame(short opcode, short apiVersion, short flags, long correlationId,
                  ByteBuffer header, ByteBuffer payload, FilePayload filePayload, ByteBuf owner,
                  int ownerHeaderIndex, int ownerHeaderLen, int ownerPayloadIndex, int ownerPayloadLen,
                  Runnable payloadReleaser, int payloadCrc) {
        this.opcode = opcode;
        this.apiVersion = apiVersion;
        this.flags = flags;
        this.correlationId = correlationId;
        this.header = header;
        this.payload = payload;
        this.filePayload = filePayload;
        this.owner = owner;
        this.ownerHeaderIndex = ownerHeaderIndex;
        this.ownerHeaderLen = ownerHeaderLen;
        this.ownerPayloadIndex = ownerPayloadIndex;
        this.ownerPayloadLen = ownerPayloadLen;
        this.payloadReleaser = payloadReleaser;
        this.payloadCrc = payloadCrc;
    }

    static Frame fromOwnedBuffer(short opcode, short apiVersion, short flags, long correlationId,
                                 ByteBuf owner, int headerIndex, int headerLen, int payloadIndex, int payloadLen,
                                 int payloadCrc) {
        return new Frame(opcode, apiVersion, flags, correlationId, null, null, null, owner,
                headerIndex, headerLen, payloadIndex, payloadLen, null,
                retainedPayloadCrc(flags, payloadLen, payloadCrc));
    }

    static Frame decoded(short opcode, short apiVersion, short flags, long correlationId,
                         ByteBuffer header, ByteBuffer payload, int payloadCrc) {
        ByteBuffer payloadSlice = readOnlySlice(payload);
        return new Frame(opcode, apiVersion, flags, correlationId,
                readOnlySlice(header), payloadSlice, null, null, null,
                retainedPayloadCrc(flags, payloadSlice.remaining(), payloadCrc));
    }

    /**
     * The payload CRC to retain on a decoded frame: the sender's value only when a non-empty payload
     * actually carried {@link #FLAG_PAYLOAD_CRC}, else 0. Centralizing the rule here — rather than at
     * each decode site — keeps the {@link #payloadCrc()} contract (0 on an unflagged or empty frame)
     * enforced at construction for every decode path, including any added later.
     */
    private static int retainedPayloadCrc(short flags, int payloadLen, int payloadCrc) {
        return (flags & FLAG_PAYLOAD_CRC) != 0 && payloadLen > 0 ? payloadCrc : 0;
    }

    public record FilePayload(FileChannel channel, long position, int length, Runnable releaser)
            implements AutoCloseable {
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
            if (releaser == null) {
                throw new IllegalArgumentException("releaser must not be null");
            }
        }

        @Override
        public void close() {
            // Releases the read resource: a channel-cache lease (sealed) or the transient FD (open).
            releaser.run();
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

    public boolean isResponse() {
        return (flags & FLAG_RESPONSE) != 0;
    }

    public ByteBuffer headerSlice() {
        return owner != null ? ownerBuffer(ownerHeaderIndex, ownerHeaderLen).asReadOnlyBuffer()
                : header.asReadOnlyBuffer();
    }

    /**
     * Independent read cursor for trusted internal decoders. Callers must not mutate the bytes; use
     * {@link #headerSlice()} when exposing a buffer outside the transport/storage stack.
     */
    public ByteBuffer headerReadBuffer() {
        return owner != null ? ownerBuffer(ownerHeaderIndex, ownerHeaderLen) : header.duplicate();
    }

    int headerLength() {
        return owner != null ? ownerHeaderLen : header.remaining();
    }

    ByteBuffer headerView() {
        return owner != null ? ownerBuffer(ownerHeaderIndex, ownerHeaderLen) : header;
    }

    public ByteBuffer payloadSlice() {
        if (filePayload != null) {
            throw new IllegalStateException("file payload is not materialized as a ByteBuffer");
        }
        return owner != null ? ownerBuffer(ownerPayloadIndex, ownerPayloadLen).asReadOnlyBuffer()
                : payload.asReadOnlyBuffer();
    }

    /**
     * Independent read cursor for trusted internal storage paths. Callers must not mutate the bytes;
     * use {@link #payloadSlice()} when exposing a buffer outside the transport/storage stack.
     */
    public ByteBuffer payloadReadBuffer() {
        if (filePayload != null) {
            throw new IllegalStateException("file payload is not materialized as a ByteBuffer");
        }
        return owner != null ? ownerBuffer(ownerPayloadIndex, ownerPayloadLen) : payload.duplicate();
    }

    ByteBuffer payloadView() {
        if (filePayload != null) {
            throw new IllegalStateException("file payload is not materialized as a ByteBuffer");
        }
        return owner != null ? ownerBuffer(ownerPayloadIndex, ownerPayloadLen) : payload;
    }

    public int payloadLength() {
        if (filePayload != null) {
            return filePayload.length();
        }
        return owner != null ? ownerPayloadLen : payload.remaining();
    }

    /** CRC32C of the payload as computed by the sender and verified at decode; 0 when no payload CRC. */
    public int payloadCrc() {
        return payloadCrc;
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
        return new Frame(opcode, apiVersion, flags, correlationId,
                copy(headerView()), copy(payloadView()), null, null, null, payloadCrc);
    }

    public boolean ownsBuffer() {
        return owner != null;
    }

    public int ownerRefCnt() {
        return owner == null ? -1 : owner.refCnt();
    }

    @Override
    public void close() {
        // a frame owns at most one inbound buffer or file payload; materialized responses may also
        // carry a payload releaser for buffers borrowed from the storage layer.
        if (CLOSED.compareAndSet(this, 0, 1)) {
            if (owner != null) {
                owner.release();
            }
            if (filePayload != null) {
                filePayload.close();
            }
            if (payloadReleaser != null) {
                payloadReleaser.run();
            }
        }
    }

    private static ByteBuffer readOnlySlice(ByteBuffer buffer) {
        return buffer == null || !buffer.hasRemaining() ? EMPTY : buffer.slice().asReadOnlyBuffer();
    }

    private static ByteBuffer slice(ByteBuffer buffer) {
        return buffer == null || !buffer.hasRemaining() ? EMPTY : buffer.slice();
    }

    private ByteBuffer ownerBuffer(int index, int length) {
        return length == 0 ? EMPTY : owner.nioBuffer(index, length);
    }

    private static ByteBuffer copy(ByteBuffer source) {
        if (source == null || !source.hasRemaining()) {
            // header-only responses (e.g. the APPEND ack) carry an empty payload; reuse the shared
            // empty buffer instead of allocating a zero-length array + wrapper per response
            return EMPTY;
        }
        ByteBuffer duplicate = source.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private static ByteBuffer headerBuffer(byte[] header) {
        return header == null || header.length == 0 ? EMPTY : ByteBuffer.wrap(header);
    }

    public static Frame request(Opcode op, byte[] header, ByteBuffer payload, long correlationId) {
        return new Frame(op.code, (short) 1, (short) 0, correlationId,
                headerBuffer(header), slice(payload), null, null, null, 0);
    }

    public static Frame response(Frame req, byte[] header, ByteBuffer payload) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                headerBuffer(header), slice(payload), null, null, null, 0);
    }

    public static Frame response(Frame req, byte[] header, ByteBuffer payload, Runnable payloadReleaser) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                headerBuffer(header), slice(payload),
                null, null, payloadReleaser, 0);
    }

    public static Frame fileResponse(Frame req, byte[] header, FilePayload filePayload) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                headerBuffer(header), EMPTY, filePayload, null, null, 0);
    }
}
