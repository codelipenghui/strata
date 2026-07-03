package io.strata.proto;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

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
    static final int OK_U64_HEADER_LENGTH = Short.BYTES + Long.BYTES + 1;
    static final int OK_TWO_U64_HEADER_LENGTH = Short.BYTES + Long.BYTES + Long.BYTES + 1;
    private static final byte HEADER_KIND_BUFFER = 0;
    private static final byte HEADER_KIND_OK_U64 = 1;
    private static final AtomicIntegerFieldUpdater<Frame> CLOSED =
            AtomicIntegerFieldUpdater.newUpdater(Frame.class, "closed");
    private static final AtomicLongFieldUpdater<Frame> RESERVED_WIRE_BYTES =
            AtomicLongFieldUpdater.newUpdater(Frame.class, "reservedWireBytes");

    private final short opcode;
    private final short apiVersion;
    private final short flags;
    private final long correlationId;
    private final ByteBuffer header;
    private final byte[] headerBytes;
    private final byte headerKind;
    private final long headerU64;
    private final ByteBuffer payload;
    private final byte[] payloadBytes;
    private final int payloadBytesOffset;
    private final int payloadBytesLen;
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
    @SuppressWarnings("unused") // updated through RESERVED_WIRE_BYTES by ScpServer
    private volatile long reservedWireBytes;

    public Frame(short opcode, short apiVersion, short flags, long correlationId,
                 ByteBuffer header, ByteBuffer payload) {
        this(opcode, apiVersion, flags, correlationId, readOnlySlice(header), null, readOnlySlice(payload),
                null, null, null, 0);
    }

    private Frame(short opcode, short apiVersion, short flags, long correlationId,
                  ByteBuffer header, byte[] headerBytes, ByteBuffer payload, FilePayload filePayload, ByteBuf owner,
                  Runnable payloadReleaser, int payloadCrc) {
        this(opcode, apiVersion, flags, correlationId, header, headerBytes, payload, filePayload, owner,
                -1, -1, -1, -1, payloadReleaser, payloadCrc);
    }

    private Frame(short opcode, short apiVersion, short flags, long correlationId,
                  ByteBuffer header, byte[] headerBytes, ByteBuffer payload, FilePayload filePayload, ByteBuf owner,
                  int ownerHeaderIndex, int ownerHeaderLen, int ownerPayloadIndex, int ownerPayloadLen,
                  Runnable payloadReleaser, int payloadCrc) {
        this.opcode = opcode;
        this.apiVersion = apiVersion;
        this.flags = flags;
        this.correlationId = correlationId;
        this.header = header;
        this.headerBytes = headerBytes;
        this.headerKind = HEADER_KIND_BUFFER;
        this.headerU64 = 0;
        this.payload = payload;
        this.payloadBytes = null;
        this.payloadBytesOffset = 0;
        this.payloadBytesLen = 0;
        this.filePayload = filePayload;
        this.owner = owner;
        this.ownerHeaderIndex = ownerHeaderIndex;
        this.ownerHeaderLen = ownerHeaderLen;
        this.ownerPayloadIndex = ownerPayloadIndex;
        this.ownerPayloadLen = ownerPayloadLen;
        this.payloadReleaser = payloadReleaser;
        this.payloadCrc = payloadCrc;
    }

    private Frame(short opcode, short apiVersion, short flags, long correlationId, long okU64Header,
                  ByteBuffer payload, FilePayload filePayload, Runnable payloadReleaser) {
        this.opcode = opcode;
        this.apiVersion = apiVersion;
        this.flags = flags;
        this.correlationId = correlationId;
        this.header = EMPTY;
        this.headerBytes = null;
        this.headerKind = HEADER_KIND_OK_U64;
        this.headerU64 = okU64Header;
        this.payload = payload;
        this.payloadBytes = null;
        this.payloadBytesOffset = 0;
        this.payloadBytesLen = 0;
        this.filePayload = filePayload;
        this.owner = null;
        this.ownerHeaderIndex = -1;
        this.ownerHeaderLen = -1;
        this.ownerPayloadIndex = -1;
        this.ownerPayloadLen = -1;
        this.payloadReleaser = payloadReleaser;
        this.payloadCrc = 0;
    }

    private Frame(short opcode, short apiVersion, short flags, long correlationId, byte[] headerBytes,
                  byte[] payloadBytes, int payloadBytesOffset, int payloadBytesLen, Runnable payloadReleaser) {
        this.opcode = opcode;
        this.apiVersion = apiVersion;
        this.flags = flags;
        this.correlationId = correlationId;
        this.header = EMPTY;
        this.headerBytes = headerBytes;
        this.headerKind = HEADER_KIND_BUFFER;
        this.headerU64 = 0;
        this.payload = EMPTY;
        this.payloadBytes = payloadBytes;
        this.payloadBytesOffset = payloadBytesOffset;
        this.payloadBytesLen = payloadBytesLen;
        this.filePayload = null;
        this.owner = null;
        this.ownerHeaderIndex = -1;
        this.ownerHeaderLen = -1;
        this.ownerPayloadIndex = -1;
        this.ownerPayloadLen = -1;
        this.payloadReleaser = payloadReleaser;
        this.payloadCrc = 0;
    }

    static Frame fromOwnedBuffer(short opcode, short apiVersion, short flags, long correlationId,
                                 ByteBuf owner, int headerIndex, int headerLen, int payloadIndex, int payloadLen,
                                 int payloadCrc) {
        return new Frame(opcode, apiVersion, flags, correlationId, null, null, null, null, owner,
                headerIndex, headerLen, payloadIndex, payloadLen, null,
                retainedPayloadCrc(flags, payloadLen, payloadCrc));
    }

    static Frame decoded(short opcode, short apiVersion, short flags, long correlationId,
                         ByteBuffer header, ByteBuffer payload, int payloadCrc) {
        ByteBuffer payloadSlice = readOnlySlice(payload);
        return new Frame(opcode, apiVersion, flags, correlationId,
                readOnlySlice(header), null, payloadSlice, null, null, null,
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
        return hasOkU64Header() ? okU64HeaderBuffer().asReadOnlyBuffer()
                : owner != null ? ownerBuffer(ownerHeaderIndex, ownerHeaderLen).asReadOnlyBuffer()
                : headerBytes != null ? ByteBuffer.wrap(headerBytes).asReadOnlyBuffer()
                : header.asReadOnlyBuffer();
    }

    /**
     * Independent read cursor for trusted internal decoders. Callers must not mutate the bytes; use
     * {@link #headerSlice()} when exposing a buffer outside the transport/storage stack.
     */
    public ByteBuffer headerReadBuffer() {
        return hasOkU64Header() ? okU64HeaderBuffer()
                : owner != null ? ownerBuffer(ownerHeaderIndex, ownerHeaderLen)
                : headerBytes != null ? ByteBuffer.wrap(headerBytes)
                : header.duplicate();
    }

    int headerLength() {
        return hasOkU64Header() ? OK_U64_HEADER_LENGTH
                : owner != null ? ownerHeaderLen : headerBytes != null ? headerBytes.length : header.remaining();
    }

    boolean hasOwnedHeader() {
        return owner != null;
    }

    boolean hasHeaderBytes() {
        return headerBytes != null;
    }

    boolean hasOkU64Header() {
        return headerKind == HEADER_KIND_OK_U64;
    }

    long okU64HeaderValue() {
        return headerU64;
    }

    byte[] headerBytes() {
        return headerBytes;
    }

    boolean hasPayloadBytes() {
        return payloadBytes != null;
    }

    byte[] payloadBytes() {
        return payloadBytes;
    }

    int payloadBytesOffset() {
        return payloadBytesOffset;
    }

    int payloadBytesLength() {
        return payloadBytesLen;
    }

    byte ownedHeaderByte(int offset) {
        return owner.getByte(ownerHeaderIndex + offset);
    }

    int ownedHeaderInt(int offset) {
        return owner.getInt(ownerHeaderIndex + offset);
    }

    long ownedHeaderLong(int offset) {
        return owner.getLong(ownerHeaderIndex + offset);
    }

    ByteBuffer headerView() {
        return hasOkU64Header() ? okU64HeaderBuffer()
                : owner != null ? ownerBuffer(ownerHeaderIndex, ownerHeaderLen)
                : headerBytes != null ? ByteBuffer.wrap(headerBytes)
                : header;
    }

    public ByteBuffer payloadSlice() {
        if (filePayload != null) {
            throw new IllegalStateException("file payload is not materialized as a ByteBuffer");
        }
        return owner != null ? ownerBuffer(ownerPayloadIndex, ownerPayloadLen).asReadOnlyBuffer()
                : payloadBytes != null ? ByteBuffer.wrap(payloadBytes, payloadBytesOffset, payloadBytesLen).asReadOnlyBuffer()
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
        return owner != null ? ownerBuffer(ownerPayloadIndex, ownerPayloadLen)
                : payloadBytes != null ? ByteBuffer.wrap(payloadBytes, payloadBytesOffset, payloadBytesLen)
                : payload.duplicate();
    }

    ByteBuffer payloadView() {
        if (filePayload != null) {
            throw new IllegalStateException("file payload is not materialized as a ByteBuffer");
        }
        return owner != null ? ownerBuffer(ownerPayloadIndex, ownerPayloadLen)
                : payloadBytes != null ? ByteBuffer.wrap(payloadBytes, payloadBytesOffset, payloadBytesLen)
                : payload;
    }

    public int payloadLength() {
        if (filePayload != null) {
            return filePayload.length();
        }
        return owner != null ? ownerPayloadLen : payloadBytes != null ? payloadBytesLen : payload.remaining();
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
        if (owner != null) {
            return new Frame(opcode, apiVersion, flags, correlationId,
                    copyOwnerRange(ownerHeaderIndex, ownerHeaderLen), null,
                    copyOwnerRange(ownerPayloadIndex, ownerPayloadLen), null, null, null, payloadCrc);
        }
        return new Frame(opcode, apiVersion, flags, correlationId,
                copy(headerView()), null, copy(payloadView()), null, null, null, payloadCrc);
    }

    public boolean ownsBuffer() {
        return owner != null;
    }

    public int ownerRefCnt() {
        return owner == null ? -1 : owner.refCnt();
    }

    void reserveWireBytes(long bytes) {
        RESERVED_WIRE_BYTES.addAndGet(this, bytes);
    }

    long drainReservedWireBytes() {
        return RESERVED_WIRE_BYTES.getAndSet(this, 0);
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

    private ByteBuffer copyOwnerRange(int index, int length) {
        if (length == 0) {
            return EMPTY;
        }
        byte[] bytes = new byte[length];
        owner.getBytes(index, bytes);
        return ByteBuffer.wrap(bytes);
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

    private static byte[] headerBytes(byte[] header) {
        return header == null || header.length == 0 ? null : header;
    }

    public static Frame request(Opcode op, byte[] header, ByteBuffer payload, long correlationId) {
        return new Frame(op.code, (short) 1, (short) 0, correlationId,
                EMPTY, headerBytes(header), slice(payload), null, null, null, 0);
    }

    public static Frame response(Frame req, byte[] header, ByteBuffer payload) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                EMPTY, headerBytes(header), slice(payload), null, null, null, 0);
    }

    public static Frame okU64Response(Frame req, long value) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                value, EMPTY, null, null);
    }

    public static Frame response(Frame req, byte[] header, ByteBuffer payload, Runnable payloadReleaser) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                EMPTY, headerBytes(header), slice(payload),
                null, null, payloadReleaser, 0);
    }

    public static Frame responseBytes(Frame req, byte[] header, byte[] payload, int payloadLen,
                                      Runnable payloadReleaser) {
        if (payloadLen < 0) {
            throw new IllegalArgumentException("negative payload length: " + payloadLen);
        }
        if (payloadLen == 0) {
            return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                    EMPTY, headerBytes(header), EMPTY, null, null, payloadReleaser, 0);
        }
        if (payload == null || payloadLen > payload.length) {
            throw new IllegalArgumentException("invalid payload length " + payloadLen);
        }
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                headerBytes(header), payload, 0, payloadLen, payloadReleaser);
    }

    public static Frame fileResponse(Frame req, byte[] header, FilePayload filePayload) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                EMPTY, headerBytes(header), EMPTY, filePayload, null, null, 0);
    }

    private ByteBuffer okU64HeaderBuffer() {
        ByteBuffer out = ByteBuffer.allocate(headerLength());
        out.putShort((short) 0).putLong(headerU64).put((byte) 0);
        return out.flip();
    }
}
