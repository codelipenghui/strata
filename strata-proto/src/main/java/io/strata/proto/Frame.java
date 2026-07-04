package io.strata.proto;

import io.netty.buffer.ByteBuf;
import io.strata.common.EnvConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayDeque;
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
    private static final int MAX_POOLED_OWNED_REQUEST_FRAMES =
            EnvConfig.intEnv("STRATA_SCP_OWNED_REQUEST_FRAME_POOL_SIZE",
                    EnvConfig.intEnv("STRATA_SCP_OWNED_APPEND_FRAME_POOL_SIZE", 64));
    private static final ThreadLocal<ArrayDeque<Frame>> OWNED_REQUEST_FRAMES =
            ThreadLocal.withInitial(ArrayDeque::new);

    private short opcode;
    private short apiVersion;
    private short flags;
    private long correlationId;
    private ByteBuffer header = EMPTY;
    private byte[] headerBytes;
    private byte headerKind = HEADER_KIND_BUFFER;
    private long headerU64;
    private ByteBuffer payload = EMPTY;
    private byte[] payloadBytes;
    private int payloadBytesOffset;
    private int payloadBytesLen;
    private FilePayload filePayload;
    private ByteBuf owner;
    private int ownerHeaderIndex = -1;
    private int ownerHeaderLen = -1;
    private int ownerPayloadIndex = -1;
    private int ownerPayloadLen = -1;
    private Runnable payloadReleaser;
    private int payloadCrc;
    private boolean recyclableOwned;
    private int closedOwnerRefCnt = -1;
    @SuppressWarnings("unused") // updated through CLOSED
    private volatile int closed;
    @SuppressWarnings("unused") // updated through RESERVED_WIRE_BYTES by ScpServer
    private volatile long reservedWireBytes;

    public Frame(short opcode, short apiVersion, short flags, long correlationId,
                 ByteBuffer header, ByteBuffer payload) {
        this(opcode, apiVersion, flags, correlationId, readOnlySlice(header), null, readOnlySlice(payload),
                null, null, null, 0);
    }

    private Frame() {
        closed = 1;
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
        return fromOwnedBuffer(opcode, apiVersion, flags, correlationId, owner, headerIndex, headerLen,
                payloadIndex, payloadLen, payloadCrc, null);
    }

    static Frame fromOwnedBuffer(short opcode, short apiVersion, short flags, long correlationId,
                                 ByteBuf owner, int headerIndex, int headerLen, int payloadIndex, int payloadLen,
                                 int payloadCrc, ByteBuffer internalPayloadReadBuffer) {
        boolean recyclable = shouldRecycleOwnedFrame(opcode, flags);
        Frame frame = recyclable ? acquireOwnedRequestFrame() : new Frame();
        return frame.initOwnedBuffer(opcode, apiVersion, flags, correlationId, owner, headerIndex, headerLen,
                payloadIndex, payloadLen, payloadCrc, internalPayloadReadBuffer, recyclable);
    }

    private Frame initOwnedBuffer(short opcode, short apiVersion, short flags, long correlationId,
                                  ByteBuf owner, int headerIndex, int headerLen, int payloadIndex, int payloadLen,
                                  int payloadCrc, ByteBuffer internalPayloadReadBuffer, boolean recyclableOwned) {
        this.opcode = opcode;
        this.apiVersion = apiVersion;
        this.flags = flags;
        this.correlationId = correlationId;
        this.header = null;
        this.headerBytes = null;
        this.headerKind = HEADER_KIND_BUFFER;
        this.headerU64 = 0;
        this.payload = internalPayloadReadBuffer;
        this.payloadBytes = null;
        this.payloadBytesOffset = 0;
        this.payloadBytesLen = 0;
        this.filePayload = null;
        this.owner = owner;
        this.ownerHeaderIndex = headerIndex;
        this.ownerHeaderLen = headerLen;
        this.ownerPayloadIndex = payloadIndex;
        this.ownerPayloadLen = payloadLen;
        this.payloadReleaser = null;
        this.payloadCrc = retainedPayloadCrc(flags, payloadLen, payloadCrc);
        this.recyclableOwned = recyclableOwned;
        this.closedOwnerRefCnt = -1;
        this.reservedWireBytes = 0;
        this.closed = 0;
        return this;
    }

    private static boolean shouldRecycleOwnedFrame(short opcode, short flags) {
        return MAX_POOLED_OWNED_REQUEST_FRAMES > 0
                && (opcode == Opcode.APPEND.code
                        || opcode == Opcode.READ.code
                        || opcode == Opcode.READ_RECOVERY.code)
                && (flags & FLAG_RESPONSE) == 0;
    }

    private static Frame acquireOwnedRequestFrame() {
        Frame frame = OWNED_REQUEST_FRAMES.get().pollFirst();
        if (frame != null) {
            return frame;
        }
        return new Frame();
    }

    private static void recycleOwnedRequestFrame(Frame frame) {
        if (MAX_POOLED_OWNED_REQUEST_FRAMES <= 0) {
            return;
        }
        ArrayDeque<Frame> frames = OWNED_REQUEST_FRAMES.get();
        if (frames.size() < MAX_POOLED_OWNED_REQUEST_FRAMES) {
            frames.addFirst(frame);
        }
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

    /**
     * Single-use internal payload view for synchronous hot paths. Unlike {@link #payloadReadBuffer()},
     * this may return a transport-cached cursor, so callers must not retain it or call it concurrently.
     */
    public ByteBuffer payloadInternalReadBuffer() {
        if (filePayload != null) {
            throw new IllegalStateException("file payload is not materialized as a ByteBuffer");
        }
        return owner != null && payload != null ? payload : payloadReadBuffer();
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

    public void writePayloadTo(FileChannel channel, long position) throws IOException {
        if (filePayload != null) {
            throw new IllegalStateException("file payload is not materialized as a ByteBuffer");
        }
        int length = payloadLength();
        if (length == 0) {
            return;
        }
        if (owner != null) {
            writeOwnedPayloadTo(channel, position, length);
            return;
        }
        writeFully(channel, payloadReadBuffer(), position);
    }

    public void copyPayloadTo(int payloadOffset, byte[] dst, int dstOffset, int length) {
        if (filePayload != null) {
            throw new IllegalStateException("file payload is not materialized as a ByteBuffer");
        }
        checkRange(payloadOffset, length, payloadLength());
        if (dstOffset < 0 || length < 0 || dstOffset > dst.length - length) {
            throw new IndexOutOfBoundsException(
                    "dstOffset=" + dstOffset + " length=" + length + " capacity=" + dst.length);
        }
        if (length == 0) {
            return;
        }
        if (owner != null) {
            owner.getBytes(ownerPayloadIndex + payloadOffset, dst, dstOffset, length);
            return;
        }
        ByteBuffer source = payloadReadBuffer();
        source.position(source.position() + payloadOffset);
        source.get(dst, dstOffset, length);
    }

    public void copyPayloadTo(int payloadOffset, ByteBuffer dst, int length) {
        if (filePayload != null) {
            throw new IllegalStateException("file payload is not materialized as a ByteBuffer");
        }
        checkRange(payloadOffset, length, payloadLength());
        if (length < 0 || length > dst.remaining()) {
            throw new IndexOutOfBoundsException(
                    "length=" + length + " remaining=" + dst.remaining());
        }
        if (length == 0) {
            return;
        }
        int originalLimit = dst.limit();
        try {
            dst.limit(dst.position() + length);
            if (owner != null) {
                owner.getBytes(ownerPayloadIndex + payloadOffset, dst);
                return;
            }
            ByteBuffer source = payloadReadBuffer();
            int sourceStart = source.position() + payloadOffset;
            source.position(sourceStart).limit(sourceStart + length);
            dst.put(source);
        } finally {
            dst.limit(originalLimit);
        }
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
        return owner == null ? closedOwnerRefCnt : owner.refCnt();
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
            ByteBuf localOwner = owner;
            FilePayload localFilePayload = filePayload;
            Runnable localPayloadReleaser = payloadReleaser;
            boolean recycle = recyclableOwned;
            if (localOwner != null) {
                localOwner.release();
                closedOwnerRefCnt = localOwner.refCnt();
            }
            if (localFilePayload != null) {
                localFilePayload.close();
            }
            if (localPayloadReleaser != null) {
                localPayloadReleaser.run();
            }
            if (recycle) {
                clearRecyclableState();
                recycleOwnedRequestFrame(this);
            }
        }
    }

    private void clearRecyclableState() {
        opcode = 0;
        apiVersion = 0;
        flags = 0;
        correlationId = 0;
        header = EMPTY;
        headerBytes = null;
        headerKind = HEADER_KIND_BUFFER;
        headerU64 = 0;
        payload = EMPTY;
        payloadBytes = null;
        payloadBytesOffset = 0;
        payloadBytesLen = 0;
        filePayload = null;
        owner = null;
        ownerHeaderIndex = -1;
        ownerHeaderLen = -1;
        ownerPayloadIndex = -1;
        ownerPayloadLen = -1;
        payloadReleaser = null;
        payloadCrc = 0;
        recyclableOwned = false;
        reservedWireBytes = 0;
    }

    private static ByteBuffer readOnlySlice(ByteBuffer buffer) {
        return buffer == null || !buffer.hasRemaining() ? EMPTY : buffer.slice().asReadOnlyBuffer();
    }

    private static ByteBuffer slice(ByteBuffer buffer) {
        return buffer == null || !buffer.hasRemaining() ? EMPTY : buffer.slice();
    }

    private void writeOwnedPayloadTo(FileChannel channel, long position, int length) throws IOException {
        int written = 0;
        while (written < length) {
            channel.position(position + written);
            int n = owner.getBytes(ownerPayloadIndex + written, (GatheringByteChannel) channel, length - written);
            if (n <= 0) {
                throw new IOException("failed to write payload bytes");
            }
            written += n;
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer source, long position) throws IOException {
        long writePosition = position;
        while (source.hasRemaining()) {
            int n = channel.write(source, writePosition);
            if (n <= 0) {
                throw new IOException("failed to write payload bytes");
            }
            writePosition += n;
        }
    }

    private static void checkRange(int offset, int length, int capacity) {
        if (offset < 0 || length < 0 || offset > capacity - length) {
            throw new IndexOutOfBoundsException(
                    "offset=" + offset + " length=" + length + " capacity=" + capacity);
        }
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
