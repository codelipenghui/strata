package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.common.Varint;

import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * SCP message structs (tech design §10.3/§10.4 + v0 additions). Every header ends with a
 * tagged-field block. Request decoders take the header buffer; response decoders are called
 * AFTER Resp.check(buf) consumed the error header.
 */
public final class Messages {
    private Messages() {}
    private static final int TAG_OWNER_EPOCH = 0;

    /** Bounded list-count reader (see {@link Varint#readCount}). */
    static int count(ByteBuffer b) {
        return Varint.readCount(b, "list");
    }

    private static void requireNonNegativeOwnerEpoch(long ownerEpoch) {
        if (ownerEpoch < 0) {
            throw new IllegalArgumentException("ownerEpoch must be non-negative: " + ownerEpoch);
        }
    }

    private static byte[] u64Field(long value) {
        BufWriter w = new BufWriter(8);
        w.u64(value);
        return w.toBytes();
    }

    private static long readU64Tag(TaggedFields tags, int tag, String name) {
        byte[] raw = tags.get(tag);
        if (raw == null) {
            return 0;
        }
        if (raw.length != Long.BYTES) {
            throw new IllegalArgumentException(name + " tag must be 8 bytes, got " + raw.length);
        }
        return ByteBuffer.wrap(raw).getLong();
    }

    private static void writeOwnerEpochTags(BufWriter w, long ownerEpoch) {
        requireNonNegativeOwnerEpoch(ownerEpoch);
        if (ownerEpoch == 0) {
            w.noTags();
        } else {
            TaggedFields.of(Map.of(TAG_OWNER_EPOCH, u64Field(ownerEpoch))).writeTo(w);
        }
    }

    /* ---------- shared sub-structs ---------- */

    public record Replica(int nodeId, String endpoint) {
        static void write(BufWriter w, Replica r) {
            w.u32(r.nodeId).string(r.endpoint);
        }

        static Replica read(ByteBuffer b) {
            return new Replica(b.getInt(), Varint.readString(b));
        }
    }

    static void writeReplicas(BufWriter w, List<Replica> rs) {
        w.varint(rs.size());
        for (Replica r : rs) Replica.write(w, r);
    }

    static List<Replica> readReplicas(ByteBuffer b) {
        int n = count(b);
        List<Replica> rs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) rs.add(Replica.read(b));
        return rs;
    }

    private static byte[] okWithU64(long value) {
        byte[] out = new byte[Short.BYTES + Long.BYTES + 1];
        putU64(out, Short.BYTES, value);
        out[out.length - 1] = 0;
        return out;
    }

    private static byte[] okWithTwoU64(long first, long second) {
        byte[] out = new byte[Short.BYTES + Long.BYTES + Long.BYTES + 1];
        putU64(out, Short.BYTES, first);
        putU64(out, Short.BYTES + Long.BYTES, second);
        out[out.length - 1] = 0;
        return out;
    }

    private static void putU64(byte[] out, int offset, long value) {
        for (int i = 7; i >= 0; i--) {
            out[offset++] = (byte) (value >>> (8 * i));
        }
    }

    /* ---------- HELLO ---------- */

    public record Hello(byte clientKind, long featureBits, String clientId) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.u16(Frame.FRAME_VERSION).u16(Frame.FRAME_VERSION);
            w.u8(clientKind).u64(featureBits).string(clientId);
            w.noTags();
            return w.toBytes();
        }

        public static Hello decode(ByteBuffer b) {
            int fvMin = b.getShort() & 0xFFFF;
            int fvMax = b.getShort() & 0xFFFF;
            if (fvMin > Frame.FRAME_VERSION || fvMax < Frame.FRAME_VERSION) {
                throw new IllegalArgumentException("no common frame version: [" + fvMin + "," + fvMax + "]");
            }
            byte kind = b.get();
            long features = b.getLong();
            String clientId = Varint.readString(b);
            TaggedFields.readFrom(b);
            return new Hello(kind, features, clientId);
        }
    }

    public record HelloResp(long featureBits, int nodeId, long incMsb, long incLsb,
                            int maxFrameBytes, long maxInflightBytes) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u16(Frame.FRAME_VERSION);
            w.u64(featureBits).u32(nodeId).u64(incMsb).u64(incLsb).u32(maxFrameBytes).u64(maxInflightBytes);
            Opcode[] ops = Opcode.values();
            w.varint(ops.length);
            for (Opcode op : ops) w.u16(op.code).u16(1);
            w.noTags();
            return w.toBytes();
        }

        public static HelloResp decode(ByteBuffer b) {
            b.getShort(); // chosen frame version
            long features = b.getLong();
            int nodeId = b.getInt();
            long msb = b.getLong(), lsb = b.getLong();
            int maxFrame = b.getInt();
            long maxInflight = b.getLong();
            int n = count(b);
            for (int i = 0; i < n; i++) { b.getShort(); b.getShort(); }
            TaggedFields.readFrom(b);
            return new HelloResp(features, nodeId, msb, lsb, maxFrame, maxInflight);
        }
    }

    /* ---------- data plane ---------- */

    public record OpenChunk(ChunkId chunkId, int writeEpoch, boolean fsyncOnAck,
                            long expectedMaxBytes, long createdAtMs, StrataNamespace namespace) {
        private static final ThreadLocal<OwnedOpenChunkDecoder> OWNED_DECODER =
                ThreadLocal.withInitial(OwnedOpenChunkDecoder::new);

        public OpenChunk {
            namespace = Objects.requireNonNull(namespace, "namespace");
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).i32(writeEpoch).u8(fsyncOnAck ? 1 : 0)
                    .u64(expectedMaxBytes).u64(createdAtMs).namespace(namespace).noTags();
            return w.toBytes();
        }

        public static OpenChunk decode(ByteBuffer b) {
            ChunkId chunkId = ChunkId.readFrom(b);
            int writeEpoch = b.getInt();
            boolean fsyncOnAck = Varint.readBoolean(b);
            long expectedMaxBytes = b.getLong();
            long createdAtMs = b.getLong();
            StrataNamespace namespace = StrataNamespace.readFrom(b);
            TaggedFields.readFrom(b);
            return new OpenChunk(chunkId, writeEpoch, fsyncOnAck, expectedMaxBytes, createdAtMs, namespace);
        }

        public static OpenChunk decode(Frame frame) {
            if (!frame.hasOwnedHeader()) {
                return decode(frame.headerReadBuffer());
            }
            return OWNED_DECODER.get().decode(frame);
        }

        private static void validateTaggedFieldTag(long tag) {
            if (tag < 0 || tag > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("bad tagged-field tag: " + tag);
            }
        }

        private static void requireEndOfTaggedFields(int remaining) {
            if (remaining != 0) {
                throw new IllegalArgumentException("trailing bytes after tagged fields: " + remaining);
            }
        }

        private static final class OwnedOpenChunkDecoder implements StrataNamespace.AsciiBytes {
            private Frame frame;
            private int pos;
            private int namespaceOffset;

            OpenChunk decode(Frame frame) {
                this.frame = frame;
                pos = 0;
                namespaceOffset = 0;
                try {
                    long fileId = readLong();
                    int chunkIndex = readInt();
                    int writeEpoch = readInt();
                    boolean fsyncOnAck = readBoolean();
                    long expectedMaxBytes = readLong();
                    long createdAtMs = readLong();
                    StrataNamespace namespace = readNamespace();
                    readTaggedFields();
                    return new OpenChunk(new ChunkId(FileId.of(fileId), chunkIndex), writeEpoch,
                            fsyncOnAck, expectedMaxBytes, createdAtMs, namespace);
                } finally {
                    this.frame = null;
                }
            }

            @Override
            public byte byteAt(int index) {
                return frame.ownedHeaderByte(namespaceOffset + index);
            }

            private long readLong() {
                require(Long.BYTES);
                long value = frame.ownedHeaderLong(pos);
                pos += Long.BYTES;
                return value;
            }

            private int readInt() {
                require(Integer.BYTES);
                int value = frame.ownedHeaderInt(pos);
                pos += Integer.BYTES;
                return value;
            }

            private boolean readBoolean() {
                byte value = readByte();
                if (value == 0) {
                    return false;
                }
                if (value == 1) {
                    return true;
                }
                throw new IllegalArgumentException("bad boolean value on wire: " + (value & 0xFF));
            }

            private StrataNamespace readNamespace() {
                long lenLong = readUnsigned();
                if (lenLong > remaining()) {
                    throw new IllegalArgumentException(
                            "bad namespace length on wire: " + lenLong + " (remaining " + remaining() + ")");
                }
                namespaceOffset = pos;
                StrataNamespace namespace = StrataNamespace.readFrom((int) lenLong, this);
                pos += (int) lenLong;
                return namespace;
            }

            private void readTaggedFields() {
                long n = readUnsigned();
                if (n == 0) {
                    requireEndOfTaggedFields(remaining());
                    return;
                }
                if (n < 0 || n > 1024) {
                    throw new IllegalArgumentException("bad tagged-field count: " + n);
                }
                for (int i = 0; i < n; i++) {
                    long tagValue = readUnsigned();
                    validateTaggedFieldTag(tagValue);
                    long size = readUnsigned();
                    if (size < 0 || size > remaining()) {
                        throw new IllegalArgumentException("bad tagged-field size: " + size);
                    }
                    pos += (int) size;
                }
                requireEndOfTaggedFields(remaining());
            }

            private long readUnsigned() {
                long value = 0;
                int shift = 0;
                while (true) {
                    if (shift > 63) throw new IllegalArgumentException("varint too long");
                    byte b = readByte();
                    if (shift == 63 && (b & 0xFE) != 0) {
                        throw new IllegalArgumentException("varint too long");
                    }
                    value |= (long) (b & 0x7F) << shift;
                    if ((b & 0x80) == 0) return value;
                    shift += 7;
                }
            }

            private byte readByte() {
                require(1);
                return frame.ownedHeaderByte(pos++);
            }

            private int remaining() {
                return frame.headerLength() - pos;
            }

            private void require(int bytes) {
                if (bytes > remaining()) {
                    throw new BufferUnderflowException();
                }
            }
        }
    }

    public record Append(ChunkId chunkId, int writeEpoch, long baseOffset, long durableOffset,
                         StrataNamespace namespace, boolean recovery) {
        private static final int TAG_RECOVERY_APPEND = 0;
        private static final ThreadLocal<OwnedAppendDecoder> OWNED_DECODER =
                ThreadLocal.withInitial(OwnedAppendDecoder::new);

        public Append(ChunkId chunkId, int writeEpoch, long baseOffset, long durableOffset,
                      StrataNamespace namespace) {
            this(chunkId, writeEpoch, baseOffset, durableOffset, namespace, false);
        }

        public static Append recovery(ChunkId chunkId, int writeEpoch, long baseOffset,
                                      long durableOffset, StrataNamespace namespace) {
            return new Append(chunkId, writeEpoch, baseOffset, durableOffset, namespace, true);
        }

        public Append {
            namespace = Objects.requireNonNull(namespace, "namespace");
        }

        public byte[] encode() {
            int len = namespace.value().length(); // ASCII namespace: UTF-8 length == char length
            BufWriter w = new BufWriter(33 + (len < 128 ? 1 : 2) + len);
            w.chunkId(chunkId).i32(writeEpoch).u64(baseOffset).u64(durableOffset)
                    .namespace(namespace);
            if (recovery) {
                TaggedFields.of(Map.of(TAG_RECOVERY_APPEND, new byte[] {1})).writeTo(w);
            } else {
                w.noTags();
            }
            return w.toBytes();
        }

        public static Append decode(ByteBuffer b) {
            ChunkId chunkId = ChunkId.readFrom(b);
            int writeEpoch = b.getInt();
            long baseOffset = b.getLong();
            long durableOffset = b.getLong();
            StrataNamespace namespace = StrataNamespace.readFrom(b);
            return new Append(chunkId, writeEpoch, baseOffset, durableOffset, namespace,
                    readRecoveryTag(b));
        }

        public static Append decode(Frame frame) {
            AppendFields fields = decodeFields(frame);
            return new Append(fields.chunkId(), fields.writeEpoch(), fields.baseOffset(), fields.durableOffset(),
                    fields.namespace(), fields.recovery());
        }

        /**
         * Thread-local decode view for hot server paths. The returned object is overwritten by the next
         * {@code decodeFields} call on the same thread; callers must copy any fields they keep asynchronously.
         */
        public static AppendFields decodeFields(Frame frame) {
            OwnedAppendDecoder decoder = OWNED_DECODER.get();
            if (!frame.hasOwnedHeader()) {
                return decoder.decode(decode(frame.headerReadBuffer()));
            }
            return decoder.decode(frame);
        }

        private static boolean readRecoveryTag(ByteBuffer b) {
            long n = Varint.readUnsigned(b);
            if (n == 0) {
                requireEndOfTaggedFields(b.remaining());
                return false;
            }
            if (n < 0 || n > 1024) {
                throw new IllegalArgumentException("bad tagged-field count: " + n);
            }
            boolean hasRecovery = false;
            boolean recovery = false;
            long recoverySize = -1;
            for (int i = 0; i < n; i++) {
                long tagValue = Varint.readUnsigned(b);
                validateTaggedFieldTag(tagValue);
                long size = Varint.readUnsigned(b);
                if (size < 0 || size > b.remaining()) {
                    throw new IllegalArgumentException("bad tagged-field size: " + size);
                }
                if ((int) tagValue == TAG_RECOVERY_APPEND) {
                    hasRecovery = true;
                    recoverySize = size;
                    if (size == 1) {
                        recovery = b.get() != 0;
                    } else {
                        b.position(b.position() + (int) size);
                    }
                } else {
                    b.position(b.position() + (int) size);
                }
            }
            requireEndOfTaggedFields(b.remaining());
            if (hasRecovery && recoverySize != 1) {
                throw new IllegalArgumentException("bad append recovery tag size: " + recoverySize);
            }
            return hasRecovery && recovery;
        }

        private static void validateTaggedFieldTag(long tag) {
            if (tag < 0 || tag > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("bad tagged-field tag: " + tag);
            }
        }

        private static void requireEndOfTaggedFields(int remaining) {
            if (remaining != 0) {
                throw new IllegalArgumentException("trailing bytes after tagged fields: " + remaining);
            }
        }

        public static final class AppendFields {
            private ChunkId chunkId;
            private long fileId;
            private int chunkIndex;
            private int writeEpoch;
            private long baseOffset;
            private long durableOffset;
            private StrataNamespace namespace;
            private boolean recovery;

            private AppendFields set(ChunkId chunkId, int writeEpoch, long baseOffset, long durableOffset,
                                     StrataNamespace namespace, boolean recovery) {
                this.chunkId = chunkId;
                this.fileId = chunkId.fileId().id();
                this.chunkIndex = chunkId.index();
                this.writeEpoch = writeEpoch;
                this.baseOffset = baseOffset;
                this.durableOffset = durableOffset;
                this.namespace = namespace;
                this.recovery = recovery;
                return this;
            }

            private AppendFields set(long fileId, int chunkIndex, int writeEpoch, long baseOffset,
                                     long durableOffset, StrataNamespace namespace, boolean recovery) {
                this.chunkId = null;
                this.fileId = fileId;
                this.chunkIndex = chunkIndex;
                this.writeEpoch = writeEpoch;
                this.baseOffset = baseOffset;
                this.durableOffset = durableOffset;
                this.namespace = namespace;
                this.recovery = recovery;
                return this;
            }

            public ChunkId chunkId() {
                if (chunkId == null) {
                    chunkId = new ChunkId(new FileId(fileId), chunkIndex);
                }
                return chunkId;
            }

            public long fileId() {
                return fileId;
            }

            public int chunkIndex() {
                return chunkIndex;
            }

            public int writeEpoch() {
                return writeEpoch;
            }

            public long baseOffset() {
                return baseOffset;
            }

            public long durableOffset() {
                return durableOffset;
            }

            public StrataNamespace namespace() {
                return namespace;
            }

            public boolean recovery() {
                return recovery;
            }
        }

        private static final class OwnedAppendDecoder implements StrataNamespace.AsciiBytes {
            private final AppendFields fields = new AppendFields();
            private Frame frame;
            private int pos;
            private int namespaceOffset;

            AppendFields decode(Append append) {
                return fields.set(append.chunkId(), append.writeEpoch(), append.baseOffset(),
                        append.durableOffset(), append.namespace(), append.recovery());
            }

            AppendFields decode(Frame frame) {
                this.frame = frame;
                pos = 0;
                namespaceOffset = 0;
                try {
                    long fileId = readLong();
                    int chunkIndex = readInt();
                    int writeEpoch = readInt();
                    long baseOffset = readLong();
                    long durableOffset = readLong();
                    StrataNamespace namespace = readNamespace();
                    boolean recovery = readRecoveryTag();
                    return fields.set(fileId, chunkIndex, writeEpoch, baseOffset, durableOffset, namespace, recovery);
                } finally {
                    this.frame = null;
                }
            }

            @Override
            public byte byteAt(int index) {
                return frame.ownedHeaderByte(namespaceOffset + index);
            }

            private long readLong() {
                require(8);
                long value = frame.ownedHeaderLong(pos);
                pos += 8;
                return value;
            }

            private int readInt() {
                require(4);
                int value = frame.ownedHeaderInt(pos);
                pos += 4;
                return value;
            }

            private StrataNamespace readNamespace() {
                long lenLong = readUnsigned();
                if (lenLong > remaining()) {
                    throw new IllegalArgumentException(
                            "bad namespace length on wire: " + lenLong + " (remaining " + remaining() + ")");
                }
                namespaceOffset = pos;
                StrataNamespace namespace = StrataNamespace.readFrom((int) lenLong, this);
                pos += (int) lenLong;
                return namespace;
            }

            private boolean readRecoveryTag() {
                long n = readUnsigned();
                if (n == 0) {
                    requireEndOfTaggedFields(remaining());
                    return false;
                }
                if (n < 0 || n > 1024) {
                    throw new IllegalArgumentException("bad tagged-field count: " + n);
                }
                boolean hasRecovery = false;
                boolean recovery = false;
                long recoverySize = -1;
                for (int i = 0; i < n; i++) {
                    long tagValue = readUnsigned();
                    validateTaggedFieldTag(tagValue);
                    long size = readUnsigned();
                    if (size < 0 || size > remaining()) {
                        throw new IllegalArgumentException("bad tagged-field size: " + size);
                    }
                    if ((int) tagValue == TAG_RECOVERY_APPEND) {
                        hasRecovery = true;
                        recoverySize = size;
                        if (size == 1) {
                            recovery = readByte() != 0;
                        } else {
                            pos += (int) size;
                        }
                    } else {
                        pos += (int) size;
                    }
                }
                requireEndOfTaggedFields(remaining());
                if (hasRecovery && recoverySize != 1) {
                    throw new IllegalArgumentException("bad append recovery tag size: " + recoverySize);
                }
                return hasRecovery && recovery;
            }

            private long readUnsigned() {
                long value = 0;
                int shift = 0;
                while (true) {
                    if (shift > 63) throw new IllegalArgumentException("varint too long");
                    byte b = readByte();
                    if (shift == 63 && (b & 0xFE) != 0) {
                        throw new IllegalArgumentException("varint too long");
                    }
                    value |= (long) (b & 0x7F) << shift;
                    if ((b & 0x80) == 0) return value;
                    shift += 7;
                }
            }

            private byte readByte() {
                require(1);
                return frame.ownedHeaderByte(pos++);
            }

            private int remaining() {
                return frame.headerLength() - pos;
            }

            private void require(int bytes) {
                if (bytes > remaining()) {
                    throw new BufferUnderflowException();
                }
            }
        }
    }

    public record AppendResp(long endOffset) {
        public byte[] encode() {
            return okWithU64(endOffset);
        }

        public static AppendResp decode(ByteBuffer b) {
            AppendResp m = new AppendResp(b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record Read(ChunkId chunkId, long offset, int maxBytes, StrataNamespace namespace) {
        private static final ThreadLocal<ReadFields> FIELDS = ThreadLocal.withInitial(ReadFields::new);
        private static final ThreadLocal<OwnedReadDecoder> OWNED_DECODER =
                ThreadLocal.withInitial(OwnedReadDecoder::new);

        public Read {
            namespace = Objects.requireNonNull(namespace, "namespace");
        }

        public static final class ReadFields {
            private ChunkId chunkId;
            private long fileId;
            private int chunkIndex;
            private long offset;
            private int maxBytes;
            private StrataNamespace namespace;

            private ReadFields set(long fileId, int chunkIndex, long offset, int maxBytes,
                                   StrataNamespace namespace) {
                this.chunkId = null;
                this.fileId = fileId;
                this.chunkIndex = chunkIndex;
                this.offset = offset;
                this.maxBytes = maxBytes;
                this.namespace = namespace;
                return this;
            }

            public ChunkId chunkId() {
                if (chunkId == null) {
                    chunkId = new ChunkId(new FileId(fileId), chunkIndex);
                }
                return chunkId;
            }

            public long fileId() {
                return fileId;
            }

            public int chunkIndex() {
                return chunkIndex;
            }

            public long offset() {
                return offset;
            }

            public int maxBytes() {
                return maxBytes;
            }

            public StrataNamespace namespace() {
                return namespace;
            }
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).u64(offset).u32(maxBytes).namespace(namespace).noTags();
            return w.toBytes();
        }

        public static Read decode(ByteBuffer b) {
            ReadFields fields = decodeFields(b);
            return new Read(fields.chunkId(), fields.offset(), fields.maxBytes(), fields.namespace());
        }

        public static ReadFields decodeFields(ByteBuffer b) {
            ReadFields fields = FIELDS.get();
            long fileId = b.getLong();
            int chunkIndex = b.getInt();
            long offset = b.getLong();
            int maxBytes = b.getInt();
            StrataNamespace namespace = StrataNamespace.readFrom(b);
            TaggedFields.readFrom(b);
            return fields.set(fileId, chunkIndex, offset, maxBytes, namespace);
        }

        /**
         * Thread-local decode view for hot server paths. The returned object is overwritten by the next
         * {@code decodeFields} call on the same thread; callers must copy any fields they keep asynchronously.
         */
        public static ReadFields decodeFields(Frame frame) {
            if (!frame.hasOwnedHeader()) {
                return decodeFields(frame.headerReadBuffer());
            }
            return OWNED_DECODER.get().decode(frame);
        }

        private static void requireEndOfTaggedFields(int remaining) {
            if (remaining != 0) {
                throw new IllegalArgumentException("trailing bytes after tagged fields: " + remaining);
            }
        }

        private static final class OwnedReadDecoder implements StrataNamespace.AsciiBytes {
            private final ReadFields fields = new ReadFields();
            private Frame frame;
            private int pos;
            private int namespaceOffset;

            ReadFields decode(Frame frame) {
                this.frame = frame;
                pos = 0;
                namespaceOffset = 0;
                try {
                    long fileId = readLong();
                    int chunkIndex = readInt();
                    long offset = readLong();
                    int maxBytes = readInt();
                    StrataNamespace namespace = readNamespace();
                    readTaggedFields();
                    return fields.set(fileId, chunkIndex, offset, maxBytes, namespace);
                } finally {
                    this.frame = null;
                }
            }

            @Override
            public byte byteAt(int index) {
                return frame.ownedHeaderByte(namespaceOffset + index);
            }

            private long readLong() {
                require(Long.BYTES);
                long value = frame.ownedHeaderLong(pos);
                pos += Long.BYTES;
                return value;
            }

            private int readInt() {
                require(Integer.BYTES);
                int value = frame.ownedHeaderInt(pos);
                pos += Integer.BYTES;
                return value;
            }

            private StrataNamespace readNamespace() {
                long lenLong = readUnsigned();
                if (lenLong > remaining()) {
                    throw new IllegalArgumentException(
                            "bad namespace length on wire: " + lenLong + " (remaining " + remaining() + ")");
                }
                namespaceOffset = pos;
                StrataNamespace namespace = StrataNamespace.readFrom((int) lenLong, this);
                pos += (int) lenLong;
                return namespace;
            }

            private void readTaggedFields() {
                long n = readUnsigned();
                if (n == 0) {
                    requireEndOfTaggedFields(remaining());
                    return;
                }
                if (n < 0 || n > 1024) {
                    throw new IllegalArgumentException("bad tagged-field count: " + n);
                }
                for (int i = 0; i < n; i++) {
                    long tagValue = readUnsigned();
                    if (tagValue < 0 || tagValue > Integer.MAX_VALUE) {
                        throw new IllegalArgumentException("bad tagged-field tag: " + tagValue);
                    }
                    long size = readUnsigned();
                    if (size < 0 || size > remaining()) {
                        throw new IllegalArgumentException("bad tagged-field size: " + size);
                    }
                    pos += (int) size;
                }
                requireEndOfTaggedFields(remaining());
            }

            private long readUnsigned() {
                long value = 0;
                int shift = 0;
                while (true) {
                    if (shift > 63) throw new IllegalArgumentException("varint too long");
                    byte b = readByte();
                    if (shift == 63 && (b & 0xFE) != 0) {
                        throw new IllegalArgumentException("varint too long");
                    }
                    value |= (long) (b & 0x7F) << shift;
                    if ((b & 0x80) == 0) return value;
                    shift += 7;
                }
            }

            private byte readByte() {
                require(Byte.BYTES);
                return frame.ownedHeaderByte(pos++);
            }

            private int remaining() {
                return frame.headerLength() - pos;
            }

            private void require(int bytes) {
                if (bytes > remaining()) {
                    throw new BufferUnderflowException();
                }
            }
        }
    }

    public record ReadResp(long localEndOffset, long durableOffset) {
        public byte[] encode() {
            return okWithTwoU64(localEndOffset, durableOffset);
        }

        public static ReadResp decode(ByteBuffer b) {
            ReadResp m = new ReadResp(b.getLong(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record Fence(ChunkId chunkId, int fenceEpoch, StrataNamespace namespace) {
        public Fence {
            namespace = Objects.requireNonNull(namespace, "namespace");
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).i32(fenceEpoch).namespace(namespace).noTags();
            return w.toBytes();
        }

        public static Fence decode(ByteBuffer b) {
            Fence m = new Fence(ChunkId.readFrom(b), b.getInt(),
                    StrataNamespace.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record FenceResp(int persistedFenceEpoch, long localEndOffset, long lastKnownDO, ChunkState state) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.i32(persistedFenceEpoch).u64(localEndOffset).u64(lastKnownDO).u8(state.value).noTags();
            return w.toBytes();
        }

        public static FenceResp decode(ByteBuffer b) {
            FenceResp m = new FenceResp(b.getInt(), b.getLong(), b.getLong(), ChunkState.fromValue(b.get()));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record StatChunk(ChunkId chunkId, StrataNamespace namespace) {
        public StatChunk {
            namespace = Objects.requireNonNull(namespace, "namespace");
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).namespace(namespace).noTags();
            return w.toBytes();
        }

        public static StatChunk decode(ByteBuffer b) {
            StatChunk m = new StatChunk(ChunkId.readFrom(b),
                    StrataNamespace.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record StatResp(ChunkState state, long localEndOffset, long lastKnownDO,
                           int writeEpoch, int fenceEpoch, long sealedLength, int sealedCrc) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u8(state.value).u64(localEndOffset).u64(lastKnownDO).i32(writeEpoch).i32(fenceEpoch)
                    .u64(sealedLength).u32(sealedCrc).noTags();
            return w.toBytes();
        }

        public static StatResp decode(ByteBuffer b) {
            StatResp m = new StatResp(ChunkState.fromValue(b.get()), b.getLong(), b.getLong(),
                    b.getInt(), b.getInt(), b.getLong(), b.getInt());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record SealChunk(ChunkId chunkId, int writeEpoch, long dataLength, StrataNamespace namespace,
                            long ownerEpoch) {
        public SealChunk(ChunkId chunkId, int writeEpoch, long dataLength, StrataNamespace namespace) {
            this(chunkId, writeEpoch, dataLength, namespace, 0);
        }

        public SealChunk {
            namespace = Objects.requireNonNull(namespace, "namespace");
            requireNonNegativeOwnerEpoch(ownerEpoch);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).i32(writeEpoch).u64(dataLength).namespace(namespace);
            writeOwnerEpochTags(w, ownerEpoch);
            return w.toBytes();
        }

        public static SealChunk decode(ByteBuffer b) {
            ChunkId chunkId = ChunkId.readFrom(b);
            int writeEpoch = b.getInt();
            long dataLength = b.getLong();
            StrataNamespace namespace = StrataNamespace.readFrom(b);
            TaggedFields tags = TaggedFields.readFrom(b);
            return new SealChunk(chunkId, writeEpoch, dataLength, namespace,
                    readU64Tag(tags, TAG_OWNER_EPOCH, "ownerEpoch"));
        }
    }

    public record SealResp(long finalLength, int chunkCrc) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u64(finalLength).u32(chunkCrc).noTags();
            return w.toBytes();
        }

        public static SealResp decode(ByteBuffer b) {
            SealResp m = new SealResp(b.getLong(), b.getInt());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record DeleteChunks(List<ChunkId> chunkIds, StrataNamespace namespace, long ownerEpoch) {
        public DeleteChunks(List<ChunkId> chunkIds, StrataNamespace namespace) {
            this(chunkIds, namespace, 0);
        }

        public DeleteChunks {
            chunkIds = List.copyOf(chunkIds);
            namespace = Objects.requireNonNull(namespace, "namespace");
            requireNonNegativeOwnerEpoch(ownerEpoch);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.varint(chunkIds.size());
            for (ChunkId c : chunkIds) w.chunkId(c);
            w.namespace(namespace);
            writeOwnerEpochTags(w, ownerEpoch);
            return w.toBytes();
        }

        public static DeleteChunks decode(ByteBuffer b) {
            int n = count(b);
            List<ChunkId> ids = new ArrayList<>(n);
            for (int i = 0; i < n; i++) ids.add(ChunkId.readFrom(b));
            StrataNamespace namespace = StrataNamespace.readFrom(b);
            TaggedFields tags = TaggedFields.readFrom(b);
            return new DeleteChunks(ids, namespace, readU64Tag(tags, TAG_OWNER_EPOCH, "ownerEpoch"));
        }
    }

    public record DeleteChunksResp(List<ChunkId> chunkIds, List<Short> codes) {
        public DeleteChunksResp {
            chunkIds = List.copyOf(chunkIds);
            codes = List.copyOf(codes);
            if (chunkIds.size() != codes.size()) {
                throw new IllegalArgumentException("chunkIds/codes size mismatch");
            }
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.varint(chunkIds.size());
            for (int i = 0; i < chunkIds.size(); i++) {
                w.chunkId(chunkIds.get(i)).u16(codes.get(i));
            }
            w.noTags();
            return w.toBytes();
        }

        public static DeleteChunksResp decode(ByteBuffer b) {
            int n = count(b);
            List<ChunkId> ids = new ArrayList<>(n);
            List<Short> codes = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                ids.add(ChunkId.readFrom(b));
                codes.add(b.getShort());
            }
            TaggedFields.readFrom(b);
            return new DeleteChunksResp(ids, codes);
        }
    }

    public record FetchChunk(ChunkId chunkId, long offset, int maxBytes, StrataNamespace namespace) {
        public FetchChunk {
            namespace = Objects.requireNonNull(namespace, "namespace");
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).u64(offset).u32(maxBytes).namespace(namespace).noTags();
            return w.toBytes();
        }

        public static FetchChunk decode(ByteBuffer b) {
            FetchChunk m = new FetchChunk(ChunkId.readFrom(b), b.getLong(), b.getInt(),
                    StrataNamespace.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record FetchResp(long fileLength, ChunkState state) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u64(fileLength).u8(state.value).noTags();
            return w.toBytes();
        }

        public static FetchResp decode(ByteBuffer b) {
            FetchResp m = new FetchResp(b.getLong(), ChunkState.fromValue(b.get()));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record ReadLedger(ChunkId chunkId, long fromOffset, StrataNamespace namespace) {
        public ReadLedger {
            namespace = Objects.requireNonNull(namespace, "namespace");
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).u64(fromOffset).namespace(namespace).noTags();
            return w.toBytes();
        }

        public static ReadLedger decode(ByteBuffer b) {
            ReadLedger m = new ReadLedger(ChunkId.readFrom(b), b.getLong(),
                    StrataNamespace.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    /** One integrity-ledger entry on the wire: bytes (prevEnd, endOffset] with the given payload CRC. */
    public record LedgerEntry(long endOffset, int payloadCrc, int writeEpoch) {}

    public record ReadLedgerResp(List<LedgerEntry> entries) {
        public ReadLedgerResp {
            entries = List.copyOf(entries);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.varint(entries.size());
            for (LedgerEntry e : entries) w.u64(e.endOffset()).u32(e.payloadCrc()).i32(e.writeEpoch());
            w.noTags();
            return w.toBytes();
        }

        public static ReadLedgerResp decode(ByteBuffer b) {
            int n = count(b);
            List<LedgerEntry> es = new ArrayList<>(n);
            for (int i = 0; i < n; i++) es.add(new LedgerEntry(b.getLong(), b.getInt(), b.getInt()));
            TaggedFields.readFrom(b);
            return new ReadLedgerResp(es);
        }
    }

    /* ---------- control plane: data node <-> metadata ---------- */

    public record StorageCapacity(long capacityBytes) {}

    public record RegisterNode(int nodeId, long incMsb, long incLsb, List<String> endpoints,
                               String zone, String rack, String host,
                               List<StorageCapacity> capacities, int onDiskFormatMax, long featureBits) {
        public RegisterNode {
            endpoints = List.copyOf(endpoints);
            capacities = List.copyOf(capacities);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.u32(nodeId).u64(incMsb).u64(incLsb);
            w.varint(endpoints.size());
            for (String e : endpoints) w.string(e);
            w.string(zone).string(rack).string(host);
            w.varint(capacities.size());
            for (StorageCapacity c : capacities) w.u64(c.capacityBytes());
            w.u32(onDiskFormatMax).u64(featureBits).noTags();
            return w.toBytes();
        }

        public static RegisterNode decode(ByteBuffer b) {
            int nodeId = b.getInt();
            long msb = b.getLong(), lsb = b.getLong();
            int ne = count(b);
            List<String> eps = new ArrayList<>(ne);
            for (int i = 0; i < ne; i++) eps.add(Varint.readString(b));
            String zone = Varint.readString(b), rack = Varint.readString(b), host = Varint.readString(b);
            int nc = count(b);
            List<StorageCapacity> caps = new ArrayList<>(nc);
            for (int i = 0; i < nc; i++) caps.add(new StorageCapacity(b.getLong()));
            int fmt = b.getInt();
            long features = b.getLong();
            TaggedFields.readFrom(b);
            return new RegisterNode(nodeId, msb, lsb, eps, zone, rack, host, caps, fmt, features);
        }
    }

    public record RegisterResp(int nodeId, long sessionEpoch, int heartbeatIntervalMs, int leaseMs) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u32(nodeId).u64(sessionEpoch).u32(heartbeatIntervalMs).u32(leaseMs).noTags();
            return w.toBytes();
        }

        public static RegisterResp decode(ByteBuffer b) {
            RegisterResp m = new RegisterResp(b.getInt(), b.getLong(), b.getInt(), b.getInt());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record StorageUsage(long usedBytes, long freeBytes) {}

    public record CompletedCommand(long commandId, short status) {}

    public record NodeHeartbeat(int nodeId, long incMsb, long incLsb, long sessionEpoch,
                                List<StorageUsage> usages, int repairQueueDepth,
                                List<CompletedCommand> completedCommands) {
        public static final int TAG_COMPLETED_COMMANDS = 0;

        public NodeHeartbeat {
            usages = List.copyOf(usages);
            completedCommands = List.copyOf(completedCommands);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.u32(nodeId).u64(incMsb).u64(incLsb).u64(sessionEpoch);
            w.varint(usages.size());
            for (StorageUsage u : usages) w.u64(u.usedBytes()).u64(u.freeBytes());
            w.u32(repairQueueDepth);
            if (completedCommands.isEmpty()) {
                w.noTags();
            } else {
                BufWriter cc = new BufWriter();
                cc.varint(completedCommands.size());
                for (CompletedCommand c : completedCommands) cc.u64(c.commandId()).u16(c.status());
                TaggedFields.of(Map.of(TAG_COMPLETED_COMMANDS, cc.toBytes())).writeTo(w);
            }
            return w.toBytes();
        }

        public static NodeHeartbeat decode(ByteBuffer b) {
            int nodeId = b.getInt();
            long msb = b.getLong(), lsb = b.getLong();
            long session = b.getLong();
            int nu = count(b);
            List<StorageUsage> us = new ArrayList<>(nu);
            for (int i = 0; i < nu; i++) us.add(new StorageUsage(b.getLong(), b.getLong()));
            int depth = b.getInt();
            TaggedFields tags = TaggedFields.readFrom(b);
            List<CompletedCommand> done = new ArrayList<>();
            byte[] cc = tags.get(TAG_COMPLETED_COMMANDS);
            if (cc != null) {
                ByteBuffer cb = ByteBuffer.wrap(cc);
                int n = count(cb);
                for (int i = 0; i < n; i++) done.add(new CompletedCommand(cb.getLong(), cb.getShort()));
                if (cb.hasRemaining()) {
                    throw new IllegalArgumentException("trailing bytes in completed-command tag");
                }
            }
            return new NodeHeartbeat(nodeId, msb, lsb, session, us, depth, done);
        }
    }

    public sealed interface Command permits ReplicateCmd, DeleteCmd, DrainCmd {
        long commandId();

        static void write(BufWriter w, Command c) {
            w.u64(c.commandId());
            switch (c) {
                case ReplicateCmd r -> {
                    w.u8(1).chunkId(r.chunkId());
                    writeReplicas(w, r.sources());
                    w.u8(r.priority()).u32(r.expectedCrc()).u64(r.expectedLength())
                     .namespace(r.namespace());
                }
                case DeleteCmd d -> {
                    w.u8(2).varint(d.chunkIds().size());
                    for (ChunkId id : d.chunkIds()) w.chunkId(id);
                    w.namespace(d.namespace());
                }
                case DrainCmd dr -> w.u8(3);
            }
        }

        static Command read(ByteBuffer b) {
            long id = b.getLong();
            byte type = b.get();
            return switch (type) {
                case 1 -> {
                    ChunkId chunkId = ChunkId.readFrom(b);
                    List<Replica> sources = readReplicas(b);
                    byte priority = b.get();
                    int expectedCrc = b.getInt();
                    long expectedLength = b.getLong();
                    StrataNamespace ns = StrataNamespace.readFrom(b);
                    yield new ReplicateCmd(id, chunkId, sources, priority, expectedCrc, expectedLength, ns);
                }
                case 2 -> {
                    int n = count(b);
                    List<ChunkId> ids = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) ids.add(ChunkId.readFrom(b));
                    StrataNamespace delNs = StrataNamespace.readFrom(b);
                    yield new DeleteCmd(id, ids, delNs);
                }
                case 3 -> new DrainCmd(id);
                default -> throw new IllegalArgumentException("unknown command type " + type);
            };
        }

        static void writeRequest(BufWriter w, Command c) {
            write(w, c);
            writeOwnerEpochTags(w, ownerEpoch(c));
        }

        static Command readRequest(ByteBuffer b) {
            Command c = read(b);
            TaggedFields tags = TaggedFields.readFrom(b);
            long ownerEpoch = readU64Tag(tags, TAG_OWNER_EPOCH, "ownerEpoch");
            return withOwnerEpoch(c, ownerEpoch);
        }

        private static long ownerEpoch(Command c) {
            return switch (c) {
                case ReplicateCmd r -> r.ownerEpoch();
                case DeleteCmd d -> d.ownerEpoch();
                case DrainCmd ignored -> 0;
            };
        }

        private static Command withOwnerEpoch(Command c, long ownerEpoch) {
            if (ownerEpoch == 0) {
                return c;
            }
            return switch (c) {
                case ReplicateCmd r -> new ReplicateCmd(r.commandId(), r.chunkId(), r.sources(), r.priority(),
                        r.expectedCrc(), r.expectedLength(), r.namespace(), ownerEpoch);
                case DeleteCmd d -> new DeleteCmd(d.commandId(), d.chunkIds(), d.namespace(), ownerEpoch);
                case DrainCmd dr -> dr;
            };
        }
    }

    public record ReplicateCmd(long commandId, ChunkId chunkId, List<Replica> sources,
                               byte priority, int expectedCrc, long expectedLength,
                               StrataNamespace namespace, long ownerEpoch) implements Command {
        public ReplicateCmd(long commandId, ChunkId chunkId, List<Replica> sources,
                            byte priority, int expectedCrc, long expectedLength,
                            StrataNamespace namespace) {
            this(commandId, chunkId, sources, priority, expectedCrc, expectedLength, namespace, 0);
        }

        public ReplicateCmd {
            sources = List.copyOf(sources);
            namespace = Objects.requireNonNull(namespace, "namespace");
            requireNonNegativeOwnerEpoch(ownerEpoch);
        }
    }

    public record DeleteCmd(long commandId, List<ChunkId> chunkIds,
                            StrataNamespace namespace, long ownerEpoch) implements Command {
        public DeleteCmd(long commandId, List<ChunkId> chunkIds, StrataNamespace namespace) {
            this(commandId, chunkIds, namespace, 0);
        }

        public DeleteCmd {
            chunkIds = List.copyOf(chunkIds);
            namespace = Objects.requireNonNull(namespace, "namespace");
            requireNonNegativeOwnerEpoch(ownerEpoch);
        }
    }

    public record DrainCmd(long commandId) implements Command {}

    public record HeartbeatResp(long leaseValidUntilMs, List<Command> commands) {
        public static final int TAG_COMMAND_OWNER_EPOCHS = 1;

        public HeartbeatResp {
            commands = List.copyOf(commands);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u64(leaseValidUntilMs);
            w.varint(commands.size());
            for (Command c : commands) Command.write(w, c);
            Map<Integer, byte[]> tags = new HashMap<>();
            byte[] ownerEpochs = commandOwnerEpochs(commands);
            if (ownerEpochs.length > 0) {
                tags.put(TAG_COMMAND_OWNER_EPOCHS, ownerEpochs);
            }
            if (tags.isEmpty()) {
                w.noTags();
            } else {
                TaggedFields.of(tags).writeTo(w);
            }
            return w.toBytes();
        }

        public static HeartbeatResp decode(ByteBuffer b) {
            long lease = b.getLong();
            int n = count(b);
            List<Command> cs = new ArrayList<>(n);
            for (int i = 0; i < n; i++) cs.add(Command.read(b));
            TaggedFields tags = TaggedFields.readFrom(b);
            Map<Long, Long> ownerEpochs = readCommandOwnerEpochs(tags);
            if (!ownerEpochs.isEmpty()) {
                cs = withCommandOwnerEpochs(cs, ownerEpochs);
            }
            return new HeartbeatResp(lease, cs);
        }

        private static byte[] commandOwnerEpochs(List<Command> commands) {
            BufWriter ownerEpochs = new BufWriter();
            int count = 0;
            for (Command command : commands) {
                long ownerEpoch = commandOwnerEpoch(command);
                if (ownerEpoch > 0) {
                    count++;
                }
            }
            if (count == 0) {
                return new byte[0];
            }
            ownerEpochs.varint(count);
            for (Command command : commands) {
                long ownerEpoch = commandOwnerEpoch(command);
                if (ownerEpoch > 0) {
                    ownerEpochs.u64(command.commandId()).u64(ownerEpoch);
                }
            }
            return ownerEpochs.toBytes();
        }

        private static long commandOwnerEpoch(Command command) {
            return switch (command) {
                case ReplicateCmd r -> r.ownerEpoch();
                case DeleteCmd d -> d.ownerEpoch();
                case DrainCmd ignored -> 0;
            };
        }

        private static Map<Long, Long> readCommandOwnerEpochs(TaggedFields tags) {
            byte[] raw = tags.get(TAG_COMMAND_OWNER_EPOCHS);
            if (raw == null) {
                return Map.of();
            }
            ByteBuffer b = ByteBuffer.wrap(raw);
            int n = count(b);
            Map<Long, Long> ownerEpochs = new HashMap<>();
            for (int i = 0; i < n; i++) {
                if (b.remaining() < 2 * Long.BYTES) {
                    throw new IllegalArgumentException("truncated command-owner-epoch tag");
                }
                long commandId = b.getLong();
                long ownerEpoch = b.getLong();
                requireNonNegativeOwnerEpoch(ownerEpoch);
                if (ownerEpochs.putIfAbsent(commandId, ownerEpoch) != null) {
                    throw new IllegalArgumentException("duplicate command-owner-epoch id " + commandId);
                }
            }
            if (b.hasRemaining()) {
                throw new IllegalArgumentException("trailing bytes in command-owner-epoch tag");
            }
            return ownerEpochs;
        }

        private static List<Command> withCommandOwnerEpochs(List<Command> commands,
                                                            Map<Long, Long> ownerEpochs) {
            List<Command> stamped = new ArrayList<>(commands.size());
            Map<Long, Long> unmatched = new HashMap<>(ownerEpochs);
            for (Command command : commands) {
                Long ownerEpoch = unmatched.remove(command.commandId());
                if (ownerEpoch == null) {
                    stamped.add(command);
                    continue;
                }
                stamped.add(switch (command) {
                    case ReplicateCmd r -> new ReplicateCmd(r.commandId(), r.chunkId(), r.sources(),
                            r.priority(), r.expectedCrc(), r.expectedLength(), r.namespace(), ownerEpoch);
                    case DeleteCmd d -> new DeleteCmd(d.commandId(), d.chunkIds(), d.namespace(), ownerEpoch);
                    case DrainCmd dr -> dr;
                });
            }
            if (!unmatched.isEmpty()) {
                throw new IllegalArgumentException("owner epoch tag references unknown command ids "
                        + unmatched.keySet());
            }
            return stamped;
        }
    }

    /* ---------- owner-pull chunk verification (design §9.2) ---------- */

    /**
     * Owner -> node: verify the listed chunks of one namespace. The node answers with the actual local
     * state of each (see {@link VerifyChunkResult}); the owner compares against its descriptor to decide
     * present-ok / missing / corrupt. {@code verifierEndpoint} is the asking owner's advertised endpoint,
     * so the node can record "last verified by which owner, when" for node-local orphan GC (design §9.2).
     */
    public record VerifyChunks(StrataNamespace namespace, String verifierEndpoint,
                               List<ChunkId> chunkIds, long ownerEpoch) {
        public VerifyChunks(StrataNamespace namespace, String verifierEndpoint, List<ChunkId> chunkIds) {
            this(namespace, verifierEndpoint, chunkIds, 0);
        }

        public VerifyChunks {
            namespace = Objects.requireNonNull(namespace, "namespace");
            verifierEndpoint = Objects.requireNonNull(verifierEndpoint, "verifierEndpoint");
            chunkIds = List.copyOf(chunkIds);
            requireNonNegativeOwnerEpoch(ownerEpoch);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.namespace(namespace).string(verifierEndpoint).varint(chunkIds.size());
            for (ChunkId c : chunkIds) w.chunkId(c);
            writeOwnerEpochTags(w, ownerEpoch);
            return w.toBytes();
        }

        public static VerifyChunks decode(ByteBuffer b) {
            StrataNamespace ns = StrataNamespace.readFrom(b);
            String verifier = Varint.readString(b);
            int n = count(b);
            List<ChunkId> ids = new ArrayList<>(n);
            for (int i = 0; i < n; i++) ids.add(ChunkId.readFrom(b));
            TaggedFields tags = TaggedFields.readFrom(b);
            return new VerifyChunks(ns, verifier, ids, readU64Tag(tags, TAG_OWNER_EPOCH, "ownerEpoch"));
        }
    }

    /**
     * One chunk's local verification fact. {@code present == false} means the node holds no such chunk
     * (a missing replica); otherwise {@code state/length/crc} are the node's actual values for the owner
     * to compare against the descriptor (a length/crc mismatch on a SEALED chunk is corruption).
     */
    public record VerifyChunkResult(ChunkId chunkId, boolean present, ChunkState state, long length, int crc) {
    }

    /** Node -> owner reply to {@link VerifyChunks}, one result per requested chunk (order-independent). */
    public record VerifyChunksResp(List<VerifyChunkResult> results) {
        public VerifyChunksResp {
            results = List.copyOf(results);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.varint(results.size());
            for (VerifyChunkResult r : results) {
                w.chunkId(r.chunkId()).u8(r.present() ? 1 : 0).u8(r.state().value)
                        .u64(r.length()).u32(r.crc());
            }
            w.noTags();
            return w.toBytes();
        }

        public static VerifyChunksResp decode(ByteBuffer b) {
            int n = count(b);
            List<VerifyChunkResult> rs = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                ChunkId chunkId = ChunkId.readFrom(b);
                boolean present = Varint.readBoolean(b);
                ChunkState state = ChunkState.fromValue(b.get());
                long length = b.getLong();
                int crc = b.getInt();
                rs.add(new VerifyChunkResult(chunkId, present, state, length, crc));
            }
            TaggedFields.readFrom(b);
            return new VerifyChunksResp(rs);
        }
    }

    /* ---------- v0 client <-> metadata ---------- */

    public record WritePolicy(int replicationFactor, int ackQuorum, boolean fsyncOnAck) {
        public static final WritePolicy DEFAULT = new WritePolicy(3, 2, false);

        public WritePolicy {
            if (replicationFactor <= 0) {
                throw new IllegalArgumentException("replicationFactor must be positive: " + replicationFactor);
            }
            if (ackQuorum <= 0 || ackQuorum > replicationFactor) {
                throw new IllegalArgumentException("ackQuorum must be in 1..replicationFactor: " + ackQuorum);
            }
            if (ackQuorum <= replicationFactor / 2) {
                throw new IllegalArgumentException("ackQuorum must intersect any other quorum: "
                        + ackQuorum + " for replicationFactor " + replicationFactor);
            }
        }

        private void writeTo(BufWriter w) {
            w.u32(replicationFactor).u32(ackQuorum).u8(fsyncOnAck ? 1 : 0);
        }

        private static WritePolicy readFrom(ByteBuffer b) {
            return new WritePolicy(b.getInt(), b.getInt(), Varint.readBoolean(b));
        }
    }

    public record CreateFile(StrataNamespace namespace, StrataPath path, WritePolicy writePolicy,
                             long opIdMsb, long opIdLsb) {
        public CreateFile {
            namespace = Objects.requireNonNull(namespace, "namespace");
            path = Objects.requireNonNull(path, "path");
            writePolicy = Objects.requireNonNull(writePolicy, "writePolicy");
        }

        public CreateFile(StrataNamespace namespace, StrataPath path, WritePolicy writePolicy) {
            this(namespace, path, writePolicy, UUID.randomUUID());
        }

        public CreateFile(String namespace, String path) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), WritePolicy.DEFAULT);
        }

        public CreateFile(String namespace, String path, WritePolicy writePolicy) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), writePolicy);
        }

        private CreateFile(StrataNamespace namespace, StrataPath path, WritePolicy writePolicy,
                           UUID opId) {
            this(namespace, path, writePolicy,
                    opId.getMostSignificantBits(), opId.getLeastSignificantBits());
        }

        public CreateFile(String namespace, String path, long opIdMsb, long opIdLsb) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), WritePolicy.DEFAULT,
                    opIdMsb, opIdLsb);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.namespace(namespace).string(path.toString());
            writePolicy.writeTo(w);
            w.u64(opIdMsb).u64(opIdLsb).noTags();
            return w.toBytes();
        }

        public static CreateFile decode(ByteBuffer b) {
            CreateFile m = new CreateFile(StrataNamespace.readFrom(b),
                    StrataPath.of(Varint.readString(b)), WritePolicy.readFrom(b),
                    b.getLong(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record CreateFileResp(FileId fileId) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.fileId(fileId).noTags();
            return w.toBytes();
        }

        public static CreateFileResp decode(ByteBuffer b) {
            CreateFileResp m = new CreateFileResp(FileId.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record CreateChunk(StrataNamespace namespace, FileId fileId, int writeEpoch, long opIdMsb, long opIdLsb,
                              List<Integer> excludedNodeIds) {
        public static final int TAG_EXCLUDED_NODE_IDS = 0;

        public CreateChunk {
            namespace = Objects.requireNonNull(namespace, "namespace");
            excludedNodeIds = List.copyOf(excludedNodeIds);
        }

        public CreateChunk(StrataNamespace namespace, FileId fileId, int writeEpoch) {
            this(namespace, fileId, writeEpoch, UUID.randomUUID());
        }

        public CreateChunk(StrataNamespace namespace, FileId fileId, int writeEpoch, long opIdMsb, long opIdLsb) {
            this(namespace, fileId, writeEpoch, opIdMsb, opIdLsb, List.of());
        }

        private CreateChunk(StrataNamespace namespace, FileId fileId, int writeEpoch, UUID opId) {
            this(namespace, fileId, writeEpoch, opId.getMostSignificantBits(), opId.getLeastSignificantBits(), List.of());
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.namespace(namespace).fileId(fileId).i32(writeEpoch).u64(opIdMsb).u64(opIdLsb);
            if (excludedNodeIds.isEmpty()) {
                w.noTags();
            } else {
                BufWriter excluded = new BufWriter();
                excluded.varint(excludedNodeIds.size());
                for (int nodeId : excludedNodeIds) excluded.u32(nodeId);
                TaggedFields.of(Map.of(TAG_EXCLUDED_NODE_IDS, excluded.toBytes())).writeTo(w);
            }
            return w.toBytes();
        }

        public static CreateChunk decode(ByteBuffer b) {
            StrataNamespace namespace = StrataNamespace.readFrom(b);
            FileId fileId = FileId.readFrom(b);
            int writeEpoch = b.getInt();
            long opIdMsb = b.getLong();
            long opIdLsb = b.getLong();
            TaggedFields tags = TaggedFields.readFrom(b);
            byte[] rawExcluded = tags.get(TAG_EXCLUDED_NODE_IDS);
            List<Integer> excluded = List.of();
            if (rawExcluded != null) {
                ByteBuffer excludedBuf = ByteBuffer.wrap(rawExcluded);
                int n = count(excludedBuf);
                List<Integer> ids = new ArrayList<>(n);
                for (int i = 0; i < n; i++) ids.add(excludedBuf.getInt());
                if (excludedBuf.hasRemaining()) {
                    throw new IllegalArgumentException(
                            "trailing bytes in CreateChunk excluded-node tag: " + excludedBuf.remaining());
                }
                excluded = ids;
            }
            return new CreateChunk(namespace, fileId, writeEpoch, opIdMsb, opIdLsb, excluded);
        }
    }

    public record CreateChunkResp(ChunkId chunkId, int writeEpoch, List<Replica> replicas) {
        public CreateChunkResp {
            replicas = List.copyOf(replicas);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.chunkId(chunkId).i32(writeEpoch);
            writeReplicas(w, replicas);
            w.noTags();
            return w.toBytes();
        }

        public static CreateChunkResp decode(ByteBuffer b) {
            CreateChunkResp m = new CreateChunkResp(ChunkId.readFrom(b), b.getInt(), readReplicas(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record AllocateWriterEpoch(StrataNamespace namespace, FileId fileId, byte purpose) {
        public static final byte FOR_APPEND = 1;
        public static final byte FOR_RECOVERY = 2;

        public AllocateWriterEpoch {
            namespace = Objects.requireNonNull(namespace, "namespace");
            fileId = Objects.requireNonNull(fileId, "fileId");
            if (purpose != FOR_APPEND && purpose != FOR_RECOVERY) {
                throw new IllegalArgumentException("unknown writer epoch purpose " + (purpose & 0xFF));
            }
        }

        public static AllocateWriterEpoch forAppend(StrataNamespace namespace, FileId fileId) {
            return new AllocateWriterEpoch(namespace, fileId, FOR_APPEND);
        }

        public static AllocateWriterEpoch forRecovery(StrataNamespace namespace, FileId fileId) {
            return new AllocateWriterEpoch(namespace, fileId, FOR_RECOVERY);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.namespace(namespace).fileId(fileId).u8(purpose).noTags();
            return w.toBytes();
        }

        public static AllocateWriterEpoch decode(ByteBuffer b) {
            AllocateWriterEpoch m = new AllocateWriterEpoch(
                    StrataNamespace.readFrom(b), FileId.readFrom(b), b.get());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record AllocateWriterEpochResp(int writerEpoch) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.i32(writerEpoch).noTags();
            return w.toBytes();
        }

        public static AllocateWriterEpochResp decode(ByteBuffer b) {
            AllocateWriterEpochResp m = new AllocateWriterEpochResp(b.getInt());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    /**
     * sealedReplicas: node ids that confirmed the seal — the descriptor keeps only these (a
     * replica skipped during seal would otherwise stay listed and serve short/stale reads).
     * Empty = keep all (compat). v0 fixed-field addition (pre-release; single apiVersion).
     */
    public record SealChunkMeta(StrataNamespace namespace, ChunkId chunkId, int writeEpoch, long length, int crc,
                                List<Integer> sealedReplicas, long opIdMsb, long opIdLsb) {
        public SealChunkMeta {
            namespace = Objects.requireNonNull(namespace, "namespace");
            sealedReplicas = List.copyOf(sealedReplicas);
        }

        /**
         * Convenience for callers that do not pin a chunk incarnation. opId (0,0) is the sentinel
         * "no incarnation pin": the controller then relies on the write-epoch fence alone. Seal
         * recovery uses this — it seals at a strictly higher epoch and never reuses an incarnation,
         * so the fence already separates it from any stale same-epoch seal.
         */
        public SealChunkMeta(StrataNamespace namespace, ChunkId chunkId, int writeEpoch, long length, int crc,
                             List<Integer> sealedReplicas) {
            this(namespace, chunkId, writeEpoch, length, crc, sealedReplicas, 0L, 0L);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.namespace(namespace).chunkId(chunkId).i32(writeEpoch).u64(length).u32(crc);
            w.varint(sealedReplicas.size());
            for (int id : sealedReplicas) w.u32(id);
            w.u64(opIdMsb).u64(opIdLsb).noTags();
            return w.toBytes();
        }

        public static SealChunkMeta decode(ByteBuffer b) {
            StrataNamespace namespace = StrataNamespace.readFrom(b);
            ChunkId id = ChunkId.readFrom(b);
            int epoch = b.getInt();
            long length = b.getLong();
            int crc = b.getInt();
            int n = count(b);
            List<Integer> sealed = new ArrayList<>(n);
            for (int i = 0; i < n; i++) sealed.add(b.getInt());
            long opIdMsb = b.getLong();
            long opIdLsb = b.getLong();
            TaggedFields.readFrom(b);
            return new SealChunkMeta(namespace, id, epoch, length, crc, sealed, opIdMsb, opIdLsb);
        }
    }

    public record AbortChunkMeta(StrataNamespace namespace, ChunkId chunkId, int writeEpoch, long opIdMsb, long opIdLsb) {
        public AbortChunkMeta {
            namespace = Objects.requireNonNull(namespace, "namespace");
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.namespace(namespace).chunkId(chunkId).i32(writeEpoch).u64(opIdMsb).u64(opIdLsb).noTags();
            return w.toBytes();
        }

        public static AbortChunkMeta decode(ByteBuffer b) {
            AbortChunkMeta m = new AbortChunkMeta(
                    StrataNamespace.readFrom(b), ChunkId.readFrom(b), b.getInt(), b.getLong(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record LookupFile(StrataNamespace namespace, FileId fileId) {
        public LookupFile {
            namespace = Objects.requireNonNull(namespace, "namespace");
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.namespace(namespace).fileId(fileId).noTags();
            return w.toBytes();
        }

        public static LookupFile decode(ByteBuffer b) {
            LookupFile m = new LookupFile(StrataNamespace.readFrom(b), FileId.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record LookupPath(StrataNamespace namespace, StrataPath path) {
        public LookupPath {
            namespace = Objects.requireNonNull(namespace, "namespace");
            path = Objects.requireNonNull(path, "path");
        }

        public LookupPath(String namespace, String path) {
            this(StrataNamespace.of(namespace), StrataPath.of(path));
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.namespace(namespace).string(path.toString()).noTags();
            return w.toBytes();
        }

        public static LookupPath decode(ByteBuffer b) {
            LookupPath m = new LookupPath(StrataNamespace.readFrom(b),
                    StrataPath.of(Varint.readString(b)));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record LookupPathResp(FileId fileId) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.fileId(fileId).noTags();
            return w.toBytes();
        }

        public static LookupPathResp decode(ByteBuffer b) {
            LookupPathResp m = new LookupPathResp(FileId.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record ChunkInfo(ChunkId chunkId, ChunkState state, long length, int crc,
                            int writeEpoch, List<Replica> replicas) {
        public ChunkInfo {
            replicas = List.copyOf(replicas);
        }

        static void write(BufWriter w, ChunkInfo c) {
            w.chunkId(c.chunkId()).u8(c.state().value).u64(c.length()).u32(c.crc()).i32(c.writeEpoch());
            writeReplicas(w, c.replicas());
        }

        static ChunkInfo read(ByteBuffer b) {
            return new ChunkInfo(ChunkId.readFrom(b), ChunkState.fromValue(b.get()), b.getLong(),
                    b.getInt(), b.getInt(), readReplicas(b));
        }
    }

    public record LookupFileResp(StrataNamespace namespace, StrataPath path, WritePolicy writePolicy,
                                 byte fileState,
                                 List<ChunkInfo> chunks, long ownerEpoch) {
        public LookupFileResp(String namespace, String path, WritePolicy writePolicy, byte fileState,
                              List<ChunkInfo> chunks) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), writePolicy, fileState, chunks, 0);
        }

        public LookupFileResp(String namespace, String path, WritePolicy writePolicy, byte fileState,
                              List<ChunkInfo> chunks, long ownerEpoch) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), writePolicy, fileState, chunks, ownerEpoch);
        }

        public LookupFileResp(StrataNamespace namespace, StrataPath path, WritePolicy writePolicy,
                              byte fileState, List<ChunkInfo> chunks) {
            this(namespace, path, writePolicy, fileState, chunks, 0);
        }

        public LookupFileResp {
            namespace = Objects.requireNonNull(namespace, "namespace");
            path = Objects.requireNonNull(path, "path");
            writePolicy = Objects.requireNonNull(writePolicy, "writePolicy");
            chunks = List.copyOf(chunks);
            requireNonNegativeOwnerEpoch(ownerEpoch);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.namespace(namespace).string(path.toString());
            writePolicy.writeTo(w);
            w.u8(fileState).varint(chunks.size());
            for (ChunkInfo c : chunks) ChunkInfo.write(w, c);
            writeOwnerEpochTags(w, ownerEpoch);
            return w.toBytes();
        }

        public static LookupFileResp decode(ByteBuffer b) {
            StrataNamespace namespace = StrataNamespace.readFrom(b);
            StrataPath path = StrataPath.of(Varint.readString(b));
            WritePolicy writePolicy = WritePolicy.readFrom(b);
            byte state = b.get();
            int n = count(b);
            List<ChunkInfo> cs = new ArrayList<>(n);
            for (int i = 0; i < n; i++) cs.add(ChunkInfo.read(b));
            TaggedFields tags = TaggedFields.readFrom(b);
            return new LookupFileResp(namespace, path, writePolicy, state, cs,
                    readU64Tag(tags, TAG_OWNER_EPOCH, "ownerEpoch"));
        }
    }

    public record DeleteFiles(StrataNamespace namespace, List<FileId> fileIds) {
        public DeleteFiles {
            namespace = Objects.requireNonNull(namespace, "namespace");
            fileIds = List.copyOf(fileIds);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.namespace(namespace);
            w.varint(fileIds.size());
            for (FileId f : fileIds) w.fileId(f);
            w.noTags();
            return w.toBytes();
        }

        public static DeleteFiles decode(ByteBuffer b) {
            StrataNamespace namespace = StrataNamespace.readFrom(b);
            int n = count(b);
            List<FileId> ids = new ArrayList<>(n);
            for (int i = 0; i < n; i++) ids.add(FileId.readFrom(b));
            TaggedFields.readFrom(b);
            return new DeleteFiles(namespace, ids);
        }
    }

    public record DeleteFilesResp(List<FileId> fileIds, List<Short> codes) {
        public DeleteFilesResp {
            fileIds = List.copyOf(fileIds);
            codes = List.copyOf(codes);
            if (fileIds.size() != codes.size()) {
                throw new IllegalArgumentException("fileIds/codes size mismatch");
            }
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.varint(fileIds.size());
            for (int i = 0; i < fileIds.size(); i++) {
                w.fileId(fileIds.get(i)).u16(codes.get(i));
            }
            w.noTags();
            return w.toBytes();
        }

        public static DeleteFilesResp decode(ByteBuffer b) {
            int n = count(b);
            List<FileId> ids = new ArrayList<>(n);
            List<Short> codes = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                ids.add(FileId.readFrom(b));
                codes.add(b.getShort());
            }
            TaggedFields.readFrom(b);
            return new DeleteFilesResp(ids, codes);
        }
    }

    public record SealFile(StrataNamespace namespace, FileId fileId, long totalLength) {
        public SealFile {
            namespace = Objects.requireNonNull(namespace, "namespace");
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.namespace(namespace).fileId(fileId).u64(totalLength).noTags();
            return w.toBytes();
        }

        public static SealFile decode(ByteBuffer b) {
            SealFile m = new SealFile(StrataNamespace.readFrom(b), FileId.readFrom(b), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    private static final byte[] OK_HEADER;
    static {
        BufWriter w = new BufWriter(8);
        Resp.writeOk(w);
        w.noTags();
        OK_HEADER = w.toBytes();
    }

    /** Shared empty success response (error header only). */
    public static byte[] okHeader() {
        return OK_HEADER.clone();
    }

    /** Consumes the error header and the empty tagged block of an ok-only response. */
    public static void decodeOkHeader(ByteBuffer b) {
        TaggedFields.readFrom(b);
    }
}
