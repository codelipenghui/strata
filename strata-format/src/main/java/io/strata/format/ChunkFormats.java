package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.FileId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * On-disk format constants and codecs (tech design §11, v0 format version 1).
 *
 * Chunk file: [4096B header][data region (raw logical bytes)][footer — sealed only][64B trailer].
 * Logical chunk offset X lives at file offset 4096 + X (address arithmetic invariant).
 */
public final class ChunkFormats {
    private ChunkFormats() {}

    public static final int FORMAT_VERSION = 1;
    public static final int HEADER_SIZE = 4096;
    public static final long DATA_START = HEADER_SIZE;
    public static final int TRAILER_SIZE = 64;
    public static final int SIDECAR_SIZE = 512;
    public static final int LEDGER_ENTRY_SIZE = 24;
    public static final int CRC_RANGE_SIZE = 4 * 1024 * 1024;

    public static final int MAGIC_CHUNK = 0x5343484B;   // "SCHK"
    public static final int MAGIC_TRAILER = 0x53465452; // "SFTR"
    public static final int MAGIC_SIDECAR = 0x534D4554; // "SMET"

    // footer section types (tech design §11.2)
    public static final int SECTION_OFFSET_INDEX = 1;
    public static final int SECTION_TIME_INDEX = 2;
    public static final int SECTION_PRODUCER_SNAPSHOT = 3;
    public static final int SECTION_ABORTED_TXN_INDEX = 4;
    public static final int SECTION_CRC_RANGES = 5;
    public static final int SECTION_STATS = 6;

    /* ---------------- header block (4096B, CRC over [0,4092) in last 4 bytes) ---------------- */

    public record Header(ChunkId chunkId, boolean fsyncOnAck,
                         int createWriteEpoch, long createdAtMs,
                         int compatFlags, int roCompatFlags, int incompatFlags) {

        public byte[] encode() {
            ByteBuffer b = ByteBuffer.allocate(HEADER_SIZE);
            b.putInt(MAGIC_CHUNK);
            b.putShort((short) FORMAT_VERSION);
            b.putShort((short) HEADER_SIZE);
            chunkId.writeTo(b);
            b.put((byte) (fsyncOnAck ? 1 : 0));
            b.putInt(createWriteEpoch);
            b.putLong(createdAtMs);
            b.putInt(compatFlags).putInt(roCompatFlags).putInt(incompatFlags);
            b.put((byte) 0); // empty tagged block (varint 0)
            // remainder stays zero
            int crc = Crc.of(b.array(), 0, HEADER_SIZE - 4);
            b.putInt(HEADER_SIZE - 4, crc);
            return b.array();
        }

        public static Header decode(byte[] bytes) {
            if (bytes.length != HEADER_SIZE) throw new CorruptChunkException("bad header size " + bytes.length);
            ByteBuffer b = ByteBuffer.wrap(bytes);
            int storedCrc = b.getInt(HEADER_SIZE - 4);
            int actual = Crc.of(bytes, 0, HEADER_SIZE - 4);
            if (storedCrc != actual) throw new CorruptChunkException("header crc mismatch");
            if (b.getInt() != MAGIC_CHUNK) throw new CorruptChunkException("bad chunk magic");
            short version = b.getShort();
            if (version != FORMAT_VERSION) throw new CorruptChunkException("unsupported chunk format " + version);
            short headerSize = b.getShort();
            if (headerSize != HEADER_SIZE) throw new CorruptChunkException("bad header size field " + headerSize);
            ChunkId id = ChunkId.readFrom(b);
            boolean fsync = readBoolean(b);
            int epoch = b.getInt();
            long created = b.getLong();
            int compat = b.getInt(), roCompat = b.getInt(), incompat = b.getInt();
            if (incompat != 0) throw new CorruptChunkException("unknown incompat flags 0x" + Integer.toHexString(incompat));
            return new Header(id, fsync, epoch, created, compat, roCompat, incompat);
        }

        private static boolean readBoolean(ByteBuffer b) {
            byte value = b.get();
            if (value == 0) return false;
            if (value == 1) return true;
            throw new CorruptChunkException("bad boolean value in header: " + (value & 0xFF));
        }
    }

    /* ---------------- trailer (fixed 64B at EOF, sealed chunks only) ----------------
     * [0]  u64 dataLength
     * [8]  u64 footerStart        (file offset where the section list begins)
     * [16] u32 sectionCount
     * [20] u32 incompatFlags
     * [24] u32 footerCrc          (crc32c over [footerStart, EOF-64))
     * [28] u32 dataCrc            (crc32c over the whole data region)
     * [32..60) zero padding
     * [60] u32 magic "SFTR"
     */

    public record Trailer(long dataLength, long footerStart, int sectionCount, int incompatFlags,
                          int footerCrc, int dataCrc) {

        public byte[] encode() {
            ByteBuffer b = ByteBuffer.allocate(TRAILER_SIZE);
            b.putLong(dataLength).putLong(footerStart).putInt(sectionCount).putInt(incompatFlags)
                    .putInt(footerCrc).putInt(dataCrc);
            b.putInt(TRAILER_SIZE - 4, MAGIC_TRAILER);
            return b.array();
        }

        public static Trailer decode(byte[] bytes) {
            if (bytes.length != TRAILER_SIZE) throw new CorruptChunkException("bad trailer size");
            ByteBuffer b = ByteBuffer.wrap(bytes);
            if (b.getInt(TRAILER_SIZE - 4) != MAGIC_TRAILER) throw new CorruptChunkException("bad trailer magic");
            return new Trailer(b.getLong(), b.getLong(), b.getInt(), b.getInt(), b.getInt(), b.getInt());
        }
    }

    /* ---------------- footer sections: {u16 type, u16 version, u32 length, bytes, u32 crc} ---------------- */

    public static void writeSection(ByteBuffer out, int type, byte[] content) {
        out.putShort((short) type).putShort((short) 1).putInt(content.length);
        out.put(content);
        out.putInt(Crc.of(content));
    }

    public static int sectionSize(byte[] content) {
        return 2 + 2 + 4 + content.length + 4;
    }

    /* ---------------- sidecar .meta (512B, single-sector atomic rewrite) ----------------
     * magic u32, u16 ver, i32 writeEpoch, i32 fenceEpoch, u64 lastKnownDO, u8 state, pad, u32 crc at [508)
     */

    public record Sidecar(int writeEpoch, int fenceEpoch, long lastKnownDO, ChunkState state) {

        public byte[] encode() {
            ByteBuffer b = ByteBuffer.allocate(SIDECAR_SIZE);
            b.putInt(MAGIC_SIDECAR);
            b.putShort((short) FORMAT_VERSION);
            b.putInt(writeEpoch).putInt(fenceEpoch).putLong(lastKnownDO).put(state.value);
            int crc = Crc.of(b.array(), 0, SIDECAR_SIZE - 4);
            b.putInt(SIDECAR_SIZE - 4, crc);
            return b.array();
        }

        public static Sidecar decode(byte[] bytes) {
            if (bytes.length != SIDECAR_SIZE) throw new CorruptChunkException("bad sidecar size");
            ByteBuffer b = ByteBuffer.wrap(bytes);
            int storedCrc = b.getInt(SIDECAR_SIZE - 4);
            if (storedCrc != Crc.of(bytes, 0, SIDECAR_SIZE - 4)) throw new CorruptChunkException("sidecar crc mismatch");
            if (b.getInt() != MAGIC_SIDECAR) throw new CorruptChunkException("bad sidecar magic");
            short v = b.getShort();
            if (v != FORMAT_VERSION) throw new CorruptChunkException("unsupported sidecar version " + v);
            return new Sidecar(b.getInt(), b.getInt(), b.getLong(), ChunkState.fromValue(b.get()));
        }
    }

    /* ---------------- integrity ledger entry (24B) ----------------
     * u64 endOffset, u32 payloadCrc, i32 writeEpoch, u32 reserved=0, u32 entryCrc (over first 20 bytes)
     */

    public record LedgerEntry(long endOffset, int payloadCrc, int writeEpoch) {

        public byte[] encode() {
            ByteBuffer b = ByteBuffer.allocate(LEDGER_ENTRY_SIZE);
            encodeInto(b);
            return b.array();
        }

        /**
         * Encodes the 24-byte entry into {@code b} at its current position (heap-backed buffer),
         * so the hot append path can reuse one scratch buffer instead of allocating a byte[] plus
         * two ByteBuffer wrappers per record. Byte-identical on-disk content to {@link #encode}.
         */
        public void encodeInto(ByteBuffer b) {
            int start = b.position();
            b.putLong(endOffset).putInt(payloadCrc).putInt(writeEpoch).putInt(0);
            b.putInt(Crc.of(b.array(), b.arrayOffset() + start, LEDGER_ENTRY_SIZE - 4));
        }

        /** Returns null if the entry bytes are invalid (torn write — recovery stops here). */
        public static LedgerEntry decodeOrNull(byte[] bytes, int off) {
            ByteBuffer b = ByteBuffer.wrap(bytes, off, LEDGER_ENTRY_SIZE);
            long end = b.getLong();
            int crc = b.getInt();
            int epoch = b.getInt();
            b.getInt(); // reserved
            int entryCrc = b.getInt();
            if (entryCrc != Crc.of(bytes, off, LEDGER_ENTRY_SIZE - 4)) return null;
            return new LedgerEntry(end, crc, epoch);
        }
    }

    /* ---------------- positional file I/O shared by the format engine ---------------- */

    static void readFully(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, pos);
            if (n < 0) throw new IOException("EOF at " + pos);
            if (n == 0) throw new IOException("zero-byte read at " + pos);
            pos += n;
        }
    }

    static void writeFully(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = ch.write(buf, pos);
            if (n <= 0) throw new IOException("write failed at " + pos);
            pos += n;
        }
    }

    /* ---------------- file naming ---------------- */

    public static String baseName(ChunkId id) {
        return id.fileId().toString() + "." + id.index();
    }

    public static ChunkId parseBaseName(String base) {
        int lastDot = base.lastIndexOf('.');
        FileId f = FileId.fromString(base.substring(0, lastDot));
        return new ChunkId(f, Integer.parseInt(base.substring(lastDot + 1)));
    }
}
