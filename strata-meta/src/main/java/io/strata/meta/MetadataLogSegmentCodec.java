package io.strata.meta;

import io.strata.common.Crc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Internal framing for one metadata-log segment (design §8): each record is wrapped in a
 * {@code [frameLen:u32][crc32c:u32][recordBytes]} frame. Recovery reads frames sequentially and stops
 * at the first torn or CRC-invalid frame, yielding only the valid durable prefix — a half-written tail
 * append is never replayed (design §13 step 6).
 */
final class MetadataLogSegmentCodec {
    private MetadataLogSegmentCodec() {}

    static final int FRAME_HEADER = 8; // u32 frameLen + u32 crc32c
    /** Bounds a frame length so a torn length field cannot trigger a huge allocation. */
    static final int MAX_FRAME_BYTES = 8 * 1024 * 1024;

    static byte[] frame(byte[] recordBytes) {
        ByteBuffer buf = ByteBuffer.allocate(FRAME_HEADER + recordBytes.length);
        buf.putInt(recordBytes.length);
        buf.putInt(Crc.of(recordBytes));
        buf.put(recordBytes);
        return buf.array();
    }

    /** The valid prefix of {@code bytes}: decoded records plus the count of valid bytes consumed. */
    record Prefix(List<MetadataLogRecord> records, int validBytes) {}

    static Prefix recoverPrefix(byte[] bytes) {
        List<MetadataLogRecord> records = new ArrayList<>();
        ByteBuffer b = ByteBuffer.wrap(bytes);
        int valid = 0;
        while (b.remaining() >= FRAME_HEADER) {
            int len = b.getInt();
            if (len < 0 || len > MAX_FRAME_BYTES || b.remaining() < 4 + len) {
                break; // torn length field or truncated frame — stop at the valid prefix
            }
            int crc = b.getInt();
            byte[] rec = new byte[len];
            b.get(rec);
            if (Crc.of(rec) != crc) {
                break; // corrupt frame — stop
            }
            records.add(MetadataLogCodec.decode(rec));
            valid = b.position();
        }
        return new Prefix(records, valid);
    }

    static Prefix recoverPrefix(byte[] bytes, int skipBytes) {
        if (skipBytes <= 0) {
            return recoverPrefix(bytes);
        }
        if (skipBytes >= bytes.length) {
            return new Prefix(List.of(), bytes.length);
        }
        Prefix suffix = recoverPrefix(Arrays.copyOfRange(bytes, skipBytes, bytes.length));
        return new Prefix(suffix.records(), skipBytes + suffix.validBytes());
    }
}
