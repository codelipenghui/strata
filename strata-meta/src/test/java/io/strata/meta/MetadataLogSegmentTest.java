package io.strata.meta;

import io.strata.common.FileId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataLogSegmentTest {

    private static final FileId F = FileId.of(1);

    private static MetadataLogRecord rec(int i) {
        return new MetadataLogRecord.ChunkSealed(F, i, 4096, 0xABCD0000 + i, 1, List.of(1, 2, 3));
    }

    @Test
    void appendAdvancesOffsetAndRecoversAllRecords() {
        MetadataLogSegment seg = new MetadataLogSegment(100);
        assertEquals(100, seg.endOffset());
        long o0 = seg.append(rec(0));
        long o1 = seg.append(rec(1));
        long o2 = seg.append(rec(2));
        assertTrue(o0 < o1 && o1 < o2, "offsets advance monotonically");
        assertEquals(o2, seg.endOffset());
        assertEquals(3, seg.recordCount());

        MetadataLogSegment recovered = MetadataLogSegment.recover(100, seg.toBytes());
        assertEquals(seg.records(), recovered.records());
        assertEquals(seg.endOffset(), recovered.endOffset());
        assertArrayEquals(seg.toBytes(), recovered.toBytes());
    }

    @Test
    void recoverDiscardsATornTailAppend() {
        MetadataLogSegment seg = new MetadataLogSegment(0);
        seg.append(rec(0));
        seg.append(rec(1));
        byte[] good = seg.toBytes();
        // a torn write left a frame header claiming an absurd length past the valid prefix
        byte[] torn = Arrays.copyOf(good, good.length + 12);
        ByteBuffer.wrap(torn, good.length, 4).putInt(Integer.MAX_VALUE);

        MetadataLogSegment recovered = MetadataLogSegment.recover(0, torn);
        assertEquals(seg.records(), recovered.records(), "only the valid prefix is replayed");
        assertEquals(good.length, recovered.byteLength(), "durable end is the valid prefix length");
    }

    @Test
    void recoverStopsAtAShortTrailingFragment() {
        MetadataLogSegment seg = new MetadataLogSegment(0);
        seg.append(rec(0));
        byte[] good = seg.toBytes();
        byte[] torn = Arrays.copyOf(good, good.length + 3); // fewer than a frame header

        MetadataLogSegment recovered = MetadataLogSegment.recover(0, torn);
        assertEquals(1, recovered.recordCount());
        assertEquals(good.length, recovered.byteLength());
    }

    @Test
    void recoverStopsAtCrcCorruption() {
        MetadataLogSegment seg = new MetadataLogSegment(0);
        seg.append(rec(0));
        seg.append(rec(1));
        byte[] bytes = seg.toBytes();
        bytes[bytes.length - 1] ^= 0xFF; // corrupt a byte inside the second frame's record

        MetadataLogSegment recovered = MetadataLogSegment.recover(0, bytes);
        assertEquals(1, recovered.recordCount(), "CRC mismatch truncates at the last good record");
        assertEquals(rec(0), recovered.records().get(0));
    }

    @Test
    void emptyBytesRecoverToAnEmptySegmentAtBaseOffset() {
        MetadataLogSegment recovered = MetadataLogSegment.recover(42, new byte[0]);
        assertEquals(0, recovered.recordCount());
        assertEquals(42, recovered.endOffset());
    }
}
