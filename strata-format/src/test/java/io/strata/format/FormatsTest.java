package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormatsTest {

    private final ChunkId id = new ChunkId(FileId.fromString("11111111-2222-3333-4444-555555555555"), 9);

    private static void refreshHeaderCrc(byte[] bytes) {
        ByteBuffer.wrap(bytes).putInt(ChunkFormats.HEADER_SIZE - 4,
                Crc.of(bytes, 0, ChunkFormats.HEADER_SIZE - 4));
    }

    private static void refreshSidecarCrc(byte[] bytes) {
        ByteBuffer.wrap(bytes).putInt(ChunkFormats.SIDECAR_SIZE - 4,
                Crc.of(bytes, 0, ChunkFormats.SIDECAR_SIZE - 4));
    }

    @Test
    void headerRoundtripAndCrc() {
        var h = new ChunkFormats.Header(id, true, 5, 1718000000000L, 0, 0, 0);
        byte[] bytes = h.encode();
        assertEquals(ChunkFormats.HEADER_SIZE, bytes.length);
        assertEquals(h, ChunkFormats.Header.decode(bytes));

        bytes[100] ^= 1; // corrupt
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Header.decode(bytes));
    }

    @Test
    void unknownIncompatFlagRejected() {
        var h = new ChunkFormats.Header(id, false, 1, 1, 0, 0, 0x8000);
        byte[] bytes = h.encode();
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Header.decode(bytes));
    }

    @Test
    void headerSizeFieldMustMatchFormat() {
        var h = new ChunkFormats.Header(id, false, 1, 1, 0, 0, 0);
        byte[] bytes = h.encode();
        ByteBuffer.wrap(bytes).putShort(6, (short) 128);
        refreshHeaderCrc(bytes);
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Header.decode(bytes));
    }

    @Test
    void headerDecodeRejectsMalformedFixedFields() {
        var h = new ChunkFormats.Header(id, false, 1, 1, 0, 0, 0);
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Header.decode(new byte[1]));

        byte[] badMagic = h.encode();
        ByteBuffer.wrap(badMagic).putInt(0, 0);
        refreshHeaderCrc(badMagic);
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Header.decode(badMagic));

        byte[] badVersion = h.encode();
        ByteBuffer.wrap(badVersion).putShort(4, (short) 99);
        refreshHeaderCrc(badVersion);
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Header.decode(badVersion));
    }

    @Test
    void trailerRoundtrip() {
        var t = new ChunkFormats.Trailer(4096, 8192, 3, 0, 0xAA, 0xBB);
        assertEquals(t, ChunkFormats.Trailer.decode(t.encode()));
    }

    @Test
    void trailerDecodeRejectsMalformedFixedFields() {
        var t = new ChunkFormats.Trailer(4096, 8192, 3, 0, 0xAA, 0xBB);
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Trailer.decode(new byte[1]));

        byte[] badMagic = t.encode();
        ByteBuffer.wrap(badMagic).putInt(ChunkFormats.TRAILER_SIZE - 4, 0);
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Trailer.decode(badMagic));
    }

    @Test
    void sidecarRoundtripAndCrc() {
        var s = new ChunkFormats.Sidecar(5, 7, 1024, ChunkState.OPEN);
        byte[] bytes = s.encode();
        assertEquals(ChunkFormats.SIDECAR_SIZE, bytes.length);
        assertEquals(s, ChunkFormats.Sidecar.decode(bytes));
        bytes[8] ^= 1;
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Sidecar.decode(bytes));
    }

    @Test
    void sidecarDecodeRejectsMalformedFixedFields() {
        var s = new ChunkFormats.Sidecar(5, 7, 1024, ChunkState.OPEN);
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Sidecar.decode(new byte[1]));

        byte[] badMagic = s.encode();
        ByteBuffer.wrap(badMagic).putInt(0, 0);
        refreshSidecarCrc(badMagic);
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Sidecar.decode(badMagic));

        byte[] badVersion = s.encode();
        ByteBuffer.wrap(badVersion).putShort(4, (short) 99);
        refreshSidecarCrc(badVersion);
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Sidecar.decode(badVersion));
    }

    @Test
    void ledgerEntryTornWriteDetected() {
        var e = new ChunkFormats.LedgerEntry(100, 0xC, 5);
        byte[] bytes = e.encode();
        assertEquals(e, ChunkFormats.LedgerEntry.decodeOrNull(bytes, 0));
        bytes[3] ^= 1;
        assertNull(ChunkFormats.LedgerEntry.decodeOrNull(bytes, 0));
    }

    @Test
    void baseNameRoundtrip() {
        assertEquals(id, ChunkFormats.parseBaseName(ChunkFormats.baseName(id)));
    }
}
