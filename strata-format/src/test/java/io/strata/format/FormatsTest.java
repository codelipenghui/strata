package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormatsTest {

    private final ChunkId id = new ChunkId(FileId.fromString("11111111-2222-3333-4444-555555555555"), 9);

    @Test
    void headerRoundtripAndCrc() {
        var h = new ChunkFormats.Header(id, (byte) 0, (byte) 1, (byte) 1, 5, 1718000000000L, 0, 0, 0);
        byte[] bytes = h.encode();
        assertEquals(ChunkFormats.HEADER_SIZE, bytes.length);
        assertEquals(h, ChunkFormats.Header.decode(bytes));

        bytes[100] ^= 1; // corrupt
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Header.decode(bytes));
    }

    @Test
    void unknownIncompatFlagRejected() {
        var h = new ChunkFormats.Header(id, (byte) 0, (byte) 0, (byte) 0, 1, 1, 0, 0, 0x8000);
        byte[] bytes = h.encode();
        assertThrows(CorruptChunkException.class, () -> ChunkFormats.Header.decode(bytes));
    }

    @Test
    void trailerRoundtrip() {
        var t = new ChunkFormats.Trailer(4096, 8192, 3, 0, 0xAA, 0xBB);
        assertEquals(t, ChunkFormats.Trailer.decode(t.encode()));
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
