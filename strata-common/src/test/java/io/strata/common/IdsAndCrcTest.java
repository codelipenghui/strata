package io.strata.common;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdsAndCrcTest {

    @Test
    void fileAndChunkIdRoundtrip() {
        FileId f = FileId.random();
        ChunkId c = new ChunkId(f, 7);
        ByteBuffer buf = ByteBuffer.allocate(ChunkId.WIRE_SIZE);
        c.writeTo(buf);
        assertEquals(ChunkId.WIRE_SIZE, buf.position());
        buf.flip();
        assertEquals(c, ChunkId.readFrom(buf));
        assertEquals(f, FileId.fromString(f.toString()));
    }

    @Test
    void chunkIdRejectsInvalidValues() {
        FileId f = FileId.random();
        assertThrows(IllegalArgumentException.class, () -> new ChunkId(null, 0));
        assertThrows(IllegalArgumentException.class, () -> new ChunkId(f, -1));

        ByteBuffer negativeWireIndex = ByteBuffer.allocate(ChunkId.WIRE_SIZE);
        f.writeTo(negativeWireIndex);
        negativeWireIndex.putInt(-1).flip();
        assertThrows(IllegalArgumentException.class, () -> ChunkId.readFrom(negativeWireIndex));
    }

    @Test
    void fileAndChunkIdsCompareByParentThenIndex() {
        FileId first = new FileId(0, 1);
        FileId second = new FileId(0, 2);
        FileId laterParent = new FileId(1, 0);

        assertTrue(first.compareTo(second) < 0);
        assertTrue(laterParent.compareTo(second) > 0);
        assertEquals(0, first.compareTo(new FileId(0, 1)));

        ChunkId firstChunk = new ChunkId(first, 1);
        ChunkId secondChunk = new ChunkId(first, 2);
        ChunkId laterParentChunk = new ChunkId(laterParent, 0);

        assertTrue(firstChunk.compareTo(secondChunk) < 0);
        assertTrue(laterParentChunk.compareTo(secondChunk) > 0);
        assertEquals(0, firstChunk.compareTo(new ChunkId(first, 1)));
    }

    @Test
    void chunkStateMappingRejectsUnknownValues() {
        for (ChunkState state : ChunkState.values()) {
            assertEquals(state, ChunkState.fromValue(state.value));
        }

        var e = assertThrows(IllegalArgumentException.class, () -> ChunkState.fromValue((byte) 99));
        assertTrue(e.getMessage().contains("state"));
    }

    @Test
    void crc32cKnownVector() {
        // RFC 3720 test vector: 32 bytes of zeros -> 0x8A9136AA
        byte[] zeros = new byte[32];
        assertEquals(0x8A9136AA, Crc.of(zeros));
        assertNotEquals(Crc.of(new byte[]{1}), Crc.of(new byte[]{2}));
    }

    @Test
    void errorCodeMapping() throws Exception {
        assertEquals(ErrorCode.FENCED_EPOCH, ErrorCode.fromCode((short) 3));
        assertEquals(ErrorCode.INTERNAL, ErrorCode.fromCode((short) 999)); // unknown future code
        assertEquals(ErrorCode.INTERNAL, ErrorCode.fromCode((short) -1));
        for (ErrorCode e : ErrorCode.values()) {
            assertEquals(e, ErrorCode.fromCode(e.code));
        }

        Field byCodeField = ErrorCode.class.getDeclaredField("BY_CODE");
        byCodeField.setAccessible(true);
        ErrorCode[] byCode = (ErrorCode[]) byCodeField.get(null);
        ErrorCode original = byCode[ErrorCode.OK.code];
        try {
            byCode[ErrorCode.OK.code] = null;
            assertEquals(ErrorCode.INTERNAL, ErrorCode.fromCode(ErrorCode.OK.code));
        } finally {
            byCode[ErrorCode.OK.code] = original;
        }
    }
}
