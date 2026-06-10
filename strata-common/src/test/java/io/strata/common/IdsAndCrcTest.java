package io.strata.common;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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
    void crc32cKnownVector() {
        // RFC 3720 test vector: 32 bytes of zeros -> 0x8A9136AA
        byte[] zeros = new byte[32];
        assertEquals(0x8A9136AA, Crc.of(zeros));
        assertNotEquals(Crc.of(new byte[]{1}), Crc.of(new byte[]{2}));
    }

    @Test
    void errorCodeMapping() {
        assertEquals(ErrorCode.FENCED_EPOCH, ErrorCode.fromCode((short) 3));
        assertEquals(ErrorCode.INTERNAL, ErrorCode.fromCode((short) 999)); // unknown future code
        for (ErrorCode e : ErrorCode.values()) {
            assertEquals(e, ErrorCode.fromCode(e.code));
        }
    }
}
