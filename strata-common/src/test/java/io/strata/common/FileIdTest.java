package io.strata.common;

import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import static org.junit.jupiter.api.Assertions.*;

class FileIdTest {
    @Test void roundTripsAsEightBytes() {
        FileId id = FileId.of(0x0123_4567_89ab_cdefL);
        ByteBuffer b = ByteBuffer.allocate(8);
        id.writeTo(b);
        assertEquals(8, b.position());
        b.flip();
        assertEquals(id, FileId.readFrom(b));
    }
    @Test void toStringIsZeroPaddedHexAndSortsNumerically() {
        assertEquals("0000000000000001", FileId.of(1).toString());
        assertEquals("00000000000000ff", FileId.of(255).toString());
        // lexical order == numeric order
        assertTrue(FileId.of(1).toString().compareTo(FileId.of(2).toString()) < 0);
        assertEquals(FileId.of(0x1234L), FileId.fromHex("0000000000001234"));
    }
    @Test void compareIsUnsigned() {
        assertTrue(FileId.of(1).compareTo(FileId.of(-1L)) < 0); // -1 == max unsigned
    }
}
