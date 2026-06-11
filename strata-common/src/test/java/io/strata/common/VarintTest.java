package io.strata.common;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VarintTest {

    @Test
    void roundtripBoundaries() {
        long[] values = {0, 1, 127, 128, 16383, 16384, Integer.MAX_VALUE, Long.MAX_VALUE, -1L /* max unsigned */};
        ByteBuffer buf = ByteBuffer.allocate(10);
        for (long v : values) {
            buf.clear();
            Varint.writeUnsigned(buf, v);
            assertEquals(Varint.sizeOfUnsigned(v), buf.position(), "size mismatch for " + v);
            buf.flip();
            assertEquals(v, Varint.readUnsigned(buf), "roundtrip for " + v);
        }
    }

    @Test
    void roundtripRandom() {
        Random r = new Random(42);
        ByteBuffer buf = ByteBuffer.allocate(10);
        for (int i = 0; i < 10_000; i++) {
            long v = r.nextLong();
            buf.clear();
            Varint.writeUnsigned(buf, v);
            buf.flip();
            assertEquals(v, Varint.readUnsigned(buf));
        }
    }

    @Test
    void advertisedLengthIsValidatedBeforeAllocation() {
        // a tiny buffer advertising a huge (or negative-after-narrowing) length must be a typed
        // rejection, not an OOM-sized allocation or NegativeArraySizeException
        ByteBuffer huge = ByteBuffer.allocate(16);
        Varint.writeUnsigned(huge, 0xFFFFFFFFL); // narrows to -1
        huge.put((byte) 1);
        huge.flip();
        var e1 = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> Varint.readBytes(huge.duplicate()));
        org.junit.jupiter.api.Assertions.assertTrue(e1.getMessage().contains("length"));

        ByteBuffer overRemaining = ByteBuffer.allocate(16);
        Varint.writeUnsigned(overRemaining, 100); // claims 100 bytes, only 3 follow
        overRemaining.put(new byte[]{1, 2, 3});
        overRemaining.flip();
        var e2 = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> Varint.readString(overRemaining.duplicate()));
        org.junit.jupiter.api.Assertions.assertTrue(e2.getMessage().contains("length"));
    }

    @Test
    void stringsAndBytes() {
        ByteBuffer buf = ByteBuffer.allocate(256);
        Varint.writeString(buf, "strata-存储");
        Varint.writeBytes(buf, new byte[]{1, 2, 3});
        buf.flip();
        assertEquals("strata-存储", Varint.readString(buf));
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{1, 2, 3}, Varint.readBytes(buf));
    }
}
