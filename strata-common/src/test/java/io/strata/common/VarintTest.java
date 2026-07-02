package io.strata.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VarintTest {

    @Test
    void roundtripBoundaries() {
        long[] values = {0, 1, 127, 128, 16383, 16384, Integer.MAX_VALUE, Long.MAX_VALUE, -1L /* max unsigned */};
        ByteBuffer buf = ByteBuffer.allocate(10);
        for (long v : values) {
            buf.clear();
            Varint.writeUnsigned(buf, v);
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
        var e1 = assertThrows(IllegalArgumentException.class, () -> Varint.readBytes(huge.duplicate()));
        assertTrue(e1.getMessage().contains("length"));

        ByteBuffer overRemaining = ByteBuffer.allocate(16);
        Varint.writeUnsigned(overRemaining, 100); // claims 100 bytes, only 3 follow
        overRemaining.put(new byte[]{1, 2, 3});
        overRemaining.flip();
        var e2 = assertThrows(IllegalArgumentException.class, () -> Varint.readString(overRemaining.duplicate()));
        assertTrue(e2.getMessage().contains("length"));

        ByteBuffer negative = ByteBuffer.allocate(16);
        Varint.writeUnsigned(negative, -1L);
        negative.flip();
        var e3 = assertThrows(IllegalArgumentException.class, () -> Varint.readBytes(negative.duplicate()));
        assertTrue(e3.getMessage().contains("length"));
    }

    @Test
    void tooLongUnsignedVarintIsRejected() {
        ByteBuffer buf = ByteBuffer.allocate(11);
        for (int i = 0; i < 11; i++) {
            buf.put((byte) 0x80);
        }
        buf.flip();

        var e = assertThrows(IllegalArgumentException.class, () -> Varint.readUnsigned(buf));
        assertTrue(e.getMessage().contains("too long"));

        ByteBuffer overflowAtTenthByte = ByteBuffer.allocate(10);
        for (int i = 0; i < 9; i++) {
            overflowAtTenthByte.put((byte) 0x80);
        }
        overflowAtTenthByte.put((byte) 0x02);
        overflowAtTenthByte.flip();

        var overflow = assertThrows(IllegalArgumentException.class,
                () -> Varint.readUnsigned(overflowAtTenthByte));
        assertTrue(overflow.getMessage().contains("too long"));
    }

    @Test
    void stringsAndBytes() {
        ByteBuffer buf = ByteBuffer.allocate(256);
        String value = "strata-存储";
        Varint.writeString(buf, value);
        Varint.writeBytes(buf, new byte[]{1, 2, 3});
        buf.flip();
        assertEquals(value, Varint.readString(buf));
        Assertions.assertArrayEquals(new byte[]{1, 2, 3}, Varint.readBytes(buf));
    }
}
