package io.strata.common;

import java.nio.ByteBuffer;

/** Unsigned LEB128 varint codec (tech design §10.2 conventions). */
public final class Varint {
    private Varint() {}

    public static void writeUnsigned(ByteBuffer buf, long value) {
        while ((value & ~0x7FL) != 0) {
            buf.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.put((byte) value);
    }

    public static long readUnsigned(ByteBuffer buf) {
        long value = 0;
        int shift = 0;
        while (true) {
            if (shift > 63) throw new IllegalArgumentException("varint too long");
            byte b = buf.get();
            if (shift == 63 && (b & 0xFE) != 0) {
                throw new IllegalArgumentException("varint too long");
            }
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return value;
            shift += 7;
        }
    }

    public static void writeString(ByteBuffer buf, String s) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        writeUnsigned(buf, bytes.length);
        buf.put(bytes);
    }

    public static String readString(ByteBuffer buf) {
        return new String(readBytes(buf), java.nio.charset.StandardCharsets.UTF_8);
    }

    public static void writeBytes(ByteBuffer buf, byte[] b) {
        writeUnsigned(buf, b.length);
        buf.put(b);
    }

    public static byte[] readBytes(ByteBuffer buf) {
        long len = readUnsigned(buf);
        // validate BEFORE allocating: a small frame advertising a huge (or negative-after-
        // narrowing) length must be a typed protocol rejection, not heap pressure or an
        // unchecked NegativeArraySizeException
        if (len < 0 || len > buf.remaining()) {
            throw new IllegalArgumentException(
                    "bad length on wire: " + len + " (remaining " + buf.remaining() + ")");
        }
        byte[] b = new byte[(int) len];
        buf.get(b);
        return b;
    }

    /** Sanity bound for any list count on the wire — far above legitimate use. */
    private static final int MAX_COUNT = 1_000_000;

    /**
     * Reads a list count and validates it: an adversarial varint (e.g. 0xFFFFFFFF) narrows to a
     * negative int and would otherwise surface as ArrayList's "Illegal Capacity" — an unchecked
     * exception that kills the connection thread instead of producing a typed protocol error.
     */
    public static int readCount(ByteBuffer buf, String what) {
        long n = readUnsigned(buf);
        if (n < 0 || n > MAX_COUNT) {
            throw new IllegalArgumentException("bad " + what + " count on wire: " + n);
        }
        return (int) n;
    }

    /** Strict 0/1 boolean decoder — any other byte is a typed wire rejection. */
    public static boolean readBoolean(ByteBuffer buf) {
        byte value = buf.get();
        if (value == 0) {
            return false;
        }
        if (value == 1) {
            return true;
        }
        throw new IllegalArgumentException("bad boolean value on wire: " + (value & 0xFF));
    }
}
