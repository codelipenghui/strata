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
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return value;
            shift += 7;
        }
    }

    public static int sizeOfUnsigned(long value) {
        int size = 1;
        while ((value & ~0x7FL) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }

    public static void writeString(ByteBuffer buf, String s) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        writeUnsigned(buf, bytes.length);
        buf.put(bytes);
    }

    public static String readString(ByteBuffer buf) {
        int len = (int) readUnsigned(buf);
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    public static void writeBytes(ByteBuffer buf, byte[] b) {
        writeUnsigned(buf, b.length);
        buf.put(b);
    }

    public static byte[] readBytes(ByteBuffer buf) {
        int len = (int) readUnsigned(buf);
        byte[] b = new byte[len];
        buf.get(b);
        return b;
    }

    public static int sizeOfString(String s) {
        int n = s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        return sizeOfUnsigned(n) + n;
    }
}
