package io.strata.common;

import java.nio.ByteBuffer;

/** Identifier of a storage-layer file, unique WITHIN a namespace (owner-assigned long). */
public record FileId(long id) implements Comparable<FileId> {
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    public static FileId of(long id) { return new FileId(id); }

    public static FileId fromHex(String s) { return new FileId(Long.parseUnsignedLong(s, 16)); }

    public void writeTo(ByteBuffer buf) { buf.putLong(id); }

    public static FileId readFrom(ByteBuffer buf) { return new FileId(buf.getLong()); }

    public static void appendHex16(StringBuilder out, long value) {
        for (int shift = 60; shift >= 0; shift -= 4) {
            out.append(HEX[(int) ((value >>> shift) & 0xF)]);
        }
    }

    public void appendHex16(StringBuilder out) {
        appendHex16(out, id);
    }

    @Override public String toString() {
        StringBuilder out = new StringBuilder(16);
        appendHex16(out);
        return out.toString();
    }

    @Override public int compareTo(FileId o) { return Long.compareUnsigned(id, o.id); }
}
