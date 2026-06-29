package io.strata.common;

import java.nio.ByteBuffer;

/** Identifier of a storage-layer file, unique WITHIN a namespace (owner-assigned long). */
public record FileId(long id) implements Comparable<FileId> {
    public static FileId of(long id) { return new FileId(id); }

    public static FileId fromHex(String s) { return new FileId(Long.parseUnsignedLong(s, 16)); }

    public void writeTo(ByteBuffer buf) { buf.putLong(id); }

    public static FileId readFrom(ByteBuffer buf) { return new FileId(buf.getLong()); }

    @Override public String toString() { return String.format("%016x", id); }

    @Override public int compareTo(FileId o) { return Long.compareUnsigned(id, o.id); }
}
