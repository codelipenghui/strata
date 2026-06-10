package io.strata.common;

import java.nio.ByteBuffer;
import java.util.UUID;

/** Identifier of a storage-layer file (tech design §3). */
public record FileId(long msb, long lsb) implements Comparable<FileId> {
    public static FileId random() {
        UUID u = UUID.randomUUID();
        return new FileId(u.getMostSignificantBits(), u.getLeastSignificantBits());
    }

    public static FileId fromString(String s) {
        UUID u = UUID.fromString(s);
        return new FileId(u.getMostSignificantBits(), u.getLeastSignificantBits());
    }

    public void writeTo(ByteBuffer buf) {
        buf.putLong(msb).putLong(lsb);
    }

    public static FileId readFrom(ByteBuffer buf) {
        return new FileId(buf.getLong(), buf.getLong());
    }

    @Override
    public String toString() {
        return new UUID(msb, lsb).toString();
    }

    @Override
    public int compareTo(FileId o) {
        int c = Long.compareUnsigned(msb, o.msb);
        return c != 0 ? c : Long.compareUnsigned(lsb, o.lsb);
    }
}
