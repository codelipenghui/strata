package io.strata.common;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/** CRC32C (Castagnoli) helpers — the only checksum in the system (tech design §10.2). */
public final class Crc {
    private Crc() {}

    private static final ThreadLocal<CRC32C> CRC = ThreadLocal.withInitial(CRC32C::new);

    public static int of(ByteBuffer buf) {
        CRC32C crc = CRC.get();
        crc.reset();
        crc.update(buf.duplicate());
        return (int) crc.getValue();
    }

    public static int of(byte[] bytes, int off, int len) {
        CRC32C crc = CRC.get();
        crc.reset();
        crc.update(bytes, off, len);
        return (int) crc.getValue();
    }

    public static int of(byte[] bytes) {
        return of(bytes, 0, bytes.length);
    }
}
