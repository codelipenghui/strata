package io.strata.proto;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.common.Varint;

import java.nio.ByteBuffer;

/**
 * Response header convention (tech design §10.2): every response header begins with u16 errorCode.
 * On error: string message + tagged fields (tag 0 = u64 numeric detail, e.g. expectedEndOffset / currentFenceEpoch).
 */
public final class Resp {
    public static final int TAG_DETAIL = 0;

    private Resp() {}

    public static void writeOk(BufWriter w) {
        w.u16(0);
    }

    public static byte[] error(ErrorCode code, String message, long detail) {
        BufWriter w = new BufWriter();
        w.u16(code.code);
        w.string(message == null ? "" : message);
        if (detail != 0) {
            BufWriter d = new BufWriter(8);
            d.u64(detail);
            TaggedFields.of(java.util.Map.of(TAG_DETAIL, d.toBytes())).writeTo(w);
        } else {
            w.noTags();
        }
        return w.toBytes();
    }

    /** Reads the error header; throws on non-OK; on OK leaves buf positioned at the success fields. */
    public static void check(ByteBuffer buf) {
        short code = buf.getShort();
        if (code == 0) return;
        String msg = Varint.readString(buf);
        TaggedFields tags = TaggedFields.readFrom(buf);
        long detail = 0;
        byte[] d = tags.get(TAG_DETAIL);
        if (d != null && d.length == 8) detail = ByteBuffer.wrap(d).getLong();
        throw new ScpException(ErrorCode.fromCode(code), msg, detail);
    }
}
