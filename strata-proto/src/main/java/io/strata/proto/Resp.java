package io.strata.proto;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.common.Varint;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

/**
 * Response header convention (tech design §10.2): every response header begins with u16 errorCode.
 * On error: string message + tagged fields (tag 0 = u64 numeric detail, e.g. expectedEndOffset /
 * currentFenceEpoch; tag 1 = leader-endpoint hint string for NOT_LEADER redirects).
 */
public final class Resp {
    public static final int TAG_DETAIL = 0;
    public static final int TAG_LEADER_HINT = 1;

    private Resp() {}

    public static void writeOk(BufWriter w) {
        w.u16(0);
    }

    public static byte[] error(ErrorCode code, String message, long detail) {
        return error(code, message, detail, null);
    }

    public static byte[] error(ErrorCode code, String message, long detail, String leaderHint) {
        BufWriter w = new BufWriter();
        w.u16(code.code);
        w.string(message == null ? "" : message);
        TreeMap<Integer, byte[]> tags = new TreeMap<>();
        if (detail != 0) {
            BufWriter d = new BufWriter(8);
            d.u64(detail);
            tags.put(TAG_DETAIL, d.toBytes());
        }
        if (leaderHint != null && !leaderHint.isBlank()) {
            tags.put(TAG_LEADER_HINT, leaderHint.getBytes(StandardCharsets.UTF_8));
        }
        if (tags.isEmpty()) {
            w.noTags();
        } else {
            TaggedFields.of(tags).writeTo(w);
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
        byte[] hint = tags.get(TAG_LEADER_HINT);
        String leaderHint = hint != null ? new String(hint, StandardCharsets.UTF_8) : null;
        throw new ScpException(ErrorCode.fromCode(code), msg, detail, leaderHint);
    }
}
