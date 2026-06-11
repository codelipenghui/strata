package io.strata.proto;

import io.strata.common.Varint;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tagged-field block (tech design §10.2): varint count, then per field {varint tag, varint size, bytes}.
 * Unknown tags are preserved on read and ignored by consumers — additive evolution path.
 */
public final class TaggedFields {
    public static final TaggedFields EMPTY = new TaggedFields(new TreeMap<>());

    private final TreeMap<Integer, byte[]> fields;

    private TaggedFields(TreeMap<Integer, byte[]> fields) {
        this.fields = fields;
    }

    public static TaggedFields of(Map<Integer, byte[]> m) {
        return new TaggedFields(new TreeMap<>(m));
    }

    public byte[] get(int tag) {
        return fields.get(tag);
    }

    public void writeTo(BufWriter w) {
        w.varint(fields.size());
        for (Map.Entry<Integer, byte[]> e : fields.entrySet()) {
            w.varint(e.getKey());
            w.varint(e.getValue().length);
            w.raw(e.getValue());
        }
    }

    public static TaggedFields readFrom(ByteBuffer buf) {
        long n = Varint.readUnsigned(buf);
        if (n == 0) return EMPTY;
        if (n < 0 || n > 1024) {
            throw new IllegalArgumentException("bad tagged-field count: " + n);
        }
        TreeMap<Integer, byte[]> m = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            int tag = (int) Varint.readUnsigned(buf);
            long size = Varint.readUnsigned(buf);
            if (size < 0 || size > buf.remaining()) {
                throw new IllegalArgumentException("bad tagged-field size: " + size);
            }
            byte[] b = new byte[(int) size];
            buf.get(b);
            m.put(tag, b);
        }
        return new TaggedFields(m);
    }
}
