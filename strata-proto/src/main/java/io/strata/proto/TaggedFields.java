package io.strata.proto;

import io.strata.common.Varint;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
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
        TreeMap<Integer, byte[]> copy = new TreeMap<>();
        for (Map.Entry<Integer, byte[]> e : m.entrySet()) {
            int tag = Objects.requireNonNull(e.getKey(), "tag");
            validateTag(tag);
            copy.put(tag, Objects.requireNonNull(e.getValue(), "field value").clone());
        }
        return new TaggedFields(copy);
    }

    public byte[] get(int tag) {
        byte[] value = fields.get(tag);
        return value == null ? null : value.clone();
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
        if (n == 0) {
            requireEndOfBlock(buf);
            return EMPTY;
        }
        if (n < 0 || n > 1024) {
            throw new IllegalArgumentException("bad tagged-field count: " + n);
        }
        TreeMap<Integer, byte[]> m = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            long tagValue = Varint.readUnsigned(buf);
            validateTag(tagValue);
            int tag = (int) tagValue;
            long size = Varint.readUnsigned(buf);
            if (size < 0 || size > buf.remaining()) {
                throw new IllegalArgumentException("bad tagged-field size: " + size);
            }
            byte[] b = new byte[(int) size];
            buf.get(b);
            m.put(tag, b);
        }
        requireEndOfBlock(buf);
        return new TaggedFields(m);
    }

    private static void validateTag(long tag) {
        if (tag < 0 || tag > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("bad tagged-field tag: " + tag);
        }
    }

    private static void requireEndOfBlock(ByteBuffer buf) {
        if (buf.hasRemaining()) {
            throw new IllegalArgumentException("trailing bytes after tagged fields: " + buf.remaining());
        }
    }
}
