package io.strata.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Top-level Strata namespace. A namespace is the tenant/application root for logical file paths
 * and is the natural future boundary for ACLs, quotas, and per-cluster administration.
 */
public record StrataNamespace(String value) implements Comparable<StrataNamespace> {
    public static final int MAX_BYTES = 255;
    private static final int DECODE_CACHE_SLOTS = 16;
    private static final ThreadLocal<DecodeCache> DECODE_CACHE = ThreadLocal.withInitial(DecodeCache::new);

    @FunctionalInterface
    public interface AsciiBytes {
        byte byteAt(int index);
    }

    public StrataNamespace {
        value = validate(value);
    }

    public static StrataNamespace of(String value) {
        return new StrataNamespace(value);
    }

    /**
     * Reads one namespace from the SCP wire format. A tiny per-thread cache avoids rebuilding and
     * revalidating the same namespace on every APPEND/READ header while keeping cardinality bounded.
     */
    public static StrataNamespace readFrom(ByteBuffer buf) {
        long lenLong = Varint.readUnsigned(buf);
        if (lenLong > buf.remaining()) {
            throw new IllegalArgumentException(
                    "bad namespace length on wire: " + lenLong + " (remaining " + buf.remaining() + ")");
        }
        if (lenLong > MAX_BYTES) {
            throw new IllegalArgumentException("namespace too long: " + lenLong);
        }
        int len = (int) lenLong;
        int pos = buf.position();
        DecodeCache cache = DECODE_CACHE.get();
        StrataNamespace cached = cache.get(buf, pos, len);
        if (cached != null) {
            buf.position(pos + len);
            return cached;
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        StrataNamespace namespace = of(new String(bytes, StandardCharsets.US_ASCII));
        cache.put(bytes, namespace);
        return namespace;
    }

    public static StrataNamespace readFrom(int len, AsciiBytes source) {
        if (len < 0) {
            throw new IllegalArgumentException("bad namespace length on wire: " + len);
        }
        if (len > MAX_BYTES) {
            throw new IllegalArgumentException("namespace too long: " + len);
        }
        DecodeCache cache = DECODE_CACHE.get();
        StrataNamespace cached = cache.get(source, len);
        if (cached != null) {
            return cached;
        }
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = source.byteAt(i);
        }
        StrataNamespace namespace = of(new String(bytes, StandardCharsets.US_ASCII));
        cache.put(bytes, namespace);
        return namespace;
    }

    @Override
    public int compareTo(StrataNamespace other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }

    private static String validate(String raw) {
        Objects.requireNonNull(raw, "namespace");
        if (raw.isEmpty()) {
            throw new IllegalArgumentException("namespace must not be empty");
        }
        // Names.isNameChar is ASCII-only, so a valid namespace's UTF-8 byte length equals char length.
        if (raw.length() > MAX_BYTES) {
            throw new IllegalArgumentException("namespace too long: " + raw.length());
        }
        if (raw.equals(".") || raw.equals("..")) {
            throw new IllegalArgumentException("namespace must not be . or ..");
        }
        if (raw.equals("__file") || raw.startsWith("__")) {
            throw new IllegalArgumentException("namespace is reserved: " + raw);
        }
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (!Names.isNameChar(c)) {
                throw new IllegalArgumentException("namespace has unsafe char '" + c + "': " + raw);
            }
        }
        return raw;
    }

    private static final class DecodeCache {
        private final byte[][] keys = new byte[DECODE_CACHE_SLOTS][];
        private final StrataNamespace[] values = new StrataNamespace[DECODE_CACHE_SLOTS];
        private int next;

        StrataNamespace get(ByteBuffer buf, int pos, int len) {
            for (int slot = 0; slot < keys.length; slot++) {
                byte[] key = keys[slot];
                if (key != null && key.length == len && matches(key, buf, pos)) {
                    return values[slot];
                }
            }
            return null;
        }

        StrataNamespace get(AsciiBytes source, int len) {
            for (int slot = 0; slot < keys.length; slot++) {
                byte[] key = keys[slot];
                if (key != null && key.length == len && matches(key, source)) {
                    return values[slot];
                }
            }
            return null;
        }

        void put(byte[] key, StrataNamespace value) {
            int slot = next++ & (DECODE_CACHE_SLOTS - 1);
            keys[slot] = key;
            values[slot] = value;
        }

        private static boolean matches(byte[] key, ByteBuffer buf, int pos) {
            for (int i = 0; i < key.length; i++) {
                if (buf.get(pos + i) != key[i]) {
                    return false;
                }
            }
            return true;
        }

        private static boolean matches(byte[] key, AsciiBytes source) {
            for (int i = 0; i < key.length; i++) {
                if (source.byteAt(i) != key[i]) {
                    return false;
                }
            }
            return true;
        }
    }
}
