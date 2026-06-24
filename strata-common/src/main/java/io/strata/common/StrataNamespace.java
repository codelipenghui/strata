package io.strata.common;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Top-level Strata namespace. A namespace is the tenant/application root for logical file paths
 * and is the natural future boundary for ACLs, quotas, and per-cluster administration.
 */
public record StrataNamespace(String value) implements Comparable<StrataNamespace> {
    public static final int MAX_BYTES = 255;

    public StrataNamespace {
        value = validate(value);
    }

    public static StrataNamespace of(String value) {
        return new StrataNamespace(value);
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
        int bytes = raw.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_BYTES) {
            throw new IllegalArgumentException("namespace too long: " + bytes);
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
}
