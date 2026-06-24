package io.strata.common;

import java.util.List;
import java.util.Objects;

/**
 * Logical Strata file path within a namespace. This is not a local filesystem path: it is the
 * stable user-facing name used for lookup and, later, path-scoped authorization.
 */
public record StrataPath(String value) implements Comparable<StrataPath> {
    public static final int MAX_PATH_BYTES = 1024;
    public static final int MAX_SEGMENT_BYTES = 255;
    public static final int MAX_SEGMENTS = 64;

    public StrataPath {
        value = normalize(value);
    }

    public static StrataPath of(String value) {
        return new StrataPath(value);
    }

    @Override
    public int compareTo(StrataPath other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }

    private static String normalize(String raw) {
        Objects.requireNonNull(raw, "path");
        if (raw.isEmpty()) {
            throw new IllegalArgumentException("path must not be empty");
        }
        if (!raw.startsWith("/")) {
            throw new IllegalArgumentException("path must be absolute: " + raw);
        }
        if (raw.equals("/")) {
            throw new IllegalArgumentException("path must identify a file, not the root directory");
        }
        if (raw.endsWith("/")) {
            throw new IllegalArgumentException("path must not end with '/': " + raw);
        }
        byte[] pathBytes = raw.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (pathBytes.length > MAX_PATH_BYTES) {
            throw new IllegalArgumentException("path too long: " + pathBytes.length);
        }

        String[] segments = raw.substring(1).split("/", -1);
        if (segments.length > MAX_SEGMENTS) {
            throw new IllegalArgumentException("too many path segments: " + segments.length);
        }
        for (String segment : segments) {
            validateSegment(segment, raw);
        }
        return raw;
    }

    private static void validateSegment(String segment, String raw) {
        if (segment.isEmpty()) {
            throw new IllegalArgumentException("path must not contain empty segments: " + raw);
        }
        if (segment.equals(".") || segment.equals("..")) {
            throw new IllegalArgumentException("path must not contain relative segments: " + raw);
        }
        if (segment.equals("__file")) {
            throw new IllegalArgumentException("path segment is reserved: " + segment);
        }
        int bytes = segment.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (bytes > MAX_SEGMENT_BYTES) {
            throw new IllegalArgumentException("path segment too long: " + segment);
        }
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
            if (!ok) {
                throw new IllegalArgumentException("invalid path character '" + c + "' in " + raw);
            }
        }
    }
}
