package io.strata.common;

/** Parsed host:port endpoint with bracketed IPv6 support. */
public record Endpoint(String host, int port) {
    public Endpoint {
        if (host == null || host.isBlank() || !host.equals(host.trim())) {
            throw new IllegalArgumentException("endpoint host must be non-blank");
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("endpoint port out of range: " + port);
        }
    }

    public static Endpoint parse(String endpoint) {
        return parse(endpoint, "endpoint");
    }

    /** Like {@link #parse(String, String)}, but maps rejection to a typed SCP error. */
    public static Endpoint parse(String endpoint, String field, ErrorCode errorCode) {
        try {
            return parse(endpoint, field);
        } catch (IllegalArgumentException e) {
            throw new ScpException(errorCode, "invalid " + field + ": " + endpoint);
        }
    }

    public static Endpoint parse(String endpoint, String field) {
        if (endpoint == null) {
            throw new IllegalArgumentException(field + " must be non-null");
        }
        int colon = endpoint.lastIndexOf(':');
        if (colon <= 0 || colon == endpoint.length() - 1) {
            throw new IllegalArgumentException(field + " must be host:port: " + endpoint);
        }
        String host = endpoint.substring(0, colon);
        if (host.startsWith("[") != host.endsWith("]")) {
            throw new IllegalArgumentException(field + " has unbalanced brackets: " + endpoint);
        }
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        int port;
        try {
            port = Integer.parseInt(endpoint.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(field + " port must be numeric: " + endpoint, e);
        }
        return new Endpoint(host, port);
    }
}
