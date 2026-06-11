package io.strata.client;

import java.util.List;

/** Client configuration. chunkRollBytes is ~1 GB nominal in production; tests use small values. */
public record ClientConfig(List<String> metadataEndpoints, long chunkRollBytes, long callTimeoutMs) {
    public ClientConfig {
        if (metadataEndpoints == null || metadataEndpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one metadata endpoint is required");
        }
        for (String endpoint : metadataEndpoints) {
            validateEndpoint(endpoint);
        }
        metadataEndpoints = List.copyOf(metadataEndpoints);
        if (chunkRollBytes <= 0) {
            throw new IllegalArgumentException("chunkRollBytes must be positive");
        }
        if (callTimeoutMs <= 0) {
            throw new IllegalArgumentException("callTimeoutMs must be positive");
        }
    }

    public static ClientConfig of(String metadataEndpoint) {
        return new ClientConfig(List.of(metadataEndpoint), 1L << 30, 10_000);
    }

    public ClientConfig withChunkRollBytes(long bytes) {
        return new ClientConfig(metadataEndpoints, bytes, callTimeoutMs);
    }

    private static void validateEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("metadata endpoint must not be blank");
        }
        int colon = endpoint.lastIndexOf(':');
        if (colon <= 0 || colon == endpoint.length() - 1) {
            throw new IllegalArgumentException("metadata endpoint must be host:port: " + endpoint);
        }
        String host = endpoint.substring(0, colon);
        if (host.startsWith("[") != host.endsWith("]")) {
            throw new IllegalArgumentException("metadata endpoint has unbalanced brackets: " + endpoint);
        }
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        if (host.isBlank() || !host.equals(host.trim())) {
            throw new IllegalArgumentException("metadata endpoint host must be non-blank: " + endpoint);
        }
        int port;
        try {
            port = Integer.parseInt(endpoint.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("metadata endpoint port must be numeric: " + endpoint, e);
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("metadata endpoint port out of range: " + endpoint);
        }
    }
}
