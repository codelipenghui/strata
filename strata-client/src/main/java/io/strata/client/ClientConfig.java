package io.strata.client;

import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;

import java.util.List;
import java.util.Objects;

/** Client configuration. chunkRollBytes is ~1 GB nominal in production; tests use small values. */
public record ClientConfig(List<String> metadataEndpoints, long chunkRollBytes, long callTimeoutMs,
                           ConnectionPolicy connectionPolicy) {
    public ClientConfig(List<String> metadataEndpoints, long chunkRollBytes, long callTimeoutMs) {
        this(metadataEndpoints, chunkRollBytes, callTimeoutMs, ConnectionPolicy.DEFAULT);
    }

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
        connectionPolicy = Objects.requireNonNull(connectionPolicy, "connectionPolicy");
    }

    public static ClientConfig of(String metadataEndpoint) {
        return new ClientConfig(List.of(metadataEndpoint), 1L << 30, 10_000, ConnectionPolicy.DEFAULT);
    }

    public ClientConfig withChunkRollBytes(long bytes) {
        return new ClientConfig(metadataEndpoints, bytes, callTimeoutMs, connectionPolicy);
    }

    public ClientConfig withConnectionPolicy(ConnectionPolicy policy) {
        return new ClientConfig(metadataEndpoints, chunkRollBytes, callTimeoutMs, policy);
    }

    private static void validateEndpoint(String endpoint) {
        Endpoint.parse(endpoint, "metadata endpoint");
    }
}
