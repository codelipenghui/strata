package io.strata.client;

import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;

import java.util.List;
import java.util.Objects;

/** Client configuration. chunkRollBytes is ~1 GB nominal in production; tests use small values. */
public record ClientConfig(List<String> metadataEndpoints, long chunkRollBytes, long callTimeoutMs,
                           ConnectionPolicy connectionPolicy, int storageConnectionsPerEndpoint) {
    public ClientConfig(List<String> metadataEndpoints, long chunkRollBytes, long callTimeoutMs) {
        this(metadataEndpoints, chunkRollBytes, callTimeoutMs, ConnectionPolicy.DEFAULT, 1);
    }

    public ClientConfig(List<String> metadataEndpoints, long chunkRollBytes, long callTimeoutMs,
                        ConnectionPolicy connectionPolicy) {
        this(metadataEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy, 1);
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
        if (storageConnectionsPerEndpoint <= 0) {
            throw new IllegalArgumentException("storageConnectionsPerEndpoint must be positive");
        }
    }

    public static ClientConfig of(String metadataEndpoint) {
        return new ClientConfig(List.of(metadataEndpoint), 1L << 30, 10_000);
    }

    public ClientConfig withChunkRollBytes(long bytes) {
        return new ClientConfig(metadataEndpoints, bytes, callTimeoutMs, connectionPolicy,
                storageConnectionsPerEndpoint);
    }

    public ClientConfig withConnectionPolicy(ConnectionPolicy policy) {
        return new ClientConfig(metadataEndpoints, chunkRollBytes, callTimeoutMs, policy,
                storageConnectionsPerEndpoint);
    }

    public ClientConfig withStorageConnectionsPerEndpoint(int connections) {
        return new ClientConfig(metadataEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy, connections);
    }

    private static void validateEndpoint(String endpoint) {
        Endpoint.parse(endpoint, "metadata endpoint");
    }
}
