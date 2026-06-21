package io.strata.client;

import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;

import java.util.List;
import java.util.Objects;

/** Client configuration. chunkRollBytes is ~2 GB nominal in production; tests use small values. */
public record ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs,
                           ConnectionPolicy connectionPolicy, int dataNodeConnectionsPerEndpoint) {
    public ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs) {
        this(controllerEndpoints, chunkRollBytes, callTimeoutMs, ConnectionPolicy.DEFAULT, 1);
    }

    public ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs,
                        ConnectionPolicy connectionPolicy) {
        this(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy, 1);
    }

    public ClientConfig {
        if (controllerEndpoints == null || controllerEndpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one controller endpoint is required");
        }
        for (String endpoint : controllerEndpoints) {
            validateEndpoint(endpoint);
        }
        controllerEndpoints = List.copyOf(controllerEndpoints);
        if (chunkRollBytes <= 0) {
            throw new IllegalArgumentException("chunkRollBytes must be positive");
        }
        if (callTimeoutMs <= 0) {
            throw new IllegalArgumentException("callTimeoutMs must be positive");
        }
        connectionPolicy = Objects.requireNonNull(connectionPolicy, "connectionPolicy");
        if (dataNodeConnectionsPerEndpoint <= 0) {
            throw new IllegalArgumentException("dataNodeConnectionsPerEndpoint must be positive");
        }
    }

    public static ClientConfig of(String metadataEndpoint) {
        return new ClientConfig(List.of(metadataEndpoint), 2L << 30, 10_000); // 2 GiB nominal chunk roll
    }

    public ClientConfig withChunkRollBytes(long bytes) {
        return new ClientConfig(controllerEndpoints, bytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint);
    }

    public ClientConfig withConnectionPolicy(ConnectionPolicy policy) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, policy,
                dataNodeConnectionsPerEndpoint);
    }

    public ClientConfig withDataNodeConnectionsPerEndpoint(int connections) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy, connections);
    }

    private static void validateEndpoint(String endpoint) {
        Endpoint.parse(endpoint, "controller endpoint");
    }
}
