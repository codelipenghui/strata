package io.strata.client;

import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;

import java.util.List;
import java.util.Objects;

/** Client configuration. chunkRollBytes is ~2 GB nominal in production; tests use small values. */
public record ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs,
                           ConnectionPolicy connectionPolicy, int dataNodeConnectionsPerEndpoint,
                           long controllerRetryDeadlineMs, int controllerRetryBackoffMs,
                           int recoveryCopyChunkBytes) {
    public ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs) {
        this(controllerEndpoints, chunkRollBytes, callTimeoutMs, ConnectionPolicy.DEFAULT, 1, 0L, 0, 0);
    }

    public ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs,
                        ConnectionPolicy connectionPolicy) {
        this(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy, 1, 0L, 0, 0);
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
        if (controllerRetryDeadlineMs <= 0) { controllerRetryDeadlineMs = 15_000L; }
        if (controllerRetryBackoffMs <= 0) { controllerRetryBackoffMs = 200; }
        if (recoveryCopyChunkBytes <= 0) { recoveryCopyChunkBytes = 4 * 1024 * 1024; }
    }

    public static ClientConfig of(String metadataEndpoint) {
        return new ClientConfig(List.of(metadataEndpoint), 2L << 30, 10_000, ConnectionPolicy.DEFAULT, 1, 0L, 0, 0);
    }

    public ClientConfig withChunkRollBytes(long bytes) {
        return new ClientConfig(controllerEndpoints, bytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint, controllerRetryDeadlineMs, controllerRetryBackoffMs,
                recoveryCopyChunkBytes);
    }

    public ClientConfig withConnectionPolicy(ConnectionPolicy policy) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, policy,
                dataNodeConnectionsPerEndpoint, controllerRetryDeadlineMs, controllerRetryBackoffMs,
                recoveryCopyChunkBytes);
    }

    public ClientConfig withDataNodeConnectionsPerEndpoint(int connections) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy,
                connections, controllerRetryDeadlineMs, controllerRetryBackoffMs, recoveryCopyChunkBytes);
    }

    public ClientConfig withControllerRetryDeadlineMs(long deadlineMs) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint, deadlineMs, controllerRetryBackoffMs, recoveryCopyChunkBytes);
    }

    public ClientConfig withControllerRetryBackoffMs(int backoffMs) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint, controllerRetryDeadlineMs, backoffMs, recoveryCopyChunkBytes);
    }

    public ClientConfig withRecoveryCopyChunkBytes(int copyChunkBytes) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint, controllerRetryDeadlineMs, controllerRetryBackoffMs,
                copyChunkBytes);
    }

    private static void validateEndpoint(String endpoint) {
        Endpoint.parse(endpoint, "controller endpoint");
    }
}
