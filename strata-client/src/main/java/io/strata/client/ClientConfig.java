package io.strata.client;

import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;
import io.strata.proto.ScpClient;

import java.util.List;
import java.util.Objects;

/** Client configuration. chunkRollBytes is ~2 GB nominal in production; tests use small values. */
public record ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs,
                           ConnectionPolicy connectionPolicy, int dataNodeConnectionsPerEndpoint,
                           long controllerRetryDeadlineMs, int controllerRetryBackoffMs,
                           int recoveryCopyChunkBytes, int appendReplicaInflightHighWatermark,
                           int appendConnectionPendingHighWatermark, int maxChunkRecords) {
    private static final int DEFAULT_APPEND_REPLICA_INFLIGHT_HIGH_WATERMARK = 64;
    private static final int DEFAULT_APPEND_CONNECTION_PENDING_HIGH_WATERMARK =
            Math.max(1, (ScpClient.maxPendingRequests() * 3) / 4);
    private static final int DEFAULT_MAX_CHUNK_RECORDS = 262_144;

    public ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs) {
        this(controllerEndpoints, chunkRollBytes, callTimeoutMs, ConnectionPolicy.DEFAULT, 1, 15_000L, 200,
                4 * 1024 * 1024);
    }

    public ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs,
                        ConnectionPolicy connectionPolicy) {
        this(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy, 1, 15_000L, 200,
                4 * 1024 * 1024);
    }

    public ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs,
                        ConnectionPolicy connectionPolicy, int dataNodeConnectionsPerEndpoint,
                        long controllerRetryDeadlineMs, int controllerRetryBackoffMs,
                        int recoveryCopyChunkBytes) {
        this(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy, dataNodeConnectionsPerEndpoint,
                controllerRetryDeadlineMs, controllerRetryBackoffMs, recoveryCopyChunkBytes,
                DEFAULT_APPEND_REPLICA_INFLIGHT_HIGH_WATERMARK,
                DEFAULT_APPEND_CONNECTION_PENDING_HIGH_WATERMARK,
                DEFAULT_MAX_CHUNK_RECORDS);
    }

    public ClientConfig(List<String> controllerEndpoints, long chunkRollBytes, long callTimeoutMs,
                        ConnectionPolicy connectionPolicy, int dataNodeConnectionsPerEndpoint,
                        long controllerRetryDeadlineMs, int controllerRetryBackoffMs,
                        int recoveryCopyChunkBytes, int appendReplicaInflightHighWatermark,
                        int appendConnectionPendingHighWatermark) {
        this(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy, dataNodeConnectionsPerEndpoint,
                controllerRetryDeadlineMs, controllerRetryBackoffMs, recoveryCopyChunkBytes,
                appendReplicaInflightHighWatermark, appendConnectionPendingHighWatermark,
                DEFAULT_MAX_CHUNK_RECORDS);
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
        if (controllerRetryDeadlineMs <= 0) {
            throw new IllegalArgumentException("controllerRetryDeadlineMs must be positive: " + controllerRetryDeadlineMs);
        }
        if (controllerRetryBackoffMs <= 0) {
            throw new IllegalArgumentException("controllerRetryBackoffMs must be positive: " + controllerRetryBackoffMs);
        }
        if (recoveryCopyChunkBytes <= 0) {
            throw new IllegalArgumentException("recoveryCopyChunkBytes must be positive: " + recoveryCopyChunkBytes);
        }
        if (appendReplicaInflightHighWatermark <= 0) {
            throw new IllegalArgumentException("appendReplicaInflightHighWatermark must be positive: "
                    + appendReplicaInflightHighWatermark);
        }
        if (appendConnectionPendingHighWatermark <= 0) {
            throw new IllegalArgumentException("appendConnectionPendingHighWatermark must be positive: "
                    + appendConnectionPendingHighWatermark);
        }
        if (maxChunkRecords <= 0) {
            throw new IllegalArgumentException("maxChunkRecords must be positive: " + maxChunkRecords);
        }
    }

    public static ClientConfig of(String metadataEndpoint) {
        return new ClientConfig(List.of(metadataEndpoint), 2L << 30, 10_000);
    }

    public ClientConfig withChunkRollBytes(long bytes) {
        return new ClientConfig(controllerEndpoints, bytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint, controllerRetryDeadlineMs, controllerRetryBackoffMs,
                recoveryCopyChunkBytes, appendReplicaInflightHighWatermark, appendConnectionPendingHighWatermark,
                maxChunkRecords);
    }

    public ClientConfig withConnectionPolicy(ConnectionPolicy policy) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, policy,
                dataNodeConnectionsPerEndpoint, controllerRetryDeadlineMs, controllerRetryBackoffMs,
                recoveryCopyChunkBytes, appendReplicaInflightHighWatermark, appendConnectionPendingHighWatermark,
                maxChunkRecords);
    }

    public ClientConfig withDataNodeConnectionsPerEndpoint(int connections) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy,
                connections, controllerRetryDeadlineMs, controllerRetryBackoffMs, recoveryCopyChunkBytes,
                appendReplicaInflightHighWatermark, appendConnectionPendingHighWatermark, maxChunkRecords);
    }

    public ClientConfig withControllerRetryDeadlineMs(long deadlineMs) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint, deadlineMs, controllerRetryBackoffMs, recoveryCopyChunkBytes,
                appendReplicaInflightHighWatermark, appendConnectionPendingHighWatermark, maxChunkRecords);
    }

    public ClientConfig withControllerRetryBackoffMs(int backoffMs) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint, controllerRetryDeadlineMs, backoffMs, recoveryCopyChunkBytes,
                appendReplicaInflightHighWatermark, appendConnectionPendingHighWatermark, maxChunkRecords);
    }

    public ClientConfig withRecoveryCopyChunkBytes(int copyChunkBytes) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint, controllerRetryDeadlineMs, controllerRetryBackoffMs,
                copyChunkBytes, appendReplicaInflightHighWatermark, appendConnectionPendingHighWatermark,
                maxChunkRecords);
    }

    public ClientConfig withAppendWatermarks(int replicaInflightHighWatermark,
                                             int connectionPendingHighWatermark) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint, controllerRetryDeadlineMs, controllerRetryBackoffMs,
                recoveryCopyChunkBytes, replicaInflightHighWatermark, connectionPendingHighWatermark,
                maxChunkRecords);
    }

    public ClientConfig withMaxChunkRecords(int maxRecords) {
        return new ClientConfig(controllerEndpoints, chunkRollBytes, callTimeoutMs, connectionPolicy,
                dataNodeConnectionsPerEndpoint, controllerRetryDeadlineMs, controllerRetryBackoffMs,
                recoveryCopyChunkBytes, appendReplicaInflightHighWatermark, appendConnectionPendingHighWatermark,
                maxRecords);
    }

    private static void validateEndpoint(String endpoint) {
        Endpoint.parse(endpoint, "controller endpoint");
    }
}
