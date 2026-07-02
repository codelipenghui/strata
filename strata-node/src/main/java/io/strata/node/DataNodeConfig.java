package io.strata.node;

import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;
import io.strata.format.ChunkStoreConfig;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Data node configuration. controllerEndpoints empty = standalone mode (no registration loop) —
 * used by data-plane tests; production always registers.
 *
 * <p>{@code nodeId} is the externally-supplied, volume-bound node identity (set via
 * {@code STRATA_NODE_ID}); {@code -1} means standalone/unregistered (data-plane tests). The id is
 * validated against the volume's {@code identity.properties} on startup.
 */
public record DataNodeConfig(
        Path dataDir,
        int listenPort,                 // 0 = ephemeral
        String advertisedHost,
        String advertisedEndpointOverride, // non-null: register THIS endpoint (chaos proxying)
        List<String> controllerEndpoints, // "host:port"
        String zone,
        String rack,
        String host,
        long capacityBytes,
        int scrubIntervalMs,             // cadence of the node-local sealed-chunk re-CRC scrub (design §20.3)
        ConnectionPolicy connectionPolicy,
        int nodeId,                      // -1 = standalone/unregistered; otherwise >= 1
        long orphanGraceMs,
        long orphanScanIntervalMs,
        long orphanStartupGraceMs,
        int orphanConfirmTimeoutMs,
        int controlCallTimeoutMs,
        int repairFetchBytes,
        int deleteMaxConcurrent,
        long deleteMinIntervalMs,
        ChunkStoreConfig chunkStoreConfig
) {
    public DataNodeConfig(Path dataDir, int listenPort, String advertisedHost, String advertisedEndpointOverride,
                      List<String> controllerEndpoints, String zone, String rack, String host,
                      long capacityBytes, int scrubIntervalMs) {
        this(dataDir, listenPort, advertisedHost, advertisedEndpointOverride, controllerEndpoints,
                zone, rack, host, capacityBytes, scrubIntervalMs, ConnectionPolicy.DEFAULT, -1,
                6_000L, 3_000L, 6_000L, 5_000, 10_000, 4 * 1024 * 1024, 1, 50L,
                ChunkStoreConfig.DEFAULT);
    }

    public DataNodeConfig {
        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir must be non-null");
        }
        if (controllerEndpoints == null) {
            throw new IllegalArgumentException("controllerEndpoints must be non-null");
        }
        requireText(advertisedHost, "advertisedHost");
        requireText(zone, "zone");
        requireText(rack, "rack");
        requireText(host, "host");
        if (advertisedEndpointOverride != null) {
            validateEndpoint(advertisedEndpointOverride, "advertisedEndpointOverride");
        }
        for (String endpoint : controllerEndpoints) {
            validateEndpoint(endpoint, "controller endpoint");
        }
        controllerEndpoints = List.copyOf(controllerEndpoints);
        if (listenPort < 0 || listenPort > 65_535) {
            throw new IllegalArgumentException("listenPort must be 0..65535: " + listenPort);
        }
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("capacityBytes must be positive: " + capacityBytes);
        }
        if (scrubIntervalMs <= 0) {
            throw new IllegalArgumentException("scrubIntervalMs must be positive: " + scrubIntervalMs);
        }
        if (nodeId != -1 && nodeId < 1) {
            throw new IllegalArgumentException("nodeId must be -1 (standalone) or >= 1: " + nodeId);
        }
        connectionPolicy = Objects.requireNonNull(connectionPolicy, "connectionPolicy");
        if (orphanGraceMs <= 0) {
            throw new IllegalArgumentException("orphanGraceMs must be positive: " + orphanGraceMs);
        }
        if (orphanScanIntervalMs <= 0) {
            throw new IllegalArgumentException("orphanScanIntervalMs must be positive: " + orphanScanIntervalMs);
        }
        if (orphanStartupGraceMs <= 0) {
            throw new IllegalArgumentException("orphanStartupGraceMs must be positive: " + orphanStartupGraceMs);
        }
        if (orphanConfirmTimeoutMs <= 0) {
            throw new IllegalArgumentException("orphanConfirmTimeoutMs must be positive: " + orphanConfirmTimeoutMs);
        }
        if (controlCallTimeoutMs <= 0) {
            throw new IllegalArgumentException("controlCallTimeoutMs must be positive: " + controlCallTimeoutMs);
        }
        if (repairFetchBytes <= 0) {
            throw new IllegalArgumentException("repairFetchBytes must be positive: " + repairFetchBytes);
        }
        if (deleteMaxConcurrent <= 0) {
            throw new IllegalArgumentException("deleteMaxConcurrent must be positive: " + deleteMaxConcurrent);
        }
        if (deleteMinIntervalMs < 0) {
            throw new IllegalArgumentException("deleteMinIntervalMs must be non-negative: " + deleteMinIntervalMs);
        }
        chunkStoreConfig = Objects.requireNonNull(chunkStoreConfig, "chunkStoreConfig");
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must be non-blank");
        }
    }

    private static void validateEndpoint(String endpoint, String field) {
        Endpoint.parse(endpoint, field);
    }

    public static DataNodeConfig standalone(Path dataDir) {
        return new DataNodeConfig(dataDir, 0, "127.0.0.1", null, List.of(), "z0", "r0",
                "h-" + dataDir.getFileName(), 1L << 40, 60_000);
    }

    public static DataNodeConfig withMetadata(Path dataDir, List<String> controllerEndpoints, String host) {
        return new DataNodeConfig(dataDir, 0, "127.0.0.1", null, controllerEndpoints, "z0", "r0",
                host, 1L << 40, 30_000);
    }

    public DataNodeConfig withListenPort(int port) {
        return new DataNodeConfig(dataDir, port, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, repairFetchBytes, deleteMaxConcurrent, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withAdvertisedEndpoint(String endpoint) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, endpoint,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, repairFetchBytes, deleteMaxConcurrent, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withConnectionPolicy(ConnectionPolicy policy) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, policy, nodeId,
                orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, repairFetchBytes, deleteMaxConcurrent, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withNodeId(int id) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, id,
                orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, repairFetchBytes, deleteMaxConcurrent, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withOrphanGraceMs(long v) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                v, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, repairFetchBytes, deleteMaxConcurrent, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withOrphanScanIntervalMs(long v) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                orphanGraceMs, v, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, repairFetchBytes, deleteMaxConcurrent, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withOrphanStartupGraceMs(long v) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                orphanGraceMs, orphanScanIntervalMs, v, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, repairFetchBytes, deleteMaxConcurrent, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withOrphanConfirmTimeoutMs(int v) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, v,
                controlCallTimeoutMs, repairFetchBytes, deleteMaxConcurrent, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withControlCallTimeoutMs(int v) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                v, repairFetchBytes, deleteMaxConcurrent, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withRepairFetchBytes(int v) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, v, deleteMaxConcurrent, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withDeleteMaxConcurrent(int v) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, repairFetchBytes, v, deleteMinIntervalMs, chunkStoreConfig);
    }

    public DataNodeConfig withDeleteMinIntervalMs(long v) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, repairFetchBytes, deleteMaxConcurrent, v, chunkStoreConfig);
    }

    public DataNodeConfig withChunkStoreConfig(ChunkStoreConfig v) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs,
                controlCallTimeoutMs, repairFetchBytes, deleteMaxConcurrent, deleteMinIntervalMs, v);
    }
}
