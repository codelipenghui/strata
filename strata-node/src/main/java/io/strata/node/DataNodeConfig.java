package io.strata.node;

import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;

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
        int inventoryIntervalMs,
        ConnectionPolicy connectionPolicy,
        int nodeId                       // -1 = standalone/unregistered; otherwise >= 1
) {
    public DataNodeConfig(Path dataDir, int listenPort, String advertisedHost, String advertisedEndpointOverride,
                      List<String> controllerEndpoints, String zone, String rack, String host,
                      long capacityBytes, int inventoryIntervalMs) {
        this(dataDir, listenPort, advertisedHost, advertisedEndpointOverride, controllerEndpoints,
                zone, rack, host, capacityBytes, inventoryIntervalMs, ConnectionPolicy.DEFAULT, -1);
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
        if (inventoryIntervalMs <= 0) {
            throw new IllegalArgumentException("inventoryIntervalMs must be positive: " + inventoryIntervalMs);
        }
        if (nodeId != -1 && nodeId < 1) {
            throw new IllegalArgumentException("nodeId must be -1 (standalone) or >= 1: " + nodeId);
        }
        connectionPolicy = Objects.requireNonNull(connectionPolicy, "connectionPolicy");
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
                host, 1L << 40, 5_000);
    }

    public DataNodeConfig withListenPort(int port) {
        return new DataNodeConfig(dataDir, port, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, inventoryIntervalMs, connectionPolicy, nodeId);
    }

    public DataNodeConfig withAdvertisedEndpoint(String endpoint) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, endpoint,
                controllerEndpoints, zone, rack, host, capacityBytes, inventoryIntervalMs, connectionPolicy, nodeId);
    }

    public DataNodeConfig withConnectionPolicy(ConnectionPolicy policy) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, inventoryIntervalMs, policy, nodeId);
    }

    public DataNodeConfig withNodeId(int id) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, inventoryIntervalMs, connectionPolicy, id);
    }
}
