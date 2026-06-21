package io.strata.node;

import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Storage node configuration. metadataEndpoints empty = standalone mode (no registration loop) —
 * used by data-plane tests; production always registers.
 */
public record NodeConfig(
        Path dataDir,
        int listenPort,                 // 0 = ephemeral
        String advertisedHost,
        String advertisedEndpointOverride, // non-null: register THIS endpoint (chaos proxying)
        List<String> metadataEndpoints, // "host:port"
        String zone,
        String rack,
        String host,
        long capacityBytes,
        int inventoryIntervalMs,
        ConnectionPolicy connectionPolicy
) {
    public NodeConfig(Path dataDir, int listenPort, String advertisedHost, String advertisedEndpointOverride,
                      List<String> metadataEndpoints, String zone, String rack, String host,
                      long capacityBytes, int inventoryIntervalMs) {
        this(dataDir, listenPort, advertisedHost, advertisedEndpointOverride, metadataEndpoints,
                zone, rack, host, capacityBytes, inventoryIntervalMs, ConnectionPolicy.DEFAULT);
    }

    public NodeConfig {
        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir must be non-null");
        }
        if (metadataEndpoints == null) {
            throw new IllegalArgumentException("metadataEndpoints must be non-null");
        }
        requireText(advertisedHost, "advertisedHost");
        requireText(zone, "zone");
        requireText(rack, "rack");
        requireText(host, "host");
        if (advertisedEndpointOverride != null) {
            validateEndpoint(advertisedEndpointOverride, "advertisedEndpointOverride");
        }
        for (String endpoint : metadataEndpoints) {
            validateEndpoint(endpoint, "metadata endpoint");
        }
        metadataEndpoints = List.copyOf(metadataEndpoints);
        if (listenPort < 0 || listenPort > 65_535) {
            throw new IllegalArgumentException("listenPort must be 0..65535: " + listenPort);
        }
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("capacityBytes must be positive: " + capacityBytes);
        }
        if (inventoryIntervalMs <= 0) {
            throw new IllegalArgumentException("inventoryIntervalMs must be positive: " + inventoryIntervalMs);
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

    public static NodeConfig standalone(Path dataDir) {
        return new NodeConfig(dataDir, 0, "127.0.0.1", null, List.of(), "z0", "r0",
                "h-" + dataDir.getFileName(), 1L << 40, 60_000);
    }

    public static NodeConfig withMetadata(Path dataDir, List<String> metadataEndpoints, String host) {
        return new NodeConfig(dataDir, 0, "127.0.0.1", null, metadataEndpoints, "z0", "r0",
                host, 1L << 40, 5_000);
    }

    public NodeConfig withListenPort(int port) {
        return new NodeConfig(dataDir, port, advertisedHost, advertisedEndpointOverride,
                metadataEndpoints, zone, rack, host, capacityBytes, inventoryIntervalMs, connectionPolicy);
    }

    public NodeConfig withAdvertisedEndpoint(String endpoint) {
        return new NodeConfig(dataDir, listenPort, advertisedHost, endpoint,
                metadataEndpoints, zone, rack, host, capacityBytes, inventoryIntervalMs, connectionPolicy);
    }

    public NodeConfig withConnectionPolicy(ConnectionPolicy policy) {
        return new NodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                metadataEndpoints, zone, rack, host, capacityBytes, inventoryIntervalMs, policy);
    }
}
