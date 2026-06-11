package io.strata.node;

import java.nio.file.Path;
import java.util.List;

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
        byte mediaClass,
        long capacityBytes,
        int inventoryIntervalMs
) {
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
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must be non-blank");
        }
    }

    private static void validateEndpoint(String endpoint, String field) {
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
        if (host.isBlank() || !host.equals(host.trim())) {
            throw new IllegalArgumentException(field + " host must be non-blank: " + endpoint);
        }
        int port;
        try {
            port = Integer.parseInt(endpoint.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(field + " port must be numeric: " + endpoint);
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException(field + " port out of range: " + endpoint);
        }
    }

    public static NodeConfig standalone(Path dataDir) {
        return new NodeConfig(dataDir, 0, "127.0.0.1", null, List.of(), "z0", "r0",
                "h-" + dataDir.getFileName(), (byte) 0, 1L << 40, 60_000);
    }

    public static NodeConfig withMetadata(Path dataDir, List<String> metadataEndpoints, String host) {
        return new NodeConfig(dataDir, 0, "127.0.0.1", null, metadataEndpoints, "z0", "r0",
                host, (byte) 0, 1L << 40, 5_000);
    }

    public NodeConfig withListenPort(int port) {
        return new NodeConfig(dataDir, port, advertisedHost, advertisedEndpointOverride,
                metadataEndpoints, zone, rack, host, mediaClass, capacityBytes, inventoryIntervalMs);
    }

    public NodeConfig withAdvertisedEndpoint(String endpoint) {
        return new NodeConfig(dataDir, listenPort, advertisedHost, endpoint,
                metadataEndpoints, zone, rack, host, mediaClass, capacityBytes, inventoryIntervalMs);
    }
}
