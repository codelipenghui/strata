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
        List<String> metadataEndpoints, // "host:port"
        String zone,
        String rack,
        String host,
        byte mediaClass,
        long capacityBytes,
        int inventoryIntervalMs
) {
    public static NodeConfig standalone(Path dataDir) {
        return new NodeConfig(dataDir, 0, "127.0.0.1", List.of(), "z0", "r0",
                "h-" + dataDir.getFileName(), (byte) 0, 1L << 40, 60_000);
    }

    public static NodeConfig withMetadata(Path dataDir, List<String> metadataEndpoints, String host) {
        return new NodeConfig(dataDir, 0, "127.0.0.1", metadataEndpoints, "z0", "r0",
                host, (byte) 0, 1L << 40, 5_000);
    }
}
