package io.strata.meta;

/** Metadata service configuration. Production values are minutes; tests use sub-second timings. */
public record MetaConfig(
        String zkConnect,
        int listenPort,            // 0 = ephemeral
        int heartbeatIntervalMs,   // told to nodes at registration
        int leaseMs,               // lease granted per heartbeat
        int deadGraceMs,           // lease expiry -> SUSPECT; expiry + grace -> DEAD (repair starts)
        int repairScanIntervalMs
) {
    public static MetaConfig forTests(String zkConnect) {
        return new MetaConfig(zkConnect, 0, 200, 1_000, 1_500, 300);
    }

    public static MetaConfig production(String zkConnect, int port) {
        return new MetaConfig(zkConnect, port, 1_000, 10_000, 300_000, 30_000);
    }
}
