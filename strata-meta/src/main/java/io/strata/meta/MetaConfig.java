package io.strata.meta;

/** Metadata service configuration. Production values are minutes; tests use sub-second timings. */
public record MetaConfig(
        String zkConnect,
        int listenPort,            // 0 = ephemeral
        int heartbeatIntervalMs,   // told to nodes at registration
        int leaseMs,               // lease granted per heartbeat
        int deadGraceMs,           // lease expiry -> SUSPECT; expiry + grace -> DEAD (repair starts)
        int repairScanIntervalMs,
        int repairCommandTimeoutMs, // in-flight command without completion past this -> re-issue
        int zkSessionTimeoutMs,
        int zkConnectionTimeoutMs,
        String advertisedHost      // host clients/peers reach this meta at; carried in the leader hint
) {
    public MetaConfig {
        if (advertisedHost == null || advertisedHost.isBlank()) {
            advertisedHost = "127.0.0.1";
        }
    }

    public MetaConfig(String zkConnect, int listenPort, int heartbeatIntervalMs, int leaseMs,
                      int deadGraceMs, int repairScanIntervalMs, int repairCommandTimeoutMs) {
        this(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs, repairScanIntervalMs,
                repairCommandTimeoutMs, 60_000, 15_000, "127.0.0.1");
    }

    public MetaConfig withAdvertisedHost(String host) {
        return new MetaConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, zkSessionTimeoutMs, zkConnectionTimeoutMs, host);
    }

    public static MetaConfig forTests(String zkConnect) {
        return new MetaConfig(zkConnect, 0, 200, 1_000, 1_500, 300, 3_000, 5_000, 20_000, "127.0.0.1");
    }
}
