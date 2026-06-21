package io.strata.meta;

import java.util.List;

/** Controller configuration. Production values are minutes; tests use sub-second timings. */
public record ControllerConfig(
        String zkConnect,
        int listenPort,            // 0 = ephemeral
        int heartbeatIntervalMs,   // told to nodes at registration
        int leaseMs,               // lease granted per heartbeat
        int deadGraceMs,           // lease expiry -> SUSPECT; expiry + grace -> DEAD (repair starts)
        int repairScanIntervalMs,
        int repairCommandTimeoutMs, // in-flight command without completion past this -> re-issue
        int zkSessionTimeoutMs,
        int zkConnectionTimeoutMs,
        String advertisedHost,     // host clients/peers reach this meta at; carried in the leader hint
        long replicaMissingGraceMs, // a node-reported-missing sealed replica is dropped only after it
                                    // stays missing this long (absorbs in-flight inventory snapshots)
        List<String> controllerEndpoints, // eligible controller endpoints for namespace ownership (rendezvous,
                                        // design §6.1). empty/size<=1 => this node owns every namespace
                                        // (no sharding — preserves single-leader behavior)
        int controllerReplicaCount   // metadata replica-set size per namespace (design §6.1)
) {
    public ControllerConfig {
        if (advertisedHost == null || advertisedHost.isBlank()) {
            advertisedHost = "127.0.0.1";
        }
        if (replicaMissingGraceMs < 0) {
            throw new IllegalArgumentException("replicaMissingGraceMs must be >= 0");
        }
        controllerEndpoints = controllerEndpoints == null ? List.of() : List.copyOf(controllerEndpoints);
        if (controllerReplicaCount <= 0) {
            controllerReplicaCount = 3;
        }
    }

    public ControllerConfig(String zkConnect, int listenPort, int heartbeatIntervalMs, int leaseMs,
                      int deadGraceMs, int repairScanIntervalMs, int repairCommandTimeoutMs) {
        this(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs, repairScanIntervalMs,
                repairCommandTimeoutMs, 60_000, 15_000, "127.0.0.1", 90_000, List.of(), 3);
    }

    /** Full v0 tuning tuple without namespace sharding (kept so existing callers compile unchanged). */
    public ControllerConfig(String zkConnect, int listenPort, int heartbeatIntervalMs, int leaseMs,
                      int deadGraceMs, int repairScanIntervalMs, int repairCommandTimeoutMs,
                      int zkSessionTimeoutMs, int zkConnectionTimeoutMs, String advertisedHost,
                      long replicaMissingGraceMs) {
        this(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs, repairScanIntervalMs,
                repairCommandTimeoutMs, zkSessionTimeoutMs, zkConnectionTimeoutMs, advertisedHost,
                replicaMissingGraceMs, List.of(), 3);
    }

    public ControllerConfig withAdvertisedHost(String host) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, zkSessionTimeoutMs, zkConnectionTimeoutMs,
                host, replicaMissingGraceMs, controllerEndpoints, controllerReplicaCount);
    }

    /** A copy with the missing-replica grace overridden — lets tests drop a deleted replica promptly. */
    public ControllerConfig withReplicaMissingGraceMs(long graceMs) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, zkSessionTimeoutMs, zkConnectionTimeoutMs,
                advertisedHost, graceMs, controllerEndpoints, controllerReplicaCount);
    }

    /**
     * A copy with the eligible controller endpoints and replica-set size for namespace sharding
     * (design §6.1). Pass this node's own advertised endpoint among {@code endpoints} so rendezvous
     * can place it; an empty list or a single endpoint means this node owns every namespace.
     */
    public ControllerConfig withControllerEndpoints(List<String> endpoints, int replicaCount) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, zkSessionTimeoutMs, zkConnectionTimeoutMs,
                advertisedHost, replicaMissingGraceMs, endpoints, replicaCount);
    }

    public static ControllerConfig forTests(String zkConnect) {
        return new ControllerConfig(zkConnect, 0, 200, 1_000, 1_500, 300, 3_000, 5_000, 20_000, "127.0.0.1",
                90_000, List.of(), 3);
    }
}
