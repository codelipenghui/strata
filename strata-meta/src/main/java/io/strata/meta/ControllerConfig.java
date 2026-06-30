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
        int reconcileIntervalMs,   // slow-reconcile cadence: full inventory sweep vs. repair-log delta
        int zkSessionTimeoutMs,
        int zkConnectionTimeoutMs,
        String advertisedHost,     // host clients/peers reach this meta at; carried in the leader hint
        long replicaMissingGraceMs, // a node-reported-missing sealed replica is dropped only after it
                                    // stays missing this long (absorbs in-flight inventory snapshots)
        List<String> controllerEndpoints, // eligible controller endpoints for namespace ownership (rendezvous,
                                        // design §6.1). empty/size<=1 => this node owns every namespace
                                        // (no sharding — preserves single-leader behavior)
        int controllerReplicaCount,  // metadata replica-set size per namespace (design §6.1)
        int verifyIntervalMs,        // owner-pull VERIFY_CHUNKS cadence (RepairCoordinator)
        int verifyBatchSize,         // chunk-ids per VERIFY_CHUNKS RPC
        int systemVerifyIntervalMs,  // slower verify cadence for the system (metadata-log) namespace
        long deletedTombstoneTtlMs,  // DELETED tombstone retention before reap
        int maxCommandsPerHeartbeat, // commands drained per node heartbeat
        int zkRetryBaseMs,           // Curator ExponentialBackoffRetry base sleep
        int zkRetryMaxRetries        // Curator ExponentialBackoffRetry max retries
) {
    public ControllerConfig {
        if (zkConnect == null || zkConnect.isBlank()) {
            throw new IllegalArgumentException("zkConnect must be non-null/non-blank");
        }
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be > 0: " + heartbeatIntervalMs);
        }
        // A node renews its lease every heartbeatIntervalMs; DEAD only fires at leaseMs + deadGraceMs, so
        // the heartbeat must be shorter than that window — otherwise nodes expire before they can heartbeat,
        // causing continuous spurious DEAD markings and repair storms.
        if (heartbeatIntervalMs >= (long) leaseMs + deadGraceMs) {
            throw new IllegalArgumentException("heartbeatIntervalMs (" + heartbeatIntervalMs
                    + ") must be < leaseMs + deadGraceMs (" + ((long) leaseMs + deadGraceMs) + ")");
        }
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
        if (verifyIntervalMs <= 0) {
            verifyIntervalMs = 2_000;
        }
        if (verifyBatchSize <= 0) {
            verifyBatchSize = 256;
        }
        if (systemVerifyIntervalMs <= 0) {
            systemVerifyIntervalMs = 30_000;
        }
        if (deletedTombstoneTtlMs <= 0) {
            deletedTombstoneTtlMs = 600_000;
        }
        if (maxCommandsPerHeartbeat <= 0) {
            maxCommandsPerHeartbeat = 16;
        }
        if (zkRetryBaseMs <= 0) {
            zkRetryBaseMs = 100;
        }
        if (zkRetryMaxRetries < 0) {
            zkRetryMaxRetries = 5;
        }
        // A DELETED tombstone fences a delayed CREATE replay; it must outlive the reconcile sweep cadence.
        if (deletedTombstoneTtlMs <= repairScanIntervalMs) {
            throw new IllegalArgumentException("deletedTombstoneTtlMs (" + deletedTombstoneTtlMs
                    + ") must exceed repairScanIntervalMs (" + repairScanIntervalMs + ")");
        }
    }

    public ControllerConfig(String zkConnect, int listenPort, int heartbeatIntervalMs, int leaseMs,
                      int deadGraceMs, int repairScanIntervalMs, int repairCommandTimeoutMs) {
        this(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs, repairScanIntervalMs,
                repairCommandTimeoutMs, 60_000, 60_000, 15_000, "127.0.0.1", 90_000, List.of(), 3,
                0, 0, 0, 0, 0, 0, -1);
    }

    /** Full v0 tuning tuple without namespace sharding (kept so existing callers compile unchanged). */
    public ControllerConfig(String zkConnect, int listenPort, int heartbeatIntervalMs, int leaseMs,
                      int deadGraceMs, int repairScanIntervalMs, int repairCommandTimeoutMs,
                      int zkSessionTimeoutMs, int zkConnectionTimeoutMs, String advertisedHost,
                      long replicaMissingGraceMs) {
        this(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs, repairScanIntervalMs,
                repairCommandTimeoutMs, 60_000, zkSessionTimeoutMs, zkConnectionTimeoutMs, advertisedHost,
                replicaMissingGraceMs, List.of(), 3, 0, 0, 0, 0, 0, 0, -1);
    }

    public ControllerConfig withAdvertisedHost(String host) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, host, replicaMissingGraceMs, controllerEndpoints, controllerReplicaCount,
                verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries);
    }

    /** A copy with the missing-replica grace overridden — lets tests drop a deleted replica promptly. */
    public ControllerConfig withReplicaMissingGraceMs(long graceMs) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, graceMs, controllerEndpoints, controllerReplicaCount,
                verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries);
    }

    /** A copy with the slow-reconcile cadence overridden. */
    public ControllerConfig withReconcileIntervalMs(int reconcileIntervalMs) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs,
                deletedTombstoneTtlMs, maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries);
    }

    /**
     * A copy with the eligible controller endpoints and replica-set size for namespace sharding
     * (design §6.1). Pass this node's own advertised endpoint among {@code endpoints} so rendezvous
     * can place it; an empty list or a single endpoint means this node owns every namespace.
     */
    public ControllerConfig withControllerEndpoints(List<String> endpoints, int replicaCount) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, endpoints, replicaCount,
                verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries);
    }

    public ControllerConfig withVerifyIntervalMs(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, v, verifyBatchSize, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries);
    }

    public ControllerConfig withVerifyBatchSize(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, v, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries);
    }

    public ControllerConfig withSystemVerifyIntervalMs(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, v, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries);
    }

    public ControllerConfig withDeletedTombstoneTtlMs(long v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs, v,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries);
    }

    public ControllerConfig withMaxCommandsPerHeartbeat(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs,
                deletedTombstoneTtlMs, v, zkRetryBaseMs, zkRetryMaxRetries);
    }

    public ControllerConfig withZkRetryBaseMs(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs,
                deletedTombstoneTtlMs, maxCommandsPerHeartbeat, v, zkRetryMaxRetries);
    }

    public ControllerConfig withZkRetryMaxRetries(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs,
                deletedTombstoneTtlMs, maxCommandsPerHeartbeat, zkRetryBaseMs, v);
    }

    public static ControllerConfig forTests(String zkConnect) {
        return new ControllerConfig(zkConnect, 0, 200, 1_000, 1_500, 300, 3_000, 5_000, 5_000, 20_000, "127.0.0.1",
                90_000, List.of(), 3, 0, 0, 0, 0, 0, 0, -1);
    }
}
