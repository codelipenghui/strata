package io.strata.meta;

import java.util.List;
import java.util.Objects;

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
        int zkRetryMaxRetries,       // Curator ExponentialBackoffRetry max retries
        MetadataBackendConfig metadataBackendConfig
) {
    public record MetadataBackendConfig(
            String backend,
            int namespaceLogReplicationFactor,
            int namespaceLogAckQuorum,
            boolean namespaceLogFsync,
            int namespaceLogCompactBytes,
            int namespaceLogCompactIntervalMs,
            boolean namespaceLogOrphanGc,
            int namespaceLogRetentionMs,
            int namespaceLogReadChunkBytes) {

        public MetadataBackendConfig {
            backend = (backend == null || backend.isBlank()) ? "zk" : backend.trim();
            if (namespaceLogReplicationFactor <= 0) {
                throw new IllegalArgumentException("namespaceLogReplicationFactor must be positive: "
                        + namespaceLogReplicationFactor);
            }
            if (namespaceLogAckQuorum <= 0 || namespaceLogAckQuorum > namespaceLogReplicationFactor) {
                throw new IllegalArgumentException("namespaceLogAckQuorum (" + namespaceLogAckQuorum
                        + ") must be 1.." + namespaceLogReplicationFactor);
            }
            if (namespaceLogCompactBytes < 0) {
                throw new IllegalArgumentException("namespaceLogCompactBytes must be non-negative: "
                        + namespaceLogCompactBytes);
            }
            if (namespaceLogCompactIntervalMs < 0) {
                throw new IllegalArgumentException("namespaceLogCompactIntervalMs must be non-negative: "
                        + namespaceLogCompactIntervalMs);
            }
            if (namespaceLogRetentionMs < 0) {
                throw new IllegalArgumentException("namespaceLogRetentionMs must be non-negative: "
                        + namespaceLogRetentionMs);
            }
            if (namespaceLogReadChunkBytes <= 0) {
                throw new IllegalArgumentException("namespaceLogReadChunkBytes must be positive: "
                        + namespaceLogReadChunkBytes);
            }
        }

        public static MetadataBackendConfig zk() {
            return new MetadataBackendConfig("zk", 3, 2, false, 4 * 1024 * 1024, 30_000,
                    true, 0, 4 * 1024 * 1024);
        }

        public static MetadataBackendConfig namespaceLog() {
            return new MetadataBackendConfig("namespace-log", 3, 2, false, 4 * 1024 * 1024,
                    30_000, true, 0, 4 * 1024 * 1024);
        }

        public boolean namespaceLogEnabled() {
            return "namespace-log".equalsIgnoreCase(backend);
        }
    }

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
            throw new IllegalArgumentException("verifyIntervalMs must be positive: " + verifyIntervalMs);
        }
        if (verifyBatchSize <= 0) {
            throw new IllegalArgumentException("verifyBatchSize must be positive: " + verifyBatchSize);
        }
        if (systemVerifyIntervalMs <= 0) {
            throw new IllegalArgumentException("systemVerifyIntervalMs must be positive: " + systemVerifyIntervalMs);
        }
        if (deletedTombstoneTtlMs <= 0) {
            throw new IllegalArgumentException("deletedTombstoneTtlMs must be positive: " + deletedTombstoneTtlMs);
        }
        if (maxCommandsPerHeartbeat <= 0) {
            throw new IllegalArgumentException("maxCommandsPerHeartbeat must be positive: " + maxCommandsPerHeartbeat);
        }
        if (zkRetryBaseMs <= 0) {
            throw new IllegalArgumentException("zkRetryBaseMs must be positive: " + zkRetryBaseMs);
        }
        if (zkRetryMaxRetries < 0) {
            throw new IllegalArgumentException("zkRetryMaxRetries must be >= 0: " + zkRetryMaxRetries);
        }
        metadataBackendConfig = Objects.requireNonNull(metadataBackendConfig, "metadataBackendConfig");
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
                2_000, 256, 30_000, 600_000L, 16, 100, 5, MetadataBackendConfig.zk());
    }

    /** Full v0 tuning tuple without namespace sharding (kept so existing callers compile unchanged). */
    public ControllerConfig(String zkConnect, int listenPort, int heartbeatIntervalMs, int leaseMs,
                      int deadGraceMs, int repairScanIntervalMs, int repairCommandTimeoutMs,
                      int zkSessionTimeoutMs, int zkConnectionTimeoutMs, String advertisedHost,
                      long replicaMissingGraceMs) {
        this(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs, repairScanIntervalMs,
                repairCommandTimeoutMs, 60_000, zkSessionTimeoutMs, zkConnectionTimeoutMs, advertisedHost,
                replicaMissingGraceMs, List.of(), 3, 2_000, 256, 30_000, 600_000L, 16, 100, 5,
                MetadataBackendConfig.zk());
    }

    public ControllerConfig(String zkConnect, int listenPort, int heartbeatIntervalMs, int leaseMs,
                            int deadGraceMs, int repairScanIntervalMs, int repairCommandTimeoutMs,
                            int reconcileIntervalMs, int zkSessionTimeoutMs, int zkConnectionTimeoutMs,
                            String advertisedHost, long replicaMissingGraceMs,
                            List<String> controllerEndpoints, int controllerReplicaCount,
                            int verifyIntervalMs, int verifyBatchSize, int systemVerifyIntervalMs,
                            long deletedTombstoneTtlMs, int maxCommandsPerHeartbeat, int zkRetryBaseMs,
                            int zkRetryMaxRetries) {
        this(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs, repairScanIntervalMs,
                repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs, zkConnectionTimeoutMs,
                advertisedHost, replicaMissingGraceMs, controllerEndpoints, controllerReplicaCount,
                verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries, MetadataBackendConfig.zk());
    }

    public ControllerConfig withAdvertisedHost(String host) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, host, replicaMissingGraceMs, controllerEndpoints, controllerReplicaCount,
                verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries, metadataBackendConfig);
    }

    /** A copy with the missing-replica grace overridden — lets tests drop a deleted replica promptly. */
    public ControllerConfig withReplicaMissingGraceMs(long graceMs) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, graceMs, controllerEndpoints, controllerReplicaCount,
                verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries, metadataBackendConfig);
    }

    /** A copy with the slow-reconcile cadence overridden. */
    public ControllerConfig withReconcileIntervalMs(int reconcileIntervalMs) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs,
                deletedTombstoneTtlMs, maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries,
                metadataBackendConfig);
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
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries, metadataBackendConfig);
    }

    public ControllerConfig withVerifyIntervalMs(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, v, verifyBatchSize, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries, metadataBackendConfig);
    }

    public ControllerConfig withVerifyBatchSize(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, v, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries, metadataBackendConfig);
    }

    public ControllerConfig withSystemVerifyIntervalMs(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, v, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries, metadataBackendConfig);
    }

    public ControllerConfig withDeletedTombstoneTtlMs(long v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs, v,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries, metadataBackendConfig);
    }

    public ControllerConfig withMaxCommandsPerHeartbeat(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs,
                deletedTombstoneTtlMs, v, zkRetryBaseMs, zkRetryMaxRetries, metadataBackendConfig);
    }

    public ControllerConfig withZkRetryBaseMs(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs,
                deletedTombstoneTtlMs, maxCommandsPerHeartbeat, v, zkRetryMaxRetries, metadataBackendConfig);
    }

    public ControllerConfig withZkRetryMaxRetries(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs,
                deletedTombstoneTtlMs, maxCommandsPerHeartbeat, zkRetryBaseMs, v, metadataBackendConfig);
    }

    public ControllerConfig withMetadataBackend(MetadataBackendConfig backendConfig) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs,
                deletedTombstoneTtlMs, maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries,
                backendConfig);
    }

    public ControllerConfig withNamespaceLogBackend() {
        return withMetadataBackend(MetadataBackendConfig.namespaceLog());
    }

    public static ControllerConfig forTests(String zkConnect) {
        return new ControllerConfig(zkConnect, 0, 200, 1_000, 1_500, 300, 3_000, 5_000, 5_000, 20_000, "127.0.0.1",
                90_000, List.of(), 3, 2_000, 256, 30_000, 600_000L, 16, 100, 5,
                MetadataBackendConfig.zk());
    }
}
