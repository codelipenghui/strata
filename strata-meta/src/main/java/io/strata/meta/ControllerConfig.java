package io.strata.meta;

import java.util.List;
import java.util.Objects;

/** Controller configuration with validated production defaults; tests commonly use shorter timings. */
public record ControllerConfig(
        String zkConnect,
        int listenPort,            // 0 = ephemeral
        int heartbeatIntervalMs,   // told to nodes at registration
        int leaseMs,               // lease granted per heartbeat
        int deadGraceMs,           // lease expiry -> SUSPECT; expiry + grace -> DEAD (repair starts)
        int repairScanIntervalMs,
        int repairCommandTimeoutMs, // in-flight command without completion past this -> re-issue
        int reconcileIntervalMs,   // slow reconciliation/tombstone-sweep cadence
        int zkSessionTimeoutMs,
        int zkConnectionTimeoutMs,
        String advertisedHost,     // host clients/peers reach this meta at; carried in the leader hint
        long replicaMissingGraceMs, // a node-reported-missing sealed replica is dropped only after it
                                    // stays missing this long (absorbs stale verification/liveness snapshots)
        List<String> controllerEndpoints, // bootstrap candidates for persisted namespace assignments.
                                        // empty/size<=1 => this node owns every namespace (no sharding)
        int controllerReplicaCount,  // metadata replica-set size per namespace (tech design §4.5)
        int verifyIntervalMs,        // owner-pull VERIFY_CHUNKS cadence (RepairCoordinator)
        int verifyBatchSize,         // chunk-ids per VERIFY_CHUNKS RPC
        int systemVerifyIntervalMs,  // slower verify cadence for the system (metadata-log) namespace
        long deletedTombstoneTtlMs,  // DELETED tombstone retention before reap
        int maxCommandsPerHeartbeat, // commands drained per node heartbeat
        int zkRetryBaseMs,           // Curator ExponentialBackoffRetry base sleep
        int zkRetryMaxRetries,       // Curator ExponentialBackoffRetry max retries
        MetadataBackendConfig metadataBackendConfig
) {
    public static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 10_000;
    public static final int DEFAULT_NAMESPACE_LOG_RETENTION_MS = 5 * 60_000;
    // Mirrors ClientConfig.of() so namespace metadata logs roll at the same default size as client files.
    public static final long DEFAULT_NAMESPACE_LOG_CHUNK_ROLL_BYTES = 2L << 30;

    public record MetadataBackendConfig(
            String backend,
            int namespaceLogReplicationFactor,
            int namespaceLogAckQuorum,
            boolean namespaceLogFsync,
            int namespaceLogCompactBytes,
            int namespaceLogCompactIntervalMs,
            boolean namespaceLogOrphanGc,
            int namespaceLogRetentionMs,
            int namespaceLogReadChunkBytes,
            long namespaceLogChunkRollBytes) {

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
            if (namespaceLogChunkRollBytes <= 0) {
                throw new IllegalArgumentException("namespaceLogChunkRollBytes must be positive: "
                        + namespaceLogChunkRollBytes);
            }
        }

        public static MetadataBackendConfig zk() {
            return new MetadataBackendConfig("zk", 3, 2, false, 4 * 1024 * 1024, 30_000,
                    true, DEFAULT_NAMESPACE_LOG_RETENTION_MS, 4 * 1024 * 1024,
                    DEFAULT_NAMESPACE_LOG_CHUNK_ROLL_BYTES);
        }

        public static MetadataBackendConfig namespaceLog() {
            return new MetadataBackendConfig("namespace-log", 3, 2, false, 4 * 1024 * 1024,
                    30_000, true, DEFAULT_NAMESPACE_LOG_RETENTION_MS, 4 * 1024 * 1024,
                    DEFAULT_NAMESPACE_LOG_CHUNK_ROLL_BYTES);
        }

        public MetadataBackendConfig withNamespaceLogChunkRollBytes(long bytes) {
            return new MetadataBackendConfig(backend, namespaceLogReplicationFactor, namespaceLogAckQuorum,
                    namespaceLogFsync, namespaceLogCompactBytes, namespaceLogCompactIntervalMs,
                    namespaceLogOrphanGc, namespaceLogRetentionMs, namespaceLogReadChunkBytes, bytes);
        }

        public MetadataBackendConfig withNamespaceLogReadChunkBytes(int bytes) {
            return new MetadataBackendConfig(backend, namespaceLogReplicationFactor, namespaceLogAckQuorum,
                    namespaceLogFsync, namespaceLogCompactBytes, namespaceLogCompactIntervalMs,
                    namespaceLogOrphanGc, namespaceLogRetentionMs, bytes, namespaceLogChunkRollBytes);
        }

        public MetadataBackendConfig withNamespaceLogCompaction(int compactBytes, int compactIntervalMs) {
            return new MetadataBackendConfig(backend, namespaceLogReplicationFactor, namespaceLogAckQuorum,
                    namespaceLogFsync, compactBytes, compactIntervalMs, namespaceLogOrphanGc,
                    namespaceLogRetentionMs, namespaceLogReadChunkBytes, namespaceLogChunkRollBytes);
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
        if (zkSessionTimeoutMs <= 0) {
            throw new IllegalArgumentException("zkSessionTimeoutMs must be positive: " + zkSessionTimeoutMs);
        }
        if (zkConnectionTimeoutMs <= 0) {
            throw new IllegalArgumentException("zkConnectionTimeoutMs must be positive: " + zkConnectionTimeoutMs);
        }
        controllerEndpoints = controllerEndpoints == null ? List.of() : List.copyOf(controllerEndpoints);
        if (controllerEndpoints.stream().distinct().count() != controllerEndpoints.size()) {
            throw new IllegalArgumentException("controllerEndpoints must not contain duplicates: "
                    + controllerEndpoints);
        }
        if (controllerReplicaCount <= 0) {
            controllerReplicaCount = 3;
        }
        if (controllerEndpoints.size() == 1 && controllerReplicaCount != 1) {
            throw new IllegalArgumentException(
                    "single-controller configuration requires controllerReplicaCount=1: "
                            + controllerReplicaCount);
        }
        if (controllerEndpoints.size() > 1) {
            if (controllerReplicaCount < 2) {
                throw new IllegalArgumentException("sharded metadata requires at least two controller replicas: "
                        + controllerReplicaCount);
            }
            if (controllerReplicaCount > controllerEndpoints.size()) {
                throw new IllegalArgumentException("controllerReplicaCount (" + controllerReplicaCount
                        + ") exceeds configured controller endpoints (" + controllerEndpoints.size() + ")");
            }
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
                repairCommandTimeoutMs, 60_000, DEFAULT_ZK_SESSION_TIMEOUT_MS, 15_000,
                "127.0.0.1", 90_000, List.of(), 3,
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

    /** A copy with the ZooKeeper session timeout used as the controller-owner failure detector. */
    public ControllerConfig withZkSessionTimeoutMs(int timeoutMs) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, timeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs,
                deletedTombstoneTtlMs, maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries,
                metadataBackendConfig);
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
     * A copy with the bootstrap controller endpoints and persisted replica-set size for namespace sharding
     * (tech design §4.5). Pass this node's own advertised endpoint among {@code endpoints}; rendezvous ordering
     * is used only when the assignment is first persisted. An empty list or one endpoint with replica count 1
     * means this node owns every namespace; Controller verifies that a configured singleton names this process.
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
