package io.strata.server;

import io.micrometer.core.instrument.MeterRegistry;
import io.strata.format.ChunkStoreConfig;
import io.strata.meta.Controller;
import io.strata.meta.ControllerConfig;
import io.strata.metrics.MetricsServer;
import io.strata.metrics.StrataMetrics;
import io.strata.node.DataNode;
import io.strata.node.DataNodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Production entrypoint. {@code strata data-node} runs a data node; {@code strata controller} runs a
 * controller. The role is taken from the first argument (or {@code STRATA_ROLE}); everything
 * else comes from environment variables. Every value has a production-sane default except the
 * deployment-specific ones — a controller needs {@code STRATA_ZK_CONNECT}, a data node
 * needs {@code STRATA_CONTROLLER_ENDPOINTS}. Blocks until SIGTERM/SIGINT, then closes the service cleanly.
 *
 * <p>This is a thin launcher around the already-tested {@link DataNode}/{@link Controller}
 * lifecycles — it adds no behavior, only configuration loading and signal handling.
 */
public final class StrataServer {
    private static final Logger log = LoggerFactory.getLogger(StrataServer.class);

    public static void main(String[] args) throws Exception {
        String role = args.length > 0 ? args[0] : env("STRATA_ROLE", null);
        if (role == null) {
            System.err.println("usage: strata <data-node|controller|combined>   (or set STRATA_ROLE=data-node|controller|combined)");
            System.exit(2);
            return;
        }
        switch (role) {
            case "data-node" -> runDataNode();
            case "controller" -> runController();
            case "combined", "data-node,controller" -> runCombined();
            case "perf" -> StrataPerf.run(args);
            default -> {
                System.err.println("unknown role '" + role
                        + "': expected 'data-node', 'controller', 'combined', or 'perf'");
                System.exit(2);
            }
        }
    }

    private static void runController() throws Exception {
        ControllerConfig config = new ControllerConfig(
                required("STRATA_ZK_CONNECT"),
                intEnv("STRATA_LISTEN_PORT", 9_200),
                intEnv("STRATA_HEARTBEAT_INTERVAL_MS", 3_000),
                intEnv("STRATA_LEASE_MS", 10_000),
                intEnv("STRATA_DEAD_GRACE_MS", 30_000),
                intEnv("STRATA_REPAIR_SCAN_INTERVAL_MS", 5_000),
                intEnv("STRATA_REPAIR_COMMAND_TIMEOUT_MS", 30_000))
                // Endpoint a standby returns as the NOT_LEADER redirect hint. In containers/k8s the
                // hostname() default is the container/pod id — set STRATA_ADVERTISED_HOST to a name
                // clients can resolve (the service/DNS name) whenever more than one replica runs.
                .withAdvertisedHost(env("STRATA_ADVERTISED_HOST", hostname()))
                .withReconcileIntervalMs(intEnv("STRATA_REPAIR_RECONCILE_INTERVAL_MS", 15_000))
                .withVerifyIntervalMs(intEnv("STRATA_VERIFY_INTERVAL_MS", 2_000))
                .withVerifyBatchSize(intEnv("STRATA_VERIFY_BATCH_SIZE", 256))
                .withSystemVerifyIntervalMs(intEnv("STRATA_SYSTEM_VERIFY_INTERVAL_MS", 30_000))
                .withDeletedTombstoneTtlMs(longEnv("STRATA_CONTROLLER_DELETED_TOMBSTONE_TTL_MS", 600_000))
                .withMaxCommandsPerHeartbeat(intEnv("STRATA_CONTROLLER_MAX_COMMANDS_PER_HEARTBEAT", 16))
                .withZkRetryBaseMs(intEnv("STRATA_CONTROLLER_ZK_RETRY_BASE_MS", 100))
                .withZkRetryMaxRetries(intEnv("STRATA_CONTROLLER_ZK_RETRY_MAX", 5))
                .withMetadataBackend(metadataBackendConfig());
        // Namespace sharding is OPT-IN (default off = single global leader). When enabled, namespaces are
        // rendezvous-assigned across STRATA_CONTROLLER_ENDPOINTS so each controller node owns a shard. The
        // client (ControllerClient) is sharding-aware: it keeps one connection per owner and routes each op
        // to its namespace's owner, learning owners from NOT_LEADER redirect hints — so concurrent ops
        // across owners do not thrash a single connection. Off by default to gate fleet-wide rollout.
        if (boolEnv("STRATA_CONTROLLER_SHARDING", false)) {
            config = config.withControllerEndpoints(endpoints(required("STRATA_CONTROLLER_ENDPOINTS")),
                    intEnv("STRATA_CONTROLLER_REPLICA_COUNT", 3));
        }
        Controller service = new Controller(config);
        log.info("controller started: endpoint={} zk={} leader={}",
                service.endpoint(), config.zkConnect(), service.isLeader());
        long nsRefreshMs = intEnv("STRATA_METRICS_NS_REFRESH_INTERVAL_MS", 10_000);
        long[] buckets = parseBucketsMs(env("STRATA_METRICS_REQUEST_DURATION_BUCKETS_MS", null),
                new long[]{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000});
        AutoCloseable metrics = null;
        try {
            metrics = startMetrics("controller", reg -> {
                ServerMetrics.registerController(reg, service, nsRefreshMs);
                service.setRequestObserver(ServerMetrics.requestObserver(reg, buckets));
            }, () -> service.isLeader() && service.zkConnected());
            awaitShutdown("controller", metrics, service);
        } catch (Exception e) {
            closeQuietly(metrics);
            closeQuietly(service);
            throw e;
        }
    }

    private static void runDataNode() throws Exception {
        String hostname = hostname();
        DataNodeConfig config = new DataNodeConfig(
                Path.of(env("STRATA_DATA_DIR", "/data")),
                intEnv("STRATA_LISTEN_PORT", 9_100),
                env("STRATA_ADVERTISED_HOST", hostname),
                null,
                endpoints(required("STRATA_CONTROLLER_ENDPOINTS")),
                env("STRATA_ZONE", "z0"),
                env("STRATA_RACK", "r0"),
                env("STRATA_HOST", hostname),
                longEnv("STRATA_CAPACITY_BYTES", 1L << 40),  // 1 TiB
                intEnv("STRATA_SCRUB_INTERVAL_MS", 300_000)) // full re-CRC every 5 min (was 30s push x10)
                .withNodeId(requiredIntEnv("STRATA_NODE_ID"))
                .withOrphanGraceMs(longEnv("STRATA_ORPHAN_GRACE_MS", 6_000))
                .withOrphanScanIntervalMs(longEnv("STRATA_ORPHAN_SCAN_INTERVAL_MS", 3_000))
                .withOrphanStartupGraceMs(longEnv("STRATA_ORPHAN_STARTUP_GRACE_MS", 6_000))
                .withOrphanConfirmTimeoutMs(intEnv("STRATA_ORPHAN_CONFIRM_TIMEOUT_MS", 5_000))
                .withControlCallTimeoutMs(intEnv("STRATA_CONTROL_CALL_TIMEOUT_MS", 10_000))
                .withControlCommandLimits(intEnv("STRATA_NODE_COMMAND_PARALLELISM", 8),
                        intEnv("STRATA_NODE_MAX_QUEUED_COMMANDS", 1024))
                .withRepairFetchBytes(intEnv("STRATA_REPAIR_FETCH_BYTES", 4 * 1024 * 1024))
                .withDeleteMaxConcurrent(intEnv("STRATA_DELETE_MAX_CONCURRENT", 1))
                .withDeleteMinIntervalMs(longEnv("STRATA_DELETE_MIN_INTERVAL_MS", 50))
                .withChunkStoreConfig(new ChunkStoreConfig(
                        intEnv("STRATA_MAX_REQUEST_BYTES", 8 * 1024 * 1024),
                        longEnv("STRATA_GROUPCOMMIT_DRAIN_TIMEOUT_MS", 10_000),
                        longEnv("STRATA_GROUPCOMMIT_MIN_ACCUMULATION_NANOS", 1_000_000),
                        longEnv("STRATA_GROUPCOMMIT_MAX_ACCUMULATION_NANOS", 50_000_000),
                        boolEnv("STRATA_SEAL_FSYNC", false),
                        longEnv("STRATA_BG_FLUSH_INTERVAL_MS", 500),
                        longEnv("STRATA_BG_FLUSH_THRESHOLD_BYTES", 4L << 20),
                        longEnv("STRATA_SLOW_APPEND_LOG_MS", 1_000),
                        longEnv("STRATA_SLOW_MUTATION_LOG_MS", 500),
                        intEnv("STRATA_FILE_CHANNEL_CACHE_MAX_SIZE",
                                ChunkStoreConfig.DEFAULT.channelCacheMaxSize())));
        DataNode node = new DataNode(config);
        log.info("data node started: endpoint={} dataDir={} controller={}",
                node.endpoint(), config.dataDir(), config.controllerEndpoints());
        long nsRefreshMs = intEnv("STRATA_METRICS_NS_REFRESH_INTERVAL_MS", 10_000);
        long[] buckets = parseBucketsMs(env("STRATA_METRICS_REQUEST_DURATION_BUCKETS_MS", null),
                new long[]{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000});
        AutoCloseable metrics = null;
        try {
            metrics = startMetrics("data-node", reg -> {
                ServerMetrics.registerDataNode(reg, node, nsRefreshMs);
                node.setRequestObserver(ServerMetrics.requestObserver(reg, buckets));
            }, node::registered);
            awaitShutdown("data node", metrics, node);
        } catch (Exception e) {
            closeQuietly(metrics);
            closeQuietly(node);
            throw e;
        }
    }

    /**
     * Co-resident mode: runs a {@link Controller} and a {@link DataNode} in one JVM, so the
     * metadata plane scales with the data fleet and there is no separate {@code strata-meta} to
     * deploy. Every combined node hosts a controller instance; one is elected leader (the rest are warm
     * standbys) and serves all metadata RPCs — clients reach it via the {@code NOT_LEADER} redirect.
     * Both planes share one SCP listener on {@code STRATA_LISTEN_PORT} (default 9100): data opcodes go
     * to the data node, metadata opcodes to the co-resident controller. {@code STRATA_CONTROLLER_ENDPOINTS}
     * lists the controller-eligible nodes (including this one) at that port.
     */
    private static void runCombined() throws Exception {
        String hostname = hostname();
        String advertisedHost = env("STRATA_ADVERTISED_HOST", hostname);
        ControllerConfig controllerConfig = new ControllerConfig(
                required("STRATA_ZK_CONNECT"),
                intEnv("STRATA_LISTEN_PORT", 9_100),  // unused in embedded mode (controller shares the node's listener)
                intEnv("STRATA_HEARTBEAT_INTERVAL_MS", 3_000),
                intEnv("STRATA_LEASE_MS", 10_000),
                intEnv("STRATA_DEAD_GRACE_MS", 30_000),
                intEnv("STRATA_REPAIR_SCAN_INTERVAL_MS", 5_000),
                intEnv("STRATA_REPAIR_COMMAND_TIMEOUT_MS", 30_000))
                .withAdvertisedHost(advertisedHost)
                .withReconcileIntervalMs(intEnv("STRATA_REPAIR_RECONCILE_INTERVAL_MS", 15_000))
                .withVerifyIntervalMs(intEnv("STRATA_VERIFY_INTERVAL_MS", 2_000))
                .withVerifyBatchSize(intEnv("STRATA_VERIFY_BATCH_SIZE", 256))
                .withSystemVerifyIntervalMs(intEnv("STRATA_SYSTEM_VERIFY_INTERVAL_MS", 30_000))
                .withDeletedTombstoneTtlMs(longEnv("STRATA_CONTROLLER_DELETED_TOMBSTONE_TTL_MS", 600_000))
                .withMaxCommandsPerHeartbeat(intEnv("STRATA_CONTROLLER_MAX_COMMANDS_PER_HEARTBEAT", 16))
                .withZkRetryBaseMs(intEnv("STRATA_CONTROLLER_ZK_RETRY_BASE_MS", 100))
                .withZkRetryMaxRetries(intEnv("STRATA_CONTROLLER_ZK_RETRY_MAX", 5))
                .withMetadataBackend(metadataBackendConfig());
        // Namespace sharding is OPT-IN (default off = single global leader). The sharding-aware client
        // (ControllerClient) keeps one connection per owner and routes each op to its namespace's owner,
        // so it does not thrash a single connection. Off by default to gate rollout. See runController /
        // STRATA_CONTROLLER_SHARDING.
        if (boolEnv("STRATA_CONTROLLER_SHARDING", false)) {
            controllerConfig = controllerConfig.withControllerEndpoints(endpoints(required("STRATA_CONTROLLER_ENDPOINTS")),
                    intEnv("STRATA_CONTROLLER_REPLICA_COUNT", 3));
        }
        DataNodeConfig nodeConfig = new DataNodeConfig(
                Path.of(env("STRATA_DATA_DIR", "/data")),
                intEnv("STRATA_LISTEN_PORT", 9_100),
                advertisedHost,
                null,
                endpoints(required("STRATA_CONTROLLER_ENDPOINTS")),
                env("STRATA_ZONE", "z0"),
                env("STRATA_RACK", "r0"),
                env("STRATA_HOST", hostname),
                longEnv("STRATA_CAPACITY_BYTES", 1L << 40),  // 1 TiB
                intEnv("STRATA_SCRUB_INTERVAL_MS", 300_000)) // full re-CRC every 5 min (was 30s push x10)
                .withNodeId(requiredIntEnv("STRATA_NODE_ID"))
                .withOrphanGraceMs(longEnv("STRATA_ORPHAN_GRACE_MS", 6_000))
                .withOrphanScanIntervalMs(longEnv("STRATA_ORPHAN_SCAN_INTERVAL_MS", 3_000))
                .withOrphanStartupGraceMs(longEnv("STRATA_ORPHAN_STARTUP_GRACE_MS", 6_000))
                .withOrphanConfirmTimeoutMs(intEnv("STRATA_ORPHAN_CONFIRM_TIMEOUT_MS", 5_000))
                .withControlCallTimeoutMs(intEnv("STRATA_CONTROL_CALL_TIMEOUT_MS", 10_000))
                .withControlCommandLimits(intEnv("STRATA_NODE_COMMAND_PARALLELISM", 8),
                        intEnv("STRATA_NODE_MAX_QUEUED_COMMANDS", 1024))
                .withRepairFetchBytes(intEnv("STRATA_REPAIR_FETCH_BYTES", 4 * 1024 * 1024))
                .withDeleteMaxConcurrent(intEnv("STRATA_DELETE_MAX_CONCURRENT", 1))
                .withDeleteMinIntervalMs(longEnv("STRATA_DELETE_MIN_INTERVAL_MS", 50))
                .withChunkStoreConfig(new ChunkStoreConfig(
                        intEnv("STRATA_MAX_REQUEST_BYTES", 8 * 1024 * 1024),
                        longEnv("STRATA_GROUPCOMMIT_DRAIN_TIMEOUT_MS", 10_000),
                        longEnv("STRATA_GROUPCOMMIT_MIN_ACCUMULATION_NANOS", 1_000_000),
                        longEnv("STRATA_GROUPCOMMIT_MAX_ACCUMULATION_NANOS", 50_000_000),
                        boolEnv("STRATA_SEAL_FSYNC", false),
                        longEnv("STRATA_BG_FLUSH_INTERVAL_MS", 500),
                        longEnv("STRATA_BG_FLUSH_THRESHOLD_BYTES", 4L << 20),
                        longEnv("STRATA_SLOW_APPEND_LOG_MS", 1_000),
                        longEnv("STRATA_SLOW_MUTATION_LOG_MS", 500),
                        intEnv("STRATA_FILE_CHANNEL_CACHE_MAX_SIZE",
                                ChunkStoreConfig.DEFAULT.channelCacheMaxSize())));
        Combined combined = startCombined(controllerConfig, nodeConfig);
        log.info("combined node started: scp={} zk={}", combined.node().endpoint(), controllerConfig.zkConnect());
        awaitShutdown("combined node", combined);
    }

    /**
     * Builds and starts a co-resident controller + data node behind a single combined
     * metrics endpoint, and returns a handle that closes both (node first, then controller). The controller is
     * built first so it can join the leader latch before the node tries to register.
     */
    static Combined startCombined(ControllerConfig controllerConfig, DataNodeConfig nodeConfig) throws Exception {
        Controller controller = null;
        DataNode node = null;
        try {
            // One SCP listener for both planes: the controller runs embedded (no own port), served on the
            // node's listener which routes metadata opcodes to it. The controller advertises the node's
            // reachable endpoint as the NOT_LEADER redirect hint, so combined mode needs a FIXED node
            // port — an ephemeral (0) port would advertise an unreachable ":0" hint before the real
            // port is even bound.
            if (nodeConfig.listenPort() == 0) {
                throw new IllegalArgumentException(
                        "combined mode requires a fixed node listenPort (not ephemeral 0): the embedded "
                                + "controller advertises advertisedHost:listenPort as its leader redirect hint");
            }
            String combinedEndpoint = nodeConfig.advertisedHost() + ":" + nodeConfig.listenPort();
            controller = new Controller(controllerConfig, combinedEndpoint);
            node = new DataNode(nodeConfig, controller.handler());
            Controller startedController = controller;
            DataNode startedNode = node;
            long nsRefreshMs = intEnv("STRATA_METRICS_NS_REFRESH_INTERVAL_MS", 10_000);
            long[] buckets = parseBucketsMs(env("STRATA_METRICS_REQUEST_DURATION_BUCKETS_MS", null),
                    new long[]{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000});
            AutoCloseable metrics = startMetrics("combined", reg -> {
                ServerMetrics.registerController(reg, startedController, nsRefreshMs);
                ServerMetrics.registerDataNode(reg, startedNode, nsRefreshMs);
                // The single (node) listener serves both planes, so observe there; the embedded controller
                // has no server of its own.
                startedNode.setRequestObserver(ServerMetrics.requestObserver(reg, buckets));
            }, () -> startedController.isLeader() && startedController.zkConnected() && startedNode.registered());
            return new Combined(controller, node, metrics);
        } catch (Exception e) {
            // a partial start must not leak the controller's ZK session or the node's listener
            closeQuietly(node);
            closeQuietly(controller);
            throw e;
        }
    }

    /** Co-resident controller + node + their shared metrics endpoint; closes node before controller on shutdown. */
    record Combined(Controller controller, DataNode node, AutoCloseable metrics) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            Exception failure = null;
            for (AutoCloseable c : new AutoCloseable[]{metrics, node, controller}) {
                try {
                    c.close();
                } catch (Exception e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best-effort cleanup on a partial start
            }
        }
    }

    /**
     * Starts the Prometheus metrics endpoint (unless STRATA_METRICS_ENABLED=false), registers the
     * role's domain metrics + JVM binders, and returns a handle that closes both. Returns a no-op
     * when metrics are disabled.
     */
    private static AutoCloseable startMetrics(String role, Consumer<MeterRegistry> registrar) throws IOException {
        return startMetrics(role, registrar, () -> true);
    }

    private static AutoCloseable startMetrics(String role, Consumer<MeterRegistry> registrar,
                                              BooleanSupplier ready) throws IOException {
        if (!boolEnv("STRATA_METRICS_ENABLED", true)) {
            log.info("metrics endpoint disabled (STRATA_METRICS_ENABLED=false)");
            return () -> { };
        }
        StrataMetrics metrics = new StrataMetrics(role);
        try {
            registrar.accept(metrics.registry());
            MetricsServer endpoint = MetricsServer.start(intEnv("STRATA_METRICS_PORT", 9_300), metrics, ready);
            return () -> {
                endpoint.close();
                metrics.close();
            };
        } catch (IOException | RuntimeException e) {
            metrics.close();
            throw e;
        }
    }

    /** Blocks until the JVM is asked to stop, then closes {@code resources} (in order) from a hook. */
    private static void awaitShutdown(String what, AutoCloseable... resources) throws InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down {}", what);
            for (AutoCloseable c : resources) {
                try {
                    c.close();
                } catch (Exception e) {
                    log.warn("error closing a {} resource", what, e);
                }
            }
            stopped.countDown();
        }, "strata-shutdown"));
        stopped.await();
        log.info("{} stopped", what);
    }

    private static List<String> endpoints(String csv) {
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }

    static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    private static ControllerConfig.MetadataBackendConfig metadataBackendConfig() {
        String backend = env("STRATA_CONTROLLER_BACKEND", "zk");
        if (!"namespace-log".equalsIgnoreCase(backend)) {
            return new ControllerConfig.MetadataBackendConfig(backend, 3, 2, false,
                    4 * 1024 * 1024, 30_000, true, 0, 4 * 1024 * 1024);
        }
        return new ControllerConfig.MetadataBackendConfig("namespace-log",
                intEnv("STRATA_CONTROLLER_LOG_RF", 3),
                intEnv("STRATA_CONTROLLER_LOG_ACK", 2),
                boolEnv("STRATA_CONTROLLER_LOG_FSYNC", false),
                intEnv("STRATA_CONTROLLER_LOG_COMPACT_BYTES", 4 * 1024 * 1024),
                intEnv("STRATA_CONTROLLER_LOG_COMPACT_INTERVAL_MS", 30_000),
                boolEnv("STRATA_CONTROLLER_LOG_ORPHAN_GC", true),
                intEnv("STRATA_CONTROLLER_LOG_RETENTION_MS", 0),
                intEnv("STRATA_CONTROLLER_LOG_READ_CHUNK_BYTES", 4 * 1024 * 1024));
    }

    private static String required(String key) {
        String v = env(key, null);
        if (v == null) {
            System.err.println("missing required environment variable " + key);
            System.exit(2);
        }
        return v;
    }

    private static int intEnv(String key, int def) {
        String v = env(key, null);
        return v == null ? def : Integer.parseInt(v);
    }

    private static int requiredIntEnv(String key) {
        return Integer.parseInt(required(key));
    }

    private static long longEnv(String key, long def) {
        String v = env(key, null);
        return v == null ? def : Long.parseLong(v);
    }

    private static boolean boolEnv(String key, boolean def) {
        return parseBoolEnv(key, env(key, null), def);
    }

    static boolean parseBoolEnv(String key, String value, boolean def) {
        String v = value == null || value.isBlank() ? null : value.trim();
        if (v == null) {
            return def;
        }
        return switch (v) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException(
                    key + " must be 'true' or 'false' but was '" + v + "'");
        };
    }

    /**
     * Parses a comma-separated list of positive, strictly-ascending millisecond bucket boundaries
     * for SLO histogram configuration. Returns {@code def} when {@code csv} is null or blank.
     * Throws {@link IllegalArgumentException} for non-positive values or non-ascending order.
     */
    static long[] parseBucketsMs(String csv, long[] def) {
        if (csv == null || csv.isBlank()) {
            return def;
        }
        String[] parts = csv.split(",");
        long[] out = new long[parts.length];
        long prev = 0;
        for (int i = 0; i < parts.length; i++) {
            long v = Long.parseLong(parts[i].trim());
            if (v <= 0) {
                throw new IllegalArgumentException("bucket must be positive ms: " + v);
            }
            if (v <= prev) {
                throw new IllegalArgumentException("buckets must be strictly ascending: " + csv);
            }
            out[i] = v;
            prev = v;
        }
        return out;
    }

    private StrataServer() {
    }
}
