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
import java.util.function.Function;
import java.util.function.Supplier;

import static io.strata.common.EnvConfig.intEnv;
import static io.strata.common.EnvConfig.longEnv;

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
    private static final Function<String, String> SYSTEM_ENV = System::getenv;

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
        // Endpoint a standby returns as the NOT_LEADER redirect hint. In containers/k8s the
        // hostname() default is the container/pod id — set STRATA_ADVERTISED_HOST to a name
        // clients can resolve (the service/DNS name) whenever more than one replica runs.
        ControllerConfig config = standaloneControllerConfigFromEnv(
                () -> env("STRATA_ADVERTISED_HOST", hostname()));
        RequestMetricsConfig requestMetrics = requestMetricsConfigFromEnv();
        Controller service = new Controller(config);
        log.info("controller started: endpoint={} zk={} leader={}",
                service.endpoint(), config.zkConnect(), service.isLeader());
        AutoCloseable metrics = null;
        try {
            metrics = startMetrics("controller", reg -> {
                ServerMetrics.registerController(reg, service, requestMetrics.namespaceRefreshMs());
                service.setRequestObserver(ServerMetrics.requestObserver(
                        reg, requestMetrics.durationBucketsMs(), requestMetrics.latencySampleRate()));
            }, () -> service.isLeader() && service.zkConnected());
            awaitShutdown("controller", metrics, service);
        } catch (Exception e) {
            closeQuietly("controller metrics", metrics);
            closeQuietly("controller", service);
            throw e;
        }
    }

    private static void runDataNode() throws Exception {
        String hostname = hostname();
        DataNodeConfig config = dataNodeConfigFromEnv(hostname,
                () -> env("STRATA_ADVERTISED_HOST", hostname));
        RequestMetricsConfig requestMetrics = requestMetricsConfigFromEnv();
        DataNode node = new DataNode(config);
        log.info("data node started: endpoint={} dataDir={} controller={}",
                node.endpoint(), config.dataDir(), config.controllerEndpoints());
        AutoCloseable metrics = null;
        try {
            metrics = startMetrics("data-node", reg -> {
                ServerMetrics.registerDataNode(reg, node, requestMetrics.namespaceRefreshMs());
                node.setRequestObserver(ServerMetrics.requestObserver(
                        reg, requestMetrics.durationBucketsMs(), requestMetrics.latencySampleRate()));
            }, node::registered);
            awaitShutdown("data node", metrics, node);
        } catch (Exception e) {
            closeQuietly("data-node metrics", metrics);
            closeQuietly("data node", node);
            throw e;
        }
    }

    /**
     * Co-resident mode: runs a {@link Controller} and a {@link DataNode} in one JVM, so the
     * metadata plane scales with the data fleet and there is no separate {@code strata-meta} to
     * deploy. Every combined node hosts a controller instance and may serve the namespaces assigned to it;
     * one is separately elected cluster coordinator for global work and dead-owner assignment rotation.
     * Clients reach the persisted namespace owner via the {@code NOT_LEADER} redirect.
     * Both planes share one SCP listener on {@code STRATA_LISTEN_PORT} (default 9100): data opcodes go
     * to the data node, metadata opcodes to the co-resident controller. {@code STRATA_CONTROLLER_ENDPOINTS}
     * lists the controller-eligible nodes (including this one) at that port.
     */
    private static void runCombined() throws Exception {
        String hostname = hostname();
        String advertisedHost = env("STRATA_ADVERTISED_HOST", hostname);
        ControllerConfig controllerConfig = combinedControllerConfigFromEnv(() -> advertisedHost);
        DataNodeConfig nodeConfig = dataNodeConfigFromEnv(hostname, () -> advertisedHost);
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
        return startCombined(controllerConfig, nodeConfig,
                (registrar, ready) -> startMetrics("combined", registrar, ready));
    }

    static Combined startCombined(ControllerConfig controllerConfig, DataNodeConfig nodeConfig,
                                  CombinedMetricsStarter metricsStarter) throws Exception {
        validateCombinedListenPort(nodeConfig);
        RequestMetricsConfig requestMetrics = requestMetricsConfigFromEnv();
        return startCombined(controllerConfig, nodeConfig, requestMetrics, metricsStarter);
    }

    private static Combined startCombined(ControllerConfig controllerConfig, DataNodeConfig nodeConfig,
                                          RequestMetricsConfig requestMetrics,
                                          CombinedMetricsStarter metricsStarter) throws Exception {
        Controller controller = null;
        DataNode node = null;
        try {
            // One SCP listener for both planes: the controller runs embedded (no own port), served on the
            // node's listener which routes metadata opcodes to it. The controller advertises the node's
            // reachable endpoint as the NOT_LEADER redirect hint, so combined mode needs a FIXED node
            // port — an ephemeral (0) port would advertise an unreachable ":0" hint before the real
            // port is even bound.
            String combinedEndpoint = nodeConfig.advertisedHost() + ":" + nodeConfig.listenPort();
            controller = new Controller(controllerConfig, combinedEndpoint);
            node = new DataNode(nodeConfig, controller.handler());
            Controller startedController = controller;
            DataNode startedNode = node;
            AutoCloseable metrics = metricsStarter.start(reg -> {
                ServerMetrics.registerController(reg, startedController, requestMetrics.namespaceRefreshMs());
                ServerMetrics.registerDataNode(reg, startedNode, requestMetrics.namespaceRefreshMs());
                // The single (node) listener serves both planes, so observe there; the embedded controller
                // has no server of its own.
                startedNode.setRequestObserver(ServerMetrics.requestObserver(
                        reg, requestMetrics.durationBucketsMs(), requestMetrics.latencySampleRate()));
            }, () -> startedController.isLeader() && startedController.zkConnected() && startedNode.registered());
            return new Combined(controller, node, metrics);
        } catch (Exception e) {
            // a partial start must not leak the controller's ZK session or the node's listener
            throw cleanupAfterFailedCombinedStart(e, node, controller);
        }
    }

    private static void validateCombinedListenPort(DataNodeConfig nodeConfig) {
        if (nodeConfig.listenPort() == 0) {
            throw new IllegalArgumentException(
                    "combined mode requires a fixed node listenPort (not ephemeral 0): the embedded "
                            + "controller advertises advertisedHost:listenPort as its leader redirect hint");
        }
    }

    private static ControllerConfig standaloneControllerConfigFromEnv(Supplier<String> advertisedHost) {
        return standaloneControllerConfigFromEnv(advertisedHost, SYSTEM_ENV);
    }

    static ControllerConfig standaloneControllerConfigFromEnv(Supplier<String> advertisedHost,
                                                               Function<String, String> environment) {
        return controllerConfigFromEnv(9_200, advertisedHost, environment);
    }

    private static ControllerConfig combinedControllerConfigFromEnv(Supplier<String> advertisedHost) {
        return combinedControllerConfigFromEnv(advertisedHost, SYSTEM_ENV);
    }

    static ControllerConfig combinedControllerConfigFromEnv(Supplier<String> advertisedHost,
                                                             Function<String, String> environment) {
        return controllerConfigFromEnv(9_100, advertisedHost, environment);
    }

    private static ControllerConfig controllerConfigFromEnv(int defaultListenPort, Supplier<String> advertisedHost,
                                                            Function<String, String> environment) {
        ControllerConfig config = new ControllerConfig(
                required(environment, "STRATA_ZK_CONNECT"),
                intEnvFrom(environment, "STRATA_LISTEN_PORT", defaultListenPort),
                intEnvFrom(environment, "STRATA_HEARTBEAT_INTERVAL_MS", 3_000),
                intEnvFrom(environment, "STRATA_LEASE_MS", 10_000),
                intEnvFrom(environment, "STRATA_DEAD_GRACE_MS", 30_000),
                intEnvFrom(environment, "STRATA_REPAIR_SCAN_INTERVAL_MS", 5_000),
                intEnvFrom(environment, "STRATA_REPAIR_COMMAND_TIMEOUT_MS", 30_000))
                .withAdvertisedHost(advertisedHost.get())
                .withReconcileIntervalMs(intEnvFrom(environment, "STRATA_REPAIR_RECONCILE_INTERVAL_MS", 15_000))
                .withVerifyIntervalMs(intEnvFrom(environment, "STRATA_VERIFY_INTERVAL_MS", 2_000))
                .withVerifyBatchSize(intEnvFrom(environment, "STRATA_VERIFY_BATCH_SIZE", 256))
                .withSystemVerifyIntervalMs(intEnvFrom(environment, "STRATA_SYSTEM_VERIFY_INTERVAL_MS", 30_000))
                .withDeletedTombstoneTtlMs(longEnvFrom(
                        environment, "STRATA_CONTROLLER_DELETED_TOMBSTONE_TTL_MS", 600_000))
                .withMaxCommandsPerHeartbeat(intEnvFrom(
                        environment, "STRATA_CONTROLLER_MAX_COMMANDS_PER_HEARTBEAT", 16))
                .withZkSessionTimeoutMs(intEnvFrom(environment, "STRATA_CONTROLLER_ZK_SESSION_TIMEOUT_MS",
                        ControllerConfig.DEFAULT_ZK_SESSION_TIMEOUT_MS))
                .withZkRetryBaseMs(intEnvFrom(environment, "STRATA_CONTROLLER_ZK_RETRY_BASE_MS", 100))
                .withZkRetryMaxRetries(intEnvFrom(environment, "STRATA_CONTROLLER_ZK_RETRY_MAX", 5))
                .withMetadataBackend(metadataBackendConfig(environment));
        // A multi-controller bootstrap set always enables persisted namespace ownership. There is no
        // production-safe static multi-endpoint mode to fall back to. Preserve a single configured endpoint
        // too: Controller validates that it is this process's advertised endpoint, so a typo cannot silently
        // degrade several processes into independent global owners.
        List<String> controllerEndpoints =
                endpoints(env(environment, "STRATA_CONTROLLER_ENDPOINTS", ""));
        if (!controllerEndpoints.isEmpty()) {
            int defaultReplicaCount = controllerEndpoints.size() == 1
                    ? 1 : Math.min(3, controllerEndpoints.size());
            config = config.withControllerEndpoints(controllerEndpoints,
                    intEnvFrom(environment, "STRATA_CONTROLLER_REPLICA_COUNT", defaultReplicaCount));
        }
        return config;
    }

    private static DataNodeConfig dataNodeConfigFromEnv(String hostname, Supplier<String> advertisedHost) {
        return new DataNodeConfig(
                Path.of(env("STRATA_DATA_DIR", "/data")),
                intEnv("STRATA_LISTEN_PORT", 9_100),
                advertisedHost.get(),
                null,
                endpoints(required("STRATA_CONTROLLER_ENDPOINTS")),
                env("STRATA_ZONE", "z0"),
                env("STRATA_RACK", "r0"),
                env("STRATA_HOST", hostname),
                longEnv("STRATA_CAPACITY_BYTES", 1L << 40),  // 1 TiB
                intEnv("STRATA_SCRUB_INTERVAL_MS", 300_000)) // full re-CRC every 5 min
                .withNodeId(requiredIntEnv("STRATA_NODE_ID"))
                .withOrphanGraceMs(longEnv("STRATA_ORPHAN_GRACE_MS", DataNodeConfig.DEFAULT_ORPHAN_GRACE_MS))
                .withOrphanScanIntervalMs(longEnv("STRATA_ORPHAN_SCAN_INTERVAL_MS",
                        DataNodeConfig.DEFAULT_ORPHAN_SCAN_INTERVAL_MS))
                .withOrphanStartupGraceMs(longEnv("STRATA_ORPHAN_STARTUP_GRACE_MS",
                        DataNodeConfig.DEFAULT_ORPHAN_STARTUP_GRACE_MS))
                .withOrphanConfirmTimeoutMs(intEnv("STRATA_ORPHAN_CONFIRM_TIMEOUT_MS",
                        DataNodeConfig.DEFAULT_ORPHAN_CONFIRM_TIMEOUT_MS))
                .withOrphanDeleteMaxConfirmedPerNamespacePerPass(
                        intEnv("STRATA_ORPHAN_DELETE_MAX_CONFIRMED_PER_NAMESPACE_PER_PASS",
                                DataNodeConfig.DEFAULT_ORPHAN_DELETE_MAX_CONFIRMED_PER_NAMESPACE_PER_PASS))
                .withOrphanDeleteMaxNamespacePercentPerPass(
                        intEnv("STRATA_ORPHAN_DELETE_MAX_NAMESPACE_PERCENT_PER_PASS",
                                DataNodeConfig.DEFAULT_ORPHAN_DELETE_MAX_NAMESPACE_PERCENT_PER_PASS))
                .withOrphanDeleteMaxConfirmedPerNodePass(
                        intEnv("STRATA_ORPHAN_DELETE_MAX_CONFIRMED_PER_NODE_PASS",
                                DataNodeConfig.DEFAULT_ORPHAN_DELETE_MAX_CONFIRMED_PER_NODE_PASS))
                .withOrphanDeleteMaxCumulativePerNamespace(
                        intEnv("STRATA_ORPHAN_DELETE_MAX_CUMULATIVE_PER_NAMESPACE",
                                DataNodeConfig.DEFAULT_ORPHAN_DELETE_MAX_CUMULATIVE_PER_NAMESPACE))
                .withOrphanDeleteMaxCumulativePerNode(
                        intEnv("STRATA_ORPHAN_DELETE_MAX_CUMULATIVE_PER_NODE",
                                DataNodeConfig.DEFAULT_ORPHAN_DELETE_MAX_CUMULATIVE_PER_NODE))
                .withControlCallTimeoutMs(intEnv("STRATA_CONTROL_CALL_TIMEOUT_MS", 10_000))
                .withControlCommandLimits(intEnv("STRATA_NODE_COMMAND_PARALLELISM", 8),
                        intEnv("STRATA_NODE_MAX_QUEUED_COMMANDS", 1024))
                .withRepairFetchBytes(intEnv("STRATA_REPAIR_FETCH_BYTES", 4 * 1024 * 1024))
                .withDeleteMaxConcurrent(intEnv("STRATA_DELETE_MAX_CONCURRENT", 1))
                .withDeleteMinIntervalMs(longEnv("STRATA_DELETE_MIN_INTERVAL_MS", 50))
                .withChunkStoreConfig(chunkStoreConfigFromEnv());
    }

    private static ChunkStoreConfig chunkStoreConfigFromEnv() {
        return new ChunkStoreConfig(
                intEnv("STRATA_MAX_REQUEST_BYTES", ChunkStoreConfig.DEFAULT_MAX_REQUEST_BYTES),
                longEnv("STRATA_GROUPCOMMIT_DRAIN_TIMEOUT_MS",
                        ChunkStoreConfig.DEFAULT_GROUP_COMMIT_DRAIN_TIMEOUT_MS),
                longEnv("STRATA_GROUPCOMMIT_MIN_ACCUMULATION_NANOS",
                        ChunkStoreConfig.DEFAULT_GROUP_COMMIT_MIN_ACCUMULATION_NANOS),
                longEnv("STRATA_GROUPCOMMIT_MAX_ACCUMULATION_NANOS",
                        ChunkStoreConfig.DEFAULT_GROUP_COMMIT_MAX_ACCUMULATION_NANOS),
                boolEnv("STRATA_SEAL_FSYNC", ChunkStoreConfig.DEFAULT_SEAL_FSYNC),
                longEnv("STRATA_BG_FLUSH_INTERVAL_MS", ChunkStoreConfig.DEFAULT_BACKGROUND_FLUSH_INTERVAL_MS),
                longEnv("STRATA_BG_FLUSH_THRESHOLD_BYTES",
                        ChunkStoreConfig.DEFAULT_BACKGROUND_FLUSH_THRESHOLD_BYTES),
                longEnv("STRATA_SLOW_APPEND_LOG_MS", ChunkStoreConfig.DEFAULT_SLOW_APPEND_LOG_MS),
                longEnv("STRATA_SLOW_MUTATION_LOG_MS", ChunkStoreConfig.DEFAULT_SLOW_MUTATION_LOG_MS),
                intEnv("STRATA_FILE_CHANNEL_CACHE_MAX_SIZE", ChunkStoreConfig.DEFAULT.channelCacheMaxSize()),
                intEnv("STRATA_MAX_OPEN_CHUNK_LEDGER_ENTRIES",
                        ChunkStoreConfig.DEFAULT.maxOpenChunkLedgerEntries()));
    }

    record RequestMetricsConfig(long namespaceRefreshMs, long[] durationBucketsMs, int latencySampleRate) {
        RequestMetricsConfig {
            if (namespaceRefreshMs <= 0) {
                throw new IllegalArgumentException("namespaceRefreshMs must be positive: " + namespaceRefreshMs);
            }
            if (durationBucketsMs == null || durationBucketsMs.length == 0) {
                throw new IllegalArgumentException("durationBucketsMs must be non-null and non-empty");
            }
            if (latencySampleRate <= 0) {
                throw new IllegalArgumentException("latencySampleRate must be positive: " + latencySampleRate);
            }
        }
    }

    @FunctionalInterface
    interface CombinedMetricsStarter {
        AutoCloseable start(Consumer<MeterRegistry> registrar, BooleanSupplier ready) throws Exception;
    }

    private static RequestMetricsConfig requestMetricsConfigFromEnv() {
        return requestMetricsConfigFromEnv(SYSTEM_ENV);
    }

    static RequestMetricsConfig requestMetricsConfigFromEnv(Function<String, String> environment) {
        return new RequestMetricsConfig(
                parsePositiveIntEnv("STRATA_METRICS_NS_REFRESH_INTERVAL_MS",
                        env(environment, "STRATA_METRICS_NS_REFRESH_INTERVAL_MS", null), 10_000),
                parseBucketsMs(env(environment, "STRATA_METRICS_REQUEST_DURATION_BUCKETS_MS", null),
                        new long[]{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}),
                requestLatencySampleRate(environment));
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

    private static Exception cleanupAfterFailedCombinedStart(Exception startupFailure,
                                                             AutoCloseable node, AutoCloseable controller) {
        closeQuietly("data node", node);
        closeQuietly("controller", controller);
        return startupFailure;
    }

    private static void closeQuietly(String resource, AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception e) {
                log.warn("error closing {} after failed startup", resource, e);
            }
        }
    }

    /**
     * Starts the Prometheus metrics endpoint (unless STRATA_METRICS_ENABLED=false), registers the
     * role's domain metrics + JVM binders, and returns a handle that closes both. Returns a no-op
     * when metrics are disabled.
     */
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
        return env(SYSTEM_ENV, key, def);
    }

    private static String env(Function<String, String> environment, String key, String def) {
        String v = environment.apply(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    private static ControllerConfig.MetadataBackendConfig metadataBackendConfig(
            Function<String, String> environment) {
        String backend = env(environment, "STRATA_CONTROLLER_BACKEND", "zk");
        if (!"namespace-log".equalsIgnoreCase(backend)) {
            return new ControllerConfig.MetadataBackendConfig(backend, 3, 2, false,
                    4 * 1024 * 1024, 30_000, true,
                    ControllerConfig.DEFAULT_NAMESPACE_LOG_RETENTION_MS, 4 * 1024 * 1024,
                    ControllerConfig.DEFAULT_NAMESPACE_LOG_CHUNK_ROLL_BYTES);
        }
        return new ControllerConfig.MetadataBackendConfig("namespace-log",
                intEnvFrom(environment, "STRATA_CONTROLLER_LOG_RF", 3),
                intEnvFrom(environment, "STRATA_CONTROLLER_LOG_ACK", 2),
                boolEnvFrom(environment, "STRATA_CONTROLLER_LOG_FSYNC", false),
                intEnvFrom(environment, "STRATA_CONTROLLER_LOG_COMPACT_BYTES", 4 * 1024 * 1024),
                intEnvFrom(environment, "STRATA_CONTROLLER_LOG_COMPACT_INTERVAL_MS", 30_000),
                boolEnvFrom(environment, "STRATA_CONTROLLER_LOG_ORPHAN_GC", true),
                intEnvFrom(environment, "STRATA_CONTROLLER_LOG_RETENTION_MS",
                        ControllerConfig.DEFAULT_NAMESPACE_LOG_RETENTION_MS),
                intEnvFrom(environment, "STRATA_CONTROLLER_LOG_READ_CHUNK_BYTES", 4 * 1024 * 1024),
                longEnvFrom(environment, "STRATA_CONTROLLER_LOG_CHUNK_ROLL_BYTES",
                        ControllerConfig.DEFAULT_NAMESPACE_LOG_CHUNK_ROLL_BYTES));
    }

    private static String required(String key) {
        return required(SYSTEM_ENV, key);
    }

    private static String required(Function<String, String> environment, String key) {
        String v = env(environment, key, null);
        if (v == null) {
            System.err.println("missing required environment variable " + key);
            System.exit(2);
        }
        return v;
    }

    private static int requiredIntEnv(String key) {
        return Integer.parseInt(required(key));
    }

    private static boolean boolEnv(String key, boolean def) {
        return boolEnvFrom(SYSTEM_ENV, key, def);
    }

    private static boolean boolEnvFrom(Function<String, String> environment, String key, boolean def) {
        return parseBoolEnv(key, env(environment, key, null), def);
    }

    private static int intEnvFrom(Function<String, String> environment, String key, int def) {
        String value = env(environment, key, null);
        return value == null ? def : Integer.parseInt(value);
    }

    private static long longEnvFrom(Function<String, String> environment, String key, long def) {
        String value = env(environment, key, null);
        return value == null ? def : Long.parseLong(value);
    }

    private static int requestLatencySampleRate(Function<String, String> environment) {
        return parsePositiveIntEnv("STRATA_METRICS_REQUEST_LATENCY_SAMPLE_RATE",
                env(environment, "STRATA_METRICS_REQUEST_LATENCY_SAMPLE_RATE", null),
                ServerMetrics.DEFAULT_REQUEST_LATENCY_SAMPLE_RATE);
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

    static int parsePositiveIntEnv(String key, String value, int def) {
        String v = value == null || value.isBlank() ? null : value.trim();
        if (v == null) {
            return def;
        }
        int parsed = Integer.parseInt(v);
        if (parsed <= 0) {
            throw new IllegalArgumentException(key + " must be positive but was " + parsed);
        }
        return parsed;
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
