package io.strata.server;

import io.micrometer.core.instrument.MeterRegistry;
import io.strata.meta.MetaConfig;
import io.strata.meta.MetadataService;
import io.strata.metrics.MetricsServer;
import io.strata.metrics.StrataMetrics;
import io.strata.node.NodeConfig;
import io.strata.node.StorageNode;
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
 * Production entrypoint. {@code strata node} runs a storage node; {@code strata meta} runs a
 * metadata service. The role is taken from the first argument (or {@code STRATA_ROLE}); everything
 * else comes from environment variables. Every value has a production-sane default except the
 * deployment-specific ones — a metadata service needs {@code STRATA_ZK_CONNECT}, a storage node
 * needs {@code STRATA_META_ENDPOINTS}. Blocks until SIGTERM/SIGINT, then closes the service cleanly.
 *
 * <p>This is a thin launcher around the already-tested {@link StorageNode}/{@link MetadataService}
 * lifecycles — it adds no behavior, only configuration loading and signal handling.
 */
public final class StrataServer {
    private static final Logger log = LoggerFactory.getLogger(StrataServer.class);

    public static void main(String[] args) throws Exception {
        String role = args.length > 0 ? args[0] : env("STRATA_ROLE", null);
        if (role == null) {
            System.err.println("usage: strata <node|meta|combined>   (or set STRATA_ROLE=node|meta|combined)");
            System.exit(2);
            return;
        }
        switch (role) {
            case "node" -> runNode();
            case "meta" -> runMeta();
            case "combined", "node,meta" -> runCombined();
            case "perf" -> StrataPerf.run(args);
            default -> {
                System.err.println("unknown role '" + role
                        + "': expected 'node', 'meta', 'combined', or 'perf'");
                System.exit(2);
            }
        }
    }

    private static void runMeta() throws Exception {
        MetaConfig config = new MetaConfig(
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
                .withAdvertisedHost(env("STRATA_ADVERTISED_HOST", hostname()));
        MetadataService service = new MetadataService(config);
        log.info("metadata service started: endpoint={} zk={} leader={}",
                service.endpoint(), config.zkConnect(), service.isLeader());
        AutoCloseable metrics = null;
        try {
            metrics = startMetrics("meta", reg -> {
                ServerMetrics.registerMeta(reg, service);
                service.setRequestObserver(ServerMetrics.requestObserver(reg));
            }, () -> service.isLeader() && service.zkConnected());
            awaitShutdown("metadata service", metrics, service);
        } catch (Exception e) {
            closeQuietly(metrics);
            closeQuietly(service);
            throw e;
        }
    }

    private static void runNode() throws Exception {
        String hostname = hostname();
        NodeConfig config = new NodeConfig(
                Path.of(env("STRATA_DATA_DIR", "/data")),
                intEnv("STRATA_LISTEN_PORT", 9_100),
                env("STRATA_ADVERTISED_HOST", hostname),
                null,
                endpoints(required("STRATA_META_ENDPOINTS")),
                env("STRATA_ZONE", "z0"),
                env("STRATA_RACK", "r0"),
                env("STRATA_HOST", hostname),
                longEnv("STRATA_CAPACITY_BYTES", 1L << 40),  // 1 TiB
                intEnv("STRATA_INVENTORY_INTERVAL_MS", 30_000));
        StorageNode node = new StorageNode(config);
        log.info("storage node started: endpoint={} dataDir={} meta={}",
                node.endpoint(), config.dataDir(), config.metadataEndpoints());
        AutoCloseable metrics = null;
        try {
            metrics = startMetrics("node", reg -> {
                ServerMetrics.registerNode(reg, node);
                node.setRequestObserver(ServerMetrics.requestObserver(reg));
            }, node::registered);
            awaitShutdown("storage node", metrics, node);
        } catch (Exception e) {
            closeQuietly(metrics);
            closeQuietly(node);
            throw e;
        }
    }

    /**
     * Co-resident mode: runs a {@link MetadataService} and a {@link StorageNode} in one JVM, so the
     * metadata plane scales with the data fleet and there is no separate {@code strata-meta} to
     * deploy. Every combined node hosts a meta instance; one is elected leader (the rest are warm
     * standbys) and serves all metadata RPCs — clients reach it via the {@code NOT_LEADER} redirect.
     * Both planes share one SCP listener on {@code STRATA_LISTEN_PORT} (default 9100): data opcodes go
     * to the storage node, metadata opcodes to the co-resident meta. {@code STRATA_META_ENDPOINTS}
     * lists the meta-eligible nodes (including this one) at that port.
     */
    private static void runCombined() throws Exception {
        String hostname = hostname();
        String advertisedHost = env("STRATA_ADVERTISED_HOST", hostname);
        MetaConfig metaConfig = new MetaConfig(
                required("STRATA_ZK_CONNECT"),
                intEnv("STRATA_LISTEN_PORT", 9_100),  // unused in embedded mode (meta shares the node's listener)
                intEnv("STRATA_HEARTBEAT_INTERVAL_MS", 3_000),
                intEnv("STRATA_LEASE_MS", 10_000),
                intEnv("STRATA_DEAD_GRACE_MS", 30_000),
                intEnv("STRATA_REPAIR_SCAN_INTERVAL_MS", 5_000),
                intEnv("STRATA_REPAIR_COMMAND_TIMEOUT_MS", 30_000))
                .withAdvertisedHost(advertisedHost);
        NodeConfig nodeConfig = new NodeConfig(
                Path.of(env("STRATA_DATA_DIR", "/data")),
                intEnv("STRATA_LISTEN_PORT", 9_100),
                advertisedHost,
                null,
                endpoints(required("STRATA_META_ENDPOINTS")),
                env("STRATA_ZONE", "z0"),
                env("STRATA_RACK", "r0"),
                env("STRATA_HOST", hostname),
                longEnv("STRATA_CAPACITY_BYTES", 1L << 40),  // 1 TiB
                intEnv("STRATA_INVENTORY_INTERVAL_MS", 30_000));
        Combined combined = startCombined(metaConfig, nodeConfig);
        log.info("combined node started: scp={} zk={}", combined.node().endpoint(), metaConfig.zkConnect());
        awaitShutdown("combined node", combined);
    }

    /**
     * Builds and starts a co-resident metadata service + storage node behind a single combined
     * metrics endpoint, and returns a handle that closes both (node first, then meta). The meta is
     * built first so it can join the leader latch before the node tries to register.
     */
    static Combined startCombined(MetaConfig metaConfig, NodeConfig nodeConfig) throws Exception {
        MetadataService meta = null;
        StorageNode node = null;
        try {
            // One SCP listener for both planes: the meta runs embedded (no own port), served on the
            // node's listener which routes metadata opcodes to it. The meta advertises the node's
            // reachable endpoint as the NOT_LEADER redirect hint, so combined mode needs a FIXED node
            // port — an ephemeral (0) port would advertise an unreachable ":0" hint before the real
            // port is even bound.
            if (nodeConfig.listenPort() == 0) {
                throw new IllegalArgumentException(
                        "combined mode requires a fixed node listenPort (not ephemeral 0): the embedded "
                                + "meta advertises advertisedHost:listenPort as its leader redirect hint");
            }
            String combinedEndpoint = nodeConfig.advertisedHost() + ":" + nodeConfig.listenPort();
            meta = new MetadataService(metaConfig, combinedEndpoint);
            node = new StorageNode(nodeConfig, meta.handler());
            MetadataService startedMeta = meta;
            StorageNode startedNode = node;
            AutoCloseable metrics = startMetrics("combined", reg -> {
                ServerMetrics.registerMeta(reg, startedMeta);
                ServerMetrics.registerNode(reg, startedNode);
                // The single (node) listener serves both planes, so observe there; the embedded meta
                // has no server of its own.
                startedNode.setRequestObserver(ServerMetrics.requestObserver(reg));
            }, () -> startedMeta.isLeader() && startedMeta.zkConnected() && startedNode.registered());
            return new Combined(meta, node, metrics);
        } catch (Exception e) {
            // a partial start must not leak the meta's ZK session or the node's listener
            closeQuietly(node);
            closeQuietly(meta);
            throw e;
        }
    }

    /** Co-resident meta + node + their shared metrics endpoint; closes node before meta on shutdown. */
    record Combined(MetadataService meta, StorageNode node, AutoCloseable metrics) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            Exception failure = null;
            for (AutoCloseable c : new AutoCloseable[]{metrics, node, meta}) {
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

    private static long longEnv(String key, long def) {
        String v = env(key, null);
        return v == null ? def : Long.parseLong(v);
    }

    private static boolean boolEnv(String key, boolean def) {
        String v = env(key, null);
        return v == null ? def : Boolean.parseBoolean(v);
    }

    private StrataServer() {
    }
}
