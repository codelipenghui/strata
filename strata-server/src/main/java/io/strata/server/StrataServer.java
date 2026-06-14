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
            System.err.println("usage: strata <node|meta>   (or set STRATA_ROLE=node|meta)");
            System.exit(2);
            return;
        }
        switch (role) {
            case "node" -> runNode();
            case "meta" -> runMeta();
            default -> {
                System.err.println("unknown role '" + role + "': expected 'node' or 'meta'");
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
                intEnv("STRATA_REPAIR_COMMAND_TIMEOUT_MS", 30_000));
        MetadataService service = new MetadataService(config);
        log.info("metadata service started: endpoint={} zk={} leader={}",
                service.endpoint(), config.zkConnect(), service.isLeader());
        AutoCloseable metrics = startMetrics("meta", reg -> ServerMetrics.registerMeta(reg, service));
        awaitShutdown("metadata service", metrics, service);
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
        AutoCloseable metrics = startMetrics("node", reg -> ServerMetrics.registerNode(reg, node));
        awaitShutdown("storage node", metrics, node);
    }

    /**
     * Starts the Prometheus metrics endpoint (unless STRATA_METRICS_ENABLED=false), registers the
     * role's domain metrics + JVM binders, and returns a handle that closes both. Returns a no-op
     * when metrics are disabled.
     */
    private static AutoCloseable startMetrics(String role, Consumer<MeterRegistry> registrar) throws IOException {
        if (!boolEnv("STRATA_METRICS_ENABLED", true)) {
            log.info("metrics endpoint disabled (STRATA_METRICS_ENABLED=false)");
            return () -> { };
        }
        StrataMetrics metrics = new StrataMetrics(role);
        registrar.accept(metrics.registry());
        MetricsServer endpoint = MetricsServer.start(intEnv("STRATA_METRICS_PORT", 9_300), metrics);
        return () -> {
            endpoint.close();
            metrics.close();
        };
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

    private static String env(String key, String def) {
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
