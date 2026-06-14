package io.strata.server;

import io.strata.meta.MetaConfig;
import io.strata.meta.MetadataService;
import io.strata.node.NodeConfig;
import io.strata.node.StorageNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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
        awaitShutdown(service, "metadata service");
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
        awaitShutdown(node, "storage node");
    }

    /** Blocks until the JVM is asked to stop, then closes {@code service} from a shutdown hook. */
    private static void awaitShutdown(AutoCloseable service, String what) throws InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down {}", what);
            try {
                service.close();
            } catch (Exception e) {
                log.warn("error closing {}", what, e);
            } finally {
                stopped.countDown();
            }
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

    private StrataServer() {
    }
}
