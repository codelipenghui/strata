package io.strata.server;

import io.micrometer.core.instrument.MeterRegistry;
import io.strata.meta.ControllerConfig;
import io.strata.meta.Controller;
import io.strata.metrics.MetricsServer;
import io.strata.metrics.StrataMetrics;
import io.strata.node.DataNodeConfig;
import io.strata.node.DataNode;
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
                .withReconcileIntervalMs(intEnv("STRATA_REPAIR_RECONCILE_INTERVAL_MS", 15_000));
        // Namespace sharding is OPT-IN (default off = single global leader). When enabled, namespaces are
        // rendezvous-assigned across STRATA_CONTROLLER_ENDPOINTS so each controller node owns a shard. The
        // client (ControllerClient) is sharding-aware: it keeps one connection per owner and routes each op
        // to its namespace's owner, learning owners from NOT_LEADER redirect hints — so concurrent ops
        // across owners do not thrash a single connection. Off by default to gate fleet-wide rollout.
        if (Boolean.parseBoolean(env("STRATA_CONTROLLER_SHARDING", "false"))) {
            config = config.withControllerEndpoints(endpoints(required("STRATA_CONTROLLER_ENDPOINTS")),
                    intEnv("STRATA_CONTROLLER_REPLICA_COUNT", 3));
        }
        Controller service = new Controller(config);
        log.info("controller started: endpoint={} zk={} leader={}",
                service.endpoint(), config.zkConnect(), service.isLeader());
        AutoCloseable metrics = null;
        try {
            metrics = startMetrics("controller", reg -> {
                ServerMetrics.registerController(reg, service);
                service.setRequestObserver(ServerMetrics.requestObserver(reg));
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
                intEnv("STRATA_INVENTORY_INTERVAL_MS", 30_000))
                .withNodeId(requiredIntEnv("STRATA_NODE_ID"));
        DataNode node = new DataNode(config);
        log.info("data node started: endpoint={} dataDir={} controller={}",
                node.endpoint(), config.dataDir(), config.controllerEndpoints());
        AutoCloseable metrics = null;
        try {
            metrics = startMetrics("data-node", reg -> {
                ServerMetrics.registerDataNode(reg, node);
                node.setRequestObserver(ServerMetrics.requestObserver(reg));
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
                .withReconcileIntervalMs(intEnv("STRATA_REPAIR_RECONCILE_INTERVAL_MS", 15_000));
        // Namespace sharding is OPT-IN (default off = single global leader). The sharding-aware client
        // (ControllerClient) keeps one connection per owner and routes each op to its namespace's owner,
        // so it does not thrash a single connection. Off by default to gate rollout. See runController /
        // STRATA_CONTROLLER_SHARDING.
        if (Boolean.parseBoolean(env("STRATA_CONTROLLER_SHARDING", "false"))) {
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
                intEnv("STRATA_INVENTORY_INTERVAL_MS", 30_000))
                .withNodeId(requiredIntEnv("STRATA_NODE_ID"));
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
            AutoCloseable metrics = startMetrics("combined", reg -> {
                ServerMetrics.registerController(reg, startedController);
                ServerMetrics.registerDataNode(reg, startedNode);
                // The single (node) listener serves both planes, so observe there; the embedded controller
                // has no server of its own.
                startedNode.setRequestObserver(ServerMetrics.requestObserver(reg));
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
        String v = env(key, null);
        return v == null ? def : Boolean.parseBoolean(v);
    }

    private StrataServer() {
    }
}
