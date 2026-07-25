package io.strata.it;

import io.strata.meta.Controller;
import io.strata.meta.ControllerConfig;
import io.strata.meta.ZkMetadataStore;
import io.strata.node.DataNode;
import io.strata.node.DataNodeConfig;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * In-process cluster for integration tests: embedded ZooKeeper + controller + N data nodes.
 * This is the primary correctness layer (tech design §16) — real sockets, real disk,
 * deterministic fault injection by killing components.
 */
final class MiniCluster implements AutoCloseable {
    TestingServer zk;             // null when an external (containerized) ZK is supplied
    /** Currently-live controllers only. Closed instances are removed immediately. */
    final List<Controller> metas = new ArrayList<>();
    Controller meta;         // first currently-live slot in stable slot order — legacy accessor
    final List<DataNode> nodes = new ArrayList<>();
    final Path root;
    private final BiFunction<String, Integer, ControllerConfig> metaConfigFactory;
    private final int metadataServiceCount;
    /** Stable controller slots let sharded tests kill/restart one fixed endpoint without shifting identity. */
    private final Controller[] metaSlots;
    /** Last endpoint bound by each slot; retained while that slot is stopped. */
    private final String[] metaSlotEndpoints;
    private String zkConnect;
    // Node ids are now externally supplied (the ZK allocator was removed); each registering node
    // needs a unique id >= 1, stable across restarts on the same dataDir. We hand out ids from a
    // monotonic counter so two concurrently-live nodes never collide; restartNode reuses the old id.
    private int nextNodeId = 1;

    MiniCluster(int nodeCount) throws Exception {
        this(nodeCount, null, 1);
    }

    MiniCluster(int nodeCount, String zkConnectOverride) throws Exception {
        this(nodeCount, zkConnectOverride, 1);
    }

    /** zkConnectOverride lets chaos tests supply a containerized ZooKeeper. */
    MiniCluster(int nodeCount, String zkConnectOverride, int metaCount) throws Exception {
        this(nodeCount, zkConnectOverride, metaCount, (zk, idx) -> ControllerConfig.forTests(zk));
    }

    static MiniCluster namespaceLog(int nodeCount) throws Exception {
        return new MiniCluster(nodeCount, null, 1,
                (zk, idx) -> ControllerConfig.forTests(zk).withNamespaceLogBackend());
    }

    /**
     * A namespace-SHARDED cluster: {@code controllerCount} controllers on fixed ports, each configured with
     * the full eligible-endpoint set and a replica set containing every controller. Exactly one replica is
     * active at a time; the remaining ordered replicas are eligible successors when owner failover is enabled.
     * A non-owner answers NOT_LEADER carrying the active owner endpoint. Exercises the owner-aware client's
     * redirect/routing over the real write+read data path.
     */
    static MiniCluster sharded(int dataNodeCount, int controllerCount) throws Exception {
        if (controllerCount < 2) {
            throw new IllegalArgumentException("sharded cluster requires at least two controllers");
        }
        int[] ports = new int[controllerCount];
        List<String> endpoints = new ArrayList<>(controllerCount);
        for (int i = 0; i < controllerCount; i++) {
            ports[i] = freePort();
            endpoints.add("127.0.0.1:" + ports[i]);
        }
        List<String> eligible = List.copyOf(endpoints);
        return new MiniCluster(dataNodeCount, null, controllerCount, (zk, idx) ->
                new ControllerConfig(zk, ports[idx], 200, 1_000, 1_500, 300, 3_000, 60_000, 5_000, 20_000,
                        "127.0.0.1", 90_000, eligible, controllerCount,
                        2_000, 256, 30_000, 600_000L, 16, 100, 5)
                        .withNamespaceLogBackend());
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** Allows fault tests to alter timing without changing production service wiring. */
    MiniCluster(int nodeCount, String zkConnectOverride, int metaCount,
                BiFunction<String, Integer, ControllerConfig> metaConfigFactory) throws Exception {
        this.root = Files.createTempDirectory("strata-it");
        this.metaConfigFactory = metaConfigFactory;
        this.metadataServiceCount = metaCount;
        this.metaSlots = new Controller[metaCount];
        this.metaSlotEndpoints = new String[metaCount];
        try {
            if (zkConnectOverride == null) {
                this.zk = new TestingServer(true);
                this.zkConnect = zk.getConnectString();
            } else {
                this.zkConnect = zkConnectOverride;
            }
            startControllers();
            for (int i = 0; i < nodeCount; i++) {
                addNode("host-" + i);
            }
            if (nodeCount > 0) awaitRegistered(nodeCount);
        } catch (Exception e) {
            // constructor failure must not leak half a cluster (a flake here previously left
            // nodes/meta/zk running AND made teardown NPE on the unassigned field)
            try {
                close();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    void awaitAnyLeader() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (metas.stream().anyMatch(Controller::isLeader)) return;
            Thread.sleep(20);
        }
        throw new IllegalStateException("no controller became leader");
    }

    List<String> metaEndpoints() {
        return metas.stream().map(Controller::endpoint).toList();
    }

    /** All configured controller endpoints, including a fixed sharded endpoint that is currently stopped. */
    List<String> configuredMetaEndpoints() {
        List<String> endpoints = new ArrayList<>(metadataServiceCount);
        for (int slot = 0; slot < metadataServiceCount; slot++) {
            String endpoint = metaSlotEndpoints[slot];
            if (endpoint == null) {
                throw new IllegalStateException("controller slot " + slot + " has never started");
            }
            endpoints.add(endpoint);
        }
        return List.copyOf(endpoints);
    }

    /** Endpoint identity for a stable controller slot, retained across a stop/restart. */
    String metaEndpoint(int slot) {
        checkMetaSlot(slot);
        String endpoint = metaSlotEndpoints[slot];
        if (endpoint == null) {
            throw new IllegalStateException("controller slot " + slot + " has never started");
        }
        return endpoint;
    }

    /** Currently-live controller in a stable slot, or {@code null} while that slot is stopped. */
    Controller metaAtSlot(int slot) {
        checkMetaSlot(slot);
        return metaSlots[slot];
    }

    /**
     * Legacy active-list kill: callers find an index in {@link #metas}. The closed instance is removed from
     * that active view immediately; its stable slot can later be restarted with {@link #restartMeta(int)}.
     */
    void killMeta(int index) throws IOException {
        Controller victim = metas.get(index);
        int slot = slotOf(victim);
        stopMeta(slot);
    }

    /** Stops one stable controller slot and removes it from every active-cluster view. */
    void stopMeta(int slot) throws IOException {
        checkMetaSlot(slot);
        Controller victim = metaSlots[slot];
        if (victim == null) {
            return;
        }
        metaSlots[slot] = null;
        rebuildActiveMetas();
        victim.close();
    }

    /** Starts a stopped stable slot. Sharded configurations bind the same fixed endpoint again. */
    Controller startMeta(int slot) throws Exception {
        checkMetaSlot(slot);
        if (metaSlots[slot] != null) {
            throw new IllegalStateException("controller slot " + slot + " is already running");
        }
        Controller started = new Controller(metaConfigFactory.apply(zkConnect, slot));
        metaSlots[slot] = started;
        metaSlotEndpoints[slot] = started.endpoint();
        rebuildActiveMetas();
        return started;
    }

    /** Stops and restarts one controller slot, preserving its fixed endpoint in sharded clusters. */
    Controller restartMeta(int slot) throws Exception {
        String before = metaEndpoint(slot);
        stopMeta(slot);
        Controller restarted = startMeta(slot);
        if (!before.equals(restarted.endpoint())) {
            throw new IllegalStateException("controller slot " + slot + " endpoint changed across restart: "
                    + before + " -> " + restarted.endpoint());
        }
        return restarted;
    }

    void stopControllers() {
        for (int slot = 0; slot < metaSlots.length; slot++) {
            Controller controller = metaSlots[slot];
            metaSlots[slot] = null;
            if (controller == null) {
                continue;
            }
            try {
                controller.close();
            } catch (Exception ignored) {
            }
        }
        rebuildActiveMetas();
    }

    void startControllers() throws Exception {
        for (int slot = 0; slot < metadataServiceCount; slot++) {
            if (metaSlots[slot] != null) {
                throw new IllegalStateException("controller slot " + slot + " is already running");
            }
            Controller started = new Controller(metaConfigFactory.apply(zkConnect, slot));
            metaSlots[slot] = started;
            metaSlotEndpoints[slot] = started.endpoint();
        }
        rebuildActiveMetas();
        awaitAnyLeader();
    }

    void restartControllers() throws Exception {
        stopControllers();
        startControllers();
    }

    void restartZooKeeper() throws Exception {
        if (zk == null) {
            throw new IllegalStateException("cannot restart externally supplied ZooKeeper");
        }
        zk.restart();
        zkConnect = zk.getConnectString();
    }

    DataNode addNode(String host) throws IOException {
        Path dir = root.resolve(host);
        DataNode node = new DataNode(
                DataNodeConfig.withMetadata(dir, controllerSeedEndpoints(), host).withNodeId(nextNodeId++));
        nodes.add(node);
        return node;
    }

    DataNode addNode(DataNodeConfig config) throws IOException {
        // Callers that build a config via withMetadata(...) leave nodeId == -1; assign a unique
        // positive id so registration succeeds (a node with a real id never collides with these).
        if (config.nodeId() < 1) {
            config = config.withNodeId(nextNodeId++);
        }
        DataNode node = new DataNode(config);
        nodes.add(node);
        return node;
    }

    Path nodeDir(String host) {
        return root.resolve(host);
    }

    /** Restarts a node on the same data dir (same volume-bound identity). */
    DataNode restartNode(int index) throws IOException {
        DataNode old = nodes.get(index);
        DataNodeConfig cfg = old.config();
        try {
            old.close();
        } catch (IOException ignored) {
        }
        // A restarted node MUST reuse its id: the dataDir's identity.properties already records it,
        // so a mismatched id would make the DataNode constructor refuse to start.
        DataNode fresh = new DataNode(DataNodeConfig.withMetadata(cfg.dataDir(),
                cfg.controllerEndpoints(), cfg.host()).withNodeId(cfg.nodeId()));
        nodes.set(index, fresh);
        return fresh;
    }

    void killNode(int index) {
        try {
            nodes.get(index).close();
        } catch (IOException ignored) {
        }
    }

    void stopDataNodes() {
        for (DataNode node : nodes) {
            try {
                node.close();
            } catch (IOException ignored) {
            }
        }
        nodes.clear();
    }

    void startDataNodes(List<String> hosts) throws IOException {
        for (String host : hosts) {
            addNode(host);
        }
    }

    /** Restarts data nodes on their original data dirs with the supplied node IDs (for cold-restart tests). */
    void startDataNodes(List<String> hosts, List<Integer> nodeIds) throws IOException {
        for (int i = 0; i < hosts.size(); i++) {
            Path dir = root.resolve(hosts.get(i));
            DataNode node = new DataNode(
                    DataNodeConfig.withMetadata(dir, controllerSeedEndpoints(), hosts.get(i))
                            .withNodeId(nodeIds.get(i)));
            nodes.add(node);
        }
    }

    void awaitRegistered(int count) throws Exception {
        try (ZkMetadataStore store = new ZkMetadataStore(zkConnect)) {
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                if (store.listNodes().size() >= count) return;
                Thread.sleep(50);
            }
        }
        throw new AssertionError("nodes did not register in time");
    }

    String metaEndpoint() {
        return meta.endpoint();
    }

    @Override
    public void close() throws Exception {
        try {
            for (DataNode n : nodes) {
                try {
                    n.close();
                } catch (IOException ignored) {
                }
            }
            stopControllers();
            if (zk != null) {
                zk.close();
            }
        } finally {
            // Delete the node data tree on close — otherwise every MiniCluster run leaks its
            // chunk/ledger files under the system temp dir (write/perf tests write GBs each),
            // which accumulates and fills the disk over many runs.
            deleteRecursively(root);
        }
    }

    /**
     * Data nodes should retain every controller seed across a single-controller outage. Before the initial
     * controller startup finishes, fall back to the live view (constructor cleanup is the only such caller).
     */
    private List<String> controllerSeedEndpoints() {
        for (String endpoint : metaSlotEndpoints) {
            if (endpoint == null) {
                return metaEndpoints();
            }
        }
        return configuredMetaEndpoints();
    }

    private int slotOf(Controller controller) {
        for (int slot = 0; slot < metaSlots.length; slot++) {
            if (metaSlots[slot] == controller) {
                return slot;
            }
        }
        throw new IllegalArgumentException("controller is not active in this cluster");
    }

    private void checkMetaSlot(int slot) {
        if (slot < 0 || slot >= metaSlots.length) {
            throw new IndexOutOfBoundsException("controller slot " + slot + " of " + metaSlots.length);
        }
    }

    /** Rebuilds the compatibility list in stable slot order, excluding every stopped/closed controller. */
    private void rebuildActiveMetas() {
        metas.clear();
        for (Controller controller : metaSlots) {
            if (controller != null) {
                metas.add(controller);
            }
        }
        meta = metas.isEmpty() ? null : metas.get(0);
    }

    private static void deleteRecursively(Path dir) {
        if (dir == null) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
    }
}
