package io.strata.it;

import io.strata.meta.ControllerConfig;
import io.strata.meta.Controller;
import io.strata.meta.ZkMetadataStore;
import io.strata.node.DataNodeConfig;
import io.strata.node.DataNode;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * In-process cluster for integration tests: embedded ZooKeeper + controller + N data nodes
 * nodes. This is the primary correctness layer (tech design §16) — real sockets, real disk,
 * deterministic fault injection by killing components.
 */
final class MiniCluster implements AutoCloseable {
    TestingServer zk;             // null when an external (containerized) ZK is supplied
    final List<Controller> metas = new ArrayList<>();
    Controller meta;         // the first instance (initial leader) — legacy accessor
    final List<DataNode> nodes = new ArrayList<>();
    final Path root;
    private final Function<String, ControllerConfig> metaConfigFactory;
    private final int metadataServiceCount;
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
        this(nodeCount, zkConnectOverride, metaCount, ControllerConfig::forTests);
    }

    /** Allows fault tests to alter timing without changing production service wiring. */
    MiniCluster(int nodeCount, String zkConnectOverride, int metaCount,
                Function<String, ControllerConfig> metaConfigFactory) throws Exception {
        this.root = Files.createTempDirectory("strata-it");
        this.metaConfigFactory = metaConfigFactory;
        this.metadataServiceCount = metaCount;
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

    void killMeta(int index) throws IOException {
        metas.get(index).close();
    }

    void stopControllers() {
        for (Controller m : metas) {
            try {
                m.close();
            } catch (Exception ignored) {
            }
        }
        metas.clear();
        meta = null;
    }

    void startControllers() throws Exception {
        for (int i = 0; i < metadataServiceCount; i++) {
            metas.add(new Controller(metaConfigFactory.apply(zkConnect)));
        }
        this.meta = metas.get(0);
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
                DataNodeConfig.withMetadata(dir, metaEndpoints(), host).withNodeId(nextNodeId++));
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
            for (Controller m : metas) {
                try {
                    m.close();
                } catch (Exception ignored) {
                }
            }
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

    private static void deleteRecursively(Path dir) {
        if (dir == null) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
    }
}
