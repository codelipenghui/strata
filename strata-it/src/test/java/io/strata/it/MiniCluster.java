package io.strata.it;

import io.strata.meta.MetaConfig;
import io.strata.meta.MetadataService;
import io.strata.meta.ZkMetadataStore;
import io.strata.node.NodeConfig;
import io.strata.node.StorageNode;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * In-process cluster for integration tests: embedded ZooKeeper + metadata service + N storage
 * nodes. This is the primary correctness layer (tech design §16) — real sockets, real disk,
 * deterministic fault injection by killing components.
 */
final class MiniCluster implements AutoCloseable {
    TestingServer zk;             // null when an external (containerized) ZK is supplied
    final List<MetadataService> metas = new ArrayList<>();
    MetadataService meta;         // the first instance (initial leader) — legacy accessor
    final List<StorageNode> nodes = new ArrayList<>();
    final Path root;
    private String zkConnect;

    MiniCluster(int nodeCount) throws Exception {
        this(nodeCount, null, 1);
    }

    MiniCluster(int nodeCount, String zkConnectOverride) throws Exception {
        this(nodeCount, zkConnectOverride, 1);
    }

    /** zkConnectOverride lets chaos tests supply a containerized ZooKeeper. */
    MiniCluster(int nodeCount, String zkConnectOverride, int metaCount) throws Exception {
        this.root = Files.createTempDirectory("strata-it");
        try {
            if (zkConnectOverride == null) {
                this.zk = new TestingServer(true);
                this.zkConnect = zk.getConnectString();
            } else {
                this.zkConnect = zkConnectOverride;
            }
            for (int i = 0; i < metaCount; i++) {
                metas.add(new MetadataService(MetaConfig.forTests(zkConnect)));
            }
            this.meta = metas.get(0);
            awaitAnyLeader();
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
            if (metas.stream().anyMatch(MetadataService::isLeader)) return;
            Thread.sleep(20);
        }
        throw new IllegalStateException("no metadata service became leader");
    }

    List<String> metaEndpoints() {
        return metas.stream().map(MetadataService::endpoint).toList();
    }

    void killMeta(int index) throws IOException {
        metas.get(index).close();
    }

    StorageNode addNode(String host) throws IOException {
        Path dir = root.resolve(host);
        StorageNode node = new StorageNode(NodeConfig.withMetadata(dir, metaEndpoints(), host));
        nodes.add(node);
        return node;
    }

    StorageNode addNode(NodeConfig config) throws IOException {
        StorageNode node = new StorageNode(config);
        nodes.add(node);
        return node;
    }

    Path nodeDir(String host) {
        return root.resolve(host);
    }

    /** Restarts a node on the same data dir (same volume-bound identity). */
    StorageNode restartNode(int index) throws IOException {
        StorageNode old = nodes.get(index);
        NodeConfig cfg = old.config();
        try {
            old.close();
        } catch (IOException ignored) {
        }
        StorageNode fresh = new StorageNode(NodeConfig.withMetadata(cfg.dataDir(),
                cfg.metadataEndpoints(), cfg.host()));
        nodes.set(index, fresh);
        return fresh;
    }

    void killNode(int index) {
        try {
            nodes.get(index).close();
        } catch (IOException ignored) {
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
        for (StorageNode n : nodes) {
            try {
                n.close();
            } catch (IOException ignored) {
            }
        }
        for (MetadataService m : metas) {
            try {
                m.close();
            } catch (Exception ignored) {
            }
        }
        if (zk != null) {
            zk.close();
        }
    }
}
