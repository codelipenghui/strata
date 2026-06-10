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
    final TestingServer zk;
    final MetadataService meta;
    final List<StorageNode> nodes = new ArrayList<>();
    final Path root;

    MiniCluster(int nodeCount) throws Exception {
        this.root = Files.createTempDirectory("strata-it");
        this.zk = new TestingServer(true);
        this.meta = new MetadataService(MetaConfig.forTests(zk.getConnectString()));
        long deadline = System.currentTimeMillis() + 10_000;
        while (!meta.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        if (!meta.isLeader()) throw new IllegalStateException("metadata service did not become leader");
        for (int i = 0; i < nodeCount; i++) {
            addNode("host-" + i);
        }
        awaitRegistered(nodeCount);
    }

    StorageNode addNode(String host) throws IOException {
        Path dir = root.resolve(host);
        StorageNode node = new StorageNode(NodeConfig.withMetadata(dir, List.of(meta.endpoint()), host));
        nodes.add(node);
        return node;
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
        try (ZkMetadataStore store = new ZkMetadataStore(zk.getConnectString())) {
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
        meta.close();
        zk.close();
    }
}
