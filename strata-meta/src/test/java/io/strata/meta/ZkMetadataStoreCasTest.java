package io.strata.meta;

import io.strata.common.FileState;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Node-record writes are CAS (split-brain guard): a deposed leader holding a stale version must
 * lose against the new leader's write — unconditional node writes let a dead leader's expire
 * scan overwrite REGISTERED with DEAD.
 */
class ZkMetadataStoreCasTest {

    @Test
    void staleNodeWriteLosesCas() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            // two store handles = two controller instances
            try (ZkMetadataStore leaderA = new ZkMetadataStore(zk.getConnectString());
                 ZkMetadataStore leaderB = new ZkMetadataStore(zk.getConnectString())) {

                Records.NodeRecord node = new Records.NodeRecord(7, 1L, 2L, List.of("h:9000"), "z", "r", "host7", 1L << 30, Records.NodeState.REGISTERED);
                assertTrue(leaderA.putNode(node, -1), "create must succeed");
                assertFalse(leaderA.putNode(node, -1), "double-create must fail");

                int v0 = leaderA.getNode(7).orElseThrow().version();

                // new leader B re-registers the node (bumps the version)
                assertTrue(leaderB.putNode(node.withState(Records.NodeState.REGISTERED), v0));

                // deposed leader A, still holding v0, tries to mark it DEAD — must lose
                assertFalse(leaderA.putNode(node.withState(Records.NodeState.DEAD), v0),
                        "stale-version write must be rejected");

                assertEquals(Records.NodeState.REGISTERED,
                        leaderA.getNode(7).orElseThrow().value().state(),
                        "the new leader's state must survive");
            }
        }
    }

    @Test
    void staleFileDeleteLosesCas() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            try (ZkMetadataStore leaderA = new ZkMetadataStore(zk.getConnectString());
                 ZkMetadataStore leaderB = new ZkMetadataStore(zk.getConnectString())) {
                FileId fileId = FileId.of(1);
                StrataNamespace ns = StrataNamespace.of("test");
                Records.FileRecord file = new Records.FileRecord(fileId, "test", "/test-file", 3, 2, false, FileState.OPEN, System.currentTimeMillis(), List.of());
                leaderA.createFile(file);
                int v0 = leaderA.getFile(ns, fileId).orElseThrow().version();

                assertTrue(leaderB.updateFile(file.withState(FileState.SEALED), v0));

                assertFalse(leaderA.deleteFile(ns, fileId, v0), "stale-version delete must be rejected");
                assertEquals(FileState.SEALED,
                        leaderA.getFile(ns, fileId).orElseThrow().value().state(),
                        "the newer file version must survive");

                int currentVersion = leaderA.getFile(ns, fileId).orElseThrow().version();
                assertTrue(leaderA.deleteFile(ns, fileId, currentVersion));
            }
        }
    }

    @Test
    void externalCuratorConstructorInitializesNamespaceWithoutTakingOwnership() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            CuratorFramework curator = CuratorFrameworkFactory.newClient(zk.getConnectString(),
                    new ExponentialBackoffRetry(100, 3));
            curator.start();
            try {
                try (ZkMetadataStore store = new ZkMetadataStore(curator)) {
                }

                assertTrue(curator.getZookeeperClient().isConnected(), "store.close must not close external curator");
                try (ZkMetadataStore second = new ZkMetadataStore(curator)) {
                    assertEquals(List.of(), second.listNamespaces());
                }
            } finally {
                curator.close();
            }
        }
    }

    @Test
    void pathMarkersMustContainAFullFileIdAndMatchDeleteExpectation() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            try (ZkMetadataStore store = new ZkMetadataStore(zk.getConnectString())) {
                FileId fileId = FileId.of(2);
                Records.FileRecord file = new Records.FileRecord(fileId, "test", "/marker-owner", 3, 2, false, FileState.OPEN,
                        System.currentTimeMillis(), List.of());
                store.createFile(file);

                FileId other = FileId.of(3);
                store.curator().setData().forPath(markerPath("test", "/marker-owner"), fileIdBytes(other));
                assertEquals(other, store.resolvePath(file.namespace(), file.path()).orElseThrow());
                assertFalse(store.deletePath(file.namespace(), file.path(), fileId));

                store.curator().setData().forPath(markerPath("test", "/marker-owner"), new byte[] {1, 2, 3});
                assertThrows(IllegalArgumentException.class, () -> store.resolvePath(file.namespace(), file.path()));
            }
        }
    }

    private static String markerPath(String namespace, String path) {
        return "/strata/namespaces/" + namespace + "/paths" + path + "/__file";
    }

    private static byte[] fileIdBytes(FileId id) {
        return ByteBuffer.allocate(8).putLong(id.id()).array();
    }
}
