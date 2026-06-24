package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-subtree ZK request/byte counters: the store attributes every ZooKeeper op to the top-level
 * {@code /strata/<subtree>} it touches, split read vs write, so the metrics layer can chart request
 * rate and throughput per subtree (files / namespaces / nodes).
 */
class ZkMetadataStoreMetricsTest {

    @Test
    void countsZkOpsAndBytesPerSubtree() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore store = new ZkMetadataStore(zk.getConnectString())) {

            // nodes subtree: putNode writes, getNode reads
            Records.NodeRecord node = new Records.NodeRecord(
                    7, 1L, 2L, List.of("h:9000"), "z", "r", "host7", 1L << 30, Records.NodeState.REGISTERED);
            assertTrue(store.putNode(node, -1));
            store.getNode(7);
            assertTrue(store.zkOps("nodes", true) >= 1, "putNode must count a nodes write");
            assertTrue(store.zkBytes("nodes", true) > 0, "putNode must count nodes write bytes");
            assertTrue(store.zkOps("nodes", false) >= 1, "getNode must count a nodes read");

            // files + namespaces subtrees: createFile writes the file record and the namespace marker
            FileId fileId = FileId.random();
            Records.FileRecord file = new Records.FileRecord(
                    fileId, "test", "/test-file", 3, 2, false, FileState.OPEN, System.currentTimeMillis(), List.of());
            store.createFile(file);
            assertTrue(store.zkOps("files", true) >= 1, "createFile must count a files write");
            assertTrue(store.zkBytes("files", true) > 0, "createFile must count files write bytes");
            assertTrue(store.zkOps("namespaces", true) >= 1, "createFile must count a namespaces write (marker)");

            // files read: getFile reads the record
            store.getFile(fileId);
            assertTrue(store.zkOps("files", false) >= 1, "getFile must count a files read");
            assertTrue(store.zkBytes("files", false) > 0, "getFile must count files read bytes");

            // namespaces read: resolvePath reads the path marker
            store.resolvePath(StrataNamespace.of("test"), StrataPath.of("/test-file"));
            assertTrue(store.zkOps("namespaces", false) >= 1, "resolvePath must count a namespaces read");

            // unknown subtree returns zero, never throws
            assertEquals(0, store.zkOps("bogus", true));
            assertEquals(0, store.zkBytes("bogus", false));
        }
    }
}
