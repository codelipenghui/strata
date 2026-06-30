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
            FileId fileId = FileId.of(1);
            Records.FileRecord file = new Records.FileRecord(
                    fileId, "test", "/test-file", 3, 2, false, FileState.OPEN, System.currentTimeMillis(), List.of());
            store.createFile(file);
            assertTrue(store.zkOps("files", true) >= 1, "createFile must count a files write");
            assertTrue(store.zkBytes("files", true) > 0, "createFile must count files write bytes");
            assertTrue(store.zkOps("namespaces", true) >= 1, "createFile must count a namespaces write (marker)");

            // files read: getFile reads the record
            store.getFile(StrataNamespace.of("test"), fileId);
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

    /**
     * {@link ZkMetadataStore#listFileIds} is a children-only enumeration for the repair/verify sweep: it
     * must NOT read any file record (the caller re-reads each via getFile). This is the fix for the
     * per-tick read amplification where listFiles read every record just to filter tombstones and the
     * repair loop then read each survivor a second time.
     */
    @Test
    void listFileIdsDoesNotReadFileRecords() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore store = new ZkMetadataStore(zk.getConnectString())) {
            StrataNamespace ns = StrataNamespace.of("n");
            for (int i = 1; i <= 4; i++) {
                store.createFile(new Records.FileRecord(FileId.of(i), "n", "/f" + i, 3, 2, false,
                        FileState.OPEN, System.currentTimeMillis(), List.of()));
            }

            long filesReadBefore = store.zkOps("files", false);
            List<FileId> ids = store.listFileIds(ns);

            assertEquals(4, ids.size(), "must enumerate every index entry");
            assertEquals(filesReadBefore, store.zkOps("files", false),
                    "listFileIds must not read any file record (children-only enumeration)");
        }
    }

    /**
     * {@link ZkMetadataStore#listNamespaces} only needs to know whether a namespace has ANY live file, so
     * it must short-circuit on the first live record instead of reading every record (it ran on every node
     * every repair/verify tick, reading all system metadata-log segment records just to test non-emptiness).
     */
    @Test
    void listNamespacesShortCircuitsEmptinessProbe() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore store = new ZkMetadataStore(zk.getConnectString())) {
            for (int i = 1; i <= 5; i++) {
                store.createFile(new Records.FileRecord(FileId.of(i), "n", "/f" + i, 3, 2, false,
                        FileState.OPEN, System.currentTimeMillis(), List.of()));
            }

            long filesReadBefore = store.zkOps("files", false);
            assertEquals(List.of(StrataNamespace.of("n")), store.listNamespaces());

            assertEquals(1, store.zkOps("files", false) - filesReadBefore,
                    "listNamespaces must read at most one record per namespace to confirm it is non-empty");
        }
    }
}
