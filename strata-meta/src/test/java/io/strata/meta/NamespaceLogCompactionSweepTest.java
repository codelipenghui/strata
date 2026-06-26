package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Steady-state open-log compaction (design §8/§10). A per-namespace repository compacts at open/failover;
 * without a background sweep a stable long-lived owner's open log would grow unbounded between failovers.
 * These tests cover the sweep that snapshot+rolls owned namespaces past a size threshold.
 */
class NamespaceLogCompactionSweepTest {

    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");

    private static Records.FileRecord file(int id, String path) {
        return new Records.FileRecord(FileId.of(id), NS, StrataPath.of(path), 3, 2, true,
                FileState.OPEN, 1_000, List.of(), 1, 1);
    }

    @Test
    void sweepCompactsOnlyNamespacesPastTheThresholdAndPreservesState() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            NamespaceLogBackend backend = new NamespaceLogBackend(root, new TestNamespaceMetadataFileStore(), false);
            NamespaceLogMetadataStore store = new NamespaceLogMetadataStore(backend);

            for (int i = 0; i < 5; i++) {
                store.createFile(file(i + 1, "/topic-" + i));
            }
            long openBytes = backend.namespaceStats().get(NS)[1];
            assertTrue(openBytes > 0, "createFile records grow the open log past the snapshot cut");
            long compactionsBefore = store.metrics().compactions();

            // a threshold above the accumulated size leaves the namespace alone; one below it compacts.
            assertEquals(0, backend.compactOversizedRepos(openBytes + 1), "under threshold: no compaction");
            assertEquals(1, backend.compactOversizedRepos(1), "over threshold: the namespace is compacted");

            assertEquals(compactionsBefore + 1, store.metrics().compactions(), "exactly one extra compaction");
            assertEquals(0, backend.namespaceStats().get(NS)[1],
                    "compaction rolls a fresh empty open log (open-log bytes reset to the snapshot cut)");

            for (int i = 0; i < 5; i++) {
                assertTrue(store.getFile(NS, FileId.of(i + 1)).isPresent(),
                        "file " + (i + 1) + " survives the snapshot+roll");
            }
            backend.close();
        }
    }

    @Test
    void backgroundSweepBoundsTheOpenLogOfAStableOwner() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            NamespaceLogBackend backend = new NamespaceLogBackend(root, new TestNamespaceMetadataFileStore(), false);
            NamespaceLogMetadataStore store = new NamespaceLogMetadataStore(backend);
            // tiny threshold + fast interval: the very first appended record trips the sweep.
            backend.startBackgroundCompaction(1, 25);

            for (int i = 0; i < 5; i++) {
                store.createFile(file(i + 1, "/topic-" + i));
            }

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (backend.namespaceStats().get(NS)[1] != 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(0, backend.namespaceStats().get(NS)[1],
                    "the background sweep must compact the oversized open log without any failover");
            assertTrue(store.metrics().compactions() >= 1, "the sweep performed at least one compaction");
            backend.close();
        }
    }
}
