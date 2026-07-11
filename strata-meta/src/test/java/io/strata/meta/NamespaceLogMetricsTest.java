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
 * The namespace-log backend exposes per-namespace counters and a live loaded-namespace gauge, so
 * metadata-log write load and sharding distribution are observable via Prometheus.
 */
class NamespaceLogMetricsTest {

    @Test
    void appendAndRecoveryCountersTrackMetadataLogActivity() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            NamespaceLogBackend backend =
                    new NamespaceLogBackend(root, new TestNamespaceMetadataFileStore(), false);
            NamespaceLogMetadataStore store = new NamespaceLogMetadataStore(backend);

            // create files in two distinct namespaces — each namespace is recovered once on first touch
            store.createFile(file(FileId.of(1), "tenant-a", "/topic-0"));
            store.createFile(file(FileId.of(2), "tenant-a", "/topic-1"));
            store.createFile(file(FileId.of(3), "tenant-b", "/topic-0"));

            NamespaceLogMetrics m = store.metrics();
            assertTrue(m.value("tenant-a", NamespaceLogMetrics.APPEND_RECORDS)
                            + m.value("tenant-b", NamespaceLogMetrics.APPEND_RECORDS) >= 3,
                    "each createFile appends at least one log record");
            assertTrue(m.value("tenant-a", NamespaceLogMetrics.APPEND_BYTES)
                            + m.value("tenant-b", NamespaceLogMetrics.APPEND_BYTES) > 0,
                    "appended frames carry bytes");
            assertEquals(2, store.loadedNamespaceCount(), "two namespaces have live owner repositories");
            assertEquals(1, m.value("tenant-a", NamespaceLogMetrics.RECOVERIES));
            assertEquals(1, m.value("tenant-b", NamespaceLogMetrics.RECOVERIES));
            assertEquals(0, m.value("tenant-a", NamespaceLogMetrics.COMPACTIONS));
            assertEquals(0, m.value("tenant-b", NamespaceLogMetrics.COMPACTIONS));

            backend.close();
        }
    }

    @Test
    void recoveryCounterIncrementsAcrossARestart() throws Exception {
        StrataNamespace ns = StrataNamespace.of("tenant-a");
        FileId fileId = FileId.of(7);
        TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();

        try (TestingServer zk = new TestingServer(true)) {
            try (ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
                NamespaceLogBackend backend = new NamespaceLogBackend(root, fileStore, false);
                NamespaceLogMetadataStore store = new NamespaceLogMetadataStore(backend);
                store.createFile(new Records.FileRecord(fileId, ns, StrataPath.of("/topic-0"), 3, 2, true,
                        FileState.OPEN, 1_000, List.of(), 1, 1));
                assertEquals(1, store.metrics().value(ns.value(), NamespaceLogMetrics.RECOVERIES));
                backend.close();
            }

            // a fresh backend recovers the namespace again from the published manifest
            try (ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
                NamespaceLogBackend backend = new NamespaceLogBackend(root, fileStore, false);
                NamespaceLogMetadataStore store = new NamespaceLogMetadataStore(backend);
                assertTrue(store.getFile(ns, fileId).isPresent(), "file recovered after restart");
                assertTrue(store.metrics().value(ns.value(), NamespaceLogMetrics.RECOVERIES) >= 1,
                        "the restart re-recovered the namespace");
                assertEquals(1, store.loadedNamespaceCount());
                backend.close();
            }
        }
    }

    @Test
    void countersAreKeyedByNamespace() {
        NamespaceLogMetrics m = new NamespaceLogMetrics();
        StrataNamespace a = StrataNamespace.of("a");
        StrataNamespace b = StrataNamespace.of("b");
        m.recordAppend(a, 100);
        m.recordAppend(a, 50);
        m.recordLogRead(a, 3, 300);
        m.recordCompaction(b);
        m.recordOwnerAcquired(b);
        m.recordSnapshotFallback(a);

        // stats() index order: [appendRecords, appendBytes, readRecords, readBytes, compactions,
        //                       recoveries, reacquisitions, ownerChanges, snapshotFallbacks]
        long[] sa = m.stats().get("a");
        long[] sb = m.stats().get("b");
        assertEquals(2, sa[0], "a appendRecords");
        assertEquals(150, sa[1], "a appendBytes");
        assertEquals(3, sa[2], "a readRecords");
        assertEquals(300, sa[3], "a readBytes");
        assertEquals(1, sa[NamespaceLogMetrics.SNAPSHOT_FALLBACKS], "a snapshotFallbacks");
        assertEquals(1, sb[4], "b compactions");
        assertEquals(1, sb[7], "b ownerChanges");
    }

    private static Records.FileRecord file(FileId id, String ns, String path) {
        return new Records.FileRecord(id, StrataNamespace.of(ns), StrataPath.of(path), 3, 2, true,
                FileState.OPEN, 1_000, List.of(), 1, 1);
    }
}
