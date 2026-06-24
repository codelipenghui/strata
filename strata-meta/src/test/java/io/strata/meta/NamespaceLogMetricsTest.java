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
 * The namespace-log backend exposes process-wide counters (append/compaction/recovery) and a live
 * loaded-namespace gauge, so the metadata log's own write load and sharding distribution are
 * observable via Prometheus (wired into ServerMetrics as strata_controller_log_* / strata_controller_namespaces_loaded).
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
            assertTrue(m.appendRecords() >= 3, "each createFile appends at least one log record");
            assertTrue(m.appendBytes() > 0, "appended frames carry bytes");
            assertEquals(2, store.loadedNamespaceCount(), "two namespaces have live owner repositories");
            assertEquals(2, m.recoveries(), "one recovery per namespace opened (tenant-a, tenant-b)");
            assertEquals(0, m.compactions(), "no explicit compaction was triggered");

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
                assertEquals(1, store.metrics().recoveries());
                backend.close();
            }

            // a fresh backend recovers the namespace again from the published manifest
            try (ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
                NamespaceLogBackend backend = new NamespaceLogBackend(root, fileStore, false);
                NamespaceLogMetadataStore store = new NamespaceLogMetadataStore(backend);
                assertTrue(store.getFile(ns, fileId).isPresent(), "file recovered after restart");
                assertTrue(store.metrics().recoveries() >= 1, "the restart re-recovered the namespace");
                assertEquals(1, store.loadedNamespaceCount());
                backend.close();
            }
        }
    }

    private static Records.FileRecord file(FileId id, String ns, String path) {
        return new Records.FileRecord(id, StrataNamespace.of(ns), StrataPath.of(path), 3, 2, true,
                FileState.OPEN, 1_000, List.of(), 1, 1);
    }
}
