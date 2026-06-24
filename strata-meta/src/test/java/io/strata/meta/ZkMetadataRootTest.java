package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Consensus-root capabilities used for namespace sharding (design §5, §6.1): the global metadata
 * epoch counter and per-namespace assignment records, both CAS-guarded against stale writers.
 */
class ZkMetadataRootTest {

    @Test
    void metadataEpochIsUniqueAndMonotonicAcrossHandles() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore a = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore b = new ZkMetadataStore(zk.getConnectString())) {
            assertEquals(1L, a.allocateMetadataEpoch());
            assertEquals(2L, b.allocateMetadataEpoch());
            assertEquals(3L, a.allocateMetadataEpoch());
            // a fresh handle continues from the persisted counter (no rewind)
            try (ZkMetadataStore c = new ZkMetadataStore(zk.getConnectString())) {
                assertEquals(4L, c.allocateMetadataEpoch());
            }
        }
    }

    @Test
    void concurrentEpochAllocationsAreAllDistinct() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore store = new ZkMetadataStore(zk.getConnectString())) {
            int n = 50;
            Set<Long> seen = ConcurrentHashMap.newKeySet();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                threads.add(new Thread(() -> {
                    try {
                        seen.add(store.allocateMetadataEpoch());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (Thread t : threads) t.start();
            for (Thread t : threads) t.join();
            assertEquals(n, seen.size(), "every allocated epoch is unique");
            assertEquals(n, seen.stream().mapToLong(Long::longValue).max().orElse(0L),
                    "CAS-increment is gap-free: epochs are exactly 1..n under contention");
        }
    }

    @Test
    void namespaceAssignmentIsCasGuardedAndScopedPerNamespace() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore a = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore b = new ZkMetadataStore(zk.getConnectString())) {
            StrataNamespace ns = StrataNamespace.of("tenant-a");
            Records.NamespaceAssignment v0 = new Records.NamespaceAssignment(ns, 0,
                    List.of("m1:9301", "m2:9301", "m3:9301"));
            assertTrue(a.putNamespaceAssignment(v0, -1), "create must succeed");
            assertFalse(a.putNamespaceAssignment(v0, -1), "double-create must fail");

            MetadataStore.Versioned<Records.NamespaceAssignment> read =
                    a.getNamespaceAssignment(ns).orElseThrow();
            assertEquals(v0, read.value());
            assertEquals("m1:9301", read.value().preferredLeader());

            Records.NamespaceAssignment v1 = new Records.NamespaceAssignment(ns, 1,
                    List.of("m2:9301", "m3:9301", "m1:9301"));
            assertTrue(b.putNamespaceAssignment(v1, read.version()), "in-version update must win");
            assertFalse(a.putNamespaceAssignment(v0, read.version()),
                    "stale-version assignment write must lose");
            assertEquals(v1, a.getNamespaceAssignment(ns).orElseThrow().value());

            assertTrue(a.getNamespaceAssignment(StrataNamespace.of("tenant-none")).isEmpty());
            assertEquals(List.of(ns), a.listAssignedNamespaces());
        }
    }

    @Test
    void namespaceManifestIsCasPublishedAndFencesStaleLeaders() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore a = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore b = new ZkMetadataStore(zk.getConnectString())) {
            StrataNamespace ns = StrataNamespace.of("tenant-a");
            Records.NamespaceManifest v0 = new Records.NamespaceManifest(ns, 1, 0, 0, 0,
                    Optional.empty(), Optional.of(FileId.of(10)));
            assertEquals(0, a.putNamespaceManifest(v0, -1).orElseThrow(), "first publish creates at version 0");
            assertTrue(a.putNamespaceManifest(v0, -1).isEmpty(), "re-create must fail");

            MetadataStore.Versioned<Records.NamespaceManifest> read =
                    a.getNamespaceManifest(ns).orElseThrow();
            assertEquals(v0, read.value());

            Records.NamespaceManifest v1 = new Records.NamespaceManifest(ns, 2, 1, 4096, 8192,
                    Optional.of(FileId.of(11)), Optional.of(FileId.of(33)));
            assertTrue(b.putNamespaceManifest(v1, read.version()).isPresent(), "the epoch-2 leader publishes");
            assertTrue(a.putNamespaceManifest(v0, read.version()).isEmpty(),
                    "a fenced (stale-version) publish must lose the CAS");
            assertEquals(v1, a.getNamespaceManifest(ns).orElseThrow().value());

            assertTrue(a.getNamespaceManifest(StrataNamespace.of("tenant-none")).isEmpty());
        }
    }
}
