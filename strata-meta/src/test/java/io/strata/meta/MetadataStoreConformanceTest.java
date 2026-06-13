package io.strata.meta;

import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Backend-neutral MetadataStore contract. Every future metadata backend, including KRaft, should
 * pass these tests before being considered semantically equivalent to the v0 ZooKeeper store.
 */
abstract class MetadataStoreConformanceTest {

    interface Backend extends AutoCloseable {
        MetadataStore openStore() throws Exception;
    }

    protected abstract Backend startBackend() throws Exception;

    @Test
    void fileLifecyclePreservesRecordsPathsListsAndCas() throws Exception {
        try (Backend backend = startBackend();
             MetadataStore leaderA = backend.openStore();
             MetadataStore leaderB = backend.openStore()) {
            FileId fileId = new FileId(1, 2);
            Records.FileRecord file = file(fileId, "tenant-a", "/logs/topic-0/segment-0", FileState.OPEN);

            leaderA.createFile(file);
            MetadataStore.Versioned<Records.FileRecord> created = leaderA.getFile(fileId).orElseThrow();
            assertEquals(file, created.value());
            assertEquals(fileId, leaderA.resolvePath(file.namespace(), file.path()).orElseThrow());
            assertEquals(Set.of(fileId), fileIds(leaderA));

            Records.FileRecord updated = file.withWriterEpoch(3).withChunks(List.of(
                    new Records.ChunkRecord(0, ChunkState.SEALED, 4096, 0xCAFE, 3,
                            List.of(1, 2, 3), 11, 12)));
            assertTrue(leaderB.updateFile(updated, created.version()));
            assertFalse(leaderA.updateFile(file.withState(FileState.SEALED), created.version()),
                    "stale file CAS update must lose");

            MetadataStore.Versioned<Records.FileRecord> afterUpdate = leaderA.getFile(fileId).orElseThrow();
            assertEquals(updated, afterUpdate.value());

            assertFalse(leaderA.deleteFile(fileId, created.version()),
                    "stale file CAS delete must lose");
            assertTrue(leaderA.getFile(fileId).isPresent());

            assertFalse(leaderA.deletePath(file.namespace(), file.path(), new FileId(9, 9)),
                    "path delete must verify marker owner");
            assertEquals(fileId, leaderA.resolvePath(file.namespace(), file.path()).orElseThrow());

            assertTrue(leaderA.deleteFile(fileId, afterUpdate.version()));
            assertTrue(leaderA.getFile(fileId).isEmpty());
            assertTrue(leaderA.resolvePath(file.namespace(), file.path()).isEmpty());
            assertEquals(Set.of(), fileIds(leaderA));
        }
    }

    @Test
    void namespacePathBindingsAreScopedAndReplacementSafe() throws Exception {
        try (Backend backend = startBackend();
             MetadataStore store = backend.openStore()) {
            StrataPath sharedPath = StrataPath.of("/logs/shared/segment-0");
            Records.FileRecord first = file(new FileId(10, 1), "tenant-a", sharedPath.toString(), FileState.OPEN);
            Records.FileRecord samePathOtherNamespace =
                    file(new FileId(10, 2), "tenant-b", sharedPath.toString(), FileState.OPEN);
            Records.FileRecord duplicateSameNamespace =
                    file(new FileId(10, 3), "tenant-a", sharedPath.toString(), FileState.OPEN);

            store.createFile(first);
            store.createFile(samePathOtherNamespace);
            assertThrows(Exception.class, () -> store.createFile(duplicateSameNamespace));
            assertTrue(store.getFile(duplicateSameNamespace.fileId()).isEmpty(),
                    "failed path binding transaction must not leave an orphan file record");

            assertEquals(first.fileId(), store.resolvePath(first.namespace(), sharedPath).orElseThrow());
            assertEquals(samePathOtherNamespace.fileId(),
                    store.resolvePath(samePathOtherNamespace.namespace(), sharedPath).orElseThrow());

            int firstVersion = store.getFile(first.fileId()).orElseThrow().version();
            assertTrue(store.deleteFile(first.fileId(), firstVersion));
            assertTrue(store.resolvePath(first.namespace(), sharedPath).isEmpty());
            assertEquals(samePathOtherNamespace.fileId(),
                    store.resolvePath(samePathOtherNamespace.namespace(), sharedPath).orElseThrow());

            Records.FileRecord replacement =
                    file(new FileId(10, 4), "tenant-a", sharedPath.toString(), FileState.OPEN);
            store.createFile(replacement);
            assertEquals(replacement.fileId(), store.resolvePath(replacement.namespace(), sharedPath).orElseThrow());
            assertEquals(Set.of(samePathOtherNamespace.fileId(), replacement.fileId()), fileIds(store));
        }
    }

    @Test
    void finalDeleteOfOldFileDoesNotRemoveReplacementPathBinding() throws Exception {
        try (Backend backend = startBackend();
             MetadataStore store = backend.openStore()) {
            StrataNamespace namespace = StrataNamespace.of("tenant-a");
            StrataPath path = StrataPath.of("/logs/reused/segment-0");
            Records.FileRecord oldDeletingFile =
                    file(new FileId(11, 1), namespace.toString(), path.toString(), FileState.DELETING);
            Records.FileRecord replacement =
                    file(new FileId(11, 2), namespace.toString(), path.toString(), FileState.OPEN);

            store.createFile(oldDeletingFile);
            int oldVersion = store.getFile(oldDeletingFile.fileId()).orElseThrow().version();
            assertTrue(store.deletePath(namespace, path, oldDeletingFile.fileId()));
            assertTrue(store.deletePath(namespace, path, oldDeletingFile.fileId()),
                    "deleting an already-freed path for the same old owner must be idempotent");
            assertTrue(store.resolvePath(namespace, path).isEmpty());

            store.createFile(replacement);
            assertEquals(replacement.fileId(), store.resolvePath(namespace, path).orElseThrow());
            assertFalse(store.deletePath(namespace, path, oldDeletingFile.fileId()),
                    "old-owner deletePath must not clear a replacement path marker");

            assertTrue(store.deleteFile(oldDeletingFile.fileId(), oldVersion));
            assertTrue(store.getFile(oldDeletingFile.fileId()).isEmpty());
            assertEquals(replacement.fileId(), store.resolvePath(namespace, path).orElseThrow(),
                    "final deletion of an old DELETING file must not delete the replacement path marker");
            assertEquals(Set.of(replacement.fileId()), fileIds(store));
        }
    }

    @Test
    void deletedFileIdsCannotBeRecreatedByStaleCreateReplay() throws Exception {
        try (Backend backend = startBackend();
             MetadataStore store = backend.openStore()) {
            StrataNamespace namespace = StrataNamespace.of("tenant-a");
            StrataPath path = StrataPath.of("/logs/deleted-id/segment-0");
            Records.FileRecord deleted = file(new FileId(12, 1), namespace.toString(), path.toString(),
                    FileState.OPEN);

            store.createFile(deleted);
            int version = store.getFile(deleted.fileId()).orElseThrow().version();
            assertTrue(store.deleteFile(deleted.fileId(), version));
            assertTrue(store.getFile(deleted.fileId()).isEmpty());
            assertTrue(store.resolvePath(namespace, path).isEmpty());

            assertThrows(Exception.class, () -> store.createFile(deleted),
                    "a stale create replay must not resurrect a fully deleted FileId");

            Records.FileRecord replacement = file(new FileId(12, 2), namespace.toString(), path.toString(),
                    FileState.OPEN);
            store.createFile(replacement);
            assertEquals(replacement.fileId(), store.resolvePath(namespace, path).orElseThrow());
            assertEquals(Set.of(replacement.fileId()), fileIds(store));
        }
    }

    @Test
    void nodeLifecyclePreservesRecordsListsAllocationAndCas() throws Exception {
        try (Backend backend = startBackend();
             MetadataStore leaderA = backend.openStore();
             MetadataStore leaderB = backend.openStore()) {
            assertEquals(1, leaderA.nextNodeId());
            assertEquals(2, leaderA.nextNodeId());

            Records.NodeRecord node = new Records.NodeRecord(7, 101, 202,
                    List.of("host-a:9000", "host-a:9001"), "z1", "r1", "host-a",
                    1L << 40, Records.NodeState.REGISTERED);
            assertTrue(leaderA.putNode(node, -1));
            assertFalse(leaderA.putNode(node, -1), "double-create must fail");
            MetadataStore.Versioned<Records.NodeRecord> created = leaderA.getNode(node.nodeId()).orElseThrow();
            assertEquals(node, created.value());
            assertEquals(Set.of(node), nodes(leaderA));

            Records.NodeRecord draining = node.withState(Records.NodeState.DRAINING);
            assertTrue(leaderB.putNode(draining, created.version()));
            assertFalse(leaderA.putNode(node.withState(Records.NodeState.DEAD), created.version()),
                    "stale node CAS update must lose");
            assertEquals(draining, leaderA.getNode(node.nodeId()).orElseThrow().value());
            assertEquals(Set.of(draining), nodes(leaderA));

            assertFalse(leaderA.putNode(new Records.NodeRecord(8, 303, 404,
                    List.of("host-b:9000"), "z1", "r1", "host-b", 1_000,
                    Records.NodeState.REGISTERED), 0),
                    "updating a missing node with a non-create version must fail");
            assertTrue(leaderA.getNode(8).isEmpty());
        }
    }

    @Test
    void missingDeletesAreIdempotent() throws Exception {
        try (Backend backend = startBackend();
             MetadataStore store = backend.openStore()) {
            FileId missing = new FileId(99, 100);
            assertTrue(store.getFile(missing).isEmpty());
            assertFalse(store.updateFile(file(missing, "tenant-a", "/missing-file", FileState.OPEN), 0));
            assertTrue(store.deleteFile(missing, 0));
            assertTrue(store.deletePath(StrataNamespace.of("tenant-a"), StrataPath.of("/missing"), missing));
            assertFalse(store.putNode(new Records.NodeRecord(99, 1, 2, List.of("missing:9000"),
                    "z1", "r1", "missing", 1000, Records.NodeState.REGISTERED), 0));
            assertTrue(store.listFiles().isEmpty());
            assertTrue(store.listNodes().isEmpty());
        }
    }

    @Test
    void storeHandleReopenPreservesStateVersionsAndAllocation() throws Exception {
        try (Backend backend = startBackend()) {
            FileId fileId = new FileId(20, 1);
            Records.FileRecord file = file(fileId, "tenant-a", "/logs/reopen/segment-0", FileState.OPEN);
            Records.NodeRecord node = new Records.NodeRecord(21, 501, 502,
                    List.of("host-reopen:9000"), "z1", "r1", "host-reopen",
                    1L << 30, Records.NodeState.REGISTERED);

            int fileVersion;
            int nodeVersion;
            try (MetadataStore writer = backend.openStore()) {
                assertEquals(1, writer.nextNodeId());
                writer.createFile(file);
                fileVersion = writer.getFile(fileId).orElseThrow().version();
                assertTrue(writer.putNode(node, -1));
                nodeVersion = writer.getNode(node.nodeId()).orElseThrow().version();
            }

            try (MetadataStore reopened = backend.openStore()) {
                MetadataStore.Versioned<Records.FileRecord> reopenedFile =
                        reopened.getFile(fileId).orElseThrow();
                assertEquals(file, reopenedFile.value());
                assertEquals(fileVersion, reopenedFile.version());
                assertEquals(fileId, reopened.resolvePath(file.namespace(), file.path()).orElseThrow());
                assertEquals(Set.of(fileId), fileIds(reopened));

                MetadataStore.Versioned<Records.NodeRecord> reopenedNode =
                        reopened.getNode(node.nodeId()).orElseThrow();
                assertEquals(node, reopenedNode.value());
                assertEquals(nodeVersion, reopenedNode.version());
                assertEquals(Set.of(node), nodes(reopened));
                assertEquals(2, reopened.nextNodeId(),
                        "node id allocation must not rewind after a store handle is reopened");

                assertTrue(reopened.updateFile(file.withWriterEpoch(2), fileVersion));
            }

            try (MetadataStore afterUpdate = backend.openStore()) {
                MetadataStore.Versioned<Records.FileRecord> updated =
                        afterUpdate.getFile(fileId).orElseThrow();
                assertEquals(2, updated.value().writerEpoch());
                assertTrue(updated.version() > fileVersion,
                        "CAS version must advance and remain visible after another reopen");
            }
        }
    }

    @Test
    void staleHandleCannotDisturbReplacementAfterDeleteAndReopen() throws Exception {
        try (Backend backend = startBackend();
             MetadataStore staleLeader = backend.openStore()) {
            StrataNamespace namespace = StrataNamespace.of("tenant-a");
            StrataPath path = StrataPath.of("/logs/stale-handle/segment-0");
            Records.FileRecord oldFile = file(new FileId(30, 1), namespace.toString(), path.toString(),
                    FileState.OPEN);
            Records.FileRecord replacement = file(new FileId(30, 2), namespace.toString(), path.toString(),
                    FileState.OPEN);

            staleLeader.createFile(oldFile);
            int oldVersion = staleLeader.getFile(oldFile.fileId()).orElseThrow().version();

            try (MetadataStore activeLeader = backend.openStore()) {
                assertTrue(activeLeader.deleteFile(oldFile.fileId(), oldVersion));
                assertTrue(activeLeader.resolvePath(namespace, path).isEmpty());
            }

            try (MetadataStore reopenedLeader = backend.openStore()) {
                reopenedLeader.createFile(replacement);
                assertEquals(replacement.fileId(), reopenedLeader.resolvePath(namespace, path).orElseThrow());
            }

            assertFalse(staleLeader.updateFile(oldFile.withState(FileState.SEALED), oldVersion),
                    "stale update must not resurrect a deleted file");
            assertFalse(staleLeader.deletePath(namespace, path, oldFile.fileId()),
                    "stale old-owner path delete must not clear the replacement marker");
            assertTrue(staleLeader.deleteFile(oldFile.fileId(), oldVersion),
                    "missing stale file delete remains idempotent");
            assertThrows(Exception.class, () -> staleLeader.createFile(oldFile),
                    "stale create replay must not reuse a permanently reserved FileId");

            try (MetadataStore verifier = backend.openStore()) {
                assertTrue(verifier.getFile(oldFile.fileId()).isEmpty());
                assertEquals(replacement, verifier.getFile(replacement.fileId()).orElseThrow().value());
                assertEquals(replacement.fileId(), verifier.resolvePath(namespace, path).orElseThrow());
                assertEquals(Set.of(replacement.fileId()), fileIds(verifier));
            }
        }
    }

    @Test
    void repeatedReplacementHistoryKeepsDeletedIdsReservedDuringFreePathWindows() throws Exception {
        try (Backend backend = startBackend();
             MetadataStore staleFirstLeader = backend.openStore()) {
            StrataNamespace namespace = StrataNamespace.of("tenant-a");
            StrataPath path = StrataPath.of("/logs/replacement-history/segment-0");
            Records.FileRecord first = file(new FileId(40, 1), namespace.toString(), path.toString(),
                    FileState.OPEN);
            Records.FileRecord second = file(new FileId(40, 2), namespace.toString(), path.toString(),
                    FileState.OPEN);
            Records.FileRecord third = file(new FileId(40, 3), namespace.toString(), path.toString(),
                    FileState.OPEN);

            staleFirstLeader.createFile(first);
            int firstVersion = staleFirstLeader.getFile(first.fileId()).orElseThrow().version();

            int secondVersion;
            try (MetadataStore secondLeader = backend.openStore()) {
                assertTrue(secondLeader.deleteFile(first.fileId(), firstVersion));
                assertTrue(secondLeader.resolvePath(namespace, path).isEmpty());
                assertThrows(Exception.class, () -> secondLeader.createFile(first),
                        "deleted first FileId must stay reserved while the path is free");

                secondLeader.createFile(second);
                secondVersion = secondLeader.getFile(second.fileId()).orElseThrow().version();
                assertEquals(second.fileId(), secondLeader.resolvePath(namespace, path).orElseThrow());
                assertEquals(Set.of(second.fileId()), fileIds(secondLeader));
            }

            assertFalse(staleFirstLeader.updateFile(first.withState(FileState.SEALED), firstVersion),
                    "stale first-generation update must not resurrect a deleted file");
            assertFalse(staleFirstLeader.deletePath(namespace, path, first.fileId()),
                    "stale first-generation path delete must not clear a later generation");
            assertTrue(staleFirstLeader.deleteFile(first.fileId(), firstVersion));
            assertThrows(Exception.class, () -> staleFirstLeader.createFile(first),
                    "stale first-generation create replay must still lose after replacement");

            try (MetadataStore thirdLeader = backend.openStore()) {
                assertTrue(thirdLeader.deleteFile(second.fileId(), secondVersion));
                assertTrue(thirdLeader.resolvePath(namespace, path).isEmpty());
                assertThrows(Exception.class, () -> thirdLeader.createFile(first),
                        "deleted first FileId must stay reserved across later free-path windows");
                assertThrows(Exception.class, () -> thirdLeader.createFile(second),
                        "deleted second FileId must stay reserved while the path is free");

                thirdLeader.createFile(third);
                assertEquals(third.fileId(), thirdLeader.resolvePath(namespace, path).orElseThrow());
                assertEquals(Set.of(third.fileId()), fileIds(thirdLeader));
            }

            try (MetadataStore verifier = backend.openStore()) {
                assertTrue(verifier.getFile(first.fileId()).isEmpty());
                assertTrue(verifier.getFile(second.fileId()).isEmpty());
                assertEquals(third, verifier.getFile(third.fileId()).orElseThrow().value());
                assertEquals(third.fileId(), verifier.resolvePath(namespace, path).orElseThrow());
                assertEquals(Set.of(third.fileId()), fileIds(verifier));
                assertThrows(Exception.class, () -> verifier.createFile(first));
                assertThrows(Exception.class, () -> verifier.createFile(second));
            }
        }
    }

    private static Records.FileRecord file(FileId fileId, String namespace, String path, FileState state) {
        return new Records.FileRecord(fileId, namespace, path, 3, 2, true, state, 1234,
                List.of(new Records.ChunkRecord(0, ChunkState.OPEN, 0, 0, 1, List.of(1, 2),
                        77, 88)), 55, 66);
    }

    private static Set<FileId> fileIds(MetadataStore store) throws Exception {
        return Set.copyOf(store.listFiles());
    }

    private static Set<Records.NodeRecord> nodes(MetadataStore store) throws Exception {
        return store.listNodes().stream()
                .map(MetadataStore.Versioned::value)
                .collect(Collectors.toSet());
    }
}
