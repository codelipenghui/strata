package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for the cross-namespace key collision fixes in RepairCoordinator (final-review
 * C1/I1-I3). File ids are per-namespace — FileId(0) exists in EVERY namespace — so every
 * in-memory structure keyed by a bare FileId/ChunkId must use a namespace-qualified key.
 *
 * <p>These tests MUST FAIL against the unfixed code (bare-key maps) and MUST PASS after all five
 * structures ({@code reported}, {@code allFilesByNamespace}, {@code chunksBeingRepaired},
 * {@code replicaMissingSince}, {@code recentlyCommittedReplicas}) are namespace-qualified.
 */
class RepairCoordinatorNamespaceKeyTest {

    private static final StrataNamespace NS_A = StrataNamespace.of("alpha");
    private static final StrataNamespace NS_B = StrataNamespace.of("beta");

    /**
     * allFilesByNamespace does not collapse — both namespaces' FileId(0) are checked for
     * under-replication. Without the fix (Map keying collapses to last-writer), only one
     * namespace's FileId(0) is scanned; the other's missing replica is never detected.
     */
    @Test
    void scanOnceDetectsUnderReplicationInBothNamespacesWithCollidingFileIds() throws Exception {
        NsFakeStore store = new NsFakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        // Two live nodes that will be repair targets
        long msb1 = 600, msb2 = 610, msb3 = 620;
        Messages.RegisterResp r1 = registry.register(new Messages.RegisterNode(
                (int) msb1, msb1, msb1 + 1, List.of("src-a:9000"), "z", "r", "src-a",
                List.of(new Messages.StorageCapacity(1_000_000)), 1, 0));
        Messages.RegisterResp r2 = registry.register(new Messages.RegisterNode(
                (int) msb2, msb2, msb2 + 1, List.of("src-b:9000"), "z", "r", "src-b",
                List.of(new Messages.StorageCapacity(1_000_000)), 1, 0));
        Messages.RegisterResp r3 = registry.register(new Messages.RegisterNode(
                (int) msb3, msb3, msb3 + 1, List.of("target:9000"), "z", "r", "target",
                List.of(new Messages.StorageCapacity(1_000_000)), 1, 0));

        // Both namespaces have FileId(0) under-replicated (RF=2, only 1 live replica each)
        FileId collidingId = FileId.of(0);
        store.createFile(new Records.FileRecord(collidingId, NS_A, io.strata.common.StrataPath.of("/file-a"),
                3, 2, false, FileState.SEALED, 1234,
                List.of(new Records.ChunkRecord(0, ChunkState.SEALED, 100, 1001, 1,
                        List.of(r1.nodeId())))));
        store.createFile(new Records.FileRecord(collidingId, NS_B, io.strata.common.StrataPath.of("/file-b"),
                3, 2, false, FileState.SEALED, 1234,
                List.of(new Records.ChunkRecord(0, ChunkState.SEALED, 200, 2002, 1,
                        List.of(r2.nodeId())))));

        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);
        coordinator.scanOnce();

        // Both under-replicated chunks should have triggered repair commands — backlog should be 2
        // (one per namespace). Without the fix, the bare-ChunkId-keyed chunksBeingRepaired set let
        // the first namespace's repair suppress the second's, so only one repair was issued.
        assertEquals(2, coordinator.repairBacklog(),
                "both namespaces' FileId(0) should be under repair — without the fix, only one would be detected");
    }

    // ---- helpers ----

    private static ControllerConfig config() {
        return new ControllerConfig("unused", 0, 1, 60_000, 0, 1, 1)
                .withReplicaMissingGraceMs(0);
    }

    /**
     * A namespace-aware FakeStore that keys files by (namespace, fileId), so that the same numeric
     * FileId can exist in multiple namespaces without collision. This reflects the production
     * invariant that file ids are unique only within a namespace.
     */
    private static final class NsFakeStore implements MetadataStore {
        private record NsFileId(StrataNamespace ns, FileId id) {}

        private final Map<NsFileId, MetadataStore.Versioned<Records.FileRecord>> files =
                new LinkedHashMap<>();
        private final Map<Integer, MetadataStore.Versioned<Records.NodeRecord>> nodes =
                new LinkedHashMap<>();

        @Override
        public void createFile(Records.FileRecord record) {
            NsFileId key = new NsFileId(record.namespace(), record.fileId());
            if (files.containsKey(key)) throw new IllegalStateException("file already exists: " + key);
            files.put(key, new MetadataStore.Versioned<>(record, 0));
        }

        @Override
        public int sweepDeletedFiles(long olderThanMs) { return 0; }

        @Override
        public Optional<MetadataStore.Versioned<Records.FileRecord>> getFile(StrataNamespace namespace, FileId id) {
            return Optional.ofNullable(files.get(new NsFileId(namespace, id)));
        }

        @Override
        public Optional<FileId> resolvePath(StrataNamespace namespace, io.strata.common.StrataPath path) {
            return Optional.empty();
        }

        @Override
        public boolean updateFile(Records.FileRecord record, int expectedVersion) {
            NsFileId key = new NsFileId(record.namespace(), record.fileId());
            MetadataStore.Versioned<Records.FileRecord> cur = files.get(key);
            if (cur == null || cur.version() != expectedVersion) return false;
            files.put(key, new MetadataStore.Versioned<>(record, cur.version() + 1));
            return true;
        }

        @Override
        public boolean deletePath(StrataNamespace namespace, io.strata.common.StrataPath path, FileId expectedFileId) {
            return true;
        }

        @Override
        public boolean deleteFile(StrataNamespace namespace, FileId id, int expectedVersion) {
            NsFileId key = new NsFileId(namespace, id);
            MetadataStore.Versioned<Records.FileRecord> cur = files.get(key);
            if (cur == null) return true;
            if (cur.version() != expectedVersion) return false;
            files.remove(key);
            return true;
        }

        @Override
        public List<FileId> listFiles(StrataNamespace namespace) {
            List<FileId> out = new ArrayList<>();
            for (NsFileId key : files.keySet()) {
                if (key.ns().equals(namespace)) out.add(key.id());
            }
            return out;
        }

        @Override
        public List<StrataNamespace> listNamespaces() {
            java.util.TreeSet<StrataNamespace> ns = new java.util.TreeSet<>();
            for (var e : files.values()) ns.add(e.value().namespace());
            return new ArrayList<>(ns);
        }

        @Override
        public boolean putNode(Records.NodeRecord record, int expectedVersion) {
            MetadataStore.Versioned<Records.NodeRecord> cur = nodes.get(record.nodeId());
            if (cur == null) {
                if (expectedVersion != -1) return false;
                nodes.put(record.nodeId(), new MetadataStore.Versioned<>(record, 0));
                return true;
            }
            if (cur.version() != expectedVersion) return false;
            nodes.put(record.nodeId(), new MetadataStore.Versioned<>(record, cur.version() + 1));
            return true;
        }

        @Override
        public Optional<MetadataStore.Versioned<Records.NodeRecord>> getNode(int nodeId) {
            return Optional.ofNullable(nodes.get(nodeId));
        }

        @Override
        public List<MetadataStore.Versioned<Records.NodeRecord>> listNodes() {
            return new ArrayList<>(nodes.values());
        }

        @Override
        public void close() {}
    }
}
