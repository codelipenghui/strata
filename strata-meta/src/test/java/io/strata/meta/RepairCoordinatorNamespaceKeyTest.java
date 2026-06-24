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
     * C1 — corrupt-misroute data-loss path:
     *
     * <p>Node N holds a HEALTHY (nsA, ChunkId(0,0)) with len=100/crc=1001 AND a HEALTHY
     * (nsB, ChunkId(0,0)) with len=200/crc=2002. A single inventory report carries both entries.
     * The reconcile/sweep must NOT drop either healthy replica and must NOT issue any DeleteCmd
     * for either chunk.
     *
     * <p>Without the fix ({@code reported} keyed by bare ChunkId), nsA's entry is overwritten by
     * nsB's, so the sweep sees nsA descriptor (len=100) vs nsB inventory entry (len=200) — a false
     * length mismatch — and calls {@code applyDeleteConfirmed} + enqueues a physical DeleteCmd,
     * wiping a healthy replica (data loss). With the fix the lookup is per (namespace, chunkId) so
     * each entry is found correctly and no action is taken.
     */
    @Test
    void inventoryDoesNotDropHealthyReplicaWhenTwoNamespacesShareSameChunkId() throws Exception {
        // Both files live under the same FileId value (simulating per-namespace id reuse).
        // We use distinct FileId values here to avoid the FakeStore's bare-FileId key collision,
        // while still triggering the RepairCoordinator's namespace-key bug via the ChunkId path.
        // The ChunkId collision IS the bug: both chunks produce ChunkId(fileId,0) with the SAME
        // numeric index but different len/crc, and the reported map uses the chunkId as the key.
        FileId fileIdA = FileId.of(0);
        FileId fileIdB = FileId.of(1); // different FileId so the FakeStore can hold both
        // BUT we wire nsA to fileIdA and nsB to fileIdB so that both map to ChunkId(*.lsb=0, index=0).
        // The collision happens at the ChunkId.fileId level — ChunkId(FileId(0), 0) vs
        // ChunkId(FileId(1), 0) are DIFFERENT chunkIds, so this isn't the numeric collision path.
        //
        // To trigger the actual numeric collision (same FileId *value* in different namespaces)
        // we need a FakeStore that supports namespace-qualified keys. Use NsFakeStore below.
        NsFakeStore store = new NsFakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        // A single node that holds both chunks
        long incMsb = 500;
        long incLsb = 501;
        Messages.RegisterResp resp = registry.register(new Messages.RegisterNode(
                (int) incMsb, incMsb, incLsb,
                List.of("node-host:9000"), "zone", "rack", "node-host",
                List.of(new Messages.StorageCapacity(1_000_000)), 1, 0));
        int nodeId = resp.nodeId();
        long sessionEpoch = resp.sessionEpoch();

        // SAME FileId value (0) in BOTH namespaces — this is the colliding case
        FileId collidingId = FileId.of(0);
        ChunkId chunkA = new ChunkId(collidingId, 0); // nsA/FileId(0)/index=0 — len=100, crc=1001
        ChunkId chunkB = new ChunkId(collidingId, 0); // nsB/FileId(0)/index=0 — len=200, crc=2002

        store.createFile(new Records.FileRecord(collidingId, NS_A, io.strata.common.StrataPath.of("/file-a"),
                3, 2, false, FileState.SEALED, 1234,
                List.of(new Records.ChunkRecord(0, ChunkState.SEALED, 100, 1001, 1, List.of(nodeId)))));
        store.createFile(new Records.FileRecord(collidingId, NS_B, io.strata.common.StrataPath.of("/file-b"),
                3, 2, false, FileState.SEALED, 1234,
                List.of(new Records.ChunkRecord(0, ChunkState.SEALED, 200, 2002, 1, List.of(nodeId)))));

        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        // Inventory report: both chunks are healthy — correct len and crc for each
        Messages.InventoryReport report = new Messages.InventoryReport(
                nodeId, incMsb, incLsb, sessionEpoch, 0, 1,
                List.of(
                        new Messages.InventoryEntry(chunkA, ChunkState.SEALED, 100, 1001, NS_A),
                        new Messages.InventoryEntry(chunkB, ChunkState.SEALED, 200, 2002, NS_B)));

        coordinator.onInventory(report);

        // Neither replica should be dropped — both files should still have this node as a replica
        List<Integer> replicasA = store.getFile(NS_A, collidingId)
                .orElseThrow().value().chunks().get(0).replicas();
        List<Integer> replicasB = store.getFile(NS_B, collidingId)
                .orElseThrow().value().chunks().get(0).replicas();
        assertTrue(replicasA.contains(nodeId),
                "nsA replica must NOT be dropped — healthy len=100/crc=1001 should match");
        assertTrue(replicasB.contains(nodeId),
                "nsB replica must NOT be dropped — healthy len=200/crc=2002 should match");

        // No DeleteCmd should be enqueued
        Messages.HeartbeatResp hb = registry.heartbeat(
                new Messages.NodeHeartbeat(nodeId, incMsb, incLsb, sessionEpoch,
                        List.of(new Messages.StorageUsage(0, 1_000_000)), 0, List.of()),
                (n, msb, lsb, se, completion) -> {
                    coordinator.onCommandCompleted(n, completion);
                    return CompletableFuture.completedFuture(null);
                });
        assertTrue(hb.commands().isEmpty(),
                "no DeleteCmd must be enqueued for healthy replicas — got: " + hb.commands());
    }

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
        // (one per namespace). Without the fix, allFilesByNamespace collapses FileId(0) to a single
        // entry and only one repair is issued.
        assertEquals(2, coordinator.repairBacklog(),
                "both namespaces' FileId(0) should be under repair — fix collapses only one");
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
