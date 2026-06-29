package io.strata.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/**
 * Tombstone-sweep gating: reaping a namespace's DELETED-file tombstones is the namespace owner's job,
 * not the global leader's. The per-namespace metadata-log tombstones live in the owner's loaded repos,
 * which the leader does not hold for namespaces another controller owns — so gating the sweep on the
 * single global latch leaks tombstones in every non-leader-owned namespace forever.
 *
 * <p>A non-leader owner must therefore sweep its own namespaces' tombstones; only the shared
 * system-root tombstones (swept globally) stay leader-scoped.
 */
class RepairCoordinatorTombstoneSweepTest {

    @Test
    void nonLeaderOwnerReapsItsOwnNamespaceTombstones() throws Exception {
        CountingStore store = new CountingStore();
        ControllerConfig config = config();
        NodeRegistry registry = new NodeRegistry(store, config);
        // Not the global leader, but a sharded owner of its namespaces (ownsAll=false).
        RepairCoordinator coord = new RepairCoordinator(store, registry, config,
                /*isLeader*/ () -> false, /*ownsAll*/ () -> false, /*ownsNamespace*/ ns -> true);

        coord.sweepTombstones();

        assertEquals(1, store.ownedSweeps.get(),
                "a non-leader owner sweeps its own namespaces' tombstones");
        assertEquals(0, store.fullSweeps.get(),
                "a non-leader must NOT run the global system-root sweep");
    }

    @Test
    void leaderRunsTheGlobalSystemRootSweep() throws Exception {
        CountingStore store = new CountingStore();
        ControllerConfig config = config();
        NodeRegistry registry = new NodeRegistry(store, config);
        RepairCoordinator coord = new RepairCoordinator(store, registry, config,
                /*isLeader*/ () -> true, /*ownsAll*/ () -> true, /*ownsNamespace*/ ns -> true);

        coord.sweepTombstones();

        assertEquals(1, store.fullSweeps.get(),
                "the leader sweeps the shared system-root tombstones (and its own repos)");
        assertEquals(0, store.ownedSweeps.get(),
                "the leader's full sweep already covers its owned repos — no extra owner sweep");
    }

    private static ControllerConfig config() {
        return new ControllerConfig("unused", 0, 1, 60_000, 0, 10, 1)
                .withReconcileIntervalMs(200)
                .withReplicaMissingGraceMs(0);
    }

    /** Minimal store that counts the two distinct tombstone-sweep entry points. */
    private static final class CountingStore implements MetadataStore {
        final AtomicInteger fullSweeps = new AtomicInteger();
        final AtomicInteger ownedSweeps = new AtomicInteger();

        @Override
        public int sweepDeletedFiles(long olderThanMs) {
            fullSweeps.incrementAndGet();
            return 0;
        }

        @Override
        public int sweepOwnedNamespaceTombstones(long olderThanMs) {
            ownedSweeps.incrementAndGet();
            return 0;
        }

        @Override
        public void createFile(Records.FileRecord record) {
        }

        @Override
        public Optional<Versioned<Records.FileRecord>> getFile(StrataNamespace namespace, FileId id) {
            return Optional.empty();
        }

        @Override
        public Optional<FileId> resolvePath(StrataNamespace namespace, StrataPath path) {
            return Optional.empty();
        }

        @Override
        public boolean updateFile(Records.FileRecord record, int expectedVersion) {
            return false;
        }

        @Override
        public boolean deletePath(StrataNamespace namespace, StrataPath path, FileId expectedFileId) {
            return true;
        }

        @Override
        public boolean deleteFile(StrataNamespace namespace, FileId id, int expectedVersion) {
            return true;
        }

        @Override
        public List<FileId> listFiles(StrataNamespace namespace) {
            return List.of();
        }

        @Override
        public List<StrataNamespace> listNamespaces() {
            return List.of();
        }

        @Override
        public boolean putNode(Records.NodeRecord record, int expectedVersion) {
            return false;
        }

        @Override
        public Optional<Versioned<Records.NodeRecord>> getNode(int nodeId) {
            return Optional.empty();
        }

        @Override
        public List<Versioned<Records.NodeRecord>> listNodes() {
            return List.of();
        }

        @Override
        public void close() {
        }
    }
}
