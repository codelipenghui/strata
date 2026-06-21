package io.strata.meta;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The repair loop runs two lanes at different cadences: light in-memory housekeeping ({@code tick()})
 * every {@code repairScanIntervalMs}, and the full reconcile backstop ({@code reconcile()}) every
 * {@code reconcileIntervalMs}. This test counts the actual loop dispatches (via overrides, not the
 * wall clock) so it stays deterministic — with reconcileIntervalMs = 3 * repairScanIntervalMs, the
 * loop must fire several ticks per reconcile.
 */
class RepairCoordinatorLoopTest {
    @Test
    void tickRunsEachIntervalReconcileRunsOnReconcileInterval() throws Exception {
        AtomicInteger ticks = new AtomicInteger();
        AtomicInteger reconciles = new AtomicInteger();
        RepairCoordinator coord = newCountingCoordinator(ticks, reconciles, /*scanMs*/ 20, /*reconcileMs*/ 60);
        coord.start();
        try {
            // ~7 fast intervals -> ~7 ticks; reconcile fires roughly every 3rd -> ~2 reconciles
            Thread.sleep(140);
        } finally {
            coord.close();
        }
        int t = ticks.get();
        int r = reconciles.get();
        // Tolerant ranges: a sleeping virtual thread is not a precise clock. The load-bearing
        // invariants are (a) tick fires far more often than reconcile, and (b) reconcile fires at
        // least once but is clearly throttled below the fast cadence.
        assertTrue(t >= 5, "ticks=" + t);
        assertTrue(r >= 1 && r <= 3, "reconciles=" + r);
        assertTrue(t > r, "tick must run more often than reconcile: ticks=" + t + " reconciles=" + r);
    }

    /**
     * A coordinator whose {@code tick()}/{@code reconcile()} only bump counters. It is always leader
     * and owns every namespace, so the loop takes the leader path; the overrides replace the real
     * housekeeping/backstop bodies, isolating this test to the loop's cadence decision.
     */
    private static RepairCoordinator newCountingCoordinator(AtomicInteger ticks, AtomicInteger reconciles,
                                                            int scanMs, int reconcileMs) throws Exception {
        ControllerConfig config = new ControllerConfig("unused", 0, 1, 60_000, 0, scanMs, 1)
                .withReconcileIntervalMs(reconcileMs)
                .withReplicaMissingGraceMs(0);
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config);
        return new RepairCoordinator(store, registry, config, () -> true) {
            @Override
            void tick() {
                ticks.incrementAndGet();
            }

            @Override
            void reconcile() {
                reconciles.incrementAndGet();
            }
        };
    }

    /** Minimal store: the counting coordinator never touches it, so every method is a no-op stub. */
    private static final class FakeStore implements MetadataStore {
        @Override
        public void createFile(Records.FileRecord record) {
        }

        @Override
        public int sweepDeletedFiles(long olderThanMs) {
            return 0;
        }

        @Override
        public java.util.Optional<Versioned<Records.FileRecord>> getFile(io.strata.common.FileId id) {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<io.strata.common.FileId> resolvePath(io.strata.common.StrataNamespace namespace,
                                                                       io.strata.common.StrataPath path) {
            return java.util.Optional.empty();
        }

        @Override
        public boolean updateFile(Records.FileRecord record, int expectedVersion) {
            return false;
        }

        @Override
        public boolean deletePath(io.strata.common.StrataNamespace namespace, io.strata.common.StrataPath path,
                                  io.strata.common.FileId expectedFileId) {
            return true;
        }

        @Override
        public boolean deleteFile(io.strata.common.FileId id, int expectedVersion) {
            return true;
        }

        @Override
        public List<io.strata.common.FileId> listFiles(io.strata.common.StrataNamespace namespace) {
            return List.of();
        }

        @Override
        public List<io.strata.common.StrataNamespace> listNamespaces() {
            return List.of();
        }

        @Override
        public int nextNodeId() {
            return 1;
        }

        @Override
        public boolean putNode(Records.NodeRecord record, int expectedVersion) {
            return false;
        }

        @Override
        public java.util.Optional<Versioned<Records.NodeRecord>> getNode(int nodeId) {
            return java.util.Optional.empty();
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
