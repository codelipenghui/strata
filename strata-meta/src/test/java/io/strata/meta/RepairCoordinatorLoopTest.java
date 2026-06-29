package io.strata.meta;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
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
    void tickAndReconcileRunEachIntervalTombstoneSweepIsThrottled() throws Exception {
        AtomicInteger ticks = new AtomicInteger();
        AtomicInteger reconciles = new AtomicInteger();
        AtomicInteger sweeps = new AtomicInteger();
        // reconcileMs = 20 * scanMs: a wide ratio so the throttle invariant (sweeps < ticks) holds even if
        // load slows the loop thread by up to ~20x — only at that pathological skew could a single tick span
        // a whole reconcile interval and let the sweep fire every tick.
        RepairCoordinator coord = newCountingCoordinator(ticks, reconciles, sweeps,
                /*scanMs*/ 10, /*reconcileMs*/ 200);
        coord.start();
        try {
            // POLL for the counts rather than sleeping a fixed time and assuming a fixed number of ticks —
            // a sleeping virtual thread is not a precise clock, and under CI load a fixed sleep yields too
            // few ticks. Wait until the loop has ticked enough AND the throttled sweep has fired at least
            // once, so the invariants below are evaluated on a meaningful sample regardless of scheduling.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while ((ticks.get() < 20 || sweeps.get() < 1) && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
        } finally {
            coord.close();
        }
        int t = ticks.get();
        int r = reconciles.get();
        int s = sweeps.get();
        // The load-bearing invariants: (a) the full repair pass (reconcile) runs every fast interval
        // alongside tick — so an interrupted delete/repair re-drives within a tick, not a whole reconcile
        // interval — and (b) only the tombstone sweep is throttled below that cadence.
        assertTrue(t >= 20, "ticks=" + t);
        assertTrue(r >= t - 1, "reconcile runs every tick: reconciles=" + r + " ticks=" + t);
        assertTrue(s >= 1, "the tombstone sweep ran at least once: sweeps=" + s);
        assertTrue(s < t,
                "tombstone sweep must be throttled below the fast cadence: sweeps=" + s + " ticks=" + t);
    }

    /**
     * A coordinator whose {@code tick()}/{@code reconcile()} only bump counters. It is always leader
     * and owns every namespace, so the loop takes the leader path; the overrides replace the real
     * housekeeping/repair bodies, isolating this test to the loop's cadence decision. The tombstone
     * sweep is not overridable (it is a {@code store} call straight from the loop), so the fake store
     * counts it.
     */
    private static RepairCoordinator newCountingCoordinator(AtomicInteger ticks, AtomicInteger reconciles,
                                                            AtomicInteger sweeps, int scanMs, int reconcileMs)
            throws Exception {
        ControllerConfig config = new ControllerConfig("unused", 0, 1, 60_000, 0, scanMs, 1)
                .withReconcileIntervalMs(reconcileMs)
                .withReplicaMissingGraceMs(0);
        FakeStore store = new FakeStore(sweeps);
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

    /** Minimal store: the counting coordinator never touches it but the loop sweeps tombstones. */
    private static final class FakeStore implements MetadataStore {
        private final AtomicInteger sweeps;

        FakeStore(AtomicInteger sweeps) {
            this.sweeps = sweeps;
        }

        @Override
        public void createFile(Records.FileRecord record) {
        }

        @Override
        public int sweepDeletedFiles(long olderThanMs) {
            sweeps.incrementAndGet();
            return 0;
        }

        @Override
        public java.util.Optional<Versioned<Records.FileRecord>> getFile(io.strata.common.StrataNamespace namespace, io.strata.common.FileId id) {
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
        public boolean deleteFile(io.strata.common.StrataNamespace namespace, io.strata.common.FileId id, int expectedVersion) {
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
