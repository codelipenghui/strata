package io.strata.meta;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Re-acquire-on-fence: a namespace owner whose cached meta-log repository has been superseded (another
 * opener republished the manifest at a higher epoch) MUST re-acquire its meta-log and complete the
 * append, instead of staying permanently fenced.
 *
 * <p>Reproduces the live wedge: under sharded ownership, two controllers briefly opened the same
 * namespace during the startup membership settle. The later opener's {@code recoverAndRepublish}
 * fenced the earlier one's open-log writer, so the still-believed owner's every meta-log append threw
 * {@code FENCED_EPOCH: epoch 1 < 2} forever — its {@code ownerRepairPass} skipped the same file every
 * 60s and the file never finalized. The fix evicts the stale cached repo on a fenced append and
 * re-opens at a fresh epoch (recovering the latest durable state), then retries once.
 *
 * <p>{@link TestNamespaceMetadataFileStore} does not model epoch fencing, so this test wraps it with a
 * store that fences appends to any superseded (non-latest) log file — exactly how the Strata data plane
 * fences a deposed meta-log writer.
 */
class NamespaceLogBackendReacquireOnFenceTest {

    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");

    @Test
    void fencedOwnerReAcquiresAndCompletesInsteadOfStayingWedged() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            FenceStaleLogFileStore fileStore = new FenceStaleLogFileStore();

            // Owner A opens NS (epoch eA): creates log L1, writes file id 0.
            NamespaceLogBackend ownerA = new NamespaceLogBackend(root, fileStore, false);
            FileId id0 = ownerA.createFileOwnerAssigned(template("/a", 1));
            assertEquals(FileId.of(0), id0);

            // A transient second opener B re-opens the SAME NS (epoch eB > eA): recoverAndRepublish
            // rolls a NEW log L2 and CAS-publishes the manifest — A's L1 is now a superseded writer.
            NamespaceLogBackend ownerB = new NamespaceLogBackend(root, fileStore, false);
            FileId id1 = ownerB.createFileOwnerAssigned(template("/b", 2));
            assertEquals(FileId.of(1), id1);

            // A still believes it owns NS and appends again, but its cached repo points at the superseded
            // L1 — the append is FENCED_EPOCH. Without the fix this throws and A is permanently wedged.
            FileId id2 = ownerA.createFileOwnerAssigned(template("/c", 3));

            // With the fix: A evicted its stale repo, re-opened at a fresh epoch (recovering B's durable
            // state), retried the append, and completed — assigning the next id off the recovered state.
            assertEquals(FileId.of(2), id2, "re-acquire must recover the latest durable state, then assign id 2");
            assertTrue(ownerA.getFile(NS, id2).isPresent(), "fenced owner must complete the append, not wedge");
            // No data loss across the fence: A recovered both its pre-fence file and B's file.
            assertTrue(ownerA.getFile(NS, id0).isPresent(), "re-acquire must preserve the pre-fence file (id 0)");
            assertTrue(ownerA.getFile(NS, id1).isPresent(), "re-acquire must recover the superseding owner's file (id 1)");

            // The fence-driven reacquire is an in-place epoch bump, NOT an ownership handoff: it must count
            // as a reacquisition, never as an owner change (else the namespace-owner-changes dashboard would
            // double-count churn and show false handoffs). A's only owner change is its original cold open.
            long[] aStats = ownerA.metrics().stats().get(NS.value());
            assertEquals(1, aStats[NamespaceLogMetrics.OWNER_CHANGES],
                    "fence reacquire must NOT increment ownerChanges (only the cold acquisition counts)");
            assertTrue(aStats[NamespaceLogMetrics.REACQUISITIONS] >= 1,
                    "the fence churn is counted as a reacquisition");
        }
    }

    @Test
    void ambiguousAppendFailurePoisonsRepoSoNextMutationReAcquires() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            DurablyFailingFileStore fileStore = new DurablyFailingFileStore();
            NamespaceLogBackend owner = new NamespaceLogBackend(root, fileStore, false);

            assertEquals(FileId.of(0), owner.createFileOwnerAssigned(template("/a", 1)));

            fileStore.failNextAppendAfterWritingFrame();
            assertScpInternal(thrownBy(() -> owner.createFileOwnerAssigned(template("/b", 2))));

            FileId retried = owner.createFileOwnerAssigned(template("/b", 2));
            assertEquals(FileId.of(1), retried,
                    "the poisoned repo must be re-acquired before the next metadata mutation");
            assertTrue(owner.getFile(NS, retried).isPresent(),
                    "re-acquire must recover the durable-but-unacked FileCreated record");

            long[] stats = owner.metrics().stats().get(NS.value());
            assertEquals(1, stats[NamespaceLogMetrics.OWNER_CHANGES],
                    "append-failure re-acquire must not count as an ownership handoff");
            assertTrue(stats[NamespaceLogMetrics.REACQUISITIONS] >= 1,
                    "append-failure recovery is counted as a re-acquisition");
        }
    }

    private static void assertScpInternal(Throwable t) {
        assertTrue(t instanceof ScpException se && se.code() == ErrorCode.INTERNAL,
                "expected INTERNAL ScpException, got " + t);
    }

    private static Throwable thrownBy(ThrowingRunnable runnable) {
        try {
            runnable.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static Records.FileRecord template(String path, long opId) {
        return new Records.FileRecord(FileId.of(0), NS, StrataPath.of(path),
                3, 2, true, FileState.OPEN, 100L, List.of(), opId, opId);
    }

    /**
     * Wraps {@link TestNamespaceMetadataFileStore} with epoch fencing: only the most recently created
     * log file accepts appends; an append to any superseded log file throws {@code FENCED_EPOCH}, as the
     * Strata data plane fences a deposed meta-log writer. Reads are never fenced (recovery must still read
     * the superseded tail).
     */
    private static final class FenceStaleLogFileStore implements NamespaceMetadataFileStore {
        private final TestNamespaceMetadataFileStore delegate = new TestNamespaceMetadataFileStore();
        private volatile FileId latestLog;

        @Override
        public FileId createLogFile(StrataNamespace ns, long generation) throws Exception {
            FileId id = delegate.createLogFile(ns, generation);
            latestLog = id;
            return id;
        }

        @Override
        public void appendLog(FileId logFileId, byte[] frameBytes) throws Exception {
            if (!logFileId.equals(latestLog)) {
                throw new ScpException(ErrorCode.FENCED_EPOCH,
                        "FENCED_EPOCH: stale meta-log writer " + logFileId + " superseded by " + latestLog);
            }
            delegate.appendLog(logFileId, frameBytes);
        }

        @Override
        public byte[] readLog(FileId logFileId) throws Exception {
            return delegate.readLog(logFileId);
        }

        @Override
        public FileId writeSnapshot(StrataNamespace ns, long generation, byte[] snapshotBytes) throws Exception {
            return delegate.writeSnapshot(ns, generation, snapshotBytes);
        }

        @Override
        public byte[] readSnapshot(FileId snapshotFileId) throws Exception {
            return delegate.readSnapshot(snapshotFileId);
        }

        @Override
        public void deleteFile(FileId fileId) throws Exception {
            delegate.deleteFile(fileId);
        }
    }

    /**
     * Simulates the production dead-appender shape for a non-fenced append failure: one append frame may
     * already be durable, but the appender returns INTERNAL and every later append to the same log repeats
     * that death cause until recovery reads/seals the log.
     */
    private static final class DurablyFailingFileStore implements NamespaceMetadataFileStore {
        private final TestNamespaceMetadataFileStore delegate = new TestNamespaceMetadataFileStore();
        private boolean failNextAppendAfterWritingFrame;
        private FileId deadLog;
        private ScpException deathCause;

        void failNextAppendAfterWritingFrame() {
            failNextAppendAfterWritingFrame = true;
        }

        @Override
        public FileId createLogFile(StrataNamespace ns, long generation) throws Exception {
            return delegate.createLogFile(ns, generation);
        }

        @Override
        public synchronized void appendLog(FileId logFileId, byte[] frameBytes) throws Exception {
            if (logFileId.equals(deadLog)) {
                throw deathCause;
            }
            if (failNextAppendAfterWritingFrame) {
                failNextAppendAfterWritingFrame = false;
                delegate.appendLog(logFileId, frameBytes);
                deadLog = logFileId;
                deathCause = new ScpException(ErrorCode.INTERNAL, "quorum lost injected");
                throw deathCause;
            }
            delegate.appendLog(logFileId, frameBytes);
        }

        @Override
        public synchronized byte[] readLog(FileId logFileId) throws Exception {
            if (logFileId.equals(deadLog)) {
                deadLog = null;
                deathCause = null;
            }
            return delegate.readLog(logFileId);
        }

        @Override
        public FileId writeSnapshot(StrataNamespace ns, long generation, byte[] snapshotBytes) throws Exception {
            return delegate.writeSnapshot(ns, generation, snapshotBytes);
        }

        @Override
        public byte[] readSnapshot(FileId snapshotFileId) throws Exception {
            return delegate.readSnapshot(snapshotFileId);
        }

        @Override
        public void deleteFile(FileId fileId) throws Exception {
            delegate.deleteFile(fileId);
        }
    }
}
