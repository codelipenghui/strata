package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void orphanConfirmationRejectsUnboundOrContradictoryVerdicts() {
        assertThrows(IllegalArgumentException.class,
                () -> new NamespaceLogBackend.OrphanConfirmation(true, false, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new NamespaceLogBackend.OrphanConfirmation(false, true, 1));
    }

    @Test
    void orphanConfirmationFailsClosedWithoutLocalHandleOrAuthoritativeManifest() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            NamespaceLogBackend owner = new NamespaceLogBackend(root, new TestNamespaceMetadataFileStore(), false);

            ScpException noHandle = assertThrows(ScpException.class,
                    () -> owner.confirmOrphan(NS, new ChunkId(FileId.of(0), 0), 42));
            assertEquals(ErrorCode.FENCED_EPOCH, noHandle.code());

            FileId id = owner.createFileOwnerAssigned(template("/manifest-loss", 1));
            root.curator().delete().forPath("/strata/meta/namespaces/tenant-a/manifest");

            ScpException missingManifest = assertThrows(ScpException.class,
                    () -> owner.confirmOrphan(NS, new ChunkId(id, 0), 42));
            assertEquals(ErrorCode.FENCED_EPOCH, missingManifest.code(),
                    "manifest loss must never become a missing-file deletion verdict");
        }
    }

    @Test
    void orphanConfirmationFailsInternalForPoisonedRepository() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            DurablyFailingFileStore fileStore = new DurablyFailingFileStore();
            NamespaceLogBackend owner = new NamespaceLogBackend(root, fileStore, false);
            FileId id = owner.createFileOwnerAssigned(template("/before-poison", 1));

            fileStore.failNextAppendAfterWritingFrame();
            assertThrows(ScpException.class,
                    () -> owner.createFileOwnerAssigned(template("/poison", 2)));

            ScpException poisoned = assertThrows(ScpException.class,
                    () -> owner.confirmOrphan(NS, new ChunkId(id, 0), 42));
            assertEquals(ErrorCode.INTERNAL, poisoned.code(),
                    "uncertain applied state must fail closed as INTERNAL");
        }
    }

    @Test
    void authoritativeFetchDoesNotHoldRepositoryMutationLock() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            AtomicBoolean blockAuthoritativeReturn = new AtomicBoolean();
            CountDownLatch authoritativeRead = new CountDownLatch(1);
            CountDownLatch releaseAuthoritativeReturn = new CountDownLatch(1);
            MetadataStore rootView = blockingAuthoritativeRoot(root, blockAuthoritativeReturn,
                    authoritativeRead, releaseAuthoritativeReturn);
            NamespaceLogBackend owner = new NamespaceLogBackend(
                    rootView, new TestNamespaceMetadataFileStore(), false);
            FileId first = owner.createFileOwnerAssigned(template("/first", 1));

            blockAuthoritativeReturn.set(true);
            CompletableFuture<NamespaceLogBackend.OrphanConfirmation> confirmation =
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            return owner.confirmOrphan(NS, new ChunkId(first, 0), 42);
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });
            assertTrue(authoritativeRead.await(2, TimeUnit.SECONDS));

            CompletableFuture<FileId> mutation = CompletableFuture.supplyAsync(() ->
                    sup(() -> owner.createFileOwnerAssigned(template("/concurrent", 2))));
            assertEquals(FileId.of(1), mutation.get(2, TimeUnit.SECONDS),
                    "append must not wait behind the authoritative ZooKeeper round trip");

            releaseAuthoritativeReturn.countDown();
            NamespaceLogBackend.OrphanConfirmation result = confirmation.get(2, TimeUnit.SECONDS);
            assertTrue(result.fileExists());
            assertFalse(result.referencedByNode());
        }
    }

    @Test
    void staleOwnerCannotAuthorizeOrphanButCurrentOwnerConfirmsReferencedReplica() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();
            NamespaceLogBackend ownerA = new NamespaceLogBackend(root, fileStore, false);

            // A opens the namespace and publishes epoch A, but knows only this placeholder file.
            assertEquals(FileId.of(0), ownerA.createFileOwnerAssigned(template("/a-placeholder", 100)));
            long epochA = ownerA.namespaceOwnerEpoch(NS);

            // B takes over, recovers A's placeholder, and then creates a B-only file/chunk. This is the
            // exact dangerous shape: stale A would report FILE_NOT_FOUND for id1 if its view were trusted.
            NamespaceLogBackend ownerB = new NamespaceLogBackend(root, fileStore, false);
            FileId fileId = ownerB.createFileOwnerAssigned(template("/b-only", 101));
            assertEquals(FileId.of(1), fileId);
            MetadataStore.Versioned<Records.FileRecord> created = ownerB.getFile(NS, fileId).orElseThrow();
            Records.ChunkRecord chunk = new Records.ChunkRecord(
                    0, ChunkState.SEALED, 128, 0xCAFE, 0, List.of(42));
            assertTrue(ownerB.updateFile(created.value().withChunks(List.of(chunk)), created.version()));
            long epochB = ownerB.namespaceOwnerEpoch(NS);
            assertTrue(epochB > epochA);

            ScpException stale = assertThrows(ScpException.class,
                    () -> ownerA.confirmOrphan(NS, new ChunkId(fileId, 0), 42));
            assertEquals(ErrorCode.FENCED_EPOCH, stale.code(),
                    "stale A must fail closed, not return a destructive verdict");
            ScpException staleRepairEpoch = assertThrows(ScpException.class,
                    () -> ownerA.authoritativeOwnerEpoch(NS));
            assertEquals(ErrorCode.FENCED_EPOCH, staleRepairEpoch.code(),
                    "repair/delete passes must fail the same authoritative manifest gate");
            assertEquals(epochA, ownerA.namespaceOwnerEpoch(NS),
                    "orphan confirmation must never auto-reacquire a newer epoch");
            assertEquals(0, ownerA.metrics().stats().get(NS.value())[NamespaceLogMetrics.REACQUISITIONS],
                    "stale destructive confirms must not reclaim namespace ownership");

            NamespaceLogBackend.OrphanConfirmation current =
                    ownerB.confirmOrphan(NS, new ChunkId(fileId, 0), 42);
            assertTrue(current.fileExists());
            assertTrue(current.referencedByNode(),
                    "current B must keep the chunk because its descriptor still references node 42");
            assertEquals(epochB, current.ownerEpoch(),
                    "verdict and authority epoch come from the same locked repository image");
        }
    }

    @Test
    void namespaceIsRecoveringUntilOpenBarrierPublishesActive() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore delegate = new TestNamespaceMetadataFileStore();
            NamespaceLogCowCompactionTest.BlockingSnapshotFileStore blocking =
                    new NamespaceLogCowCompactionTest.BlockingSnapshotFileStore(delegate);
            blocking.armed = true;

            try (NamespaceLogBackend owner = new NamespaceLogBackend(root, blocking, false)) {
                CompletableFuture<FileId> create = CompletableFuture.supplyAsync(() ->
                        sup(() -> owner.createFileOwnerAssigned(template("/recovering", 10))));

                assertTrue(blocking.entered.await(2, TimeUnit.SECONDS),
                        "open recovery must park before the manifest publish");
                assertEquals(NamespaceLeaderState.RECOVERING, owner.leaderState(NS),
                        "the namespace is not active while the open recovery barrier is still running");
                assertFalse(owner.isNamespaceActive(NS),
                        "non-ACTIVE namespaces must not be treated as serviceable");
                assertEquals(0, owner.namespaceActiveSinceMs(NS),
                        "activeSince is published only after the recovery barrier completes");
                assertEquals(0, owner.loadedNamespaceCount(),
                        "a recovering repo must not be published as a loaded active namespace");
                ScpException recovering = assertThrows(ScpException.class,
                        () -> owner.confirmOrphan(NS, new ChunkId(FileId.of(0), 0), 42));
                assertEquals(ErrorCode.FENCED_EPOCH, recovering.code(),
                        "a RECOVERING handle must not authorize an orphan verdict");

                blocking.block.countDown();
                assertEquals(FileId.of(0), create.get(5, TimeUnit.SECONDS));
                assertEquals(NamespaceLeaderState.ACTIVE, owner.leaderState(NS));
                assertTrue(owner.isNamespaceActive(NS));
                assertTrue(owner.namespaceActiveSinceMs(NS) > 0,
                        "activeSince is stamped when the namespace becomes ACTIVE");
                assertEquals(1, owner.loadedNamespaceCount());
            }
        }
    }

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
    void staleFileCasFromPreviousRepositoryLineageCannotResurrectDeletingFile() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            FenceStaleLogFileStore fileStore = new FenceStaleLogFileStore();

            NamespaceLogBackend ownerA = new NamespaceLogBackend(root, fileStore, false);
            FileId id = ownerA.createFileOwnerAssigned(template("/aba", 10));
            MetadataStore.Versioned<Records.FileRecord> created = ownerA.getFile(NS, id).orElseThrow();
            assertTrue(ownerA.updateFile(created.value().withWriterEpoch(1), created.version()));
            MetadataStore.Versioned<Records.FileRecord> staleRead = ownerA.getFile(NS, id).orElseThrow();
            assertEquals(1, staleRead.version());

            NamespaceLogBackend ownerB = new NamespaceLogBackend(root, fileStore, false);
            MetadataStore.Versioned<Records.FileRecord> bRead = ownerB.getFile(NS, id).orElseThrow();
            assertEquals(1, bRead.version());
            assertTrue(ownerB.updateFile(bRead.value().withState(FileState.DELETING), bRead.version()));

            boolean committed = ownerA.updateFile(staleRead.value().withState(FileState.SEALED), staleRead.version());

            assertFalse(committed, "stale CAS from owner A must lose after owner B changes the file lineage");
            assertEquals(FileState.DELETING, ownerA.getFile(NS, id).orElseThrow().value().state());
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
            ScpException failedAppend = assertThrows(ScpException.class,
                    () -> owner.createFileOwnerAssigned(template("/b", 2)));
            assertEquals(ErrorCode.INTERNAL, failedAppend.code());

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

    @Test
    void poisonedRepoStillServesLastAppliedReadsUntilNextMutationReAcquires() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            DurablyFailingFileStore fileStore = new DurablyFailingFileStore();
            NamespaceLogBackend owner = new NamespaceLogBackend(root, fileStore, false);

            FileId id0 = owner.createFileOwnerAssigned(template("/a", 1));
            assertEquals(FileId.of(0), id0);

            fileStore.failNextAppendAfterWritingFrame();
            ScpException failedAppend = assertThrows(ScpException.class,
                    () -> owner.createFileOwnerAssigned(template("/b", 2)));
            assertEquals(ErrorCode.INTERNAL, failedAppend.code());

            assertTrue(owner.getFile(NS, id0).isPresent(), "reads serve the last-applied state");
            assertEquals(List.of(id0), owner.listFiles(NS));
            assertEquals(List.of(id0), owner.listFileIds(NS));
            assertEquals(id0, owner.resolvePath(NS, StrataPath.of("/a")).orElseThrow());
            assertTrue(owner.listNamespaces().contains(NS),
                    "one poisoned cached repo must not break namespace enumeration");

            FileId retried = owner.createFileOwnerAssigned(template("/b", 2));
            assertEquals(FileId.of(1), retried);
            assertTrue(owner.getFile(NS, retried).isPresent(),
                    "the next mutation re-acquires and recovers the durable failed append");
        }
    }

    @Test
    void compactionSkipsPoisonedRepoUntilMutationReAcquiresDurableTail() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            DurablyFailingFileStore fileStore = new DurablyFailingFileStore();
            NamespaceLogBackend owner = new NamespaceLogBackend(root, fileStore, false);

            assertEquals(FileId.of(0), owner.createFileOwnerAssigned(template("/a", 1)));

            fileStore.failNextAppendAfterWritingFrame();
            ScpException failedAppend = assertThrows(ScpException.class,
                    () -> owner.createFileOwnerAssigned(template("/b", 2)));
            assertEquals(ErrorCode.INTERNAL, failedAppend.code());

            assertEquals(0, owner.compactOversizedRepos(1),
                    "a poisoned repo must not compact a stale appliedOffset cut");

            FileId id2 = owner.createFileOwnerAssigned(template("/c", 3));
            assertEquals(FileId.of(2), id2,
                    "re-acquire must recover /b before assigning the next id to /c");
            assertTrue(owner.getFile(NS, FileId.of(1)).isPresent(),
                    "the durable-but-unacked /b frame must survive until re-acquire");
        }
    }

    @Test
    void inFlightCompactionAbortsWhenRepoIsPoisonedBeforePublish() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            DurablyFailingFileStore fileStore = new DurablyFailingFileStore();
            NamespaceLogCowCompactionTest.BlockingSnapshotFileStore blocking =
                    new NamespaceLogCowCompactionTest.BlockingSnapshotFileStore(fileStore);
            NamespaceLogBackend owner = new NamespaceLogBackend(root, blocking, false);

            assertEquals(FileId.of(0), owner.createFileOwnerAssigned(template("/a", 1)));
            blocking.armed = true;

            CompletableFuture<Integer> compaction = CompletableFuture.supplyAsync(() ->
                    sup(() -> owner.compactOversizedRepos(1)));
            try {
                assertTrue(blocking.entered.await(2, TimeUnit.SECONDS), "compaction parked off-lock");

                fileStore.failNextAppendAfterWritingFrame();
                ScpException failedAppend = assertThrows(ScpException.class,
                        () -> owner.createFileOwnerAssigned(template("/b", 2)));
                assertEquals(ErrorCode.INTERNAL, failedAppend.code());
            } finally {
                blocking.block.countDown();
            }

            assertEquals(0, (int) compaction.get(5, TimeUnit.SECONDS),
                    "in-flight compaction must abort rather than publish a stale cut");
            FileId id2 = owner.createFileOwnerAssigned(template("/c", 3));
            assertEquals(FileId.of(2), id2,
                    "re-acquire must recover /b before assigning the next id to /c");
            assertTrue(owner.getFile(NS, FileId.of(1)).isPresent(),
                    "the durable-but-unacked /b frame must survive the aborted compaction");
        }
    }

    @Test
    void tombstoneSweepReAcquiresPoisonedRepoAndDoesNotWedge() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            DurablyFailingFileStore fileStore = new DurablyFailingFileStore();
            NamespaceLogBackend owner = new NamespaceLogBackend(root, fileStore, false);

            FileId id0 = owner.createFileOwnerAssigned(template("/a", 1));
            int version = owner.getFile(NS, id0).orElseThrow().version();
            assertTrue(owner.deleteFile(NS, id0, version));

            fileStore.failNextAppendAfterWritingFrame();
            ScpException failedAppend = assertThrows(ScpException.class,
                    () -> owner.createFileOwnerAssigned(template("/b", 2)));
            assertEquals(ErrorCode.INTERNAL, failedAppend.code());

            assertEquals(1, owner.sweepOwnedNamespaceTombstones(0),
                    "sweep must re-acquire before appending TombstoneSwept on a poisoned repo");
            FileId id2 = owner.createFileOwnerAssigned(template("/c", 3));
            assertEquals(FileId.of(2), id2,
                    "sweep recovery must preserve the durable-but-unacked /b frame");
        }
    }

    @Test
    void failedReAcquireRestoresPoisonedRepoSoOwnerChangesStayStable() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            DurablyFailingFileStore fileStore = new DurablyFailingFileStore();
            NamespaceLogBackend owner = new NamespaceLogBackend(root, fileStore, false);

            assertEquals(FileId.of(0), owner.createFileOwnerAssigned(template("/a", 1)));

            fileStore.failNextAppendAfterWritingFrame();
            ScpException failedAppend = assertThrows(ScpException.class,
                    () -> owner.createFileOwnerAssigned(template("/b", 2)));
            assertEquals(ErrorCode.INTERNAL, failedAppend.code());

            fileStore.failNextReadLog();
            ScpException failedReAcquire = assertThrows(ScpException.class,
                    () -> owner.createFileOwnerAssigned(template("/c", 3)));
            assertEquals(ErrorCode.INTERNAL, failedReAcquire.code());

            FileId id2 = owner.createFileOwnerAssigned(template("/c", 3));
            assertEquals(FileId.of(2), id2,
                    "the second re-acquire must recover /b before assigning /c");

            long[] stats = owner.metrics().stats().get(NS.value());
            assertEquals(1, stats[NamespaceLogMetrics.OWNER_CHANGES],
                    "a failed re-acquire must not turn the next successful open into a phantom owner change");
            assertTrue(stats[NamespaceLogMetrics.REACQUISITIONS] >= 2,
                    "both re-acquire attempts are counted as churn, not owner handoffs");
        }
    }

    @Test
    void nonOwnedNamespaceCannotBeOpenedThroughBackend() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            NamespaceLogBackend backend = new NamespaceLogBackend(root, new TestNamespaceMetadataFileStore(), false);
            backend.setOwnership(ns -> false);

            ScpException rejected = assertThrows(ScpException.class,
                    () -> backend.createFileOwnerAssigned(template("/not-owned", 9)));

            assertEquals(ErrorCode.NOT_LEADER, rejected.code());
        }
    }

    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private static <T> T sup(ThrowingSupplier<T> s) {
        try {
            return s.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Records.FileRecord template(String path, long opId) {
        return new Records.FileRecord(FileId.of(0), NS, StrataPath.of(path),
                3, 2, true, FileState.OPEN, 100L, List.of(), opId, opId);
    }

    private static MetadataStore blockingAuthoritativeRoot(
            ZkMetadataStore delegate,
            AtomicBoolean block,
            CountDownLatch authoritativeRead,
            CountDownLatch release) {
        return (MetadataStore) Proxy.newProxyInstance(
                MetadataStore.class.getClassLoader(), new Class<?>[] {MetadataStore.class},
                (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(delegate, args);
                        if (method.getName().equals("getNamespaceManifestAuthoritative") && block.get()) {
                            authoritativeRead.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("timed out waiting to release authoritative read");
                            }
                        }
                        return result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    /**
     * Wraps {@link TestNamespaceMetadataFileStore} with epoch fencing: only the most recently created
     * log file accepts appends; an append to any superseded log file throws {@code FENCED_EPOCH}, as the
     * Strata data plane fences a deposed meta-log writer. Reads are never fenced (recovery must still read
     * the superseded tail).
     */
    private static final class FenceStaleLogFileStore extends TestNamespaceMetadataFileStore {
        private volatile FileId latestLog;

        @Override
        public FileId createLogFile(StrataNamespace ns, long generation) {
            FileId id = super.createLogFile(ns, generation);
            latestLog = id;
            return id;
        }

        @Override
        public void appendLog(FileId logFileId, byte[] frameBytes) {
            if (!logFileId.equals(latestLog)) {
                throw new ScpException(ErrorCode.FENCED_EPOCH,
                        "FENCED_EPOCH: stale meta-log writer " + logFileId + " superseded by " + latestLog);
            }
            super.appendLog(logFileId, frameBytes);
        }
    }

    /**
     * Simulates the production dead-appender shape for a non-fenced append failure: one append frame may
     * already be durable, but the appender returns INTERNAL and every later append to the same log repeats
     * that death cause until recovery reads/seals the log.
     */
    private static final class DurablyFailingFileStore extends TestNamespaceMetadataFileStore {
        private boolean failNextAppendAfterWritingFrame;
        private boolean failNextReadLog;
        private FileId deadLog;
        private ScpException deathCause;

        void failNextAppendAfterWritingFrame() {
            failNextAppendAfterWritingFrame = true;
        }

        void failNextReadLog() {
            failNextReadLog = true;
        }

        @Override
        public synchronized void appendLog(FileId logFileId, byte[] frameBytes) {
            if (logFileId.equals(deadLog)) {
                throw deathCause;
            }
            if (failNextAppendAfterWritingFrame) {
                failNextAppendAfterWritingFrame = false;
                super.appendLog(logFileId, frameBytes);
                deadLog = logFileId;
                deathCause = new ScpException(ErrorCode.INTERNAL, "quorum lost injected");
                throw deathCause;
            }
            super.appendLog(logFileId, frameBytes);
        }

        @Override
        public synchronized byte[] readLog(FileId logFileId) {
            if (failNextReadLog) {
                failNextReadLog = false;
                throw new ScpException(ErrorCode.INTERNAL, "readLog injected");
            }
            if (logFileId.equals(deadLog)) {
                deadLog = null;
                deathCause = null;
            }
            return super.readLog(logFileId);
        }
    }
}
