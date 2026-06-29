package io.strata.meta;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Non-blocking (copy-on-write) open-log compaction (design §10, issue #7). Compaction must encode the
 * snapshot and write the snapshot/log files OFF the namespace's mutation lock, holding the lock only for
 * the state freeze and the final manifest CAS + pointer swap — so a namespace keeps accepting metadata
 * writes for the (size-proportional) duration of its own compaction.
 *
 * <p>The {@link BlockingSnapshotFileStore} parks the compaction inside the off-lock snapshot write so the
 * test can interleave a same-namespace append against it. Records appended in that freeze→CAS window land
 * in the old open log and must be carried into the new published segment, or they are lost on cutover.
 */
class NamespaceLogCowCompactionTest {

    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");

    private static Records.FileRecord file(int id, String path) {
        return new Records.FileRecord(FileId.of(id), NS, StrataPath.of(path), 3, 2, true,
                FileState.OPEN, 1_000, List.of(), id, 0L);
    }

    /** Wraps a delegate file store; parks the FIRST armed {@code writeSnapshot} on a latch. */
    static final class BlockingSnapshotFileStore implements NamespaceMetadataFileStore {
        final NamespaceMetadataFileStore delegate;
        final CountDownLatch block = new CountDownLatch(1);
        final CountDownLatch entered = new CountDownLatch(1);
        volatile boolean armed = false; // off during setup so the open-time snapshot is not blocked
        private boolean blockedOnce = false;

        BlockingSnapshotFileStore(NamespaceMetadataFileStore delegate) {
            this.delegate = delegate;
        }

        private synchronized boolean takeBlockToken() {
            if (blockedOnce) {
                return false;
            }
            blockedOnce = true;
            return true;
        }

        @Override
        public FileId writeSnapshot(StrataNamespace ns, long generation, byte[] snapshotBytes) throws Exception {
            if (armed && takeBlockToken()) {
                entered.countDown();
                block.await();
            }
            return delegate.writeSnapshot(ns, generation, snapshotBytes);
        }

        @Override
        public FileId createLogFile(StrataNamespace ns, long generation) throws Exception {
            return delegate.createLogFile(ns, generation);
        }

        @Override
        public void appendLog(FileId logFileId, byte[] frameBytes) throws Exception {
            delegate.appendLog(logFileId, frameBytes);
        }

        @Override
        public byte[] readLog(FileId logFileId) throws Exception {
            return delegate.readLog(logFileId);
        }

        @Override
        public byte[] readSnapshot(FileId snapshotFileId) throws Exception {
            return delegate.readSnapshot(snapshotFileId);
        }

        @Override
        public void deleteFile(FileId fileId) throws Exception {
            delegate.deleteFile(fileId);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    @Test
    void anAppendCompletesWhileTheSameNamespaceIsCompactingOffLock() throws Exception {
        try (TestingServer zk = NamespaceLogTestSupport.testingServer();
             ZkMetadataStore root = NamespaceLogTestSupport.inMemoryRoot(zk)) {
            TestNamespaceMetadataFileStore fs = NamespaceLogTestSupport.inMemoryFileStore();
            BlockingSnapshotFileStore blocking = new BlockingSnapshotFileStore(fs);
            try (NamespaceLogBackend backend = new NamespaceLogBackend(root, blocking, false)) {
                for (int i = 1; i <= 3; i++) {
                    backend.createFile(file(i, "/f" + i));
                }
                blocking.armed = true; // the next snapshot write (the compaction's) parks off-lock

                CompletableFuture<Integer> compaction = CompletableFuture.supplyAsync(() ->
                        sup(() -> backend.compactOversizedRepos(1)));
                try {
                    assertTrue(blocking.entered.await(2, TimeUnit.SECONDS),
                            "compaction should reach the off-lock snapshot encode/write");

                    // While compaction is parked in the off-lock snapshot write, a mutation on the SAME
                    // namespace must still acquire the mutation lock and complete. Under the old
                    // lock-across-compaction design this createFile blocks on the held lock and times out.
                    CompletableFuture<Void> append = CompletableFuture.runAsync(() ->
                            run(() -> backend.createFile(file(4, "/f4"))));
                    append.get(3, TimeUnit.SECONDS);
                } finally {
                    blocking.block.countDown();
                }
                assertEquals(1, (int) compaction.get(5, TimeUnit.SECONDS),
                        "the oversized namespace was compacted");
            }
        }
    }

    @Test
    void anAppendInTheFreezeToCasWindowSurvivesCompaction() throws Exception {
        try (TestingServer zk = NamespaceLogTestSupport.testingServer();
             ZkMetadataStore root = NamespaceLogTestSupport.inMemoryRoot(zk)) {
            TestNamespaceMetadataFileStore fs = NamespaceLogTestSupport.inMemoryFileStore();
            BlockingSnapshotFileStore blocking = new BlockingSnapshotFileStore(fs);
            try (NamespaceLogBackend backend = new NamespaceLogBackend(root, blocking, false)) {
                for (int i = 1; i <= 3; i++) {
                    backend.createFile(file(i, "/f" + i));
                }
                blocking.armed = true;

                CompletableFuture<Integer> compaction = CompletableFuture.supplyAsync(() ->
                        sup(() -> backend.compactOversizedRepos(1)));
                try {
                    assertTrue(blocking.entered.await(2, TimeUnit.SECONDS), "compaction parked off-lock");

                    // This append lands AFTER the snapshot freeze (cut) but BEFORE the manifest CAS — it goes
                    // to the old open log and must be carried into the new segment, or it is lost on cutover.
                    CompletableFuture<Void> windowAppend = CompletableFuture.runAsync(() ->
                            run(() -> backend.createFile(file(4, "/f4"))));
                    windowAppend.get(3, TimeUnit.SECONDS);
                } finally {
                    blocking.block.countDown();
                }
                assertEquals(1, (int) compaction.get(5, TimeUnit.SECONDS), "compaction published");

                // A successor recovers from the freshly published manifest: f1..f3 from the snapshot, and
                // f4 — the freeze→CAS-window append — from the carried open-log tail.
                NamespaceMetadataLogRepository successor =
                        NamespaceMetadataLogRepository.open(NS, fs, root, 999);
                for (int i = 1; i <= 4; i++) {
                    assertTrue(successor.state().file(FileId.of(i)).isPresent(),
                            "file " + i + " must survive compaction (f4 is the freeze→CAS-window append)");
                }
            }
        }
    }

    /**
     * Mimics the production {@code StrataSystemMetadataFileStore.readLog}, which recover-and-SEALS the file:
     * a {@code readLog} marks the id sealed, and any later {@code appendLog} to a sealed id fails with
     * {@code FILE_SEALED}. A windowed compaction must therefore NOT read the live open log to carry its tail.
     */
    static final class SealOnReadFileStore implements NamespaceMetadataFileStore {
        final NamespaceMetadataFileStore delegate;
        final Set<FileId> sealed = ConcurrentHashMap.newKeySet();
        final AtomicInteger readLogCalls = new AtomicInteger();

        SealOnReadFileStore(NamespaceMetadataFileStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] readLog(FileId logFileId) throws Exception {
            readLogCalls.incrementAndGet();
            sealed.add(logFileId); // production readLog recover-and-seals the open log
            return delegate.readLog(logFileId);
        }

        @Override
        public void appendLog(FileId logFileId, byte[] frameBytes) throws Exception {
            if (sealed.contains(logFileId)) {
                throw new ScpException(ErrorCode.FILE_SEALED, "metadata log " + logFileId + " is sealed");
            }
            delegate.appendLog(logFileId, frameBytes);
        }

        @Override
        public FileId createLogFile(StrataNamespace ns, long generation) throws Exception {
            return delegate.createLogFile(ns, generation);
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
            sealed.remove(fileId);
            delegate.deleteFile(fileId);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    @Test
    void aWindowedCompactionCarriesTheTailWithoutReadingTheLiveLog() throws Exception {
        // Happy path against a SEALING readLog (production semantics). The freeze→CAS-window append must be
        // carried into the new published log AND the carry must never read (and so never seal) the live old
        // open log: a successor recovers snapshot@cut (f1..f3) + the carried tail (f4) exactly.
        try (TestingServer zk = NamespaceLogTestSupport.testingServer();
             ZkMetadataStore root = NamespaceLogTestSupport.inMemoryRoot(zk)) {
            TestNamespaceMetadataFileStore fs = NamespaceLogTestSupport.inMemoryFileStore();
            BlockingSnapshotFileStore blocking = new BlockingSnapshotFileStore(fs);
            SealOnReadFileStore sealing = new SealOnReadFileStore(blocking);
            try (NamespaceLogBackend backend = new NamespaceLogBackend(root, sealing, false)) {
                for (int i = 1; i <= 3; i++) {
                    backend.createFile(file(i, "/f" + i));
                }
                blocking.armed = true;

                CompletableFuture<Integer> compaction = CompletableFuture.supplyAsync(() ->
                        sup(() -> backend.compactOversizedRepos(1)));
                try {
                    assertTrue(blocking.entered.await(2, TimeUnit.SECONDS), "compaction parked off-lock");
                    CompletableFuture<Void> windowAppend = CompletableFuture.runAsync(() ->
                            run(() -> backend.createFile(file(4, "/f4"))));
                    windowAppend.get(3, TimeUnit.SECONDS);
                } finally {
                    blocking.block.countDown();
                }
                assertEquals(1, (int) compaction.get(5, TimeUnit.SECONDS), "compaction published");

                // The tail carry must NOT have read (and so must not have sealed) the live old open log.
                // (Capture before opening a successor — recovery legitimately readLog()s the new published log.)
                assertEquals(0, sealing.readLogCalls.get(),
                        "the tail carry must not read/seal the live open log (production readLog recover-and-seals)");

                NamespaceMetadataLogRepository successor =
                        NamespaceMetadataLogRepository.open(NS, sealing, root, 999);
                for (int i = 1; i <= 4; i++) {
                    assertTrue(successor.state().file(FileId.of(i)).isPresent(),
                            "file " + i + " must survive a windowed compaction against a sealing readLog");
                }
                assertTrue(successor.state().file(FileId.of(5)).isEmpty(),
                        "recovery reconstructs exactly snapshot@cut + [cut, appliedOffset) — no spurious file");
            }
        }
    }

    @Test
    void aTransientCasErrorDuringAWindowedCompactionLeavesTheNamespaceWritable() throws Exception {
        // Failure path: the manifest CAS itself throws a transient ZK error (ConnectionLoss) — NOT the
        // version-conflict that returns an empty OptionalInt. The carry happened first; if it had sealed the
        // live old log (the original readLog-based bug), the repo would point logFileId at a sealed log and
        // every later append would fail forever with FILE_SEALED (withRepoReacquiringOnFence retries only
        // FENCED_EPOCH). The namespace must instead remain writable.
        try (TestingServer zk = NamespaceLogTestSupport.testingServer();
             ZkMetadataStore real = NamespaceLogTestSupport.inMemoryRoot(zk)) {
            AtomicBoolean failNextManifestCas = new AtomicBoolean(false);
            MetadataStore root = failManifestCasOnce(real, failNextManifestCas);
            TestNamespaceMetadataFileStore fs = NamespaceLogTestSupport.inMemoryFileStore();
            BlockingSnapshotFileStore blocking = new BlockingSnapshotFileStore(fs);
            SealOnReadFileStore sealing = new SealOnReadFileStore(blocking);
            try (NamespaceLogBackend backend = new NamespaceLogBackend(root, sealing, false)) {
                for (int i = 1; i <= 3; i++) {
                    backend.createFile(file(i, "/f" + i));
                }
                blocking.armed = true;
                failNextManifestCas.set(true); // the compaction's manifest CAS will throw ConnectionLoss

                CompletableFuture<Integer> compaction = CompletableFuture.supplyAsync(() ->
                        sup(() -> backend.compactOversizedRepos(1)));
                try {
                    assertTrue(blocking.entered.await(2, TimeUnit.SECONDS), "compaction parked off-lock");
                    CompletableFuture<Void> windowAppend = CompletableFuture.runAsync(() ->
                            run(() -> backend.createFile(file(4, "/f4"))));
                    windowAppend.get(3, TimeUnit.SECONDS);
                } finally {
                    blocking.block.countDown();
                }
                compaction.get(5, TimeUnit.SECONDS); // the manifest CAS threw; the sweep caught it, kept the repo

                // The transient CAS error is distinct from the lost-CAS empty OptionalInt. With the original
                // readLog-based carry this createFile would fail FOREVER with FILE_SEALED (the carry sealed the
                // live old log and the repo still points logFileId at it); with the buffered carry it succeeds.
                backend.createFile(file(5, "/f5"));
                assertTrue(backend.getFile(NS, FileId.of(5)).isPresent(),
                        "namespace remains writable after a transient manifest-CAS error mid-compaction");
            }
        }
    }

    /**
     * Wraps {@code delegate} so the next {@code putNamespaceManifest} (once {@code armed}) throws a transient
     * {@link KeeperException.ConnectionLossException} — the propagating, non-version-conflict failure that
     * {@code ZkMetadataStore.casCreateOrSet} does NOT map to an empty {@code OptionalInt}. A dynamic proxy
     * forwards every other method to the real store (so {@code ZkMetadataStore}'s overridden defaults stay).
     */
    private static MetadataStore failManifestCasOnce(MetadataStore delegate, AtomicBoolean armed) {
        return (MetadataStore) Proxy.newProxyInstance(
                MetadataStore.class.getClassLoader(),
                new Class<?>[]{MetadataStore.class},
                (proxy, method, methodArgs) -> {
                    if (method.getName().equals("putNamespaceManifest") && armed.compareAndSet(true, false)) {
                        throw new KeeperException.ConnectionLossException();
                    }
                    try {
                        return method.invoke(delegate, methodArgs);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    @Test
    void anOverlappingCompactionIsSkippedNotFenced() throws Exception {
        try (TestingServer zk = NamespaceLogTestSupport.testingServer();
             ZkMetadataStore root = NamespaceLogTestSupport.inMemoryRoot(zk)) {
            TestNamespaceMetadataFileStore fs = NamespaceLogTestSupport.inMemoryFileStore();
            BlockingSnapshotFileStore blocking = new BlockingSnapshotFileStore(fs);
            NamespaceMetadataLogRepository repo =
                    NamespaceMetadataLogRepository.open(NS, blocking, root, 1);
            repo.append(new MetadataLogRecord.FileCreated(FileId.of(1), NS, StrataPath.of("/a"),
                    3, 2, true, 100, 1, 1));
            blocking.armed = true; // the first compaction's snapshot write parks off-lock

            CompletableFuture<Boolean> first = CompletableFuture.supplyAsync(() ->
                    sup(() -> repo.compact(1)));
            try {
                assertTrue(blocking.entered.await(2, TimeUnit.SECONDS), "first compaction parked off-lock");

                // A second compaction while the first is mid-encode must be SKIPPED (the per-repo compacting
                // guard), not proceed to a CAS that would later false-fence the first compaction's publish.
                assertEquals(false, repo.compact(1), "an overlapping compaction is skipped, not attempted");
            } finally {
                blocking.block.countDown();
            }
            assertEquals(true, first.get(5, TimeUnit.SECONDS), "the first compaction published");
        }
    }

    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static <T> T sup(ThrowingSupplier<T> s) {
        try {
            return s.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void run(ThrowingRunnable r) {
        try {
            r.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
