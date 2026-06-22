package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A slow durable append in one namespace must not block a mutation in another (design §8 — per-namespace
 * single-writer ordering with no cross-namespace head-of-line blocking).
 *
 * <p>The {@link BlockingFileStore} wraps the real in-memory file store and stalls the FIRST namespace's
 * open-log append on a latch. Capturing the block target as the first {@code createLogFile()} id is
 * race-free: namespace A's {@code createFile} is launched (and {@code entered} awaited) before B's, so
 * A's repo — and therefore A's log file — is created first. B's log is a different id and is never blocked.
 * Under a single global lock, B cannot proceed while A holds it mid-append, so {@code bCreate.get} times
 * out; with a per-namespace lock B completes immediately.
 */
class NamespaceLogPerNamespaceLockTest {

    /** Wraps a delegate file store; blocks appendLog for the first log fileId created until released. */
    static final class BlockingFileStore implements NamespaceMetadataFileStore {
        final NamespaceMetadataFileStore delegate;
        final CountDownLatch block = new CountDownLatch(1);
        final CountDownLatch entered = new CountDownLatch(1);
        // The first log file ever created — namespace A's, since A's createFile runs first.
        final AtomicReference<FileId> blockLogId = new AtomicReference<>();

        BlockingFileStore(NamespaceMetadataFileStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public FileId createLogFile() throws Exception {
            FileId id = delegate.createLogFile();
            blockLogId.compareAndSet(null, id); // arm on A's first log, race-free
            return id;
        }

        @Override
        public void appendLog(FileId logFileId, byte[] frameBytes) throws Exception {
            if (logFileId.equals(blockLogId.get())) {
                entered.countDown();
                block.await();
            }
            delegate.appendLog(logFileId, frameBytes);
        }

        @Override
        public byte[] readLog(FileId logFileId) throws Exception {
            return delegate.readLog(logFileId);
        }

        @Override
        public FileId writeSnapshot(byte[] snapshotBytes) throws Exception {
            return delegate.writeSnapshot(snapshotBytes);
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
    void slowAppendInOneNamespaceDoesNotBlockAnother() throws Exception {
        StrataNamespace nsA = StrataNamespace.of("ns-a");
        StrataNamespace nsB = StrataNamespace.of("ns-b");
        try (TestingServer zk = NamespaceLogTestSupport.testingServer();
             ZkMetadataStore root = NamespaceLogTestSupport.inMemoryRoot(zk)) {
            BlockingFileStore blocking =
                    new BlockingFileStore(NamespaceLogTestSupport.inMemoryFileStore());
            try (NamespaceLogBackend backend = new NamespaceLogBackend(root, blocking, false)) {
                // A's createFile creates A's repo+log first, then its first append blocks on the latch.
                FileId fA = FileId.random();
                CompletableFuture<Void> aCreate = CompletableFuture.runAsync(() -> sneaky(() ->
                        backend.createFile(NamespaceLogTestSupport.fileRecord(fA, nsA, StrataPath.of("/a")))));
                CompletableFuture<Void> bCreate = null;
                try {
                    assertTrue(blocking.entered.await(2, TimeUnit.SECONDS), "A's append should be in flight");

                    // While A is blocked mid-append (holding A's lock), B must proceed.
                    FileId fB = FileId.random();
                    bCreate = CompletableFuture.runAsync(() -> sneaky(() -> backend.createFile(
                            NamespaceLogTestSupport.fileRecord(fB, nsB, StrataPath.of("/b")))));
                    // FAILS (times out) under the global lock; PASSES per-namespace.
                    bCreate.get(3, TimeUnit.SECONDS);
                } finally {
                    // Always release A so the blocked append completes and close() never deadlocks — even
                    // on the RED path where bCreate.get times out and throws before we get here.
                    blocking.block.countDown();
                    aCreate.get(5, TimeUnit.SECONDS);
                    if (bCreate != null) {
                        bCreate.get(5, TimeUnit.SECONDS);
                    }
                }
            }
        }
    }

    interface ThrowingRunnable {
        void run() throws Exception;
    }

    static void sneaky(ThrowingRunnable r) {
        try {
            r.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
