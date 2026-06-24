package io.strata.format;

import io.strata.common.ErrorCode;
import io.strata.common.ChunkId;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Group commit (tech design §5.3): pipelined fsync-mode appends are acked by coalesced forces —
 * far fewer force() calls than appends — and an acked append is always on disk (covered by a
 * completed force), which restart-recovery must confirm.
 */
class GroupCommitTest {

    @TempDir
    Path dir;

    private final ChunkId id = new ChunkId(FileId.of(1), 0);
    private static final StrataNamespace TEST_NS = StrataNamespace.of("test");

    @Test
    void pipelinedFsyncAppendsCoalesceForces() throws Exception {
        int appends = 400;
        try (ChunkStore store = new ChunkStore(dir)) {
            store.open(TEST_NS, id, true, 1, 1L);

            List<CompletableFuture<ChunkStore.AppendResult>> futures = new ArrayList<>(appends);
            byte[] payload = "group-commit-payload".getBytes();
            long offset = 0;
            for (int i = 0; i < appends; i++) {
                futures.add(store.appendAsync(id, 1, offset, offset, ByteBuffer.wrap(payload)));
                offset += payload.length;
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);

            long forces = store.fsyncForceCount();
            assertTrue(forces >= 1, "at least one force must have happened");
            assertTrue(forces < appends / 2,
                    "forces not coalesced: " + forces + " forces for " + appends + " appends");

            // every acked append completed in order with monotonic end offsets
            for (int i = 0; i < appends; i++) {
                assertEquals((long) (i + 1) * payload.length, futures.get(i).join().endOffset());
            }
            store.seal(id, 1, offset, null);
        }
        // acked data survives restart
        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(id);
            assertEquals(appends * "group-commit-payload".length(), stat.sealedLength());
        }
    }

    @Test
    void ackedFsyncAppendsAreOnDiskBeforeFutureCompletes() throws Exception {
        // sequential (non-pipelined) appends: each future completion implies a covering force;
        // crash-recovery must retain everything acked even with no clean close
        ChunkStore store = new ChunkStore(dir);
        store.open(TEST_NS, id, true, 1, 1L);
        byte[] payload = "durable!".getBytes();
        long offset = 0;
        for (int i = 0; i < 20; i++) {
            store.appendAsync(id, 1, offset, offset, ByteBuffer.wrap(payload)).get(10, TimeUnit.SECONDS);
            offset += payload.length;
        }
        // crash: no close()
        try (ChunkStore recovered = new ChunkStore(dir)) {
            var r = recovered.read(id, 0, 1 << 20);
            assertEquals(offset, r.localEndOffset(), "acked fsync appends lost across crash");
            byte[] expected = new byte[(int) offset];
            for (int i = 0; i < 20; i++) {
                System.arraycopy(payload, 0, expected, i * payload.length, payload.length);
            }
            assertArrayEquals(expected, r.bytes());
        }
    }

    @Test
    void sealWithPipelinedFsyncAppendsDrainsCleanly() throws Exception {
        try (ChunkStore store = new ChunkStore(dir)) {
            store.open(TEST_NS, id, true, 1, 1L);
            byte[] payload = "drain-me".getBytes();
            List<CompletableFuture<ChunkStore.AppendResult>> futures = new ArrayList<>();
            long offset = 0;
            for (int i = 0; i < 50; i++) {
                futures.add(store.appendAsync(id, 1, offset, offset, ByteBuffer.wrap(payload)));
                offset += payload.length;
            }
            // seal immediately: the committer's final force must drain every waiter
            var sealed = store.seal(id, 1, offset, null);
            assertEquals(offset, sealed.finalLength());
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void replicateModeDoesNotForce() throws Exception {
        try (ChunkStore store = new ChunkStore(dir)) {
            store.open(TEST_NS, id, false, 1, 1L);
            byte[] payload = "fast".getBytes();
            long offset = 0;
            for (int i = 0; i < 50; i++) {
                store.appendAsync(id, 1, offset, offset, ByteBuffer.wrap(payload)).join();
                offset += payload.length;
            }
            assertEquals(0, store.fsyncForceCount(), "replicate mode must never group-commit");
        }
    }

    @Test
    void fsyncFailurePoisonsCurrentAndFutureWaiters() {
        AtomicLong forces = new AtomicLong();
        GroupCommitter committer = new GroupCommitter("fail", () -> {
            throw new IOException("disk unavailable");
        }, forces);

        CompletionException first = assertThrows(CompletionException.class,
                () -> committer.awaitFlush(1).join());
        assertTrue(first.getCause() instanceof ScpException se && se.code() == ErrorCode.INTERNAL,
                "expected INTERNAL ScpException, got " + first.getCause());
        assertEquals(0, forces.get(), "failed force must not be counted as durable");

        CompletionException second = assertThrows(CompletionException.class,
                () -> committer.awaitFlush(2).join());
        assertTrue(second.getCause() instanceof ScpException se && se.code() == ErrorCode.INTERNAL,
                "expected poisoned committer failure, got " + second.getCause());

        assertTrue(committer.closeAndConfirm());
        assertTrue(committer.isPoisoned());
    }

    @Test
    void awaitAfterCloseFailsUnlessAlreadyFlushed() {
        AtomicLong forces = new AtomicLong();
        GroupCommitter committer = new GroupCommitter("closed", forces::incrementAndGet, forces);
        assertTrue(committer.closeAndConfirm());

        assertEquals(null, committer.awaitFlush(0).join());
        CompletionException e = assertThrows(CompletionException.class, () -> committer.awaitFlush(1).join());
        assertTrue(e.getCause() instanceof ScpException se && se.code() == ErrorCode.CHUNK_SEALED,
                "expected CHUNK_SEALED after close, got " + e.getCause());

        committer.close();
    }

    @Test
    void terminalFailureCompletesQueuedWaitersAndPoisonsFutureWaiters() throws Exception {
        AtomicLong forces = new AtomicLong();
        CountDownLatch forceStarted = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        GroupCommitter committer = new GroupCommitter("terminal", () -> {
            forceStarted.countDown();
            try {
                if (!releaseForce.await(1, TimeUnit.SECONDS)) {
                    throw new IOException("test did not release force");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            }
        }, forces);

        CompletableFuture<Void> waiter = committer.awaitFlush(1);
        assertTrue(forceStarted.await(1, TimeUnit.SECONDS));

        Method failRemaining = GroupCommitter.class.getDeclaredMethod("failRemainingLocked", String.class);
        failRemaining.setAccessible(true);
        failRemaining.invoke(committer, "terminal failure");

        CompletionException first = assertThrows(CompletionException.class, waiter::join);
        assertTrue(first.getCause() instanceof ScpException se && se.code() == ErrorCode.INTERNAL,
                "expected terminal failure, got " + first.getCause());

        CompletionException second = assertThrows(CompletionException.class,
                () -> committer.awaitFlush(2).join());
        assertTrue(second.getCause() instanceof ScpException se && se.code() == ErrorCode.INTERNAL,
                "expected poisoned committer failure, got " + second.getCause());

        releaseForce.countDown();
        assertTrue(committer.closeAndConfirm());
        assertTrue(committer.isPoisoned());
    }

    @Test
    void flusherInterruptPoisonsFutureWaiters() throws Exception {
        AtomicLong forces = new AtomicLong();
        GroupCommitter committer = new GroupCommitter("interrupt", forces::incrementAndGet, forces);
        Thread flusher = flusherThread(committer);

        flusher.interrupt();
        flusher.join(1_000);

        assertTrue(!flusher.isAlive(), "flusher did not stop after interrupt");
        CompletionException e = assertThrows(CompletionException.class,
                () -> committer.awaitFlush(1).join());
        assertTrue(e.getCause() instanceof ScpException se && se.code() == ErrorCode.INTERNAL,
                "expected poisoned committer failure, got " + e.getCause());
        assertTrue(committer.closeAndConfirm());
        assertTrue(committer.isPoisoned());
    }

    @Test
    void closeAndConfirmPreservesCallerInterrupt() throws Exception {
        AtomicLong forces = new AtomicLong();
        GroupCommitter committer = new GroupCommitter("caller-interrupted", forces::incrementAndGet, forces);

        Thread.currentThread().interrupt();
        try {
            committer.closeAndConfirm();
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            assertTrue(committer.closeAndConfirm());
        }
    }

    private static Thread flusherThread(GroupCommitter committer) throws ReflectiveOperationException {
        Field flusher = GroupCommitter.class.getDeclaredField("flusher");
        flusher.setAccessible(true);
        return (Thread) flusher.get(committer);
    }
}
