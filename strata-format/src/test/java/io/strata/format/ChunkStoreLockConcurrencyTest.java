package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the per-{@code Handle} chunk lock (intrinsic monitor → {@link java.util.concurrent.locks.ReentrantLock}
 * conversion): every critical section must release the lock on EVERY exit path, including the early
 * {@code return}s in {@code appendAsync}/{@code read} and the {@code continue}s in the background-loop
 * sites. A leaked lock surfaces as a deadlock — the next acquirer blocks forever — so each assertion is
 * wrapped in {@link org.junit.jupiter.api.Assertions#assertTimeoutPreemptively} to turn a hang into a
 * failure instead of a stuck build.
 *
 * <p>These tests pass on the original {@code synchronized(h)} code (auto-release) and must stay green
 * after the lock swap; they fail (time out) if any converted site forgets its {@code finally}.
 */
class ChunkStoreLockConcurrencyTest {

    @TempDir
    Path dir;

    static final StrataNamespace TEST_NS = StrataNamespace.of("test");
    static final long NOW = 1718000000000L;

    private ChunkStore newStore() throws IOException {
        return new ChunkStore(dir);
    }

    private static ByteBuffer payload(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte) ('a' + (i % 26));
        return ByteBuffer.wrap(b);
    }

    /**
     * A zero-length append is a durable-offset beacon that returns early from {@code appendAsync}
     * (the {@code len == 0} branch) WITHOUT advancing {@code h.end}. If that early return leaks the
     * chunk lock, the very next op on the same chunk deadlocks. Deterministic, single-threaded.
     */
    @Test
    void earlyReturnDoBeaconAppendReleasesChunkLock() throws Exception {
        try (ChunkStore store = newStore()) {
            ChunkId id = new ChunkId(FileId.of(1), 0);
            store.open(TEST_NS, id, false, 1, NOW);

            // early-return path: zero-length DO beacon
            store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap(new byte[0]));

            // every one of these must be able to acquire the chunk lock immediately
            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                store.stat(TEST_NS, id);
                store.append(TEST_NS, id, 1, 0, 0, payload(64));     // normal early-return at the tail
                store.read(TEST_NS, id, 0, 64);
                store.readLedger(TEST_NS, id, 0);
                store.fence(TEST_NS, id, 5);
            });
            assertEquals(64, store.stat(TEST_NS, id).localEndOffset());
        }
    }

    /**
     * One writer (mirrors the single-epoch-owner reality) appends sequentially — interleaving
     * zero-length beacons and real payloads — while many concurrent readers hammer the same chunk
     * via read/stat/readLedger/describeChunks/orphanSuspects/usedBytes. Asserts: no deadlock, no
     * reader throws, and every byte the writer appended is accounted for (serialized, none lost).
     */
    @Test
    void concurrentMixedOpsOnOneOpenChunkNeverDeadlock() throws Exception {
        try (ChunkStore store = newStore()) {
            ChunkId id = new ChunkId(FileId.of(7), 0);
            store.open(TEST_NS, id, false, 1, NOW);

            final int appendRounds = 1500;
            final int readers = 8;
            final AtomicLong end = new AtomicLong(0);
            final AtomicBoolean writerDone = new AtomicBoolean(false);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final CountDownLatch go = new CountDownLatch(1);

            List<Thread> threads = new ArrayList<>();

            Thread writer = Thread.ofVirtual().name("lock-test-writer").unstarted(() -> {
                try {
                    go.await();
                    for (int i = 0; i < appendRounds; i++) {
                        long base = end.get();
                        if (i % 7 == 0) {
                            // zero-length DO beacon — early-return path, end unchanged
                            store.append(TEST_NS, id, 1, base, base, ByteBuffer.wrap(new byte[0]));
                        } else {
                            int len = 16 + (i % 48);
                            store.append(TEST_NS, id, 1, base, base, payload(len));
                            end.addAndGet(len);
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    writerDone.set(true);
                }
            });
            threads.add(writer);

            for (int r = 0; r < readers; r++) {
                final int which = r;
                threads.add(Thread.ofVirtual().name("lock-test-reader-" + r).unstarted(() -> {
                    try {
                        go.await();
                        while (!writerDone.get()) {
                            long e = end.get();
                            switch (which % 6) {
                                case 0 -> store.stat(TEST_NS, id);
                                case 1 -> store.read(TEST_NS, id, 0, (int) Math.max(1, Math.min(e, 4096)));
                                case 2 -> store.readLedger(TEST_NS, id, 0);
                                case 3 -> store.describeChunks();
                                case 4 -> store.orphanSuspects(0, NOW);
                                default -> store.usedBytes();
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }));
            }

            awaitNoFailure(Duration.ofSeconds(60), failure, () -> {
                threads.forEach(Thread::start);
                go.countDown();
                for (Thread t : threads) t.join();
            });

            assertEquals(end.get(), store.stat(TEST_NS, id).localEndOffset(),
                    "every appended byte must be serialized and accounted for");
        }
    }

    /**
     * Sealing transitions the chunk under the lock and holds it across the committer stop + truncate;
     * concurrent readers must neither deadlock nor see a torn state. Covers the seal(1010) site and the
     * read-vs-seal interaction.
     */
    @Test
    void sealWhileConcurrentlyReadingNeverDeadlocks() throws Exception {
        try (ChunkStore store = newStore()) {
            ChunkId id = new ChunkId(FileId.of(9), 0);
            store.open(TEST_NS, id, false, 1, NOW);
            int len = 4096;
            store.append(TEST_NS, id, 1, 0, 0, payload(len));

            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicBoolean sealed = new AtomicBoolean(false);
            final CountDownLatch go = new CountDownLatch(1);

            List<Thread> readers = new ArrayList<>();
            for (int r = 0; r < 6; r++) {
                readers.add(Thread.ofVirtual().name("seal-test-reader-" + r).unstarted(() -> {
                    try {
                        go.await();
                        while (!sealed.get()) {
                            store.stat(TEST_NS, id);
                            store.read(TEST_NS, id, 0, len);
                        }
                        // one more read after seal: the immutable sealed region
                        store.read(TEST_NS, id, 0, len);
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }));
            }

            awaitNoFailure(Duration.ofSeconds(30), failure, () -> {
                readers.forEach(Thread::start);
                go.countDown();
                store.seal(TEST_NS, id, 1, len, null);
                sealed.set(true);
                for (Thread t : readers) t.join();
            });

            assertEquals(ChunkState.SEALED, store.stat(TEST_NS, id).state());
            assertEquals(len, store.stat(TEST_NS, id).sealedLength());
        }
    }

    // ---- Phase 2: seal()'s off-lock committer-stop window ----

    /**
     * Phase 2 stops the group committer OFF the chunk lock, so for the duration of that window the
     * handle's {@code sealing} flag stands in for "not appendable". Appends MUST be rejected while it
     * is set — otherwise an append could advance end / write bytes that the in-flight seal then
     * finalizes inconsistently. Drives the flag directly (the real window is sub-millisecond).
     */
    @Test
    void appendIsRejectedWhileChunkIsSealing() throws Exception {
        try (ChunkStore store = newStore()) {
            ChunkId id = new ChunkId(FileId.of(11), 0);
            store.open(TEST_NS, id, false, 1, NOW);
            store.append(TEST_NS, id, 1, 0, 0, payload(32));

            Object handle = handleOf(store, id);
            setBool(handle, "sealing", true);
            ScpException e = assertThrows(ScpException.class,
                    () -> store.append(TEST_NS, id, 1, 32, 0, payload(8)));
            assertEquals(ErrorCode.CHUNK_SEALED, e.code());

            setBool(handle, "sealing", false);
            store.append(TEST_NS, id, 1, 32, 0, payload(8)); // resumes once the window closes
            assertEquals(40, store.stat(TEST_NS, id).localEndOffset());
        }
    }

    /**
     * Sealing an ack-on-fsync chunk (which has a live committer → takes the off-lock path) while other
     * threads hammer appends at the seal boundary: no deadlock, no unexpected throw, and the sealed
     * region reads back CRC-verified (a corrupt seal would fail the verified read).
     */
    @Test
    void sealAckOnFsyncConcurrentWithAppendAttemptsStaysConsistent() throws Exception {
        try (ChunkStore store = newStore()) {
            ChunkId id = new ChunkId(FileId.of(12), 0);
            store.open(TEST_NS, id, true, 1, NOW); // ack-on-fsync: has a group committer
            int len = 8192;
            store.append(TEST_NS, id, 1, 0, len, payload(len));

            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicBoolean sealed = new AtomicBoolean(false);
            final CountDownLatch go = new CountDownLatch(1);

            List<Thread> appenders = new ArrayList<>();
            for (int a = 0; a < 4; a++) {
                appenders.add(Thread.ofVirtual().name("seal-race-appender-" + a).unstarted(() -> {
                    try {
                        go.await();
                        while (!sealed.get()) {
                            try {
                                store.append(TEST_NS, id, 1, len, len, payload(8));
                            } catch (ScpException expected) {
                                // CHUNK_SEALED (sealing/sealed) or OFFSET_GAP (end moved) — both fine
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }));
            }

            awaitNoFailure(Duration.ofSeconds(30), failure, () -> {
                appenders.forEach(Thread::start);
                go.countDown();
                store.seal(TEST_NS, id, 1, len, null);
                sealed.set(true);
                for (Thread t : appenders) t.join();
            });

            assertEquals(ChunkState.SEALED, store.stat(TEST_NS, id).state());
            assertEquals(len, store.stat(TEST_NS, id).sealedLength(),
                    "seal must finalize at len; the durable prefix may not be extended past it");
            // verified read of the sealed region: a corrupt/torn seal fails the CRC check here
            assertEquals(len, store.read(TEST_NS, id, 0, len).bytes().length);
        }
    }

    /**
     * A seal (off-lock committer-stop window) racing a delete on the same chunk must never corrupt:
     * the outcome is exactly one of {cleanly sealed, cleanly deleted}, never a half-state that throws
     * on read. Many iterations to shuffle the interleaving.
     */
    @Test
    void concurrentSealAndDeleteLeaveNoTornState() throws Exception {
        for (int iter = 0; iter < 40; iter++) {
            final int it = iter;
            try (ChunkStore store = newStore()) {
                ChunkId id = new ChunkId(FileId.of(40 + it), 0);
                store.open(TEST_NS, id, true, 1, NOW); // ack-on-fsync
                int len = 1024;
                store.append(TEST_NS, id, 1, 0, len, payload(len));

                final AtomicReference<Throwable> err = new AtomicReference<>();
                final CountDownLatch go = new CountDownLatch(1);
                Thread sealer = Thread.ofVirtual().name("sd-sealer-" + it).unstarted(() -> {
                    try {
                        go.await();
                        store.seal(TEST_NS, id, 1, len, null);
                    } catch (ScpException ok) {
                        // CHUNK_NOT_FOUND / CHUNK_SEALED if delete won the race — expected
                    } catch (Throwable t) {
                        err.compareAndSet(null, t);
                    }
                });
                Thread deleter = Thread.ofVirtual().name("sd-deleter-" + it).unstarted(() -> {
                    try {
                        go.await();
                        store.delete(TEST_NS, id);
                    } catch (Throwable t) {
                        err.compareAndSet(null, t);
                    }
                });

                awaitNoFailure(Duration.ofSeconds(20), err, () -> {
                    sealer.start();
                    deleter.start();
                    go.countDown();
                    sealer.join();
                    deleter.join();
                });

                if (store.contains(TEST_NS, id)) {
                    // survived the delete → must be a clean SEALED chunk that reads back
                    assertEquals(ChunkState.SEALED, store.stat(TEST_NS, id).state());
                    assertTrue(store.read(TEST_NS, id, 0, len).bytes().length <= len);
                }
            }
        }
    }

    /**
     * delete() stops the group committer OFF the chunk lock (ack-on-fsync chunks). Two concurrent
     * deleters plus read pressure must never deadlock, the deleters must never throw (delete() returns
     * an {@link ErrorCode}, it is not exceptional), and the chunk must end up gone. Reads racing a delete
     * are an EXPECTED race (CHUNK_NOT_FOUND / ClosedChannelException — clients retry), so readers are
     * pure pressure here and tolerate any error.
     */
    @Test
    void concurrentDeleteWithReadPressureNeverDeadlocksAndEndsDeleted() throws Exception {
        for (int iter = 0; iter < 40; iter++) {
            final int it = iter;
            try (ChunkStore store = newStore()) {
                ChunkId id = new ChunkId(FileId.of(80 + it), 0);
                store.open(TEST_NS, id, true, 1, NOW); // ack-on-fsync → exercises the off-lock delete path
                int len = 1024;
                store.append(TEST_NS, id, 1, 0, len, payload(len));

                final AtomicReference<Throwable> deleterErr = new AtomicReference<>();
                final CountDownLatch go = new CountDownLatch(1);
                List<Thread> threads = new ArrayList<>();
                for (int d = 0; d < 2; d++) {
                    threads.add(Thread.ofVirtual().name("del-" + it + "-" + d).unstarted(() -> {
                        try {
                            go.await();
                            store.delete(TEST_NS, id); // returns a code; must not throw
                        } catch (Throwable t) {
                            deleterErr.compareAndSet(null, t);
                        }
                    }));
                }
                for (int r = 0; r < 4; r++) {
                    threads.add(Thread.ofVirtual().name("rd-" + it + "-" + r).unstarted(() -> {
                        try {
                            go.await();
                            for (int i = 0; i < 80; i++) {
                                try {
                                    store.stat(TEST_NS, id);
                                    store.read(TEST_NS, id, 0, len);
                                } catch (Exception raceExpected) {
                                    // reading a chunk being deleted is an expected race; clients retry
                                }
                            }
                        } catch (Throwable ignored) {
                            // pure pressure thread
                        }
                    }));
                }

                awaitNoFailure(Duration.ofSeconds(20), deleterErr, () -> {
                    threads.forEach(Thread::start);
                    go.countDown();
                    for (Thread t : threads) t.join();
                });

                assertTrue(!store.contains(TEST_NS, id), "chunk must be gone after concurrent deletes");
            }
        }
    }

    // ---- Phase 3: readRegion()'s zero-copy open/acquire moved off the chunk lock ----

    /**
     * readRegion resolves the OPEN client fast path by opening a transient READ FD; Phase 3 moves that
     * blocking open OFF the chunk lock (snapshot under lock, open outside). The durable prefix [0,
     * lastKnownDO) is stable even after the lock is released — a concurrent seal can never truncate below
     * lastKnownDO — so a concurrent writer must never make a reader see a torn/short region. Eight readers
     * hammer readRegion while a writer appends; each reader fully reads the returned region and checks
     * every byte. No deadlock, no corruption, no unexpected throw.
     */
    @Test
    void concurrentReadRegionDuringAppendsReturnsStableDurablePrefix() throws Exception {
        try (ChunkStore store = newStore()) {
            ChunkId id = new ChunkId(FileId.of(130), 0);
            store.open(TEST_NS, id, false, 1, NOW);
            store.append(TEST_NS, id, 1, 0, 0, allBytes(256, (byte) 'X'));
            store.append(TEST_NS, id, 1, 256, 256, allBytes(256, (byte) 'X')); // lastKnownDO -> 256

            final int appendRounds = 800;
            final AtomicLong end = new AtomicLong(512);
            final AtomicBoolean writerDone = new AtomicBoolean(false);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final CountDownLatch go = new CountDownLatch(1);

            List<Thread> threads = new ArrayList<>();
            threads.add(Thread.ofVirtual().name("rr-writer").unstarted(() -> {
                try {
                    go.await();
                    for (int i = 0; i < appendRounds; i++) {
                        long base = end.get();
                        store.append(TEST_NS, id, 1, base, base, allBytes(256, (byte) 'X')); // durableOffset=base
                        end.addAndGet(256);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    writerDone.set(true);
                }
            }));
            for (int r = 0; r < 8; r++) {
                threads.add(Thread.ofVirtual().name("rr-reader-" + r).unstarted(() -> {
                    try {
                        go.await();
                        while (!writerDone.get()) {
                            try (ChunkStore.ReadRegionResult region = store.readRegion(TEST_NS, id, 0, 1 << 20)) {
                                int len = region.length();
                                var ch = region.channel();
                                if (len == 0 || ch == null) continue; // empty/non-channel shape
                                byte[] got = new byte[len];
                                ChunkFormats.readFully(ch, ByteBuffer.wrap(got), region.filePosition());
                                for (int b = 0; b < len; b++) {
                                    if (got[b] != 'X') throw new AssertionError("torn region byte at " + b + " = " + got[b]);
                                }
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }));
            }

            awaitNoFailure(Duration.ofSeconds(60), failure, () -> {
                threads.forEach(Thread::start);
                go.countDown();
                for (Thread t : threads) t.join();
            });
        }
    }

    private static ByteBuffer allBytes(int n, byte v) {
        byte[] b = new byte[n];
        java.util.Arrays.fill(b, v);
        return ByteBuffer.wrap(b);
    }

    /**
     * Runs {@code body} (typically: start the threads, release the latch, join them) within {@code timeout}
     * — turning a leaked-lock deadlock into a failure rather than a hung build — then fails if any worker
     * captured a throwable in {@code failure}.
     */
    private static void awaitNoFailure(Duration timeout, AtomicReference<Throwable> failure,
                                       org.junit.jupiter.api.function.Executable body) {
        assertTimeoutPreemptively(timeout, body);
        assertNull(failure.get(), () -> "a concurrent op failed: " + failure.get());
    }

    private static Object handleOf(ChunkStore store, ChunkId id) throws Exception {
        Field chunksField = ChunkStore.class.getDeclaredField("chunks");
        chunksField.setAccessible(true);
        java.util.Map<?, ?> chunks = (java.util.Map<?, ?>) chunksField.get(store);
        return chunks.get(new io.strata.common.NsChunkId(TEST_NS, id));
    }

    private static void setBool(Object handle, String field, boolean v) throws Exception {
        Field f = handle.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.setBoolean(handle, v);
    }
}
