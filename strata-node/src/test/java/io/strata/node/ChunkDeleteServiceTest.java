package io.strata.node;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.format.ChunkStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkDeleteServiceTest {
    private static final StrataNamespace NS = StrataNamespace.of("test");

    @TempDir
    Path dir;

    @Test
    void pacesPhysicalDeleteStarts() throws Exception {
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"))) {
            ChunkId first = new ChunkId(FileId.of(1), 0);
            ChunkId second = new ChunkId(FileId.of(2), 0);
            seal(store, first);
            seal(store, second);

            ChunkDeleteService deletes = new ChunkDeleteService(store, 1, 150);
            CountDownLatch start = new CountDownLatch(1);
            FutureTask<ErrorCode> a = new FutureTask<>(() -> {
                start.await();
                return deletes.delete(NS, first);
            });
            FutureTask<ErrorCode> b = new FutureTask<>(() -> {
                start.await();
                return deletes.delete(NS, second);
            });
            Thread.ofVirtual().start(a);
            Thread.ofVirtual().start(b);

            long t0 = System.nanoTime();
            start.countDown();

            assertEquals(ErrorCode.OK, a.get(5, TimeUnit.SECONDS));
            assertEquals(ErrorCode.OK, b.get(5, TimeUnit.SECONDS));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertTrue(elapsedMs >= 100, "second delete should be paced; elapsedMs=" + elapsedMs);
            assertEquals(2, deletes.okDeletes());
            assertEquals(0, deletes.inFlightDeletes());
            assertEquals(0, deletes.waitingDeletes());
        }
    }

    @Test
    void interruptedPacingWaitIsNotCountedAsFailedDelete() throws Exception {
        try (ChunkStore store = new ChunkStore(dir.resolve("interrupt-chunks"))) {
            ChunkId first = new ChunkId(FileId.of(3), 0);
            ChunkId second = new ChunkId(FileId.of(4), 0);
            seal(store, first);
            seal(store, second);

            ChunkDeleteService deletes = new ChunkDeleteService(store, 1, 60_000);
            assertEquals(ErrorCode.OK, deletes.delete(NS, first));

            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread worker = Thread.ofVirtual().start(() -> {
                try {
                    deletes.delete(NS, second);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });

            waitFor(() -> deletes.waitingDeletes() == 0 && deletes.inFlightDeletes() == 0);
            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(5));

            assertTrue(failure.get() instanceof InterruptedException);
            assertEquals(0, deletes.failedDeletes(), "shutdown interrupt must not look like delete failure");
            assertEquals(1, deletes.okDeletes());
            assertEquals(0, deletes.inFlightDeletes());
            assertEquals(0, deletes.waitingDeletes());
        }
    }

    @Test
    void resultCountersTrackNotFoundAndStoreFailures() throws Exception {
        try (ChunkStore store = new ChunkStore(dir.resolve("counter-chunks"))) {
            ChunkDeleteService deletes = new ChunkDeleteService(store, 1, 0);

            assertEquals(ErrorCode.CHUNK_NOT_FOUND, deletes.delete(NS, new ChunkId(FileId.of(5), 0)));
            assertEquals(1, deletes.notFoundDeletes());

            ChunkId creating = new ChunkId(FileId.of(6), 0);
            creatingSet(store).add(newNsChunkId(creating));
            assertEquals(ErrorCode.INTERNAL, deletes.delete(NS, creating));
            assertEquals(1, deletes.failedDeletes());
        }
    }

    private static void seal(ChunkStore store, ChunkId id) throws Exception {
        byte[] bytes = ("payload-" + id.index()).getBytes();
        store.open(NS, id, false, 1, 1L);
        store.append(NS, id, 1, 0, 0, ByteBuffer.wrap(bytes));
        store.seal(NS, id, 1, bytes.length, null);
    }

    @SuppressWarnings("unchecked")
    private static java.util.Set<Object> creatingSet(ChunkStore store) throws Exception {
        java.lang.reflect.Field field = ChunkStore.class.getDeclaredField("creating");
        field.setAccessible(true);
        return (java.util.Set<Object>) field.get(store);
    }

    private static Object newNsChunkId(ChunkId id) throws Exception {
        Class<?> type = Class.forName("io.strata.common.NsChunkId");
        java.lang.reflect.Constructor<?> ctor =
                type.getDeclaredConstructor(StrataNamespace.class, ChunkId.class);
        ctor.setAccessible(true);
        return ctor.newInstance(NS, id);
    }

    private static void waitFor(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
