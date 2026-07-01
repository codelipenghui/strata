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

    private static void seal(ChunkStore store, ChunkId id) throws Exception {
        byte[] bytes = ("payload-" + id.index()).getBytes();
        store.open(NS, id, false, 1, 1L);
        store.append(NS, id, 1, 0, 0, ByteBuffer.wrap(bytes));
        store.seal(NS, id, 1, bytes.length, null);
    }
}
