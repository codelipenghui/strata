package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Group commit (tech design §5.3): pipelined fsync-mode appends are acked by coalesced forces —
 * far fewer force() calls than appends — and an acked append is always on disk (covered by a
 * completed force), which restart-recovery must confirm.
 */
class GroupCommitTest {

    @TempDir
    Path dir;

    private final ChunkId id = new ChunkId(FileId.random(), 0);

    @Test
    void pipelinedFsyncAppendsCoalesceForces() throws Exception {
        int appends = 400;
        try (ChunkStore store = new ChunkStore(dir)) {
            store.open(id, (byte) 0, (byte) 0, ChunkStore.ACK_ON_FSYNC, 1, 1L);

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
        store.open(id, (byte) 0, (byte) 0, ChunkStore.ACK_ON_FSYNC, 1, 1L);
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
            store.open(id, (byte) 0, (byte) 0, ChunkStore.ACK_ON_FSYNC, 1, 1L);
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
            store.open(id, (byte) 0, (byte) 0, ChunkStore.ACK_ON_REPLICATE, 1, 1L);
            byte[] payload = "fast".getBytes();
            long offset = 0;
            for (int i = 0; i < 50; i++) {
                store.appendAsync(id, 1, offset, offset, ByteBuffer.wrap(payload)).join();
                offset += payload.length;
            }
            assertEquals(0, store.fsyncForceCount(), "replicate mode must never group-commit");
        }
    }
}
