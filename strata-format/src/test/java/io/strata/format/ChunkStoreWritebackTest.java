package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Background writeback ({@link ChunkStore#backgroundFlushOnce}) periodically fsyncs OPEN, non-ack-on-
 * fsync chunks that have grown past a threshold, so the seal-time fsync only has to flush a bounded
 * residual. These tests confirm it (1) flushes an eligible chunk while preserving seal correctness,
 * (2) skips ack-on-fsync chunks (their committer already forces), and (3) skips chunks below the
 * threshold. The scheduled daemon may also run during a test, but it can only add flushes that the
 * manual call already accounts for — the assertions hold either way.
 */
class ChunkStoreWritebackTest {

    private static final int MIB = 1 << 20;

    @TempDir
    Path dir;

    private final ChunkId id = new ChunkId(FileId.random(), 0);

    private static int wholeCrc(byte[] data) {
        CRC32C c = new CRC32C();
        c.update(data, 0, data.length);
        return (int) c.getValue();
    }

    private static byte[] randomBytes(long seed, int len) {
        byte[] b = new byte[len];
        new Random(seed).nextBytes(b);
        return b;
    }

    private byte[] readAllVerified(ChunkStore store, int len) throws IOException {
        byte[] out = new byte[len];
        int off = 0;
        while (off < len) {
            byte[] chunk = store.read(id, off, len - off).bytes();
            if (chunk.length == 0) {
                break;
            }
            System.arraycopy(chunk, 0, out, off, chunk.length);
            off += chunk.length;
        }
        assertEquals(len, off, "short read");
        return out;
    }

    @Test
    void flushesEligibleChunkAndPreservesSealCorrectness() throws IOException {
        byte[] all = randomBytes(1, 8 * MIB);            // two 4 MiB ranges
        try (ChunkStore store = new ChunkStore(dir)) {
            store.open(id, false, 1, 1718000000000L);
            store.append(id, 1, 0, 0, ByteBuffer.wrap(all, 0, 6 * MIB)); // > 4 MiB threshold

            store.backgroundFlushOnce();
            assertTrue(store.backgroundFlushes() >= 1, "an open chunk past the threshold must be flushed");

            store.append(id, 1, 6 * MIB, 6 * MIB, ByteBuffer.wrap(all, 6 * MIB, 2 * MIB));
            ChunkStore.SealResult sealed = store.seal(id, 1, all.length, null);
            assertEquals(wholeCrc(all), sealed.dataCrc(), "background flush must not affect the sealed CRC");
            assertArrayEquals(all, readAllVerified(store, all.length));
        }
    }

    @Test
    void skipsAckOnFsyncChunks() throws IOException {
        try (ChunkStore store = new ChunkStore(dir)) {
            store.open(id, true, 1, 1718000000000L); // ack-on-fsync: its committer already forces
            store.append(id, 1, 0, 0, ByteBuffer.wrap(new byte[6 * MIB]));
            store.backgroundFlushOnce();
            assertEquals(0, store.backgroundFlushes(), "ack-on-fsync chunks are flushed by their committer");
        }
    }

    @Test
    void skipsChunksBelowThreshold() throws IOException {
        try (ChunkStore store = new ChunkStore(dir)) {
            store.open(id, false, 1, 1718000000000L);
            store.append(id, 1, 0, 0, ByteBuffer.wrap(new byte[1 * MIB])); // < 4 MiB threshold
            store.backgroundFlushOnce();
            assertEquals(0, store.backgroundFlushes(), "a chunk below the threshold should not be flushed");
        }
    }

    @Test
    void writebackConfPrefersPropertyAndFallsBackSafely() {
        String prop = "strata.test.bgFlush.cfg";
        String env = "STRATA_NONEXISTENT_ENV_XYZ"; // unset in the test JVM
        System.clearProperty(prop);
        try {
            assertEquals(500L, ChunkStore.longConf(prop, env, 500L), "unset → default");
            System.setProperty(prop, "150");
            assertEquals(150L, ChunkStore.longConf(prop, env, 500L), "system property wins");
            System.setProperty(prop, "not-a-number");
            assertEquals(500L, ChunkStore.longConf(prop, env, 500L), "malformed → default");
            System.setProperty(prop, "0");
            assertEquals(500L, ChunkStore.longConf(prop, env, 500L), "non-positive → default");
        } finally {
            System.clearProperty(prop);
        }
    }
}
