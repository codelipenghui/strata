package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link ChunkStore} correctly wires {@link ChunkStoreConfig} knobs at runtime.
 */
class ChunkStoreConfigWiringTest {

    @TempDir Path dir;

    private static final StrataNamespace NS = StrataNamespace.of("test");

    @Test
    void maxRequestBytesCapIsHonoredByRead() throws Exception {
        int cap = 4096;
        ChunkStoreConfig cfg = ChunkStoreConfig.DEFAULT.withMaxRequestBytes(cap);
        try (ChunkStore store = new ChunkStore(dir, cfg)) {
            ChunkId id = new ChunkId(FileId.of(1), 0);
            // write a sealed chunk larger than cap
            byte[] payload = new byte[cap * 2]; // 8192 bytes > 4096 cap
            store.open(NS, id, false, 1, 1718000000000L);
            store.append(NS, id, 1, 0, 0, ByteBuffer.wrap(payload));
            store.seal(NS, id, 1, payload.length, null);

            // request more than exists — should be clamped to cap
            var result = store.read(NS, id, 0, Integer.MAX_VALUE);
            assertTrue(result.bytes().length <= cap,
                    "expected read result capped at " + cap + " but got " + result.bytes().length);
            assertEquals(cap, result.bytes().length,
                    "expected exactly " + cap + " bytes from read with custom cap");
        }
    }

    @Test
    void defaultCtorBehavesIdenticallyToExplicitDefault() throws Exception {
        ChunkId id = new ChunkId(FileId.of(2), 0);
        byte[] payload = new byte[9 * 1024 * 1024]; // 9 MiB > default 8 MiB cap

        try (ChunkStore defaultStore = new ChunkStore(dir.resolve("default"))) {
            defaultStore.open(NS, id, false, 1, 1718000000000L);
            defaultStore.append(NS, id, 1, 0, 0, ByteBuffer.wrap(payload));
            defaultStore.seal(NS, id, 1, payload.length, null);
            var r1 = defaultStore.read(NS, id, 0, Integer.MAX_VALUE);
            assertEquals(ChunkStoreConfig.DEFAULT.maxRequestBytes(), r1.bytes().length);
        }

        try (ChunkStore explicitStore = new ChunkStore(dir.resolve("explicit"), ChunkStoreConfig.DEFAULT)) {
            explicitStore.open(NS, id, false, 1, 1718000000000L);
            explicitStore.append(NS, id, 1, 0, 0, ByteBuffer.wrap(payload));
            explicitStore.seal(NS, id, 1, payload.length, null);
            var r2 = explicitStore.read(NS, id, 0, Integer.MAX_VALUE);
            assertEquals(ChunkStoreConfig.DEFAULT.maxRequestBytes(), r2.bytes().length);
        }
    }
}
