package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Crash-safety tests (tech design §11.3, §16 "crash safety"): a new ChunkStore over the same
 * directory simulates restart-after-crash; direct file mutation simulates torn writes.
 * Recovery must converge to a CRC-verified prefix without parsing payload bytes.
 */
class CrashRecoveryTest {

    @TempDir
    Path dir;

    private final ChunkId id = new ChunkId(FileId.random(), 0);

    private void open(ChunkStore store) throws IOException {
        store.open(id, false, 1, 1718000000000L);
    }

    private Path dataPath() {
        return dir.resolve(ChunkFormats.baseName(id) + ".chunk");
    }

    private Path ledgerPath() {
        return dir.resolve(ChunkFormats.baseName(id) + ".j");
    }

    private Path metaPath() {
        return dir.resolve(ChunkFormats.baseName(id) + ".meta");
    }

    @Test
    void cleanRestartPreservesOpenChunk() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(id, 1, 0, 0, ByteBuffer.wrap("hello".getBytes()));
        store.append(id, 1, 5, 5, ByteBuffer.wrap("world".getBytes()));
        // crash: no close()

        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(id);
            assertEquals(ChunkState.OPEN, stat.state());
            assertEquals(10, stat.localEndOffset());
            assertArrayEquals("helloworld".getBytes(), recovered.read(id, 0, 100).bytes());
            // and the chunk remains appendable at the recovered end
            assertEquals(11, recovered.append(id, 1, 10, 10, ByteBuffer.wrap("!".getBytes())).endOffset());
        }
    }

    @Test
    void tornDataTailTruncatedToLastVerifiedBoundary() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(id, 1, 0, 0, ByteBuffer.wrap("aaaa".getBytes()));
        store.append(id, 1, 4, 0, ByteBuffer.wrap("bbbb".getBytes()));
        // torn write: data file lost the last 2 bytes of the second append
        try (FileChannel ch = FileChannel.open(dataPath(), StandardOpenOption.WRITE)) {
            ch.truncate(ChunkFormats.DATA_START + 6);
        }
        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(4, recovered.stat(id).localEndOffset());
            assertArrayEquals("aaaa".getBytes(), recovered.read(id, 0, 100).bytes());
        }
    }

    @Test
    void corruptDataTailTruncated() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(id, 1, 0, 0, ByteBuffer.wrap("aaaa".getBytes()));
        store.append(id, 1, 4, 0, ByteBuffer.wrap("bbbb".getBytes()));
        // bit rot / partial sector in the second append
        try (FileChannel ch = FileChannel.open(dataPath(), StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{0x7F}), ChunkFormats.DATA_START + 5);
        }
        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(4, recovered.stat(id).localEndOffset());
        }
    }

    @Test
    void tornLedgerEntryDiscarded() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(id, 1, 0, 0, ByteBuffer.wrap("aaaa".getBytes()));
        store.append(id, 1, 4, 0, ByteBuffer.wrap("bbbb".getBytes()));
        // torn ledger: last entry half-written
        long ledgerSize = Files.size(ledgerPath());
        try (FileChannel ch = FileChannel.open(ledgerPath(), StandardOpenOption.WRITE)) {
            ch.truncate(ledgerSize - 7);
        }
        try (ChunkStore recovered = new ChunkStore(dir)) {
            // second append's ledger entry gone -> data beyond first boundary is untrusted
            assertEquals(4, recovered.stat(id).localEndOffset());
            assertEquals(1, recovered.readLedger(id, 0).size());
        }
    }

    @Test
    void garbageBeyondLedgerEndCutOff() throws Exception {
        // covers crash-after-footer-write-before-sidecar-update: bytes beyond the ledger end
        // exist in the data file but are not covered by any ledger entry
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(id, 1, 0, 0, ByteBuffer.wrap("aaaa".getBytes()));
        try (FileChannel ch = FileChannel.open(dataPath(), StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ch.write(ByteBuffer.wrap("garbage-footer-bytes".getBytes()));
        }
        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(4, recovered.stat(id).localEndOffset());
            assertEquals(ChunkFormats.DATA_START + 4, Files.size(dataPath()));
            // chunk is still open and sealable — the client retries the seal
            assertEquals(4, recovered.seal(id, 1, 4, null).finalLength());
        }
    }

    @Test
    void sealedChunkSurvivesRestartAndStragglerLedgerRemoved() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(id, 1, 0, 0, ByteBuffer.wrap("payload".getBytes()));
        store.seal(id, 1, 7, null);
        // simulate crash between sidecar update and ledger delete: recreate a stale ledger file
        Files.write(ledgerPath(), new ChunkFormats.LedgerEntry(7, 0, 1).encode());

        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(id);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(7, stat.sealedLength());
            assertArrayEquals("payload".getBytes(), recovered.read(id, 0, 100).bytes());
            assertFalse(Files.exists(ledgerPath()), "stale ledger must be removed");
        }
    }

    @Test
    void corruptFooterIsDetectedAtRecoveryAndChunkQuarantined() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(id, 1, 0, 0, ByteBuffer.wrap("sealed-data".getBytes()));
        var sealed = store.seal(id, 1, 11, null);
        byte[] repairImage = store.fetch(id, 0, Integer.MAX_VALUE).bytes();
        // bit-rot inside the footer section area (between data end and the 64B trailer)
        try (FileChannel ch = FileChannel.open(dataPath(), StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{0x7F}), ChunkFormats.DATA_START + 11 + 4);
        }
        try (ChunkStore recovered = new ChunkStore(dir)) {
            // a sealed chunk whose footer fails its CRC must be quarantined, not served
            assertEquals(0, recovered.inventory().size(),
                    "corrupt-footer chunk must not be recovered as healthy");
            assertFalse(Files.exists(dataPath()), "live chunk name must be free for repair import");
            try (Stream<Path> files = Files.list(dir)) {
                assertEquals(2, files.filter(p -> p.getFileName().toString().contains(".quarantine-")).count(),
                        "quarantine must preserve the corrupt evidence under a non-live name");
            }
            recovered.importSealed(id, repairImage, 11, sealed.dataCrc());
            assertArrayEquals("sealed-data".getBytes(), recovered.read(id, 0, 100).bytes());
        }
    }

    @Test
    void chunkWithoutSidecarIsRemovedAsUnacked() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(id, 1, 0, 0, ByteBuffer.wrap("x".getBytes()));
        Files.delete(dir.resolve(ChunkFormats.baseName(id) + ".meta"));
        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(0, recovered.inventory().size());
            assertFalse(Files.exists(dataPath()));
        }
    }

    @Test
    void ackedFsyncChunkSurvivesMissingSidecarNameAfterCrash() throws Exception {
        byte[] payload = "acked-fsync".getBytes();
        ChunkStore store = new ChunkStore(dir);
        store.open(id, true, 1, 1718000000000L);
        store.appendAsync(id, 1, 0, 0, ByteBuffer.wrap(payload)).get(5, TimeUnit.SECONDS);
        store.close();

        Files.delete(metaPath());

        assertDoesNotThrow(() -> {
            try (ChunkStore recovered = new ChunkStore(dir)) {
                assertEquals(ChunkState.OPEN, recovered.stat(id).state());
                assertArrayEquals(payload, recovered.read(id, 0, 100).bytes());
            }
        });
    }

    @Test
    void missingSidecarOpenChunkWithFooterShapedPayloadRecoversFromLedger() throws Exception {
        byte[] payload = footerShapedPayload("live".getBytes());
        ChunkStore store = new ChunkStore(dir);
        store.open(id, true, 1, 1718000000000L);
        store.appendAsync(id, 1, 0, 0, ByteBuffer.wrap(payload)).get(5, TimeUnit.SECONDS);
        store.close();

        Files.delete(metaPath());

        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(id);
            assertEquals(ChunkState.OPEN, stat.state());
            assertEquals(payload.length, stat.localEndOffset());
            assertArrayEquals(payload, recovered.read(id, 0, payload.length).bytes());
            assertTrue(Files.exists(ledgerPath()), "ledger must not be deleted by false sealed recovery");
        }
    }

    @Test
    void missingSidecarFsyncOpenChunkRequiresFreshFenceBeforeAppend() throws Exception {
        byte[] payload = "acked-fsync".getBytes();
        ChunkStore store = new ChunkStore(dir);
        store.open(id, true, 1, 1718000000000L);
        store.appendAsync(id, 1, 0, 0, ByteBuffer.wrap(payload)).get(5, TimeUnit.SECONDS);
        store.close();

        Files.delete(metaPath());

        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(ChunkState.OPEN, recovered.stat(id).state());
            ScpException stale = assertThrows(ScpException.class,
                    () -> recovered.append(id, 1, payload.length, payload.length, ByteBuffer.wrap("stale".getBytes())));
            assertEquals(ErrorCode.FENCED_EPOCH, stale.code());

            assertEquals(payload.length, recovered.fence(id, 2).localEndOffset());
            assertEquals(payload.length + 5,
                    recovered.append(id, 2, payload.length, payload.length, ByteBuffer.wrap("fresh".getBytes()))
                            .endOffset());
        }
    }

    private static byte[] footerShapedPayload(byte[] livePrefix) {
        ByteBuffer crcRanges = ByteBuffer.allocate(12);
        crcRanges.putInt(ChunkFormats.CRC_RANGE_SIZE).putInt(1).putInt(Crc.of(livePrefix));
        byte[] crcRangeBytes = crcRanges.array();

        ByteBuffer footer = ByteBuffer.allocate(ChunkFormats.sectionSize(crcRangeBytes));
        ChunkFormats.writeSection(footer, ChunkFormats.SECTION_CRC_RANGES, crcRangeBytes);
        byte[] footerBytes = footer.array();

        ChunkFormats.Trailer trailer = new ChunkFormats.Trailer(livePrefix.length,
                ChunkFormats.DATA_START + livePrefix.length, 1, 0,
                Crc.of(footerBytes), Crc.of(livePrefix));

        ByteBuffer payload = ByteBuffer.allocate(livePrefix.length + footerBytes.length + ChunkFormats.TRAILER_SIZE);
        payload.put(livePrefix).put(footerBytes).put(trailer.encode());
        return payload.array();
    }

    @Test
    void fenceEpochSurvivesCrash() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(id, 1, 0, 0, ByteBuffer.wrap("a".getBytes()));
        store.fence(id, 9); // persists sidecar
        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(io.strata.common.ErrorCode.FENCED_EPOCH,
                    org.junit.jupiter.api.Assertions.assertThrows(io.strata.common.ScpException.class,
                            () -> recovered.append(id, 8, 1, 0, ByteBuffer.wrap("b".getBytes()))).code());
            assertEquals(2, recovered.append(id, 9, 1, 0, ByteBuffer.wrap("b".getBytes())).endOffset());
        }
    }
}
