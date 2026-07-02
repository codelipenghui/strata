package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FailureInjector;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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

    private final ChunkId id = new ChunkId(FileId.of(1), 0);

    static final StrataNamespace TEST_NS = StrataNamespace.of("test");

    private void open(ChunkStore store) throws IOException {
        store.open(TEST_NS, id, false, 1, 1718000000000L);
    }

    private Path dataPath() {
        return dir.resolve(ChunkFormats.chunkRelativePath(TEST_NS, id) + ".chunk");
    }

    private Path ledgerPath() {
        return dir.resolve(ChunkFormats.chunkRelativePath(TEST_NS, id) + ".j");
    }

    private Path metaPath() {
        return dir.resolve(ChunkFormats.chunkRelativePath(TEST_NS, id) + ".meta");
    }

    @Test
    void cleanRestartPreservesOpenChunk() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("hello".getBytes()));
        store.append(TEST_NS, id, 1, 5, 5, ByteBuffer.wrap("world".getBytes()));
        // crash: no close()

        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(TEST_NS, id);
            assertEquals(ChunkState.OPEN, stat.state());
            assertEquals(10, stat.localEndOffset());
            assertArrayEquals("helloworld".getBytes(), recovered.read(TEST_NS, id, 0, 100).bytes());
            // and the chunk remains appendable at the recovered end
            assertEquals(11, recovered.append(TEST_NS, id, 1, 10, 10, ByteBuffer.wrap("!".getBytes())).endOffset());
        }
    }

    @Test
    void tornDataTailTruncatedToLastVerifiedBoundary() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("aaaa".getBytes()));
        store.append(TEST_NS, id, 1, 4, 0, ByteBuffer.wrap("bbbb".getBytes()));
        // torn write: data file lost the last 2 bytes of the second append
        try (FileChannel ch = FileChannel.open(dataPath(), StandardOpenOption.WRITE)) {
            ch.truncate(ChunkFormats.DATA_START + 6);
        }
        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(4, recovered.stat(TEST_NS, id).localEndOffset());
            assertArrayEquals("aaaa".getBytes(), recovered.read(TEST_NS, id, 0, 100).bytes());
        }
    }

    @Test
    void corruptDataTailTruncated() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("aaaa".getBytes()));
        store.append(TEST_NS, id, 1, 4, 0, ByteBuffer.wrap("bbbb".getBytes()));
        // bit rot / partial sector in the second append
        try (FileChannel ch = FileChannel.open(dataPath(), StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{0x7F}), ChunkFormats.DATA_START + 5);
        }
        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(4, recovered.stat(TEST_NS, id).localEndOffset());
        }
    }

    @Test
    void tornLedgerEntryDiscarded() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("aaaa".getBytes()));
        store.append(TEST_NS, id, 1, 4, 0, ByteBuffer.wrap("bbbb".getBytes()));
        // torn ledger: last entry half-written
        long ledgerSize = Files.size(ledgerPath());
        try (FileChannel ch = FileChannel.open(ledgerPath(), StandardOpenOption.WRITE)) {
            ch.truncate(ledgerSize - 7);
        }
        try (ChunkStore recovered = new ChunkStore(dir)) {
            // second append's ledger entry gone -> data beyond first boundary is untrusted
            assertEquals(4, recovered.stat(TEST_NS, id).localEndOffset());
            assertEquals(1, recovered.readLedger(TEST_NS, id, 0).size());
        }
    }

    @Test
    void garbageBeyondLedgerEndCutOff() throws Exception {
        // covers crash-after-footer-write-before-sidecar-update: bytes beyond the ledger end
        // exist in the data file but are not covered by any ledger entry
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("aaaa".getBytes()));
        try (FileChannel ch = FileChannel.open(dataPath(), StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ch.write(ByteBuffer.wrap("garbage-footer-bytes".getBytes()));
        }
        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(4, recovered.stat(TEST_NS, id).localEndOffset());
            assertEquals(ChunkFormats.DATA_START + 4, Files.size(dataPath()));
            // chunk is still open and sealable — the client retries the seal
            assertEquals(4, recovered.seal(TEST_NS, id, 1, 4, null).finalLength());
        }
    }

    @Test
    void sealedChunkSurvivesRestartAndStragglerLedgerRemoved() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("payload".getBytes()));
        store.seal(TEST_NS, id, 1, 7, null);
        // simulate crash between sidecar update and ledger delete: recreate a stale ledger file
        Files.write(ledgerPath(), new ChunkFormats.LedgerEntry(7, 0, 1).encode());

        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(TEST_NS, id);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(7, stat.sealedLength());
            assertArrayEquals("payload".getBytes(), recovered.read(TEST_NS, id, 0, 100).bytes());
            assertFalse(Files.exists(ledgerPath()), "stale ledger must be removed");
        }
    }

    @Test
    void sealedChunkWithRetainedLedgerAndOpenSidecarRecoversAsSealed() throws Exception {
        ChunkStore store = new ChunkStore(dir); // non-fsync: the ledger is retained until reclaim
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("payload".getBytes()));
        store.seal(TEST_NS, id, 1, 7, null);
        assertTrue(Files.exists(ledgerPath()), "non-fsync seal retains the ledger as the recovery net");

        // Durability-v2 (Lever 1) no longer writes a SEALED sidecar: in the pre-reclaim window the
        // sidecar still reads OPEN while the durable SEALED signal is the trailer. Recovery must treat
        // the trailer as authoritative. The retained ledger covers exactly [0, dataLength), which
        // distinguishes a real sealed chunk from an OPEN chunk whose payload merely looks like a footer
        // (whose ledger would cover the whole appended payload, strictly past dataLength).
        Files.write(metaPath(), new ChunkFormats.Sidecar(1, -1, 7, ChunkState.OPEN).encode());

        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(TEST_NS, id);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(7, stat.sealedLength());
            assertArrayEquals("payload".getBytes(), recovered.read(TEST_NS, id, 0, 100).bytes());
        }
    }

    @Test
    void fsyncSealLeavesNoSidecar() throws Exception {
        try (ChunkStore store = new ChunkStore(dir, true)) { // sealFsync=true: ledger + sidecar dropped at seal
            open(store);
            store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("payload".getBytes()));
            store.seal(TEST_NS, id, 1, 7, null);
            assertFalse(Files.exists(metaPath()), "fsync-sealed chunk carries no .meta sidecar");
            assertFalse(Files.exists(ledgerPath()), "fsync-sealed chunk carries no retained ledger");
        }
        assertFalse(Files.exists(metaPath()), "clean close must not recreate a sealed .meta sidecar");
        assertFalse(Files.exists(ledgerPath()), "clean close must not recreate a sealed ledger");
        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(TEST_NS, id);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(7, stat.sealedLength());
            assertArrayEquals("payload".getBytes(), recovered.read(TEST_NS, id, 0, 100).bytes());
        }
    }

    @Test
    void reclaimedSealedChunkLeavesNoSidecarOrLedger() throws Exception {
        ChunkStore store = new ChunkStore(dir); // non-fsync: ledger retained until reclaim
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("payload".getBytes()));
        store.seal(TEST_NS, id, 1, 7, null);
        store.reclaimSealedLedgersOnce();
        assertFalse(Files.exists(metaPath()), "reclaimed sealed chunk carries no .meta sidecar");
        assertFalse(Files.exists(ledgerPath()), "reclaim drops the retained ledger");
        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(TEST_NS, id);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(7, stat.sealedLength());
            assertArrayEquals("payload".getBytes(), recovered.read(TEST_NS, id, 0, 100).bytes());
        }
    }

    @Test
    void closeMustNotPromotePendingSealPastRetainedOpenRecoveryNet() throws Exception {
        byte[] payload = "payload".getBytes();
        ChunkStore store = new ChunkStore(dir); // non-fsync: seal leaves trailer/page-cache durability pending
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap(payload));
        store.seal(TEST_NS, id, 1, payload.length, null);
        assertTrue(Files.exists(ledgerPath()), "retained ledger is the pre-reclaim recovery net");
        store.close();

        // Crash image: clean close happened, then power loss lost the still-unforced footer/trailer.
        // The retained OPEN sidecar + ledger must remain the recovery net.
        try (FileChannel data = FileChannel.open(dataPath(), StandardOpenOption.WRITE)) {
            data.truncate(ChunkFormats.DATA_START + payload.length);
        }

        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(ChunkState.OPEN, recovered.stat(TEST_NS, id).state());
            assertArrayEquals(payload, recovered.read(TEST_NS, id, 0, payload.length).bytes());
        }
    }

    @Test
    void closeDoesNotRecreateSidecarForReclaimedSealedChunk() throws Exception {
        try (ChunkStore store = new ChunkStore(dir)) {
            open(store);
            store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("payload".getBytes()));
            store.seal(TEST_NS, id, 1, 7, null);
            store.reclaimSealedLedgersOnce();
            assertFalse(Files.exists(metaPath()), "reclaimed sealed chunk carries no .meta sidecar");
            assertFalse(Files.exists(ledgerPath()), "reclaimed sealed chunk carries no retained ledger");
        }
        assertFalse(Files.exists(metaPath()), "clean close must not recreate a reclaimed sealed .meta sidecar");
        assertFalse(Files.exists(ledgerPath()), "clean close must not recreate a reclaimed sealed ledger");

        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(TEST_NS, id);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(7, stat.sealedLength());
            assertArrayEquals("payload".getBytes(), recovered.read(TEST_NS, id, 0, 100).bytes());
        }
    }

    @Test
    void reclaimCrashAfterLedgerDurableBeforeMetaUnlinkLeavesShortSealRecoverable() throws Exception {
        byte[] fullPayload = "payload-tail".getBytes();
        byte[] sealedPayload = "payload".getBytes();
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap(fullPayload));
        store.seal(TEST_NS, id, 1, sealedPayload.length, null);

        FailureInjector.arm("format.reclaim.afterLedgerDurableBeforeMetaDelete", p -> {
            throw new RuntimeException("stop after ledger unlink is durable");
        });
        try {
            store.reclaimSealedLedgersOnce();
        } finally {
            FailureInjector.reset();
        }

        assertFalse(Files.exists(ledgerPath()), "ledger unlink must be durable before sidecar unlink starts");
        assertTrue(Files.exists(metaPath()), "OPEN sidecar must remain if reclaim stops after ledger unlink");
        store.close();

        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(TEST_NS, id);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(sealedPayload.length, stat.sealedLength());
            assertArrayEquals(sealedPayload, recovered.read(TEST_NS, id, 0, sealedPayload.length).bytes());
        }
    }

    @Test
    void reclaimedSealedChunkWithRottedTrailerMagicIsQuarantined() throws Exception {
        byte[] repairImage;
        int dataCrc;
        try (ChunkStore store = new ChunkStore(dir)) {
            open(store);
            store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("sealed-data".getBytes()));
            dataCrc = store.seal(TEST_NS, id, 1, 11, null).dataCrc();
            store.reclaimSealedLedgersOnce();
            repairImage = store.fetch(TEST_NS, id, 0, Integer.MAX_VALUE).bytes();
        }

        try (FileChannel ch = FileChannel.open(dataPath(), StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{0, 0, 0, 0}), Files.size(dataPath()) - Integer.BYTES);
        }

        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(0, recovered.describeChunks().size(),
                    "rotted trailer-magic chunk must not be recovered as healthy");
            assertFalse(Files.exists(dataPath()), "live chunk name must be free for repair import");
            try (Stream<Path> files = Files.list(dataPath().getParent())) {
                assertEquals(1, files.filter(p -> p.getFileName().toString().contains(".quarantine-")).count(),
                        "quarantine must preserve the rotted sealed evidence");
            }
            recovered.importSealed(TEST_NS, id, repairImage, 11, dataCrc);
            assertArrayEquals("sealed-data".getBytes(), recovered.read(TEST_NS, id, 0, 100).bytes());
        }
    }

    @Test
    void corruptFooterIsDetectedAtRecoveryAndChunkQuarantined() throws Exception {
        byte[] repairImage;
        int dataCrc;
        try (ChunkStore store = new ChunkStore(dir)) {
            open(store);
            store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("sealed-data".getBytes()));
            dataCrc = store.seal(TEST_NS, id, 1, 11, null).dataCrc();
            store.reclaimSealedLedgersOnce();
            repairImage = store.fetch(TEST_NS, id, 0, Integer.MAX_VALUE).bytes();
        }
        // bit-rot inside the footer section area (between data end and the 64B trailer)
        try (FileChannel ch = FileChannel.open(dataPath(), StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{0x7F}), ChunkFormats.DATA_START + 11 + 4);
        }
        try (ChunkStore recovered = new ChunkStore(dir)) {
            // a sealed chunk whose footer fails its CRC must be quarantined, not served
            assertEquals(0, recovered.describeChunks().size(),
                    "corrupt-footer chunk must not be recovered as healthy");
            assertFalse(Files.exists(dataPath()), "live chunk name must be free for repair import");
            Path shardDir = dataPath().getParent();
            try (Stream<Path> files = Files.list(shardDir)) {
                // After reclaim, the trailer is the only SEALED signal; a corrupt sealed-looking trailer
                // must quarantine the chunk evidence instead of being treated as an unacked remnant.
                assertEquals(1, files.filter(p -> p.getFileName().toString().contains(".quarantine-")).count(),
                        "quarantine must preserve the corrupt evidence under a non-live name");
            }
            recovered.importSealed(TEST_NS, id, repairImage, 11, dataCrc);
            assertArrayEquals("sealed-data".getBytes(), recovered.read(TEST_NS, id, 0, 100).bytes());
        }
    }

    @Test
    void malformedSidecarWithoutLedgerIsQuarantinedInsteadOfTruncatedToOpen() throws Exception {
        byte[] payload = "sealed-data".getBytes();
        try (ChunkStore store = new ChunkStore(dir, true)) {
            store.open(TEST_NS, id, true, 1, 1718000000000L);
            store.appendAsync(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap(payload)).get(5, TimeUnit.SECONDS);
            store.seal(TEST_NS, id, 1, payload.length, null);
        }
        assertFalse(Files.exists(ledgerPath()), "sealed fsync chunk should not retain a ledger");

        // Corrupt both durability witnesses that would otherwise classify the chunk as SEALED:
        // footer rot makes the trailer probe fail; sidecar rot must not fall back to destructive OPEN replay
        // when there is no ledger to prove an acked OPEN prefix.
        try (FileChannel ch = FileChannel.open(dataPath(), StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{0x7F}), ChunkFormats.DATA_START + payload.length + 4);
        }
        Files.write(metaPath(), new byte[ChunkFormats.SIDECAR_SIZE]);
        long sealedFileSize = Files.size(dataPath());

        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(0, recovered.describeChunks().size(),
                    "double-rotted sealed chunk must be quarantined, not reconstructed as OPEN");
            assertFalse(Files.exists(dataPath()), "live chunk name must be free after quarantine");
            Path shardDir = dataPath().getParent();
            try (Stream<Path> files = Files.list(shardDir)) {
                List<Path> quarantined = files.filter(p -> p.getFileName().toString().contains(".quarantine-"))
                        .toList();
                assertEquals(2, quarantined.size(), "quarantine must preserve chunk+meta evidence");
                Path quarantinedChunk = quarantined.stream()
                        .filter(p -> p.getFileName().toString().contains(".chunk.quarantine-"))
                        .findFirst()
                        .orElseThrow();
                assertEquals(sealedFileSize, Files.size(quarantinedChunk),
                        "quarantine must not truncate the sealed payload before preserving it");
            }
        }
    }

    @Test
    void chunkWithoutSidecarIsRemovedAsUnacked() throws Exception {
        ChunkStore store = new ChunkStore(dir);
        open(store);
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("x".getBytes()));
        Files.delete(metaPath());
        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(0, recovered.describeChunks().size());
            assertFalse(Files.exists(dataPath()));
        }
    }

    @Test
    void ackedFsyncChunkSurvivesMissingSidecarNameAfterCrash() throws Exception {
        byte[] payload = "acked-fsync".getBytes();
        ChunkStore store = new ChunkStore(dir);
        store.open(TEST_NS, id, true, 1, 1718000000000L);
        store.appendAsync(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap(payload)).get(5, TimeUnit.SECONDS);
        store.close();

        Files.delete(metaPath());

        assertDoesNotThrow(() -> {
            try (ChunkStore recovered = new ChunkStore(dir)) {
                assertEquals(ChunkState.OPEN, recovered.stat(TEST_NS, id).state());
                assertArrayEquals(payload, recovered.read(TEST_NS, id, 0, 100).bytes());
            }
        });
    }

    @Test
    void ackedFsyncChunkSurvivesTornSidecarAfterCrash() throws Exception {
        byte[] payload = "acked-fsync".getBytes();
        ChunkStore store = new ChunkStore(dir);
        store.open(TEST_NS, id, true, 1, 1718000000000L);
        store.appendAsync(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap(payload)).get(5, TimeUnit.SECONDS);
        store.close();

        Files.write(metaPath(), new byte[ChunkFormats.SIDECAR_SIZE]);

        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(ChunkState.OPEN, recovered.stat(TEST_NS, id).state());
            assertArrayEquals(payload, recovered.read(TEST_NS, id, 0, 100).bytes());

            ScpException stale = assertThrows(ScpException.class,
                    () -> recovered.append(TEST_NS, id, 1, payload.length, payload.length,
                            ByteBuffer.wrap("stale".getBytes())));
            assertEquals(ErrorCode.FENCED_EPOCH, stale.code());
        }
    }

    @Test
    void missingSidecarOpenChunkWithFooterShapedPayloadRecoversFromLedger() throws Exception {
        byte[] payload = footerShapedPayload("live".getBytes());
        ChunkStore store = new ChunkStore(dir);
        store.open(TEST_NS, id, true, 1, 1718000000000L);
        store.appendAsync(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap(payload)).get(5, TimeUnit.SECONDS);
        store.close();

        Files.delete(metaPath());

        try (ChunkStore recovered = new ChunkStore(dir)) {
            var stat = recovered.stat(TEST_NS, id);
            assertEquals(ChunkState.OPEN, stat.state());
            assertEquals(payload.length, stat.localEndOffset());
            assertArrayEquals(payload, recovered.read(TEST_NS, id, 0, payload.length).bytes());
            assertTrue(Files.exists(ledgerPath()), "ledger must not be deleted by false sealed recovery");
        }
    }

    @Test
    void openChunkWithFooterShapedPayloadAndTornLedgerTailRecoversAsOpen() throws Exception {
        // Reviewer repro (PR #37): an OPEN chunk whose payload suffix is footer-shaped AND whose last
        // ledger entry is torn. A backward scan over the torn tail lands on the PREVIOUS intact entry,
        // which here ends exactly at the fake trailer's dataLength — so the trailer disambiguator would
        // wrongly install SEALED and delete the sidecar/ledger. Recovery MUST keep the chunk OPEN.
        byte[] live = "live".getBytes();
        byte[] payload = footerShapedPayload(live);   // live + footer + trailer; fake trailer.dataLength == live.length
        byte[] tail = Arrays.copyOfRange(payload, live.length, payload.length);

        ChunkStore store = new ChunkStore(dir);
        store.open(TEST_NS, id, true, 1, 1718000000000L);
        // two appends split exactly at the fake dataLength, so ledger entry #1 ends at live.length (== dataLength)
        store.appendAsync(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap(live)).get(5, TimeUnit.SECONDS);
        store.appendAsync(TEST_NS, id, 1, live.length, 0, ByteBuffer.wrap(tail)).get(5, TimeUnit.SECONDS);
        store.close();

        Files.delete(metaPath());
        // tear the second (last) ledger entry so the backward scan falls back to entry #1 (ends at dataLength)
        try (FileChannel j = FileChannel.open(ledgerPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            j.read(one, ChunkFormats.LEDGER_ENTRY_SIZE);   // first byte of entry #2
            j.write(ByteBuffer.wrap(new byte[]{(byte) (one.get(0) ^ 0xFF)}), ChunkFormats.LEDGER_ENTRY_SIZE);
        }

        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(ChunkState.OPEN, recovered.stat(TEST_NS, id).state(),
                    "footer-shaped payload + torn ledger tail must not be misread as SEALED");
            assertTrue(Files.exists(ledgerPath()), "the OPEN chunk's ledger must not be deleted by a false sealed recovery");
        }
    }

    @Test
    void missingSidecarFsyncOpenChunkRequiresFreshFenceBeforeAppend() throws Exception {
        byte[] payload = "acked-fsync".getBytes();
        ChunkStore store = new ChunkStore(dir);
        store.open(TEST_NS, id, true, 1, 1718000000000L);
        store.appendAsync(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap(payload)).get(5, TimeUnit.SECONDS);
        store.close();

        Files.delete(metaPath());

        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(ChunkState.OPEN, recovered.stat(TEST_NS, id).state());
            ScpException stale = assertThrows(ScpException.class,
                    () -> recovered.append(TEST_NS, id, 1, payload.length, payload.length, ByteBuffer.wrap("stale".getBytes())));
            assertEquals(ErrorCode.FENCED_EPOCH, stale.code());

            assertEquals(payload.length, recovered.fence(TEST_NS, id, 2).localEndOffset());
            assertEquals(payload.length + 5,
                    recovered.append(TEST_NS, id, 2, payload.length, payload.length, ByteBuffer.wrap("fresh".getBytes()))
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
        store.append(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("a".getBytes()));
        store.fence(TEST_NS, id, 9); // persists sidecar
        try (ChunkStore recovered = new ChunkStore(dir)) {
            assertEquals(ErrorCode.FENCED_EPOCH,
                    Assertions.assertThrows(ScpException.class,
                            () -> recovered.append(TEST_NS, id, 8, 1, 0, ByteBuffer.wrap("b".getBytes()))).code());
            assertEquals(2, recovered.append(TEST_NS, id, 9, 1, 0, ByteBuffer.wrap("b".getBytes())).endOffset());
        }
    }
}
