package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The whole-chunk {@code dataCrc} and per-{@link ChunkFormats#CRC_RANGE_SIZE} range CRCs written at
 * seal must equal a from-scratch CRC32C over the chunk's logical bytes, regardless of how the data
 * was split across appends. This pins the property so the incremental (compute-at-append) CRC stays
 * bit-identical to the historical seal-time full re-scan — across appends that straddle 4 MiB range
 * boundaries, a single append larger than a range, an exact range multiple, a tiny single range, and
 * an open chunk rebuilt after a crash/reopen.
 */
class ChunkStoreCrcTest {

    @TempDir
    Path dir;

    private final ChunkId id = new ChunkId(FileId.of(1), 0);

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

    /** Appends {@code data} as the given contiguous pieces, plus any remainder as a final append. */
    private void appendInPieces(ChunkStore store, byte[] data, int[] sizes) throws IOException {
        int off = 0;
        for (int n : sizes) {
            store.append(id, 1, off, off, ByteBuffer.wrap(data, off, n));
            off += n;
        }
        if (off < data.length) {
            store.append(id, 1, off, off, ByteBuffer.wrap(data, off, data.length - off));
        }
    }

    /** Reads the whole sealed chunk back through the verified read path (checks the range CRCs). */
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

    private void assertSealCrcMatchesScan(byte[] data, int[] pieces) throws IOException {
        try (ChunkStore store = new ChunkStore(dir)) {
            store.open(StrataNamespace.of("test"), id, false, 1, 1718000000000L);
            appendInPieces(store, data, pieces);
            ChunkStore.SealResult sealed = store.seal(id, 1, data.length, null);
            assertEquals(wholeCrc(data), sealed.dataCrc(), "sealed dataCrc must equal CRC32C over all bytes");
            assertArrayEquals(data, readAllVerified(store, data.length), "read-back must verify against range CRCs");
        }
    }

    @Test
    void crcMatchesScanForAppendsStraddlingRangeBoundaries() throws IOException {
        byte[] data = randomBytes(1, 10 * 1024 * 1024 + 12_345); // > 2 full ranges
        int[] pieces = {3_000_000, 1_500_000, 4_200_000, 777, 900_000}; // odd sizes that cross boundaries
        assertSealCrcMatchesScan(data, pieces);
    }

    @Test
    void crcMatchesScanForExactRangeMultiple() throws IOException {
        byte[] data = randomBytes(2, 2 * ChunkFormats.CRC_RANGE_SIZE); // exactly 2 ranges, no partial tail
        int[] pieces = {ChunkFormats.CRC_RANGE_SIZE, ChunkFormats.CRC_RANGE_SIZE};
        assertSealCrcMatchesScan(data, pieces);
    }

    @Test
    void crcMatchesScanForSingleAppendLargerThanRange() throws IOException {
        byte[] data = randomBytes(3, ChunkFormats.CRC_RANGE_SIZE + 1_000_000); // one append crosses a boundary
        int[] pieces = {data.length};
        assertSealCrcMatchesScan(data, pieces);
    }

    @Test
    void crcMatchesScanForTinySingleRange() throws IOException {
        byte[] data = randomBytes(4, 4096);
        int[] pieces = {4096};
        assertSealCrcMatchesScan(data, pieces);
    }

    @Test
    void crcSurvivesCrashRecoveryReopen() throws IOException {
        byte[] part1 = randomBytes(5, 5 * 1024 * 1024 + 321); // > 1 range; leaves a partial range mid-flight
        byte[] part2 = randomBytes(6, 3 * 1024 * 1024 + 99);
        byte[] all = new byte[part1.length + part2.length];
        System.arraycopy(part1, 0, all, 0, part1.length);
        System.arraycopy(part2, 0, all, part1.length, part2.length);

        ChunkStore store = new ChunkStore(dir);
        store.open(StrataNamespace.of("test"), id, false, 1, 1718000000000L);
        appendInPieces(store, part1, new int[]{2_000_000, 1_234_567});
        store.close(); // reopen before sealing — the in-memory running CRC state is gone

        try (ChunkStore recovered = new ChunkStore(dir)) {
            recovered.append(id, 1, part1.length, part1.length, ByteBuffer.wrap(part2));
            ChunkStore.SealResult sealed = recovered.seal(id, 1, all.length, null);
            assertEquals(wholeCrc(all), sealed.dataCrc(),
                    "dataCrc after recovery + further appends must equal CRC32C over all bytes");
            assertArrayEquals(all, readAllVerified(recovered, all.length));
        }
    }

    @Test
    void failedLedgerAppendDoesNotFoldIntoRunningCrc() throws Exception {
        byte[] committed = "AAAA".getBytes(StandardCharsets.UTF_8);
        try (ChunkStore store = new ChunkStore(dir)) {
            store.open(StrataNamespace.of("test"), id, false, 1, 1718000000000L);
            store.append(id, 1, 0, 0, ByteBuffer.wrap(committed)); // written, logged, folded

            // Make the next ledger append fail (closed channel), then attempt an append: its data write
            // lands but the ledger entry is rejected, so the append is NOT committed. The running CRC
            // must not have advanced — otherwise this seal (or a same-offset retry on the live handle)
            // would emit CRCs that disagree with the on-disk bytes / read-time range verification.
            closeLedgerChannel(store, id);
            byte[] rejected = "BBBB".getBytes(StandardCharsets.UTF_8);
            assertThrows(IOException.class,
                    () -> store.append(id, 1, committed.length, committed.length, ByteBuffer.wrap(rejected)));

            ChunkStore.SealResult sealed = store.seal(id, 1, committed.length, null);
            assertEquals(wholeCrc(committed), sealed.dataCrc(),
                    "a failed (uncommitted) ledger append must not fold its bytes into the running CRC");
            assertArrayEquals(committed, readAllVerified(store, committed.length));
        }
    }

    /** Reflectively closes the chunk's integrity-ledger channel so the next append's ledger write fails. */
    @SuppressWarnings("unchecked")
    private void closeLedgerChannel(ChunkStore store, ChunkId chunkId) throws Exception {
        Field chunksF = ChunkStore.class.getDeclaredField("chunks");
        chunksF.setAccessible(true);
        Object handle = ((Map<ChunkId, ?>) chunksF.get(store)).get(chunkId);
        Field ledgerF = handle.getClass().getDeclaredField("ledger");
        ledgerF.setAccessible(true);
        Object ledger = ledgerF.get(handle);
        Field channelF = ledger.getClass().getDeclaredField("channel");
        channelF.setAccessible(true);
        ((FileChannel) channelF.get(ledger)).close();
    }
}
