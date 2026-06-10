package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChunkStoreTest {

    @TempDir
    Path dir;

    private final ChunkId id = new ChunkId(FileId.random(), 0);

    private static ByteBuffer bytes(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private ChunkStore newStore() throws IOException {
        return new ChunkStore(dir);
    }

    private void open(ChunkStore store, ChunkId id, int epoch) throws IOException {
        store.open(id, (byte) 0, (byte) 0, ChunkStore.ACK_ON_REPLICATE, epoch, 1718000000000L);
    }

    @Test
    void appendReadSealLifecycle() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            assertEquals(5, store.append(id, 1, 0, 0, bytes("hello")).endOffset());
            assertEquals(11, store.append(id, 1, 5, 5, bytes(" world")).endOffset());

            var r = store.read(id, 0, 1024);
            assertArrayEquals("hello world".getBytes(), r.bytes());
            assertEquals(11, r.localEndOffset());
            assertEquals(5, r.lastKnownDO()); // piggybacked DO from second append

            var sealed = store.seal(id, 1, 11, null);
            assertEquals(11, sealed.finalLength());
            // idempotent re-seal returns same result
            assertEquals(sealed, store.seal(id, 1, 11, null));

            var r2 = store.read(id, 6, 1024);
            assertArrayEquals("world".getBytes(), r2.bytes());

            var stat = store.stat(id);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(11, stat.sealedLength());

            // append after seal rejected
            assertEquals(ErrorCode.CHUNK_SEALED,
                    assertThrows(ScpException.class, () -> store.append(id, 1, 11, 0, bytes("x"))).code());
        }
    }

    @Test
    void epochFencingRules() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 5);
            store.append(id, 5, 0, 0, bytes("aa"));

            // lower epoch rejected
            var e = assertThrows(ScpException.class, () -> store.append(id, 4, 2, 0, bytes("bb")));
            assertEquals(ErrorCode.FENCED_EPOCH, e.code());
            assertEquals(5, e.detail());

            // fence at 7: epoch 5,6 appends rejected; 7 accepted
            var f = store.fence(id, 7);
            assertEquals(7, f.persistedFenceEpoch());
            assertEquals(2, f.localEndOffset());
            assertEquals(ErrorCode.FENCED_EPOCH,
                    assertThrows(ScpException.class, () -> store.append(id, 5, 2, 0, bytes("bb"))).code());
            assertEquals(ErrorCode.FENCED_EPOCH,
                    assertThrows(ScpException.class, () -> store.append(id, 6, 2, 0, bytes("bb"))).code());
            assertEquals(4, store.append(id, 7, 2, 0, bytes("cc")).endOffset());

            // fence is monotonic: lower fence is a no-op
            assertEquals(7, store.fence(id, 3).persistedFenceEpoch());

            // seal with fenced epoch rejected, with current epoch ok
            assertEquals(ErrorCode.FENCED_EPOCH,
                    assertThrows(ScpException.class, () -> store.seal(id, 5, 4, null)).code());
            assertEquals(4, store.seal(id, 7, 4, null).finalLength());
        }
    }

    @Test
    void offsetGapRejectedWithExpectedDetail() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("abc"));
            var e = assertThrows(ScpException.class, () -> store.append(id, 1, 5, 0, bytes("d")));
            assertEquals(ErrorCode.OFFSET_GAP, e.code());
            assertEquals(3, e.detail());
        }
    }

    @Test
    void sealTruncatesUnackedTail() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("aaaa"));
            store.append(id, 1, 4, 0, bytes("bbbb")); // never quorum-acked, will be cut
            var sealed = store.seal(id, 1, 4, null);
            assertEquals(4, sealed.finalLength());
            var r = store.read(id, 0, 100);
            assertArrayEquals("aaaa".getBytes(), r.bytes());
        }
    }

    @Test
    void emptyAppendIsDoBeacon() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("data"));
            store.append(id, 1, 4, 4, ByteBuffer.allocate(0)); // beacon advances DO only
            var stat = store.stat(id);
            assertEquals(4, stat.localEndOffset());
            assertEquals(4, stat.lastKnownDO());
            assertEquals(1, store.readLedger(id, 0).size()); // beacon adds no ledger entry
        }
    }

    @Test
    void doNeverExceedsLocalEnd() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            // a DO claim can only cover previously acked data — it is clamped to the
            // pre-append end (0 here), never to the bytes carried by this very append
            store.append(id, 1, 0, 999, bytes("ab"));
            assertEquals(0, store.stat(id).lastKnownDO());
            // next append may legitimately claim the previous bytes as durable
            store.append(id, 1, 2, 2, bytes("cd"));
            assertEquals(2, store.stat(id).lastKnownDO());
        }
    }

    @Test
    void fetchAndImportProduceByteIdenticalReplica(@TempDir Path otherDir) throws Exception {
        long sealedLen;
        int crc;
        byte[] fileBytes;
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            byte[] big = new byte[100_000];
            for (int i = 0; i < big.length; i++) big[i] = (byte) (i % 251);
            store.append(id, 1, 0, 0, ByteBuffer.wrap(big));
            var sealed = store.seal(id, 1, big.length, null);
            sealedLen = sealed.finalLength();
            crc = sealed.dataCrc();
            var fetched = store.fetch(id, 0, Integer.MAX_VALUE);
            assertEquals(fetched.fileLength(), fetched.bytes().length);
            fileBytes = fetched.bytes();
        }
        try (ChunkStore other = new ChunkStore(otherDir)) {
            other.importSealed(id, fileBytes, sealedLen, crc);
            var fetched2 = other.fetch(id, 0, Integer.MAX_VALUE);
            assertArrayEquals(fileBytes, fetched2.bytes()); // invariant §14.6: byte-identical replicas
            var r = other.read(id, 0, 10);
            assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, r.bytes());
        }
    }

    @Test
    void importRejectsCorruptBytes(@TempDir Path otherDir) throws Exception {
        byte[] fileBytes;
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("payload-payload"));
            store.seal(id, 1, 15, null);
            fileBytes = store.fetch(id, 0, Integer.MAX_VALUE).bytes();
        }
        fileBytes[ChunkFormats.HEADER_SIZE + 3] ^= 1; // corrupt data region
        try (ChunkStore other = new ChunkStore(otherDir)) {
            assertEquals(ErrorCode.CRC_MISMATCH,
                    assertThrows(ScpException.class, () -> other.importSealed(id, fileBytes, 15, 0)).code());
        }
    }

    @Test
    void deleteRemovesEverything() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("x"));
            assertEquals(ErrorCode.OK, store.delete(id));
            assertEquals(ErrorCode.CHUNK_NOT_FOUND, store.delete(id));
            assertEquals(ErrorCode.CHUNK_NOT_FOUND,
                    assertThrows(ScpException.class, () -> store.read(id, 0, 1)).code());
            assertEquals(0, store.inventory().size());
        }
    }
}
