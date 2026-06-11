package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import static io.strata.format.ChunkFormats.DATA_START;
import static io.strata.format.ChunkFormats.HEADER_SIZE;
import static io.strata.format.ChunkFormats.TRAILER_SIZE;

/**
 * Node-local chunk engine (tech design §5, §11): epoch-fenced appends, integrity-ledger crash
 * recovery, seal with node-computed CRC_RANGES/STATS, sealed-chunk import for repair.
 *
 * All mutations are synchronized per chunk handle. The data region is raw logical bytes —
 * this engine never parses payload content (invariant §14.10), not even during recovery.
 */
public final class ChunkStore implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);

    public static final byte ACK_ON_REPLICATE = 0;
    public static final byte ACK_ON_FSYNC = 1;

    private final Path dir;
    private final Map<ChunkId, Handle> chunks = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong forceCount =
            new java.util.concurrent.atomic.AtomicLong();

    public ChunkStore(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
        recoverAll();
    }

    /** Number of group-commit force() calls — observability + coalescing tests. */
    public long fsyncForceCount() {
        return forceCount.get();
    }

    /* ---------------- per-chunk state ---------------- */

    private final class Handle {
        final ChunkId id;
        final ChunkFormats.Header header;
        final Path dataPath;
        final Path metaPath;
        final Path ledgerPath;
        FileChannel data;
        IntegrityLedger ledger; // null once sealed
        GroupCommitter committer; // non-null only for OPEN ack-on-fsync chunks
        ChunkState state;
        long end;               // logical data length
        int writeEpoch;
        int fenceEpoch;
        long lastKnownDO;
        long sealedLength = -1;
        int dataCrc;

        Handle(ChunkId id, ChunkFormats.Header header) {
            this.id = id;
            this.header = header;
            String base = ChunkFormats.baseName(id);
            this.dataPath = dir.resolve(base + ".chunk");
            this.metaPath = dir.resolve(base + ".meta");
            this.ledgerPath = dir.resolve(base + ".j");
        }

        void persistSidecar() throws IOException {
            byte[] bytes = new ChunkFormats.Sidecar(writeEpoch, fenceEpoch, lastKnownDO, state).encode();
            try (FileChannel ch = FileChannel.open(metaPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                ch.write(ByteBuffer.wrap(bytes), 0);
                ch.force(true);
            }
        }

        void startCommitterIfFsync(java.util.concurrent.atomic.AtomicLong counter) {
            if (header.ackPolicy() == ACK_ON_FSYNC) {
                committer = new GroupCommitter(id.toString(), () -> {
                    // both must be durable before acking; either alone is safe for recovery
                    data.force(false);
                    ledger.force();
                }, counter);
            }
        }

        void stopCommitter() throws IOException {
            if (committer != null) {
                // drains with a final force; a flusher stuck in a hung force means we must NOT
                // proceed to truncate/close/delete these files — fail the operation instead
                if (!committer.closeAndConfirm()) {
                    throw new IOException("group-commit flusher stuck for " + id
                            + " — refusing to mutate chunk files");
                }
                committer = null;
            }
        }
    }

    private Handle lookup(ChunkId id) {
        Handle h = chunks.get(id);
        if (h == null) throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, id.toString());
        return h;
    }

    private static void checkEpoch(Handle h, int epoch) {
        int floor = Math.max(h.fenceEpoch, h.writeEpoch);
        if (epoch < floor) {
            throw new ScpException(ErrorCode.FENCED_EPOCH, "epoch " + epoch + " < " + floor, floor);
        }
    }

    /* ---------------- operations ---------------- */

    public void open(ChunkId id, byte fileKind, byte mediaClass, byte ackPolicy, int writeEpoch,
                     long createdAtMs) throws IOException {
        if (chunks.containsKey(id)) throw new ScpException(ErrorCode.CHUNK_ALREADY_EXISTS, id.toString());
        ChunkFormats.Header header = new ChunkFormats.Header(id, fileKind, mediaClass, ackPolicy,
                writeEpoch, createdAtMs, 0, 0, 0);
        Handle h = new Handle(id, header);
        if (Files.exists(h.dataPath)) throw new ScpException(ErrorCode.CHUNK_ALREADY_EXISTS, id.toString());
        h.data = FileChannel.open(h.dataPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        h.data.write(ByteBuffer.wrap(header.encode()), 0);
        h.data.force(true);
        h.ledger = IntegrityLedger.create(h.ledgerPath);
        h.state = ChunkState.OPEN;
        h.end = 0;
        h.writeEpoch = writeEpoch;
        h.fenceEpoch = -1;
        h.lastKnownDO = 0;
        h.persistSidecar();
        h.startCommitterIfFsync(forceCount);
        chunks.put(id, h);
    }

    public record AppendResult(long endOffset) {}

    /**
     * Validates and writes synchronously (per-chunk ordering and contiguity preserved); the
     * returned future completes when the append is durable per the chunk's ack policy —
     * immediately for ack-on-replicate, after a covering group-commit force for ack-on-fsync.
     */
    public java.util.concurrent.CompletableFuture<AppendResult> appendAsync(
            ChunkId id, int epoch, long baseOffset, long durableOffset, ByteBuffer payload) throws IOException {
        Handle h = lookup(id);
        long newEnd;
        GroupCommitter committer;
        synchronized (h) {
            // fence check dominates the state check: a deposed writer must learn FENCED_EPOCH
            // (permanent death), never CHUNK_SEALED (which reads as "roll and continue")
            checkEpoch(h, epoch);
            if (h.state != ChunkState.OPEN) throw new ScpException(ErrorCode.CHUNK_SEALED, id.toString());
            h.writeEpoch = Math.max(h.writeEpoch, epoch);
            if (baseOffset != h.end) {
                throw new ScpException(ErrorCode.OFFSET_GAP, "expected " + h.end + " got " + baseOffset, h.end);
            }
            h.lastKnownDO = Math.max(h.lastKnownDO, Math.min(durableOffset, h.end));
            int len = payload.remaining();
            if (len == 0) {
                return java.util.concurrent.CompletableFuture.completedFuture(new AppendResult(h.end)); // DO beacon
            }
            int payloadCrc = Crc.of(payload.duplicate());
            h.data.write(payload.duplicate(), DATA_START + baseOffset);
            newEnd = baseOffset + len;
            h.ledger.append(new ChunkFormats.LedgerEntry(newEnd, payloadCrc, epoch));
            h.end = newEnd;
            committer = h.committer;
        }
        if (committer == null) {
            return java.util.concurrent.CompletableFuture.completedFuture(new AppendResult(newEnd));
        }
        long end = newEnd;
        return committer.awaitFlush(end).thenApply(v -> new AppendResult(end));
    }

    /** Synchronous convenience (tests, simple callers); production append path uses appendAsync. */
    public AppendResult append(ChunkId id, int epoch, long baseOffset, long durableOffset,
                               ByteBuffer payload) throws IOException {
        try {
            return appendAsync(id, epoch, baseOffset, durableOffset, payload).join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof ScpException se) throw se;
            throw new ScpException(ErrorCode.INTERNAL, String.valueOf(e.getCause()));
        }
    }

    public record ReadResult(byte[] bytes, long localEndOffset, long lastKnownDO) {}

    public ReadResult read(ChunkId id, long offset, int maxBytes) throws IOException {
        Handle h = lookup(id);
        synchronized (h) {
            long end = h.state == ChunkState.SEALED ? h.sealedLength : h.end;
            if (offset >= end) return new ReadResult(new byte[0], end, h.lastKnownDO);
            int n = (int) Math.min(maxBytes, end - offset);
            byte[] out = new byte[n];
            readFully(h.data, ByteBuffer.wrap(out), DATA_START + offset);
            return new ReadResult(out, end, h.lastKnownDO);
        }
    }

    public record FenceResult(int persistedFenceEpoch, long localEndOffset, long lastKnownDO, ChunkState state) {}

    public FenceResult fence(ChunkId id, int fenceEpoch) throws IOException {
        Handle h = lookup(id);
        synchronized (h) {
            if (fenceEpoch > h.fenceEpoch) {
                h.fenceEpoch = fenceEpoch;
                h.persistSidecar();
            }
            long end = h.state == ChunkState.SEALED ? h.sealedLength : h.end;
            return new FenceResult(h.fenceEpoch, end, h.lastKnownDO, h.state);
        }
    }

    public record StatResult(ChunkState state, long localEndOffset, long lastKnownDO, int writeEpoch,
                             int fenceEpoch, long sealedLength, int dataCrc) {}

    public StatResult stat(ChunkId id) {
        Handle h = lookup(id);
        synchronized (h) {
            long end = h.state == ChunkState.SEALED ? h.sealedLength : h.end;
            return new StatResult(h.state, end, h.lastKnownDO, h.writeEpoch, h.fenceEpoch,
                    h.sealedLength, h.dataCrc);
        }
    }

    public record SealResult(long finalLength, int dataCrc) {}

    /**
     * Seals at dataLength (must be <= current end; shorter means truncate the never-acked tail).
     * callerSections, if non-empty, is a pre-encoded section list: u32 count + section bytes.
     */
    public SealResult seal(ChunkId id, int epoch, long dataLength, ByteBuffer callerSections) throws IOException {
        Handle h = lookup(id);
        synchronized (h) {
            if (h.state == ChunkState.SEALED) {
                if (h.sealedLength == dataLength) return new SealResult(h.sealedLength, h.dataCrc); // idempotent
                throw new ScpException(ErrorCode.CHUNK_SEALED, "sealed at " + h.sealedLength, h.sealedLength);
            }
            checkEpoch(h, epoch);
            if (dataLength > h.end) {
                throw new ScpException(ErrorCode.INTERNAL, "seal beyond end: " + dataLength + " > " + h.end);
            }
            // stop the group committer BEFORE truncating: its flusher must not race the truncate,
            // and its final force drains any remaining waiters
            h.stopCommitter();
            if (dataLength < h.end) {
                h.data.truncate(DATA_START + dataLength);
                h.ledger.truncateTo(dataLength);
                h.end = dataLength;
            }

            int callerCount = 0;
            byte[] callerBytes = new byte[0];
            if (callerSections != null && callerSections.remaining() >= 4) {
                callerCount = callerSections.getInt();
                callerBytes = new byte[callerSections.remaining()];
                callerSections.get(callerBytes);
            }

            // node-computed sections: CRC_RANGES + STATS (deterministic — replicas stay byte-identical)
            CrcScan scan = scanDataCrcs(h, dataLength);
            ByteBuffer crcRanges = ByteBuffer.allocate(4 + 4 + scan.rangeCrcs.size() * 4);
            crcRanges.putInt(ChunkFormats.CRC_RANGE_SIZE).putInt(scan.rangeCrcs.size());
            for (int c : scan.rangeCrcs) crcRanges.putInt(c);
            byte[] stats = ByteBuffer.allocate(12).putLong(dataLength).putInt(h.ledger.entries().size()).array();

            int footerLen = callerBytes.length
                    + ChunkFormats.sectionSize(crcRanges.array())
                    + ChunkFormats.sectionSize(stats);
            ByteBuffer footer = ByteBuffer.allocate(footerLen);
            footer.put(callerBytes);
            ChunkFormats.writeSection(footer, ChunkFormats.SECTION_CRC_RANGES, crcRanges.array());
            ChunkFormats.writeSection(footer, ChunkFormats.SECTION_STATS, stats);
            footer.flip();

            long footerStart = DATA_START + dataLength;
            int footerCrc = Crc.of(footer.duplicate());
            ChunkFormats.Trailer trailer = new ChunkFormats.Trailer(dataLength, footerStart,
                    callerCount + 2, 0, footerCrc, scan.dataCrc);

            h.data.write(footer.duplicate(), footerStart);
            h.data.write(ByteBuffer.wrap(trailer.encode()), footerStart + footerLen);
            h.data.force(false);

            h.state = ChunkState.SEALED;
            h.sealedLength = dataLength;
            h.dataCrc = scan.dataCrc;
            h.lastKnownDO = dataLength;
            h.persistSidecar();
            h.ledger.close();
            Files.deleteIfExists(h.ledgerPath);
            h.ledger = null;
            return new SealResult(dataLength, scan.dataCrc);
        }
    }

    private record CrcScan(int dataCrc, List<Integer> rangeCrcs) {}

    private CrcScan scanDataCrcs(Handle h, long dataLength) throws IOException {
        CRC32C whole = new CRC32C();
        List<Integer> ranges = new ArrayList<>();
        byte[] buf = new byte[1 << 20];
        long pos = 0;
        CRC32C range = new CRC32C();
        long rangeRemaining = ChunkFormats.CRC_RANGE_SIZE;
        while (pos < dataLength) {
            int n = (int) Math.min(buf.length, Math.min(dataLength - pos, rangeRemaining));
            ByteBuffer bb = ByteBuffer.wrap(buf, 0, n);
            readFully(h.data, bb, DATA_START + pos);
            whole.update(buf, 0, n);
            range.update(buf, 0, n);
            pos += n;
            rangeRemaining -= n;
            if (rangeRemaining == 0 || pos == dataLength) {
                ranges.add((int) range.getValue());
                range.reset();
                rangeRemaining = ChunkFormats.CRC_RANGE_SIZE;
            }
        }
        return new CrcScan((int) whole.getValue(), ranges);
    }

    public List<ChunkFormats.LedgerEntry> readLedger(ChunkId id, long fromOffset) {
        Handle h = lookup(id);
        synchronized (h) {
            if (h.ledger == null) return List.of();
            return List.copyOf(h.ledger.entriesAfter(fromOffset));
        }
    }

    public record FetchResult(long fileLength, ChunkState state, byte[] bytes) {}

    /** Raw file bytes (header + data + footer) — repair/relocation transfer. Sealed chunks only. */
    public FetchResult fetch(ChunkId id, long offset, int maxBytes) throws IOException {
        Handle h = lookup(id);
        synchronized (h) {
            if (h.state != ChunkState.SEALED) {
                throw new ScpException(ErrorCode.INTERNAL, "fetch of non-sealed chunk " + id);
            }
            long fileLen = h.data.size();
            if (offset >= fileLen) return new FetchResult(fileLen, h.state, new byte[0]);
            int n = (int) Math.min(maxBytes, fileLen - offset);
            byte[] out = new byte[n];
            readFully(h.data, ByteBuffer.wrap(out), offset);
            return new FetchResult(fileLen, h.state, out);
        }
    }

    /** Imports a sealed chunk from raw file bytes (repair pull target side). Validates everything. */
    public void importSealed(ChunkId id, byte[] fileBytes, long expectedLength, int expectedCrc) throws IOException {
        if (chunks.containsKey(id)) throw new ScpException(ErrorCode.CHUNK_ALREADY_EXISTS, id.toString());
        if (fileBytes.length < HEADER_SIZE + TRAILER_SIZE) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "file too short");
        }
        // fileBytes is int-indexed, so anything that fit arrived < 2 GB — but the trailer's
        // claimed dataLength must still be range-checked before the (int) narrowing below
        long maxData = fileBytes.length - HEADER_SIZE - TRAILER_SIZE;
        byte[] headerBytes = new byte[HEADER_SIZE];
        System.arraycopy(fileBytes, 0, headerBytes, 0, HEADER_SIZE);
        ChunkFormats.Header header = ChunkFormats.Header.decode(headerBytes);
        if (!header.chunkId().equals(id)) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "chunk id mismatch: " + header.chunkId());
        }
        byte[] trailerBytes = new byte[TRAILER_SIZE];
        System.arraycopy(fileBytes, fileBytes.length - TRAILER_SIZE, trailerBytes, 0, TRAILER_SIZE);
        ChunkFormats.Trailer trailer = ChunkFormats.Trailer.decode(trailerBytes);
        if (expectedLength >= 0 && trailer.dataLength() != expectedLength) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "length mismatch: " + trailer.dataLength());
        }
        if (trailer.dataLength() < 0 || trailer.dataLength() > maxData) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "bad dataLength " + trailer.dataLength());
        }
        int dataCrc = Crc.of(fileBytes, HEADER_SIZE, (int) trailer.dataLength());
        if (dataCrc != trailer.dataCrc() || (expectedCrc != 0 && dataCrc != expectedCrc)) {
            throw new ScpException(ErrorCode.CRC_MISMATCH, "data crc mismatch on import");
        }

        Handle h = new Handle(id, header);
        Path tmp = h.dataPath.resolveSibling(h.dataPath.getFileName() + ".tmp");
        Files.write(tmp, fileBytes);
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
        Files.move(tmp, h.dataPath, StandardCopyOption.ATOMIC_MOVE);
        h.data = FileChannel.open(h.dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        h.state = ChunkState.SEALED;
        h.end = trailer.dataLength();
        h.sealedLength = trailer.dataLength();
        h.dataCrc = trailer.dataCrc();
        h.writeEpoch = header.createWriteEpoch();
        h.fenceEpoch = -1;
        h.lastKnownDO = trailer.dataLength();
        h.persistSidecar();
        chunks.put(id, h);
    }

    public ErrorCode delete(ChunkId id) {
        Handle h = chunks.remove(id);
        if (h == null) return ErrorCode.CHUNK_NOT_FOUND;
        synchronized (h) {
            try {
                h.stopCommitter();
                if (h.data != null) h.data.close();
                if (h.ledger != null) h.ledger.close();
                Files.deleteIfExists(h.dataPath);
                Files.deleteIfExists(h.metaPath);
                Files.deleteIfExists(h.ledgerPath);
            } catch (IOException e) {
                log.warn("delete {} failed", id, e);
                return ErrorCode.INTERNAL;
            }
        }
        return ErrorCode.OK;
    }

    public record InventoryItem(ChunkId chunkId, ChunkState state, long length, int crc) {}

    public List<InventoryItem> inventory() {
        List<InventoryItem> out = new ArrayList<>();
        for (Handle h : chunks.values()) {
            synchronized (h) {
                long len = h.state == ChunkState.SEALED ? h.sealedLength : h.end;
                out.add(new InventoryItem(h.id, h.state, len, h.dataCrc));
            }
        }
        return out;
    }

    public long usedBytes() {
        long total = 0;
        for (Handle h : chunks.values()) {
            synchronized (h) {
                total += h.end;
            }
        }
        return total;
    }

    public boolean contains(ChunkId id) {
        return chunks.containsKey(id);
    }

    /* ---------------- startup recovery (tech design §11.3) ---------------- */

    private void recoverAll() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(p -> p.getFileName().toString().endsWith(".chunk"))
                    .forEach(p -> {
                        String base = p.getFileName().toString();
                        base = base.substring(0, base.length() - ".chunk".length());
                        try {
                            recoverOne(ChunkFormats.parseBaseName(base));
                        } catch (Exception e) {
                            log.error("failed to recover chunk {} — quarantined", base, e);
                        }
                    });
        }
    }

    private void recoverOne(ChunkId id) throws IOException {
        Handle probe = new Handle(id, null);
        if (!Files.exists(probe.metaPath)) {
            // crash between data-file creation and sidecar creation: OPEN_CHUNK was never acked
            log.warn("chunk {} has no sidecar — removing incomplete remnants", id);
            Files.deleteIfExists(probe.dataPath);
            Files.deleteIfExists(probe.ledgerPath);
            return;
        }
        ChunkFormats.Sidecar sidecar = ChunkFormats.Sidecar.decode(Files.readAllBytes(probe.metaPath));

        byte[] headerBytes = new byte[HEADER_SIZE];
        try (FileChannel ch = FileChannel.open(probe.dataPath, StandardOpenOption.READ)) {
            readFully(ch, ByteBuffer.wrap(headerBytes), 0);
        }
        ChunkFormats.Header header = ChunkFormats.Header.decode(headerBytes);
        Handle h = new Handle(id, header);
        h.data = FileChannel.open(h.dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        h.writeEpoch = sidecar.writeEpoch();
        h.fenceEpoch = sidecar.fenceEpoch();
        h.lastKnownDO = sidecar.lastKnownDO();

        if (sidecar.state() == ChunkState.SEALED) {
            long fileLen = h.data.size();
            byte[] trailerBytes = new byte[TRAILER_SIZE];
            readFully(h.data, ByteBuffer.wrap(trailerBytes), fileLen - TRAILER_SIZE);
            ChunkFormats.Trailer trailer = ChunkFormats.Trailer.decode(trailerBytes);
            // verify the footer sections against the trailer's CRC (cheap — KBs): a sealed chunk
            // with rotted footer metadata must be quarantined, not served as healthy (full
            // data-region verification is deferred to scrub; readers have CRC_RANGES + batch CRCs)
            long footerLen = fileLen - TRAILER_SIZE - trailer.footerStart();
            if (footerLen < 0 || trailer.footerStart() != DATA_START + trailer.dataLength()) {
                throw new CorruptChunkException("trailer geometry invalid for " + id);
            }
            byte[] footerBytes = new byte[(int) footerLen];
            readFully(h.data, ByteBuffer.wrap(footerBytes), trailer.footerStart());
            if (Crc.of(footerBytes) != trailer.footerCrc()) {
                throw new CorruptChunkException("footer crc mismatch for sealed chunk " + id);
            }
            h.state = ChunkState.SEALED;
            h.end = trailer.dataLength();
            h.sealedLength = trailer.dataLength();
            h.dataCrc = trailer.dataCrc();
            Files.deleteIfExists(h.ledgerPath); // seal crashed before ledger delete
        } else {
            // OPEN: replay ledger, verify tail data CRCs, truncate to the last verified boundary
            if (!Files.exists(h.ledgerPath) && h.data.size() > HEADER_SIZE) {
                // the ledger should only ever be absent for SEALED chunks; an open chunk with
                // data but no ledger means external damage — its bytes are unverifiable and the
                // truncate below discards them, so say it loudly
                log.warn("chunk {} is OPEN with {} data bytes but NO integrity ledger — "
                        + "unverifiable data will be discarded", id, h.data.size() - HEADER_SIZE);
            }
            h.ledger = IntegrityLedger.open(h.ledgerPath);
            long verifiedEnd = 0;
            byte[] buf = null;
            for (ChunkFormats.LedgerEntry e : h.ledger.entries()) {
                long start = verifiedEnd;
                int len = (int) (e.endOffset() - start);
                if (DATA_START + e.endOffset() > h.data.size()) break; // torn data tail
                if (buf == null || buf.length < len) buf = new byte[len];
                ByteBuffer bb = ByteBuffer.wrap(buf, 0, len);
                readFully(h.data, bb, DATA_START + start);
                if (Crc.of(buf, 0, len) != e.payloadCrc()) break; // corrupt tail
                verifiedEnd = e.endOffset();
            }
            h.ledger.truncateTo(verifiedEnd);
            h.data.truncate(DATA_START + verifiedEnd);
            h.state = ChunkState.OPEN;
            h.end = verifiedEnd;
            h.lastKnownDO = Math.min(h.lastKnownDO, verifiedEnd);
            h.startCommitterIfFsync(forceCount);
        }
        chunks.put(id, h);
        log.info("recovered chunk {} state={} end={}", id, h.state, h.end);
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, pos);
            if (n < 0) throw new IOException("EOF at " + pos);
            pos += n;
        }
    }

    @Override
    public void close() throws IOException {
        for (Handle h : chunks.values()) {
            synchronized (h) {
                try {
                    h.stopCommitter();
                    h.persistSidecar(); // persist advisory DO/epochs on clean shutdown
                    if (h.ledger != null) h.ledger.close();
                    if (h.data != null) h.data.close();
                } catch (IOException e) {
                    log.warn("close {} failed", h.id, e);
                }
            }
        }
        chunks.clear();
    }
}
