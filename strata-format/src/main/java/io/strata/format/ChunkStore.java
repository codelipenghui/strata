package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Closeables;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import static io.strata.common.Checks.checkedAdd;
import static io.strata.format.ChunkFormats.DATA_START;
import static io.strata.format.ChunkFormats.HEADER_SIZE;
import static io.strata.format.ChunkFormats.TRAILER_SIZE;
import static io.strata.format.ChunkFormats.readFully;
import static io.strata.format.ChunkFormats.writeFully;

/**
 * Node-local chunk engine (tech design §5, §11): epoch-fenced appends, integrity-ledger crash
 * recovery, seal with node-computed CRC_RANGES/STATS, sealed-chunk import for repair.
 *
 * All mutations are synchronized per chunk handle. The data region is raw logical bytes —
 * this engine never parses payload content (invariant §14.10), not even during recovery.
 */
public final class ChunkStore implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);

    /** Per-request read/fetch cap: callers loop; the frame layer tops out at 64 MB anyway. */
    public static final int MAX_REQUEST_BYTES = 8 * 1024 * 1024;

    private final Path dir;
    private final Map<ChunkId, Handle> chunks = new ConcurrentHashMap<>();
    private final Set<ChunkId> creating = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicLong forceCount =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong appendOps =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong appendBytes =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong readOps =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong readBytes =
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

    /** Total appended records since start (observability; drives write-ops/sec via rate()). */
    public long appendOps() {
        return appendOps.get();
    }

    /** Total appended payload bytes since start (observability; drives write throughput). */
    public long appendBytes() {
        return appendBytes.get();
    }

    /** Total client READ operations that served data since start (drives read-ops/sec via rate()). Only {@link #readRegion} updates this; {@code read()} and {@code fetch()} do not. */
    public long readOps() {
        return readOps.get();
    }

    /** Total client READ payload bytes served since start (drives read throughput via rate()). Only {@link #readRegion} updates this; {@code read()} and {@code fetch()} do not. */
    public long readBytes() {
        return readBytes.get();
    }

    /** Open (being-written) chunk count — best-effort snapshot for observability. */
    public int openChunks() {
        return countByState(ChunkState.OPEN);
    }

    /** Sealed (immutable) chunk count — best-effort snapshot for observability. */
    public int sealedChunks() {
        return countByState(ChunkState.SEALED);
    }

    private int countByState(ChunkState state) {
        int n = 0;
        for (Handle h : chunks.values()) {
            if (h.state == state) n++;
        }
        return n;
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
        List<Integer> sealedRangeCrcs = List.of();

        /** The end offset served to readers: the sealed length once sealed, the live end before. */
        long currentEnd() {
            return state == ChunkState.SEALED ? sealedLength : end;
        }

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
                writeFully(ch, ByteBuffer.wrap(bytes), 0);
                ch.force(true);
            }
        }

        void startCommitterIfFsync(java.util.concurrent.atomic.AtomicLong counter) {
            if (header.fsyncOnAck()) {
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
                if (committer.isPoisoned()) {
                    throw new IOException("group-commit flusher failed for " + id
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

    private void reserveNewChunk(ChunkId id) {
        if (!creating.add(id)) throw chunkAlreadyExists(id);
        if (chunks.containsKey(id)) {
            creating.remove(id);
            throw chunkAlreadyExists(id);
        }
    }

    private void releaseReservation(ChunkId id) {
        creating.remove(id);
    }

    private static ScpException chunkAlreadyExists(ChunkId id) {
        return new ScpException(ErrorCode.CHUNK_ALREADY_EXISTS, id.toString());
    }

    private static void requireNonNegative(long value, String what) {
        if (value < 0) {
            throw new ScpException(ErrorCode.INTERNAL, "negative " + what + ": " + value);
        }
    }

    private static void checkEpoch(Handle h, int epoch) {
        int floor = Math.max(h.fenceEpoch, h.writeEpoch);
        if (epoch < floor) {
            throw new ScpException(ErrorCode.FENCED_EPOCH, "epoch " + epoch + " < " + floor, floor);
        }
    }

    /* ---------------- operations ---------------- */

    public void open(ChunkId id, boolean fsyncOnAck, int writeEpoch, long createdAtMs) throws IOException {
        reserveNewChunk(id);
        Handle h = null;
        boolean dataCreated = false;
        boolean ledgerOwned = false;
        boolean metaOwned = false;
        boolean installed = false;
        try {
            ChunkFormats.Header header = new ChunkFormats.Header(id, fsyncOnAck, writeEpoch, createdAtMs, 0, 0, 0);
            h = new Handle(id, header);
            if (Files.exists(h.dataPath)) throw chunkAlreadyExists(id);
            boolean ledgerPreexisted = Files.exists(h.ledgerPath);
            boolean metaPreexisted = Files.exists(h.metaPath);
            h.data = FileChannel.open(h.dataPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            dataCreated = true;
            writeFully(h.data, ByteBuffer.wrap(header.encode()), 0);
            h.data.force(true);
            ledgerOwned = !ledgerPreexisted;
            h.ledger = IntegrityLedger.create(h.ledgerPath);
            h.state = ChunkState.OPEN;
            h.end = 0;
            h.writeEpoch = writeEpoch;
            h.fenceEpoch = -1;
            h.lastKnownDO = 0;
            metaOwned = !metaPreexisted;
            h.persistSidecar();
            h.startCommitterIfFsync(forceCount);
            chunks.put(id, h);
            installed = true;
        } finally {
            if (!installed && h != null) {
                cleanupFailedOpen(h, dataCreated, ledgerOwned, metaOwned);
            }
            releaseReservation(id);
        }
    }

    private void cleanupFailedOpen(Handle h, boolean dataCreated, boolean ledgerOwned, boolean metaOwned) {
        if (h.committer != null && !h.committer.closeAndConfirm()) {
            log.warn("failed open cleanup could not stop group-commit flusher for {}", h.id);
        }
        if (h.ledger != null) {
            try {
                h.ledger.close();
            } catch (IOException e) {
                log.warn("failed to close incomplete ledger {}", h.ledgerPath, e);
            }
        }
        if (h.data != null) {
            try {
                h.data.close();
            } catch (IOException e) {
                log.warn("failed to close incomplete chunk {}", h.dataPath, e);
            }
        }
        deleteOwnedPath(dataCreated, h.dataPath, "incomplete chunk data");
        deleteOwnedPath(ledgerOwned, h.ledgerPath, "incomplete chunk ledger");
        deleteOwnedPath(metaOwned, h.metaPath, "incomplete chunk sidecar");
    }

    private void deleteOwnedPath(boolean owned, Path path, String description) {
        if (!owned) return;
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.warn("failed to delete {} {}", description, path, e);
        }
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
        // CRC the payload before taking the chunk monitor: the pass is state-independent and
        // would otherwise serialize behind every other append to this chunk
        // Crc.of(ByteBuffer) duplicates the buffer internally, so it leaves payload's position
        // and limit intact — no need to allocate an outer duplicate here.
        int payloadCrc = payload.hasRemaining() ? Crc.of(payload) : 0;
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
            newEnd = checkedAdd(baseOffset, len, "chunk offset");
            long writePos = checkedAdd(DATA_START, baseOffset, "chunk file offset");
            writeFully(h.data, payload.duplicate(), writePos);
            h.ledger.append(new ChunkFormats.LedgerEntry(newEnd, payloadCrc, epoch));
            h.end = newEnd;
            appendOps.incrementAndGet();
            appendBytes.addAndGet(len);
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

    /**
     * A read response that is EITHER a zero-copy file region (sealed chunks, whose data is
     * immutable) OR a heap snapshot of the bytes (open chunks, materialized under the chunk lock
     * so a concurrent seal-truncate cannot corrupt a deferred transfer). Exactly one of
     * {@code channel} / {@code bytes} is set for a non-empty result.
     */
    public record ReadRegionResult(FileChannel channel, long filePosition, int length, byte[] bytes,
                                   long localEndOffset, long lastKnownDO) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            if (channel != null) {
                channel.close();
            }
        }
    }

    public ReadResult read(ChunkId id, long offset, int maxBytes) throws IOException {
        requireNonNegative(offset, "read offset");
        requireNonNegative(maxBytes, "read maxBytes");
        Handle h = lookup(id);
        synchronized (h) {
            long end = h.currentEnd();
            if (offset >= end) return new ReadResult(new byte[0], end, h.lastKnownDO);
            int n = (int) Math.min(Math.min(maxBytes, MAX_REQUEST_BYTES), end - offset);
            byte[] out = new byte[n];
            if (h.state == ChunkState.SEALED) {
                readSealedVerified(h, offset, out);
            } else {
                readFully(h.data, ByteBuffer.wrap(out), checkedAdd(DATA_START, offset, "chunk file offset"));
            }
            return new ReadResult(out, end, h.lastKnownDO);
        }
    }

    public ReadRegionResult readRegion(ChunkId id, long offset, int maxBytes) throws IOException {
        requireNonNegative(offset, "read offset");
        requireNonNegative(maxBytes, "read maxBytes");
        Handle h = lookup(id);
        synchronized (h) {
            long end = h.currentEnd();
            if (offset >= end) {
                return new ReadRegionResult(null, 0, 0, null, end, h.lastKnownDO);
            }
            int n = (int) Math.min(Math.min(maxBytes, MAX_REQUEST_BYTES), end - offset);
            if (n == 0) {
                return new ReadRegionResult(null, 0, 0, null, end, h.lastKnownDO);
            }
            // observability: count client READ bytes served (mirrors append counters; drives read throughput)
            readOps.incrementAndGet();
            readBytes.addAndGet(n);
            long filePos = checkedAdd(DATA_START, offset, "chunk file offset");
            if (h.state != ChunkState.SEALED) {
                // OPEN chunk: a concurrent seal can truncate the never-acked tail of this shared
                // inode. A zero-copy region resolved LATER (the data-plane transfers the file
                // region off the chunk lock, on the transport event loop) could then stream the
                // seal footer/trailer as payload, or short-transfer past the new EOF — silent
                // corruption / SCP frame desync. Materialize a stable snapshot HERE under the lock
                // (as read() does) so the wire payload is fixed before the lock is released.
                byte[] out = new byte[n];
                readFully(h.data, ByteBuffer.wrap(out), filePos);
                return new ReadRegionResult(null, 0, n, out, end, h.lastKnownDO);
            }
            // SEALED: the data region is immutable (no further append/seal/truncate); a concurrent
            // delete only unlinks the path while this independent FD keeps the inode alive, so the
            // deferred zero-copy transfer is safe.
            FileChannel readChannel = FileChannel.open(h.dataPath, StandardOpenOption.READ);
            try {
                return new ReadRegionResult(readChannel, filePos, n, null, end, h.lastKnownDO);
            } catch (RuntimeException e) {
                try {
                    readChannel.close();
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
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
            return new FenceResult(h.fenceEpoch, h.currentEnd(), h.lastKnownDO, h.state);
        }
    }

    public record StatResult(ChunkState state, long localEndOffset, long lastKnownDO, int writeEpoch,
                             int fenceEpoch, long sealedLength, int dataCrc) {}

    public StatResult stat(ChunkId id) {
        Handle h = lookup(id);
        synchronized (h) {
            return new StatResult(h.state, h.currentEnd(), h.lastKnownDO, h.writeEpoch, h.fenceEpoch,
                    h.sealedLength, h.dataCrc);
        }
    }

    public record SealResult(long finalLength, int dataCrc) {}

    /**
     * Seals at dataLength (must be <= current end; shorter means truncate the never-acked tail).
     * callerSections, if non-empty, is a pre-encoded section list: u32 count + section bytes.
     */
    public SealResult seal(ChunkId id, int epoch, long dataLength, ByteBuffer callerSections) throws IOException {
        // a negative length would pass the > end check and truncate(DATA_START + negative)
        // destroys the chunk HEADER before anything throws — reject at the boundary
        requireNonNegative(dataLength, "seal dataLength");
        Handle h = lookup(id);
        synchronized (h) {
            checkEpoch(h, epoch);
            if (h.state == ChunkState.SEALED) {
                if (h.sealedLength == dataLength) return new SealResult(h.sealedLength, h.dataCrc); // idempotent
                throw new ScpException(ErrorCode.CHUNK_SEALED, "sealed at " + h.sealedLength, h.sealedLength);
            }
            if (dataLength > h.end) {
                throw new ScpException(ErrorCode.INTERNAL, "seal beyond end: " + dataLength + " > " + h.end);
            }
            CallerSections caller = readCallerSections(callerSections);
            int callerCount = caller.count();
            byte[] callerBytes = caller.bytes();

            // Node-computed sections are deterministic, so replicas stay byte-identical. Compute
            // them before stopping the fsync committer; caller validation or read verification
            // failures must leave an OPEN ack-on-fsync chunk with its committer still running.
            CrcScan scan = scanDataCrcs(h, dataLength);
            int ledgerEntryCount = ledgerEntriesThrough(h.ledger, dataLength);

            // stop the group committer BEFORE truncating: its flusher must not race the truncate,
            // and its final force drains any remaining waiters
            h.stopCommitter();
            if (dataLength < h.end) {
                h.data.truncate(checkedAdd(DATA_START, dataLength, "chunk file offset"));
                h.ledger.truncateTo(dataLength);
                h.end = dataLength;
            }

            ByteBuffer crcRanges = ByteBuffer.allocate(4 + 4 + scan.rangeCrcs.size() * 4);
            crcRanges.putInt(ChunkFormats.CRC_RANGE_SIZE).putInt(scan.rangeCrcs.size());
            for (int c : scan.rangeCrcs) crcRanges.putInt(c);
            byte[] stats = ByteBuffer.allocate(12).putLong(dataLength).putInt(ledgerEntryCount).array();

            int footerLen = callerBytes.length
                    + ChunkFormats.sectionSize(crcRanges.array())
                    + ChunkFormats.sectionSize(stats);
            ByteBuffer footer = ByteBuffer.allocate(footerLen);
            footer.put(callerBytes);
            ChunkFormats.writeSection(footer, ChunkFormats.SECTION_CRC_RANGES, crcRanges.array());
            ChunkFormats.writeSection(footer, ChunkFormats.SECTION_STATS, stats);
            footer.flip();

            long footerStart = checkedAdd(DATA_START, dataLength, "footer start");
            int footerCrc = Crc.of(footer.duplicate());
            ChunkFormats.Trailer trailer = new ChunkFormats.Trailer(dataLength, footerStart,
                    callerCount + 2, 0, footerCrc, scan.dataCrc);

            writeFully(h.data, footer.duplicate(), footerStart);
            writeFully(h.data, ByteBuffer.wrap(trailer.encode()),
                    checkedAdd(footerStart, footerLen, "trailer offset"));
            h.data.force(false);

            h.state = ChunkState.SEALED;
            h.sealedLength = dataLength;
            h.dataCrc = scan.dataCrc;
            h.sealedRangeCrcs = List.copyOf(scan.rangeCrcs);
            h.lastKnownDO = dataLength;
            h.persistSidecar();
            h.ledger.close();
            Files.deleteIfExists(h.ledgerPath);
            h.ledger = null;
            return new SealResult(dataLength, scan.dataCrc);
        }
    }

    private record CrcScan(int dataCrc, List<Integer> rangeCrcs) {}

    private record CallerSections(int count, byte[] bytes) {}

    private static int ledgerEntriesThrough(IntegrityLedger ledger, long endOffset) {
        int count = 0;
        for (ChunkFormats.LedgerEntry e : ledger.entries()) {
            if (e.endOffset() > endOffset) break;
            count++;
        }
        return count;
    }

    private static CallerSections readCallerSections(ByteBuffer callerSections) {
        if (callerSections == null || !callerSections.hasRemaining()) {
            return new CallerSections(0, new byte[0]);
        }
        ByteBuffer input = callerSections.slice();
        if (input.remaining() < Integer.BYTES) {
            throw new ScpException(ErrorCode.PRECONDITION_FAILED, "malformed caller footer section list");
        }
        int expectedCount = input.getInt();
        if (expectedCount < 0) {
            throw new ScpException(ErrorCode.PRECONDITION_FAILED, "negative caller footer section count");
        }
        byte[] bytes = new byte[input.remaining()];
        input.get(bytes);
        validateCallerFooterSections(bytes, expectedCount);
        return new CallerSections(expectedCount, bytes);
    }

    private static void validateCallerFooterSections(byte[] bytes, int expectedCount) {
        ByteBuffer footer = ByteBuffer.wrap(bytes);
        int sections = 0;
        while (footer.hasRemaining()) {
            if (footer.remaining() < 12) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED, "trailing caller footer bytes");
            }
            int type = footer.getShort() & 0xFFFF;
            footer.getShort(); // section version
            int length = footer.getInt();
            if (length < 0 || length > footer.remaining() - Integer.BYTES) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED, "bad caller footer section length");
            }
            if (type == ChunkFormats.SECTION_CRC_RANGES) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED, "caller may not provide CRC_RANGES");
            }
            byte[] content = new byte[length];
            footer.get(content);
            int sectionCrc = footer.getInt();
            if (Crc.of(content) != sectionCrc) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED, "caller footer section crc mismatch");
            }
            sections++;
        }
        if (sections != expectedCount) {
            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                    "caller footer section count mismatch: " + sections + " != " + expectedCount);
        }
    }

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
            readFully(h.data, bb, checkedAdd(DATA_START, pos, "chunk file offset"));
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
        requireNonNegative(fromOffset, "ledger offset");
        Handle h = lookup(id);
        synchronized (h) {
            if (h.ledger == null) return List.of();
            return h.ledger.entriesAfter(fromOffset);
        }
    }

    public record FetchResult(long fileLength, ChunkState state, byte[] bytes) {}

    /** Raw file bytes (header + data + footer) — repair/relocation transfer. Sealed chunks only. */
    public FetchResult fetch(ChunkId id, long offset, int maxBytes) throws IOException {
        requireNonNegative(offset, "fetch offset");
        requireNonNegative(maxBytes, "fetch maxBytes");
        Handle h = lookup(id);
        synchronized (h) {
            if (h.state != ChunkState.SEALED) {
                throw new ScpException(ErrorCode.INTERNAL, "fetch of non-sealed chunk " + id);
            }
            long fileLen = h.data.size();
            if (offset >= fileLen) return new FetchResult(fileLen, h.state, new byte[0]);
            int n = (int) Math.min(Math.min(maxBytes, MAX_REQUEST_BYTES), fileLen - offset);
            byte[] out = new byte[n];
            readFully(h.data, ByteBuffer.wrap(out), offset);
            return new FetchResult(fileLen, h.state, out);
        }
    }

    /** Imports a sealed chunk from raw file bytes (repair pull target side). Validates everything. */
    public void importSealed(ChunkId id, byte[] fileBytes, long expectedLength, int expectedCrc) throws IOException {
        reserveNewChunk(id);
        try {
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
            if (dataCrc != trailer.dataCrc() || dataCrc != expectedCrc) {
                throw new ScpException(ErrorCode.CRC_MISMATCH, "data crc mismatch on import");
            }
            int footerLen = checkedFooterLength(trailer, fileBytes.length);
            byte[] footerBytes = new byte[(int) footerLen];
            System.arraycopy(fileBytes, (int) trailer.footerStart(), footerBytes, 0, footerBytes.length);
            if (Crc.of(footerBytes) != trailer.footerCrc()) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "footer crc mismatch on import");
            }
            List<Integer> rangeCrcs = decodeCrcRanges(footerBytes, trailer.dataLength(), trailer.sectionCount());

            Handle h = new Handle(id, header);
            if (Files.exists(h.dataPath)) throw chunkAlreadyExists(id);
            Path tmp = h.dataPath.resolveSibling(h.dataPath.getFileName() + ".tmp");
            boolean movedData = false;
            boolean sidecarStarted = false;
            boolean installed = false;
            try {
                Files.write(tmp, fileBytes);
                try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                    ch.force(true);
                }
                Files.move(tmp, h.dataPath, StandardCopyOption.ATOMIC_MOVE);
                movedData = true;
                h.data = FileChannel.open(h.dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
                h.state = ChunkState.SEALED;
                h.end = trailer.dataLength();
                h.sealedLength = trailer.dataLength();
                h.dataCrc = trailer.dataCrc();
                h.sealedRangeCrcs = rangeCrcs;
                h.writeEpoch = header.createWriteEpoch();
                h.fenceEpoch = -1;
                h.lastKnownDO = trailer.dataLength();
                sidecarStarted = true;
                h.persistSidecar();
                Files.deleteIfExists(h.ledgerPath);
                chunks.put(id, h);
                installed = true;
            } finally {
                if (!installed) {
                    cleanupFailedImport(h, tmp, movedData, sidecarStarted);
                }
            }
        } finally {
            releaseReservation(id);
        }
    }

    private void cleanupFailedImport(Handle h, Path tmp, boolean movedData, boolean sidecarStarted) {
        if (h.data != null) {
            try {
                h.data.close();
            } catch (IOException e) {
                log.warn("failed to close incomplete import {}", h.id, e);
            }
        }
        try {
            Files.deleteIfExists(tmp);
        } catch (IOException e) {
            log.warn("failed to delete incomplete import temp file {}", tmp, e);
        }
        if (movedData) {
            try {
                Files.deleteIfExists(h.dataPath);
            } catch (IOException e) {
                log.warn("failed to delete incomplete import data file {}", h.dataPath, e);
            }
        }
        if (sidecarStarted) {
            try {
                Files.deleteIfExists(h.metaPath);
            } catch (IOException e) {
                log.warn("failed to delete incomplete import sidecar {}", h.metaPath, e);
            }
        }
    }

    public ErrorCode delete(ChunkId id) {
        Handle h = chunks.get(id);
        if (h == null && creating.contains(id)) return ErrorCode.INTERNAL;
        if (h == null) return ErrorCode.CHUNK_NOT_FOUND;
        synchronized (h) {
            try {
                h.stopCommitter();
                if (h.data != null) h.data.close();
                if (h.ledger != null) h.ledger.close();
                Files.deleteIfExists(h.dataPath);
                Files.deleteIfExists(h.metaPath);
                Files.deleteIfExists(h.ledgerPath);
                chunks.remove(id, h);
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
                out.add(new InventoryItem(h.id, h.state, h.currentEnd(), h.dataCrc));
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

    /**
     * Re-verifies sealed chunks' data regions against their trailer CRC (tech design §16
     * crash-safety / the read-path's deferred verification). On rot, the handle's dataCrc is
     * updated to the RECOMPUTED value so the next inventory report mismatches the descriptor —
     * the coordinator's existing corrupt-replica path then drops and re-repairs this copy.
     * Returns the number of corrupt chunks found.
     */
    public int scrubOnce() throws IOException {
        int corrupt = 0;
        for (Handle h : chunks.values()) {
            synchronized (h) {
                if (h.state != ChunkState.SEALED) continue;
                int actual = scanDataCrcs(h, h.sealedLength).dataCrc();
                if (actual != h.dataCrc) {
                    log.error("scrub: sealed chunk {} data rot — stored crc {} actual {}; "
                            + "exposing via inventory for re-repair", h.id, h.dataCrc, actual);
                    h.dataCrc = actual;
                    corrupt++;
                }
            }
        }
        return corrupt;
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
            int footerLen = checkedFooterLength(trailer, fileLen);
            byte[] footerBytes = new byte[(int) footerLen];
            readFully(h.data, ByteBuffer.wrap(footerBytes), trailer.footerStart());
            if (Crc.of(footerBytes) != trailer.footerCrc()) {
                throw new CorruptChunkException("footer crc mismatch for sealed chunk " + id);
            }
            h.state = ChunkState.SEALED;
            h.end = trailer.dataLength();
            h.sealedLength = trailer.dataLength();
            h.dataCrc = trailer.dataCrc();
            h.sealedRangeCrcs = decodeCrcRanges(footerBytes, trailer.dataLength(), trailer.sectionCount());
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
                long delta = e.endOffset() - start;
                if (delta <= 0 || delta > Integer.MAX_VALUE) break; // impossible append extent
                int len = (int) delta;
                long entryFileEnd;
                try {
                    entryFileEnd = checkedAdd(DATA_START, e.endOffset(), "chunk file offset");
                } catch (ScpException overflow) {
                    break;
                }
                if (entryFileEnd > h.data.size()) break; // torn data tail
                if (buf == null || buf.length < len) buf = new byte[len];
                ByteBuffer bb = ByteBuffer.wrap(buf, 0, len);
                readFully(h.data, bb, checkedAdd(DATA_START, start, "chunk file offset"));
                if (Crc.of(buf, 0, len) != e.payloadCrc()) break; // corrupt tail
                verifiedEnd = e.endOffset();
            }
            h.ledger.truncateTo(verifiedEnd);
            h.data.truncate(checkedAdd(DATA_START, verifiedEnd, "chunk file offset"));
            h.state = ChunkState.OPEN;
            h.end = verifiedEnd;
            h.lastKnownDO = Math.min(h.lastKnownDO, verifiedEnd);
            h.startCommitterIfFsync(forceCount);
        }
        chunks.put(id, h);
        log.info("recovered chunk {} state={} end={}", id, h.state, h.end);
    }

    private void readSealedVerified(Handle h, long offset, byte[] out) throws IOException {
        if (out.length == 0) return;
        if (h.sealedRangeCrcs.isEmpty()) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "sealed chunk missing CRC ranges: " + h.id);
        }
        long firstRange = offset / ChunkFormats.CRC_RANGE_SIZE;
        long lastRange = (offset + out.length - 1) / ChunkFormats.CRC_RANGE_SIZE;
        byte[] rangeBuf = new byte[ChunkFormats.CRC_RANGE_SIZE];
        for (long range = firstRange; range <= lastRange; range++) {
            if (range >= h.sealedRangeCrcs.size()) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "CRC range missing for " + h.id);
            }
            long rangeStart = range * (long) ChunkFormats.CRC_RANGE_SIZE;
            int rangeLen = (int) Math.min(ChunkFormats.CRC_RANGE_SIZE, h.sealedLength - rangeStart);
            readFully(h.data, ByteBuffer.wrap(rangeBuf, 0, rangeLen),
                    checkedAdd(DATA_START, rangeStart, "chunk file offset"));
            int actual = Crc.of(rangeBuf, 0, rangeLen);
            int expected = h.sealedRangeCrcs.get((int) range);
            if (actual != expected) {
                throw new ScpException(ErrorCode.CRC_MISMATCH,
                        "sealed range crc mismatch on " + h.id + " range " + range);
            }
            // serve the requested bytes from the just-verified range instead of re-reading disk
            long copyStart = Math.max(offset, rangeStart);
            long copyEnd = Math.min(offset + out.length, rangeStart + rangeLen);
            System.arraycopy(rangeBuf, (int) (copyStart - rangeStart),
                    out, (int) (copyStart - offset), (int) (copyEnd - copyStart));
        }
    }

    private static int checkedFooterLength(ChunkFormats.Trailer trailer, long fileLen) {
        if (trailer.incompatFlags() != 0) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                    "unsupported trailer incompat flags 0x" + Integer.toHexString(trailer.incompatFlags()));
        }
        long maxData = fileLen - DATA_START - TRAILER_SIZE;
        if (trailer.dataLength() < 0 || trailer.dataLength() > maxData) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "bad dataLength " + trailer.dataLength());
        }
        if (trailer.sectionCount() < 0) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "bad footer section count");
        }
        long footerLen = fileLen - TRAILER_SIZE - trailer.footerStart();
        if (footerLen < 0 || footerLen > Integer.MAX_VALUE
                || trailer.footerStart() != checkedAdd(DATA_START, trailer.dataLength(), "footer start")) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "trailer geometry invalid");
        }
        return (int) footerLen;
    }

    private static List<Integer> decodeCrcRanges(byte[] footerBytes, long dataLength, int expectedSections) {
        ByteBuffer footer = ByteBuffer.wrap(footerBytes);
        List<Integer> crcRanges = null;
        int sections = 0;
        while (footer.hasRemaining()) {
            if (footer.remaining() < 12) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "trailing bytes in sealed footer");
            }
            int type = footer.getShort() & 0xFFFF;
            footer.getShort(); // section version
            int length = footer.getInt();
            if (length < 0 || length > footer.remaining() - Integer.BYTES) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "bad footer section length");
            }
            sections++;
            byte[] content = new byte[length];
            footer.get(content);
            int sectionCrc = footer.getInt();
            if (Crc.of(content) != sectionCrc) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "footer section crc mismatch");
            }
            if (type == ChunkFormats.SECTION_CRC_RANGES) {
                if (crcRanges != null) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "duplicate CRC_RANGES section");
                }
                ByteBuffer c = ByteBuffer.wrap(content);
                if (content.length < 8) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "bad CRC_RANGES section");
                }
                int rangeSize = c.getInt();
                int count = c.getInt();
                if (rangeSize != ChunkFormats.CRC_RANGE_SIZE || count < 0 || count > c.remaining() / Integer.BYTES) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "invalid CRC_RANGES section");
                }
                long expectedCountLong = dataLength == 0 ? 0 : ((dataLength - 1) / rangeSize) + 1;
                if (expectedCountLong > Integer.MAX_VALUE) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "too many CRC_RANGES entries");
                }
                int expectedCount = (int) expectedCountLong;
                if (count != expectedCount || c.remaining() != count * Integer.BYTES) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "invalid CRC_RANGES section");
                }
                List<Integer> ranges = new ArrayList<>(count);
                for (int i = 0; i < count; i++) ranges.add(c.getInt());
                crcRanges = List.copyOf(ranges);
            }
        }
        if (sections != expectedSections) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                    "footer section count mismatch: " + sections + " != " + expectedSections);
        }
        if (crcRanges != null) return crcRanges;
        throw new ScpException(ErrorCode.CORRUPT_CHUNK, "missing CRC_RANGES section");
    }

    @Override
    public void close() throws IOException {
        Throwable failure = null;
        for (Handle h : chunks.values()) {
            synchronized (h) {
                boolean mayCloseFiles = true;
                if (h.committer != null) {
                    try {
                        if (!h.committer.closeAndConfirm()) {
                            IOException e = new IOException("group-commit flusher stuck for " + h.id
                                    + " — refusing to close chunk files");
                            log.warn("close {} failed", h.id, e);
                            failure = Closeables.suppress(failure, e);
                            mayCloseFiles = false;
                        } else if (h.committer.isPoisoned()) {
                            IOException e = new IOException("group-commit flusher failed for " + h.id
                                    + " — chunk shutdown was not clean");
                            log.warn("close {} failed", h.id, e);
                            failure = Closeables.suppress(failure, e);
                            h.committer = null;
                        } else {
                            h.committer = null;
                        }
                    } catch (RuntimeException e) {
                        log.warn("close {} failed", h.id, e);
                        failure = Closeables.suppress(failure, e);
                        mayCloseFiles = false;
                    }
                }
                if (!mayCloseFiles) {
                    continue;
                }
                try {
                    h.persistSidecar(); // persist advisory DO/epochs on clean shutdown
                } catch (IOException | RuntimeException e) {
                    log.warn("close {} failed", h.id, e);
                    failure = Closeables.suppress(failure, e);
                }
                if (h.ledger != null) {
                    try {
                        h.ledger.close();
                    } catch (IOException | RuntimeException e) {
                        log.warn("close {} failed", h.id, e);
                        failure = Closeables.suppress(failure, e);
                    }
                }
                if (h.data != null) {
                    try {
                        h.data.close();
                    } catch (IOException | RuntimeException e) {
                        log.warn("close {} failed", h.id, e);
                        failure = Closeables.suppress(failure, e);
                    }
                }
            }
        }
        if (failure == null) {
            chunks.clear();
        } else {
            Closeables.throwIfFailed(failure);
        }
    }
}
