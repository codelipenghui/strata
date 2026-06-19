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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    private static final long MAX_IMPORT_FOOTER_BYTES = 64L * 1024 * 1024;

    private static final int RECOVERY_FENCE_REQUIRED = Integer.MAX_VALUE;

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
    private final java.util.concurrent.atomic.AtomicLong backgroundFlushes =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong sealedLedgerReclaims =
            new java.util.concurrent.atomic.AtomicLong();

    // Background writeback: a daemon periodically fsyncs OPEN, non-ack-on-fsync chunks that have
    // accumulated enough new data since their last flush, so the dirty-page backlog never grows to a
    // whole chunk. Best-effort, decoupled from the append/ack path. When seal fsync is enabled, this
    // keeps seal-time fsync small; with the default Kafka-like seal fsync disabled, it still prevents
    // unbounded dirty-page buildup from later stalling ordinary append writes.
    //
    // Both knobs are tunable (system property, then env, else default) so a deployment can trade fsync
    // syscall rate against seal-time fsync size: a shorter interval / smaller threshold keeps less dirty
    // data per open chunk, so a synchronized roll does not stampede the disk's fsync queue with many
    // large concurrent forces. Background writeback keeps the original 500ms / 4 MiB default; seal
    // fsync is opt-in for deployments that require sealed-chunk local crash durability.
    private static final boolean SEAL_FSYNC =
            booleanConf("strata.seal.fsync", "STRATA_SEAL_FSYNC", false);
    private static final long BG_FLUSH_INTERVAL_MS =
            longConf("strata.bgFlush.intervalMs", "STRATA_BG_FLUSH_INTERVAL_MS", 500);
    private static final long BG_FLUSH_THRESHOLD_BYTES =
            longConf("strata.bgFlush.thresholdBytes", "STRATA_BG_FLUSH_THRESHOLD_BYTES", 4L << 20); // 4 MiB
    private static final long SLOW_APPEND_LOG_NANOS = TimeUnit.MILLISECONDS.toNanos(
            longConf("strata.slowAppendLogMs", "STRATA_SLOW_APPEND_LOG_MS", 1_000));
    private static final long SLOW_MUTATION_LOG_NANOS = TimeUnit.MILLISECONDS.toNanos(
            longConf("strata.slowMutationLogMs", "STRATA_SLOW_MUTATION_LOG_MS", 500));
    private final ScheduledExecutorService flusher;
    private final ExecutorService cleanupExecutor;

    /** System property (preferred, for tests) → environment variable → default; malformed values fall
     *  back to the default rather than failing node startup. */
    static long longConf(String property, String env, long def) {
        String raw = System.getProperty(property);
        if (raw == null) raw = System.getenv(env);
        if (raw == null || raw.isBlank()) return def;
        try {
            long v = Long.parseLong(raw.trim());
            return v > 0 ? v : def;
        } catch (NumberFormatException e) {
            log.warn("ignoring non-numeric {}/{}='{}', using default {}", property, env, raw, def);
            return def;
        }
    }

    static boolean booleanConf(String property, String env, boolean def) {
        String raw = System.getProperty(property);
        if (raw == null) raw = System.getenv(env);
        if (raw == null || raw.isBlank()) return def;
        return switch (raw.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "true", "1", "yes", "y", "on" -> true;
            case "false", "0", "no", "n", "off" -> false;
            default -> {
                log.warn("ignoring non-boolean {}/{}='{}', using default {}", property, env, raw, def);
                yield def;
            }
        };
    }

    public ChunkStore(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
        recoverAll();
        this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "chunk-writeback-" + dir.getFileName());
            t.setDaemon(true);
            return t;
        });
        this.cleanupExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "chunk-cleanup-" + dir.getFileName());
            t.setDaemon(true);
            return t;
        });
        log.info("chunk store writeback configured: intervalMs={} thresholdBytes={} sealFsync={}",
                BG_FLUSH_INTERVAL_MS, BG_FLUSH_THRESHOLD_BYTES, SEAL_FSYNC);
        flusher.scheduleWithFixedDelay(this::backgroundFlushSafely,
                BG_FLUSH_INTERVAL_MS, BG_FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /** Number of group-commit force() calls — observability + coalescing tests. */
    public long fsyncForceCount() {
        return forceCount.get();
    }

    /** Number of background-writeback fsyncs of open chunks — observability; should keep seals cheap. */
    public long backgroundFlushes() {
        return backgroundFlushes.get();
    }

    /** Number of sealed chunks whose integrity ledger was reclaimed after the SEALED state was forced
     *  durable (SEAL_FSYNC=false path) — observability. */
    public long sealedLedgerReclaims() {
        return sealedLedgerReclaims.get();
    }

    /** Total appended records since start (observability; drives write-ops/sec via rate()). */
    public long appendOps() {
        return appendOps.get();
    }

    /** Total appended payload bytes since start (observability; drives write throughput). */
    public long appendBytes() {
        return appendBytes.get();
    }

    /** Total client READ operations that served data since start (drives read-ops/sec via rate()). Only {@link #readRegion} updates this; {@link #readRegionForRecovery}, {@code read()}, and {@code fetch()} do not. */
    public long readOps() {
        return readOps.get();
    }

    /** Total client READ payload bytes served since start (drives read throughput via rate()). Only {@link #readRegion} updates this; {@link #readRegionForRecovery}, {@code read()}, and {@code fetch()} do not. */
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
            synchronized (h) {
                if (h.state == state) n++;
            }
        }
        return n;
    }

    private void backgroundFlushSafely() {
        try {
            backgroundFlushOnce();
        } catch (Throwable t) {
            log.warn("background chunk-writeback round failed", t);
        }
        try {
            reclaimSealedLedgersOnce();
        } catch (Throwable t) {
            log.warn("sealed-ledger reclaim round failed", t);
        }
    }

    /**
     * Reclaims the integrity ledger of chunks sealed under {@code STRATA_SEAL_FSYNC=false}. seal()
     * deliberately retains that ledger because the SEALED footer/sidecar are left in the page cache:
     * a crash before they reach disk leaves a stale OPEN sidecar, and recovery's OPEN branch rebuilds
     * the chunk by replaying the ledger. Removing the ledger before the SEALED state is durable would
     * make recovery truncate acknowledged data to zero (C1). So this re-establishes the seal-time
     * durability ordering off the seal hot path: force the data (footer/trailer), then force the
     * SEALED sidecar, and only THEN unlink the ledger. The data force runs outside the chunk monitor
     * (it is slow); the monitor is held only to snapshot and to finish. Package-private so tests can
     * drive a round deterministically.
     */
    void reclaimSealedLedgersOnce() {
        for (Handle h : chunks.values()) {
            FileChannel data;
            synchronized (h) {
                if (h.state != ChunkState.SEALED || !h.sealedLedgerPending) {
                    continue;
                }
                data = h.data;
            }
            try {
                data.force(false); // footer/trailer durable before the sidecar may claim SEALED on disk
            } catch (IOException | RuntimeException e) {
                synchronized (h) {
                    if (chunks.get(h.id) != h || h.data != data || !h.sealedLedgerPending) {
                        continue; // delete()/close() won the race after we snapshotted — not ours to log
                    }
                }
                log.warn("sealed-ledger reclaim of {} failed to force data (will retry)", h.id, e);
                continue;
            }
            boolean reclaimed = false;
            try {
                synchronized (h) {
                    if (chunks.get(h.id) != h || h.data != data
                            || h.state != ChunkState.SEALED || !h.sealedLedgerPending) {
                        continue; // superseded after the force — leave it to the winner
                    }
                    h.persistSidecar(true);             // SEALED sidecar (+ directory) durable
                    Files.deleteIfExists(h.ledgerPath); // safe now: SEALED state recoverable without it
                    h.sealedLedgerPending = false;
                    reclaimed = true;
                }
                forceDirectory(); // make the unlink durable (recovery's SEALED branch re-deletes otherwise)
            } catch (IOException | RuntimeException e) {
                log.warn("sealed-ledger reclaim of {} failed (will retry)", h.id, e);
                continue;
            }
            if (reclaimed) {
                sealedLedgerReclaims.incrementAndGet();
            }
        }
    }

    /**
     * Best-effort writeback: fsync each OPEN, non-ack-on-fsync chunk that has accumulated at least
     * {@link #BG_FLUSH_THRESHOLD_BYTES} since its last background flush, bounding the dirty backlog a
     * later seal must flush. The force runs OUTSIDE the chunk monitor (it is slow and safe to run
     * concurrently with positional appends); the monitor is held only to read state and record
     * progress. Package-private so tests can drive a round deterministically.
     */
    void backgroundFlushOnce() {
        for (Handle h : chunks.values()) {
            long flushTo;
            FileChannel data;
            synchronized (h) {
                // ack-on-fsync chunks already force continuously via their committer; sealed chunks are
                // immutable and were forced at seal — neither needs background writeback
                if (h.state != ChunkState.OPEN || h.committer != null) {
                    continue;
                }
                if (h.end - h.bgFlushedOffset < BG_FLUSH_THRESHOLD_BYTES) {
                    continue;
                }
                flushTo = h.end;
                data = h.data;
            }
            try {
                data.force(false); // flushes all currently-dirty pages (>= flushTo)
            } catch (IOException | RuntimeException e) {
                synchronized (h) {
                    if (chunks.get(h.id) != h || h.state != ChunkState.OPEN || h.data != data) {
                        // delete()/close()/seal() won the race after we snapshotted the channel.
                        // The handle is no longer eligible for background writeback, so do not log
                        // and do not retry this stale closed channel every period.
                        continue;
                    }
                }
                log.warn("background writeback of {} failed (will retry)", h.id, e);
                continue;
            }
            boolean credited = false;
            synchronized (h) {
                if (chunks.get(h.id) == h && h.state == ChunkState.OPEN
                        && h.data == data && h.bgFlushedOffset < flushTo) {
                    h.bgFlushedOffset = flushTo;
                    credited = true;
                }
            }
            if (credited) {
                backgroundFlushes.incrementAndGet();
            }
        }
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
        // SEAL_FSYNC=false leaves a sealed chunk's footer/sidecar unforced, so its ledger is retained
        // as the recovery safety net until reclaimSealedLedgersOnce() forces the SEALED state durable.
        boolean sealedLedgerPending;
        ChunkState state;
        long end;               // logical data length
        long bgFlushedOffset;   // end offset already pushed to disk by background writeback
        int writeEpoch;
        int fenceEpoch;
        long lastKnownDO;
        long sealedLength = -1;
        int dataCrc;
        List<Integer> sealedRangeCrcs = List.of();

        // Running CRC state for an OPEN chunk: the whole-chunk CRC and the per-CRC_RANGE_SIZE range
        // CRCs are folded as bytes are appended (and rebuilt from the verified prefix on recovery),
        // so seal can emit them without re-reading the data region. Unused once SEALED.
        final CRC32C runningWhole = new CRC32C();
        CRC32C runningRange = new CRC32C();
        long rangeRemaining = ChunkFormats.CRC_RANGE_SIZE;
        final List<Integer> completedRangeCrcs = new ArrayList<>();

        /** The end offset served to readers: the sealed length once sealed, the live end before. */
        long currentEnd() {
            return state == ChunkState.SEALED ? sealedLength : end;
        }

        /**
         * Folds freshly-appended logical bytes into the running whole + range CRCs, rolling a range
         * CRC every CRC_RANGE_SIZE. Must be called in append order while holding this handle's
         * monitor; {@code payload}'s position/limit are left untouched.
         */
        void crcAccumulate(ByteBuffer payload) {
            ByteBuffer src = payload.duplicate();
            while (src.hasRemaining()) {
                int n = (int) Math.min(src.remaining(), rangeRemaining);
                ByteBuffer slice = src.duplicate();
                slice.limit(slice.position() + n);
                runningWhole.update(slice.duplicate());
                runningRange.update(slice);
                src.position(src.position() + n);
                rangeRemaining -= n;
                if (rangeRemaining == 0) {
                    completedRangeCrcs.add((int) runningRange.getValue());
                    runningRange.reset();
                    rangeRemaining = ChunkFormats.CRC_RANGE_SIZE;
                }
            }
        }

        /** Emits the accumulated CRCs in the same shape scanDataCrcs produces, without re-reading. */
        CrcScan snapshotRunningCrcs() {
            List<Integer> ranges = new ArrayList<>(completedRangeCrcs);
            if (rangeRemaining != ChunkFormats.CRC_RANGE_SIZE) { // a partial final range holds bytes
                ranges.add((int) runningRange.getValue());
            }
            return new CrcScan((int) runningWhole.getValue(), ranges);
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
            persistSidecar(true);
        }

        void persistSidecar(boolean sync) throws IOException {
            byte[] bytes = new ChunkFormats.Sidecar(writeEpoch, fenceEpoch, lastKnownDO, state).encode();
            boolean existed = Files.exists(metaPath);
            try (FileChannel ch = FileChannel.open(metaPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                writeFully(ch, ByteBuffer.wrap(bytes), 0);
                if (sync) {
                    ch.force(true);
                }
            }
            if (sync && !existed) {
                forceDirectory();
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
        if (h.fenceEpoch == RECOVERY_FENCE_REQUIRED) {
            throw new ScpException(ErrorCode.FENCED_EPOCH,
                    "fresh recovery fence required for " + h.id, nextEpochAfter(h.writeEpoch));
        }
        int floor = Math.max(h.fenceEpoch, h.writeEpoch);
        if (epoch < floor) {
            throw new ScpException(ErrorCode.FENCED_EPOCH, "epoch " + epoch + " < " + floor, floor);
        }
    }

    private static int nextEpochAfter(int epoch) {
        return epoch == Integer.MAX_VALUE ? Integer.MAX_VALUE : epoch + 1;
    }

    /* ---------------- operations ---------------- */

    public void open(ChunkId id, boolean fsyncOnAck, int writeEpoch, long createdAtMs) throws IOException {
        long t0 = System.nanoTime();
        long tChannelOpen = t0;
        long tHeaderWrite = t0;
        long tDataForce = t0;
        long tDataDirForce = t0;
        long tLedgerCreate = t0;
        long tLedgerDirForce = t0;
        long tSidecar = t0;
        long tInstall = t0;
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
            tChannelOpen = System.nanoTime();
            dataCreated = true;
            writeFully(h.data, ByteBuffer.wrap(header.encode()), 0);
            tHeaderWrite = System.nanoTime();
            if (SEAL_FSYNC) {
                h.data.force(true);
            }
            tDataForce = System.nanoTime();
            if (SEAL_FSYNC) {
                forceDirectory();
            }
            tDataDirForce = System.nanoTime();
            ledgerOwned = !ledgerPreexisted;
            h.ledger = IntegrityLedger.create(h.ledgerPath);
            tLedgerCreate = System.nanoTime();
            if (SEAL_FSYNC) {
                forceDirectory();
            }
            tLedgerDirForce = System.nanoTime();
            h.state = ChunkState.OPEN;
            h.end = 0;
            h.writeEpoch = writeEpoch;
            h.fenceEpoch = -1;
            h.lastKnownDO = 0;
            metaOwned = !metaPreexisted;
            h.persistSidecar(SEAL_FSYNC);
            tSidecar = System.nanoTime();
            h.startCommitterIfFsync(forceCount);
            chunks.put(id, h);
            tInstall = System.nanoTime();
            if (tInstall - t0 > SLOW_MUTATION_LOG_NANOS) {
                log.info("slow open {} fsyncOnAck={} phases(ms): dataOpen={} headerWrite={} dataFsync={} "
                                + "dataDirFsync={} ledgerCreate={} ledgerDirFsync={} sidecarPersist={} total={}",
                        id, fsyncOnAck, msBetween(t0, tChannelOpen), msBetween(tChannelOpen, tHeaderWrite),
                        SEAL_FSYNC ? msBetween(tHeaderWrite, tDataForce) : "-1.0",
                        SEAL_FSYNC ? msBetween(tDataForce, tDataDirForce) : "-1.0",
                        msBetween(tDataDirForce, tLedgerCreate),
                        SEAL_FSYNC ? msBetween(tLedgerCreate, tLedgerDirForce) : "-1.0",
                        msBetween(tLedgerDirForce, tSidecar), msBetween(t0, tInstall));
            }
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
            if (SEAL_FSYNC) {
                forceDirectory();
            }
        } catch (IOException e) {
            log.warn("failed to delete {} {}", description, path, e);
        }
    }

    private void forceDirectory() throws IOException {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        }
    }

    private void deleteLedgerAsync(ChunkId id, Path ledgerPath) {
        try {
            cleanupExecutor.execute(() -> {
                long t0 = System.nanoTime();
                try {
                    Files.deleteIfExists(ledgerPath);
                    long tDone = System.nanoTime();
                    if (tDone - t0 > SLOW_MUTATION_LOG_NANOS) {
                        log.info("slow async ledger delete {} phases(ms): delete={} total={}",
                                id, msBetween(t0, tDone), msBetween(t0, tDone));
                    }
                } catch (IOException | RuntimeException e) {
                    log.warn("async ledger delete {} failed (left for recovery cleanup): {}", id, ledgerPath, e);
                }
            });
        } catch (RuntimeException e) {
            log.warn("async ledger delete {} was not scheduled (left for recovery cleanup): {}", id, ledgerPath, e);
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
        long t0 = System.nanoTime();
        int payloadCrc = payload.hasRemaining() ? Crc.of(payload) : 0;
        long tPayloadCrc = System.nanoTime();
        long newEnd;
        GroupCommitter committer;
        int len;
        long tBeforeLock = System.nanoTime();
        long tLock = tBeforeLock;
        long tWrite = tBeforeLock;
        long tLedger = tBeforeLock;
        long tRunningCrc = tBeforeLock;
        long tUnlock;
        synchronized (h) {
            tLock = System.nanoTime();
            // fence check dominates the state check: a deposed writer must learn FENCED_EPOCH
            // (permanent death), never CHUNK_SEALED (which reads as "roll and continue")
            checkEpoch(h, epoch);
            if (h.state != ChunkState.OPEN) throw new ScpException(ErrorCode.CHUNK_SEALED, id.toString());
            h.writeEpoch = Math.max(h.writeEpoch, epoch);
            if (baseOffset != h.end) {
                throw new ScpException(ErrorCode.OFFSET_GAP, "expected " + h.end + " got " + baseOffset, h.end);
            }
            h.lastKnownDO = Math.max(h.lastKnownDO, Math.min(durableOffset, h.end));
            len = payload.remaining();
            if (len == 0) {
                return java.util.concurrent.CompletableFuture.completedFuture(new AppendResult(h.end)); // DO beacon
            }
            newEnd = checkedAdd(baseOffset, len, "chunk offset");
            long writePos = checkedAdd(DATA_START, baseOffset, "chunk file offset");
            writeFully(h.data, payload.duplicate(), writePos);
            tWrite = System.nanoTime();
            h.ledger.append(new ChunkFormats.LedgerEntry(newEnd, payloadCrc, epoch));
            tLedger = System.nanoTime();
            // Fold into the running whole + range CRCs ONLY after the data + ledger writes commit: a
            // throwing ledger.append must leave the accumulators (and h.end) untouched, or a same-offset
            // retry on this Handle would fold the same bytes twice and corrupt the seal-time snapshot.
            // crcAccumulate is pure CPU under the monitor — it preserves append order and cannot throw.
            h.crcAccumulate(payload);
            tRunningCrc = System.nanoTime();
            h.end = newEnd;
            appendOps.incrementAndGet();
            appendBytes.addAndGet(len);
            committer = h.committer;
        }
        tUnlock = System.nanoTime();
        if (tUnlock - t0 > SLOW_APPEND_LOG_NANOS) {
            log.info("slow append {} len={} base={} phases(ms): payloadCrc={} lockWait={} dataWrite={} "
                            + "ledgerAppend={} runningCrc={} lockHeld={} total={}",
                    id, len, baseOffset, msBetween(t0, tPayloadCrc), msBetween(tBeforeLock, tLock),
                    msBetween(tLock, tWrite), msBetween(tWrite, tLedger), msBetween(tLedger, tRunningCrc),
                    msBetween(tLock, tRunningCrc), msBetween(t0, tUnlock));
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
     * A read response that is either a zero-copy file region or a heap snapshot of the bytes.
     * Client reads of open chunks are bounded by the replica-known durable high watermark and may
     * return a zero-copy channel region without per-read ledger verification. Recovery reads can
     * include the undurable open tail, so they are snapshotted before any concurrent seal-truncate
     * can alter the transferred payload.
     * Exactly one of {@code channel} / {@code bytes} is set for a non-empty result.
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
                readOpenVerified(h, offset, out);
            }
            return new ReadResult(out, end, h.lastKnownDO);
        }
    }

    public ReadRegionResult readRegion(ChunkId id, long offset, int maxBytes) throws IOException {
        return readRegion(id, offset, maxBytes, false);
    }

    /**
     * Seal-recovery variant of {@link #readRegion}: serves locally-present bytes up to the chunk's
     * local end, INCLUDING the never-acked tail above the durable high watermark. Recovery must see
     * that tail to decide whether a quorum still holds it (tech design §7.3); clamping it away — as
     * the client read path does — makes recovery seal short and drop quorum-durable bytes. Reads are
     * still integrity-ledger verified, and the recovery path does not count toward client read
     * throughput metrics.
     */
    public ReadRegionResult readRegionForRecovery(ChunkId id, long offset, int maxBytes) throws IOException {
        return readRegion(id, offset, maxBytes, true);
    }

    private ReadRegionResult readRegion(ChunkId id, long offset, int maxBytes,
                                        boolean includeUndurableTail) throws IOException {
        requireNonNegative(offset, "read offset");
        requireNonNegative(maxBytes, "read maxBytes");
        Handle h = lookup(id);
        synchronized (h) {
            long localEnd = h.currentEnd();
            long readableEnd = (h.state == ChunkState.SEALED || includeUndurableTail)
                    ? localEnd : Math.min(localEnd, h.lastKnownDO);
            if (offset >= readableEnd) {
                return new ReadRegionResult(null, 0, 0, null, localEnd, h.lastKnownDO);
            }
            int n = (int) Math.min(Math.min(maxBytes, MAX_REQUEST_BYTES), readableEnd - offset);
            if (n == 0) {
                return new ReadRegionResult(null, 0, 0, null, localEnd, h.lastKnownDO);
            }
            if (!includeUndurableTail) {
                // observability: count client READ bytes served (mirrors append counters; drives read
                // throughput). Recovery reads are internal control-plane traffic, not client reads.
                readOps.incrementAndGet();
                readBytes.addAndGet(n);
            }
            long filePos = checkedAdd(DATA_START, offset, "chunk file offset");
            if (h.state != ChunkState.SEALED) {
                if (includeUndurableTail) {
                    // Recovery reads may inspect bytes above lastKnownDO. Keep them materialized and
                    // verified under the chunk lock because that tail can be truncated by a later seal.
                    byte[] out = new byte[n];
                    readOpenVerified(h, offset, out);
                    return new ReadRegionResult(null, 0, n, out, localEnd, h.lastKnownDO);
                }
                // Intentional fast client-read path: client READs are already clamped to lastKnownDO
                // above, so they never expose the undurable tail that a concurrent seal may truncate.
                // We do NOT re-run readOpenVerified here. Doing so would materialize and CRC the range
                // under the chunk lock before sendfile, removing the throughput benefit for actively
                // written segments. This means read-time detection of local rot in the durable prefix is
                // deferred to verified read()/READ_RECOVERY paths, crash recovery, and sealed scrub.
                FileChannel readChannel = FileChannel.open(h.dataPath, StandardOpenOption.READ);
                try {
                    return new ReadRegionResult(readChannel, filePos, n, null, localEnd, h.lastKnownDO);
                } catch (RuntimeException e) {
                    try {
                        readChannel.close();
                    } catch (IOException closeFailure) {
                        e.addSuppressed(closeFailure);
                    }
                    throw e;
                }
            }
            // SEALED: the data region is immutable (no further append/seal/truncate), so hand back a
            // zero-copy region the transport streams via sendfile — the fast client read path. We do
            // NOT re-verify range CRCs per read here: that forced a full CRC-range read + a 4 MiB
            // allocation per request and serialized readers on the chunk lock (~20x slower, measured);
            // integrity is covered by background scrub and the verified read()/fetch()/import paths. A
            // concurrent delete only unlinks the path while this independent FD keeps the inode alive,
            // so the deferred transfer is safe.
            FileChannel readChannel = FileChannel.open(h.dataPath, StandardOpenOption.READ);
            try {
                return new ReadRegionResult(readChannel, filePos, n, null, localEnd, h.lastKnownDO);
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
            if (h.fenceEpoch == RECOVERY_FENCE_REQUIRED) {
                if (fenceEpoch <= h.writeEpoch) {
                    throw new ScpException(ErrorCode.FENCED_EPOCH,
                            "fresh recovery fence must exceed " + h.writeEpoch, nextEpochAfter(h.writeEpoch));
                }
                h.fenceEpoch = fenceEpoch;
                h.persistSidecar();
            } else if (fenceEpoch > h.fenceEpoch) {
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
            if (dataLength < h.lastKnownDO) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "seal below durable watermark: " + dataLength + " < " + h.lastKnownDO, h.lastKnownDO);
            }
            CallerSections caller = readCallerSections(callerSections);
            int callerCount = caller.count();
            byte[] callerBytes = caller.bytes();

            // Node-computed sections are deterministic, so replicas stay byte-identical. Compute
            // them before stopping the fsync committer; caller validation or read verification
            // failures must leave an OPEN ack-on-fsync chunk with its committer still running.
            // Common path: seal at the live end — emit the CRCs accumulated during append, no re-read.
            // A truncating seal (dataLength < end, e.g. after recovery) can't use the running state.
            long t0 = System.nanoTime();
            CrcScan scan = dataLength == h.end ? h.snapshotRunningCrcs() : scanDataCrcs(h, dataLength);
            long tCrc = System.nanoTime();
            int ledgerEntryCount = ledgerEntriesThrough(h.ledger, dataLength);
            long tLedgerCount = System.nanoTime();

            // stop the group committer BEFORE truncating: its flusher must not race the truncate,
            // and its final force drains any remaining waiters
            h.stopCommitter();
            long tCommitter = System.nanoTime();
            if (dataLength < h.end) {
                h.data.truncate(checkedAdd(DATA_START, dataLength, "chunk file offset"));
                h.ledger.truncateTo(dataLength);
                h.end = dataLength;
            }
            long tTruncate = System.nanoTime();

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
            long tFooterBuild = System.nanoTime();

            writeFully(h.data, footer.duplicate(), footerStart);
            writeFully(h.data, ByteBuffer.wrap(trailer.encode()),
                    checkedAdd(footerStart, footerLen, "trailer offset"));
            long tWrite = System.nanoTime();
            if (SEAL_FSYNC) {
                h.data.force(false);
            }
            long tForce = System.nanoTime();

            h.state = ChunkState.SEALED;
            h.sealedLength = dataLength;
            h.dataCrc = scan.dataCrc;
            h.sealedRangeCrcs = List.copyOf(scan.rangeCrcs);
            h.lastKnownDO = dataLength;
            h.persistSidecar(SEAL_FSYNC);
            long tSidecar = System.nanoTime();
            h.ledger.close();
            long tLedgerClose = System.nanoTime();
            Path ledgerPath = h.ledgerPath;
            h.ledger = null;
            if (SEAL_FSYNC) {
                // data + sidecar were just forced durable above, so the SEALED state is recoverable
                // from the trailer without the ledger — drop it now.
                deleteLedgerAsync(id, ledgerPath);
            } else {
                // SEAL_FSYNC=false left the footer/sidecar only in the page cache, so a crash can leave
                // a stale OPEN sidecar on disk. Recovery's OPEN branch rebuilds from the ledger, so the
                // ledger must outlive the unforced SEALED state: retain it until reclaimSealedLedgersOnce()
                // has forced the SEALED state durable. Deleting it here loses acknowledged data (C1).
                h.sealedLedgerPending = true;
            }
            long tLedgerDeleteEnqueue = System.nanoTime();
            if (tLedgerDeleteEnqueue - t0 > SLOW_MUTATION_LOG_NANOS) {
                log.info("slow seal {} len={}MiB phases(ms): crc={} ledgerCount={} stopCommitter={} "
                                + "truncate={} footerBuild={} footerWrite={} dataFsync={} sidecarPersist={} "
                                + "ledgerClose={} ledgerDeleteEnqueue={} total={}",
                        id, dataLength >> 20, msBetween(t0, tCrc), msBetween(tCrc, tLedgerCount),
                        msBetween(tLedgerCount, tCommitter), msBetween(tCommitter, tTruncate),
                        msBetween(tTruncate, tFooterBuild), msBetween(tFooterBuild, tWrite),
                        SEAL_FSYNC ? msBetween(tWrite, tForce) : "-1.0",
                        SEAL_FSYNC ? msBetween(tForce, tSidecar) : "-1.0",
                        msBetween(tSidecar, tLedgerClose), msBetween(tLedgerClose, tLedgerDeleteEnqueue),
                        msBetween(t0, tLedgerDeleteEnqueue));
            }
            return new SealResult(dataLength, scan.dataCrc);
        }
    }

    private static String msBetween(long fromNs, long toNs) {
        return String.format("%.1f", (toNs - fromNs) / 1_000_000.0);
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

    /** Creates a temp file in the chunk directory for a streaming sealed-chunk import. */
    public Path createImportTemp(ChunkId id) throws IOException {
        Files.createDirectories(dir);
        return Files.createTempFile(dir, ChunkFormats.baseName(id) + ".", ".import");
    }

    /** Imports a sealed chunk from raw file bytes (tests/simple callers). Validates everything. */
    public void importSealed(ChunkId id, byte[] fileBytes, long expectedLength, int expectedCrc) throws IOException {
        Path tmp = createImportTemp(id);
        try {
            Files.write(tmp, fileBytes);
            importSealed(id, tmp, expectedLength, expectedCrc);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /**
     * Imports a sealed chunk from a raw chunk-file image already written to {@code sourceFile}.
     * The source file is consumed: on success it is atomically moved into place; on failure the
     * caller should delete it.
     */
    public void importSealed(ChunkId id, Path sourceFile, long expectedLength, int expectedCrc) throws IOException {
        reserveNewChunk(id);
        try {
            long fileLen = Files.size(sourceFile);
            if (fileLen < HEADER_SIZE + TRAILER_SIZE) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "file too short");
            }
            ChunkFormats.Header header;
            ChunkFormats.Trailer trailer;
            List<Integer> rangeCrcs;
            try (FileChannel input = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
                header = ChunkFormats.Header.decode(readBytes(input, HEADER_SIZE, 0));
                if (!header.chunkId().equals(id)) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "chunk id mismatch: " + header.chunkId());
                }
                trailer = ChunkFormats.Trailer.decode(readBytes(input, TRAILER_SIZE, fileLen - TRAILER_SIZE));
                if (expectedLength >= 0 && trailer.dataLength() != expectedLength) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "length mismatch: " + trailer.dataLength());
                }
                long maxData = fileLen - HEADER_SIZE - TRAILER_SIZE;
                if (trailer.dataLength() < 0 || trailer.dataLength() > maxData) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "bad dataLength " + trailer.dataLength());
                }
                int dataCrc = crcOfFileRange(input, HEADER_SIZE, trailer.dataLength());
                if (dataCrc != trailer.dataCrc() || dataCrc != expectedCrc) {
                    throw new ScpException(ErrorCode.CRC_MISMATCH, "data crc mismatch on import");
                }
                int footerLen = checkedFooterLength(trailer, fileLen);
                if (footerLen > MAX_IMPORT_FOOTER_BYTES) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "footer too large: " + footerLen);
                }
                byte[] footerBytes = readBytes(input, footerLen, trailer.footerStart());
                if (Crc.of(footerBytes) != trailer.footerCrc()) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "footer crc mismatch on import");
                }
                rangeCrcs = decodeCrcRanges(footerBytes, trailer.dataLength(), trailer.sectionCount());
            }

            Handle h = new Handle(id, header);
            if (Files.exists(h.dataPath)) throw chunkAlreadyExists(id);
            boolean movedData = false;
            boolean sidecarStarted = false;
            boolean installed = false;
            try {
                try (FileChannel ch = FileChannel.open(sourceFile, StandardOpenOption.WRITE)) {
                    ch.force(true);
                }
                Files.move(sourceFile, h.dataPath, StandardCopyOption.ATOMIC_MOVE);
                forceDirectory();
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
                forceDirectory();
                chunks.put(id, h);
                installed = true;
            } finally {
                if (!installed) {
                    cleanupFailedImport(h, sourceFile, movedData, sidecarStarted);
                }
            }
        } finally {
            releaseReservation(id);
        }
    }

    private static byte[] readBytes(FileChannel channel, int length, long position) throws IOException {
        byte[] bytes = new byte[length];
        readFully(channel, ByteBuffer.wrap(bytes), position);
        return bytes;
    }

    private static int crcOfFileRange(FileChannel channel, long position, long length) throws IOException {
        CRC32C crc = new CRC32C();
        byte[] buf = new byte[1 << 20];
        long read = 0;
        while (read < length) {
            int n = (int) Math.min(buf.length, length - read);
            readFully(channel, ByteBuffer.wrap(buf, 0, n), position + read);
            crc.update(buf, 0, n);
            read += n;
        }
        return (int) crc.getValue();
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
            forceDirectory();
        } catch (IOException e) {
            log.warn("failed to delete incomplete import temp file {}", tmp, e);
        }
        if (movedData) {
            try {
                Files.deleteIfExists(h.dataPath);
                forceDirectory();
            } catch (IOException e) {
                log.warn("failed to delete incomplete import data file {}", h.dataPath, e);
            }
        }
        if (sidecarStarted) {
            try {
                Files.deleteIfExists(h.metaPath);
                forceDirectory();
            } catch (IOException e) {
                log.warn("failed to delete incomplete import sidecar {}", h.metaPath, e);
            }
        }
    }

    public ErrorCode delete(ChunkId id) {
        long t0 = System.nanoTime();
        Handle h = chunks.get(id);
        if (h == null && creating.contains(id)) return ErrorCode.INTERNAL;
        if (h == null) return ErrorCode.CHUNK_NOT_FOUND;
        long tBeforeLock = System.nanoTime();
        long tLock = tBeforeLock;
        long tStopCommitter = tBeforeLock;
        long tDataClose = tBeforeLock;
        long tLedgerClose = tBeforeLock;
        long tFileDelete = tBeforeLock;
        long tDirForce = tBeforeLock;
        long tRemove = tBeforeLock;
        synchronized (h) {
            tLock = System.nanoTime();
            try {
                h.state = ChunkState.DELETING;
                h.stopCommitter();
                tStopCommitter = System.nanoTime();
                if (h.data != null) h.data.close();
                tDataClose = System.nanoTime();
                if (h.ledger != null) h.ledger.close();
                tLedgerClose = System.nanoTime();
                Files.deleteIfExists(h.dataPath);
                Files.deleteIfExists(h.metaPath);
                Files.deleteIfExists(h.ledgerPath);
                tFileDelete = System.nanoTime();
                if (SEAL_FSYNC) {
                    forceDirectory();
                }
                tDirForce = System.nanoTime();
                chunks.remove(id, h);
                tRemove = System.nanoTime();
            } catch (IOException e) {
                log.warn("delete {} failed", id, e);
                return ErrorCode.INTERNAL;
            }
        }
        if (tRemove - t0 > SLOW_MUTATION_LOG_NANOS) {
            log.info("slow delete {} phases(ms): preLookup={} lockWait={} stopCommitter={} dataClose={} "
                            + "ledgerClose={} fileDelete={} dirFsync={} lockHeld={} total={}",
                    id, msBetween(t0, tBeforeLock), msBetween(tBeforeLock, tLock),
                    msBetween(tLock, tStopCommitter), msBetween(tStopCommitter, tDataClose),
                    msBetween(tDataClose, tLedgerClose), msBetween(tLedgerClose, tFileDelete),
                    SEAL_FSYNC ? msBetween(tFileDelete, tDirForce) : "-1.0",
                    msBetween(tLock, tRemove), msBetween(t0, tRemove));
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
                total += sizeIfExists(h.dataPath);
                total += sizeIfExists(h.metaPath);
                total += sizeIfExists(h.ledgerPath);
            }
        }
        return total;
    }

    private static long sizeIfExists(Path path) {
        try {
            return Files.exists(path) ? Files.size(path) : 0;
        } catch (IOException e) {
            return 0;
        }
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
                            quarantineRecoveredFiles(base);
                        }
                    });
        }
    }

    private void quarantineRecoveredFiles(String base) {
        boolean moved = false;
        String suffix = ".quarantine-" + System.currentTimeMillis();
        for (String ext : List.of(".chunk", ".meta", ".j")) {
            Path source = dir.resolve(base + ext);
            if (!Files.exists(source)) {
                continue;
            }
            try {
                Files.move(source, quarantineTarget(source, suffix), StandardCopyOption.ATOMIC_MOVE);
                moved = true;
            } catch (IOException e) {
                log.warn("failed to quarantine {}", source, e);
            }
        }
        if (moved) {
            try {
                forceDirectory();
            } catch (IOException e) {
                log.warn("failed to fsync quarantine directory {}", dir, e);
            }
        }
    }

    private Path quarantineTarget(Path source, String suffix) {
        String name = source.getFileName().toString();
        Path target = source.resolveSibling(name + suffix);
        int attempt = 1;
        while (Files.exists(target)) {
            target = source.resolveSibling(name + suffix + "-" + attempt++);
        }
        return target;
    }

    private void recoverOne(ChunkId id) throws IOException {
        Handle probe = new Handle(id, null);
        boolean hasSidecar = Files.exists(probe.metaPath);
        byte[] headerBytes = new byte[HEADER_SIZE];
        ChunkFormats.Header header;
        try {
            try (FileChannel ch = FileChannel.open(probe.dataPath, StandardOpenOption.READ)) {
                readFully(ch, ByteBuffer.wrap(headerBytes), 0);
            }
            header = ChunkFormats.Header.decode(headerBytes);
        } catch (IOException | RuntimeException e) {
            if (!hasSidecar) {
                removeMissingSidecarRemnants(id, probe, "malformed pre-sidecar");
                return;
            }
            throw e;
        }
        if (!header.chunkId().equals(id)) {
            throw new CorruptChunkException("chunk id mismatch in header: " + header.chunkId() + " != " + id);
        }
        boolean reconstructedSidecar = false;
        ChunkFormats.Sidecar sidecar;
        if (hasSidecar) {
            sidecar = ChunkFormats.Sidecar.decode(Files.readAllBytes(probe.metaPath));
        } else {
            sidecar = recoverMissingSidecar(id, probe, header);
            if (sidecar == null) {
                return;
            }
            reconstructedSidecar = true;
        }
        Handle h = new Handle(id, header);
        boolean installed = false;
        try {
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
                h.lastKnownDO = Math.max(h.lastKnownDO, trailer.dataLength());
                if (reconstructedSidecar) {
                    h.persistSidecar();
                }
                Files.deleteIfExists(h.ledgerPath); // seal crashed before ledger delete
                forceDirectory();
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
                    h.crcAccumulate(ByteBuffer.wrap(buf, 0, len)); // rebuild running CRCs from the verified prefix
                    verifiedEnd = e.endOffset();
                }
                h.ledger.truncateTo(verifiedEnd);
                h.data.truncate(checkedAdd(DATA_START, verifiedEnd, "chunk file offset"));
                h.state = ChunkState.OPEN;
                h.end = verifiedEnd;
                h.lastKnownDO = Math.min(h.lastKnownDO, verifiedEnd);
                if (reconstructedSidecar) {
                    h.persistSidecar();
                }
                h.startCommitterIfFsync(forceCount);
            }
            chunks.put(id, h);
            installed = true;
            log.info("recovered chunk {} state={} end={}", id, h.state, h.end);
        } finally {
            if (!installed) {
                closeRecoveringHandle(h);
            }
        }
    }

    private void closeRecoveringHandle(Handle h) {
        if (h.ledger != null) {
            try {
                h.ledger.close();
            } catch (IOException e) {
                log.warn("failed to close unrecovered ledger {}", h.ledgerPath, e);
            }
        }
        if (h.data != null) {
            try {
                h.data.close();
            } catch (IOException e) {
                log.warn("failed to close unrecovered chunk {}", h.dataPath, e);
            }
        }
    }

    private ChunkFormats.Sidecar recoverMissingSidecar(ChunkId id, Handle probe,
                                                       ChunkFormats.Header header) throws IOException {
        if (Files.exists(probe.ledgerPath)) {
            if (header.fsyncOnAck()) {
                log.warn("chunk {} has no sidecar but has a ledger — reconstructing fenced OPEN state", id);
                return reconstructedOpenSidecar(header);
            }
            removeMissingSidecarRemnants(id, probe, "incomplete non-fsync");
            return null;
        }
        if (looksLikeValidSealedChunk(probe.dataPath)) {
            log.warn("chunk {} has no sidecar but has a valid sealed footer — reconstructing sidecar", id);
            return new ChunkFormats.Sidecar(header.createWriteEpoch(), -1, 0, ChunkState.SEALED);
        }
        if (header.fsyncOnAck()) {
            log.warn("chunk {} has no sidecar — reconstructing fenced fsync-on-ack OPEN state", id);
            return reconstructedOpenSidecar(header);
        }
        // For ack-on-replicate, file contents may have reached the page cache without any durable
        // local ack point. Without a sidecar, the node cannot distinguish an acked replica from an
        // unacked create remnant, so keep the existing conservative cleanup behavior.
        removeMissingSidecarRemnants(id, probe, "incomplete non-fsync");
        return null;
    }

    private ChunkFormats.Sidecar reconstructedOpenSidecar(ChunkFormats.Header header) {
        return new ChunkFormats.Sidecar(header.createWriteEpoch(), RECOVERY_FENCE_REQUIRED, 0, ChunkState.OPEN);
    }

    private void removeMissingSidecarRemnants(ChunkId id, Handle probe, String reason) throws IOException {
        log.warn("chunk {} has no sidecar — removing {} remnants", id, reason);
        Files.deleteIfExists(probe.dataPath);
        Files.deleteIfExists(probe.ledgerPath);
        forceDirectory();
    }

    private boolean looksLikeValidSealedChunk(Path dataPath) {
        try (FileChannel ch = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            long fileLen = ch.size();
            if (fileLen < DATA_START + TRAILER_SIZE) {
                return false;
            }
            byte[] trailerBytes = new byte[TRAILER_SIZE];
            readFully(ch, ByteBuffer.wrap(trailerBytes), fileLen - TRAILER_SIZE);
            ChunkFormats.Trailer trailer = ChunkFormats.Trailer.decode(trailerBytes);
            int footerLen = checkedFooterLength(trailer, fileLen);
            byte[] footerBytes = new byte[footerLen];
            readFully(ch, ByteBuffer.wrap(footerBytes), trailer.footerStart());
            if (Crc.of(footerBytes) != trailer.footerCrc()) {
                return false;
            }
            decodeCrcRanges(footerBytes, trailer.dataLength(), trailer.sectionCount());
            return true;
        } catch (IOException | RuntimeException e) {
            return false;
        }
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

    private void readOpenVerified(Handle h, long offset, byte[] out) throws IOException {
        if (out.length == 0) {
            return;
        }
        if (h.ledger == null) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "open chunk missing integrity ledger: " + h.id);
        }
        long readEnd = checkedAdd(offset, out.length, "open read end");
        long entryStart = 0;
        int copied = 0;
        for (ChunkFormats.LedgerEntry e : h.ledger.entries()) {
            long entryEnd = e.endOffset();
            if (entryEnd <= entryStart) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                        "non-increasing ledger entry for " + h.id + ": " + entryEnd);
            }
            if (entryEnd <= offset) {
                entryStart = entryEnd;
                continue;
            }
            if (entryStart >= readEnd) {
                break;
            }
            long entryLenLong = entryEnd - entryStart;
            if (entryLenLong > Integer.MAX_VALUE) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                        "oversized ledger entry for " + h.id + ": " + entryLenLong);
            }
            int entryLen = (int) entryLenLong;
            byte[] entryBytes = new byte[entryLen];
            readFully(h.data, ByteBuffer.wrap(entryBytes),
                    checkedAdd(DATA_START, entryStart, "chunk file offset"));
            int actual = Crc.of(entryBytes, 0, entryLen);
            if (actual != e.payloadCrc()) {
                throw new ScpException(ErrorCode.CRC_MISMATCH,
                        "open ledger crc mismatch on " + h.id + " range [" + entryStart + ".." + entryEnd + ")");
            }
            long copyStart = Math.max(offset, entryStart);
            long copyEnd = Math.min(readEnd, entryEnd);
            if (copyEnd > copyStart) {
                int src = (int) (copyStart - entryStart);
                int dst = (int) (copyStart - offset);
                int len = (int) (copyEnd - copyStart);
                System.arraycopy(entryBytes, src, out, dst, len);
                copied += len;
            }
            entryStart = entryEnd;
        }
        if (copied != out.length) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                    "open read is not covered by ledger for " + h.id);
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
        // Stop background writeback first so it can't race the file close/delete below. shutdown()
        // (not shutdownNow()) avoids interrupting an in-flight force(), which — FileChannel being an
        // InterruptibleChannel — would close the channel out from under the close path.
        flusher.shutdown();
        try {
            if (!flusher.awaitTermination(5, TimeUnit.SECONDS)) {
                flusher.shutdownNow();
            }
        } catch (InterruptedException e) {
            flusher.shutdownNow();
            Thread.currentThread().interrupt();
        }
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
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
