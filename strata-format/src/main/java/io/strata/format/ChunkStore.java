package io.strata.format;

import com.sun.management.UnixOperatingSystemMXBean;
import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Closeables;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FailureInjector;
import io.strata.common.NsChunkId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import static io.strata.common.Checks.checkedAdd;
import static io.strata.common.Fsync.forceDirectory;
import static io.strata.format.ChunkFormats.DATA_START;
import static io.strata.format.ChunkFormats.HEADER_SIZE;
import static io.strata.format.ChunkFormats.TRAILER_SIZE;
import static io.strata.format.ChunkFormats.readFully;
import static io.strata.format.ChunkFormats.writeFully;

/**
 * Node-local chunk engine (tech design §5, §11): epoch-fenced appends, integrity-ledger crash
 * recovery, seal with node-computed CRC_RANGES/STATS, sealed-chunk import for repair.
 *
 * All mutations are serialized per chunk handle by a per-{@code Handle} {@link ReentrantLock}
 * (NOT the intrinsic monitor: critical sections block on FileChannel I/O and the group-commit
 * flusher join, and on Java 21 a virtual thread that blocks while holding {@code synchronized}
 * pins its carrier — ReentrantLock unmounts cleanly). The data region is raw logical bytes —
 * this engine never parses payload content (invariant §14.10), not even during recovery.
 */
public final class ChunkStore implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);

    private static final long MAX_IMPORT_FOOTER_BYTES = 64L * 1024 * 1024;

    private static final int RECOVERY_FENCE_REQUIRED = Integer.MAX_VALUE;

    private final Path dir;
    private final Map<NsChunkId, Handle> chunks = new ConcurrentHashMap<>();
    private final ChannelCache channelCache;
    private final Set<NsChunkId> creating = ConcurrentHashMap.newKeySet();
    private final AtomicLong forceCount =
            new AtomicLong();
    private final AtomicLong appendOps =
            new AtomicLong();
    private final AtomicLong appendBytes =
            new AtomicLong();
    private final AtomicLong readOps =
            new AtomicLong();
    private final AtomicLong readBytes =
            new AtomicLong();
    // Per-namespace [appendOps, appendBytes, readOps, readBytes] for the namespace dashboard. A map of
    // LongAdder quads — the data-path cost is one lock-free lookup + adder increment. The global counters
    // above stay as the cluster rollup; ServerMetrics exports the per-namespace view and derives the fleet
    // line as sum without(namespace).
    private final ConcurrentHashMap<String, LongAdder[]> nsIo =
            new ConcurrentHashMap<>();
    private final AtomicLong backgroundFlushes =
            new AtomicLong();
    private final AtomicLong sealedLedgerReclaims =
            new AtomicLong();

    // Background writeback: a daemon periodically fsyncs OPEN, non-ack-on-fsync chunks that have
    // accumulated enough new data since their last flush, so the dirty-page backlog never grows to a
    // whole chunk. Best-effort, decoupled from the append/ack path. When seal fsync is enabled, this
    // keeps seal-time fsync small; with the default Kafka-like seal fsync disabled, it still prevents
    // unbounded dirty-page buildup from later stalling ordinary append writes.
    //
    // Both knobs are tunable (system property, then env, else default) so a deployment can trade fsync
    // syscall rate against seal-time fsync size: a shorter interval / smaller threshold keeps less dirty
    // data per open chunk, so a synchronized roll does not stampede the disk's fsync queue with many
    // large concurrent forces. Background writeback keeps the original 500ms / 4 MiB default.
    // STRATA_SEAL_FSYNC (default false) gates the best-effort, off-the-ack-path fsyncs at open, seal,
    // and delete (header/sidecar/dir plus the seal-time data force) — NOT just seal. It does not affect
    // ack durability (the group committer always forces data+ledger before acking on fsyncOnAck files),
    // and even with it off a sealed chunk's ledger is retained until reclaimSealedLedgersOnce() forces
    // the SEALED state durable, so recovery never discards acknowledged data.
    private static final boolean SEAL_FSYNC_DEFAULT =
            booleanConf("strata.seal.fsync", "STRATA_SEAL_FSYNC", false);
    private static final long BG_FLUSH_INTERVAL_MS =
            longConf("strata.bgFlush.intervalMs", "STRATA_BG_FLUSH_INTERVAL_MS", 500);
    private static final long BG_FLUSH_THRESHOLD_BYTES =
            longConf("strata.bgFlush.thresholdBytes", "STRATA_BG_FLUSH_THRESHOLD_BYTES", 4L << 20); // 4 MiB
    private static final long SLOW_APPEND_LOG_NANOS = TimeUnit.MILLISECONDS.toNanos(
            longConf("strata.slowAppendLogMs", "STRATA_SLOW_APPEND_LOG_MS", 1_000));
    private static final long SLOW_MUTATION_LOG_NANOS = TimeUnit.MILLISECONDS.toNanos(
            longConf("strata.slowMutationLogMs", "STRATA_SLOW_MUTATION_LOG_MS", 500));
    private static final long CHANNEL_CACHE_MAX_SIZE =
            longConf("strata.fileChannelCache.maxSize", "STRATA_FILE_CHANNEL_CACHE_MAX_SIZE",
                    defaultChannelCacheCapacity());
    /** Whether seal/open/delete force their metadata + data to disk synchronously. Defaults from
     *  STRATA_SEAL_FSYNC; overridable per instance so tests can exercise both durability paths. */
    private final boolean sealFsync;
    private final ChunkStoreConfig csConfig;
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
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "true", "1", "yes", "y", "on" -> true;
            case "false", "0", "no", "n", "off" -> false;
            default -> {
                log.warn("ignoring non-boolean {}/{}='{}', using default {}", property, env, raw, def);
                yield def;
            }
        };
    }

    /**
     * Default sealed-chunk channel-cache capacity: derived from the soft RLIMIT_NOFILE minus headroom
     * for pinned OPEN channels, ledgers, sockets, and in-flight transient FDs. Falls back to a fixed
     * default on non-Unix / non-HotSpot JVMs where the FD limit is not introspectable.
     */
    static int defaultChannelCacheCapacity() {
        long max = -1;
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof UnixOperatingSystemMXBean unix) {
            max = unix.getMaxFileDescriptorCount();
        }
        if (max <= 0) {
            return 1024; // non-Unix fallback
        }
        long headroom = Math.max(256, max / 4);
        long cap = max - headroom;
        return (int) Math.max(128, Math.min(cap, Integer.MAX_VALUE));
    }

    /** Live process open file-descriptor count for observability; {@code -1} when unavailable. */
    public long openFds() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof UnixOperatingSystemMXBean unix) {
            return unix.getOpenFileDescriptorCount();
        }
        return -1;
    }

    public ChunkStore(Path dir) throws IOException {
        this(dir, SEAL_FSYNC_DEFAULT);
    }

    public ChunkStore(Path dir, ChunkStoreConfig csConfig) throws IOException {
        this(dir, SEAL_FSYNC_DEFAULT, (int) CHANNEL_CACHE_MAX_SIZE, csConfig);
    }

    /** Package-private: lets tests exercise both the seal-fsync-on and -off durability paths. */
    ChunkStore(Path dir, boolean sealFsync) throws IOException {
        this(dir, sealFsync, (int) CHANNEL_CACHE_MAX_SIZE, ChunkStoreConfig.DEFAULT);
    }

    /**
     * Package-private: lets tests specify a small channel cache capacity to exercise eviction
     * without creating thousands of files (avoids static-final class-load timing coupling).
     */
    ChunkStore(Path dir, boolean sealFsync, int channelCacheCapacity) throws IOException {
        this(dir, sealFsync, channelCacheCapacity, ChunkStoreConfig.DEFAULT);
    }

    ChunkStore(Path dir, boolean sealFsync, int channelCacheCapacity, ChunkStoreConfig csConfig) throws IOException {
        this.dir = dir;
        this.sealFsync = sealFsync;
        this.csConfig = csConfig;
        this.channelCache = new ChannelCache(channelCacheCapacity);
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
                BG_FLUSH_INTERVAL_MS, BG_FLUSH_THRESHOLD_BYTES, sealFsync);
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

    private LongAdder[] nsIoFor(StrataNamespace ns) {
        return nsIo.computeIfAbsent(ns.value(), k -> new LongAdder[]{
                new LongAdder(), new LongAdder(),
                new LongAdder(), new LongAdder()});
    }

    /** Per-namespace [appendOps, appendBytes, readOps, readBytes] snapshot for the namespace dashboard. */
    public Map<String, long[]> namespaceIoStats() {
        Map<String, long[]> out = new HashMap<>(nsIo.size());
        nsIo.forEach((ns, a) -> out.put(ns, new long[]{a[0].sum(), a[1].sum(), a[2].sum(), a[3].sum()}));
        return out;
    }

    /** Namespaces that have seen I/O — drives lazy per-namespace meter registration (no snapshot alloc). */
    public Set<String> ioNamespaces() {
        return nsIo.keySet();
    }

    /** One per-namespace I/O counter (index: 0 appendOps, 1 appendBytes, 2 readOps, 3 readBytes); 0 if absent.
     *  O(1) — bound per Micrometer FunctionCounter so each scrape is a single {@code LongAdder.sum()}. */
    public long ioValue(String namespace, int index) {
        LongAdder[] a = nsIo.get(namespace);
        return a == null ? 0L : a[index].sum();
    }

    /** Open (being-written) chunk count — best-effort snapshot for observability. */
    public int openChunks() {
        return countByState(ChunkState.OPEN);
    }

    /** Sealed (immutable) chunk count — best-effort snapshot for observability. */
    public int sealedChunks() {
        return countByState(ChunkState.SEALED);
    }

    /** Sealed-chunk channel-cache hits (served an already-open FD) — observability. */
    public long channelCacheHits() { return channelCache.hits(); }

    /** Sealed-chunk channel-cache misses (opened a new FD) — observability. */
    public long channelCacheMisses() { return channelCache.misses(); }

    /** Sealed-chunk channel-cache evictions (closed an idle FD over capacity) — observability. */
    public long channelCacheEvictions() { return channelCache.evictions(); }

    /** Currently-open cached sealed-chunk channels — observability. */
    public int cachedChannels() { return channelCache.size(); }

    /** Configured channel-cache capacity — observability. */
    public int channelCacheCapacity() { return channelCache.capacity(); }

    private int countByState(ChunkState state) {
        int n = 0;
        for (Handle h : chunks.values()) {
            h.lock.lock();
            try {
                if (h.state == state) n++;
            } finally {
                h.lock.unlock();
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
     * SEALED sidecar, and only THEN unlink the ledger. The data force runs outside the chunk lock
     * (it is slow); the lock is held only to snapshot and to finish. Package-private so tests can
     * drive a round deterministically.
     */
    void reclaimSealedLedgersOnce() {
        for (Handle h : chunks.values()) {
            FileChannel data;
            h.lock.lock();
            try {
                if (h.state != ChunkState.SEALED || !h.sealedLedgerPending) {
                    continue;
                }
                data = h.data;
            } finally {
                h.lock.unlock();
            }
            try {
                data.force(false); // footer/trailer durable before the sidecar may claim SEALED on disk
            } catch (IOException | RuntimeException e) {
                h.lock.lock();
                try {
                    if (chunks.get(h.nsKey) != h || h.data != data || !h.sealedLedgerPending) {
                        continue; // delete()/close() won the race after we snapshotted — not ours to log
                    }
                } finally {
                    h.lock.unlock();
                }
                log.warn("sealed-ledger reclaim of {} failed to force data (will retry)", h.id, e);
                continue;
            }
            boolean reclaimed = false;
            try {
                h.lock.lock();
                try {
                    if (chunks.get(h.nsKey) != h || h.data != data
                            || h.state != ChunkState.SEALED || !h.sealedLedgerPending) {
                        continue; // superseded after the force — leave it to the winner
                    }
                    // Durability-v2 (Lever 1): the trailer (forced durable above) is the SEALED signal.
                    // Drop the retained ledger and the now-stale OPEN sidecar; with no ledger, recovery
                    // classifies SEALED from the trailer alone (unambiguous).
                    Files.deleteIfExists(h.ledgerPath);
                    Files.deleteIfExists(h.metaPath);
                    h.sealedLedgerPending = false;
                    closeAndNullData(h);                // release the writable FD; reads use the cache
                    reclaimed = true;
                } finally {
                    h.lock.unlock();
                }
                forceDirectory(h.shardDir); // make the unlink durable (recovery's SEALED branch re-deletes otherwise)
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
     * later seal must flush. The force runs OUTSIDE the chunk lock (it is slow and safe to run
     * concurrently with positional appends); the lock is held only to read state and record
     * progress. Package-private so tests can drive a round deterministically.
     */
    void backgroundFlushOnce() {
        for (Handle h : chunks.values()) {
            long flushTo;
            FileChannel data;
            h.lock.lock();
            try {
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
            } finally {
                h.lock.unlock();
            }
            try {
                data.force(false); // flushes all currently-dirty pages (>= flushTo)
            } catch (IOException | RuntimeException e) {
                h.lock.lock();
                try {
                    if (chunks.get(h.nsKey) != h || h.state != ChunkState.OPEN || h.data != data) {
                        // delete()/close()/seal() won the race after we snapshotted the channel.
                        // The handle is no longer eligible for background writeback, so do not log
                        // and do not retry this stale closed channel every period.
                        continue;
                    }
                } finally {
                    h.lock.unlock();
                }
                log.warn("background writeback of {} failed (will retry)", h.id, e);
                continue;
            }
            boolean credited = false;
            h.lock.lock();
            try {
                if (chunks.get(h.nsKey) == h && h.state == ChunkState.OPEN
                        && h.data == data && h.bgFlushedOffset < flushTo) {
                    h.bgFlushedOffset = flushTo;
                    credited = true;
                }
            } finally {
                h.lock.unlock();
            }
            if (credited) {
                backgroundFlushes.incrementAndGet();
            }
        }
    }

    /* ---------------- per-chunk state ---------------- */

    private final class Handle {
        // Serializes all access to this chunk's mutable state. Deliberately a ReentrantLock, NOT the
        // intrinsic monitor: critical sections block on FileChannel I/O (write/force/open/truncate) and
        // on the group-commit flusher join, and on Java 21 a virtual thread that blocks while holding
        // `synchronized` pins its carrier. ReentrantLock unmounts cleanly; no wait/notify is used.
        final ReentrantLock lock = new ReentrantLock();
        final ChunkId id;
        final StrataNamespace ns;
        final NsChunkId nsKey;  // pre-computed map key — avoids per-iteration allocation in background loops
        final ChunkFormats.Header header;
        final Path shardDir;   // dir/<ns>/<l1>/<l2> — durability target for chunk-file mutations
        final Path dataPath;
        final Path metaPath;
        final Path ledgerPath;
        FileChannel data;
        IntegrityLedger ledger; // null once sealed
        GroupCommitter committer; // non-null only for OPEN ack-on-fsync chunks
        // Set while seal() stops the committer OFF the chunk lock (the up-to-12s flusher join). The
        // state is still OPEN during that window, so this flag is what makes the chunk un-appendable:
        // appendAsync rejects when it is set, so no append can advance end / write bytes that the
        // in-flight seal would then finalize inconsistently. A reversible in-memory flag, deliberately NOT
        // a new ChunkState: a failed or lost-race seal must restore the chunk to clean OPEN+committer-running
        // (delete() instead blocks appends with the *terminal* DELETING state). Never persisted.
        boolean sealing;
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
        // Last time an owner attested this replica via VERIFY_CHUNKS (design §20.3); seeded to when this
        // node first learned of the chunk so a freshly-created/recovered chunk gets the full orphan grace
        // (§20.4) before it can be considered a suspect. In-memory only: a restart re-earns verification.
        volatile long lastVerifiedAtMs = System.currentTimeMillis();

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
         * lock; {@code payload}'s position/limit are left untouched.
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

        Handle(ChunkId id, ChunkFormats.Header header, StrataNamespace ns) {
            this.id = id;
            this.ns = ns;
            this.nsKey = new NsChunkId(ns, id);
            this.header = header;
            String rel = ChunkFormats.chunkRelativePath(ns, id);
            Path chunkFile = dir.resolve(rel + ".chunk");
            this.shardDir = chunkFile.getParent();
            this.dataPath = chunkFile;
            this.metaPath = dir.resolve(rel + ".meta");
            this.ledgerPath = dir.resolve(rel + ".j");
        }

        /**
         * Path-based constructor for recovery: Task 7 supplies the exact chunk file path discovered
         * during the namespace-aware directory walk; the paths are derived directly from the given
         * dataPath rather than recomputed from a namespace, avoiding a second path-encoding step.
         */
        Handle(ChunkId id, ChunkFormats.Header header, Path dataPath, StrataNamespace ns) {
            this.id = id;
            this.ns = ns;
            this.nsKey = new NsChunkId(ns, id);
            this.header = header;
            this.dataPath = dataPath;
            this.shardDir = dataPath.getParent();
            String baseName = ChunkFormats.baseName(id);
            this.metaPath = shardDir.resolve(baseName + ".meta");
            this.ledgerPath = shardDir.resolve(baseName + ".j");
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
                forceDirectory(shardDir);
            }
        }

        void startCommitterIfFsync(AtomicLong counter) {
            if (header.fsyncOnAck()) {
                committer = new GroupCommitter(id.toString(), () -> {
                    // both must be durable before acking; either alone is safe for recovery
                    data.force(false);
                    ledger.force();
                }, counter,
                csConfig.groupCommitDrainTimeoutMs(),
                csConfig.groupCommitMinAccumulationNanos(),
                csConfig.groupCommitMaxAccumulationNanos());
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

    private Handle lookup(StrataNamespace ns, ChunkId id) {
        Handle h = chunks.get(new NsChunkId(ns, id));
        if (h == null) throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, id.toString());
        return h;
    }

    private void reserveNewChunk(StrataNamespace ns, ChunkId id) {
        NsChunkId key = new NsChunkId(ns, id);
        if (!creating.add(key)) throw chunkAlreadyExists(id);
        if (chunks.containsKey(key)) {
            creating.remove(key);
            throw chunkAlreadyExists(id);
        }
    }

    private void releaseReservation(StrataNamespace ns, ChunkId id) {
        creating.remove(new NsChunkId(ns, id));
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

    public void open(StrataNamespace ns, ChunkId id, boolean fsyncOnAck, int writeEpoch, long createdAtMs)
            throws IOException {
        long t0 = System.nanoTime();
        long tChannelOpen = t0;
        long tHeaderWrite = t0;
        long tDataForce = t0;
        long tDataDirForce = t0;
        long tLedgerCreate = t0;
        long tLedgerDirForce = t0;
        long tSidecar = t0;
        long tInstall = t0;
        reserveNewChunk(ns, id);
        Handle h = null;
        boolean dataCreated = false;
        boolean ledgerOwned = false;
        boolean metaOwned = false;
        boolean installed = false;
        try {
            ChunkFormats.Header header = new ChunkFormats.Header(id, fsyncOnAck, writeEpoch, createdAtMs, 0, 0, 0);
            h = new Handle(id, header, ns);
            Files.createDirectories(h.shardDir);
            if (sealFsync) {
                forceDirectory(h.shardDir);
            }
            if (Files.exists(h.dataPath)) throw chunkAlreadyExists(id);
            boolean ledgerPreexisted = Files.exists(h.ledgerPath);
            boolean metaPreexisted = Files.exists(h.metaPath);
            h.data = FileChannel.open(h.dataPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            tChannelOpen = System.nanoTime();
            dataCreated = true;
            writeFully(h.data, ByteBuffer.wrap(header.encode()), 0);
            tHeaderWrite = System.nanoTime();
            if (sealFsync) {
                h.data.force(true);
            }
            tDataForce = System.nanoTime();
            if (sealFsync) {
                forceDirectory(h.shardDir);
            }
            tDataDirForce = System.nanoTime();
            ledgerOwned = !ledgerPreexisted;
            h.ledger = IntegrityLedger.create(h.ledgerPath);
            tLedgerCreate = System.nanoTime();
            if (sealFsync) {
                forceDirectory(h.shardDir);
            }
            tLedgerDirForce = System.nanoTime();
            h.state = ChunkState.OPEN;
            h.end = 0;
            h.writeEpoch = writeEpoch;
            h.fenceEpoch = -1;
            h.lastKnownDO = 0;
            metaOwned = !metaPreexisted;
            h.persistSidecar(sealFsync);
            tSidecar = System.nanoTime();
            h.startCommitterIfFsync(forceCount);
            chunks.put(new NsChunkId(ns, id), h);
            tInstall = System.nanoTime();
            if (tInstall - t0 > SLOW_MUTATION_LOG_NANOS) {
                log.info("slow open {} ns={} fsyncOnAck={} phases(ms): dataOpen={} headerWrite={} dataFsync={} "
                                + "dataDirFsync={} ledgerCreate={} ledgerDirFsync={} sidecarPersist={} total={}",
                        id, ns, fsyncOnAck, msBetween(t0, tChannelOpen), msBetween(tChannelOpen, tHeaderWrite),
                        sealFsync ? msBetween(tHeaderWrite, tDataForce) : "-1.0",
                        sealFsync ? msBetween(tDataForce, tDataDirForce) : "-1.0",
                        msBetween(tDataDirForce, tLedgerCreate),
                        sealFsync ? msBetween(tLedgerCreate, tLedgerDirForce) : "-1.0",
                        msBetween(tLedgerDirForce, tSidecar), msBetween(t0, tInstall));
            }
            installed = true;
        } finally {
            if (!installed && h != null) {
                cleanupFailedOpen(h, dataCreated, ledgerOwned, metaOwned);
            }
            releaseReservation(ns, id);
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
        deleteOwnedPath(dataCreated, h.dataPath, h.shardDir, "incomplete chunk data");
        deleteOwnedPath(ledgerOwned, h.ledgerPath, h.shardDir, "incomplete chunk ledger");
        deleteOwnedPath(metaOwned, h.metaPath, h.shardDir, "incomplete chunk sidecar");
    }

    private void deleteOwnedPath(boolean owned, Path path, Path containingDir, String description) {
        if (!owned) return;
        try {
            Files.deleteIfExists(path);
            if (sealFsync) {
                forceDirectory(containingDir);
            }
        } catch (IOException e) {
            log.warn("failed to delete {} {}", description, path, e);
        }
    }

    private static void closeQuietly(FileChannel ch) {
        if (ch == null) return;
        try {
            ch.close();
        } catch (IOException e) {
            log.warn("failed to close transient read channel", e);
        }
    }

    /** Closes and nulls a sealed Handle's writable data channel under its lock. Caller holds the lock. */
    private void closeAndNullData(Handle h) {
        if (h.data != null) {
            try {
                h.data.close();
            } catch (IOException e) {
                log.warn("failed to close writable data channel for sealed chunk {}", h.id, e);
            }
            h.data = null;
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
    public CompletableFuture<AppendResult> appendAsync(
            StrataNamespace ns, ChunkId id, int epoch, long baseOffset, long durableOffset,
            ByteBuffer payload, int payloadCrc) throws IOException {
        Handle h = lookup(ns, id);
        // payloadCrc is the writer's CRC32C over this payload, already verified by the frame decoder;
        // the node stores it as the per-record digest and never originates its own (no node-side CRC
        // pass on this path; the convenience overload below computes one for callers that lack a digest).
        long t0 = System.nanoTime();
        long newEnd;
        GroupCommitter committer;
        int len;
        long tBeforeLock = System.nanoTime();
        long tLock = tBeforeLock;
        long tWrite = tBeforeLock;
        long tLedger = tBeforeLock;
        long tRunningCrc = tBeforeLock;
        long tUnlock;
        h.lock.lock();
        try {
            tLock = System.nanoTime();
            // fence check dominates the state check: a deposed writer must learn FENCED_EPOCH
            // (permanent death), never CHUNK_SEALED (which reads as "roll and continue")
            checkEpoch(h, epoch);
            // h.sealing: a seal is finalizing this chunk with the lock released for its committer stop;
            // reject as if already sealed so no append slips into that window (see Handle.sealing).
            if (h.state != ChunkState.OPEN || h.sealing) throw new ScpException(ErrorCode.CHUNK_SEALED, id.toString());
            h.writeEpoch = Math.max(h.writeEpoch, epoch);
            if (baseOffset != h.end) {
                throw new ScpException(ErrorCode.OFFSET_GAP, "expected " + h.end + " got " + baseOffset, h.end);
            }
            h.lastKnownDO = Math.max(h.lastKnownDO, Math.min(durableOffset, h.end));
            len = payload.remaining();
            if (len == 0) {
                return CompletableFuture.completedFuture(new AppendResult(h.end)); // DO beacon
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
            // crcAccumulate is pure CPU under the lock — it preserves append order and cannot throw.
            h.crcAccumulate(payload);
            tRunningCrc = System.nanoTime();
            h.end = newEnd;
            appendOps.incrementAndGet();
            appendBytes.addAndGet(len);
            var nsCounters = nsIoFor(ns);
            nsCounters[0].increment();
            nsCounters[1].add(len);
            committer = h.committer;
        } finally {
            h.lock.unlock();
        }
        tUnlock = System.nanoTime();
        if (tUnlock - t0 > SLOW_APPEND_LOG_NANOS) {
            log.info("slow append {} len={} base={} phases(ms): lockWait={} dataWrite={} "
                            + "ledgerAppend={} runningCrc={} lockHeld={} total={}",
                    id, len, baseOffset, msBetween(tBeforeLock, tLock),
                    msBetween(tLock, tWrite), msBetween(tWrite, tLedger), msBetween(tLedger, tRunningCrc),
                    msBetween(tLock, tRunningCrc), msBetween(t0, tUnlock));
        }
        if (committer == null) {
            return CompletableFuture.completedFuture(new AppendResult(newEnd));
        }
        long end = newEnd;
        return committer.awaitFlush(end).thenApply(v -> new AppendResult(end));
    }

    /** Convenience for tests/simple callers without a precomputed digest: computes it then delegates. */
    public CompletableFuture<AppendResult> appendAsync(
            StrataNamespace ns, ChunkId id, int epoch, long baseOffset, long durableOffset, ByteBuffer payload)
            throws IOException {
        return appendAsync(ns, id, epoch, baseOffset, durableOffset, payload,
                payload.hasRemaining() ? Crc.of(payload) : 0);
    }

    /** Synchronous convenience (tests, simple callers); production append path uses appendAsync. */
    public AppendResult append(StrataNamespace ns, ChunkId id, int epoch, long baseOffset, long durableOffset,
                               ByteBuffer payload) throws IOException {
        try {
            return appendAsync(ns, id, epoch, baseOffset, durableOffset, payload).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof ScpException se) throw se;
            throw new ScpException(ErrorCode.INTERNAL, String.valueOf(e.getCause()));
        }
    }

    public record ReadResult(byte[] bytes, long localEndOffset, long lastKnownDO) {}

    /**
     * A read response that is either a zero-copy file region or a heap snapshot of the bytes.
     * Client reads of open chunks are bounded by the replica-known durable high watermark and may
     * return a zero-copy channel region without per-read ledger verification. Open recovery reads can
     * include the undurable tail, so they are snapshotted before any concurrent seal-truncate can alter
     * the transferred payload.
     * Exactly one of {@code channel} / {@code bytes} is set for a non-empty result.
     */
    public record ReadRegionResult(FileChannel channel, long filePosition, int length, byte[] bytes,
                                   long localEndOffset, long lastKnownDO, Runnable releaser)
            implements AutoCloseable {
        /** Releases the underlying read resource: a cache lease (sealed) or the transient FD (open). */
        @Override
        public void close() {
            if (releaser != null) {
                releaser.run();
            }
        }
    }

    public ReadResult read(StrataNamespace ns, ChunkId id, long offset, int maxBytes) throws IOException {
        requireNonNegative(offset, "read offset");
        requireNonNegative(maxBytes, "read maxBytes");
        Handle h = lookup(ns, id);
        long sealedLength;
        List<Integer> rangeCrcs;
        long lastKnownDO;
        Path dataPath;
        h.lock.lock();
        try {
            if (h.state != ChunkState.SEALED) {
                long end = h.currentEnd();
                if (offset >= end) return new ReadResult(new byte[0], end, h.lastKnownDO);
                int n = (int) Math.min(Math.min(maxBytes, csConfig.maxRequestBytes()), end - offset);
                byte[] out = new byte[n];
                readOpenVerified(h, offset, out);
                return new ReadResult(out, end, h.lastKnownDO);
            }
            sealedLength = h.sealedLength;
            rangeCrcs = h.sealedRangeCrcs;
            lastKnownDO = h.lastKnownDO;
            dataPath = h.dataPath;
        } finally {
            h.lock.unlock();
        }
        if (offset >= sealedLength) return new ReadResult(new byte[0], sealedLength, lastKnownDO);
        int n = (int) Math.min(Math.min(maxBytes, csConfig.maxRequestBytes()), sealedLength - offset);
        byte[] out = new byte[n];
        try (ChannelCache.Lease lease = channelCache.acquire(h.nsKey, dataPath)) {
            readSealedVerified(lease.channel(), sealedLength, rangeCrcs, id, offset, out);
        }
        return new ReadResult(out, sealedLength, lastKnownDO);
    }

    public ReadRegionResult readRegion(StrataNamespace ns, ChunkId id, long offset, int maxBytes) throws IOException {
        return readRegion(ns, id, offset, maxBytes, false);
    }

    /**
     * Seal-recovery variant of {@link #readRegion}: serves locally-present bytes up to the chunk's
     * local end, INCLUDING the never-acked tail above the durable high watermark. Recovery must see
     * that tail to decide whether a quorum still holds it (tech design §7.3); clamping it away — as
     * the client read path does — makes recovery seal short and drop quorum-durable bytes. Reads are
     * still integrity-verified (open chunks against the ledger, sealed chunks against footer CRC
     * ranges), and the recovery path does not count toward client read throughput metrics.
     */
    public ReadRegionResult readRegionForRecovery(StrataNamespace ns, ChunkId id, long offset, int maxBytes) throws IOException {
        return readRegion(ns, id, offset, maxBytes, true);
    }

    private ReadRegionResult readRegion(StrataNamespace ns, ChunkId id, long offset, int maxBytes,
                                        boolean includeUndurableTail) throws IOException {
        requireNonNegative(offset, "read offset");
        requireNonNegative(maxBytes, "read maxBytes");
        Handle h = lookup(ns, id);
        boolean sealed;
        long localEnd;
        long lastKnownDO;
        long filePos;
        int n;
        Path dataPath;
        NsChunkId nsKey;
        long sealedLength = -1;
        List<Integer> sealedRangeCrcs = null;
        h.lock.lock();
        try {
            localEnd = h.currentEnd();
            lastKnownDO = h.lastKnownDO;
            long readableEnd = (h.state == ChunkState.SEALED || includeUndurableTail)
                    ? localEnd : Math.min(localEnd, lastKnownDO);
            if (offset >= readableEnd) {
                return new ReadRegionResult(null, 0, 0, null, localEnd, lastKnownDO, null);
            }
            n = (int) Math.min(Math.min(maxBytes, csConfig.maxRequestBytes()), readableEnd - offset);
            if (n == 0) {
                return new ReadRegionResult(null, 0, 0, null, localEnd, lastKnownDO, null);
            }
            if (!includeUndurableTail) {
                // observability: count client READ bytes served (mirrors append counters; drives read
                // throughput). Recovery reads are internal control-plane traffic, not client reads.
                readOps.incrementAndGet();
                readBytes.addAndGet(n);
                var nsCounters = nsIoFor(ns);
                nsCounters[2].increment();
                nsCounters[3].add(n);
            }
            filePos = checkedAdd(DATA_START, offset, "chunk file offset");
            if (h.state != ChunkState.SEALED && includeUndurableTail) {
                // Recovery reads may inspect bytes above lastKnownDO. Keep them materialized and verified
                // UNDER the chunk lock because that tail can be truncated by a later seal.
                byte[] out = new byte[n];
                readOpenVerified(h, offset, out);
                return new ReadRegionResult(null, 0, n, out, localEnd, lastKnownDO, null);
            }
            if (h.state == ChunkState.SEALED && includeUndurableTail) {
                sealedLength = h.sealedLength;
                sealedRangeCrcs = h.sealedRangeCrcs;
            }
            // Snapshot what the off-lock open needs. The bytes we will expose are stable once the lock is
            // released: an OPEN client read is clamped to lastKnownDO (which no concurrent seal can truncate
            // below), and a SEALED chunk's data region is immutable. So the blocking open/acquire below need
            // not hold the chunk lock — keeping it off-lock stops a blocking open from serializing every
            // other op on this chunk.
            sealed = h.state == ChunkState.SEALED;
            dataPath = h.dataPath;
            nsKey = h.nsKey;
        } finally {
            h.lock.unlock();
        }
        // Test seam: lets a test delete + re-import this id in the window between the lock release and the
        // off-lock open/acquire below, to exercise the post-open handle revalidation.
        FailureInjector.point("format.readRegion.beforeOpen");
        if (sealed && includeUndurableTail) {
            // Recovery can promote a sealed donor's bytes into the committed replica set, so it must not
            // use the client zero-copy sealed fast path that relies on deferred scrub for rot detection.
            byte[] out = new byte[n];
            try (ChannelCache.Lease lease = channelCache.acquire(nsKey, dataPath)) {
                requireCurrentHandle(h, nsKey, id);
                readSealedVerified(lease.channel(), sealedLength, sealedRangeCrcs, id, offset, out);
            }
            return new ReadRegionResult(null, 0, n, out, localEnd, lastKnownDO, null);
        }
        if (!sealed) {
            // OPEN client fast path: independent transient READ FD, opened OFF the chunk lock. We do NOT
            // re-run readOpenVerified here (it would materialize + CRC the range, removing the zero-copy
            // throughput benefit); read-time rot detection in the durable prefix is deferred to the verified
            // read()/READ_RECOVERY paths, crash recovery, and sealed scrub. A concurrent delete may unlink
            // the file first → open throws the expected delete/read race; clients retry. The FD is released
            // by closing it; the transport streams the region via sendfile.
            FileChannel readChannel = FileChannel.open(dataPath, StandardOpenOption.READ);
            try {
                requireCurrentHandle(h, nsKey, id);
                return new ReadRegionResult(readChannel, filePos, n, null, localEnd, lastKnownDO,
                        () -> closeQuietly(readChannel));
            } catch (RuntimeException e) {
                closeQuietly(readChannel);
                throw e;
            }
        }
        // SEALED client fast path: the data region is immutable (no further append/seal/truncate), so hand
        // back a zero-copy region the transport streams via sendfile. We do NOT re-verify range CRCs per
        // client read here: that forced a full CRC-range read + a 4 MiB allocation per request (~20x slower,
        // measured); integrity is covered by background scrub and the verified read()/fetch()/import paths.
        // The exclusive leased cache channel is acquired OFF the chunk lock (never shared across readers —
        // FileChannel is interruptible, so a shared FD would close for all on any reader's interrupt). A
        // concurrent delete only unlinks the path while this independent FD keeps the inode alive, so the
        // deferred transfer is safe. The lease is released (returning the channel to the idle pool) when
        // the response Frame closes.
        ChannelCache.Lease lease = channelCache.acquire(nsKey, dataPath);
        try {
            requireCurrentHandle(h, nsKey, id);
            return new ReadRegionResult(lease.channel(), filePos, n, null, localEnd, lastKnownDO,
                    lease::release);
        } catch (RuntimeException e) {
            lease.release();
            throw e;
        }
    }

    /**
     * Revalidates, after an off-lock open/acquire in {@link #readRegion}, that this handle is still the
     * mapped one for its id. A delete + re-import for the same {@link NsChunkId} (same data path) in the
     * window between the metadata snapshot and the open would otherwise bind the stale snapshot
     * (length/state) to the freshly-replaced physical file. A {@code Handle} is in the map iff it has not
     * been deleted, so an identity mismatch means the snapshot is stale: fail with CHUNK_NOT_FOUND and let
     * the client retry — the same contract as opening an already-unlinked file. The verified read() path
     * needs no equivalent: it CRC-checks the bytes against the snapshot, so a replaced file fails the check.
     */
    private void requireCurrentHandle(Handle h, NsChunkId nsKey, ChunkId id) {
        if (chunks.get(nsKey) != h) {
            throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, id.toString());
        }
    }

    public record FenceResult(int persistedFenceEpoch, long localEndOffset, long lastKnownDO, ChunkState state) {}

    public FenceResult fence(StrataNamespace ns, ChunkId id, int fenceEpoch) throws IOException {
        Handle h = lookup(ns, id);
        h.lock.lock();
        try {
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
        } finally {
            h.lock.unlock();
        }
    }

    public record StatResult(ChunkState state, long localEndOffset, long lastKnownDO, int writeEpoch,
                             int fenceEpoch, long sealedLength, int dataCrc) {}

    public StatResult stat(StrataNamespace ns, ChunkId id) {
        Handle h = lookup(ns, id);
        h.lock.lock();
        try {
            return new StatResult(h.state, h.currentEnd(), h.lastKnownDO, h.writeEpoch, h.fenceEpoch,
                    h.sealedLength, h.dataCrc);
        } finally {
            h.lock.unlock();
        }
    }

    public record SealResult(long finalLength, int dataCrc) {}

    /**
     * Seals at dataLength (must be <= current end; shorter means truncate the never-acked tail).
     * callerSections, if non-empty, is a pre-encoded section list: u32 count + section bytes.
     */
    public SealResult seal(StrataNamespace ns, ChunkId id, int epoch, long dataLength, ByteBuffer callerSections) throws IOException {
        // a negative length would pass the > end check and truncate(DATA_START + negative)
        // destroys the chunk HEADER before anything throws — reject at the boundary
        requireNonNegative(dataLength, "seal dataLength");
        Handle h = lookup(ns, id);
        GroupCommitter committerToStop = null;
        SealPrep prep = null;
        h.lock.lock();
        try {
            checkEpoch(h, epoch);
            if (h.state == ChunkState.SEALED) {
                if (h.sealedLength == dataLength) return new SealResult(h.sealedLength, h.dataCrc); // idempotent
                throw new ScpException(ErrorCode.CHUNK_SEALED, "sealed at " + h.sealedLength, h.sealedLength);
            }
            if (dataLength > h.end) {
                throw new ScpException(ErrorCode.INTERNAL, "seal beyond end: " + dataLength + " > " + h.end);
            }
            // Load-bearing invariant: a seal may never floor below the durable high watermark. The
            // zero-copy open-read fast path (readRegion) hands back a bare file region clamped to
            // lastKnownDO without re-snapshotting under the lock; that is only safe because a concurrent
            // seal can never truncate the data out from under [0, lastKnownDO). Recovery relies on it too
            // (it must not drop quorum-durable bytes). Do not relax without revisiting both.
            if (dataLength < h.lastKnownDO) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "seal below durable watermark: " + dataLength + " < " + h.lastKnownDO, h.lastKnownDO);
            }
            if (h.state == ChunkState.DELETING) {
                // a concurrent delete is tearing this chunk down; it is no longer sealable
                throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, id.toString());
            }
            if (h.sealing) {
                // another seal is finalizing this chunk in its off-lock committer-stop window; reject so
                // two sealers never both own it. A retry will observe SEALED and get the idempotent result.
                throw new ScpException(ErrorCode.CHUNK_SEALED, "seal already in progress for " + id);
            }
            // Validate the caller footer + snapshot CRCs BEFORE stopping the committer: a caller-validation
            // or read-verification failure must leave an OPEN ack-on-fsync chunk with its committer running.
            prep = prepareSealLocked(h, callerSections, dataLength);
            committerToStop = h.committer;
            if (committerToStop == null) {
                return finalizeSealLocked(h, id, dataLength, prep); // ack-on-replicate: nothing to drain off-lock
            }
            h.sealing = true; // enter the off-lock committer-stop window
        } finally {
            h.lock.unlock();
        }
        // Phase B (off the chunk lock): drain + join the committer — the up-to-12s pole — so other ops on
        // this chunk are not blocked, and no carrier is held, while a degraded fsync drains.
        boolean confirmed = committerToStop.closeAndConfirm();
        boolean poisoned = confirmed && committerToStop.isPoisoned();
        h.lock.lock();
        try {
            if (!confirmed) {
                h.sealing = false;
                throw new IOException("group-commit flusher stuck for " + id + " — refusing to seal");
            }
            if (poisoned) {
                h.sealing = false;
                throw new IOException("group-commit flusher failed for " + id + " — refusing to seal");
            }
            if (h.state == ChunkState.DELETING || chunks.get(h.nsKey) != h) {
                // a concurrent delete won the released-lock window; it owns teardown. Abort the seal.
                h.sealing = false;
                throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, id.toString());
            }
            h.committer = null; // confirmed stopped above; detach before mutating files
            try {
                return finalizeSealLocked(h, id, dataLength, prep);
            } finally {
                h.sealing = false;
            }
        } finally {
            h.lock.unlock();
        }
    }

    private record SealPrep(CallerSections caller, CrcScan scan, int ledgerEntryCount) {}

    /**
     * Validates the caller footer and snapshots the seal CRCs + ledger count under the chunk lock,
     * BEFORE the committer is stopped — a caller-validation or read-verification failure here must
     * leave an OPEN ack-on-fsync chunk with its committer still running.
     */
    private SealPrep prepareSealLocked(Handle h, ByteBuffer callerSections, long dataLength) throws IOException {
        CallerSections caller = readCallerSections(callerSections);
        CrcScan scan = dataLength == h.end ? h.snapshotRunningCrcs() : scanDataCrcs(h.data, dataLength);
        int ledgerEntryCount = ledgerEntriesThrough(h.ledger, dataLength);
        return new SealPrep(caller, scan, ledgerEntryCount);
    }

    /**
     * Finalizes a seal with the chunk lock held and the committer already stopped/absent: truncate the
     * never-acked tail, write footer+trailer, optional fsync, publish SEALED.
     */
    private SealResult finalizeSealLocked(Handle h, ChunkId id, long dataLength, SealPrep prep) throws IOException {
        int callerCount = prep.caller().count();
        byte[] callerBytes = prep.caller().bytes();
        CrcScan scan = prep.scan();
        int ledgerEntryCount = prep.ledgerEntryCount();
        long t0 = System.nanoTime();
        // The committer is already stopped by the caller (off the lock for ack-on-fsync, or it never
        // existed for ack-on-replicate), so no flusher can race the truncate/footer writes below.
        long tCommitter = t0;
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
        if (sealFsync) {
            h.data.force(false);
        }
        long tForce = System.nanoTime();

        h.state = ChunkState.SEALED;
        h.sealedLength = dataLength;
        h.dataCrc = scan.dataCrc;
        h.sealedRangeCrcs = List.copyOf(scan.rangeCrcs);
        h.lastKnownDO = dataLength;
        // Durability-v2 (Lever 1): the SEALED state is recovered from the trailer, not a sidecar, so we
        // no longer write a SEALED .meta. The OPEN sidecar created at open() stays as the pre-reclaim
        // recovery net (a crash before the trailer is durable rebuilds OPEN from the retained ledger) and
        // is unlinked once durably sealed — below for fsync, in reclaimSealedLedgersOnce() for the non-fsync net.
        long tStateUpdated = System.nanoTime();
        h.ledger.close();
        long tLedgerClose = System.nanoTime();
        Path ledgerPath = h.ledgerPath;
        h.ledger = null;
        if (sealFsync) {
            // data was forced durable above, so the SEALED state is recoverable from the trailer without
            // the ledger — drop both the ledger and the now-stale OPEN sidecar.
            deleteLedgerAsync(id, ledgerPath);
            Files.deleteIfExists(h.metaPath);
            forceDirectory(h.shardDir); // make the sidecar unlink durable
            // sealed + durable: release the writable FD; reads now go through the channel cache.
            closeAndNullData(h);
        } else {
            // SEAL_FSYNC=false left the footer/trailer only in the page cache. The OPEN sidecar plus the
            // retained ledger are the recovery net until reclaimSealedLedgersOnce() forces the trailer
            // durable and unlinks both. Deleting the ledger here loses acknowledged data on a crash before
            // the trailer is durable (C1).
            h.sealedLedgerPending = true;
        }
        long tLedgerDeleteEnqueue = System.nanoTime();
        if (tLedgerDeleteEnqueue - t0 > SLOW_MUTATION_LOG_NANOS) {
            log.info("slow seal {} len={}MiB phases(ms): stopCommitter={} "
                            + "truncate={} footerBuild={} footerWrite={} dataFsync={} "
                            + "ledgerClose={} ledgerDeleteEnqueue={} total={}",
                    id, dataLength >> 20, msBetween(t0, tCommitter), msBetween(tCommitter, tTruncate),
                    msBetween(tTruncate, tFooterBuild), msBetween(tFooterBuild, tWrite),
                    sealFsync ? msBetween(tWrite, tForce) : "-1.0",
                    msBetween(tStateUpdated, tLedgerClose), msBetween(tLedgerClose, tLedgerDeleteEnqueue),
                    msBetween(t0, tLedgerDeleteEnqueue));
        }
        return new SealResult(dataLength, scan.dataCrc);
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

    private CrcScan scanDataCrcs(FileChannel data, long dataLength) throws IOException {
        CRC32C whole = new CRC32C();
        List<Integer> ranges = new ArrayList<>();
        byte[] buf = new byte[1 << 20];
        long pos = 0;
        CRC32C range = new CRC32C();
        long rangeRemaining = ChunkFormats.CRC_RANGE_SIZE;
        while (pos < dataLength) {
            int n = (int) Math.min(buf.length, Math.min(dataLength - pos, rangeRemaining));
            ByteBuffer bb = ByteBuffer.wrap(buf, 0, n);
            readFully(data, bb, checkedAdd(DATA_START, pos, "chunk file offset"));
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

    public List<ChunkFormats.LedgerEntry> readLedger(StrataNamespace ns, ChunkId id, long fromOffset) {
        requireNonNegative(fromOffset, "ledger offset");
        Handle h = lookup(ns, id);
        h.lock.lock();
        try {
            if (h.ledger == null) return List.of();
            return h.ledger.entriesAfter(fromOffset);
        } finally {
            h.lock.unlock();
        }
    }

    public record FetchResult(long fileLength, ChunkState state, byte[] bytes) {}

    /** Raw file bytes (header + data + footer) — repair/relocation transfer. Sealed chunks only. */
    public FetchResult fetch(StrataNamespace ns, ChunkId id, long offset, int maxBytes) throws IOException {
        requireNonNegative(offset, "fetch offset");
        requireNonNegative(maxBytes, "fetch maxBytes");
        Handle h = lookup(ns, id);
        ChunkState state;
        Path dataPath;
        h.lock.lock();
        try {
            if (h.state != ChunkState.SEALED) {
                throw new ScpException(ErrorCode.INTERNAL, "fetch of non-sealed chunk " + id);
            }
            state = h.state;
            dataPath = h.dataPath;
        } finally {
            h.lock.unlock();
        }
        try (ChannelCache.Lease lease = channelCache.acquire(h.nsKey, dataPath)) {
            FileChannel data = lease.channel();
            long fileLen = data.size();
            if (offset >= fileLen) return new FetchResult(fileLen, state, new byte[0]);
            int n = (int) Math.min(Math.min(maxBytes, csConfig.maxRequestBytes()), fileLen - offset);
            byte[] out = new byte[n];
            readFully(data, ByteBuffer.wrap(out), offset);
            return new FetchResult(fileLen, state, out);
        }
    }

    /** Creates a temp file in the chunk store root for a streaming sealed-chunk import. */
    public Path createImportTemp(ChunkId id) throws IOException {
        Files.createDirectories(dir);
        return Files.createTempFile(dir, ChunkFormats.baseName(id) + ".", ".import");
    }

    /** Imports a sealed chunk from raw file bytes (tests/simple callers). Validates everything. */
    public void importSealed(StrataNamespace ns, ChunkId id, byte[] fileBytes,
                             long expectedLength, int expectedCrc) throws IOException {
        Path tmp = createImportTemp(id);
        try {
            Files.write(tmp, fileBytes);
            importSealed(ns, id, tmp, expectedLength, expectedCrc);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /**
     * Imports a sealed chunk from a raw chunk-file image already written to {@code sourceFile}.
     * The source file is consumed: on success it is atomically moved into place; on failure the
     * caller should delete it.
     */
    public void importSealed(StrataNamespace ns, ChunkId id, Path sourceFile,
                             long expectedLength, int expectedCrc) throws IOException {
        reserveNewChunk(ns, id);
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

            Handle h = new Handle(id, header, ns);
            Files.createDirectories(h.shardDir);
            if (Files.exists(h.dataPath)) throw chunkAlreadyExists(id);
            boolean movedData = false;
            boolean sidecarStarted = false;
            boolean installed = false;
            try {
                try (FileChannel ch = FileChannel.open(sourceFile, StandardOpenOption.WRITE)) {
                    ch.force(true);
                }
                Files.move(sourceFile, h.dataPath, StandardCopyOption.ATOMIC_MOVE);
                forceDirectory(h.shardDir);
                movedData = true;
                h.data = null; // sealed + durable on import: reads go through the channel cache
                h.state = ChunkState.SEALED;
                h.end = trailer.dataLength();
                h.sealedLength = trailer.dataLength();
                h.dataCrc = trailer.dataCrc();
                h.sealedRangeCrcs = rangeCrcs;
                h.writeEpoch = header.createWriteEpoch();
                h.fenceEpoch = -1;
                h.lastKnownDO = trailer.dataLength();
                // Durability-v2 (Lever 1): an imported sealed chunk carries a verified trailer and no
                // ledger, which recovery classifies as SEALED — no sidecar written.
                Files.deleteIfExists(h.ledgerPath);
                forceDirectory(h.shardDir);
                chunks.put(new NsChunkId(ns, id), h);
                installed = true;
            } finally {
                if (!installed) {
                    cleanupFailedImport(h, sourceFile, movedData, sidecarStarted);
                }
            }
        } finally {
            releaseReservation(ns, id);
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
            forceDirectory(dir); // temp file lives in store root
        } catch (IOException e) {
            log.warn("failed to delete incomplete import temp file {}", tmp, e);
        }
        if (movedData) {
            try {
                Files.deleteIfExists(h.dataPath);
                forceDirectory(h.shardDir);
            } catch (IOException e) {
                log.warn("failed to delete incomplete import data file {}", h.dataPath, e);
            }
        }
        if (sidecarStarted) {
            try {
                Files.deleteIfExists(h.metaPath);
                forceDirectory(h.shardDir);
            } catch (IOException e) {
                log.warn("failed to delete incomplete import sidecar {}", h.metaPath, e);
            }
        }
    }

    public ErrorCode delete(StrataNamespace ns, ChunkId id) {
        long t0 = System.nanoTime();
        NsChunkId key = new NsChunkId(ns, id);
        Handle h = chunks.get(key);
        if (h == null && creating.contains(key)) return ErrorCode.INTERNAL;
        if (h == null) return ErrorCode.CHUNK_NOT_FOUND;
        GroupCommitter committerToStop;
        h.lock.lock();
        try {
            h.state = ChunkState.DELETING; // blocks appends/seal for the whole teardown
            committerToStop = h.committer;
            if (committerToStop == null) {
                return deleteLocked(h, key, id, t0); // nothing to drain off-lock — tear down under the lock
            }
        } finally {
            h.lock.unlock();
        }
        // Phase B (off the chunk lock): drain + join the committer — the up-to-12s pole — so other ops on
        // this chunk are not blocked while a degraded fsync drains.
        if (!committerToStop.closeAndConfirm() || committerToStop.isPoisoned()) {
            // flusher stuck/failed: a force may still be in flight, so we must NOT close/delete the files.
            // Leave the chunk DELETING and visible so a later delete retries (mirrors the file-IO-failure path).
            log.warn("delete {} — group-commit flusher did not stop cleanly; left for retry", id);
            return ErrorCode.INTERNAL;
        }
        h.lock.lock();
        try {
            h.committer = null; // confirmed stopped above; safe to mutate files
            return deleteLocked(h, key, id, t0);
        } finally {
            h.lock.unlock();
        }
    }

    /**
     * Closes and unlinks a chunk's files and removes it from the map, with the chunk lock held and the
     * committer already stopped/absent. On I/O failure the chunk is left visible (state DELETING) so a
     * later delete retries; idempotent, so a re-entrant delete after a partial failure re-runs cleanly.
     */
    private ErrorCode deleteLocked(Handle h, NsChunkId key, ChunkId id, long t0) {
        try {
            // committer already stopped by the caller (off-lock, or it never existed) — files are safe to mutate
            if (h.data != null) h.data.close();
            if (h.ledger != null) h.ledger.close();
            Files.deleteIfExists(h.dataPath);
            Files.deleteIfExists(h.metaPath);
            Files.deleteIfExists(h.ledgerPath);
            if (sealFsync) {
                forceDirectory(h.shardDir);
            }
            chunks.remove(key, h);
        } catch (IOException e) {
            log.warn("delete {} failed", id, e);
            return ErrorCode.INTERNAL;
        }
        channelCache.invalidate(key);
        if (System.nanoTime() - t0 > SLOW_MUTATION_LOG_NANOS) {
            log.info("slow delete {} took {}ms", id, msBetween(t0, System.nanoTime()));
        }
        return ErrorCode.OK;
    }

    /** A point-in-time view of one stored chunk — for tests/diagnostics that inspect the store's contents. */
    public record ChunkSummary(StrataNamespace namespace, ChunkId chunkId, ChunkState state, long length, int crc) {}

    /** Snapshots every chunk this store holds. No longer pushed anywhere (Phase 2 replaced the inventory
     *  push with owner-pull VERIFY_CHUNKS); retained as a store-inspection accessor. */
    public List<ChunkSummary> describeChunks() {
        List<ChunkSummary> out = new ArrayList<>();
        for (Handle h : chunks.values()) {
            h.lock.lock();
            try {
                out.add(new ChunkSummary(h.ns, h.id, h.state, h.currentEnd(), h.dataCrc));
            } finally {
                h.lock.unlock();
            }
        }
        return out;
    }

    /** One chunk's local verification fact for {@link #verify}; {@code present == false} means absent. */
    public record VerifyResult(ChunkId chunkId, boolean present, ChunkState state, long length, int crc) {}

    /**
     * Owner-pull verification (design §20.3): report the local state of each requested chunk and stamp
     * the present ones as freshly verified — which both refreshes the orphan-GC grace (§20.4) and lets
     * the owner compare state/length/crc against its descriptor to find missing/corrupt replicas. An
     * absent chunk reports {@code present == false} (a missing replica). Read-only on the data itself.
     */
    public List<VerifyResult> verify(StrataNamespace ns, List<ChunkId> chunkIds) {
        long now = System.currentTimeMillis();
        List<VerifyResult> out = new ArrayList<>(chunkIds.size());
        for (ChunkId id : chunkIds) {
            Handle h = chunks.get(new NsChunkId(ns, id));
            if (h == null) {
                out.add(new VerifyResult(id, false, ChunkState.OPEN, 0, 0));
                continue;
            }
            h.lock.lock();
            try {
                h.lastVerifiedAtMs = now;
                out.add(new VerifyResult(id, true, h.state, h.currentEnd(), h.dataCrc));
            } finally {
                h.lock.unlock();
            }
        }
        return out;
    }

    /** A locally-held sealed chunk no owner has verified within the grace window (orphan-GC candidate). */
    public record SuspectChunk(StrataNamespace namespace, ChunkId chunkId) {}

    /**
     * Node-local orphan-GC candidates (design §20.4): sealed chunks no owner has attested within
     * {@code olderThanMs} (via {@link #verify}). Open chunks (in-flight writes) and freshly-known chunks
     * (still inside grace) are excluded. Returns a snapshot; the caller confirms each with the owner
     * before deleting — a suspect is not yet a confirmed orphan.
     */
    public List<SuspectChunk> orphanSuspects(long olderThanMs, long now) {
        List<SuspectChunk> out = new ArrayList<>();
        for (Handle h : chunks.values()) {
            h.lock.lock();
            try {
                if (h.state == ChunkState.SEALED && now - h.lastVerifiedAtMs >= olderThanMs) {
                    out.add(new SuspectChunk(h.ns, h.id));
                }
            } finally {
                h.lock.unlock();
            }
        }
        return out;
    }

    public long usedBytes() {
        long total = 0;
        for (Handle h : chunks.values()) {
            total += sizeIfExists(h.dataPath);
            total += sizeIfExists(h.metaPath);
            total += sizeIfExists(h.ledgerPath);
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

    public boolean contains(StrataNamespace ns, ChunkId id) {
        return chunks.containsKey(new NsChunkId(ns, id));
    }

    /**
     * Re-verifies sealed chunks' data regions against their trailer CRC (tech design §16
     * crash-safety / the read-path's deferred verification). On rot, the handle's dataCrc is
     * updated to the RECOMPUTED value so the next owner-pull VERIFY_CHUNKS reports a crc that
     * mismatches the descriptor — the coordinator's corrupt-replica path then drops and
     * re-repairs this copy. Returns the number of corrupt chunks found.
     */
    public int scrubOnce() throws IOException {
        int corrupt = 0;
        for (Handle h : chunks.values()) {
            long sealedLength;
            int storedCrc;
            Path dataPath;
            h.lock.lock();
            try {
                if (h.state != ChunkState.SEALED) continue;
                sealedLength = h.sealedLength;
                storedCrc = h.dataCrc;
                dataPath = h.dataPath;
            } finally {
                h.lock.unlock();
            }
            int actual;
            try (ChannelCache.Lease lease = channelCache.acquire(h.nsKey, dataPath)) {
                actual = scanDataCrcs(lease.channel(), sealedLength).dataCrc();
            }
            if (actual != storedCrc) {
                h.lock.lock();
                try {
                    if (h.state == ChunkState.SEALED && h.dataCrc == storedCrc) {
                        log.error("scrub: sealed chunk {} data rot — stored crc {} actual {}; "
                                + "updating reported crc so the next owner verify re-repairs", h.id, storedCrc, actual);
                        h.dataCrc = actual;
                        corrupt++;
                    }
                } finally {
                    h.lock.unlock();
                }
            }
        }
        return corrupt;
    }

    /* ---------------- startup recovery (tech design §11.3) ---------------- */

    /**
     * Startup recovery: walks the namespace-sharded directory tree in parallel, recovering every
     * {@code .chunk} file found under {@code dir/<ns>/<shard>/}. Each top-level namespace directory
     * is recovered concurrently via virtual threads. Any unexpected flat {@code .chunk} files sitting
     * directly in the store root (outside a namespace directory) are quarantined and logged as errors.
     */
    private void recoverAll() throws IOException {
        if (!Files.isDirectory(dir)) return;

        // Single pass: partition store-root entries into namespace dirs and stray .chunk files.
        List<Path> nsDirs = new ArrayList<>();
        List<Path> rootChunks = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dir)) {
            stream.forEach(p -> {
                if (Files.isDirectory(p)) {
                    nsDirs.add(p);
                } else if (p.getFileName().toString().endsWith(".chunk")) {
                    rootChunks.add(p);
                }
            });
        }
        // Quarantine any unexpected flat .chunk files placed directly in the store root
        // (they cannot belong to any namespace directory and indicate corruption or misplaced files).
        for (Path p : rootChunks) {
            log.error("unexpected flat .chunk file in store root — quarantined: {}", p);
            quarantineRecoveredFiles(p);
        }

        if (nsDirs.isEmpty()) return;

        // Recover each namespace in parallel via virtual threads
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (Path nsDir : nsDirs) {
                String nsName = nsDir.getFileName().toString();
                StrataNamespace ns = StrataNamespace.of(nsName);
                futures.add(executor.submit(() -> {
                    try {
                        recoverNamespace(ns, nsDir);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }));
            }
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof UncheckedIOException uioe) throw uioe.getCause();
                    if (cause instanceof IOException ioe) throw ioe;
                    throw new IOException("recovery failed", cause);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("recovery interrupted", e);
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private void recoverNamespace(StrataNamespace ns, Path nsDir) throws IOException {
        // Collect all .chunk files first (crash-safety: avoid delete-during-walk)
        List<Path> chunkFiles;
        try (Stream<Path> files = Files.walk(nsDir)) {
            chunkFiles = files.filter(p -> p.getFileName().toString().endsWith(".chunk")).toList();
        }
        for (Path p : chunkFiles) {
            String name = p.getFileName().toString();
            String base = name.substring(0, name.length() - ".chunk".length());
            try {
                recoverOne(ns, ChunkFormats.parseBaseName(base), p);
            } catch (Exception e) {
                log.error("failed to recover chunk {} — quarantined", base, e);
                quarantineRecoveredFiles(p);
            }
        }
    }

    private void quarantineRecoveredFiles(Path dataPath) {
        boolean moved = false;
        String suffix = ".quarantine-" + System.currentTimeMillis();
        String base = dataPath.getFileName().toString();
        base = base.substring(0, base.length() - ".chunk".length());
        Path shardDir = dataPath.getParent();
        for (String ext : List.of(".chunk", ".meta", ".j")) {
            Path source = shardDir.resolve(base + ext);
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
                forceDirectory(shardDir);
            } catch (IOException e) {
                log.warn("failed to fsync quarantine directory {}", shardDir, e);
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

    /**
     * Recovers a single chunk given its namespace and data file path. The namespace-aware Handle
     * constructor stores the namespace so the chunk is keyed correctly in the NsChunkId map.
     */
    void recoverOne(StrataNamespace ns, ChunkId id, Path dataPath) throws IOException {
        Handle probe = new Handle(id, null, dataPath, ns);
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
        // Durability-v2 (Lever 1): a valid sealed trailer is the authoritative SEALED signal, even over
        // a sidecar still reading OPEN from the pre-reclaim window. isSealedByTrailer reconciles a
        // retained straggler ledger by coverage so a footer-shaped OPEN payload is not misread as sealed.
        boolean reconstructedSidecar = false;
        ChunkFormats.Sidecar sidecar;
        SealedProbe sealedProbe = trySealedProbe(probe.dataPath, id);
        if (isSealedByTrailer(sealedProbe, probe.ledgerPath)) {
            sidecar = new ChunkFormats.Sidecar(header.createWriteEpoch(), -1, 0, ChunkState.SEALED);
            if (hasSidecar) {
                Files.deleteIfExists(probe.metaPath); // a stale OPEN sidecar must not outlive the trailer
                forceDirectory(probe.shardDir);
            }
        } else if (hasSidecar) {
            sidecar = ChunkFormats.Sidecar.decode(Files.readAllBytes(probe.metaPath));
        } else {
            sidecar = recoverMissingSidecar(id, probe, header);
            if (sidecar == null) {
                return;
            }
            reconstructedSidecar = true;
        }
        Handle h = new Handle(id, header, dataPath, ns);
        boolean installed = false;
        try {
            if (sidecar.state() == ChunkState.SEALED) {
                // Sealed chunks are durable + immutable: reuse the trailer/footer the recovery probe
                // already read, and DO NOT keep a persistent data FD — reads open via the channel cache.
                // If the SEALED verdict came from a sidecar instead of the trailer (a rotted footer the
                // probe rejected, so sealedProbe is null), re-read to re-prove the footer, quarantining
                // (CorruptChunkException) a sealed chunk with bad footer metadata.
                SealedProbe sealed = sealedProbe != null ? sealedProbe : readSealedFooterFromPath(h.dataPath, id);
                ChunkFormats.Trailer trailer = sealed.trailer();
                h.writeEpoch = sidecar.writeEpoch();
                h.fenceEpoch = sidecar.fenceEpoch();
                h.data = null;
                h.state = ChunkState.SEALED;
                h.end = trailer.dataLength();
                h.sealedLength = trailer.dataLength();
                h.dataCrc = trailer.dataCrc();
                h.sealedRangeCrcs = sealed.rangeCrcs();
                h.lastKnownDO = Math.max(sidecar.lastKnownDO(), trailer.dataLength());
                if (reconstructedSidecar) {
                    h.persistSidecar();
                }
                Files.deleteIfExists(h.ledgerPath); // seal crashed before ledger delete
                forceDirectory(h.shardDir);
            } else {
                // OPEN: keep a persistent writable channel + ledger (pinned, as before).
                h.data = FileChannel.open(h.dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
                h.writeEpoch = sidecar.writeEpoch();
                h.fenceEpoch = sidecar.fenceEpoch();
                h.lastKnownDO = sidecar.lastKnownDO();
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
            chunks.put(new NsChunkId(ns, id), h);
            installed = true;
            log.info("recovered chunk {} ns={} state={} end={}", id, ns, h.state, h.end);
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
        // A no-ledger chunk with a valid sealed trailer is already classified SEALED by recoverOne's
        // trailer-authoritative check, so reaching here (no sidecar, not sealed-by-trailer, no ledger)
        // means the chunk has no valid sealed footer — treat it as an incomplete/unacked remnant.
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
        forceDirectory(probe.shardDir);
    }

    /** A sealed chunk's trailer + decoded CRC ranges, read and CRC-validated from the file in one pass. */
    private record SealedProbe(ChunkFormats.Trailer trailer, List<Integer> rangeCrcs) {}

    /**
     * Reads and CRC-validates the sealed trailer + footer from an open chunk channel. Throws
     * {@link CorruptChunkException} on a footer-CRC mismatch — the quarantine signal for a sealed chunk
     * whose footer metadata has rotted. Shared by the recovery probe (which swallows the throw into
     * "not validly sealed") and the SEALED install path (which lets it quarantine). Full data-region
     * verification is deferred to scrub; readers have CRC_RANGES + batch CRCs.
     */
    private static SealedProbe readSealedFooter(FileChannel data, long fileLen, ChunkId id) throws IOException {
        byte[] trailerBytes = new byte[TRAILER_SIZE];
        readFully(data, ByteBuffer.wrap(trailerBytes), fileLen - TRAILER_SIZE);
        ChunkFormats.Trailer trailer = ChunkFormats.Trailer.decode(trailerBytes);
        int footerLen = checkedFooterLength(trailer, fileLen);
        byte[] footerBytes = new byte[footerLen];
        readFully(data, ByteBuffer.wrap(footerBytes), trailer.footerStart());
        if (Crc.of(footerBytes) != trailer.footerCrc()) {
            throw new CorruptChunkException("footer crc mismatch for sealed chunk " + id);
        }
        return new SealedProbe(trailer, decodeCrcRanges(footerBytes, trailer.dataLength(), trailer.sectionCount()));
    }

    /** The validated sealed trailer/footer of {@code dataPath}, or null if it is not a valid sealed chunk. */
    private SealedProbe trySealedProbe(Path dataPath, ChunkId id) {
        try (FileChannel ch = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            long fileLen = ch.size();
            if (fileLen < DATA_START + TRAILER_SIZE) {
                return null;
            }
            return readSealedFooter(ch, fileLen, id);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    private static SealedProbe readSealedFooterFromPath(Path dataPath, ChunkId id) throws IOException {
        try (FileChannel data = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            return readSealedFooter(data, data.size(), id);
        }
    }

    /**
     * A valid sealed trailer is the authoritative SEALED signal at recovery, but a retained straggler
     * ledger must be reconciled. A cleanly-sealed chunk's ledger was truncated to whole entries ending
     * exactly at trailer.dataLength, with an INTACT last entry and no torn tail. An OPEN chunk whose
     * payload merely looks like a footer has a longer ledger — and, crucially, the disambiguator must
     * require the ledger to END CLEANLY at dataLength, not merely to CONTAIN an intact entry there: a
     * torn last entry on a footer-shaped open chunk could leave the previous intact entry sitting exactly
     * at the fake trailer's dataLength, which a backward scan would mistake for a clean seal (PR #37).
     */
    private boolean isSealedByTrailer(SealedProbe probe, Path ledgerPath) {
        if (probe == null) {
            return false;
        }
        if (!Files.exists(ledgerPath)) {
            return true;
        }
        return ledgerEndsCleanlyAt(ledgerPath, probe.trailer().dataLength());
    }

    /**
     * True iff {@code ledgerPath} is a cleanly-terminated ledger whose LAST entry is intact and ends
     * exactly at {@code dataLength}. A torn tail — a partial trailing entry (size not a whole multiple of
     * the entry size) or a corrupt last entry — means the chunk was not cleanly sealed, so it must not
     * read as sealed even if an EARLIER intact entry happens to hit dataLength. Unlike a backward scan,
     * this does not skip a torn tail.
     */
    private static boolean ledgerEndsCleanlyAt(Path ledgerPath, long dataLength) {
        try {
            long size = Files.size(ledgerPath);
            if (size == 0 || size % ChunkFormats.LEDGER_ENTRY_SIZE != 0) {
                return false;
            }
            byte[] buf = new byte[ChunkFormats.LEDGER_ENTRY_SIZE];
            try (FileChannel ch = FileChannel.open(ledgerPath, StandardOpenOption.READ)) {
                readFully(ch, ByteBuffer.wrap(buf), size - ChunkFormats.LEDGER_ENTRY_SIZE);
            }
            ChunkFormats.LedgerEntry last = ChunkFormats.LedgerEntry.decodeOrNull(buf, 0);
            return last != null && last.endOffset() == dataLength;
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    private void readSealedVerified(FileChannel data, long sealedLength, List<Integer> rangeCrcs,
                                    ChunkId id, long offset, byte[] out) throws IOException {
        if (out.length == 0) return;
        if (rangeCrcs.isEmpty()) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "sealed chunk missing CRC ranges: " + id);
        }
        long firstRange = offset / ChunkFormats.CRC_RANGE_SIZE;
        long lastRange = (offset + out.length - 1) / ChunkFormats.CRC_RANGE_SIZE;
        byte[] rangeBuf = new byte[ChunkFormats.CRC_RANGE_SIZE];
        for (long range = firstRange; range <= lastRange; range++) {
            if (range >= rangeCrcs.size()) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "CRC range missing for " + id);
            }
            long rangeStart = range * (long) ChunkFormats.CRC_RANGE_SIZE;
            int rangeLen = (int) Math.min(ChunkFormats.CRC_RANGE_SIZE, sealedLength - rangeStart);
            readFully(data, ByteBuffer.wrap(rangeBuf, 0, rangeLen),
                    checkedAdd(DATA_START, rangeStart, "chunk file offset"));
            int actual = Crc.of(rangeBuf, 0, rangeLen);
            int expected = rangeCrcs.get((int) range);
            if (actual != expected) {
                throw new ScpException(ErrorCode.CRC_MISMATCH,
                        "sealed range crc mismatch on " + id + " range " + range);
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
            h.lock.lock();
            try {
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
            } finally {
                h.lock.unlock();
            }
        }
        channelCache.close();
        if (failure == null) {
            chunks.clear();
        } else {
            Closeables.throwIfFailed(failure);
        }
    }
}
