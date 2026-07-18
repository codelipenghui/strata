package io.strata.format;

import com.sun.management.UnixOperatingSystemMXBean;
import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Closeables;
import io.strata.common.Crc;
import io.strata.common.EnvConfig;
import io.strata.common.ErrorCode;
import io.strata.common.FailureInjector;
import io.strata.common.FileId;
import io.strata.common.Fsync;
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
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
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
 * All mutations are serialized per chunk handle by a per-{@code Handle} {@link ReentrantLock}
 * (NOT the intrinsic monitor: critical sections block on FileChannel I/O and the group-commit
 * flusher join, and on Java 21 a virtual thread that blocks while holding {@code synchronized}
 * pins its carrier — ReentrantLock unmounts cleanly). The data region is raw logical bytes —
 * this engine never parses payload content (invariant §14.10), not even during recovery.
 */
public final class ChunkStore implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);
    static final long REPAIR_IMPORT_ORPHAN_PROTECTION_MS = 90_000;

    public interface AppendPayload {
        /**
         * Returns the exact byte count for this append. The value must stay stable across the
         * matching {@link #writeFully(FileChannel, long)} and {@link #accumulateCrc(PayloadCrcAccumulator)}
         * calls.
         */
        int remaining();

        /** Writes exactly {@link #remaining()} bytes at {@code position}, or throws before acking. */
        void writeFully(FileChannel channel, long position) throws IOException;

        /**
         * Folds the full payload exactly once. Implementations must not throw after partially updating
         * {@code accumulator}; otherwise a same-offset retry can corrupt the seal-time CRC snapshot.
         */
        void accumulateCrc(PayloadCrcAccumulator accumulator) throws IOException;
    }

    public interface PayloadCrcAccumulator {
        void update(ByteBuffer bytes);

        void update(byte[] bytes, int offset, int length);
    }

    @FunctionalInterface
    interface DirectorySyncer {
        void force(Path dir) throws IOException;
    }

    private static final long MAX_IMPORT_FOOTER_BYTES = 64L * 1024 * 1024;

    private static final int RECOVERY_FENCE_REQUIRED = Integer.MAX_VALUE;
    private static final byte[] EMPTY_READ_BYTES = new byte[0];
    private static final int[] EMPTY_INT_ARRAY = new int[0];
    private static final int OPEN_LEDGER_INITIAL_ENTRIES = 4096;
    private static final int INITIAL_RANGE_CRC_ENTRIES = 32;
    private static final int CRC_SCAN_BUFFER_BYTES = 1 << 20;
    private static final ThreadLocal<LookupKey> LOOKUP_KEY = ThreadLocal.withInitial(LookupKey::new);
    private static final int READ_BUFFER_POOL_MAX_BYTES =
            EnvConfig.intEnv("STRATA_READ_BUFFER_POOL_MAX_BYTES", 1 << 20);
    private static final int READ_BUFFER_POOL_MAX_BUFFERS =
            EnvConfig.intEnv("STRATA_READ_BUFFER_POOL_MAX_BUFFERS", 64);
    private static final int SEALED_VERIFY_BUFFER_POOL_MAX_BYTES =
            EnvConfig.intEnv("STRATA_SEALED_VERIFY_BUFFER_POOL_MAX_BYTES", ChunkFormats.CRC_RANGE_SIZE);
    private static final int SEALED_VERIFY_BUFFER_POOL_MAX_BUFFERS =
            EnvConfig.intEnv("STRATA_SEALED_VERIFY_BUFFER_POOL_MAX_BUFFERS", 8);
    private static final Set<OpenOption> READ_OPEN_OPTIONS = Set.of(StandardOpenOption.READ);

    private final Path dir;
    private final Map<ChunkKey, Handle> chunks = new ChunkHandleMap();
    private final ChannelCache channelCache;
    private final ReadBufferPool readBufferPool =
            new ReadBufferPool(READ_BUFFER_POOL_MAX_BYTES, READ_BUFFER_POOL_MAX_BUFFERS);
    private final ReadBufferPool sealedVerifyBufferPool =
            new ReadBufferPool(SEALED_VERIFY_BUFFER_POOL_MAX_BYTES, SEALED_VERIFY_BUFFER_POOL_MAX_BUFFERS);
    private final Set<NsChunkId> creating = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Path, Object> directoryDurabilityLocks = new ConcurrentHashMap<>();
    private final Set<Path> durableDirectories = ConcurrentHashMap.newKeySet();
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
    // Both knobs are tunable through ChunkStoreConfig so a deployment can trade fsync
    // syscall rate against seal-time fsync size: a shorter interval / smaller threshold keeps less dirty
    // data per open chunk, so a synchronized roll does not stampede the disk's fsync queue with many
    // large concurrent forces. Background writeback keeps the original 500ms / 4 MiB default.
    // ChunkStoreConfig.sealFsync (default false) gates the best-effort, off-the-ack-path fsyncs at open,
    // seal, and delete (header/sidecar/dir plus the seal-time data force) — NOT just seal. fsyncOnAck chunks
    // force their create-time header/sidecar/dirents regardless of this flag, then the group committer
    // forces data+ledger before acking appends. With seal fsync off, a sealed chunk's ledger is retained
    // until reclaimSealedLedgersOnce() forces the SEALED state durable, so recovery never discards
    // acknowledged data.
    /** Whether seal/open/delete force their metadata + data to disk synchronously. */
    private final boolean sealFsync;
    private final DirectorySyncer directorySyncer;
    private final ChunkStoreConfig csConfig;
    private final ScheduledExecutorService flusher;

    /** Live process open file-descriptor count for observability; {@code -1} when unavailable. */
    public long openFds() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof UnixOperatingSystemMXBean unix) {
            return unix.getOpenFileDescriptorCount();
        }
        return -1;
    }

    public ChunkStore(Path dir) throws IOException {
        this(dir, ChunkStoreConfig.DEFAULT);
    }

    public ChunkStore(Path dir, ChunkStoreConfig csConfig) throws IOException {
        this(dir, csConfig.sealFsync(), csConfig.channelCacheMaxSize(), csConfig);
    }

    /** Package-private: lets tests exercise both the seal-fsync-on and -off durability paths. */
    ChunkStore(Path dir, boolean sealFsync) throws IOException {
        this(dir, ChunkStoreConfig.DEFAULT.withSealFsync(sealFsync));
    }

    /**
     * Package-private: lets tests specify a small channel cache capacity to exercise eviction
     * without creating thousands of files (avoids static-final class-load timing coupling).
     */
    ChunkStore(Path dir, boolean sealFsync, int channelCacheCapacity) throws IOException {
        this(dir, ChunkStoreConfig.DEFAULT.withSealFsync(sealFsync)
                .withChannelCacheMaxSize(channelCacheCapacity));
    }

    ChunkStore(Path dir, boolean sealFsync, int channelCacheCapacity, ChunkStoreConfig csConfig) throws IOException {
        this(dir, sealFsync, channelCacheCapacity, csConfig, Fsync::forceDirectory);
    }

    ChunkStore(Path dir, boolean sealFsync, int channelCacheCapacity, ChunkStoreConfig csConfig,
               DirectorySyncer directorySyncer) throws IOException {
        this.dir = dir;
        this.sealFsync = sealFsync;
        this.directorySyncer = Objects.requireNonNull(directorySyncer, "directorySyncer");
        this.csConfig = csConfig;
        this.channelCache = new ChannelCache(channelCacheCapacity);
        // The store root's own dirent lives in its parent. Force it on every construction so a retry
        // cannot mistake a root left behind by a failed or concurrent initializer for a durable one.
        ensureDirectoryDurable(dir);
        recoverAll();
        this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "chunk-writeback-" + dir.getFileName());
            t.setDaemon(true);
            return t;
        });
        log.info("chunk store writeback configured: intervalMs={} thresholdBytes={} sealFsync={}",
                csConfig.backgroundFlushIntervalMs(), csConfig.backgroundFlushThresholdBytes(), sealFsync);
        flusher.scheduleWithFixedDelay(this::backgroundFlushSafely,
                csConfig.backgroundFlushIntervalMs(), csConfig.backgroundFlushIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    private void forceDirectory(Path directory) throws IOException {
        directorySyncer.force(directory);
    }

    private void forceParentDirectory(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            forceDirectory(parent);
        }
    }

    private void forceSourceDirectoryAfterMove(Path sourceParent, Path targetDir) throws IOException {
        if (sourceParent == null) {
            return;
        }
        Path source = sourceParent.toAbsolutePath().normalize();
        Path target = targetDir.toAbsolutePath().normalize();
        if (!source.equals(target)) {
            forceDirectory(source);
        }
    }

    private void createDirectories(Path targetDir, boolean syncCreatedDirents) throws IOException {
        if (!syncCreatedDirents) {
            Files.createDirectories(targetDir);
            return;
        }
        Path root = dir.toAbsolutePath().normalize();
        Path normalized = targetDir.toAbsolutePath().normalize();
        if (normalized.equals(root)) {
            // The root's own dirent is made durable during construction.
            return;
        }
        if (!normalized.startsWith(root)) {
            ensureDirectoryDurable(normalized);
            return;
        }
        Path current = root;
        for (Path segment : root.relativize(normalized)) {
            current = current.resolve(segment);
            ensureDirectoryDurable(current);
        }
    }

    private void ensureDirectoryDurable(Path directory) throws IOException {
        Path normalized = directory.toAbsolutePath().normalize();
        if (durableDirectories.contains(normalized)) {
            return;
        }
        Object lock = directoryDurabilityLocks.computeIfAbsent(normalized, ignored -> new Object());
        synchronized (lock) {
            if (durableDirectories.contains(normalized)) {
                return;
            }
            try {
                Files.createDirectory(normalized);
            } catch (FileAlreadyExistsException e) {
                if (!Files.isDirectory(normalized)) {
                    throw e;
                }
            }
            Path parent = normalized.getParent();
            if (parent != null) {
                forceDirectory(parent);
            }
            durableDirectories.add(normalized);
        }
    }

    /** Number of group-commit force() calls — observability + coalescing tests. */
    public long fsyncForceCount() {
        return forceCount.get();
    }

    /** Number of background-writeback fsyncs of open chunks — observability; should keep seals cheap. */
    public long backgroundFlushes() {
        return backgroundFlushes.get();
    }

    private long slowAppendLogNanos() {
        return TimeUnit.MILLISECONDS.toNanos(csConfig.slowAppendLogMs());
    }

    private long slowMutationLogNanos() {
        return TimeUnit.MILLISECONDS.toNanos(csConfig.slowMutationLogMs());
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
        String namespace = ns.value();
        LongAdder[] counters = nsIo.get(namespace);
        return counters != null ? counters : nsIo.computeIfAbsent(namespace, ChunkStore::newNsIoCounters);
    }

    private static LongAdder[] newNsIoCounters(String ignored) {
        return new LongAdder[]{
                new LongAdder(), new LongAdder(),
                new LongAdder(), new LongAdder()};
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

    /** Sealed chunks by namespace — best-effort snapshot for orphan-GC delete budgets. */
    public Map<StrataNamespace, Integer> sealedChunksByNamespace() {
        Map<StrataNamespace, Integer> out = new HashMap<>();
        for (Handle h : chunks.values()) {
            h.lock.lock();
            try {
                if (h.state == ChunkState.SEALED) {
                    out.merge(h.ns, 1, Integer::sum);
                }
            } finally {
                h.lock.unlock();
            }
        }
        return out;
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
     * Reclaims the integrity ledger of chunks sealed under {@code ChunkStoreConfig.sealFsync=false}. seal()
     * deliberately retains that ledger because the SEALED footer/sidecar are left in the page cache:
     * a crash before they reach disk leaves a stale OPEN sidecar, and recovery's OPEN branch rebuilds
     * the chunk by replaying the ledger. Removing the ledger before the SEALED state is durable would
     * make recovery truncate acknowledged data to zero (C1). So this re-establishes the seal-time
     * durability ordering off the seal hot path: force the data (footer/trailer), durably unlink the
     * ledger, and only THEN unlink the stale OPEN sidecar. The data force runs outside the chunk lock
     * (it is slow); finish work is off the hot path. Package-private so tests can drive a round
     * deterministically.
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
                    if (chunks.get(h.mapKey) != h || h.data != data || !h.sealedLedgerPending) {
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
                    if (chunks.get(h.mapKey) != h || h.data != data
                            || h.state != ChunkState.SEALED || !h.sealedLedgerPending) {
                        continue; // superseded after the force — leave it to the winner
                    }
                    // Durability-v2 (Lever 1): the trailer (forced durable above) is the SEALED signal.
                    // Drop the retained ledger durably before deleting the now-stale OPEN sidecar, so a
                    // crash cannot make the sidecar unlink durable while a stale pre-truncate ledger
                    // resurrects for a chunk sealed shorter than its old end.
                    deleteLedgerDurably(h.id, h.ledgerPath, h.shardDir);
                    FailureInjector.point("format.reclaim.afterLedgerDurableBeforeMetaDelete");
                    Files.deleteIfExists(h.metaPath);
                } finally {
                    h.lock.unlock();
                }
                forceDirectory(h.shardDir); // make the unlink durable (recovery's SEALED branch re-deletes otherwise)
                h.lock.lock();
                try {
                    if (chunks.get(h.mapKey) == h && h.data == data
                            && h.state == ChunkState.SEALED && h.sealedLedgerPending) {
                        h.sealedLedgerPending = false;
                        closeAndNullData(h);                // release the writable FD; reads use the cache
                        reclaimed = true;
                    }
                } finally {
                    h.lock.unlock();
                }
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
     * the configured threshold since its last background flush, bounding the dirty backlog a
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
                if (h.end - h.bgFlushedOffset < csConfig.backgroundFlushThresholdBytes()) {
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
                    if (chunks.get(h.mapKey) != h || h.state != ChunkState.OPEN || h.data != data) {
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
                if (chunks.get(h.mapKey) == h && h.state == ChunkState.OPEN
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

    final class Handle implements PayloadCrcAccumulator {
        // Serializes all access to this chunk's mutable state. Deliberately a ReentrantLock, NOT the
        // intrinsic monitor: critical sections block on FileChannel I/O (write/force/open/truncate) and
        // on the group-commit flusher join, and on Java 21 a virtual thread that blocks while holding
        // `synchronized` pins its carrier. ReentrantLock unmounts cleanly; no wait/notify is used.
        final ReentrantLock lock = new ReentrantLock();
        final ChunkId id;
        final StrataNamespace ns;
        final ChunkKey mapKey;  // pre-computed map key — avoids per-iteration allocation in background loops
        final NsChunkId nsKey;
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
        // a new ChunkState: a seal aborted before file mutation must restore clean OPEN+committer-running.
        // Never persisted.
        boolean sealing;
        // finalizeSealLocked may fail after partially truncating the data/ledger. Poison further appends
        // before they mutate that uncertain OPEN state; a seal retry or process recovery can converge it.
        // This also protects ack-on-replicate chunks, whose null committer is otherwise normal.
        boolean sealFinalizationFailed;
        long failedSealLength = -1;
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
        int[] sealedRangeCrcs = EMPTY_INT_ARRAY;
        // Last time an owner attested this replica via VERIFY_CHUNKS (design §9.2); seeded to when this
        // node first learned of the chunk so a freshly-created/recovered chunk gets the full orphan grace
        // before it can be considered a suspect. In-memory only: a restart re-earns verification.
        volatile long lastVerifiedAtMs = System.currentTimeMillis();
        // Repair imports are locally durable before the controller commits the descriptor swap. During that
        // window the authoritative owner-confirm truthfully does not list this node yet, so node-local orphan
        // GC must not delete the just-copied replica before the completion heartbeat and swap/retry path lands.
        volatile long orphanProtectedUntilMs;

        // Running CRC state for an OPEN chunk: the whole-chunk CRC and the per-CRC_RANGE_SIZE range
        // CRCs are folded as bytes are appended (and rebuilt from the verified prefix on recovery),
        // so seal can emit them without re-reading the data region. Unused once SEALED.
        final CRC32C runningWhole = new CRC32C();
        CRC32C runningRange = new CRC32C();
        long rangeRemaining = ChunkFormats.CRC_RANGE_SIZE;
        final IntList completedRangeCrcs = new IntList(INITIAL_RANGE_CRC_ENTRIES);

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
            int originalPosition = payload.position();
            int originalLimit = payload.limit();
            int cursor = originalPosition;
            try {
                while (cursor < originalLimit) {
                    int n = (int) Math.min(originalLimit - cursor, rangeRemaining);
                    int next = cursor + n;
                    payload.position(cursor).limit(next);
                    runningWhole.update(payload);
                    payload.position(cursor).limit(next);
                    runningRange.update(payload);
                    cursor = next;
                    rangeRemaining -= n;
                    if (rangeRemaining == 0) {
                        completedRangeCrcs.add((int) runningRange.getValue());
                        runningRange.reset();
                        rangeRemaining = ChunkFormats.CRC_RANGE_SIZE;
                    }
                }
            } finally {
                payload.limit(originalLimit).position(originalPosition);
            }
        }

        void crcAccumulate(AppendPayload payload) throws IOException {
            payload.accumulateCrc(this);
        }

        @Override
        public void update(ByteBuffer bytes) {
            crcAccumulate(bytes);
        }

        @Override
        public void update(byte[] bytes, int offset, int length) {
            if (length < 0 || offset < 0 || offset > bytes.length - length) {
                throw new IndexOutOfBoundsException(
                        "offset=" + offset + " length=" + length + " capacity=" + bytes.length);
            }
            int cursor = offset;
            int end = offset + length;
            while (cursor < end) {
                int n = (int) Math.min(end - cursor, rangeRemaining);
                runningWhole.update(bytes, cursor, n);
                runningRange.update(bytes, cursor, n);
                cursor += n;
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
            if (rangeRemaining != ChunkFormats.CRC_RANGE_SIZE) { // a partial final range holds bytes
                return new CrcScan((int) runningWhole.getValue(),
                        completedRangeCrcs.toArrayWithTail((int) runningRange.getValue()));
            }
            return new CrcScan((int) runningWhole.getValue(), completedRangeCrcs.toArray());
        }

        Handle(ChunkId id, ChunkFormats.Header header, StrataNamespace ns) {
            this.id = id;
            this.ns = ns;
            this.mapKey = new ChunkKey(ns, id);
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
            this.mapKey = new ChunkKey(ns, id);
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
            Path tmp = metaPath.resolveSibling(metaPath.getFileName() + ".tmp-"
                    + Thread.currentThread().threadId() + "-" + System.nanoTime());
            boolean moved = false;
            try {
                try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                    writeFully(ch, ByteBuffer.wrap(bytes), 0);
                    if (sync) {
                        ch.force(true);
                    }
                }
                try {
                    Files.move(tmp, metaPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmp, metaPath, StandardCopyOption.REPLACE_EXISTING);
                }
                moved = true;
                if (sync) {
                    forceDirectory(shardDir);
                }
            } finally {
                if (!moved) {
                    try {
                        Files.deleteIfExists(tmp);
                    } catch (IOException e) {
                        log.warn("failed to clean up sidecar temp {}", tmp, e);
                    }
                }
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

    private static final class IntList {
        private int[] values;
        private int size;

        IntList(int initialCapacity) {
            values = initialCapacity <= 0 ? EMPTY_INT_ARRAY : new int[initialCapacity];
        }

        void add(int value) {
            int[] local = values;
            if (size == local.length) {
                values = local = Arrays.copyOf(local, local.length == 0 ? 4 : local.length << 1);
            }
            local[size++] = value;
        }

        int[] toArray() {
            if (size == 0) {
                return EMPTY_INT_ARRAY;
            }
            return size == values.length ? values : Arrays.copyOf(values, size);
        }

        int[] toArrayWithTail(int tail) {
            int[] out = Arrays.copyOf(values, size + 1);
            out[size] = tail;
            return out;
        }
    }

    private Handle lookup(StrataNamespace ns, ChunkId id) {
        Handle h = chunks.get(LOOKUP_KEY.get().set(ns, id));
        if (h == null) throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, id.toString());
        return h;
    }

    private Handle lookup(StrataNamespace ns, long fileId, int chunkIndex) {
        Handle h = chunks.get(LOOKUP_KEY.get().set(ns, fileId, chunkIndex));
        if (h == null) {
            throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, chunkString(fileId, chunkIndex));
        }
        return h;
    }

    private static String chunkString(long fileId, int chunkIndex) {
        String index = Integer.toString(chunkIndex);
        StringBuilder out = new StringBuilder(16 + 1 + index.length());
        FileId.appendHex16(out, fileId);
        out.append('.').append(index);
        return out.toString();
    }

    private final class ChunkHandleMap extends ConcurrentHashMap<ChunkKey, Handle> {
        @Override
        public Handle get(Object key) {
            return super.get(canonicalKey(key));
        }

        @Override
        public boolean containsKey(Object key) {
            return super.containsKey(canonicalKey(key));
        }

        @Override
        public boolean remove(Object key, Object value) {
            return super.remove(canonicalKey(key), value);
        }

        private Object canonicalKey(Object key) {
            if (key instanceof NsChunkId nsChunkId) {
                return new ChunkKey(nsChunkId.namespace(), nsChunkId.chunkId());
            }
            return key;
        }
    }

    private static final class ChunkKey {
        private final StrataNamespace namespace;
        private final long fileId;
        private final int chunkIndex;
        private final int hash;

        private ChunkKey(StrataNamespace namespace, ChunkId chunkId) {
            this(namespace, chunkId.fileId().id(), chunkId.index());
        }

        private ChunkKey(StrataNamespace namespace, long fileId, int chunkIndex) {
            this.namespace = namespace;
            this.fileId = fileId;
            this.chunkIndex = chunkIndex;
            this.hash = hash(namespace, fileId, chunkIndex);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof ChunkKey key) {
                return Objects.equals(namespace, key.namespace)
                        && fileId == key.fileId
                        && chunkIndex == key.chunkIndex;
            }
            if (obj instanceof LookupKey key) {
                return Objects.equals(namespace, key.namespace)
                        && fileId == key.fileId
                        && chunkIndex == key.chunkIndex;
            }
            if (obj instanceof NsChunkId key) {
                return Objects.equals(namespace, key.namespace())
                        && chunkEquals(key.chunkId());
            }
            return false;
        }

        private boolean chunkEquals(ChunkId other) {
            return other != null && fileId == other.fileId().id() && chunkIndex == other.index();
        }

        private static int hash(StrataNamespace namespace, long fileId, int chunkIndex) {
            int result = 1;
            result = 31 * result + Objects.hashCode(namespace);
            result = 31 * result + chunkHash(fileId, chunkIndex);
            return result;
        }

        private static int chunkHash(long fileId, int chunkIndex) {
            int result = Long.hashCode(fileId);
            result = 31 * result + Integer.hashCode(chunkIndex);
            return result;
        }
    }

    private static final class LookupKey {
        private StrataNamespace namespace;
        private long fileId;
        private int chunkIndex;

        private LookupKey set(StrataNamespace namespace, ChunkId chunkId) {
            return set(namespace, chunkId.fileId().id(), chunkId.index());
        }

        private LookupKey set(StrataNamespace namespace, long fileId, int chunkIndex) {
            this.namespace = namespace;
            this.fileId = fileId;
            this.chunkIndex = chunkIndex;
            return this;
        }

        @Override
        public int hashCode() {
            return ChunkKey.hash(namespace, fileId, chunkIndex);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof NsChunkId key) {
                return Objects.equals(namespace, key.namespace())
                        && chunkEquals(key.chunkId());
            }
            if (obj instanceof ChunkKey key) {
                return Objects.equals(namespace, key.namespace)
                        && fileId == key.fileId
                        && chunkIndex == key.chunkIndex;
            }
            if (obj instanceof LookupKey key) {
                return Objects.equals(namespace, key.namespace)
                        && fileId == key.fileId
                        && chunkIndex == key.chunkIndex;
            }
            return false;
        }

        private boolean chunkEquals(ChunkId other) {
            return other != null && fileId == other.fileId().id() && chunkIndex == other.index();
        }
    }

    private void reserveNewChunk(StrataNamespace ns, ChunkId id) {
        NsChunkId reservationKey = new NsChunkId(ns, id);
        if (!creating.add(reservationKey)) throw chunkAlreadyExists(id);
        if (chunks.containsKey(new ChunkKey(ns, id))) {
            creating.remove(reservationKey);
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

    private static void requireRecoveryFence(Handle h, int recoveryEpoch, String op) {
        if (h.fenceEpoch == RECOVERY_FENCE_REQUIRED) {
            throw new ScpException(ErrorCode.FENCED_EPOCH,
                    "fresh recovery fence required for " + h.id, nextEpochAfter(h.writeEpoch));
        }
        if (h.fenceEpoch == recoveryEpoch) {
            return;
        }
        if (h.fenceEpoch > recoveryEpoch) {
            throw new ScpException(ErrorCode.FENCED_EPOCH,
                    op + " recovery epoch " + recoveryEpoch + " < local fence " + h.fenceEpoch
                            + " for chunk " + h.id,
                    h.fenceEpoch);
        }
        throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                op + " requires chunk " + h.id + " to be fenced at recovery epoch "
                        + recoveryEpoch + " (local fence " + h.fenceEpoch + ")");
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
            boolean syncCreate = sealFsync || fsyncOnAck;
            h = new Handle(id, header, ns);
            createDirectories(h.shardDir, syncCreate);
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
            if (syncCreate) {
                h.data.force(true);
            }
            tDataForce = System.nanoTime();
            if (syncCreate) {
                forceDirectory(h.shardDir);
            }
            tDataDirForce = System.nanoTime();
            ledgerOwned = !ledgerPreexisted;
            h.ledger = IntegrityLedger.create(h.ledgerPath,
                    Math.min(OPEN_LEDGER_INITIAL_ENTRIES, csConfig.maxOpenChunkLedgerEntries()));
            tLedgerCreate = System.nanoTime();
            if (syncCreate) {
                forceDirectory(h.shardDir);
            }
            tLedgerDirForce = System.nanoTime();
            h.state = ChunkState.OPEN;
            h.end = 0;
            h.writeEpoch = writeEpoch;
            h.fenceEpoch = -1;
            h.lastKnownDO = 0;
            metaOwned = !metaPreexisted;
            h.persistSidecar(syncCreate);
            tSidecar = System.nanoTime();
            h.startCommitterIfFsync(forceCount);
            chunks.put(h.mapKey, h);
            tInstall = System.nanoTime();
            if (tInstall - t0 > slowMutationLogNanos()) {
                log.info("slow open {} ns={} fsyncOnAck={} phases(ms): dataOpen={} headerWrite={} dataFsync={} "
                                + "dataDirFsync={} ledgerCreate={} ledgerDirFsync={} sidecarPersist={} total={}",
                        id, ns, fsyncOnAck, msBetween(t0, tChannelOpen), msBetween(tChannelOpen, tHeaderWrite),
                        syncCreate ? msBetween(tHeaderWrite, tDataForce) : "-1.0",
                        syncCreate ? msBetween(tDataForce, tDataDirForce) : "-1.0",
                        msBetween(tDataDirForce, tLedgerCreate),
                        syncCreate ? msBetween(tLedgerCreate, tLedgerDirForce) : "-1.0",
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
        boolean syncDelete = sealFsync || h.header.fsyncOnAck();
        deleteOwnedPath(dataCreated, h.dataPath, h.shardDir, "incomplete chunk data", syncDelete);
        deleteOwnedPath(ledgerOwned, h.ledgerPath, h.shardDir, "incomplete chunk ledger", syncDelete);
        deleteOwnedPath(metaOwned, h.metaPath, h.shardDir, "incomplete chunk sidecar", syncDelete);
    }

    private void deleteOwnedPath(boolean owned, Path path, Path containingDir, String description, boolean syncDelete) {
        if (!owned) return;
        try {
            Files.deleteIfExists(path);
            if (syncDelete) {
                forceDirectory(containingDir);
            }
        } catch (IOException e) {
            log.warn("failed to delete {} {}", description, path, e);
        }
    }

    private static void writeFullyPreservingPosition(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        int originalPosition = buf.position();
        try {
            writeFully(ch, buf, position);
        } finally {
            buf.position(originalPosition);
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

    private void deleteLedgerDurably(ChunkId id, Path ledgerPath, Path shardDir) throws IOException {
        long t0 = System.nanoTime();
        Files.deleteIfExists(ledgerPath);
        long tDelete = System.nanoTime();
        forceDirectory(shardDir);
        long tForce = System.nanoTime();
        if (tForce - t0 > slowMutationLogNanos()) {
            log.info("slow durable ledger delete {} phases(ms): delete={} dirFsync={} total={}",
                    id, msBetween(t0, tDelete), msBetween(tDelete, tForce), msBetween(t0, tForce));
        }
    }

    public record AppendResult(long endOffset) {}

    /**
     * Caller-owned append result for hot paths that only need the primitive end offset and an optional
     * durability future. Reuse from one thread at a time; the store resets it before publishing a new
     * outcome.
     */
    public static final class AppendOutcome {
        private long endOffset;
        private CompletableFuture<Void> waitForFlush;

        public long endOffset() {
            return endOffset;
        }

        public CompletableFuture<Void> waitForFlush() {
            return waitForFlush;
        }

        private void reset() {
            endOffset = 0;
            waitForFlush = null;
        }

        private void complete(long endOffset, CompletableFuture<Void> waitForFlush) {
            this.endOffset = endOffset;
            this.waitForFlush = waitForFlush;
        }
    }

    public static AppendPayload byteBufferPayload(ByteBuffer payload) {
        return new ByteBufferAppendPayload(Objects.requireNonNull(payload, "payload"));
    }

    private static final class ByteBufferAppendPayload implements AppendPayload {
        private final ByteBuffer payload;

        private ByteBufferAppendPayload(ByteBuffer payload) {
            this.payload = payload;
        }

        @Override
        public int remaining() {
            return payload.remaining();
        }

        @Override
        public void writeFully(FileChannel channel, long position) throws IOException {
            writeFullyPreservingPosition(channel, payload, position);
        }

        @Override
        public void accumulateCrc(PayloadCrcAccumulator accumulator) {
            accumulator.update(payload);
        }
    }

    /**
     * Validates and writes synchronously (per-chunk ordering and contiguity preserved); the
     * returned future completes when the append is durable per the chunk's ack policy —
     * immediately for ack-on-replicate, after a covering group-commit force for ack-on-fsync.
     */
    public CompletableFuture<AppendResult> appendAsync(
            StrataNamespace ns, ChunkId id, int epoch, long baseOffset, long durableOffset,
            ByteBuffer payload, int payloadCrc) throws IOException {
        return appendAsync(ns, id, epoch, baseOffset, durableOffset, payload, payloadCrc, false);
    }

    public CompletableFuture<AppendResult> appendAsync(
            StrataNamespace ns, ChunkId id, int epoch, long baseOffset, long durableOffset,
            ByteBuffer payload, int payloadCrc, boolean recoveryAppend) throws IOException {
        AppendOutcome outcome = new AppendOutcome();
        appendAsync(ns, id, epoch, baseOffset, durableOffset, payload, payloadCrc, recoveryAppend, outcome);
        long end = outcome.endOffset();
        CompletableFuture<Void> waitForFlush = outcome.waitForFlush();
        if (waitForFlush == null) {
            return CompletableFuture.completedFuture(new AppendResult(end));
        }
        return waitForFlush.thenApply(v -> new AppendResult(end));
    }

    public void appendAsync(
            StrataNamespace ns, ChunkId id, int epoch, long baseOffset, long durableOffset,
            ByteBuffer payload, int payloadCrc, boolean recoveryAppend, AppendOutcome outcome) throws IOException {
        appendAsync0(ns, id, id.fileId().id(), id.index(), epoch, baseOffset, durableOffset,
                byteBufferPayload(payload), payloadCrc, recoveryAppend, outcome);
    }

    public void appendAsync(
            StrataNamespace ns, long fileId, int chunkIndex, int epoch, long baseOffset, long durableOffset,
            ByteBuffer payload, int payloadCrc, boolean recoveryAppend, AppendOutcome outcome) throws IOException {
        appendAsync0(ns, null, fileId, chunkIndex, epoch, baseOffset, durableOffset,
                byteBufferPayload(payload), payloadCrc, recoveryAppend, outcome);
    }

    public void appendAsync(
            StrataNamespace ns, long fileId, int chunkIndex, int epoch, long baseOffset, long durableOffset,
            AppendPayload payload, int payloadCrc, boolean recoveryAppend, AppendOutcome outcome) throws IOException {
        appendAsync0(ns, null, fileId, chunkIndex, epoch, baseOffset, durableOffset,
                Objects.requireNonNull(payload, "payload"), payloadCrc, recoveryAppend, outcome);
    }

    private void appendAsync0(
            StrataNamespace ns, ChunkId requestedId, long fileId, int chunkIndex, int epoch,
            long baseOffset, long durableOffset, AppendPayload payload, int payloadCrc,
            boolean recoveryAppend, AppendOutcome outcome) throws IOException {
        Objects.requireNonNull(outcome, "outcome").reset();
        Handle h = requestedId != null ? lookup(ns, requestedId) : lookup(ns, fileId, chunkIndex);
        ChunkId id = h.id;
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
            if (h.sealFinalizationFailed) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "append refused after failed seal finalization for " + id);
            }
            if (h.header.fsyncOnAck() && h.committer == null) {
                // A post-detach seal-finalization failure can leave partially truncated files. Do not
                // mutate them further, and never downgrade this durability tier to page-cache-only acks.
                // A seal retry or process recovery is responsible for converging the chunk.
                throw new ScpException(ErrorCode.INTERNAL,
                        "fsync-on-ack committer unavailable after failed seal for " + id);
            }
            h.writeEpoch = Math.max(h.writeEpoch, epoch);
            if (baseOffset != h.end) {
                throw new ScpException(ErrorCode.OFFSET_GAP, "expected " + h.end + " got " + baseOffset, h.end);
            }
            h.lastKnownDO = Math.max(h.lastKnownDO, Math.min(durableOffset, h.end));
            len = payload.remaining();
            if (len == 0) {
                outcome.complete(h.end, null); // DO beacon
                return;
            }
            if (!recoveryAppend && h.ledger.size() >= csConfig.maxOpenChunkLedgerEntries()) {
                throw new ScpException(ErrorCode.CHUNK_SEALED,
                        "open chunk ledger entry cap reached for " + id + ": "
                                + h.ledger.size() + " >= " + csConfig.maxOpenChunkLedgerEntries(),
                        h.end);
            }
            newEnd = checkedAdd(baseOffset, len, "chunk offset");
            long writePos = checkedAdd(DATA_START, baseOffset, "chunk file offset");
            payload.writeFully(h.data, writePos);
            tWrite = System.nanoTime();
            h.ledger.append(newEnd, payloadCrc, epoch);
            tLedger = System.nanoTime();
            // Fold into the running whole + range CRCs ONLY after the data + ledger writes commit: a
            // throwing ledger.append must leave the accumulators (and h.end) untouched, or a same-offset
            // retry on this Handle would fold the same bytes twice and corrupt the seal-time snapshot.
            // crcAccumulate is CPU-only for the built-in payloads; it preserves append order.
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
        if (tUnlock - t0 > slowAppendLogNanos()) {
            log.info("slow append {} len={} base={} phases(ms): lockWait={} dataWrite={} "
                            + "ledgerAppend={} runningCrc={} lockHeld={} total={}",
                    id, len, baseOffset, msBetween(tBeforeLock, tLock),
                    msBetween(tLock, tWrite), msBetween(tWrite, tLedger), msBetween(tLedger, tRunningCrc),
                    msBetween(tLock, tRunningCrc), msBetween(t0, tUnlock));
        }
        if (committer == null) {
            outcome.complete(newEnd, null);
            return;
        }
        outcome.complete(newEnd, committer.awaitFlush(newEnd));
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

    /** A verified read response whose payload buffer remains owned until {@link #close()}. */
    public static final class ReadRegionResult implements AutoCloseable {
        private static final AtomicIntegerFieldUpdater<ReadRegionResult> CLOSED =
                AtomicIntegerFieldUpdater.newUpdater(ReadRegionResult.class, "closed");
        private BufferSlot slot = BufferSlot.EMPTY;
        private ReadBufferPool slotOwner;
        private ReadBufferPool resultOwner;
        private int length;
        private long localEndOffset;
        private long lastKnownDO;
        @SuppressWarnings("unused") // updated through CLOSED
        private volatile int closed = 1;

        private ReadRegionResult() {
        }

        private ReadRegionResult init(BufferSlot slot, ReadBufferPool slotOwner, ReadBufferPool resultOwner,
                                      int length, long localEndOffset, long lastKnownDO) {
            this.slot = Objects.requireNonNull(slot, "slot");
            this.slotOwner = slotOwner;
            this.resultOwner = Objects.requireNonNull(resultOwner, "resultOwner");
            if (length < 0 || length > slot.bytes().length) {
                throw new IllegalArgumentException("invalid read length " + length
                        + " for buffer length " + slot.bytes().length);
            }
            this.length = length;
            this.localEndOffset = localEndOffset;
            this.lastKnownDO = lastKnownDO;
            closed = 0;
            return this;
        }

        public byte[] bytes() {
            return length == 0 ? EMPTY_READ_BYTES : Arrays.copyOf(slot.bytes(), length);
        }

        public ByteBuffer payloadBuffer() {
            return length == 0 ? null : ByteBuffer.wrap(slot.bytes(), 0, length);
        }

        public byte[] payloadBytes() {
            return slot.bytes();
        }

        public int length() {
            return length;
        }

        public long localEndOffset() {
            return localEndOffset;
        }

        public long lastKnownDO() {
            return lastKnownDO;
        }

        byte[] array() {
            return slot.bytes();
        }

        ByteBuffer slice(int offset, int length) {
            return slot.slice(offset, length);
        }

        @Override
        public void close() {
            if (!CLOSED.compareAndSet(this, 0, 1)) {
                return;
            }
            BufferSlot localSlot = slot;
            ReadBufferPool localSlotOwner = slotOwner;
            ReadBufferPool localResultOwner = resultOwner;
            slot = BufferSlot.EMPTY;
            slotOwner = null;
            resultOwner = null;
            length = 0;
            localEndOffset = 0;
            lastKnownDO = 0;
            if (localSlotOwner != null) {
                localSlotOwner.release(localSlot);
            }
            if (localResultOwner != null) {
                localResultOwner.releaseResult(this);
            }
        }
    }

    private static final class ReadBuffer implements AutoCloseable {
        private static final ReadBuffer EMPTY = new ReadBuffer(BufferSlot.EMPTY, null);
        private static final AtomicIntegerFieldUpdater<ReadBuffer> CLOSED =
                AtomicIntegerFieldUpdater.newUpdater(ReadBuffer.class, "closed");
        private final BufferSlot slot;
        private final ReadBufferPool owner;
        @SuppressWarnings("unused") // updated through CLOSED
        private volatile int closed;

        private ReadBuffer(BufferSlot slot, ReadBufferPool owner) {
            this.slot = Objects.requireNonNull(slot, "slot");
            this.owner = owner;
        }

        static ReadBuffer unpooled(byte[] bytes) {
            if (bytes.length == 0) {
                return EMPTY;
            }
            return new ReadBuffer(new BufferSlot(bytes), null);
        }

        byte[] bytes() {
            return slot.bytes();
        }

        ByteBuffer slice(int offset, int length) {
            return slot.slice(offset, length);
        }

        @Override
        public void close() {
            if (owner != null && CLOSED.compareAndSet(this, 0, 1)) {
                owner.release(slot);
            }
        }
    }

    private static final class BufferSlot {
        private static final BufferSlot EMPTY = new BufferSlot(EMPTY_READ_BYTES);
        private final byte[] bytes;
        private final ByteBuffer view;

        private BufferSlot(byte[] bytes) {
            this.bytes = Objects.requireNonNull(bytes, "bytes");
            this.view = ByteBuffer.wrap(bytes);
        }

        private byte[] bytes() {
            return bytes;
        }

        private ByteBuffer slice(int offset, int length) {
            view.clear();
            view.position(offset);
            view.limit(offset + length);
            return view;
        }

        private void resetForAcquire() {
            view.clear();
        }
    }

    private static final class ReadBufferPool {
        private static final int MAX_BUCKETS = 32;
        private static final int OVERFLOW_BUCKET_INDEX = MAX_BUCKETS - 1;

        private final int maxBufferBytes;
        private final int maxBuffers;
        private final AtomicReferenceArray<Bucket> buckets = new AtomicReferenceArray<>(MAX_BUCKETS);
        private final AtomicInteger pooled = new AtomicInteger();
        private final ArrayDeque<ReadRegionResult> results = new ArrayDeque<>();

        private ReadBufferPool(int maxBufferBytes, int maxBuffers) {
            this.maxBufferBytes = Math.max(0, maxBufferBytes);
            this.maxBuffers = Math.max(0, maxBuffers);
        }

        ReadBuffer acquire(int length) {
            if (length == 0) {
                return ReadBuffer.unpooled(EMPTY_READ_BYTES);
            }
            int capacity = pooledCapacity(length);
            return new ReadBuffer(acquireSlot(length, capacity), poolable(capacity) ? this : null);
        }

        ReadRegionResult acquireResult(int length, long localEndOffset, long lastKnownDO) {
            if (length == 0) {
                return acquireResult(BufferSlot.EMPTY, null, 0, localEndOffset, lastKnownDO);
            }
            int capacity = pooledCapacity(length);
            return acquireResult(acquireSlot(length, capacity), poolable(capacity) ? this : null,
                    length, localEndOffset, lastKnownDO);
        }

        private ReadRegionResult acquireResult(BufferSlot slot, ReadBufferPool slotOwner, int length,
                                               long localEndOffset, long lastKnownDO) {
            ReadRegionResult result = null;
            if (maxBuffers > 0) {
                synchronized (results) {
                    result = results.pollFirst();
                }
            }
            if (result == null) {
                result = new ReadRegionResult();
            }
            return result.init(slot, slotOwner, this, length, localEndOffset, lastKnownDO);
        }

        private BufferSlot acquireSlot(int length, int capacity) {
            if (!poolable(capacity)) {
                return new BufferSlot(new byte[length]);
            }
            BufferSlot pooledSlot = poll(capacity);
            return pooledSlot != null ? pooledSlot : new BufferSlot(new byte[capacity]);
        }

        private BufferSlot poll(int length) {
            Bucket bucket = findBucket(length);
            if (bucket == null) {
                return null;
            }
            synchronized (bucket.buffers) {
                BufferSlot slot = bucket.buffers.pollFirst();
                if (slot != null) {
                    pooled.decrementAndGet();
                    slot.resetForAcquire();
                }
                return slot;
            }
        }

        void release(BufferSlot slot) {
            if (!poolable(slot.bytes().length)) {
                return;
            }
            while (true) {
                int current = pooled.get();
                if (current >= maxBuffers) {
                    return;
                }
                if (pooled.compareAndSet(current, current + 1)) {
                    break;
                }
            }
            Bucket bucket = bucketFor(slot.bytes().length);
            if (bucket == null) {
                pooled.decrementAndGet();
                return;
            }
            synchronized (bucket.buffers) {
                bucket.buffers.addFirst(slot);
            }
        }

        void releaseResult(ReadRegionResult result) {
            if (maxBuffers <= 0) {
                return;
            }
            synchronized (results) {
                if (results.size() < maxBuffers) {
                    results.addFirst(result);
                }
            }
        }

        private boolean poolable(int length) {
            return length > 0 && length <= maxBufferBytes && maxBuffers > 0;
        }

        private int pooledCapacity(int length) {
            if (length <= 0 || length > maxBufferBytes) {
                return length;
            }
            if (length == 1) {
                return 1;
            }
            if (length > (1 << 30)) {
                return length;
            }
            int capacity = 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(length - 1));
            return Math.min(capacity, maxBufferBytes);
        }

        private Bucket findBucket(int length) {
            int directIndex = bucketIndex(length);
            Bucket direct = buckets.get(directIndex);
            if (direct != null && direct.length == length) {
                return direct;
            }
            return findBucketSlow(length, directIndex);
        }

        private Bucket findBucketSlow(int length, int skipIndex) {
            for (int i = 0; i < buckets.length(); i++) {
                if (i == skipIndex) {
                    continue;
                }
                Bucket bucket = buckets.get(i);
                if (bucket != null && bucket.length == length) {
                    return bucket;
                }
            }
            return null;
        }

        private Bucket bucketFor(int length) {
            int directIndex = bucketIndex(length);
            Bucket existing = buckets.get(directIndex);
            if (existing != null) {
                if (existing.length == length) {
                    return existing;
                }
            } else {
                Bucket created = new Bucket(length);
                if (buckets.compareAndSet(directIndex, null, created)) {
                    return created;
                }
                existing = buckets.get(directIndex);
                if (existing != null && existing.length == length) {
                    return existing;
                }
            }
            return bucketForSlow(length, directIndex);
        }

        private Bucket bucketForSlow(int length, int skipIndex) {
            Bucket existing = findBucketSlow(length, skipIndex);
            if (existing != null) {
                return existing;
            }
            Bucket created = new Bucket(length);
            for (int i = 0; i < buckets.length(); i++) {
                if (i == skipIndex) {
                    continue;
                }
                Bucket bucket = buckets.get(i);
                if (bucket != null) {
                    if (bucket.length == length) {
                        return bucket;
                    }
                    continue;
                }
                if (buckets.compareAndSet(i, null, created)) {
                    return created;
                }
            }
            return null;
        }

        private static int bucketIndex(int length) {
            if (length <= 1) {
                return 0;
            }
            if ((length & (length - 1)) == 0) {
                return Integer.numberOfTrailingZeros(length);
            }
            return OVERFLOW_BUCKET_INDEX;
        }

        private static final class Bucket {
            private final int length;
            private final ArrayDeque<BufferSlot> buffers = new ArrayDeque<>();

            private Bucket(int length) {
                this.length = length;
            }
        }
    }

    public ReadResult read(StrataNamespace ns, ChunkId id, long offset, int maxBytes) throws IOException {
        try (ReadRegionResult r = readRegion0(ns, id, id.fileId().id(), id.index(), offset, maxBytes, true, 0)) {
            return new ReadResult(r.bytes(), r.localEndOffset(), r.lastKnownDO());
        }
    }

    public ReadRegionResult readRegion(StrataNamespace ns, ChunkId id, long offset, int maxBytes) throws IOException {
        return readRegion0(ns, id, id.fileId().id(), id.index(), offset, maxBytes, false, 0);
    }

    public ReadRegionResult readRegion(StrataNamespace ns, long fileId, int chunkIndex, long offset, int maxBytes)
            throws IOException {
        return readRegion0(ns, null, fileId, chunkIndex, offset, maxBytes, false, 0);
    }

    /**
     * Seal-recovery variant of {@link #readRegion}: serves locally-present bytes up to the chunk's
     * local end, INCLUDING the never-acked tail above the durable high watermark. Recovery must see
     * that tail to decide whether a quorum still holds it (tech design §7.3); clamping it away — as
     * the client read path does — makes recovery seal short and drop quorum-durable bytes. Reads are
     * still integrity-verified (open chunks against the ledger, sealed chunks against footer CRC
     * ranges), and the recovery path does not count toward client read throughput metrics.
     * A zero recovery epoch is reserved for explicit trusted in-process inspection; wire callers
     * must supply the positive epoch persisted by {@link #fence}.
     */
    public ReadRegionResult readRegionForRecovery(StrataNamespace ns, long fileId, int chunkIndex,
                                                  long offset, int maxBytes, int recoveryEpoch) throws IOException {
        return readRegion0(ns, null, fileId, chunkIndex, offset, maxBytes, true, recoveryEpoch);
    }

    private ReadRegionResult readRegion0(StrataNamespace ns, ChunkId requestedId, long fileId, int chunkIndex,
                                         long offset, int maxBytes, boolean includeUndurableTail,
                                         int recoveryEpoch)
            throws IOException {
        requireNonNegative(offset, "read offset");
        requireNonNegative(maxBytes, "read maxBytes");
        Handle h = requestedId != null ? lookup(ns, requestedId) : lookup(ns, fileId, chunkIndex);
        ChunkId id = h.id;
        long localEnd;
        long lastKnownDO;
        int n;
        Path dataPath = null;
        NsChunkId nsKey = null;
        IntegrityLedger.EntrySpan openReadSpan = null;
        long sealedLength = -1;
        int[] sealedRangeCrcs = null;
        h.lock.lock();
        try {
            if (recoveryEpoch > 0) {
                requireRecoveryFence(h, recoveryEpoch, "READ_RECOVERY");
            }
            localEnd = h.currentEnd();
            lastKnownDO = h.lastKnownDO;
            long readableEnd = (h.state == ChunkState.SEALED || includeUndurableTail)
                    ? localEnd : Math.min(localEnd, lastKnownDO);
            if (offset >= readableEnd) {
                return readBufferPool.acquireResult(0, localEnd, lastKnownDO);
            }
            n = (int) Math.min(Math.min(maxBytes, csConfig.maxRequestBytes()), readableEnd - offset);
            if (n == 0) {
                return readBufferPool.acquireResult(0, localEnd, lastKnownDO);
            }
            if (h.state != ChunkState.SEALED) {
                if (!includeUndurableTail) {
                    if (h.ledger == null) {
                        throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                                "open chunk missing integrity ledger: " + h.id);
                    }
                    openReadSpan = h.ledger.reusableEntriesCovering(offset, checkedAdd(offset, n, "open read end"));
                    dataPath = h.dataPath;
                    nsKey = h.nsKey;
                } else {
                    // Recovery may inspect bytes above lastKnownDO; that tail can be seal-truncated, so keep
                    // the read and CRC under the chunk lock.
                    ReadRegionResult out = readBufferPool.acquireResult(n, localEnd, lastKnownDO);
                    boolean success = false;
                    try {
                        readOpenVerified(h, offset, out);
                        success = true;
                        return out;
                    } finally {
                        if (!success) {
                            out.close();
                        }
                    }
                }
            } else {
                sealedLength = h.sealedLength;
                sealedRangeCrcs = h.sealedRangeCrcs;
                dataPath = h.dataPath;
                nsKey = h.nsKey;
            }
        } finally {
            h.lock.unlock();
        }
        // Test seam: lets a test delete + re-import this id in the window between the lock release and the
        // off-lock channel acquisition below, to exercise the post-open handle revalidation.
        FailureInjector.point("format.readRegion.beforeOpen");
        if (openReadSpan != null) {
            // Client open reads are clamped to lastKnownDO. Seal may not truncate below that floor, so the
            // verified disk I/O can run off-lock against an independent FD after handle revalidation.
            ReadRegionResult out = readBufferPool.acquireResult(n, localEnd, lastKnownDO);
            boolean success = false;
            try (ChannelCache.Lease lease = channelCache.acquire(nsKey, dataPath)) {
                requireCurrentHandle(h, id);
                readOpenVerified(lease.channel(), openReadSpan, id, offset, out);
                countClientRead(ns, n);
                success = true;
                return out;
            } finally {
                openReadSpan.clear();
                if (!success) {
                    out.close();
                }
            }
        }
        // SEALED READs are immutable, so we can verify off-lock against the footer CRC ranges. Both client
        // reads and recovery reads use this path: a corrupt local block fails before the node writes a READ
        // response, instead of waiting for background scrub.
        ReadRegionResult out = readBufferPool.acquireResult(n, localEnd, lastKnownDO);
        boolean success = false;
        try (ChannelCache.Lease lease = channelCache.acquire(nsKey, dataPath)) {
            requireCurrentHandle(h, id);
            readSealedVerified(lease.channel(), sealedLength, sealedRangeCrcs, id, offset, out);
            if (!includeUndurableTail) {
                countClientRead(ns, n);
            }
            success = true;
            return out;
        } finally {
            if (!success) {
                out.close();
            }
        }
    }

    private void countClientRead(StrataNamespace ns, int n) {
        // observability: count client READ bytes served after verification (mirrors append counters; drives
        // read throughput). Recovery reads are internal control-plane traffic, not client reads.
        readOps.incrementAndGet();
        readBytes.addAndGet(n);
        var nsCounters = nsIoFor(ns);
        nsCounters[2].increment();
        nsCounters[3].add(n);
    }

    /**
     * Revalidates, after an off-lock open/acquire in {@link #readRegion}, that this handle is still the
     * mapped one for its id. A delete + re-import for the same {@link NsChunkId} (same data path) in the
     * window between the metadata snapshot and the open would otherwise bind the stale snapshot
     * (length/state) to the freshly-replaced physical file. A {@code Handle} is in the map iff it has not
     * been deleted, so an identity mismatch means the snapshot is stale: fail with CHUNK_NOT_FOUND and let
     * the client retry — the same contract as opening an already-unlinked file. Both open client reads and
     * sealed reads revalidate after their off-lock open/acquire before trusting the snapshot.
     */
    private void requireCurrentHandle(Handle h, ChunkId id) {
        if (chunks.get(h.mapKey) != h) {
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
            // Load-bearing invariant: a seal may never floor below the durable high watermark. Client
            // reads clamp open chunks to lastKnownDO, and recovery relies on the floor too because it must
            // not drop quorum-durable bytes. Do not relax without revisiting both.
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
            if (h.sealFinalizationFailed && dataLength > h.failedSealLength) {
                // A partial footer/trailer can extend beyond the truncated data. Treating a larger retry
                // length as data could seal those footer bytes, so retries may only keep or lower the target.
                throw new ScpException(ErrorCode.INTERNAL,
                        "failed seal retry for " + id + " must not exceed length " + h.failedSealLength
                                + ", got " + dataLength,
                        h.failedSealLength);
            }
            // Validate the caller footer + snapshot CRCs BEFORE stopping the committer: a caller-validation
            // or read-verification failure must leave an OPEN ack-on-fsync chunk with its committer running.
            prep = prepareSealLocked(h, callerSections, dataLength);
            committerToStop = h.committer;
            if (committerToStop == null) {
                return finalizeSealFailClosed(h, id, dataLength, prep); // nothing to drain off-lock
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
            if (h.state == ChunkState.DELETING || chunks.get(h.mapKey) != h) {
                // a concurrent delete won the released-lock window; it owns teardown. Abort the seal.
                h.sealing = false;
                throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, id.toString());
            }
            h.committer = null; // confirmed stopped above; detach before mutating files
            try {
                // fence() can advance the epoch while the committer is stopped off-lock. Do not let a
                // sealer validated under the old epoch publish after that newer fence became durable.
                try {
                    checkEpoch(h, epoch);
                } catch (RuntimeException staleSeal) {
                    // No file mutation has started, so this abort can safely restore clean OPEN service.
                    h.startCommitterIfFsync(forceCount);
                    throw staleSeal;
                }
                // Do not restart here if finalization throws: truncate/ledger/footer mutation may be
                // partial. finalizeSealFailClosed poisons further OPEN appends, and a retry can safely
                // re-run finalization from its original logical end.
                return finalizeSealFailClosed(h, id, dataLength, prep);
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
        // Once finalization has touched the files and failed, the in-memory running CRC may describe
        // bytes that a partial truncate removed. Every retry must rescan physical data, even when the
        // requested length still equals the pre-failure logical end.
        CrcScan scan = dataLength == h.end && !h.sealFinalizationFailed
                ? h.snapshotRunningCrcs()
                : scanDataCrcs(h.data, dataLength);
        int ledgerEntryCount = ledgerEntriesThroughSeal(h.ledger, dataLength);
        return new SealPrep(caller, scan, ledgerEntryCount);
    }

    /** Runs finalization and poisons further OPEN appends if file mutation fails partway through. */
    private SealResult finalizeSealFailClosed(Handle h, ChunkId id, long dataLength, SealPrep prep)
            throws IOException {
        try {
            return finalizeSealLocked(h, id, dataLength, prep);
        } catch (IOException | RuntimeException | Error failure) {
            if (h.state == ChunkState.OPEN) {
                if (!h.sealFinalizationFailed) {
                    h.failedSealLength = dataLength;
                } else {
                    h.failedSealLength = Math.min(h.failedSealLength, dataLength);
                }
                h.sealFinalizationFailed = true;
            }
            throw failure;
        }
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
            // From this point until SEALED is published, the safe recovery path for an I/O failure is
            // a seal retry: retry re-truncates and re-appends the synthetic boundary before publishing.
            h.data.truncate(checkedAdd(DATA_START, dataLength, "chunk file offset"));
            h.ledger.truncateTo(dataLength);
            appendSealBoundaryLedgerEntryIfNeeded(h, dataLength);
            FailureInjector.point("format.seal.afterTruncate");
        }
        long tTruncate = System.nanoTime();

        int[] scanRangeCrcs = scan.rangeCrcs;
        ByteBuffer crcRanges = ByteBuffer.allocate(4 + 4 + scanRangeCrcs.length * 4);
        crcRanges.putInt(ChunkFormats.CRC_RANGE_SIZE).putInt(scanRangeCrcs.length);
        for (int c : scanRangeCrcs) crcRanges.putInt(c);
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

        h.end = dataLength;
        h.state = ChunkState.SEALED;
        h.sealedLength = dataLength;
        h.dataCrc = scan.dataCrc;
        h.sealedRangeCrcs = scanRangeCrcs;
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
            // the ledger. Make the ledger unlink durable before the OPEN sidecar unlink can become durable:
            // otherwise recovery may see no sidecar plus a resurrected stale ledger and discard the chunk.
            deleteLedgerDurably(id, ledgerPath, h.shardDir);
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
        if (tLedgerDeleteEnqueue - t0 > slowMutationLogNanos()) {
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

    private record CrcScan(int dataCrc, int[] rangeCrcs) {}

    private record CallerSections(int count, byte[] bytes) {}

    private static int ledgerEntriesThroughSeal(IntegrityLedger ledger, long endOffset) {
        int count = ledger.entriesThrough(endOffset);
        long lastEnd = count == 0 ? 0 : ledger.endOffsetAt(count - 1);
        if (endOffset > lastEnd) {
            count++;
        }
        return count;
    }

    private void appendSealBoundaryLedgerEntryIfNeeded(Handle h, long dataLength) throws IOException {
        long ledgerEnd = h.ledger.lastEndOffset();
        if (ledgerEnd == dataLength) {
            if (h.sealFinalizationFailed) {
                // The first attempt may have appended this boundary and then failed its force. A poisoned
                // retry must re-force it before publishing SEALED; in-memory presence is not durability.
                forceSealBoundary(h);
            }
            return;
        }
        if (ledgerEnd > dataLength) {
            throw new ScpException(ErrorCode.INTERNAL,
                    "seal ledger boundary beyond data length: " + ledgerEnd + " > " + dataLength);
        }
        int crc = crcDataRange(h.data, ledgerEnd, dataLength);
        h.ledger.append(dataLength, crc, h.writeEpoch);
        // Keep this force even for sealFsync=true: until deleteLedgerDurably completes, recovery can
        // still see the retained ledger beside a durable trailer and needs the boundary to classify it.
        forceSealBoundary(h);
    }

    private void forceSealBoundary(Handle h) throws IOException {
        FailureInjector.point("format.seal.beforeBoundaryForce");
        h.ledger.force();
    }

    private int crcDataRange(FileChannel data, long start, long end) throws IOException {
        CRC32C crc = new CRC32C();
        byte[] buf = new byte[1 << 20];
        long pos = start;
        while (pos < end) {
            int n = (int) Math.min(buf.length, end - pos);
            ByteBuffer bb = ByteBuffer.wrap(buf, 0, n);
            readFully(data, bb, checkedAdd(DATA_START, pos, "chunk file offset"));
            crc.update(buf, 0, n);
            pos += n;
        }
        return (int) crc.getValue();
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
        return scanDataCrcs(data, dataLength, new byte[CRC_SCAN_BUFFER_BYTES]);
    }

    private CrcScan scanDataCrcs(FileChannel data, long dataLength, byte[] buf) throws IOException {
        CRC32C whole = new CRC32C();
        IntList ranges = new IntList(INITIAL_RANGE_CRC_ENTRIES);
        ByteBuffer bb = ByteBuffer.wrap(buf);
        long pos = 0;
        CRC32C range = new CRC32C();
        long rangeRemaining = ChunkFormats.CRC_RANGE_SIZE;
        while (pos < dataLength) {
            int n = (int) Math.min(buf.length, Math.min(dataLength - pos, rangeRemaining));
            bb.clear().limit(n);
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
        return new CrcScan((int) whole.getValue(), ranges.toArray());
    }

    private int scanDataCrc(FileChannel data, long dataLength, byte[] buf) throws IOException {
        CRC32C whole = new CRC32C();
        ByteBuffer bb = ByteBuffer.wrap(buf);
        long pos = 0;
        while (pos < dataLength) {
            int n = (int) Math.min(buf.length, dataLength - pos);
            bb.clear().limit(n);
            readFully(data, bb, checkedAdd(DATA_START, pos, "chunk file offset"));
            whole.update(buf, 0, n);
            pos += n;
        }
        return (int) whole.getValue();
    }

    /** Zero recovery epoch is reserved for explicit trusted in-process inspection. */
    public List<ChunkFormats.LedgerEntry> readLedger(StrataNamespace ns, ChunkId id, long fromOffset,
                                                     int recoveryEpoch) {
        requireNonNegative(fromOffset, "ledger offset");
        Handle h = lookup(ns, id);
        h.lock.lock();
        try {
            if (recoveryEpoch > 0) {
                requireRecoveryFence(h, recoveryEpoch, "READ_LEDGER");
            }
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
            int[] rangeCrcs;
            try (FileChannel input = FileChannel.open(sourceFile, READ_OPEN_OPTIONS)) {
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
            createDirectories(h.shardDir, true);
            if (Files.exists(h.dataPath)) throw chunkAlreadyExists(id);
            boolean movedData = false;
            boolean sidecarStarted = false;
            boolean installed = false;
            try {
                try (FileChannel ch = FileChannel.open(sourceFile, StandardOpenOption.WRITE)) {
                    ch.force(true);
                }
                Path sourceParent = sourceFile.getParent();
                Files.move(sourceFile, h.dataPath, StandardCopyOption.ATOMIC_MOVE);
                forceDirectory(h.shardDir);
                forceSourceDirectoryAfterMove(sourceParent, h.shardDir);
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
                long now = System.currentTimeMillis();
                h.lastVerifiedAtMs = now;
                h.orphanProtectedUntilMs = now + REPAIR_IMPORT_ORPHAN_PROTECTION_MS;
                // Durability-v2 (Lever 1): an imported sealed chunk carries a verified trailer and no
                // ledger, which recovery classifies as SEALED — no sidecar written.
                Files.deleteIfExists(h.ledgerPath);
                forceDirectory(h.shardDir);
                chunks.put(h.mapKey, h);
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
            forceParentDirectory(tmp);
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
        ChunkKey key = new ChunkKey(ns, id);
        Handle h = chunks.get(key);
        if (h == null && creating.contains(new NsChunkId(ns, id))) return ErrorCode.INTERNAL;
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
    private ErrorCode deleteLocked(Handle h, ChunkKey key, ChunkId id, long t0) {
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
        channelCache.invalidate(h.nsKey);
        if (System.nanoTime() - t0 > slowMutationLogNanos()) {
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
     * Owner-pull verification (design §9.2): report the local state of each requested chunk and stamp
     * the present ones as freshly verified — which both refreshes the orphan-GC grace and lets
     * the owner compare state/length/crc against its descriptor to find missing/corrupt replicas. An
     * absent chunk reports {@code present == false} (a missing replica). Read-only on the data itself.
     */
    public List<VerifyResult> verify(StrataNamespace ns, List<ChunkId> chunkIds) {
        long now = System.currentTimeMillis();
        List<VerifyResult> out = new ArrayList<>(chunkIds.size());
        for (ChunkId id : chunkIds) {
            Handle h = chunks.get(new ChunkKey(ns, id));
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
     * Node-local orphan-GC candidates (design §9.2): sealed chunks no owner has attested within
     * {@code olderThanMs} (via {@link #verify}). Open chunks (in-flight writes) and freshly-known chunks
     * (still inside grace) are excluded. Returns a snapshot; the caller confirms each with the owner
     * before deleting — a suspect is not yet a confirmed orphan.
     */
    public List<SuspectChunk> orphanSuspects(long olderThanMs, long now) {
        List<SuspectChunk> out = new ArrayList<>();
        for (Handle h : chunks.values()) {
            h.lock.lock();
            try {
                if (h.state == ChunkState.SEALED
                        && now >= h.orphanProtectedUntilMs
                        && now - h.lastVerifiedAtMs >= olderThanMs) {
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
        return chunks.containsKey(new ChunkKey(ns, id));
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
        byte[] scanBuffer = null;
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
            if (sealedLength == 0) {
                actual = 0;
            } else {
                if (scanBuffer == null) {
                    scanBuffer = new byte[CRC_SCAN_BUFFER_BYTES];
                }
                try (ChannelCache.Lease lease = channelCache.acquire(h.nsKey, dataPath)) {
                    actual = scanDataCrc(lease.channel(), sealedLength, scanBuffer);
                }
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

        // Single pass: partition store-root entries into namespace dirs and stray flat files.
        List<Path> nsDirs = new ArrayList<>();
        List<Path> rootChunks = new ArrayList<>();
        List<Path> rootImports = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dir)) {
            stream.forEach(p -> {
                String name = p.getFileName().toString();
                if (Files.isDirectory(p)) {
                    nsDirs.add(p);
                } else if (name.endsWith(".chunk")) {
                    rootChunks.add(p);
                } else if (name.endsWith(".import")) {
                    rootImports.add(p);
                }
            });
        }
        deleteStaleRootImportTemps(rootImports);
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
        // Collect files first (crash-safety: avoid delete-during-walk)
        List<Path> chunkFiles;
        List<Path> sidecarFiles;
        try (Stream<Path> files = Files.walk(nsDir)) {
            List<Path> recoveredFiles = files
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.endsWith(".chunk") || name.endsWith(".meta") || name.endsWith(".j");
                    })
                    .toList();
            chunkFiles = recoveredFiles.stream()
                    .filter(p -> p.getFileName().toString().endsWith(".chunk"))
                    .toList();
            sidecarFiles = recoveredFiles.stream()
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.endsWith(".meta") || name.endsWith(".j");
                    })
                    .toList();
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
        quarantineOrphanSidecarFiles(sidecarFiles);
        removeSidecarTempFiles(nsDir);
    }

    private void deleteStaleRootImportTemps(List<Path> rootImports) {
        if (rootImports.isEmpty()) {
            return;
        }
        boolean deleted = false;
        for (Path p : rootImports) {
            try {
                if (Files.deleteIfExists(p)) {
                    deleted = true;
                    log.warn("deleted stale import temp file left in store root: {}", p);
                }
            } catch (IOException e) {
                log.warn("failed to delete stale import temp file {}", p, e);
            }
        }
        if (deleted) {
            try {
                forceDirectory(dir);
            } catch (IOException e) {
                log.warn("failed to fsync store root after import-temp cleanup {}", dir, e);
            }
        }
    }

    private void quarantineOrphanSidecarFiles(List<Path> sidecarFiles) {
        if (sidecarFiles.isEmpty()) {
            return;
        }
        String suffix = ".quarantine-" + System.currentTimeMillis();
        Set<Path> touchedDirs = new HashSet<>();
        for (Path p : sidecarFiles) {
            if (!Files.exists(p)) {
                continue;
            }
            Path dataPath = sidecarDataPath(p);
            if (Files.exists(dataPath)) {
                continue;
            }
            try {
                Files.move(p, quarantineTarget(p, suffix), StandardCopyOption.ATOMIC_MOVE);
                touchedDirs.add(p.getParent());
                log.warn("quarantined orphan chunk sidecar without data file: {}", p);
            } catch (IOException e) {
                log.warn("failed to quarantine orphan chunk sidecar {}", p, e);
            }
        }
        for (Path touchedDir : touchedDirs) {
            try {
                forceDirectory(touchedDir);
            } catch (IOException e) {
                log.warn("failed to fsync sidecar quarantine directory {}", touchedDir, e);
            }
        }
    }

    private static Path sidecarDataPath(Path sidecarPath) {
        String name = sidecarPath.getFileName().toString();
        if (name.endsWith(".meta")) {
            return sidecarPath.resolveSibling(name.substring(0, name.length() - ".meta".length()) + ".chunk");
        }
        return sidecarPath.resolveSibling(name.substring(0, name.length() - ".j".length()) + ".chunk");
    }

    private void removeSidecarTempFiles(Path nsDir) throws IOException {
        List<Path> tempFiles;
        try (Stream<Path> files = Files.walk(nsDir)) {
            tempFiles = files.filter(ChunkStore::isSidecarTempFile).toList();
        }
        Set<Path> dirtiedDirs = new HashSet<>();
        for (Path p : tempFiles) {
            try {
                if (Files.deleteIfExists(p)) {
                    dirtiedDirs.add(p.getParent());
                }
            } catch (IOException e) {
                log.warn("failed to remove sidecar temp file {}", p, e);
            }
        }
        for (Path shardDir : dirtiedDirs) {
            try {
                forceDirectory(shardDir);
            } catch (IOException e) {
                log.warn("failed to fsync sidecar temp cleanup directory {}", shardDir, e);
            }
        }
    }

    private static boolean isSidecarTempFile(Path p) {
        Path name = p.getFileName();
        return name != null && name.toString().contains(".meta.tmp-");
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
            try (FileChannel ch = FileChannel.open(probe.dataPath, READ_OPEN_OPTIONS)) {
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
        } else if (sealedProbe == null && !hasSidecar && !Files.exists(probe.ledgerPath)
                && mayHaveSealedTrailer(probe.dataPath)) {
            throw new CorruptChunkException("corrupt sealed footer/trailer for " + id);
        } else if (hasSidecar) {
            try {
                sidecar = ChunkFormats.Sidecar.decode(Files.readAllBytes(probe.metaPath));
            } catch (CorruptChunkException | IllegalArgumentException e) {
                sidecar = recoverMalformedSidecar(id, probe, header, e);
                if (sidecar == null) {
                    return;
                }
                reconstructedSidecar = true;
            }
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
                long dataSizeBeforeRecovery = h.data.size();
                long ledgerSizeBeforeRecovery = Files.exists(h.ledgerPath) ? Files.size(h.ledgerPath) : 0;
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
                long recoveredDataSize = checkedAdd(DATA_START, verifiedEnd, "chunk file offset");
                h.data.truncate(recoveredDataSize);
                long recoveredLedgerSize = (long) h.ledger.size() * ChunkFormats.LEDGER_ENTRY_SIZE;
                if (dataSizeBeforeRecovery != recoveredDataSize || ledgerSizeBeforeRecovery != recoveredLedgerSize) {
                    h.data.force(false);
                    h.ledger.force();
                    FailureInjector.point("format.recovery.afterOpenTruncateForce");
                }
                h.state = ChunkState.OPEN;
                h.end = verifiedEnd;
                h.lastKnownDO = Math.min(h.lastKnownDO, verifiedEnd);
                if (reconstructedSidecar) {
                    h.persistSidecar();
                }
                h.startCommitterIfFsync(forceCount);
            }
            chunks.put(h.mapKey, h);
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

    private ChunkFormats.Sidecar recoverMalformedSidecar(ChunkId id, Handle probe, ChunkFormats.Header header,
                                                         RuntimeException cause) {
        if (header.fsyncOnAck() && Files.exists(probe.ledgerPath)) {
            log.warn("chunk {} has malformed sidecar but is fsync-on-ack — reconstructing fenced OPEN state",
                    id, cause);
            return reconstructedOpenSidecar(header);
        }
        throw cause;
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
    private record SealedProbe(ChunkFormats.Trailer trailer, int[] rangeCrcs) {}

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
        try (FileChannel ch = FileChannel.open(dataPath, READ_OPEN_OPTIONS)) {
            long fileLen = ch.size();
            if (fileLen < DATA_START + TRAILER_SIZE) {
                return null;
            }
            return readSealedFooter(ch, fileLen, id);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    private static boolean mayHaveSealedTrailer(Path dataPath) throws IOException {
        return Files.size(dataPath) >= DATA_START + TRAILER_SIZE;
    }

    private static SealedProbe readSealedFooterFromPath(Path dataPath, ChunkId id) throws IOException {
        try (FileChannel data = FileChannel.open(dataPath, READ_OPEN_OPTIONS)) {
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
            if (size == 0) {
                return dataLength == 0;
            }
            if (size % ChunkFormats.LEDGER_ENTRY_SIZE != 0) {
                return false;
            }
            byte[] buf = new byte[ChunkFormats.LEDGER_ENTRY_SIZE];
            try (FileChannel ch = FileChannel.open(ledgerPath, READ_OPEN_OPTIONS)) {
                readFully(ch, ByteBuffer.wrap(buf), size - ChunkFormats.LEDGER_ENTRY_SIZE);
            }
            ChunkFormats.LedgerEntry last = ChunkFormats.LedgerEntry.decodeOrNull(buf, 0);
            return last != null && last.endOffset() == dataLength;
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    private void readSealedVerified(FileChannel data, long sealedLength, int[] rangeCrcs,
                                    ChunkId id, long offset, ReadRegionResult out)
            throws IOException {
        byte[] outBytes = out.array();
        int readLength = out.length();
        if (readLength == 0) return;
        if (rangeCrcs.length == 0) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "sealed chunk missing CRC ranges: " + id);
        }
        long firstRange = offset / ChunkFormats.CRC_RANGE_SIZE;
        long lastRange = (offset + readLength - 1) / ChunkFormats.CRC_RANGE_SIZE;
        for (long range = firstRange; range <= lastRange; range++) {
            if (range >= rangeCrcs.length) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "CRC range missing for " + id);
            }
            int rangeIndex = Math.toIntExact(range);
            long rangeStart = range * (long) ChunkFormats.CRC_RANGE_SIZE;
            int rangeLen = (int) Math.min(ChunkFormats.CRC_RANGE_SIZE, sealedLength - rangeStart);
            long copyStart = Math.max(offset, rangeStart);
            long copyEnd = Math.min(offset + readLength, rangeStart + rangeLen);
            int copyLen = (int) (copyEnd - copyStart);
            int actual;
            if (copyStart == rangeStart && copyLen == rangeLen) {
                int dst = (int) (copyStart - offset);
                readFully(data, out.slice(dst, rangeLen),
                        checkedAdd(DATA_START, rangeStart, "chunk file offset"));
                actual = Crc.of(outBytes, dst, rangeLen);
            } else {
                ReadBuffer rangeBuf = sealedVerifyBufferPool.acquire(rangeLen);
                byte[] rangeBytes = rangeBuf.bytes();
                try {
                    readFully(data, rangeBuf.slice(0, rangeLen),
                            checkedAdd(DATA_START, rangeStart, "chunk file offset"));
                    actual = Crc.of(rangeBytes, 0, rangeLen);
                    System.arraycopy(rangeBytes, (int) (copyStart - rangeStart), outBytes,
                            (int) (copyStart - offset), copyLen);
                } finally {
                    rangeBuf.close();
                }
            }
            int expected = rangeCrcs[rangeIndex];
            if (actual != expected) {
                throw new ScpException(ErrorCode.CRC_MISMATCH,
                        "sealed range crc mismatch on " + id + " range " + range);
            }
        }
    }

    private void readOpenVerified(Handle h, long offset, ReadRegionResult out) throws IOException {
        int readLength = out.length();
        if (readLength == 0) {
            return;
        }
        if (h.ledger == null) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "open chunk missing integrity ledger: " + h.id);
        }
        long readEnd = checkedAdd(offset, readLength, "open read end");
        IntegrityLedger.EntrySpan span = h.ledger.reusableEntriesCovering(offset, readEnd);
        // This overload uses the handle's shared FileChannel for recovery/local reads. Server request
        // threads must not be interrupted with cancel(true): FileChannel is interruptible and may close.
        try {
            readOpenVerified(h.data, span, h.id, offset, out);
        } finally {
            span.clear();
        }
    }

    private void readOpenVerified(FileChannel data, IntegrityLedger.EntrySpan span, ChunkId id, long offset,
                                  ReadRegionResult out) throws IOException {
        byte[] outBytes = out.array();
        int readLength = out.length();
        if (readLength == 0) {
            return;
        }
        long readEnd = checkedAdd(offset, readLength, "open read end");
        readFully(data, out.slice(0, readLength), checkedAdd(DATA_START, offset, "chunk file offset"));
        long entryStart = span.firstStart();
        int copied = 0;
        for (int i = 0; i < span.length(); i++) {
            long entryEnd = span.endOffset(i);
            if (entryEnd <= entryStart) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                        "non-increasing ledger entry for " + id + ": " + entryEnd);
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
                        "oversized ledger entry for " + id + ": " + entryLenLong);
            }
            int entryLen = (int) entryLenLong;
            long copyStart = Math.max(offset, entryStart);
            long copyEnd = Math.min(readEnd, entryEnd);
            boolean fullEntryCovered = copyStart == entryStart && copyEnd == entryEnd;
            if (fullEntryCovered) {
                int dst = (int) (copyStart - offset);
                int actual = Crc.of(outBytes, dst, entryLen);
                if (actual != span.payloadCrc(i)) {
                    throw new ScpException(ErrorCode.CRC_MISMATCH,
                            "open ledger crc mismatch on " + id + " range [" + entryStart + ".." + entryEnd + ")");
                }
                copied += entryLen;
                entryStart = entryEnd;
                continue;
            }
            ReadBuffer entryBuffer = readBufferPool.acquire(entryLen);
            try {
                readFully(data, entryBuffer.slice(0, entryLen),
                        checkedAdd(DATA_START, entryStart, "chunk file offset"));
                int actual = Crc.of(entryBuffer.bytes(), 0, entryLen);
                if (actual != span.payloadCrc(i)) {
                    throw new ScpException(ErrorCode.CRC_MISMATCH,
                            "open ledger crc mismatch on " + id + " range [" + entryStart + ".." + entryEnd + ")");
                }
            } finally {
                entryBuffer.close();
            }
            if (copyEnd > copyStart) {
                int len = (int) (copyEnd - copyStart);
                copied += len;
            }
            entryStart = entryEnd;
        }
        if (copied != readLength) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                    "open read is not covered by ledger for " + id);
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

    private static int[] decodeCrcRanges(byte[] footerBytes, long dataLength, int expectedSections) {
        ByteBuffer footer = ByteBuffer.wrap(footerBytes);
        int[] crcRanges = null;
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
                int[] ranges = new int[count];
                for (int i = 0; i < count; i++) ranges[i] = c.getInt();
                crcRanges = ranges;
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
                if (h.state != ChunkState.SEALED) {
                    try {
                        // Non-fsync ack-on-replicate chunks keep clean-close sidecars advisory: the
                        // temp+rename write is old-or-new, but we skip the directory fsync to preserve
                        // that tier's shutdown cost and power-loss durability contract.
                        h.persistSidecar(sealFsync || h.header.fsyncOnAck());
                    } catch (IOException | RuntimeException e) {
                        log.warn("close {} failed", h.id, e);
                        failure = Closeables.suppress(failure, e);
                    }
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
