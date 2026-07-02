package io.strata.format;

import com.sun.management.UnixOperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/** Tuning knobs for {@link ChunkStore}'s read cap, durability, writeback, and local file handles. */
public record ChunkStoreConfig(
        int maxRequestBytes,
        long groupCommitDrainTimeoutMs,
        long groupCommitMinAccumulationNanos,
        long groupCommitMaxAccumulationNanos,
        boolean sealFsync,
        long backgroundFlushIntervalMs,
        long backgroundFlushThresholdBytes,
        long slowAppendLogMs,
        long slowMutationLogMs,
        int channelCacheMaxSize,
        int maxOpenChunkLedgerEntries) {

    public static final ChunkStoreConfig DEFAULT =
            new ChunkStoreConfig(8 * 1024 * 1024, 10_000L, 1_000_000L, 50_000_000L,
                    false, 500L, 4L << 20, 1_000L, 500L, defaultChannelCacheCapacity(), 262_144);

    public ChunkStoreConfig(int maxRequestBytes, long groupCommitDrainTimeoutMs,
                            long groupCommitMinAccumulationNanos,
                            long groupCommitMaxAccumulationNanos) {
        this(maxRequestBytes, groupCommitDrainTimeoutMs, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos, DEFAULT.sealFsync, DEFAULT.backgroundFlushIntervalMs,
                DEFAULT.backgroundFlushThresholdBytes, DEFAULT.slowAppendLogMs, DEFAULT.slowMutationLogMs,
                DEFAULT.channelCacheMaxSize, DEFAULT.maxOpenChunkLedgerEntries);
    }

    public ChunkStoreConfig(int maxRequestBytes, long groupCommitDrainTimeoutMs,
                            long groupCommitMinAccumulationNanos,
                            long groupCommitMaxAccumulationNanos,
                            boolean sealFsync, long backgroundFlushIntervalMs,
                            long backgroundFlushThresholdBytes, long slowAppendLogMs,
                            long slowMutationLogMs, int channelCacheMaxSize) {
        this(maxRequestBytes, groupCommitDrainTimeoutMs, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos, sealFsync, backgroundFlushIntervalMs,
                backgroundFlushThresholdBytes, slowAppendLogMs, slowMutationLogMs, channelCacheMaxSize,
                DEFAULT.maxOpenChunkLedgerEntries);
    }

    public ChunkStoreConfig {
        if (maxRequestBytes <= 0) {
            throw new IllegalArgumentException("maxRequestBytes must be positive: " + maxRequestBytes);
        }
        if (groupCommitDrainTimeoutMs <= 0) {
            throw new IllegalArgumentException("groupCommitDrainTimeoutMs must be positive: " + groupCommitDrainTimeoutMs);
        }
        if (groupCommitMinAccumulationNanos <= 0) {
            throw new IllegalArgumentException("groupCommitMinAccumulationNanos must be positive: " + groupCommitMinAccumulationNanos);
        }
        if (groupCommitMaxAccumulationNanos < groupCommitMinAccumulationNanos) {
            throw new IllegalArgumentException("groupCommitMaxAccumulationNanos (" + groupCommitMaxAccumulationNanos
                    + ") must be >= groupCommitMinAccumulationNanos (" + groupCommitMinAccumulationNanos + ")");
        }
        if (backgroundFlushIntervalMs <= 0) {
            throw new IllegalArgumentException("backgroundFlushIntervalMs must be positive: "
                    + backgroundFlushIntervalMs);
        }
        if (backgroundFlushThresholdBytes <= 0) {
            throw new IllegalArgumentException("backgroundFlushThresholdBytes must be positive: "
                    + backgroundFlushThresholdBytes);
        }
        if (slowAppendLogMs <= 0) {
            throw new IllegalArgumentException("slowAppendLogMs must be positive: " + slowAppendLogMs);
        }
        if (slowMutationLogMs <= 0) {
            throw new IllegalArgumentException("slowMutationLogMs must be positive: " + slowMutationLogMs);
        }
        if (channelCacheMaxSize <= 0) {
            throw new IllegalArgumentException("channelCacheMaxSize must be positive: " + channelCacheMaxSize);
        }
        if (maxOpenChunkLedgerEntries <= 0) {
            throw new IllegalArgumentException("maxOpenChunkLedgerEntries must be positive: "
                    + maxOpenChunkLedgerEntries);
        }
    }

    public ChunkStoreConfig withMaxRequestBytes(int v) {
        return new ChunkStoreConfig(v, groupCommitDrainTimeoutMs, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos, sealFsync, backgroundFlushIntervalMs,
                backgroundFlushThresholdBytes, slowAppendLogMs, slowMutationLogMs, channelCacheMaxSize,
                maxOpenChunkLedgerEntries);
    }

    public ChunkStoreConfig withGroupCommitDrainTimeoutMs(long v) {
        return new ChunkStoreConfig(maxRequestBytes, v, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos, sealFsync, backgroundFlushIntervalMs,
                backgroundFlushThresholdBytes, slowAppendLogMs, slowMutationLogMs, channelCacheMaxSize,
                maxOpenChunkLedgerEntries);
    }

    public ChunkStoreConfig withGroupCommitAccumulationNanos(long min, long max) {
        return new ChunkStoreConfig(maxRequestBytes, groupCommitDrainTimeoutMs, min, max, sealFsync,
                backgroundFlushIntervalMs, backgroundFlushThresholdBytes, slowAppendLogMs, slowMutationLogMs,
                channelCacheMaxSize, maxOpenChunkLedgerEntries);
    }

    public ChunkStoreConfig withSealFsync(boolean enabled) {
        return new ChunkStoreConfig(maxRequestBytes, groupCommitDrainTimeoutMs, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos, enabled, backgroundFlushIntervalMs,
                backgroundFlushThresholdBytes, slowAppendLogMs, slowMutationLogMs, channelCacheMaxSize,
                maxOpenChunkLedgerEntries);
    }

    public ChunkStoreConfig withBackgroundFlush(long intervalMs, long thresholdBytes) {
        return new ChunkStoreConfig(maxRequestBytes, groupCommitDrainTimeoutMs, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos, sealFsync, intervalMs, thresholdBytes, slowAppendLogMs,
                slowMutationLogMs, channelCacheMaxSize, maxOpenChunkLedgerEntries);
    }

    public ChunkStoreConfig withSlowLogThresholds(long appendMs, long mutationMs) {
        return new ChunkStoreConfig(maxRequestBytes, groupCommitDrainTimeoutMs, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos, sealFsync, backgroundFlushIntervalMs,
                backgroundFlushThresholdBytes, appendMs, mutationMs, channelCacheMaxSize,
                maxOpenChunkLedgerEntries);
    }

    public ChunkStoreConfig withChannelCacheMaxSize(int maxSize) {
        return new ChunkStoreConfig(maxRequestBytes, groupCommitDrainTimeoutMs, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos, sealFsync, backgroundFlushIntervalMs,
                backgroundFlushThresholdBytes, slowAppendLogMs, slowMutationLogMs, maxSize,
                maxOpenChunkLedgerEntries);
    }

    public ChunkStoreConfig withMaxOpenChunkLedgerEntries(int maxEntries) {
        return new ChunkStoreConfig(maxRequestBytes, groupCommitDrainTimeoutMs, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos, sealFsync, backgroundFlushIntervalMs,
                backgroundFlushThresholdBytes, slowAppendLogMs, slowMutationLogMs, channelCacheMaxSize,
                maxEntries);
    }

    /**
     * Default sealed-chunk channel-cache capacity: derived from the soft RLIMIT_NOFILE minus headroom
     * for pinned OPEN channels, ledgers, sockets, and in-flight transient FDs. Falls back to a fixed
     * default on non-Unix / non-HotSpot JVMs where the FD limit is not introspectable.
     */
    static int defaultChannelCacheCapacity() {
        long max = -1;
        java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof UnixOperatingSystemMXBean unix) {
            max = unix.getMaxFileDescriptorCount();
        }
        if (max <= 0) {
            return 1024;
        }
        long headroom = Math.max(256, max / 4);
        long capacity = max - headroom;
        return (int) Math.max(128, Math.min(capacity, Integer.MAX_VALUE));
    }
}
