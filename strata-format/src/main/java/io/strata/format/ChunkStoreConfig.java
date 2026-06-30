package io.strata.format;

/** Tuning knobs for {@link ChunkStore}'s read cap and group-commit fsync batching. */
public record ChunkStoreConfig(
        int maxRequestBytes,
        long groupCommitDrainTimeoutMs,
        long groupCommitMinAccumulationNanos,
        long groupCommitMaxAccumulationNanos) {

    public static final ChunkStoreConfig DEFAULT =
            new ChunkStoreConfig(8 * 1024 * 1024, 10_000L, 1_000_000L, 50_000_000L);

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
    }

    public ChunkStoreConfig withMaxRequestBytes(int v) {
        return new ChunkStoreConfig(v, groupCommitDrainTimeoutMs, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos);
    }

    public ChunkStoreConfig withGroupCommitDrainTimeoutMs(long v) {
        return new ChunkStoreConfig(maxRequestBytes, v, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos);
    }

    public ChunkStoreConfig withGroupCommitAccumulationNanos(long min, long max) {
        return new ChunkStoreConfig(maxRequestBytes, groupCommitDrainTimeoutMs, min, max);
    }
}
