package io.strata.format;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChunkStoreConfigTest {
    @Test
    void defaultsMatchHistoricalConstants() {
        ChunkStoreConfig c = ChunkStoreConfig.DEFAULT;
        assertEquals(8 * 1024 * 1024, c.maxRequestBytes());
        assertEquals(10_000L, c.groupCommitDrainTimeoutMs());
        assertEquals(1_000_000L, c.groupCommitMinAccumulationNanos());
        assertEquals(50_000_000L, c.groupCommitMaxAccumulationNanos());
    }

    @Test
    void rejectsNonPositiveAndBadAccumulationOrder() {
        assertThrows(IllegalArgumentException.class, () -> new ChunkStoreConfig(0, 10_000L, 1_000_000L, 50_000_000L));
        assertThrows(IllegalArgumentException.class, () -> new ChunkStoreConfig(8 << 20, 0L, 1_000_000L, 50_000_000L));
        // max < min -> invalid
        assertThrows(IllegalArgumentException.class, () -> new ChunkStoreConfig(8 << 20, 10_000L, 50_000_000L, 1_000_000L));
    }

    @Test
    void rejectsZeroMinAccumulationNanos() {
        assertThrows(IllegalArgumentException.class,
                () -> new ChunkStoreConfig(8 << 20, 10_000L, 0L, 50_000_000L));
    }

    @Test
    void rejectsNegativeMaxRequestBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> new ChunkStoreConfig(-1, 10_000L, 1_000_000L, 50_000_000L));
    }

    @Test
    void withGroupCommitAccumulationNanosRejectsMinGreaterThanMax() {
        assertThrows(IllegalArgumentException.class,
                () -> ChunkStoreConfig.DEFAULT.withGroupCommitAccumulationNanos(50_000_000L, 1_000_000L));
    }

    @Test
    void withSettersOverride() {
        ChunkStoreConfig c = ChunkStoreConfig.DEFAULT.withMaxRequestBytes(4 << 20)
                .withGroupCommitDrainTimeoutMs(20_000L)
                .withGroupCommitAccumulationNanos(2_000_000L, 80_000_000L);
        assertEquals(4 << 20, c.maxRequestBytes());
        assertEquals(20_000L, c.groupCommitDrainTimeoutMs());
        assertEquals(2_000_000L, c.groupCommitMinAccumulationNanos());
        assertEquals(80_000_000L, c.groupCommitMaxAccumulationNanos());
    }
}
