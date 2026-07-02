package io.strata.meta;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ControllerConfigDefaultsTest {
    @Test
    void reconcileIntervalDefaultsToSixtySeconds() {
        ControllerConfig c = new ControllerConfig("zk:2181", 9100, 3000, 60000, 300000, 5000, 30000);
        assertEquals(60_000, c.reconcileIntervalMs());
    }

    @Test
    void rejectsNullZkConnect() {
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerConfig(null, 0, 200, 1000, 0, 1, 1));
    }

    @Test
    void rejectsHeartbeatNotShorterThanLeasePlusGrace() {
        // heartbeat 1000 >= lease 500 + grace 0 -> nodes would expire before heartbeating
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerConfig("zk:2181", 0, 1000, 500, 0, 1, 1));
    }

    @Test
    void newTuningFieldsDefault() {
        ControllerConfig c = new ControllerConfig("zk:2181", 9100, 3000, 60000, 300000, 5000, 30000);
        assertEquals(2_000, c.verifyIntervalMs());
        assertEquals(256, c.verifyBatchSize());
        assertEquals(30_000, c.systemVerifyIntervalMs());
        assertEquals(600_000L, c.deletedTombstoneTtlMs());
        assertEquals(16, c.maxCommandsPerHeartbeat());
        assertEquals(100, c.zkRetryBaseMs());
        assertEquals(5, c.zkRetryMaxRetries());
        assertEquals("zk", c.metadataBackendConfig().backend());
    }

    @Test
    void withSettersOverrideTuning() {
        ControllerConfig c = new ControllerConfig("zk:2181", 9100, 3000, 60000, 300000, 5000, 30000)
                .withVerifyIntervalMs(500).withVerifyBatchSize(64).withSystemVerifyIntervalMs(10_000)
                .withDeletedTombstoneTtlMs(120_000).withMaxCommandsPerHeartbeat(64)
                .withZkRetryBaseMs(50).withZkRetryMaxRetries(9).withNamespaceLogBackend();
        assertEquals(500, c.verifyIntervalMs());
        assertEquals(64, c.verifyBatchSize());
        assertEquals(10_000, c.systemVerifyIntervalMs());
        assertEquals(120_000L, c.deletedTombstoneTtlMs());
        assertEquals(64, c.maxCommandsPerHeartbeat());
        assertEquals(50, c.zkRetryBaseMs());
        assertEquals(9, c.zkRetryMaxRetries());
        assertEquals("namespace-log", c.metadataBackendConfig().backend());
        assertEquals(3, c.metadataBackendConfig().namespaceLogReplicationFactor());
        assertEquals(2, c.metadataBackendConfig().namespaceLogAckQuorum());
    }

    @Test
    void failFastOnNonPositiveTuningOverrides() {
        ControllerConfig base = new ControllerConfig("zk:2181", 9100, 3000, 60000, 300000, 5000, 30000);
        assertThrows(IllegalArgumentException.class, () -> base.withVerifyIntervalMs(0));
        assertThrows(IllegalArgumentException.class, () -> base.withVerifyBatchSize(-1));
        assertThrows(IllegalArgumentException.class, () -> base.withMaxCommandsPerHeartbeat(0));
        assertThrows(IllegalArgumentException.class, () -> base.withZkRetryMaxRetries(-1));
        assertThrows(IllegalArgumentException.class, () -> base.withMetadataBackend(
                new ControllerConfig.MetadataBackendConfig("namespace-log", 2, 3, false,
                        4 * 1024 * 1024, 30_000, true, 0, 4 * 1024 * 1024)));
        // 0 retries is valid (means no retry)
        assertDoesNotThrow(() -> base.withZkRetryMaxRetries(0));
        assertEquals(0, base.withZkRetryMaxRetries(0).zkRetryMaxRetries());
    }

    @Test
    void rejectsTombstoneTtlNotExceedingRepairScan() {
        // deletedTombstoneTtlMs (1000) must exceed repairScanIntervalMs (5000) -> invalid
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerConfig("zk:2181", 9100, 3000, 60000, 300000, 5000, 30000)
                        .withDeletedTombstoneTtlMs(1_000));
    }
}
