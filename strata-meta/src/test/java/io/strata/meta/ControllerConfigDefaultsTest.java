package io.strata.meta;

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
}
