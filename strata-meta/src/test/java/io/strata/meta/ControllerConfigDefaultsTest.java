package io.strata.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class ControllerConfigDefaultsTest {
    @Test
    void reconcileIntervalDefaultsToSixtySeconds() {
        ControllerConfig c = new ControllerConfig("zk:2181", 9100, 3000, 60000, 300000, 5000, 30000);
        assertEquals(60_000, c.reconcileIntervalMs());
    }
}
