package io.strata.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConnectionPolicyTest {

    @Test
    void defaultPolicyMatchesDocumentedLifecycleSettings() {
        ConnectionPolicy policy = ConnectionPolicy.DEFAULT;

        assertEquals(5_000, policy.connectTimeoutMs());
        assertEquals(10_000, policy.heartbeatIntervalMs());
        assertEquals(2_000, policy.heartbeatTimeoutMs());
        assertEquals(60_000, policy.idleTimeoutMs());
        assertEquals(100, policy.reconnectInitialBackoffMs());
        assertEquals(5_000, policy.reconnectMaxBackoffMs());
    }

    @Test
    void validatesPositiveDurationsAndBackoffOrdering() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionPolicy(0, 1, 1, 1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionPolicy(1, 0, 1, 1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionPolicy(1, 1, 0, 1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionPolicy(1, 1, 1, 0, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionPolicy(1, 1, 1, 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionPolicy(1, 1, 1, 1, 2, 1));
    }
}
