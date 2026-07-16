package io.strata.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WritePolicyChecksTest {
    @Test
    void rejectsInvalidPoliciesWithStableMessages() {
        assertMessage("replicationFactor must be positive: 0", 0, 1);
        assertMessage("ackQuorum must be in 1..3: 0", 3, 0);
        assertMessage("ackQuorum must be in 1..5: 6", 5, 6);
        assertMessage("ackQuorum must intersect any other quorum: 1 for replicationFactor 2", 2, 1);
        assertMessage("ackQuorum must intersect any other quorum: 2 for replicationFactor 4", 4, 2);
    }

    @Test
    void acceptsIntersectingQuorumsAtSupportedBoundaries() {
        assertDoesNotThrow(() -> WritePolicyChecks.validate(1, 1));
        assertDoesNotThrow(() -> WritePolicyChecks.validate(3, 2));
        assertDoesNotThrow(() -> WritePolicyChecks.validate(4, 3));
        assertDoesNotThrow(() -> WritePolicyChecks.validate(5, 3));
    }

    private static void assertMessage(String expected, int replicationFactor, int ackQuorum) {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> WritePolicyChecks.validate(replicationFactor, ackQuorum));
        assertEquals(expected, error.getMessage());
    }
}
