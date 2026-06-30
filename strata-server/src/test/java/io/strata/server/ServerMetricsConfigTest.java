package io.strata.server;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerMetricsConfigTest {
    @Test
    void parsesAscendingDistinctPositiveCsv() {
        assertArrayEquals(new long[]{1, 5, 10, 250}, StrataServer.parseBucketsMs("1,5,10,250", new long[]{1}));
    }

    @Test
    void unsetUsesDefault() {
        assertArrayEquals(new long[]{1, 2, 5}, StrataServer.parseBucketsMs(null, new long[]{1, 2, 5}));
    }

    @Test
    void rejectsNonAscendingOrNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> StrataServer.parseBucketsMs("5,5", new long[]{1}));
        assertThrows(IllegalArgumentException.class, () -> StrataServer.parseBucketsMs("0,5", new long[]{1}));
        assertThrows(IllegalArgumentException.class, () -> StrataServer.parseBucketsMs("10,5", new long[]{1}));
    }
}
