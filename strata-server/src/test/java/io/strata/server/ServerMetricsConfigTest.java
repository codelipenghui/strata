package io.strata.server;

import io.strata.meta.ControllerConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerMetricsConfigTest {
    @Test
    void controllerRoleDefaultsKeepDistinctPortsAndAutomaticallyShardMultipleEndpoints() {
        Map<String, String> environment = new HashMap<>();
        environment.put("STRATA_ZK_CONNECT", " zk:2181 ");

        ControllerConfig standalone = StrataServer.standaloneControllerConfigFromEnv(
                () -> "standalone-host", environment::get);
        ControllerConfig combined = StrataServer.combinedControllerConfigFromEnv(
                () -> "combined-host", environment::get);

        assertEquals(9_200, standalone.listenPort());
        assertEquals(9_100, combined.listenPort());
        assertEquals("zk:2181", standalone.zkConnect(), "environment values remain trimmed");
        assertEquals(ControllerConfig.DEFAULT_ZK_SESSION_TIMEOUT_MS, standalone.zkSessionTimeoutMs());
        assertEquals(List.of(), standalone.controllerEndpoints());
        assertEquals(List.of(), combined.controllerEndpoints());

        environment.put("STRATA_CONTROLLER_ENDPOINTS", "combined-host:9100");
        ControllerConfig singleEndpoint = StrataServer.combinedControllerConfigFromEnv(
                () -> "combined-host", environment::get);
        assertEquals(List.of("combined-host:9100"), singleEndpoint.controllerEndpoints(),
                "a singleton remains visible so Controller can reject a mismatched local identity");
        assertEquals(1, singleEndpoint.controllerReplicaCount());

        environment.put("STRATA_CONTROLLER_ENDPOINTS", "ctrl-a:9100, ctrl-b:9100");
        environment.put("STRATA_CONTROLLER_ZK_SESSION_TIMEOUT_MS", "7500");
        ControllerConfig sharded = StrataServer.combinedControllerConfigFromEnv(
                () -> "combined-host", environment::get);

        assertEquals(List.of("ctrl-a:9100", "ctrl-b:9100"), sharded.controllerEndpoints());
        assertEquals(2, sharded.controllerReplicaCount(),
                "the default replica count is capped by the bootstrap membership size");
        assertEquals(7_500, sharded.zkSessionTimeoutMs());
    }

    @Test
    void requestMetricsConfigAcceptsValidOverrides() {
        Map<String, String> environment = Map.of(
                "STRATA_METRICS_NS_REFRESH_INTERVAL_MS", "250",
                "STRATA_METRICS_REQUEST_DURATION_BUCKETS_MS", "1,5,10",
                "STRATA_METRICS_REQUEST_LATENCY_SAMPLE_RATE", "4");

        StrataServer.RequestMetricsConfig config =
                StrataServer.requestMetricsConfigFromEnv(environment::get);

        assertEquals(250, config.namespaceRefreshMs());
        assertArrayEquals(new long[]{1, 5, 10}, config.durationBucketsMs());
        assertEquals(4, config.latencySampleRate());
    }

    @Test
    void requestMetricsConfigRejectsInvalidRefreshWithTheEnvironmentKnobName() {
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> StrataServer.requestMetricsConfigFromEnv(
                        Map.of("STRATA_METRICS_NS_REFRESH_INTERVAL_MS", "0")::get));

        assertTrue(rejected.getMessage().contains("STRATA_METRICS_NS_REFRESH_INTERVAL_MS"));
    }

    @Test
    void requestMetricsConfigDefendsEveryRecordInvariant() {
        assertThrows(IllegalArgumentException.class,
                () -> new StrataServer.RequestMetricsConfig(0, new long[]{1}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new StrataServer.RequestMetricsConfig(1, new long[0], 1));
        assertThrows(IllegalArgumentException.class,
                () -> new StrataServer.RequestMetricsConfig(1, new long[]{1}, 0));
    }

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

    @Test
    void boolEnvOnlyAcceptsExplicitTrueOrFalse() {
        assertTrue(StrataServer.parseBoolEnv("X", "true", false));
        assertFalse(StrataServer.parseBoolEnv("X", "false", true));
        assertTrue(StrataServer.parseBoolEnv("X", null, true));
        assertThrows(IllegalArgumentException.class, () -> StrataServer.parseBoolEnv("X", "1", false));
        assertThrows(IllegalArgumentException.class, () -> StrataServer.parseBoolEnv("X", "yes", false));
    }

    @Test
    void positiveIntEnvRejectsZeroOrNegative() {
        assertEquals(16, StrataServer.parsePositiveIntEnv("X", null, 16));
        assertEquals(4, StrataServer.parsePositiveIntEnv("X", "4", 16));
        assertThrows(IllegalArgumentException.class, () -> StrataServer.parsePositiveIntEnv("X", "0", 16));
        assertThrows(IllegalArgumentException.class, () -> StrataServer.parsePositiveIntEnv("X", "-1", 16));
    }
}
