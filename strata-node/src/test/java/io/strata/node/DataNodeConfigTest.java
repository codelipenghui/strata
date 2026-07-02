package io.strata.node;

import io.strata.common.ConnectionPolicy;
import io.strata.format.ChunkStoreConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataNodeConfigTest {

    @Test
    void copyHelpersOnlyChangeRequestedFields() {
        DataNodeConfig base = new DataNodeConfig(Path.of("data"), 0, "10.0.0.1", null,
                List.of("meta:9000"), "zone", "rack", "host", 1234, 55);

        DataNodeConfig withPort = base.withListenPort(123);
        assertEquals(123, withPort.listenPort());
        assertEquals(base.dataDir(), withPort.dataDir());
        assertEquals(base.advertisedHost(), withPort.advertisedHost());
        assertNull(withPort.advertisedEndpointOverride());
        assertEquals(base.controllerEndpoints(), withPort.controllerEndpoints());
        assertEquals(base.zone(), withPort.zone());
        assertEquals(base.rack(), withPort.rack());
        assertEquals(base.host(), withPort.host());
        assertEquals(base.capacityBytes(), withPort.capacityBytes());
        assertEquals(base.scrubIntervalMs(), withPort.scrubIntervalMs());
        assertEquals(base.connectionPolicy(), withPort.connectionPolicy());

        DataNodeConfig withEndpoint = withPort.withAdvertisedEndpoint("proxy:19000");
        assertEquals(123, withEndpoint.listenPort());
        assertEquals("proxy:19000", withEndpoint.advertisedEndpointOverride());
        assertEquals(base.dataDir(), withEndpoint.dataDir());
        assertEquals(base.controllerEndpoints(), withEndpoint.controllerEndpoints());
        assertEquals(base.zone(), withEndpoint.zone());
        assertEquals(base.rack(), withEndpoint.rack());
        assertEquals(base.host(), withEndpoint.host());
        assertEquals(base.capacityBytes(), withEndpoint.capacityBytes());
        assertEquals(base.scrubIntervalMs(), withEndpoint.scrubIntervalMs());
        assertEquals(base.connectionPolicy(), withEndpoint.connectionPolicy());

        ConnectionPolicy policy = ConnectionPolicy.DEFAULT.withIdleTimeoutMs(1234);
        assertEquals(policy, base.withConnectionPolicy(policy).connectionPolicy());

        assertEquals(-1, base.nodeId(), "convenience constructor defaults to the standalone sentinel");
        DataNodeConfig withNodeId = base.withNodeId(42);
        assertEquals(42, withNodeId.nodeId());
        assertEquals(base.dataDir(), withNodeId.dataDir());
        assertEquals(base.controllerEndpoints(), withNodeId.controllerEndpoints());
        assertEquals(base.host(), withNodeId.host());
        assertEquals(base.connectionPolicy(), withNodeId.connectionPolicy());
    }

    @Test
    void validatesConstructionInputsAndCopiesEndpointList() {
        List<String> endpoints = new ArrayList<>(List.of("[::1]:2181"));
        DataNodeConfig config = new DataNodeConfig(Path.of("data"), 0, "127.0.0.1", "node:9000",
                endpoints, "zone", "rack", "host", 1234, 55);
        endpoints.set(0, "changed:2181");
        assertEquals(List.of("[::1]:2181"), config.controllerEndpoints());

        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(null, 0, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, null, "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), -1, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 65_536, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, " ",
                null, List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, null,
                null, List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(), null, "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                "bad-endpoint", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                "node:", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                "[]:9000", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                "[::1:9000", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                "node:65536", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                "node :9000", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of("meta:not-a-port"), "zone", "rack", "host", 1, 1));
        List<String> endpointsWithNull = new ArrayList<>();
        endpointsWithNull.add(null);
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, endpointsWithNull, "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of("meta:65536"), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of("meta:0"), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(" meta:9000"), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(), "zone", "rack", "", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 1, 0));
        assertThrows(NullPointerException.class, () -> new DataNodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 1, 1, null, -1,
                6_000L, 3_000L, 6_000L, 5_000, 10_000, 4 * 1024 * 1024, 1, 50L,
                ChunkStoreConfig.DEFAULT));
    }

    @Test
    void newNodeTuningFieldsDefault() {
        DataNodeConfig c = new DataNodeConfig(Path.of("data"), 0, "10.0.0.1", null,
                List.of("meta:9000"), "zone", "rack", "host", 1234, 55);
        assertEquals(6_000, c.orphanGraceMs());
        assertEquals(3_000, c.orphanScanIntervalMs());
        assertEquals(6_000, c.orphanStartupGraceMs());
        assertEquals(5_000, c.orphanConfirmTimeoutMs());
        assertEquals(10_000, c.controlCallTimeoutMs());
        assertEquals(4 * 1024 * 1024, c.repairFetchBytes());
        assertEquals(1, c.deleteMaxConcurrent());
        assertEquals(50, c.deleteMinIntervalMs());
        assertEquals(ChunkStoreConfig.DEFAULT, c.chunkStoreConfig());
    }

    @Test
    void failFastOnNonPositiveNodeTuningOverrides() {
        DataNodeConfig base = new DataNodeConfig(Path.of("data"), 0, "10.0.0.1", null,
                List.of("meta:9000"), "zone", "rack", "host", 1234, 55);
        assertThrows(IllegalArgumentException.class, () -> base.withOrphanGraceMs(0));
        assertThrows(IllegalArgumentException.class, () -> base.withControlCallTimeoutMs(-1));
        assertThrows(IllegalArgumentException.class, () -> base.withDeleteMaxConcurrent(0));
        assertThrows(IllegalArgumentException.class, () -> base.withDeleteMinIntervalMs(-1));
    }

    @Test
    void withSettersOverrideNodeTuning() {
        DataNodeConfig c = new DataNodeConfig(Path.of("data"), 0, "10.0.0.1", null,
                List.of("meta:9000"), "zone", "rack", "host", 1234, 55)
                .withOrphanGraceMs(1000).withOrphanScanIntervalMs(500).withOrphanStartupGraceMs(1500)
                .withOrphanConfirmTimeoutMs(2500).withControlCallTimeoutMs(8000).withRepairFetchBytes(1 << 20)
                .withDeleteMaxConcurrent(2).withDeleteMinIntervalMs(25)
                .withChunkStoreConfig(ChunkStoreConfig.DEFAULT.withMaxRequestBytes(4096));
        assertEquals(1000, c.orphanGraceMs());
        assertEquals(500, c.orphanScanIntervalMs());
        assertEquals(1500, c.orphanStartupGraceMs());
        assertEquals(2500, c.orphanConfirmTimeoutMs());
        assertEquals(8000, c.controlCallTimeoutMs());
        assertEquals(1 << 20, c.repairFetchBytes());
        assertEquals(2, c.deleteMaxConcurrent());
        assertEquals(25, c.deleteMinIntervalMs());
        assertEquals(4096, c.chunkStoreConfig().maxRequestBytes());
    }
}
