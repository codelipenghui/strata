package io.strata.node;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NodeConfigTest {

    @Test
    void copyHelpersOnlyChangeRequestedFields() {
        NodeConfig base = new NodeConfig(Path.of("data"), 0, "10.0.0.1", null,
                List.of("meta:9000"), "zone", "rack", "host", 1234, 55);

        NodeConfig withPort = base.withListenPort(123);
        assertEquals(123, withPort.listenPort());
        assertEquals(base.dataDir(), withPort.dataDir());
        assertEquals(base.advertisedHost(), withPort.advertisedHost());
        assertNull(withPort.advertisedEndpointOverride());
        assertEquals(base.metadataEndpoints(), withPort.metadataEndpoints());
        assertEquals(base.zone(), withPort.zone());
        assertEquals(base.rack(), withPort.rack());
        assertEquals(base.host(), withPort.host());
        assertEquals(base.capacityBytes(), withPort.capacityBytes());
        assertEquals(base.inventoryIntervalMs(), withPort.inventoryIntervalMs());

        NodeConfig withEndpoint = withPort.withAdvertisedEndpoint("proxy:19000");
        assertEquals(123, withEndpoint.listenPort());
        assertEquals("proxy:19000", withEndpoint.advertisedEndpointOverride());
        assertEquals(base.dataDir(), withEndpoint.dataDir());
        assertEquals(base.metadataEndpoints(), withEndpoint.metadataEndpoints());
        assertEquals(base.zone(), withEndpoint.zone());
        assertEquals(base.rack(), withEndpoint.rack());
        assertEquals(base.host(), withEndpoint.host());
        assertEquals(base.capacityBytes(), withEndpoint.capacityBytes());
        assertEquals(base.inventoryIntervalMs(), withEndpoint.inventoryIntervalMs());
    }

    @Test
    void validatesConstructionInputsAndCopiesEndpointList() {
        List<String> endpoints = new ArrayList<>(List.of("[::1]:2181"));
        NodeConfig config = new NodeConfig(Path.of("data"), 0, "127.0.0.1", "node:9000",
                endpoints, "zone", "rack", "host", 1234, 55);
        endpoints.set(0, "changed:2181");
        assertEquals(List.of("[::1]:2181"), config.metadataEndpoints());

        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(null, 0, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, null, "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), -1, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 65_536, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, " ",
                null, List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, null,
                null, List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(), null, "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                "bad-endpoint", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                "node:", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                "[]:9000", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                "[::1:9000", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                "node:65536", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                "node :9000", List.of(), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of("meta:not-a-port"), "zone", "rack", "host", 1, 1));
        List<String> endpointsWithNull = new ArrayList<>();
        endpointsWithNull.add(null);
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, endpointsWithNull, "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of("meta:65536"), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of("meta:0"), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(" meta:9000"), "zone", "rack", "host", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(), "zone", "rack", "", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new NodeConfig(Path.of("data"), 0, "127.0.0.1",
                null, List.of(), "zone", "rack", "host", 1, 0));
    }
}
