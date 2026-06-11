package io.strata.client;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientConfigTest {

    @Test
    void rejectsInvalidValuesAtConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(null, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of(), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of(""), 1, 1));
        List<String> endpointsWithNull = new ArrayList<>();
        endpointsWithNull.add(null);
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(endpointsWithNull, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of("host"), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of("host:"), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of("[::1:123"), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of(" host:123"), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of("host :123"), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of(" :123"), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of("[]:123"), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of("host:not-a-port"), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of("host:0"), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of("host:65536"), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of("host:123"), 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new ClientConfig(List.of("host:123"), 1, 0));
    }

    @Test
    void copiesEndpointListAndAcceptsBracketedIpv6StyleEndpoint() {
        List<String> endpoints = new ArrayList<>();
        endpoints.add("[::1]:2181");

        ClientConfig cfg = new ClientConfig(endpoints, 1024, 500);
        endpoints.set(0, "127.0.0.1:9999");

        assertEquals(List.of("[::1]:2181"), cfg.metadataEndpoints());
        assertThrows(UnsupportedOperationException.class, () -> cfg.metadataEndpoints().add("127.0.0.1:1"));
        assertEquals(2048, cfg.withChunkRollBytes(2048).chunkRollBytes());
    }
}
