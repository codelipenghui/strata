package io.strata.client;

import io.strata.common.ConnectionPolicy;
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
        assertThrows(IllegalArgumentException.class,
                () -> new ClientConfig(List.of("host:123"), 1, 1, ConnectionPolicy.DEFAULT, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new ClientConfig(List.of("host:123"), 1, 1, ConnectionPolicy.DEFAULT, -1));
        assertThrows(NullPointerException.class,
                () -> new ClientConfig(List.of("host:123"), 1, 1, null));
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
        assertEquals(ConnectionPolicy.DEFAULT, cfg.connectionPolicy());
        assertEquals(1, cfg.storageConnectionsPerEndpoint());
        ConnectionPolicy policy = ConnectionPolicy.DEFAULT.withHeartbeatIntervalMs(1234);
        assertEquals(policy, cfg.withConnectionPolicy(policy).connectionPolicy());
        assertEquals(3, cfg.withStorageConnectionsPerEndpoint(3).storageConnectionsPerEndpoint());
    }
}
