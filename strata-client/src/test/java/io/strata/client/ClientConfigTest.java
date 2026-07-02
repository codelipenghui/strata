package io.strata.client;

import io.strata.common.ChunkLimits;
import io.strata.common.ConnectionPolicy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                () -> new ClientConfig(List.of("host:123"), 1, 1, ConnectionPolicy.DEFAULT, 0, 0L, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new ClientConfig(List.of("host:123"), 1, 1, ConnectionPolicy.DEFAULT, -1, 0L, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new ClientConfig(List.of("host:123"), 1, 1, ConnectionPolicy.DEFAULT, 1, 1L, 1, 1, 0, 1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new ClientConfig(List.of("host:123"), 1, 1, ConnectionPolicy.DEFAULT, 1, 1L, 1, 1, 1, 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ClientConfig.of("host:123").withMaxChunkRecords(0));
        assertThrows(NullPointerException.class,
                () -> new ClientConfig(List.of("host:123"), 1, 1, null));
    }

    @Test
    void failFastOnNonPositiveClientTuningOverrides() {
        ClientConfig base = ClientConfig.of("host:123");
        assertThrows(IllegalArgumentException.class, () -> base.withControllerRetryDeadlineMs(0));
        assertThrows(IllegalArgumentException.class, () -> base.withRecoveryCopyChunkBytes(-1));
    }

    @Test
    void newClientTuningFieldsDefault() {
        ClientConfig c = ClientConfig.of("host:123");
        assertEquals(15_000L, c.controllerRetryDeadlineMs());
        assertEquals(200, c.controllerRetryBackoffMs());
        assertEquals(4 * 1024 * 1024, c.recoveryCopyChunkBytes());
        assertEquals(64, c.appendReplicaInflightHighWatermark());
        assertEquals(Math.max(1, (io.strata.proto.ScpClient.maxPendingRequests() * 3) / 4),
                c.appendConnectionPendingHighWatermark());
        assertEquals(ChunkLimits.DEFAULT_MAX_CLIENT_CHUNK_RECORDS, c.maxChunkRecords());
        assertTrue(c.maxChunkRecords() < ChunkLimits.DEFAULT_MAX_OPEN_CHUNK_LEDGER_ENTRIES);
    }

    @Test
    void withSettersOverrideClientTuning() {
        ClientConfig c = ClientConfig.of("host:123")
                .withControllerRetryDeadlineMs(30_000L).withControllerRetryBackoffMs(50)
                .withRecoveryCopyChunkBytes(1 << 20)
                .withAppendWatermarks(32, 128)
                .withMaxChunkRecords(512);
        assertEquals(30_000L, c.controllerRetryDeadlineMs());
        assertEquals(50, c.controllerRetryBackoffMs());
        assertEquals(1 << 20, c.recoveryCopyChunkBytes());
        assertEquals(32, c.appendReplicaInflightHighWatermark());
        assertEquals(128, c.appendConnectionPendingHighWatermark());
        assertEquals(512, c.maxChunkRecords());
    }

    @Test
    void copiesEndpointListAndAcceptsBracketedIpv6StyleEndpoint() {
        List<String> endpoints = new ArrayList<>();
        endpoints.add("[::1]:2181");

        ClientConfig cfg = new ClientConfig(endpoints, 1024, 500);
        endpoints.set(0, "127.0.0.1:9999");

        assertEquals(List.of("[::1]:2181"), cfg.controllerEndpoints());
        assertThrows(UnsupportedOperationException.class, () -> cfg.controllerEndpoints().add("127.0.0.1:1"));
        assertEquals(2048, cfg.withChunkRollBytes(2048).chunkRollBytes());
        assertEquals(ConnectionPolicy.DEFAULT, cfg.connectionPolicy());
        assertEquals(1, cfg.dataNodeConnectionsPerEndpoint());
        ConnectionPolicy policy = ConnectionPolicy.DEFAULT.withHeartbeatIntervalMs(1234);
        assertEquals(policy, cfg.withConnectionPolicy(policy).connectionPolicy());
        assertEquals(3, cfg.withDataNodeConnectionsPerEndpoint(3).dataNodeConnectionsPerEndpoint());
    }
}
