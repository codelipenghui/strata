package io.strata.server;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strata.common.ChunkId;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.node.DataNode;
import io.strata.node.DataNodeConfig;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.RequestObserver;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke-tests that {@link ServerMetrics#registerDataNode} registers the expected read-counter meters.
 */
class ServerMetricsTest {

    @TempDir
    Path dir;

    @Test
    void registerNodeExposesChannelCacheAndFdMeters() throws Exception {
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir))) {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            ServerMetrics.registerDataNode(registry, node, 10_000);

            assertNotNull(registry.find("strata_data_node_filechannel_cache").tag("event", "hit").functionCounter());
            assertNotNull(registry.find("strata_data_node_filechannel_cache").tag("event", "miss").functionCounter());
            assertNotNull(registry.find("strata_data_node_filechannel_cache").tag("event", "eviction").functionCounter());
            assertNotNull(registry.find("strata_data_node_filechannel_cache_size").gauge());
            assertNotNull(registry.find("strata_data_node_filechannel_cache_capacity").gauge());
            assertNotNull(registry.find("strata_data_node_open_fds").gauge());
            assertNotNull(registry.find("strata_data_node_orphan_gc_breaker_open_namespaces").gauge());
            assertNotNull(registry.find("strata_data_node_orphan_gc_node_breaker_open").gauge());
            assertNotNull(registry.find("strata_data_node_orphan_gc_breaker_halted_namespaces").gauge());
            assertNotNull(registry.find("strata_data_node_orphan_gc_breaker_halted_chunks").gauge());
            assertNotNull(registry.find("strata_data_node_orphan_gc_breaker_trips_total").functionCounter());
            assertNotNull(registry.find("strata_data_node_orphan_gc_breaker_skipped_chunk_total").functionCounter());
            assertNotNull(registry.find("strata_data_node_owner_epoch_fence_rejects").functionCounter());
        }
    }

    @Test
    void perNamespaceDataCountersRegisterLazily() throws Exception {
        // No I/O yet → no namespace → the per-namespace counters are absent (registered lazily on first I/O).
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir))) {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            ServerMetrics.registerDataNode(registry, node, 10_000);
            ServerMetrics.registerNewDataNodeNamespaces(registry, node); // idempotent; no-op with no namespaces
            assertNull(registry.find("strata_data_node_append_bytes").functionCounter(),
                    "no per-namespace counter until a namespace sees I/O");
        }
    }

    @Test
    void perNamespaceDataCountersExportedAfterNodeIo() throws Exception {
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir))) {
            StrataNamespace ns = StrataNamespace.of("ns1");
            ChunkId chunk = new ChunkId(FileId.of(1), 0);
            byte[] payload = "namespace-throughput".getBytes(StandardCharsets.UTF_8);
            // Drive a real OPEN_CHUNK + APPEND straight at the node's data plane (no controller/placement),
            // so its ChunkStore records per-namespace I/O for "ns1".
            try (ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_TOOL, "ns-metrics-test")) {
                client.call(Opcode.OPEN_CHUNK,
                        new Messages.OpenChunk(chunk, 1, false, 1L << 20, 1718000000000L, ns).encode(), null, 5_000);
                client.call(Opcode.APPEND,
                        new Messages.Append(chunk, 1, 0, 0, ns).encode(), ByteBuffer.wrap(payload), 5_000);
            }

            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            ServerMetrics.registerDataNode(registry, node, 10_000);
            ServerMetrics.registerNewDataNodeNamespaces(registry, node); // force the lazy registration now
            ServerMetrics.registerNewDataNodeNamespaces(registry, node);

            var appendBytes = registry.find("strata_data_node_append_bytes")
                    .tag("namespace", "ns1").functionCounter();
            assertNotNull(appendBytes, "per-namespace append_bytes counter must register after node I/O");
            assertTrue(appendBytes.count() >= payload.length,
                    "append_bytes for ns1 must reflect the written payload");
            long appendBytesMeters = registry.getMeters().stream()
                    .filter(m -> "strata_data_node_append_bytes".equals(m.getId().getName()))
                    .filter(m -> "ns1".equals(m.getId().getTag("namespace")))
                    .count();
            assertEquals(1, appendBytesMeters, "lazy registration must not duplicate namespace meters");
        }
    }

    @Test
    void requestObserverTagsNamespace() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RequestObserver obs = ServerMetrics.requestObserver(registry,
                new long[]{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}, 1);
        obs.observe("READ", "orders", 1_000_000L, true);
        assertNotNull(registry.find("strata_scp_request_duration")
                        .tag("namespace", "orders").tag("opcode", "READ").tag("status", "ok").timer(),
                "request timer must carry a namespace tag");
        assertNotNull(registry.find("strata_scp_requests")
                        .tag("namespace", "orders").tag("opcode", "READ").tag("status", "ok").functionCounter(),
                "exact request counter must carry the same tags");
    }

    @Test
    void requestObserverKeepsExactCountWhenLatencyIsSampled() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RequestObserver obs = ServerMetrics.requestObserver(registry,
                new long[]{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}, 16);

        for (int i = 0; i < 10; i++) {
            obs.observe("APPEND", "orders", 1_000_000L, true);
        }

        var requests = registry.find("strata_scp_requests")
                .tag("namespace", "orders").tag("opcode", "APPEND").tag("status", "ok").functionCounter();
        assertNotNull(requests);
        assertEquals(10.0, requests.count());
    }

    @Test
    void requestObserverAlwaysRecordsErrorLatency() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RequestObserver obs = ServerMetrics.requestObserver(registry,
                new long[]{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}, 16);

        obs.observe("APPEND", "orders", 2_000_000L, false);

        var requests = registry.find("strata_scp_requests")
                .tag("namespace", "orders").tag("opcode", "APPEND").tag("status", "error").functionCounter();
        var latency = registry.find("strata_scp_request_duration")
                .tag("namespace", "orders").tag("opcode", "APPEND").tag("status", "error").timer();
        assertNotNull(requests);
        assertNotNull(latency);
        assertEquals(1.0, requests.count());
        assertEquals(1L, latency.count());
    }
}
