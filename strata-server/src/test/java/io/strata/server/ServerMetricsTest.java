package io.strata.server;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strata.node.NodeConfig;
import io.strata.node.StorageNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Smoke-tests that {@link ServerMetrics#registerNode} registers the expected read-counter meters.
 */
class ServerMetricsTest {

    @TempDir
    Path dir;

    @Test
    void registerNodeExposesChannelCacheAndFdMeters() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            ServerMetrics.registerNode(registry, node);

            assertNotNull(registry.find("strata_node_filechannel_cache").tag("event", "hit").functionCounter());
            assertNotNull(registry.find("strata_node_filechannel_cache").tag("event", "miss").functionCounter());
            assertNotNull(registry.find("strata_node_filechannel_cache").tag("event", "eviction").functionCounter());
            assertNotNull(registry.find("strata_node_filechannel_cache_size").gauge());
            assertNotNull(registry.find("strata_node_filechannel_cache_capacity").gauge());
            assertNotNull(registry.find("strata_node_open_fds").gauge());
        }
    }

    @Test
    void registerNodeExposesReadCounterMeters() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            ServerMetrics.registerNode(registry, node);

            assertNotNull(registry.find("strata_node_read_ops").functionCounter(),
                    "strata_node_read_ops FunctionCounter must be registered");
            assertNotNull(registry.find("strata_node_read_bytes").functionCounter(),
                    "strata_node_read_bytes FunctionCounter must be registered");
        }
    }
}
