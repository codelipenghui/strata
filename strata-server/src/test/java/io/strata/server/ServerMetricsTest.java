package io.strata.server;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strata.node.DataNodeConfig;
import io.strata.node.DataNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Smoke-tests that {@link ServerMetrics#registerDataNode} registers the expected read-counter meters.
 */
class ServerMetricsTest {

    @TempDir
    Path dir;

    @Test
    void registerNodeExposesReadCounterMeters() throws Exception {
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir))) {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            ServerMetrics.registerDataNode(registry, node);

            assertNotNull(registry.find("strata_data_node_read_ops").functionCounter(),
                    "strata_data_node_read_ops FunctionCounter must be registered");
            assertNotNull(registry.find("strata_data_node_read_bytes").functionCounter(),
                    "strata_data_node_read_bytes FunctionCounter must be registered");
        }
    }
}
