package io.strata.server;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.common.FileId;
import io.strata.meta.MetaConfig;
import io.strata.meta.ZkMetadataStore;
import io.strata.node.NodeConfig;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The {@code combined} run mode hosts a {@link io.strata.meta.MetadataService} and a
 * {@link io.strata.node.StorageNode} in one JVM. This proves both halves come up and interoperate in
 * one process: the co-resident storage node registers with the co-resident meta leader, and a client
 * can create files through that same meta.
 */
class CombinedServerTest {

    @Test
    void combinedNodeServesMetadataAndRegistersItsStorageNode() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            Path dataDir = Files.createTempDirectory("strata-combined-it");
            int metaPort = freePort();
            MetaConfig metaConfig = new MetaConfig(zk.getConnectString(), metaPort,
                    200, 1_000, 1_500, 300, 3_000, 5_000, 20_000, "127.0.0.1");
            NodeConfig nodeConfig = NodeConfig.withMetadata(
                    dataDir, List.of("127.0.0.1:" + metaPort), "host-0");

            try (StrataServer.Combined combined = StrataServer.startCombined(metaConfig, nodeConfig)) {
                // Both halves are up and talking: the co-resident node registers with the
                // co-resident meta leader (registration is leader-gated, so this also confirms the
                // embedded meta won the latch).
                awaitRegistered(zk.getConnectString(), 1);

                // The co-resident meta serves client metadata RPCs.
                try (StrataClient client = StrataClient.connect(ClientConfig.of(combined.meta().endpoint()))) {
                    FileId a = client.create(StrataClient.FileSpec.log("combined", "/a")).id();
                    FileId b = client.create(StrataClient.FileSpec.log("combined", "/b")).id();
                    assertNotNull(a);
                    assertNotEquals(a, b);
                }
            } finally {
                deleteRecursively(dataDir);
            }
        }
    }

    private static void awaitRegistered(String zkConnect, int count) throws Exception {
        try (ZkMetadataStore store = new ZkMetadataStore(zkConnect)) {
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                if (store.listNodes().size() >= count) {
                    return;
                }
                Thread.sleep(50);
            }
        }
        throw new AssertionError("combined node did not register within 15s");
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort temp cleanup
                }
            });
        }
    }
}
