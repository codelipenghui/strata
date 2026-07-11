package io.strata.server;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.meta.ControllerConfig;
import io.strata.meta.ZkMetadataStore;
import io.strata.node.DataNodeConfig;
import io.strata.proto.BufWriter;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The {@code combined} run mode hosts a {@link io.strata.meta.Controller} and a
 * {@link io.strata.node.DataNode} in one JVM. This proves both halves come up and interoperate in
 * one process: the co-resident data node registers with the co-resident controller leader, and a client
 * can create files through that same meta.
 */
class CombinedServerTest {

    @Test
    void combinedNodeServesMetadataAndRegistersItsDataNode() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            Path dataDir = Files.createTempDirectory("strata-combined-it");
            int port = freePort();
            // The meta is embedded (server-less) and shares the node's listener, so its own listenPort
            // is unused here.
            ControllerConfig controllerConfig = ControllerConfig.forTests(zk.getConnectString());
            DataNodeConfig nodeConfig = DataNodeConfig
                    .withMetadata(dataDir, List.of("127.0.0.1:" + port), "host-0")
                    .withListenPort(port)
                    .withNodeId(1);  // node id is now externally supplied (STRATA_NODE_ID)

            try (StrataServer.Combined combined = StrataServer.startCombined(controllerConfig, nodeConfig)) {
                // Both planes share ONE listener: the co-resident node registers with the co-resident
                // controller leader over that single port (registration is leader-gated, so this also
                // confirms the embedded meta won the latch).
                awaitRegistered(zk.getConnectString(), 1);

                // Metadata RPCs are served on the same SCP port as the data plane.
                try (StrataClient client = StrataClient.connect(ClientConfig.of(combined.node().endpoint()))) {
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

    /** The shared combined listener must route data-plane repair opcodes to the data-node handler. */
    @Test
    void combinedNodeRoutesExecReplicateToDataPlane() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            Path dataDir = Files.createTempDirectory("strata-combined-exec-replicate");
            int port = freePort();
            ControllerConfig controllerConfig = ControllerConfig.forTests(zk.getConnectString());
            DataNodeConfig nodeConfig = DataNodeConfig
                    .withMetadata(dataDir, List.of("127.0.0.1:" + port), "host-0")
                    .withListenPort(port)
                    .withNodeId(1);

            try (StrataServer.Combined combined = StrataServer.startCombined(controllerConfig, nodeConfig);
                 ScpClient client = new ScpClient("127.0.0.1", port, ScpClient.KIND_TOOL, "repair-test")) {

                // Build a minimal EXEC_REPLICATE payload (a ReplicateCmd with no sources).
                BufWriter w = new BufWriter();
                Messages.Command.writeRequest(w, new Messages.ReplicateCmd(
                        1L,
                        new ChunkId(FileId.of(42), 0),
                        List.of(),
                        (byte) 0, 0, 0,
                        StrataNamespace.of("test-ns")));

                ScpException ex = assertThrows(ScpException.class,
                        () -> client.call(Opcode.EXEC_REPLICATE, w.toBytes(), null, 5_000));
                assertEquals(ErrorCode.INTERNAL.code, ex.code().code,
                        "EXEC_REPLICATE must be served by the data plane (INTERNAL from DataNodeHandlers), "
                                + "not UNKNOWN_OPCODE from the Controller; got: " + ex.getMessage());
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
