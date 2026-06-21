package io.strata.meta;

import io.strata.common.FileId;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared cluster liveness (design §11): data-node heartbeats reach only the global controller, so
 * the controller publishes a live-node snapshot to the consensus root. A non-controller namespace
 * owner — which has no heartbeat channel — merges that snapshot into its placement view and can place
 * chunk replicas for the namespaces it owns.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetadataSharedLivenessTest {

    private TestingServer zk;
    private Controller node0;
    private Controller node1;
    private ScpClient client0;
    private ScpClient client1;
    private List<String> endpoints;

    @BeforeAll
    void setup() throws Exception {
        zk = new TestingServer(true);
        int port0 = freePort();
        int port1 = freePort();
        String ep0 = "127.0.0.1:" + port0;
        String ep1 = "127.0.0.1:" + port1;
        endpoints = List.of(ep0, ep1);
        node0 = new Controller(shardedConfig(port0));
        node1 = new Controller(shardedConfig(port1));
        client0 = new ScpClient("127.0.0.1", port0, ScpClient.KIND_TOOL, "liveness-test-0");
        client1 = new ScpClient("127.0.0.1", port1, ScpClient.KIND_TOOL, "liveness-test-1");
    }

    @AfterAll
    void teardown() throws Exception {
        if (client0 != null) client0.close();
        if (client1 != null) client1.close();
        if (node0 != null) node0.close();
        if (node1 != null) node1.close();
        if (zk != null) zk.close();
    }

    private ControllerConfig shardedConfig(int port) {
        return new ControllerConfig(zk.getConnectString(), port, 200, 1_000, 1_500, 300, 3_000,
                5_000, 20_000, "127.0.0.1", 90_000, endpoints, 2);
    }

    @Test
    void bothOwnersIncludingNonControllerPlaceChunksViaSharedLiveness() throws Exception {
        awaitOneLeader();
        Controller controller = node0.isLeader() ? node0 : node1;
        Controller follower = node0.isLeader() ? node1 : node0;
        ScpClient controllerClient = controller == node0 ? client0 : client1;
        ScpClient followerClient = follower == node0 ? client0 : client1;
        assertTrue(controller.isLeader());
        assertTrue(!follower.isLeader());

        // 3 data nodes register + heartbeat with the cluster controller (REGISTER/HEARTBEAT require
        // the global latch). Distinct hosts so anti-affinity can place 3 replicas.
        for (int i = 0; i < 3; i++) {
            FakeDataNode f = new FakeDataNode("live-host-" + i, controllerClient);
            f.register();
            f.heartbeat();
        }
        controller.reconcileNow(); // controller publishes the live-node snapshot

        Messages.WritePolicy rf3 = new Messages.WritePolicy(3, 2, true);

        // controller owns nsC: places from its own in-memory liveness.
        String nsC = namespaceOwnedBy(controller.endpoint());
        assertEquals(3, placeChunkAndCountReplicas(controllerClient, nsC, "/live-c"),
                "controller places from in-memory liveness");

        // follower owns nsF: it has no heartbeat channel and must place from the shared snapshot.
        String nsF = namespaceOwnedBy(follower.endpoint());
        assertEquals(3, placeChunkAndCountReplicas(followerClient, nsF, "/live-f"),
                "non-controller owner places via the shared live-node snapshot");
    }

    private int placeChunkAndCountReplicas(ScpClient client, String namespace, String path) {
        var created = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile(namespace, path, new Messages.WritePolicy(3, 2, true)).encode(),
                null, 5_000));
        FileId fileId = created.fileId();
        var epoch = Messages.AllocateWriterEpochResp.decode(client.call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forAppend(fileId).encode(), null, 5_000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(fileId, epoch.writerEpoch()).encode(), null, 5_000));
        return chunk.replicas().size();
    }

    private void awaitOneLeader() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (node0.isLeader() == node1.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertNotEquals(node0.isLeader(), node1.isLeader(), "exactly one node should win the global latch");
    }

    private String namespaceOwnedBy(String endpoint) {
        for (int i = 0; i < 10_000; i++) {
            io.strata.common.StrataNamespace ns = io.strata.common.StrataNamespace.of("live-ns-" + i);
            if (NamespaceAssignmentPolicy.assign(ns, 0, endpoints, 2).preferredLeader().equals(endpoint)) {
                return ns.value();
            }
        }
        throw new IllegalStateException("no namespace found owned by " + endpoint);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** A data node that registers and heartbeats but executes nothing. */
    private static final class FakeDataNode {
        private final UUID inc = UUID.randomUUID();
        private final String host;
        private final ScpClient client;
        private int nodeId = -1;
        private long session = -1;

        FakeDataNode(String host, ScpClient client) {
            this.host = host;
            this.client = client;
        }

        void register() {
            var resp = Messages.RegisterResp.decode(client.call(Opcode.REGISTER_NODE,
                    new Messages.RegisterNode(inc.getMostSignificantBits(), inc.getLeastSignificantBits(),
                            List.of(host + ":9000"), "z1", "r1", host,
                            List.of(new Messages.StorageCapacity(1L << 40)), 1, 0).encode(),
                    null, 5_000));
            nodeId = resp.nodeId();
            session = resp.sessionEpoch();
        }

        void heartbeat() {
            Messages.HeartbeatResp.decode(client.call(Opcode.NODE_HEARTBEAT,
                    new Messages.NodeHeartbeat(nodeId, inc.getMostSignificantBits(),
                            inc.getLeastSignificantBits(), session,
                            List.of(new Messages.StorageUsage(0, 1L << 40)), 0, List.of()).encode(),
                    null, 5_000));
        }
    }
}
