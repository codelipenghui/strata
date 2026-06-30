package io.strata.meta;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Namespace-sharded routing (design §6): a two-controller cluster where each namespace has exactly one
 * owner (rendezvous). The owner serves its namespaces; a non-owner answers NOT_LEADER carrying the owner
 * endpoint, so the owner-aware client (see {@code ControllerClient}) caches namespace→owner and routes directly,
 * re-resolving only on this redirect. Independent of which controller holds the global cluster latch.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetadataShardingRoutingTest {

    private TestingServer zk;
    private Controller node0;
    private Controller node1;
    private ScpClient client0;
    private ScpClient client1;
    private String ep0;
    private String ep1;
    private List<String> endpoints;

    @BeforeAll
    void setup() throws Exception {
        zk = new TestingServer(true);
        int port0 = freePort();
        int port1 = freePort();
        ep0 = "127.0.0.1:" + port0;
        ep1 = "127.0.0.1:" + port1;
        endpoints = List.of(ep0, ep1);
        node0 = new Controller(shardedConfig(port0));
        node1 = new Controller(shardedConfig(port1));
        client0 = new ScpClient("127.0.0.1", port0, ScpClient.KIND_TOOL, "shard-test-0");
        client1 = new ScpClient("127.0.0.1", port1, ScpClient.KIND_TOOL, "shard-test-1");
        assertEquals(ep0, node0.endpoint());
        assertEquals(ep1, node1.endpoint());
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
                60_000, 5_000, 20_000, "127.0.0.1", 90_000, endpoints, 2, 0, 0, 0, 0, 0, 0, -1);
    }

    @Test
    void ownerServesItsNamespaceAndNonOwnerRedirectsToTheOwner() {
        String nsOf0 = namespaceOwnedBy(ep0);
        String nsOf1 = namespaceOwnedBy(ep1);

        // node0 owns nsOf0: it serves CREATE_FILE; node1 redirects with NOT_LEADER → ep0.
        var created0 = Messages.CreateFileResp.decode(client0.call(Opcode.CREATE_FILE,
                new Messages.CreateFile(nsOf0, "/seg-0").encode(), null, 5_000));
        ScpException redirect0 = assertThrows(ScpException.class, () -> client1.call(Opcode.CREATE_FILE,
                new Messages.CreateFile(nsOf0, "/seg-1").encode(), null, 5_000));
        assertEquals(ErrorCode.NOT_LEADER, redirect0.code());
        assertEquals(ep0, redirect0.leaderHint(), "non-owner redirects to the namespace's owner");

        // node1 owns nsOf1: it serves; node0 redirects → ep1.
        var created1 = Messages.CreateFileResp.decode(client1.call(Opcode.CREATE_FILE,
                new Messages.CreateFile(nsOf1, "/seg-0").encode(), null, 5_000));
        ScpException redirect1 = assertThrows(ScpException.class, () -> client0.call(Opcode.CREATE_FILE,
                new Messages.CreateFile(nsOf1, "/seg-1").encode(), null, 5_000));
        assertEquals(ErrorCode.NOT_LEADER, redirect1.code());
        assertEquals(ep1, redirect1.leaderHint());

        // each owner resolves its own file; a file-scoped op carries the namespace so routing is by
        // the supplied namespace's owner — the wrong node redirects to the owning controller.
        StrataNamespace owningNs = StrataNamespace.of(nsOf0);
        assertEquals(nsOf0, Messages.LookupFileResp.decode(client0.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(owningNs, created0.fileId()).encode(), null, 5_000)).namespace().value());
        ScpException wrongNode = assertThrows(ScpException.class, () -> client1.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(owningNs, created0.fileId()).encode(), null, 5_000));
        assertEquals(ErrorCode.NOT_LEADER, wrongNode.code());
        assertEquals(ep0, wrongNode.leaderHint(), "file-id op redirects to the supplied namespace's owner");
    }

    @Test
    void theNonControllerNodeStillServesItsOwnedNamespaces() throws Exception {
        // Exactly one node holds the global cluster-controller latch; the other is a follower. The
        // follower must still serve CREATE_FILE for the namespaces it owns (ownership != latch).
        awaitOneLeader();
        Controller follower = node0.isLeader() ? node1 : node0;
        ScpClient followerClient = follower == node0 ? client0 : client1;
        assertFalse(follower.isLeader(), "the follower must not hold the global latch");

        String nsOwnedByFollower = namespaceOwnedBy(follower.endpoint());
        var created = Messages.CreateFileResp.decode(followerClient.call(Opcode.CREATE_FILE,
                new Messages.CreateFile(nsOwnedByFollower, "/follower-seg-0").encode(), null, 5_000));
        var look = Messages.LookupFileResp.decode(followerClient.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(StrataNamespace.of(nsOwnedByFollower), created.fileId()).encode(), null, 5_000));
        assertEquals(nsOwnedByFollower, look.namespace().value());
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
            StrataNamespace ns = StrataNamespace.of("shard-ns-" + i);
            if (NamespaceAssignmentPolicy.assign(ns, 0, endpoints, 2).preferredLeader().equals(endpoint)) {
                return ns.value();
            }
        }
        throw new IllegalStateException("no namespace found owned by " + endpoint);
    }

    @Test
    void shardedCreateAndDeleteLeaveNoOwnerIndexZnode() throws Exception {
        // Use a namespace owned by ep0 so we exercise the sharded path on node0.
        String ns = namespaceOwnedBy(ep0);
        // CREATE_FILE: sharded path; pre-Task-4 code wrote /strata/meta/fidx/<fileId> here.
        FileId id = Messages.CreateFileResp.decode(client0.call(Opcode.CREATE_FILE,
                new Messages.CreateFile(ns, "/fidx-check").encode(), null, 5_000)).fileId();
        // DELETE_FILES: cleans up the file; also wrote the owner-index on old code.
        Messages.DeleteFilesResp del = Messages.DeleteFilesResp.decode(client0.call(Opcode.DELETE_FILES,
                new Messages.DeleteFiles(StrataNamespace.of(ns), List.of(id)).encode(), null, 5_000));
        assertEquals(ErrorCode.OK.code, del.codes().get(0));

        // Assert that no per-file owner-index znode was ever created.
        // The parent /strata/meta IS created by the store (confirmed: controller writes it on start-up),
        // so checkExists returning null for /strata/meta/fidx is meaningful, not a missing-parent false pass.
        try (CuratorFramework zkClient = CuratorFrameworkFactory.newClient(
                zk.getConnectString(), new RetryOneTime(100))) {
            zkClient.start();
            assertNull(zkClient.checkExists().forPath("/strata/meta/fidx"),
                    "no per-file owner-index znode should exist after a sharded create+delete");
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
