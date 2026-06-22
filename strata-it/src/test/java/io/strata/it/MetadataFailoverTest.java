package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.node.DataNode;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Metadata failover (tech design §4.4 / §7.4): two metadata instances over one ZooKeeper.
 * Kill the leader mid-stream; the standby takes over via leader election. Asserts:
 *  - appends keep acking THROUGH the failover, including chunk rolls (retry + endpoint rotation)
 *  - data-node identity is stable across re-registration with the new leader
 *  - every acked byte reads back afterwards
 *  - the standby never ran failure detection while it wasn't leader (no spurious DEAD nodes)
 */
class MetadataFailoverTest {

    @Test
    void leaderKillMidStreamIsAbsorbed() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3, null, 2)) {
            ClientConfig cfg = new ClientConfig(cluster.metaEndpoints(), 1024, 5_000);
            try (StrataClient client = StrataClient.connect(cfg)) {
                FileId fileId = client.create(StrataClient.FileSpec.log("test", "/failover")).id();
                Workload workload = new Workload();

                List<Integer> nodeIdsBefore = new ArrayList<>();
                for (var n : cluster.nodes) nodeIdsBefore.add(n.nodeId());

                try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                    workload.appendAcked(appender, 0, 300);

                    // kill the current leader
                    int leaderIdx = -1;
                    for (int i = 0; i < cluster.metas.size(); i++) {
                        if (cluster.metas.get(i).isLeader()) leaderIdx = i;
                    }
                    assertTrue(leaderIdx >= 0, "no leader before failover");
                    cluster.killMeta(leaderIdx);

                    // write straight through the failover: ~8 chunk rolls must survive the
                    // window where the standby is acquiring leadership and nodes re-register
                    workload.appendAcked(appender, 300, 600);
                    var sealed = appender.seal();
                    assertEquals(workload.ackedBytes(), sealed.sealedLength());
                }

                cluster.awaitAnyLeader();
                long leaders = cluster.metas.stream().filter(m -> {
                    try {
                        return m.isLeader();
                    } catch (Exception e) {
                        return false;
                    }
                }).count();
                assertEquals(1, leaders, "exactly the survivor should lead");

                // every acked byte reads back through the new leader
                workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
                ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, workload.ackedBytes());

                // volume-bound identity survived re-registration with the new leader
                // (NodeRegistry falls back to the persistent store for incarnation matching)
                for (int i = 0; i < cluster.nodes.size(); i++) {
                    assertEquals(nodeIdsBefore.get(i), cluster.nodes.get(i).nodeId(),
                            "node " + i + " changed identity across metadata failover");
                }
            }
        }
    }

    @Test
    void leaderFailoverAfterReplicaSealBeforeMetadataCommitIsRecovered() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3, null, 2)) {
            ClientConfig cfg = new ClientConfig(cluster.metaEndpoints(), 1 << 16, 5_000)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (StrataClient client = StrataClient.connect(cfg)) {
                FileId fileId = client.create(StrataClient.FileSpec.log("test",
                        "/failover-after-replica-seal")).id();
                Workload workload = new Workload();
                StrataFile.Appender zombie = client.openById(StrataNamespace.of("test"), fileId).openForAppend();
                try {
                    workload.appendAcked(zombie, 0, 80);
                    long ackedBytes = workload.ackedBytes();

                    Messages.ChunkInfo openChunk = latestChunk(cluster, fileId);
                    assertEquals(ChunkState.OPEN, openChunk.state());
                    Messages.Replica sealedReplica =
                            ConsistencyVerifier.waitForOpenReplicaEndAtLeast(openChunk, ackedBytes);
                    Messages.SealResp replicaSeal =
                            ConsistencyVerifier.sealReplicaOnly(sealedReplica, openChunk, ackedBytes);
                    assertEquals(ackedBytes, replicaSeal.finalLength());
                    assertEquals(ChunkState.OPEN, latestChunk(cluster, fileId).state(),
                            "metadata must still be open before failover");

                    cluster.killMeta(leaderIndex(cluster));
                    cluster.awaitAnyLeader();

                    StrataFile.SealInfo sealed = client.openById(StrataNamespace.of("test"), fileId).recoverAndSeal();
                    assertEquals(ackedBytes, sealed.sealedLength(),
                            "recovery after metadata failover must preserve the replica-sealed prefix");

                    Throwable t = assertThrows(CompletionException.class,
                            () -> zombie.append(ByteBuffer.wrap("zombie-write".getBytes())).join()).getCause();
                    assertTrue(t instanceof ScpException se && se.code() == ErrorCode.FENCED_EPOCH,
                            "expected FENCED_EPOCH, got " + t);

                    workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
                    ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                            sealed.sealedLength());
                } finally {
                    zombie.close();
                }
            }
        }
    }

    @Test
    void leaderFailoverDuringOpenFileDeletionConvergesAndDoesNotResurrect() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3, null, 2)) {
            ClientConfig cfg = new ClientConfig(cluster.metaEndpoints(), 1 << 16, 5_000)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (StrataClient client = StrataClient.connect(cfg)) {
                FileId fileId = client.create(StrataClient.FileSpec.log("test",
                        "/failover-delete-open")).id();
                Workload workload = new Workload();
                StrataFile.Appender stale = client.openById(StrataNamespace.of("test"), fileId).openForAppend();
                try {
                    workload.appendAcked(stale, 0, 80);
                    List<ChunkId> chunkIds = ConsistencyVerifier.lookupFile(cluster, fileId).chunks().stream()
                            .map(Messages.ChunkInfo::chunkId)
                            .toList();
                    assertTrue(!chunkIds.isEmpty(), "test setup requires an open metadata chunk");

                    client.deleteById(StrataNamespace.of("test"), fileId);
                    cluster.killMeta(leaderIndex(cluster));
                    cluster.awaitAnyLeader();

                    ScpException sealFailure = assertThrows(ScpException.class, stale::seal,
                            "stale appender must not seal or resurrect a DELETING file after failover");
                    assertTrue(sealFailure.code() == ErrorCode.PRECONDITION_FAILED
                                    || sealFailure.code() == ErrorCode.CHUNK_NOT_FOUND
                                    || sealFailure.code() == ErrorCode.INTERNAL,
                            "unexpected stale seal failure code: " + sealFailure.code());

                    waitForFileDeleted(cluster, fileId);
                    assertNoNodeContains(cluster, chunkIds);
                } finally {
                    stale.close();
                }
            }
        }
    }

    @Test
    void staleInventoryAfterLeaderFailoverCannotDropHealthyReplica() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3, null, 2)) {
            ClientConfig cfg = new ClientConfig(cluster.metaEndpoints(), 1 << 16, 5_000)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (StrataClient client = StrataClient.connect(cfg)) {
                FileId fileId = client.create(StrataClient.FileSpec.log("test",
                        "/failover-stale-inventory")).id();
                Workload workload = new Workload();
                StrataFile.SealInfo sealed;
                try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                    workload.appendAcked(appender, 0, 160);
                    sealed = appender.seal();
                }

                Messages.LookupFileResp before =
                        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                                sealed.sealedLength());
                Messages.Replica replica = before.chunks().get(0).replicas().get(0);
                DataNode node = nodeById(cluster, replica.nodeId());

                cluster.killMeta(leaderIndex(cluster));
                cluster.awaitAnyLeader();

                sendInventoryThroughAnyEndpoint(cluster, new Messages.InventoryReport(replica.nodeId(),
                        node.incarnation().getMostSignificantBits(),
                        node.incarnation().getLeastSignificantBits(),
                        -1, 0, 1, List.of()));

                Messages.LookupFileResp after = ConsistencyVerifier.lookupFile(cluster, fileId);
                assertTrue(after.chunks().get(0).replicas().stream()
                                .anyMatch(r -> r.nodeId() == replica.nodeId()),
                        "stale inventory on the new leader must not drop a healthy replica");
                workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
                ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                        sealed.sealedLength());
            }
        }
    }

    private static Messages.ChunkInfo latestChunk(MiniCluster cluster, FileId fileId) throws Exception {
        Messages.LookupFileResp lookup = ConsistencyVerifier.lookupFile(cluster, fileId);
        return lookup.chunks().get(lookup.chunks().size() - 1);
    }

    private static int leaderIndex(MiniCluster cluster) {
        for (int i = 0; i < cluster.metas.size(); i++) {
            if (cluster.metas.get(i).isLeader()) {
                return i;
            }
        }
        throw new AssertionError("no leader before failover");
    }

    private static DataNode nodeById(MiniCluster cluster, int nodeId) {
        for (DataNode node : cluster.nodes) {
            if (node.nodeId() == nodeId) {
                return node;
            }
        }
        throw new AssertionError("node " + nodeId + " not found");
    }

    private static void waitForFileDeleted(MiniCluster cluster, FileId fileId) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(250);
            try {
                lookupThroughAnyEndpoint(cluster, fileId);
            } catch (ScpException e) {
                assertEquals(ErrorCode.FILE_NOT_FOUND, e.code());
                return;
            }
        }
        throw new AssertionError("file record did not converge to deleted");
    }

    private static Messages.LookupFileResp lookupThroughAnyEndpoint(MiniCluster cluster, FileId fileId) {
        ScpException last = null;
        for (String endpoint : cluster.metaEndpoints()) {
            String[] hp = endpoint.split(":");
            try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                    ScpClient.KIND_TOOL, "failover-lookup")) {
                ByteBuffer h = direct.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(StrataNamespace.of("test"), fileId).encode(), null, 5_000);
                return Messages.LookupFileResp.decode(h);
            } catch (ScpException e) {
                last = e;
            } catch (Exception e) {
                last = new ScpException(ErrorCode.INTERNAL, e.toString());
            }
        }
        throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "no controller endpoint");
    }

    private static void sendInventoryThroughAnyEndpoint(MiniCluster cluster, Messages.InventoryReport report) {
        ScpException last = null;
        for (String endpoint : cluster.metaEndpoints()) {
            String[] hp = endpoint.split(":");
            try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                    ScpClient.KIND_TOOL, "failover-inventory")) {
                direct.call(Opcode.INVENTORY_REPORT, report.encode(), null, 5_000);
                return;
            } catch (ScpException e) {
                last = e;
            } catch (Exception e) {
                last = new ScpException(ErrorCode.INTERNAL, e.toString());
            }
        }
        throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "no controller endpoint");
    }

    private static void assertNoNodeContains(MiniCluster cluster, List<ChunkId> chunkIds) {
        for (DataNode node : cluster.nodes) {
            for (ChunkId id : chunkIds) {
                assertTrue(!node.store().contains(id),
                        "node " + node.nodeId() + " still holds deleted chunk " + id);
            }
        }
    }
}
