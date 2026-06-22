package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkId;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenQuorumFailureTest {

    @Test
    void failedInitialOpenAbortsMetadataTail() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(new ClientConfig(
                     List.of(cluster.metaEndpoint()), 4096, 500))) {
            FileId fileId = client.create(StrataClient.FileSpec.log("test", "/open-abort")).id();

            cluster.killNode(1);
            cluster.killNode(2);

            try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[]{1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
            }

            var lookup = lookup(cluster.metaEndpoint(), fileId);
            assertEquals(0, lookup.chunks().size(),
                    "failed OPEN_CHUNK quorum must not strand an OPEN metadata tail");
        }
    }

    @Test
    void partialInitialOpenSealsShortReplicaSetAndRepairsAfterFailedNodeReturns() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(new ClientConfig(
                     List.of(cluster.metaEndpoint()), 4096, 500))) {
            FileId fileId = client.create(StrataClient.FileSpec.log("test", "/partial-open-repair")).id();
            int victimIndex = 0;
            int victimNodeId = cluster.nodes.get(victimIndex).nodeId();
            cluster.killNode(victimIndex);

            Workload workload = new Workload();
            StrataFile.SealInfo sealed;
            try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                workload.appendAcked(appender, 0, 1);
                sealed = appender.seal();
            }

            Messages.LookupFileResp shortSet = ConsistencyVerifier.lookupFile(cluster, fileId);
            assertEquals(1, shortSet.chunks().size());
            Messages.ChunkInfo chunk = shortSet.chunks().get(0);
            ChunkId chunkId = chunk.chunkId();
            assertEquals(2, chunk.replicas().size(),
                    "partial open must seal only replicas that actually opened");
            assertTrue(chunk.replicas().stream().noneMatch(replica -> replica.nodeId() == victimNodeId),
                    "failed-open replica must not remain in the sealed descriptor");

            DataNode restarted = cluster.restartNode(victimIndex);
            waitForNodeId(restarted, victimNodeId);
            waitForChunkRepaired(cluster, fileId, chunkId, victimNodeId);

            workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
            ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
        }
    }

    private static Messages.LookupFileResp lookup(String endpoint, FileId fileId) throws Exception {
        String[] hp = endpoint.split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "lookup")) {
            ByteBuffer h = direct.call(Opcode.LOOKUP_FILE, new Messages.LookupFile(StrataNamespace.of("test"), fileId).encode(), null, 5000);
            return Messages.LookupFileResp.decode(h);
        }
    }

    private static void waitForNodeId(DataNode node, int expectedNodeId) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (node.nodeId() != expectedNodeId && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(expectedNodeId, node.nodeId(), "node identity must survive restart");
    }

    private static void waitForChunkRepaired(MiniCluster cluster, FileId fileId, ChunkId chunkId,
                                             int expectedNodeId) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            Messages.LookupFileResp lookup = ConsistencyVerifier.lookupFile(cluster, fileId);
            for (Messages.ChunkInfo chunk : lookup.chunks()) {
                if (!chunk.chunkId().equals(chunkId)) {
                    continue;
                }
                boolean hasExpectedNode = chunk.replicas().stream()
                        .anyMatch(replica -> replica.nodeId() == expectedNodeId);
                boolean allPresent = chunk.replicas().stream()
                        .allMatch(replica -> nodeById(cluster, replica.nodeId()).store().contains(chunkId));
                if (chunk.replicas().size() == 3 && hasExpectedNode && allPresent) {
                    return;
                }
            }
            Thread.sleep(250);
        }
        throw new AssertionError("partial-open chunk was not repaired to RF=3");
    }

    private static DataNode nodeById(MiniCluster cluster, int nodeId) {
        for (DataNode node : cluster.nodes) {
            if (node.nodeId() == nodeId) {
                return node;
            }
        }
        throw new AssertionError("node " + nodeId + " not found");
    }
}
