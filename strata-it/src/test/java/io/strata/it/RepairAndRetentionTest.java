package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.format.ChunkFormats;
import io.strata.meta.ControllerConfig;
import io.strata.node.DataNode;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Repair, reconciliation, and retention end-to-end (tech design §7.2, §9): a dead node's sealed
 * chunks are re-replicated by the pool (RF restored, byte-identical), orphans are reconciled
 * away, and file deletion physically converges on the nodes.
 */
class RepairAndRetentionTest {
    private static final StrataNamespace TEST_NS = StrataNamespace.of("test");
    private static final int TEST_REPLICA_UNHEALTHY_GRACE_MS = 2_000;

    private MiniCluster cluster;
    private StrataClient client;

    @BeforeEach
    void setup() throws Exception {
        cluster = new MiniCluster(4, null, 1,
                (zk, idx) -> ControllerConfig.forTests(zk)
                        .withReplicaMissingGraceMs(TEST_REPLICA_UNHEALTHY_GRACE_MS));
        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(4096)
                .withDataNodeConnectionsPerEndpoint(3));
    }

    @AfterEach
    void teardown() throws Exception {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void nodeDeathRepairsAllChunksBackToRf3() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/repair-me")).id();
        Workload workload = new Workload();
        StrataFile.SealInfo sealed;
        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 1200); // several chunks across 4 nodes
            sealed = appender.seal();
        }

        // kill the node holding the most chunks
        var lookup = lookupFile(fileId);
        int victim = lookup.chunks().get(0).replicas().get(0).nodeId();
        int victimIndex = -1;
        for (int i = 0; i < cluster.nodes.size(); i++) {
            if (cluster.nodes.get(i).nodeId() == victim) victimIndex = i;
        }
        cluster.killNode(victimIndex);

        // repair: lease expiry -> DEAD -> REPLICATE -> pull -> descriptor swap. Poll until RF=3
        // with the victim absent everywhere.
        long deadline = System.currentTimeMillis() + 60_000;
        boolean repaired = false;
        while (System.currentTimeMillis() < deadline && !repaired) {
            Thread.sleep(250);
            var current = lookupFile(fileId);
            repaired = true;
            for (var c : current.chunks()) {
                Set<Integer> ids = new HashSet<>();
                for (var r : c.replicas()) ids.add(r.nodeId());
                if (ids.contains(victim) || ids.size() != 3) {
                    repaired = false;
                    break;
                }
            }
        }
        assertTrue(repaired, "repair did not restore RF=3 without the dead node in time");

        // all data still reads back intact
        workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
    }

    @Test
    void orphanChunkIsReconciledAway() throws Exception {
        // plant an orphan: a sealed chunk on a node that metadata knows nothing about
        DataNode node = cluster.nodes.get(0);
        ChunkId orphan = new ChunkId(FileId.of(1), 0);
        try (ScpClient direct = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_TOOL, "planter")) {
            direct.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(orphan, 1, false, 1 << 20, System.currentTimeMillis(), TEST_NS).encode(), null, 5000);
            direct.call(Opcode.APPEND, new Messages.Append(orphan, 1, 0, 0, TEST_NS).encode(),
                    ByteBuffer.wrap("orphan-bytes".getBytes()), 5000);
            direct.call(Opcode.SEAL_CHUNK, new Messages.SealChunk(orphan, 1, 12, TEST_NS).encode(), null, 5000);
        }
        assertTrue(node.store().contains(TEST_NS, orphan));

        // node-local orphan GC (grace -> owner-confirm via LOOKUP_FILE -> delete) reaps the unreferenced chunk
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline && node.store().contains(TEST_NS, orphan)) {
            Thread.sleep(250);
        }
        assertFalse(node.store().contains(TEST_NS, orphan), "orphan chunk was not reconciled away");
    }

    @Test
    void liveNodeMissingSealedReplicaIsDroppedAndRepaired() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/missing-live-replica")).id();
        Workload workload = new Workload();
        StrataFile.SealInfo sealed;
        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 300);
            sealed = appender.seal();
        }
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());

        var lookup = lookupFile(fileId);
        Messages.ChunkInfo chunk = lookup.chunks().get(0);
        ChunkId chunkId = chunk.chunkId();
        DataNode victim = nodeById(chunk.replicas().get(0).nodeId());
        assertEquals(ErrorCode.OK, victim.store().delete(TEST_NS, chunkId));
        assertFalse(victim.store().contains(TEST_NS, chunkId));

        // allow for the inventory-loss grace (a sealed replica is only dropped once it has stayed
        // missing from inventory past ControllerConfig.replicaMissingGraceMs()) plus the re-repair
        long deadline = System.currentTimeMillis() + 140_000;
        boolean repaired = false;
        while (System.currentTimeMillis() < deadline && !repaired) {
            Thread.sleep(250);
            var current = lookupFile(fileId);
            for (Messages.ChunkInfo currentChunk : current.chunks()) {
                if (!currentChunk.chunkId().equals(chunkId)) {
                    continue;
                }
                repaired = currentChunk.replicas().size() == 3
                        && currentChunk.replicas().stream()
                        .allMatch(replica -> nodeById(replica.nodeId()).store().contains(TEST_NS, chunkId));
                break;
            }
        }
        assertTrue(repaired, "missing sealed replica was not dropped and repaired in time");

        workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
    }

    @Test
    void scrubbedCorruptSealedReplicaIsDroppedAndRepaired() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/corrupt-live-replica")).id();
        Workload workload = new Workload();
        StrataFile.SealInfo sealed;
        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 300);
            sealed = appender.seal();
        }
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());

        Messages.ChunkInfo chunk = lookupFile(fileId).chunks().get(0);
        assertTrue(chunk.length() > 0, "test setup requires a non-empty sealed chunk");
        ChunkId chunkId = chunk.chunkId();
        DataNode victim = nodeById(chunk.replicas().get(0).nodeId());
        corruptDataByte(victim, chunkId, Math.min(3, chunk.length() - 1));

        assertEquals(ErrorCode.CRC_MISMATCH,
                assertThrows(ScpException.class, () -> victim.store().read(TEST_NS, chunkId, 0, 1)).code(),
                "corrupt sealed replica must not serve bytes before scrub reports it");
        assertEquals(1, victim.store().scrubOnce(), "scrub must expose the CRC mismatch through inventory");

        long deadline = System.currentTimeMillis() + 60_000;
        boolean repaired = false;
        while (System.currentTimeMillis() < deadline && !repaired) {
            Thread.sleep(250);
            var current = lookupFile(fileId);
            for (Messages.ChunkInfo currentChunk : current.chunks()) {
                if (!currentChunk.chunkId().equals(chunkId)) {
                    continue;
                }
                repaired = currentChunk.replicas().size() == 3
                        && currentChunk.replicas().stream().allMatch(replica -> {
                            try {
                                Messages.StatResp stat = ConsistencyVerifier.statReplica(replica, currentChunk);
                                return stat.state() == currentChunk.state()
                                        && stat.sealedLength() == currentChunk.length()
                                        && stat.sealedCrc() == currentChunk.crc();
                            } catch (Exception | AssertionError e) {
                                return false;
                            }
                        });
                break;
            }
        }
        assertTrue(repaired, "corrupt sealed replica was not dropped and repaired in time");

        workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
    }

    @Test
    void fileDeletionConvergesOnNodes() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/delete-me")).id();
        Workload workload = new Workload();
        StrataFile.SealInfo sealed;
        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 600);
            sealed = appender.seal();
        }
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
        var lookup = lookupFile(fileId);
        List<ChunkId> chunkIds = lookup.chunks().stream().map(Messages.ChunkInfo::chunkId).toList();
        assertTrue(chunkIds.size() >= 2);

        client.deleteById(StrataNamespace.of("test"), fileId);

        // metadata record disappears once all replicas confirm physical deletion
        waitForFileDeleted(fileId);

        // and no node still holds any of its chunks
        assertNoNodeContains(chunkIds);
    }

    @Test
    void fileDeletionConvergesWhenReplicaWasAlreadyMissingLocally() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/delete-missing-local")).id();
        Workload workload = new Workload();
        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 300);
            appender.seal();
        }

        var lookup = lookupFile(fileId);
        List<ChunkId> chunkIds = lookup.chunks().stream().map(Messages.ChunkInfo::chunkId).toList();
        Messages.ChunkInfo chunk = lookup.chunks().get(0);
        DataNode victim = nodeById(chunk.replicas().get(0).nodeId());
        assertEquals(ErrorCode.OK, victim.store().delete(TEST_NS, chunk.chunkId()));
        assertFalse(victim.store().contains(TEST_NS, chunk.chunkId()));

        client.deleteById(StrataNamespace.of("test"), fileId);

        waitForFileDeleted(fileId);
        assertNoNodeContains(chunkIds);
    }

    @Test
    void deletingOpenFileCannotBeResurrectedByStaleAppender() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/delete-open-writer")).id();
        Workload workload = new Workload();
        StrataFile.Appender stale = client.openById(StrataNamespace.of("test"), fileId).openForAppend();
        try {
            workload.appendAcked(stale, 0, 80);
            List<ChunkId> chunkIds = lookupFile(fileId).chunks().stream()
                    .map(Messages.ChunkInfo::chunkId)
                    .toList();
            assertTrue(!chunkIds.isEmpty(), "test setup requires an open metadata chunk");

            client.deleteById(StrataNamespace.of("test"), fileId);

            ScpException sealFailure = assertThrows(ScpException.class, stale::seal,
                    "stale appender must not seal or resurrect a DELETING file");
            assertTrue(sealFailure.code() == ErrorCode.PRECONDITION_FAILED
                            || sealFailure.code() == ErrorCode.CHUNK_NOT_FOUND
                            || sealFailure.code() == ErrorCode.INTERNAL,
                    "unexpected stale seal failure code: " + sealFailure.code());

            waitForFileDeleted(fileId);
            assertNoNodeContains(chunkIds);
        } finally {
            stale.close();
        }
    }

    private Messages.LookupFileResp lookupFile(FileId fileId) throws Exception {
        String[] hp = cluster.metaEndpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
            ByteBuffer h = direct.call(Opcode.LOOKUP_FILE, new Messages.LookupFile(StrataNamespace.of("test"), fileId).encode(), null, 5000);
            return Messages.LookupFileResp.decode(h);
        }
    }


    private DataNode nodeById(int nodeId) {
        for (DataNode node : cluster.nodes) {
            if (node.nodeId() == nodeId) {
                return node;
            }
        }
        throw new AssertionError("node " + nodeId + " not found");
    }

    private static void corruptDataByte(DataNode node, ChunkId chunkId, long dataOffset) throws Exception {
        var chunkPath = node.config().dataDir().resolve("chunks")
                .resolve(ChunkFormats.chunkRelativePath(TEST_NS, chunkId) + ".chunk");
        long position = ChunkFormats.DATA_START + dataOffset;
        try (FileChannel channel = FileChannel.open(chunkPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            assertEquals(1, channel.read(one, position));
            one.flip();
            byte current = one.get();
            one.clear();
            one.put((byte) (current ^ 0x7F));
            one.flip();
            assertEquals(1, channel.write(one, position));
            channel.force(false);
        }
    }

    private void waitForFileDeleted(FileId fileId) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(250);
            try {
                lookupFile(fileId);
            } catch (ScpException e) {
                assertEquals(ErrorCode.FILE_NOT_FOUND, e.code());
                return;
            }
        }
        throw new AssertionError("file record did not converge to deleted");
    }

    private void assertNoNodeContains(List<ChunkId> chunkIds) {
        for (DataNode node : cluster.nodes) {
            for (ChunkId id : chunkIds) {
                assertFalse(node.store().contains(TEST_NS, id),
                        "node " + node.nodeId() + " still holds deleted chunk " + id);
            }
        }
    }
}
