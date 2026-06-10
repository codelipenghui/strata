package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.SegmentStore;
import io.strata.client.StrataClient;
import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.node.StorageNode;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Repair, reconciliation, and retention end-to-end (tech design §7.2, §9): a dead node's sealed
 * chunks are re-replicated by the pool (RF restored, byte-identical), orphans are reconciled
 * away, and file deletion physically converges on the nodes.
 */
class RepairAndRetentionTest {

    private MiniCluster cluster;
    private StrataClient client;

    @BeforeEach
    void setup() throws Exception {
        cluster = new MiniCluster(4);
        client = new StrataClient(ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(4096));
    }

    @AfterEach
    void teardown() throws Exception {
        client.close();
        cluster.close();
    }

    @Test
    void nodeDeathRepairsAllChunksBackToRf3() throws Exception {
        FileId fileId = client.create(SegmentStore.FileSpec.log("repair-me"));
        Workload workload = new Workload();
        try (SegmentStore.Appender appender = client.openForAppend(fileId, 1)) {
            workload.appendAcked(appender, 0, 1200); // several chunks across 4 nodes
            appender.seal();
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
        workload.verifyAckedPrefix(client, fileId);

        // and repaired replicas are byte-identical to the survivors (invariant §14.6)
        var current = lookupFile(fileId);
        for (var c : current.chunks()) {
            Set<String> hashes = new HashSet<>();
            for (var r : c.replicas()) {
                hashes.add(sha256OfChunkFile(r.endpoint(), c.chunkId()));
            }
            assertEquals(1, hashes.size(), "replica divergence after repair on " + c.chunkId());
        }
    }

    @Test
    void orphanChunkIsReconciledAway() throws Exception {
        // plant an orphan: a sealed chunk on a node that metadata knows nothing about
        StorageNode node = cluster.nodes.get(0);
        ChunkId orphan = new ChunkId(FileId.random(), 0);
        try (ScpClient direct = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_TOOL, "planter")) {
            direct.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(orphan, 1, (byte) 0, (byte) 0, (byte) 0,
                    1 << 20, System.currentTimeMillis()).encode(), null, 5000);
            direct.call(Opcode.APPEND, new Messages.Append(orphan, 1, 0, 0).encode(),
                    ByteBuffer.wrap("orphan-bytes".getBytes()), 5000);
            direct.call(Opcode.SEAL_CHUNK, new Messages.SealChunk(orphan, 1, 12).encode(), null, 5000);
        }
        assertTrue(node.store().contains(orphan));

        // inventory report (5s interval) -> coordinator spots the orphan -> DELETE command
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline && node.store().contains(orphan)) {
            Thread.sleep(250);
        }
        assertFalse(node.store().contains(orphan), "orphan chunk was not reconciled away");
    }

    @Test
    void fileDeletionConvergesOnNodes() throws Exception {
        FileId fileId = client.create(SegmentStore.FileSpec.log("delete-me"));
        Workload workload = new Workload();
        try (SegmentStore.Appender appender = client.openForAppend(fileId, 1)) {
            workload.appendAcked(appender, 0, 600);
            appender.seal();
        }
        var lookup = lookupFile(fileId);
        List<ChunkId> chunkIds = lookup.chunks().stream().map(Messages.ChunkInfo::chunkId).toList();
        assertTrue(chunkIds.size() >= 2);

        client.delete(List.of(fileId));

        // metadata record disappears once all replicas confirm physical deletion
        long deadline = System.currentTimeMillis() + 30_000;
        boolean gone = false;
        while (System.currentTimeMillis() < deadline && !gone) {
            Thread.sleep(250);
            try {
                lookupFile(fileId);
            } catch (ScpException e) {
                assertEquals(ErrorCode.FILE_NOT_FOUND, e.code());
                gone = true;
            }
        }
        assertTrue(gone, "file record did not converge to deleted");

        // and no node still holds any of its chunks
        for (StorageNode node : cluster.nodes) {
            for (ChunkId id : chunkIds) {
                assertFalse(node.store().contains(id),
                        "node " + node.nodeId() + " still holds deleted chunk " + id);
            }
        }
    }

    private String sha256OfChunkFile(String endpoint, ChunkId chunkId) throws Exception {
        String[] hp = endpoint.split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "verify")) {
            var frame = direct.callFrame(Opcode.FETCH_CHUNK,
                    new Messages.FetchChunk(chunkId, 0, Integer.MAX_VALUE).encode(), null, 10_000);
            ByteBuffer h = frame.headerSlice();
            io.strata.proto.Resp.check(h);
            byte[] bytes = new byte[frame.payloadLength()];
            frame.payloadSlice().get(bytes);
            return java.util.HexFormat.of().formatHex(
                    java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
        }
    }

    private Messages.LookupFileResp lookupFile(FileId fileId) throws Exception {
        String[] hp = cluster.metaEndpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
            ByteBuffer h = direct.call(Opcode.LOOKUP_FILE, new Messages.LookupFile(fileId).encode(), null, 5000);
            return Messages.LookupFileResp.decode(h);
        }
    }
}
