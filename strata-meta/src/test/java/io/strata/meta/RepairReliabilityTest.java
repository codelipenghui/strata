package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Repair-path reliability: a corrupt sealed replica must lose its descriptor entry on inventory
 * (so it stops being a read/repair source), and a repair whose target dies before completing must
 * be re-issued — never suppressed forever by the in-flight marker.
 */
class RepairReliabilityTest {

    private TestingServer zk;
    private MetadataService service;
    private ScpClient client;

    /** Minimal fake storage node: registers, heartbeats, auto-acks received commands. */
    private final class FakeNode {
        final UUID inc = UUID.randomUUID();
        final String host;
        int nodeId = -1;
        long session = -1;
        boolean alive = true;
        final List<Messages.Command> received = new ArrayList<>();
        private final List<Messages.CompletedCommand> toReport = new ArrayList<>();

        FakeNode(String host) {
            this.host = host;
        }

        void register() {
            var resp = Messages.RegisterResp.decode(client.call(Opcode.REGISTER_NODE,
                    new Messages.RegisterNode(inc.getMostSignificantBits(), inc.getLeastSignificantBits(),
                            List.of(host + ":9000"), "z1", "r1", host,
                            List.of(new Messages.MediaCapacity((byte) 0, 1L << 40)), 1, 0).encode(),
                    null, 5000));
            nodeId = resp.nodeId();
            session = resp.sessionEpoch();
        }

        void heartbeat() {
            if (!alive) return;
            List<Messages.CompletedCommand> done = new ArrayList<>(toReport);
            toReport.clear();
            var resp = Messages.HeartbeatResp.decode(client.call(Opcode.NODE_HEARTBEAT,
                    new Messages.NodeHeartbeat(nodeId, inc.getMostSignificantBits(),
                            inc.getLeastSignificantBits(), session,
                            List.of(new Messages.MediaUsage((byte) 0, 0, 1L << 40)), 0, done).encode(),
                    null, 5000));
            received.addAll(resp.commands());
            for (Messages.Command c : resp.commands()) {
                toReport.add(new Messages.CompletedCommand(c.commandId(), (short) 0));
            }
        }

        boolean receivedReplicateFor(ChunkId chunkId) {
            return received.stream().anyMatch(c -> c instanceof Messages.ReplicateCmd r
                    && r.chunkId().equals(chunkId));
        }
    }

    @BeforeEach
    void setup() throws Exception {
        zk = new TestingServer(true);
        service = new MetadataService(MetaConfig.forTests(zk.getConnectString()));
        long deadline = System.currentTimeMillis() + 10_000;
        while (!service.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(service.isLeader());
        client = new ScpClient("127.0.0.1", service.port(), ScpClient.KIND_TOOL, "test");
    }

    @AfterEach
    void teardown() throws Exception {
        client.close();
        service.close();
        zk.close();
    }

    private Messages.LookupFileResp lookup(FileId fileId) {
        return Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(fileId).encode(), null, 5000));
    }

    @Test
    void corruptSealedReplicaIsDroppedOnInventoryMismatch() {
        List<FakeNode> nodes = new ArrayList<>();
        for (String h : List.of("invA", "invB", "invC", "invD")) {
            FakeNode n = new FakeNode(h);
            n.register();
            nodes.add(n);
        }
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile((byte) 0, (byte) 0, (byte) 0, "t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1, (byte) 0).encode(), null, 5000));
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xC0FFEE, List.of()).encode(), null, 5000);

        int replicaNode = chunk.replicas().get(0).nodeId();

        // a CORRECT inventory report changes nothing
        client.call(Opcode.INVENTORY_REPORT, new Messages.InventoryReport(replicaNode, 0, 1,
                List.of(new Messages.InventoryEntry(chunk.chunkId(), ChunkState.SEALED, 100, 0xC0FFEE)))
                .encode(), null, 5000);
        assertEquals(3, lookup(file.fileId()).chunks().get(0).replicas().size());

        // a report with the right id but the WRONG crc means corrupt bytes: the replica must be
        // dropped from the descriptor so it stops being a read/repair source
        client.call(Opcode.INVENTORY_REPORT, new Messages.InventoryReport(replicaNode, 0, 1,
                List.of(new Messages.InventoryEntry(chunk.chunkId(), ChunkState.SEALED, 100, 0xBAD)))
                .encode(), null, 5000);
        var replicas = lookup(file.fileId()).chunks().get(0).replicas();
        assertEquals(2, replicas.size(), "corrupt replica must be dropped from the descriptor");
        for (var r : replicas) {
            assertTrue(r.nodeId() != replicaNode, "the corrupt node must be the one dropped");
        }

        // CRITICAL follow-through: the chunk is now at RF=2 with NO dead node in its replica
        // list — repair must be able to ADD a replica (not just swap a dead one), otherwise the
        // chunk is stranded under-replicated forever
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            for (FakeNode n : nodes) n.heartbeat();
            try {
                service.reconcileNow();
            } catch (Exception ignored) {
            }
            if (lookup(file.fileId()).chunks().get(0).replicas().size() == 3) break;
            try {
                Thread.sleep(150);
            } catch (InterruptedException ignored) {
            }
        }
        var restored = lookup(file.fileId()).chunks().get(0).replicas();
        assertEquals(3, restored.size(), "repair must restore RF=3 after a corrupt-replica drop");

        // the corrupt node MAY be re-picked as the add-target — but only after its corrupt copy
        // was scheduled for physical deletion FIRST (FIFO command delivery: delete precedes the
        // re-replicate, so the node re-pulls a clean copy instead of trusting stale bytes)
        boolean corruptNodeReturned = restored.stream().anyMatch(r -> r.nodeId() == replicaNode);
        if (corruptNodeReturned) {
            FakeNode corrupt = nodes.stream().filter(n -> n.nodeId == replicaNode).findFirst().orElseThrow();
            int deleteIdx = -1, replicateIdx = -1;
            for (int i = 0; i < corrupt.received.size(); i++) {
                var c = corrupt.received.get(i);
                if (c instanceof Messages.DeleteCmd d && d.chunkIds().contains(chunk.chunkId())
                        && deleteIdx < 0) {
                    deleteIdx = i;
                }
                if (c instanceof Messages.ReplicateCmd r && r.chunkId().equals(chunk.chunkId())
                        && replicateIdx < 0) {
                    replicateIdx = i;
                }
            }
            assertTrue(deleteIdx >= 0, "corrupt node must receive a DELETE for its bad copy");
            assertTrue(replicateIdx < 0 || deleteIdx < replicateIdx,
                    "DELETE must precede any re-REPLICATE to the corrupt node");
        }
    }

    @Test
    void losingAllReplicasOfALiveFileKeepsTheChunkRecordAsHardFailure() {
        List<FakeNode> nodes = new ArrayList<>();
        for (String h : List.of("lossA", "lossB", "lossC")) {
            FakeNode n = new FakeNode(h);
            n.register();
            nodes.add(n);
        }
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile((byte) 0, (byte) 0, (byte) 0, "t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1, (byte) 0).encode(), null, 5000));
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xC, List.of()).encode(), null, 5000);

        // catastrophic: every replica reports a corrupt copy — all three get dropped
        for (var replica : chunk.replicas()) {
            client.call(Opcode.INVENTORY_REPORT, new Messages.InventoryReport(replica.nodeId(), 0, 1,
                    List.of(new Messages.InventoryEntry(chunk.chunkId(), ChunkState.SEALED, 100, 0xBAD)))
                    .encode(), null, 5000);
        }

        // the chunk record must SURVIVE with zero replicas: erasing it would silently shorten a
        // live file (readers' offset accounting shifts) — data loss must be a hard read failure
        var lookup = lookup(file.fileId());
        assertEquals(1, lookup.chunks().size(),
                "chunk record of a live file must never be erased by reconciliation");
        assertEquals(0, lookup.chunks().get(0).replicas().size(),
                "all replicas lost -> empty replica list, hard failure for readers");
    }

    @Test
    void sealCommitsOnlyTheActuallySealedReplicasAndRepairRestoresRf() throws Exception {
        List<FakeNode> nodes = new ArrayList<>();
        for (String h : List.of("subA", "subB", "subC", "subD")) {
            FakeNode n = new FakeNode(h);
            n.register();
            nodes.add(n);
        }
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile((byte) 0, (byte) 0, (byte) 0, "t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1, (byte) 0).encode(), null, 5000));

        // the writer sealed on a quorum of 2 — the third replica failed mid-chunk and holds a
        // short OPEN copy; committing it into the SEALED descriptor would let it serve reads
        List<Integer> sealedSubset = List.of(chunk.replicas().get(0).nodeId(),
                chunk.replicas().get(1).nodeId());
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xC, sealedSubset).encode(), null, 5000);

        var sealedLookup = lookup(file.fileId()).chunks().get(0);
        assertEquals(2, sealedLookup.replicas().size(),
                "descriptor must contain only replicas that actually sealed");
        for (var r : sealedLookup.replicas()) {
            assertTrue(sealedSubset.contains(r.nodeId()));
        }

        // and the under-replication scan must then restore RF=3 via add-mode repair
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            for (FakeNode n : nodes) n.heartbeat();
            try {
                service.reconcileNow();
            } catch (Exception ignored) {
            }
            if (lookup(file.fileId()).chunks().get(0).replicas().size() == 3) break;
            Thread.sleep(150);
        }
        assertEquals(3, lookup(file.fileId()).chunks().get(0).replicas().size(),
                "add-mode repair must restore RF after a subset seal");
    }

    @Test
    void openReplicaOfSealedChunkIsDroppedOnInventory() {
        List<FakeNode> nodes = new ArrayList<>();
        for (String h : List.of("strA", "strB", "strC", "strD")) {
            FakeNode n = new FakeNode(h);
            n.register();
            nodes.add(n);
        }
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile((byte) 0, (byte) 0, (byte) 0, "t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1, (byte) 0).encode(), null, 5000));
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xC, List.of()).encode(), null, 5000);

        // a replica still OPEN under a SEALED descriptor never converges by itself: nothing
        // re-seals it, FETCH from it fails, and reads to it return short — it must be dropped
        int straggler = chunk.replicas().get(0).nodeId();
        client.call(Opcode.INVENTORY_REPORT, new Messages.InventoryReport(straggler, 0, 1,
                List.of(new Messages.InventoryEntry(chunk.chunkId(), ChunkState.OPEN, 40, 0)))
                .encode(), null, 5000);

        var replicas = lookup(file.fileId()).chunks().get(0).replicas();
        assertEquals(2, replicas.size(), "OPEN straggler under a SEALED descriptor must be dropped");
        for (var r : replicas) {
            assertTrue(r.nodeId() != straggler);
        }
    }

    @Test
    void repairIsReissuedWhenTargetDiesBeforeCompleting() throws Exception {
        List<FakeNode> nodes = new ArrayList<>();
        for (String h : List.of("rrA", "rrB", "rrC", "rrD", "rrE")) {
            FakeNode n = new FakeNode(h);
            n.register();
            nodes.add(n);
        }
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile((byte) 0, (byte) 0, (byte) 0, "t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1, (byte) 0).encode(), null, 5000));
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xD, List.of()).encode(), null, 5000);

        Set<Integer> replicaIds = new HashSet<>();
        for (var r : chunk.replicas()) replicaIds.add(r.nodeId());
        List<FakeNode> spares = nodes.stream().filter(n -> !replicaIds.contains(n.nodeId)).toList();
        assertEquals(2, spares.size());

        // kill one replica holder; pump heartbeats for everyone else past lease+grace
        FakeNode victim = nodes.stream().filter(n -> n.nodeId == chunk.replicas().get(0).nodeId())
                .findFirst().orElseThrow();
        victim.alive = false;
        pumpUntilReplicateDelivered(nodes, null, 8_000);

        FakeNode firstTarget = spares.stream()
                .filter(s -> s.receivedReplicateFor(chunk.chunkId())).findFirst().orElse(null);
        assertTrue(firstTarget != null, "repair was never issued to a spare");
        FakeNode otherSpare = spares.get(0) == firstTarget ? spares.get(1) : spares.get(0);

        // the target dies holding the command — it will never report completion
        firstTarget.alive = false;

        // the repair must be re-issued to the remaining spare, not suppressed forever
        pumpUntilReplicateDelivered(nodes, otherSpare, 15_000);
        assertTrue(otherSpare.receivedReplicateFor(chunk.chunkId()),
                "repair stuck: marker never released after target death");
    }

    /** Heartbeats alive nodes + reconciles until `until` has a REPLICATE (or just runs the window). */
    private void pumpUntilReplicateDelivered(List<FakeNode> nodes, FakeNode until, long windowMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + windowMs;
        while (System.currentTimeMillis() < deadline) {
            for (FakeNode n : nodes) n.heartbeat();
            service.reconcileNow();
            if (until != null && until.received.stream().anyMatch(c -> c instanceof Messages.ReplicateCmd)) {
                return;
            }
            if (until == null && nodes.stream().anyMatch(
                    n -> n.received.stream().anyMatch(c -> c instanceof Messages.ReplicateCmd))) {
                return;
            }
            Thread.sleep(200);
        }
    }
}
