package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
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
    private static final StrataNamespace TEST_NS = StrataNamespace.of("test");

    private TestingServer zk;
    private Controller service;
    private ScpClient client;

    /** Minimal fake data node: registers, heartbeats, auto-acks received commands. */
    private final class FakeNode {
        private static final java.util.concurrent.atomic.AtomicInteger ID_SEQ =
                new java.util.concurrent.atomic.AtomicInteger(1);
        final UUID inc = UUID.randomUUID();
        final int assignedNodeId = ID_SEQ.getAndIncrement();
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
                    new Messages.RegisterNode(assignedNodeId, inc.getMostSignificantBits(),
                            inc.getLeastSignificantBits(),
                            List.of(host + ":9000"), "z1", "r1", host,
                            List.of(new Messages.StorageCapacity(1L << 40)), 1, 0).encode(),
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
                            List.of(new Messages.StorageUsage(0, 1L << 40)), 0, done).encode(),
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
        Exception failure = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                zk = new TestingServer(true);
                // grace 0: exercise prompt missing-replica drop -> repair
                service = new Controller(
                        ControllerConfig.forTests(zk.getConnectString()).withReplicaMissingGraceMs(0));
                long deadline = System.currentTimeMillis() + 10_000;
                while (!service.isLeader() && System.currentTimeMillis() < deadline) {
                    Thread.sleep(20);
                }
                assertTrue(service.isLeader());
                client = connectClient();
                return;
            } catch (Exception e) {
                failure = e;
                teardown();
                Thread.sleep(100);
            }
        }
        throw failure;
    }

    @AfterEach
    void teardown() throws Exception {
        Exception failure = null;
        failure = closeIfPresent(client, failure);
        client = null;
        failure = closeIfPresent(service, failure);
        service = null;
        failure = closeIfPresent(zk, failure);
        zk = null;
        if (failure != null) {
            throw failure;
        }
    }

    private ScpClient connectClient() throws Exception {
        Exception failure = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try {
                return new ScpClient("127.0.0.1", service.port(), ScpClient.KIND_TOOL, "test");
            } catch (Exception e) {
                failure = e;
                Thread.sleep(50);
            }
        }
        throw failure;
    }

    private static Exception closeIfPresent(AutoCloseable closeable, Exception previous) {
        if (closeable == null) {
            return previous;
        }
        try {
            closeable.close();
            return previous;
        } catch (Exception e) {
            if (previous != null) {
                previous.addSuppressed(e);
                return previous;
            }
            return e;
        }
    }

    private Messages.LookupFileResp lookup(FileId fileId) {
        return Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(StrataNamespace.of("test"), fileId).encode(), null, 5000));
    }

    @Test
    void sealCommitsOnlyTheActuallySealedReplicasAndRepairRestoresRf() throws Exception {
        List<FakeNode> nodes = new ArrayList<>();
        for (String h : List.of("subA", "subB", "subC", "subD")) {
            FakeNode n = new FakeNode(h);
            n.register();
            n.heartbeat();
            nodes.add(n);
        }
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(StrataNamespace.of("test"), file.fileId(), 1).encode(), null, 5000));

        // the writer sealed on a quorum of 2 — the third replica failed mid-chunk and holds a
        // short OPEN copy; committing it into the SEALED descriptor would let it serve reads
        List<Integer> sealedSubset = List.of(chunk.replicas().get(0).nodeId(),
                chunk.replicas().get(1).nodeId());
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(StrataNamespace.of("test"), chunk.chunkId(), 1, 100, 0xC, sealedSubset).encode(), null, 5000);

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
    void repairIsReissuedWhenTargetDiesBeforeCompleting() throws Exception {
        List<FakeNode> nodes = new ArrayList<>();
        for (String h : List.of("rrA", "rrB", "rrC", "rrD", "rrE")) {
            FakeNode n = new FakeNode(h);
            n.register();
            n.heartbeat();
            nodes.add(n);
        }
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(StrataNamespace.of("test"), file.fileId(), 1).encode(), null, 5000));
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(StrataNamespace.of("test"), chunk.chunkId(), 1, 100, 0xD, List.of()).encode(), null, 5000);

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

    @Test
    void deleteScanDoesNotDuplicateInflightDeletes() throws Exception {
        List<FakeNode> nodes = new ArrayList<>();
        for (String h : List.of("dupDelA", "dupDelB", "dupDelC")) {
            FakeNode n = new FakeNode(h);
            n.register();
            n.heartbeat();
            nodes.add(n);
        }
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(StrataNamespace.of("test"), file.fileId(), 1).encode(), null, 5000));
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(StrataNamespace.of("test"), chunk.chunkId(), 1, 100, 0xD1E7, List.of()).encode(), null, 5000);

        var delResp = Messages.DeleteFilesResp.decode(client.call(Opcode.DELETE_FILES,
                new Messages.DeleteFiles(StrataNamespace.of("test"), List.of(file.fileId())).encode(), null, 5000));
        assertEquals((short) 0, delResp.codes().get(0).shortValue());

        service.reconcileNow();
        service.reconcileNow();
        for (FakeNode n : nodes) n.heartbeat();

        for (var replica : chunk.replicas()) {
            FakeNode holder = nodes.stream().filter(n -> n.nodeId == replica.nodeId()).findFirst().orElseThrow();
            long deletes = holder.received.stream()
                    .filter(c -> c instanceof Messages.DeleteCmd d && d.chunkIds().contains(chunk.chunkId()))
                    .count();
            assertEquals(1, deletes, "in-flight delete must suppress duplicate commands per replica");
        }
    }

    @Test
    void deletingFileWithDeadReplicaDropsReferenceAndConverges() throws Exception {
        List<FakeNode> nodes = new ArrayList<>();
        for (String h : List.of("deadDelA", "deadDelB", "deadDelC")) {
            FakeNode n = new FakeNode(h);
            n.register();
            n.heartbeat();
            nodes.add(n);
        }
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(StrataNamespace.of("test"), file.fileId(), 1).encode(), null, 5000));
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(StrataNamespace.of("test"), chunk.chunkId(), 1, 100, 0xD1E7, List.of()).encode(), null, 5000);

        FakeNode deadReplica = nodes.stream()
                .filter(n -> n.nodeId == chunk.replicas().get(0).nodeId())
                .findFirst()
                .orElseThrow();
        deadReplica.alive = false;

        var delResp = Messages.DeleteFilesResp.decode(client.call(Opcode.DELETE_FILES,
                new Messages.DeleteFiles(StrataNamespace.of("test"), List.of(file.fileId())).encode(), null, 5000));
        assertEquals((short) 0, delResp.codes().get(0).shortValue());

        long deadline = System.currentTimeMillis() + 8_000;
        while (System.currentTimeMillis() < deadline) {
            for (FakeNode n : nodes) n.heartbeat();
            service.reconcileNow();
            for (FakeNode n : nodes) n.heartbeat();
            try {
                lookup(file.fileId());
            } catch (ScpException e) {
                assertEquals(ErrorCode.FILE_NOT_FOUND, e.code());
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("deleting file with a dead replica did not converge to FILE_NOT_FOUND");
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

    private int countReplicateCommands(List<FakeNode> nodes, ChunkId chunkId) {
        int count = 0;
        for (FakeNode node : nodes) {
            for (Messages.Command command : node.received) {
                if (command instanceof Messages.ReplicateCmd r && r.chunkId().equals(chunkId)) {
                    count++;
                }
            }
        }
        return count;
    }
}
