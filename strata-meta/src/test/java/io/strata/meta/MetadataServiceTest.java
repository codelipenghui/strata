package io.strata.meta;

import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetadataServiceTest {

    private TestingServer zk;
    private MetadataService service;
    private ScpClient client;

    /** A fake storage node: registers and heartbeats but executes nothing for real. */
    private class FakeNode {
        final UUID inc = UUID.randomUUID();
        final String host;
        int nodeId = -1;
        long session = -1;
        final List<Messages.Command> received = new ArrayList<>();
        final List<Messages.CompletedCommand> toReport = new ArrayList<>();

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

        Messages.HeartbeatResp heartbeat() {
            List<Messages.CompletedCommand> done = new ArrayList<>(toReport);
            toReport.clear();
            var resp = Messages.HeartbeatResp.decode(client.call(Opcode.NODE_HEARTBEAT,
                    new Messages.NodeHeartbeat(nodeId, inc.getMostSignificantBits(),
                            inc.getLeastSignificantBits(), session,
                            List.of(new Messages.MediaUsage((byte) 0, 0, 1L << 40)), 0, done).encode(),
                    null, 5000));
            received.addAll(resp.commands());
            for (Messages.Command c : resp.commands()) {
                toReport.add(new Messages.CompletedCommand(c.commandId(), (short) 0)); // pretend success
            }
            return resp;
        }
    }

    @BeforeAll
    void setup() throws Exception {
        zk = new TestingServer(true);
        service = new MetadataService(MetaConfig.forTests(zk.getConnectString()));
        long deadline = System.currentTimeMillis() + 10_000;
        while (!service.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(service.isLeader(), "service must acquire leadership");
        client = new ScpClient("127.0.0.1", service.port(), ScpClient.KIND_TOOL, "test");
    }

    @AfterAll
    void teardown() throws Exception {
        client.close();
        service.close();
        zk.close();
    }

    @Test
    void fileChunkLifecycleWithPlacement() throws Exception {
        FakeNode n1 = new FakeNode("hostA");
        FakeNode n2 = new FakeNode("hostB");
        FakeNode n3 = new FakeNode("hostC");
        n1.register();
        n2.register();
        n3.register();
        assertEquals(3, Set.of(n1.nodeId, n2.nodeId, n3.nodeId).size());

        // create file
        var fileResp = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile((byte) 0, (byte) 0, (byte) 0, "topicA-0").encode(), null, 5000));
        FileId fileId = fileResp.fileId();

        // create chunk: 3 replicas, all distinct nodes and hosts
        var chunkResp = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(fileId, 1, (byte) 0xFF).encode(), null, 5000));
        assertEquals(0, chunkResp.chunkId().index());
        assertEquals(3, chunkResp.replicas().size());
        Set<Integer> ids = new HashSet<>();
        for (var r : chunkResp.replicas()) {
            ids.add(r.nodeId());
            assertTrue(r.endpoint().endsWith(":9000"));
        }
        assertEquals(3, ids.size());

        // lower-epoch create rejected after epoch-2 chunk exists (stale leader guard)
        var chunk2 = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(fileId, 2, (byte) 0xFF).encode(), null, 5000));
        assertEquals(1, chunk2.chunkId().index());
        ScpException stale = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(fileId, 1, (byte) 0xFF).encode(), null, 5000));
        assertEquals(ErrorCode.FENCED_EPOCH, stale.code());

        // seal chunk 0 and look it up
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunkResp.chunkId(), 1, 4096, 0xAB).encode(), null, 5000);
        var lookup = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(fileId).encode(), null, 5000));
        assertEquals(2, lookup.chunks().size());
        assertEquals(ChunkState.SEALED, lookup.chunks().get(0).state());
        assertEquals(4096, lookup.chunks().get(0).length());
        assertEquals(ChunkState.OPEN, lookup.chunks().get(1).state());

        // idempotent seal ok; conflicting seal rejected
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunkResp.chunkId(), 1, 4096, 0xAB).encode(), null, 5000);
        ScpException conflict = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunkResp.chunkId(), 1, 5000, 0xAB).encode(), null, 5000));
        assertEquals(ErrorCode.CHUNK_SEALED, conflict.code());

        // delete the file: DELETING -> DELETE commands ride heartbeats -> confirmations -> gone
        var delResp = Messages.DeleteFilesResp.decode(client.call(Opcode.DELETE_FILES,
                new Messages.DeleteFiles(List.of(fileId)).encode(), null, 5000));
        assertEquals((short) 0, delResp.codes().get(0).shortValue());

        for (int round = 0; round < 10; round++) {
            service.reconcileNow();
            n1.heartbeat();
            n2.heartbeat();
            n3.heartbeat();
            try {
                Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(fileId).encode(), null, 5000));
            } catch (ScpException e) {
                assertEquals(ErrorCode.FILE_NOT_FOUND, e.code());
                assertTrue(n1.received.stream().anyMatch(c -> c instanceof Messages.DeleteCmd)
                                || n2.received.stream().anyMatch(c -> c instanceof Messages.DeleteCmd)
                                || n3.received.stream().anyMatch(c -> c instanceof Messages.DeleteCmd),
                        "delete commands must have been delivered");
                return; // fully deleted
            }
        }
        throw new AssertionError("file was not fully deleted after delete confirmations");
    }

    @Test
    void reRegistrationKeepsNodeIdAndInvalidatesOldSession() {
        // mediaClass 9 keeps this node out of other tests' placement decisions
        FakeNode n = new FakeNode("hostR") {
            @Override
            void register() {
                var resp = Messages.RegisterResp.decode(client.call(Opcode.REGISTER_NODE,
                        new Messages.RegisterNode(inc.getMostSignificantBits(), inc.getLeastSignificantBits(),
                                List.of(host + ":9000"), "z1", "r1", host,
                                List.of(new Messages.MediaCapacity((byte) 9, 1L << 40)), 1, 0).encode(),
                        null, 5000));
                nodeId = resp.nodeId();
                session = resp.sessionEpoch();
            }
        };
        n.register();
        int firstId = n.nodeId;
        long firstSession = n.session;

        n.register(); // node restarted with same volume identity
        assertEquals(firstId, n.nodeId, "same incarnation -> same nodeId");
        assertNotEquals(firstSession, n.session);

        // a heartbeat with the stale session is rejected
        ScpException e = assertThrows(ScpException.class, () -> client.call(Opcode.NODE_HEARTBEAT,
                new Messages.NodeHeartbeat(firstId, n.inc.getMostSignificantBits(),
                        n.inc.getLeastSignificantBits(), firstSession, List.of(), 0, List.of()).encode(),
                null, 5000));
        assertEquals(ErrorCode.LEASE_EXPIRED, e.code());
    }

    /** Registers three fake nodes on distinct hosts with the given media class. */
    private void registerTrio(byte mediaClass, String hostPrefix) {
        for (String host : List.of(hostPrefix + "A", hostPrefix + "B", hostPrefix + "C")) {
            UUID inc = UUID.randomUUID();
            client.call(Opcode.REGISTER_NODE,
                    new Messages.RegisterNode(inc.getMostSignificantBits(), inc.getLeastSignificantBits(),
                            List.of(host + ":9000"), "z1", "r1", host,
                            List.of(new Messages.MediaCapacity(mediaClass, 1L << 40)), 1, 0).encode(),
                    null, 5000);
        }
    }

    @Test
    void chunkSealIdempotenceRequiresMatchingCrc() {
        registerTrio((byte) 5, "crcHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile((byte) 0, (byte) 5, (byte) 0, "t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1, (byte) 5).encode(), null, 5000));

        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xA).encode(), null, 5000);
        // true idempotent retry: same length AND same crc -> OK
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xA).encode(), null, 5000);
        // same length, DIFFERENT crc: byte divergence — the metadata layer must refuse
        ScpException e = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xB).encode(), null, 5000));
        assertEquals(ErrorCode.CHUNK_SEALED, e.code());
    }

    @Test
    void sealFileValidatesChunkStatesAndTotalLength() {
        registerTrio((byte) 6, "sfHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile((byte) 0, (byte) 6, (byte) 0, "t").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1, (byte) 6).encode(), null, 5000));

        // open chunk present -> sealing the file must be refused
        ScpException open = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_FILE,
                new Messages.SealFile(file.fileId(), 0).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, open.code());

        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xC).encode(), null, 5000);

        // wrong total length -> refused
        ScpException wrongLen = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_FILE,
                new Messages.SealFile(file.fileId(), 999).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, wrongLen.code());

        // correct total -> OK, and idempotent
        client.call(Opcode.SEAL_FILE, new Messages.SealFile(file.fileId(), 100).encode(), null, 5000);
        client.call(Opcode.SEAL_FILE, new Messages.SealFile(file.fileId(), 100).encode(), null, 5000);
        var lookup = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(file.fileId()).encode(), null, 5000));
        assertEquals(1, lookup.fileState());
    }

    @Test
    void placementRequiresThreeAliveNodesOnDistinctHosts() throws Exception {
        // fresh service state: nodes from other tests may be alive; use a distinct media class
        FakeNode a = new FakeNode("hostX");
        FakeNode b = new FakeNode("hostX"); // same host as a -> anti-affinity blocks the pair
        FakeNode c = new FakeNode("hostY");
        // register with mediaClass 7
        for (FakeNode n : List.of(a, b, c)) {
            var resp = Messages.RegisterResp.decode(client.call(Opcode.REGISTER_NODE,
                    new Messages.RegisterNode(n.inc.getMostSignificantBits(), n.inc.getLeastSignificantBits(),
                            List.of(n.host + ":9000"), "z1", "r1", n.host,
                            List.of(new Messages.MediaCapacity((byte) 7, 1L << 40)), 1, 0).encode(),
                    null, 5000));
            n.nodeId = resp.nodeId();
            n.session = resp.sessionEpoch();
        }
        var fileResp = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile((byte) 0, (byte) 7, (byte) 0, "t").encode(), null, 5000));

        // only 2 distinct hosts with mediaClass 7 -> NO_CAPACITY
        ScpException e = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(fileResp.fileId(), 1, (byte) 7).encode(), null, 5000));
        assertEquals(ErrorCode.NO_CAPACITY, e.code());
    }
}
