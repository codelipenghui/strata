package io.strata.meta;

import io.strata.common.FileState;
import io.strata.common.ChunkState;
import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.FrameIO;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import io.strata.proto.ScpClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
        final ScpClient controlClient;
        int nodeId = -1;
        long session = -1;
        final List<Messages.Command> received = new ArrayList<>();
        final List<Messages.CompletedCommand> toReport = new ArrayList<>();

        FakeNode(String host) {
            this(host, MetadataServiceTest.this.client);
        }

        FakeNode(String host, ScpClient controlClient) {
            this.host = host;
            this.controlClient = controlClient;
        }

        void register() {
            var resp = Messages.RegisterResp.decode(controlClient.call(Opcode.REGISTER_NODE,
                    new Messages.RegisterNode(inc.getMostSignificantBits(), inc.getLeastSignificantBits(),
                            List.of(host + ":9000"), "z1", "r1", host,
                            List.of(new Messages.StorageCapacity(1L << 40)), 1, 0).encode(),
                    null, 5000));
            nodeId = resp.nodeId();
            session = resp.sessionEpoch();
        }

        Messages.HeartbeatResp heartbeat() {
            List<Messages.CompletedCommand> done = new ArrayList<>(toReport);
            toReport.clear();
            var resp = Messages.HeartbeatResp.decode(controlClient.call(Opcode.NODE_HEARTBEAT,
                    new Messages.NodeHeartbeat(nodeId, inc.getMostSignificantBits(),
                            inc.getLeastSignificantBits(), session,
                            List.of(new Messages.StorageUsage(0, 1L << 40)), 0, done).encode(),
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
        awaitLeader(service);
        client = new ScpClient("127.0.0.1", service.port(), ScpClient.KIND_TOOL, "test");
    }

    @AfterAll
    void teardown() throws Exception {
        client.close();
        service.close();
        zk.close();
    }

    private static void awaitLeader(MetadataService metadataService) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!metadataService.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(metadataService.isLeader(), "service must acquire leadership");
    }

    @Test
    void createFileStoresClientWritePolicy() {
        assertEquals("127.0.0.1:" + service.port(), service.endpoint());
        Messages.WritePolicy policy = new Messages.WritePolicy(3, 2, true);
        var accepted = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/write-policy", policy).encode(),
                null, 5000));
        var lookup = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(accepted.fileId()).encode(), null, 5000));
        assertEquals(policy, lookup.writePolicy());
    }

    @Test
    void createFileBindsAndDeletesLogicalPath() {
        var created = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/namespace/topicA/0/segment-0")
                        .encode(), null, 5000));

        var byPath = Messages.LookupPathResp.decode(client.call(Opcode.LOOKUP_PATH,
                new Messages.LookupPath("test", "/namespace/topicA/0/segment-0").encode(), null, 5000));
        assertEquals(created.fileId(), byPath.fileId());

        var byId = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(created.fileId()).encode(), null, 5000));
        assertEquals(io.strata.common.StrataNamespace.of("test"), byId.namespace());
        assertEquals(io.strata.common.StrataPath.of("/namespace/topicA/0/segment-0"), byId.path());

        ScpException duplicate = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/namespace/topicA/0/segment-0")
                        .encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, duplicate.code());

        var samePathOtherNamespace = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test-alt", "/namespace/topicA/0/segment-0")
                        .encode(), null, 5000));
        var byOtherNamespace = Messages.LookupPathResp.decode(client.call(Opcode.LOOKUP_PATH,
                new Messages.LookupPath("test-alt", "/namespace/topicA/0/segment-0").encode(), null, 5000));
        assertEquals(samePathOtherNamespace.fileId(), byOtherNamespace.fileId());

        var delete = Messages.DeleteFilesResp.decode(client.call(Opcode.DELETE_FILES,
                new Messages.DeleteFiles(List.of(created.fileId(), samePathOtherNamespace.fileId())).encode(),
                null, 5000));
        assertEquals(List.of(ErrorCode.OK.code, ErrorCode.OK.code), delete.codes());

        ScpException missing = assertThrows(ScpException.class, () -> client.call(Opcode.LOOKUP_PATH,
                new Messages.LookupPath("test", "/namespace/topicA/0/segment-0").encode(), null, 5000));
        assertEquals(ErrorCode.FILE_NOT_FOUND, missing.code());
    }

    @Test
    void lookupPathRejectsMarkerThatPointsAtDifferentLogicalFile() throws Exception {
        var first = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/marker-identity-a")
                        .encode(), null, 5000));
        var second = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/marker-identity-b")
                        .encode(), null, 5000));

        metadataStore().curator().setData()
                .forPath("/strata/namespaces/test/paths/marker-identity-a/__file", fileIdBytes(second.fileId()));

        ScpException wrongBinding = assertThrows(ScpException.class, () -> client.call(Opcode.LOOKUP_PATH,
                new Messages.LookupPath("test", "/marker-identity-a").encode(), null, 5000));
        assertEquals(ErrorCode.FILE_NOT_FOUND, wrongBinding.code());

        var byId = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(first.fileId()).encode(), null, 5000));
        assertEquals(io.strata.common.StrataPath.of("/marker-identity-a"), byId.path());
    }

    @Test
    void pingEchoesPayloadAndUnknownOpcodesAreRejected() throws Exception {
        byte[] payload = "metadata-ping".getBytes();
        Frame ping = client.callFrame(Opcode.PING, Messages.okHeader(), ByteBuffer.wrap(payload), 5000);
        Resp.check(ping.headerSlice());
        byte[] echoed = new byte[ping.payloadLength()];
        ping.payloadSlice().get(echoed);
        assertArrayEquals(payload, echoed);

        ScpException wrongPlane = assertThrows(ScpException.class,
                () -> client.call(Opcode.OPEN_CHUNK, new byte[0], null, 5000));
        assertEquals(ErrorCode.UNKNOWN_OPCODE, wrongPlane.code());

        ScpException unknownNumeric = assertThrows(ScpException.class, () -> callRawOpcode((short) 0x7FFF));
        assertEquals(ErrorCode.UNKNOWN_OPCODE, unknownNumeric.code());
    }

    @Test
    void nonLeaderRejectsRequestsWithTypedRedirectSignal() throws Exception {
        try (MetadataService follower = new MetadataService(MetaConfig.forTests(zk.getConnectString()));
             ScpClient followerClient = new ScpClient("127.0.0.1", follower.port(),
                     ScpClient.KIND_TOOL, "follower-test")) {
            Thread.sleep(200); // let the follower's LeaderLatch finish its background join
            assertTrue(service.isLeader(), "primary service should keep leadership for this test");
            assertEquals(false, follower.isLeader(), "second service should remain standby");

            ScpException e = assertThrows(ScpException.class,
                    () -> followerClient.call(Opcode.PING, Messages.okHeader(), null, 5000));
            assertEquals(ErrorCode.NOT_LEADER, e.code());
        }
    }

    @Test
    void failedServerBindCleansStartupResources() throws Exception {
        int participantsBefore = leaderParticipantCount();
        try (ServerSocket blocker = new ServerSocket(0)) {
            MetaConfig blocked = new MetaConfig(zk.getConnectString(), blocker.getLocalPort(),
                    200, 1_000, 1_500, 300, 3_000);
            assertThrows(Exception.class, () -> new MetadataService(blocked));
        }

        long deadline = System.currentTimeMillis() + 2_000;
        while (leaderParticipantCount() != participantsBefore && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(participantsBefore, leaderParticipantCount(),
                "failed metadata startup left a leader-latch participant behind");
    }

    @Test
    void missingFileOperationsReturnTypedErrorsOrIdempotentSuccess() {
        FileId missing = FileId.random();

        ScpException createChunk = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(missing, 1).encode(), null, 5000));
        assertEquals(ErrorCode.FILE_NOT_FOUND, createChunk.code());

        ScpException allocate = assertThrows(ScpException.class, () -> client.call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forAppend(missing).encode(), null, 5000));
        assertEquals(ErrorCode.FILE_NOT_FOUND, allocate.code());

        ScpException sealChunk = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(new ChunkId(missing, 0), 1, 10, 0x1, List.of()).encode(),
                null, 5000));
        assertEquals(ErrorCode.FILE_NOT_FOUND, sealChunk.code());

        ScpException lookup = assertThrows(ScpException.class, () -> client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(missing).encode(), null, 5000));
        assertEquals(ErrorCode.FILE_NOT_FOUND, lookup.code());

        var delete = Messages.DeleteFilesResp.decode(client.call(Opcode.DELETE_FILES,
                new Messages.DeleteFiles(List.of(missing)).encode(), null, 5000));
        assertEquals(ErrorCode.FILE_NOT_FOUND.code, delete.codes().get(0).shortValue());

        client.call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(new ChunkId(missing, 0), 1, 1, 2).encode(), null, 5000);
    }

    @Test
    void deleteFilesReportsInternalWhenPathMarkerBelongsToAnotherFile() throws Exception {
        var created = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/delete-path-owner-mismatch").encode(), null, 5000));

        DeletePathRejectingStore rejecting = new DeletePathRejectingStore(metadataStore());
        MetadataStore original = replaceStore(rejecting);
        try {
            var delete = Messages.DeleteFilesResp.decode(client.call(Opcode.DELETE_FILES,
                    new Messages.DeleteFiles(List.of(created.fileId())).encode(), null, 5000));

            assertEquals(ErrorCode.INTERNAL.code, delete.codes().get(0).shortValue());
            assertEquals(1, rejecting.deletePathCalls);
        } finally {
            replaceStore(original);
        }
    }

    @Test
    void fileChunkLifecycleWithPlacement() throws Exception {
        try (TestingServer localZk = new TestingServer(true);
             MetadataService localService = new MetadataService(MetaConfig.forTests(localZk.getConnectString()));
             ScpClient localClient = new ScpClient("127.0.0.1", localService.port(),
                     ScpClient.KIND_TOOL, "isolated-lifecycle")) {
            awaitLeader(localService);
            FakeNode n1 = new FakeNode("hostA", localClient);
            FakeNode n2 = new FakeNode("hostB", localClient);
            FakeNode n3 = new FakeNode("hostC", localClient);
            n1.register();
            n2.register();
            n3.register();
            assertEquals(3, Set.of(n1.nodeId, n2.nodeId, n3.nodeId).size());

            // create file
            var fileResp = Messages.CreateFileResp.decode(localClient.call(Opcode.CREATE_FILE,
                    new Messages.CreateFile("test", "/topicA-0").encode(), null, 5000));
            FileId fileId = fileResp.fileId();

            // create chunk: 3 replicas, all distinct nodes and hosts
            var chunkResp = Messages.CreateChunkResp.decode(localClient.call(Opcode.CREATE_CHUNK,
                    new Messages.CreateChunk(fileId, 1).encode(), null, 5000));
            assertEquals(0, chunkResp.chunkId().index());
            assertEquals(3, chunkResp.replicas().size());
            Set<Integer> ids = new HashSet<>();
            for (var r : chunkResp.replicas()) {
                ids.add(r.nodeId());
                assertTrue(r.endpoint().endsWith(":9000"));
            }
            assertEquals(3, ids.size());

            // seal chunk 0, then a higher-epoch writer takes over with chunk 1
            localClient.call(Opcode.SEAL_CHUNK_META,
                    new Messages.SealChunkMeta(chunkResp.chunkId(), 1, 4096, 0xAB, List.of()).encode(), null, 5000);
            var chunk2 = Messages.CreateChunkResp.decode(localClient.call(Opcode.CREATE_CHUNK,
                    new Messages.CreateChunk(fileId, 2).encode(), null, 5000));
            assertEquals(1, chunk2.chunkId().index());

            // lower-epoch create rejected (stale leader guard; fence precedence over tail-open)
            ScpException stale = assertThrows(ScpException.class, () -> localClient.call(Opcode.CREATE_CHUNK,
                    new Messages.CreateChunk(fileId, 1).encode(), null, 5000));
            assertEquals(ErrorCode.FENCED_EPOCH, stale.code());

            var lookup = Messages.LookupFileResp.decode(localClient.call(Opcode.LOOKUP_FILE,
                    new Messages.LookupFile(fileId).encode(), null, 5000));
            assertEquals(2, lookup.chunks().size());
            assertEquals(ChunkState.SEALED, lookup.chunks().get(0).state());
            assertEquals(4096, lookup.chunks().get(0).length());
            assertEquals(ChunkState.OPEN, lookup.chunks().get(1).state());

            // idempotent seal ok; conflicting seal rejected
            localClient.call(Opcode.SEAL_CHUNK_META,
                    new Messages.SealChunkMeta(chunkResp.chunkId(), 1, 4096, 0xAB, List.of()).encode(), null, 5000);
            ScpException conflict = assertThrows(ScpException.class, () -> localClient.call(Opcode.SEAL_CHUNK_META,
                    new Messages.SealChunkMeta(chunkResp.chunkId(), 1, 5000, 0xAB, List.of()).encode(), null, 5000));
            assertEquals(ErrorCode.CHUNK_SEALED, conflict.code());

            // delete the file: DELETING -> DELETE commands ride heartbeats -> confirmations -> gone
            var delResp = Messages.DeleteFilesResp.decode(localClient.call(Opcode.DELETE_FILES,
                    new Messages.DeleteFiles(List.of(fileId)).encode(), null, 5000));
            assertEquals((short) 0, delResp.codes().get(0).shortValue());

            for (int round = 0; round < 10; round++) {
                localService.reconcileNow();
                n1.heartbeat();
                n2.heartbeat();
                n3.heartbeat();
                try {
                    Messages.LookupFileResp.decode(localClient.call(Opcode.LOOKUP_FILE,
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
    }

    @Test
    void reRegistrationKeepsNodeIdAndInvalidatesOldSession() {
        FakeNode n = new FakeNode("hostR") {
            @Override
            void register() {
                var resp = Messages.RegisterResp.decode(client.call(Opcode.REGISTER_NODE,
                        new Messages.RegisterNode(inc.getMostSignificantBits(), inc.getLeastSignificantBits(),
                                List.of(host + ":9000"), "z1", "r1", host,
                                List.of(new Messages.StorageCapacity(1L << 40)), 1, 0).encode(),
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

    @Test
    void registrationRejectsNodesWithoutUsableEndpointOrCapacity() {
        UUID noEndpoint = UUID.randomUUID();
        ScpException missingEndpoint = assertThrows(ScpException.class, () -> client.call(Opcode.REGISTER_NODE,
                new Messages.RegisterNode(noEndpoint.getMostSignificantBits(), noEndpoint.getLeastSignificantBits(),
                        List.of(), "z1", "r1", "badEndpoint",
                        List.of(new Messages.StorageCapacity(100)), 1, 0).encode(),
                null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, missingEndpoint.code());

        UUID noCapacity = UUID.randomUUID();
        ScpException missingCapacity = assertThrows(ScpException.class, () -> client.call(Opcode.REGISTER_NODE,
                new Messages.RegisterNode(noCapacity.getMostSignificantBits(), noCapacity.getLeastSignificantBits(),
                        List.of("noCapacity:9000"), "z1", "r1", "noCapacity",
                        List.of(), 1, 0).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, missingCapacity.code());

        UUID zeroCapacity = UUID.randomUUID();
        ScpException unusableCapacity = assertThrows(ScpException.class, () -> client.call(Opcode.REGISTER_NODE,
                new Messages.RegisterNode(zeroCapacity.getMostSignificantBits(),
                        zeroCapacity.getLeastSignificantBits(), List.of("zeroCapacity:9000"),
                        "z1", "r1", "zeroCapacity",
                        List.of(new Messages.StorageCapacity(0)), 1, 0).encode(),
                null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, unusableCapacity.code());
    }

    @Test
    void heartbeatRejectsInvalidStorageUsage() {
        FakeNode n = new FakeNode("badUsage");
        n.register();

        ScpException negativeUsage = assertThrows(ScpException.class, () -> client.call(Opcode.NODE_HEARTBEAT,
                new Messages.NodeHeartbeat(n.nodeId, n.inc.getMostSignificantBits(),
                        n.inc.getLeastSignificantBits(), n.session,
                        List.of(new Messages.StorageUsage(-1, 100)), 0, List.of()).encode(),
                null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, negativeUsage.code());
    }

    @Test
    void rejectedHeartbeatDoesNotExtendLease() throws Exception {
        FakeNode n = new FakeNode("leaseReject");
        n.register();
        NodeRegistry.LiveNode live = registry().aliveNodes().stream()
                .filter(node -> node.record.nodeId() == n.nodeId)
                .findFirst()
                .orElseThrow();
        long leaseBefore = live.leaseUntil;

        Thread.sleep(10);
        ScpException invalidUsage = assertThrows(ScpException.class, () -> client.call(Opcode.NODE_HEARTBEAT,
                new Messages.NodeHeartbeat(n.nodeId, n.inc.getMostSignificantBits(),
                        n.inc.getLeastSignificantBits(), n.session,
                        List.of(new Messages.StorageUsage(-1, 100)), 0, List.of()).encode(),
                null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, invalidUsage.code());
        assertEquals(leaseBefore, live.leaseUntil);
    }

    /** Registers three fake nodes on distinct hosts. */
    private void registerTrio(String hostPrefix) {
        for (String host : List.of(hostPrefix + "A", hostPrefix + "B", hostPrefix + "C")) {
            UUID inc = UUID.randomUUID();
            client.call(Opcode.REGISTER_NODE,
                    new Messages.RegisterNode(inc.getMostSignificantBits(), inc.getLeastSignificantBits(),
                            List.of(host + ":9000"), "z1", "r1", host,
                            List.of(new Messages.StorageCapacity(1L << 40)), 1, 0).encode(),
                    null, 5000);
        }
    }

    @Test
    void placementExcludesNodesWithNoFreeBytes() throws Exception {
        try (TestingServer localZk = new TestingServer(true);
             MetadataService localService = new MetadataService(MetaConfig.forTests(localZk.getConnectString()));
             ScpClient localClient = new ScpClient("127.0.0.1", localService.port(),
                     ScpClient.KIND_TOOL, "isolated-fullness")) {
            awaitLeader(localService);
            List<UUID> incarnations = new ArrayList<>();
            List<Integer> nodeIds = new ArrayList<>();
            List<Long> sessions = new ArrayList<>();
            for (String host : List.of("fullA", "fullB", "fullC")) {
                UUID inc = UUID.randomUUID();
                incarnations.add(inc);
                var resp = Messages.RegisterResp.decode(localClient.call(Opcode.REGISTER_NODE,
                        new Messages.RegisterNode(inc.getMostSignificantBits(), inc.getLeastSignificantBits(),
                                List.of(host + ":9000"), "z1", "r1", host,
                                List.of(new Messages.StorageCapacity(100)), 1, 0).encode(),
                        null, 5000));
                nodeIds.add(resp.nodeId());
                sessions.add(resp.sessionEpoch());
            }

            localClient.call(Opcode.NODE_HEARTBEAT,
                    new Messages.NodeHeartbeat(nodeIds.get(0), incarnations.get(0).getMostSignificantBits(),
                            incarnations.get(0).getLeastSignificantBits(), sessions.get(0),
                            List.of(new Messages.StorageUsage(100, 0)), 0, List.of()).encode(),
                    null, 5000);

            var fileResp = Messages.CreateFileResp.decode(localClient.call(Opcode.CREATE_FILE,
                    new Messages.CreateFile("test", "/full").encode(), null, 5000));
            ScpException noCapacity = assertThrows(ScpException.class, () -> localClient.call(Opcode.CREATE_CHUNK,
                    new Messages.CreateChunk(fileResp.fileId(), 1).encode(), null, 5000));
            assertEquals(ErrorCode.NO_CAPACITY, noCapacity.code());

            localClient.call(Opcode.NODE_HEARTBEAT,
                    new Messages.NodeHeartbeat(nodeIds.get(0), incarnations.get(0).getMostSignificantBits(),
                            incarnations.get(0).getLeastSignificantBits(), sessions.get(0),
                            List.of(new Messages.StorageUsage(0, 100)), 0, List.of()).encode(),
                    null, 5000);
            var chunkResp = Messages.CreateChunkResp.decode(localClient.call(Opcode.CREATE_CHUNK,
                    new Messages.CreateChunk(fileResp.fileId(), 1).encode(), null, 5000));
            assertEquals(3, chunkResp.replicas().size());
        }
    }

    private NodeRegistry registry() throws Exception {
        Field field = MetadataService.class.getDeclaredField("registry");
        field.setAccessible(true);
        return (NodeRegistry) field.get(service);
    }

    private ZkMetadataStore metadataStore() throws Exception {
        Field field = MetadataService.class.getDeclaredField("store");
        field.setAccessible(true);
        return (ZkMetadataStore) field.get(service);
    }

    private int leaderParticipantCount() throws Exception {
        if (metadataStore().curator().checkExists().forPath("/strata/leader") == null) {
            return 0;
        }
        return metadataStore().curator().getChildren().forPath("/strata/leader").size();
    }

    private static byte[] fileIdBytes(FileId fileId) {
        ByteBuffer b = ByteBuffer.allocate(16);
        fileId.writeTo(b);
        return b.array();
    }

    private MetadataStore replaceStore(MetadataStore replacement) throws Exception {
        Field field = MetadataService.class.getDeclaredField("store");
        field.setAccessible(true);
        MetadataStore previous = (MetadataStore) field.get(service);
        field.set(service, replacement);
        return previous;
    }

    private void callRawOpcode(short opcode) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", service.port());
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            FrameIO.write(out, Frame.request(Opcode.HELLO,
                    new Messages.Hello(ScpClient.KIND_TOOL, 0, "raw-opcode-test").encode(), null, 0));
            Frame hello = FrameIO.read(in);
            Resp.check(hello.headerSlice());

            FrameIO.write(out, new Frame(opcode, (short) 1, (short) 0, 42,
                    ByteBuffer.allocate(0), ByteBuffer.allocate(0)));
            Frame response = FrameIO.read(in);
            Resp.check(response.headerSlice());
        }
    }

    private static class DelegatingMetadataStore implements MetadataStore {
        protected final MetadataStore delegate;

        private DelegatingMetadataStore(MetadataStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public java.util.Optional<Versioned<Records.FileRecord>> getFile(FileId id) throws Exception {
            return delegate.getFile(id);
        }

        @Override
        public java.util.Optional<FileId> resolvePath(io.strata.common.StrataNamespace namespace,
                                                      io.strata.common.StrataPath path) throws Exception {
            return delegate.resolvePath(namespace, path);
        }

        @Override
        public void createFile(Records.FileRecord record) throws Exception {
            delegate.createFile(record);
        }

        @Override
        public boolean updateFile(Records.FileRecord record, int expectedVersion) throws Exception {
            return delegate.updateFile(record, expectedVersion);
        }

        @Override
        public boolean deletePath(io.strata.common.StrataNamespace namespace, io.strata.common.StrataPath path,
                                  FileId expectedFileId) throws Exception {
            return delegate.deletePath(namespace, path, expectedFileId);
        }

        @Override
        public boolean deleteFile(FileId id, int expectedVersion) throws Exception {
            return delegate.deleteFile(id, expectedVersion);
        }

        @Override
        public List<FileId> listFiles() throws Exception {
            return delegate.listFiles();
        }

        @Override
        public int nextNodeId() throws Exception {
            return delegate.nextNodeId();
        }

        @Override
        public boolean putNode(Records.NodeRecord record, int expectedVersion) throws Exception {
            return delegate.putNode(record, expectedVersion);
        }

        @Override
        public java.util.Optional<Versioned<Records.NodeRecord>> getNode(int nodeId) throws Exception {
            return delegate.getNode(nodeId);
        }

        @Override
        public List<Versioned<Records.NodeRecord>> listNodes() throws Exception {
            return delegate.listNodes();
        }

        @Override
        public void close() {
        }
    }

    private static final class UpdateRejectingStore extends DelegatingMetadataStore {
        int updateCalls;

        private UpdateRejectingStore(MetadataStore delegate) {
            super(delegate);
        }

        @Override
        public boolean updateFile(Records.FileRecord record, int expectedVersion) {
            updateCalls++;
            return false;
        }
    }

    private static final class DeletePathRejectingStore extends DelegatingMetadataStore {
        int deletePathCalls;

        private DeletePathRejectingStore(MetadataStore delegate) {
            super(delegate);
        }

        @Override
        public boolean deletePath(io.strata.common.StrataNamespace namespace, io.strata.common.StrataPath path,
                                  FileId expectedFileId) {
            deletePathCalls++;
            return false;
        }
    }

    @Test
    void createChunkRejectedWhileTailChunkIsOpen() {
        registerTrio("tailHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/tail-open").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1).encode(), null, 5000));

        // no legitimate flow creates a chunk while the tail is OPEN (appenders seal before
        // rolling; recovery seals before a new appender opens) — two same-epoch appenders racing
        // would otherwise both claim file offset 0 in different chunks
        ScpException e = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, e.code());
        ScpException e2 = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 2).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, e2.code());

        // sealing the tail re-enables creation
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 10, 0xA, List.of()).encode(), null, 5000);
        var next = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1).encode(), null, 5000));
        assertEquals(1, next.chunkId().index());
    }

    @Test
    void fileAndChunkCreatesAreIdempotentByOperationId() {
        registerTrio("idemHost");

        FileId requested = FileId.random();
        var createFile = new Messages.CreateFile("test", "/idem", requested, 100, 200);
        var file1 = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                createFile.encode(), null, 5000));
        var file2 = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                createFile.encode(), null, 5000));
        assertEquals(file1.fileId(), file2.fileId());
        assertEquals(requested, file1.fileId());

        assertCreateReplayConflict("/idem-path", "test", "/idem-path-replay");
        assertCreateReplayConflict("/idem-namespace", "test-replay", "/idem-namespace");

        ScpException conflictingFile = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/idem", requested, 101, 201).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, conflictingFile.code());

        var createChunk = new Messages.CreateChunk(file1.fileId(), 1, 300, 400);
        var chunk1 = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                createChunk.encode(), null, 5000));
        var chunk2 = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                createChunk.encode(), null, 5000));
        assertEquals(chunk1, chunk2);

        ScpException conflictingChunk = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file1.fileId(), 1, 301, 401).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, conflictingChunk.code());
    }

    private void assertCreateReplayConflict(String path, String replayNamespace, String replayPath) {
        FileId requested = FileId.random();
        long opMsb = requested.msb();
        long opLsb = requested.lsb();
        client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", path, requested, opMsb, opLsb).encode(),
                null, 5000);

        ScpException conflict = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile(replayNamespace, replayPath,
                        requested, opMsb, opLsb).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, conflict.code());
    }

    @Test
    void abortChunkRemovesOnlyMatchingOpenTail() {
        registerTrio("abortHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/abort").encode(), null, 5000));
        var createChunk = new Messages.CreateChunk(file.fileId(), 1, 500, 600);
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                createChunk.encode(), null, 5000));

        ScpException wrongToken = assertThrows(ScpException.class, () -> client.call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(chunk.chunkId(), 1, 501, 601).encode(), null, 5000));
        assertEquals(ErrorCode.FENCED_EPOCH, wrongToken.code());

        ScpException wrongEpoch = assertThrows(ScpException.class, () -> client.call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(chunk.chunkId(), 2,
                        createChunk.opIdMsb(), createChunk.opIdLsb()).encode(), null, 5000));
        assertEquals(ErrorCode.FENCED_EPOCH, wrongEpoch.code());

        client.call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(chunk.chunkId(), 1,
                        createChunk.opIdMsb(), createChunk.opIdLsb()).encode(), null, 5000);
        client.call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(chunk.chunkId(), 1,
                        createChunk.opIdMsb(), createChunk.opIdLsb()).encode(), null, 5000);

        var lookup = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(file.fileId()).encode(), null, 5000));
        assertEquals(0, lookup.chunks().size());

        var replacement = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1).encode(), null, 5000));
        assertEquals(0, replacement.chunkId().index());
    }

    @Test
    void abortChunkIsIdempotentOnlyForMissingChunksNotNonTailChunks() {
        registerTrio("abortTailHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/abort-non-tail").encode(), null, 5000));
        var chunk0 = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1, 700, 800).encode(), null, 5000));

        client.call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(new ChunkId(file.fileId(), 99), 1, 1, 2).encode(), null, 5000);
        var lookupAfterMissingAbort = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(file.fileId()).encode(), null, 5000));
        assertEquals(1, lookupAfterMissingAbort.chunks().size());

        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk0.chunkId(), 1, 100, 0xD, List.of()).encode(), null, 5000);
        client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 2, 701, 801).encode(), null, 5000);

        ScpException nonTail = assertThrows(ScpException.class, () -> client.call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(chunk0.chunkId(), 1, 700, 800).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, nonTail.code());
    }

    @Test
    void allocateWriterEpochFencesAppendAndRecoveryOwnership() {
        registerTrio("epochHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/writer-epoch").encode(), null, 5000));

        var first = Messages.AllocateWriterEpochResp.decode(client.call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forAppend(file.fileId()).encode(), null, 5000));
        assertEquals(1, first.writerEpoch());

        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), first.writerEpoch(), 900, 901).encode(), null, 5000));
        assertEquals(first.writerEpoch(), chunk.writeEpoch());

        ScpException appendWhileOpen = assertThrows(ScpException.class, () -> client.call(
                Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forAppend(file.fileId()).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, appendWhileOpen.code());

        var recovery = Messages.AllocateWriterEpochResp.decode(client.call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forRecovery(file.fileId()).encode(), null, 5000));
        assertEquals(2, recovery.writerEpoch());

        ScpException staleSeal = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), first.writerEpoch(), 10, 0xA, List.of()).encode(),
                null, 5000));
        assertEquals(ErrorCode.FENCED_EPOCH, staleSeal.code());

        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), recovery.writerEpoch(), 10, 0xA, List.of()).encode(),
                null, 5000);

        var nextAppend = Messages.AllocateWriterEpochResp.decode(client.call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forAppend(file.fileId()).encode(), null, 5000));
        assertEquals(3, nextAppend.writerEpoch());

        ScpException staleCreate = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), recovery.writerEpoch()).encode(), null, 5000));
        assertEquals(ErrorCode.FENCED_EPOCH, staleCreate.code());

        var nextChunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), nextAppend.writerEpoch()).encode(), null, 5000));
        assertEquals(1, nextChunk.chunkId().index());
        assertEquals(nextAppend.writerEpoch(), nextChunk.writeEpoch());
    }

    @Test
    void abortChunkRejectsSealedTailButMissingChunkAbortIsIdempotent() {
        registerTrio("abortSealedHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/abort-sealed").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1, 710, 810).encode(), null, 5000));

        client.call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(new ChunkId(file.fileId(), 99), 1, 1, 2).encode(), null, 5000);
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 20, 0xE, List.of()).encode(), null, 5000);

        ScpException sealed = assertThrows(ScpException.class, () -> client.call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(chunk.chunkId(), 1, 710, 810).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, sealed.code());
    }

    @Test
    void deletingFileCannotBeSealedOrResurrected() {
        registerTrio("delHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/deleting-seal").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1).encode(), null, 5000));

        client.call(Opcode.DELETE_FILES, new Messages.DeleteFiles(List.of(file.fileId())).encode(), null, 5000);

        ScpException allocate = assertThrows(ScpException.class, () -> client.call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forAppend(file.fileId()).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, allocate.code());

        // an in-flight appender's seal must not mutate a DELETING file...
        ScpException sealChunk = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 10, 0xA, List.of()).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, sealChunk.code());

        // ...and SEAL_FILE must never resurrect it to SEALED (deletion would half-stop, leaving
        // a live file with missing chunks)
        ScpException sealFile = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_FILE,
                new Messages.SealFile(file.fileId(), 0).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, sealFile.code());

        ScpException createChunk = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 2).encode(), null, 5000));
        assertEquals(ErrorCode.FILE_SEALED, createChunk.code());

        var lookup = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(file.fileId()).encode(), null, 5000));
        assertEquals(2, lookup.fileState(), "file must remain DELETING");
    }

    @Test
    void chunkSealIdempotenceRequiresMatchingCrc() {
        registerTrio("crcHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/seal-length").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1).encode(), null, 5000));

        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xA, List.of()).encode(), null, 5000);
        // true idempotent retry: same length AND same crc -> OK
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xA, List.of()).encode(), null, 5000);
        // same length, DIFFERENT crc: byte divergence — the metadata layer must refuse
        ScpException e = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xB, List.of()).encode(), null, 5000));
        assertEquals(ErrorCode.CHUNK_SEALED, e.code());

        // A stale writer must still be fenced even if it reports the already-committed bytes.
        ScpException stale = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 0, 100, 0xA, List.of()).encode(), null, 5000));
        assertEquals(ErrorCode.FENCED_EPOCH, stale.code());
    }

    @Test
    void sealChunkMetaRejectsNegativeLength() {
        registerTrio("negSealHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/seal-replica-quorum").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1).encode(), null, 5000));

        ScpException e = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, -1, 0xC, List.of()).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, e.code());

        var lookup = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(file.fileId()).encode(), null, 5000));
        assertEquals(ChunkState.OPEN, lookup.chunks().get(0).state());
        assertEquals(0, lookup.chunks().get(0).length());
    }

    @Test
    void sealChunkMetaRequiresConfirmedReplicaQuorumWhenProvided() {
        registerTrio("sealQuorumHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/seal-replica-retain").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1).encode(), null, 5000));

        ScpException oneReplica = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xC,
                        List.of(chunk.replicas().get(0).nodeId())).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, oneReplica.code());

        List<Integer> quorum = List.of(chunk.replicas().get(0).nodeId(), chunk.replicas().get(1).nodeId());
        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xC, quorum).encode(), null, 5000);
        var lookup = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(file.fileId()).encode(), null, 5000));
        assertEquals(2, lookup.chunks().get(0).replicas().size());
    }

    @Test
    void sealChunkMetaRejectsDisjointConfirmedReplicas() {
        registerTrio("sealDisjointHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/seal-disjoint").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1).encode(), null, 5000));

        ScpException disjoint = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xC, List.of(999_999)).encode(),
                null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, disjoint.code());

        ScpException missingChunk = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(new ChunkId(file.fileId(), 99), 1, 100, 0xC, List.of()).encode(),
                null, 5000));
        assertEquals(ErrorCode.CHUNK_NOT_FOUND, missingChunk.code());
    }

    @Test
    void sealFileValidatesChunkStatesAndTotalLength() {
        registerTrio("sfHost");
        var file = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/seal-file-state").encode(), null, 5000));
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 1).encode(), null, 5000));

        // open chunk present -> sealing the file must be refused
        ScpException open = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_FILE,
                new Messages.SealFile(file.fileId(), 0).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, open.code());

        client.call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xC, List.of()).encode(), null, 5000);

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

        ScpException allocate = assertThrows(ScpException.class, () -> client.call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forAppend(file.fileId()).encode(), null, 5000));
        assertEquals(ErrorCode.FILE_SEALED, allocate.code());

        ScpException sealedFile = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(file.fileId(), 2).encode(), null, 5000));
        assertEquals(ErrorCode.FILE_SEALED, sealedFile.code());
    }

    @Test
    void sealFileRejectsCorruptCommittedChunkLengths() throws Exception {
        FileId negative = FileId.random();
        metadataStore().createFile(new Records.FileRecord(negative, "test", "/negative-length", 3, 2, false, FileState.OPEN, System.currentTimeMillis(),
                List.of(new Records.ChunkRecord(0, ChunkState.SEALED, -1, 0, 1, List.of()))));

        ScpException negativeLength = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_FILE,
                new Messages.SealFile(negative, 0).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, negativeLength.code());

        FileId overflow = FileId.random();
        metadataStore().createFile(new Records.FileRecord(overflow, "test", "/overflow-length", 3, 2, false, FileState.OPEN, System.currentTimeMillis(),
                List.of(
                        new Records.ChunkRecord(0, ChunkState.SEALED, Long.MAX_VALUE, 0, 1, List.of()),
                        new Records.ChunkRecord(1, ChunkState.SEALED, 1, 0, 1, List.of()))));

        ScpException overflowLength = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_FILE,
                new Messages.SealFile(overflow, 0).encode(), null, 5000));
        assertEquals(ErrorCode.PRECONDITION_FAILED, overflowLength.code());
    }

    @Test
    void metadataMutationsReportCasExhaustion() throws Exception {
        registerTrio("casHost");

        var createChunkFile = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/cas-create-chunk").encode(),
                null, 5000));
        UpdateRejectingStore createRejecting = new UpdateRejectingStore(metadataStore());
        MetadataStore original = replaceStore(createRejecting);
        try {
            ScpException createChunk = assertThrows(ScpException.class, () -> client.call(Opcode.CREATE_CHUNK,
                    new Messages.CreateChunk(createChunkFile.fileId(), 1).encode(), null, 5000));
            assertEquals(ErrorCode.INTERNAL, createChunk.code());
            assertTrue(createChunk.getMessage().contains("createChunk CAS exhausted"));
            assertTrue(createRejecting.updateCalls >= 5);
        } finally {
            replaceStore(original);
        }

        var allocateEpochFile = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/cas-allocate-epoch").encode(),
                null, 5000));
        UpdateRejectingStore epochRejecting = new UpdateRejectingStore(metadataStore());
        original = replaceStore(epochRejecting);
        try {
            ScpException allocateEpoch = assertThrows(ScpException.class,
                    () -> client.call(Opcode.ALLOCATE_WRITER_EPOCH,
                            Messages.AllocateWriterEpoch.forAppend(allocateEpochFile.fileId()).encode(),
                            null, 5000));
            assertEquals(ErrorCode.INTERNAL, allocateEpoch.code());
            assertTrue(allocateEpoch.getMessage().contains("allocateWriterEpoch CAS exhausted"));
            assertTrue(epochRejecting.updateCalls >= 5);
        } finally {
            replaceStore(original);
        }

        var chunkFile = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/cas-chunk").encode(), null, 5000));
        var createChunk = new Messages.CreateChunk(chunkFile.fileId(), 1, 7000, 8000);
        var chunk = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                createChunk.encode(), null, 5000));

        UpdateRejectingStore sealRejecting = new UpdateRejectingStore(metadataStore());
        original = replaceStore(sealRejecting);
        try {
            ScpException sealChunk = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK_META,
                    new Messages.SealChunkMeta(chunk.chunkId(), 1, 100, 0xCC, List.of()).encode(), null, 5000));
            assertEquals(ErrorCode.INTERNAL, sealChunk.code());
            assertTrue(sealChunk.getMessage().contains("sealChunk CAS exhausted"));
            assertTrue(sealRejecting.updateCalls >= 5);
        } finally {
            replaceStore(original);
        }

        UpdateRejectingStore abortRejecting = new UpdateRejectingStore(metadataStore());
        original = replaceStore(abortRejecting);
        try {
            ScpException abortChunk = assertThrows(ScpException.class, () -> client.call(Opcode.ABORT_CHUNK_META,
                    new Messages.AbortChunkMeta(chunk.chunkId(), 1,
                            createChunk.opIdMsb(), createChunk.opIdLsb()).encode(), null, 5000));
            assertEquals(ErrorCode.INTERNAL, abortChunk.code());
            assertTrue(abortChunk.getMessage().contains("abortChunk CAS exhausted"));
            assertTrue(abortRejecting.updateCalls >= 5);
        } finally {
            replaceStore(original);
        }

        var sealFile = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/cas-seal-file").encode(),
                null, 5000));
        UpdateRejectingStore mutateRejecting = new UpdateRejectingStore(metadataStore());
        original = replaceStore(mutateRejecting);
        try {
            ScpException seal = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_FILE,
                    new Messages.SealFile(sealFile.fileId(), 0).encode(), null, 5000));
            assertEquals(ErrorCode.INTERNAL, seal.code());
            assertTrue(seal.getMessage().contains("file mutation CAS exhausted"));
            assertTrue(mutateRejecting.updateCalls >= 5);
        } finally {
            replaceStore(original);
        }
    }

    @Test
    void placementRequiresThreeAliveNodesOnDistinctHosts() throws Exception {
        try (TestingServer localZk = new TestingServer(true);
             MetadataService localService = new MetadataService(MetaConfig.forTests(localZk.getConnectString()));
             ScpClient localClient = new ScpClient("127.0.0.1", localService.port(),
                     ScpClient.KIND_TOOL, "isolated-host-affinity")) {
            awaitLeader(localService);
            FakeNode a = new FakeNode("hostX", localClient);
            FakeNode b = new FakeNode("hostX", localClient); // same host as a -> anti-affinity blocks the pair
            FakeNode c = new FakeNode("hostY", localClient);
            for (FakeNode n : List.of(a, b, c)) {
                n.register();
            }
            var fileResp = Messages.CreateFileResp.decode(localClient.call(Opcode.CREATE_FILE,
                    new Messages.CreateFile("test", "/sealed-overflow").encode(), null, 5000));

            // only 2 distinct hosts -> NO_CAPACITY
            ScpException e = assertThrows(ScpException.class, () -> localClient.call(Opcode.CREATE_CHUNK,
                    new Messages.CreateChunk(fileResp.fileId(), 1).encode(), null, 5000));
            assertEquals(ErrorCode.NO_CAPACITY, e.code());
        }
    }

    @Test
    void placementToleratesHugeCapacityWeights() {
        for (String host : List.of("hugeA", "hugeB", "hugeC")) {
            UUID inc = UUID.randomUUID();
            client.call(Opcode.REGISTER_NODE,
                    new Messages.RegisterNode(inc.getMostSignificantBits(), inc.getLeastSignificantBits(),
                            List.of(host + ":9000"), "z1", "r1", host,
                            List.of(new Messages.StorageCapacity(Long.MAX_VALUE)), 1, 0).encode(),
                    null, 5000);
        }

        var fileResp = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                new Messages.CreateFile("test", "/huge").encode(), null, 5000));
        var chunkResp = Messages.CreateChunkResp.decode(client.call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(fileResp.fileId(), 1).encode(), null, 5000));

        assertEquals(3, chunkResp.replicas().size());
        assertEquals(3, chunkResp.replicas().stream().map(Messages.Replica::nodeId).collect(
                java.util.stream.Collectors.toSet()).size());
    }
}
