package io.strata.node;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import io.strata.proto.ScpClient;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Single-node data-plane test over the real wire (standalone mode, no metadata plane). */
class StorageNodeWireTest {

    @TempDir
    Path dir;

    private final ChunkId id = new ChunkId(FileId.random(), 0);

    @Test
    void failedServerBindLeavesDataDirReusable() throws Exception {
        Path nodeDir = dir.resolve("node");
        try (ServerSocket blocker = new ServerSocket(0)) {
            NodeConfig config = NodeConfig.standalone(nodeDir).withListenPort(blocker.getLocalPort());
            assertThrows(IOException.class, () -> new StorageNode(config));
        }

        try (StorageNode node = new StorageNode(NodeConfig.standalone(nodeDir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {
            Frame ping = client.callFrame(Opcode.PING, Messages.okHeader(),
                    ByteBuffer.wrap("ok".getBytes()), 5000);
            Resp.check(ping.headerSlice());
            assertEquals(2, ping.payloadLength());
        }
    }

    @Test
    void corruptIdentityFailsAsIOException() throws Exception {
        Files.writeString(dir.resolve("identity.properties"), "nodeId=1\nincarnation=not-a-uuid\n");

        IOException e = assertThrows(IOException.class, () -> new StorageNode(NodeConfig.standalone(dir)));
        assertTrue(e.getMessage().contains("identity"));
    }

    @Test
    void fullChunkLifecycleOverTheWire() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {

            // open
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, (byte) 0, (byte) 0, (byte) 0,
                    1 << 20, 1718000000000L).encode(), null, 5000);

            // pipelined appends
            byte[] a = "first-batch-".getBytes(), b = "second-batch".getBytes();
            CompletableFuture<Frame> f1 = client.send(Opcode.APPEND,
                    new Messages.Append(id, 1, 0, 0).encode(), ByteBuffer.wrap(a));
            CompletableFuture<Frame> f2 = client.send(Opcode.APPEND,
                    new Messages.Append(id, 1, a.length, 0).encode(), ByteBuffer.wrap(b));
            ByteBuffer h1 = f1.get().headerSlice();
            Resp.check(h1);
            assertEquals(a.length, Messages.AppendResp.decode(h1).endOffset());
            ByteBuffer h2 = f2.get().headerSlice();
            Resp.check(h2);
            assertEquals(a.length + b.length, Messages.AppendResp.decode(h2).endOffset());

            // read with payload
            Frame readFrame = client.callFrame(Opcode.READ,
                    new Messages.Read(id, 0, 1 << 20).encode(), null, 5000);
            ByteBuffer rh = readFrame.headerSlice();
            Resp.check(rh);
            var readResp = Messages.ReadResp.decode(rh);
            assertEquals(a.length + b.length, readResp.localEndOffset());
            byte[] got = new byte[readFrame.payloadLength()];
            readFrame.payloadSlice().get(got);
            assertArrayEquals("first-batch-second-batch".getBytes(), got);

            // ledger over the wire
            ByteBuffer lh = client.call(Opcode.READ_LEDGER, new Messages.ReadLedger(id, 0).encode(), null, 5000);
            var ledger = Messages.ReadLedgerResp.decode(lh);
            assertEquals(2, ledger.entries().size());
            assertEquals(a.length, ledger.entries().get(0).endOffset());

            // fence at 2 -> epoch-1 append rejected with typed error
            client.call(Opcode.FENCE, new Messages.Fence(id, 2).encode(), null, 5000);
            ScpException fenced = assertThrows(ScpException.class, () -> client.call(Opcode.APPEND,
                    new Messages.Append(id, 1, a.length + b.length, 0).encode(),
                    ByteBuffer.wrap("x".getBytes()), 5000));
            assertEquals(ErrorCode.FENCED_EPOCH, fenced.code());
            assertEquals(2, fenced.detail());

            // seal with the post-fence epoch
            ByteBuffer sh = client.call(Opcode.SEAL_CHUNK,
                    new Messages.SealChunk(id, 2, a.length + b.length).encode(), null, 5000);
            var sealResp = Messages.SealResp.decode(sh);
            assertEquals(a.length + b.length, sealResp.finalLength());

            // stat reflects sealed state
            ByteBuffer sth = client.call(Opcode.STAT_CHUNK, new Messages.StatChunk(id).encode(), null, 5000);
            var stat = Messages.StatResp.decode(sth);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(a.length + b.length, stat.sealedLength());

            // fetch whole file and delete
            Frame fetch = client.callFrame(Opcode.FETCH_CHUNK,
                    new Messages.FetchChunk(id, 0, Integer.MAX_VALUE).encode(), null, 5000);
            ByteBuffer fh = fetch.headerSlice();
            Resp.check(fh);
            assertEquals(Messages.FetchResp.decode(fh).fileLength(), fetch.payloadLength());

            ByteBuffer dh = client.call(Opcode.DELETE_CHUNKS,
                    new Messages.DeleteChunks(List.of(id)).encode(), null, 5000);
            var del = Messages.DeleteChunksResp.decode(dh);
            assertEquals((short) 0, del.codes().get(0));
        }
    }

    @Test
    void pingDrainingAndUnsupportedStorageOpcodesAreHandled() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {
            byte[] payload = "storage-ping".getBytes();
            Frame ping = client.callFrame(Opcode.PING, Messages.okHeader(), ByteBuffer.wrap(payload), 5000);
            Resp.check(ping.headerSlice());
            byte[] echoed = new byte[ping.payloadLength()];
            ping.payloadSlice().get(echoed);
            assertArrayEquals(payload, echoed);

            node.setDraining(true);
            ScpException draining = assertThrows(ScpException.class, () -> client.call(Opcode.OPEN_CHUNK,
                    new Messages.OpenChunk(new ChunkId(FileId.random(), 0), 1, (byte) 0, (byte) 0, (byte) 0,
                            1 << 20, 1L).encode(), null, 5000));
            assertEquals(ErrorCode.NO_CAPACITY, draining.code());

            ScpException metadataOpcode = assertThrows(ScpException.class,
                    () -> client.call(Opcode.CREATE_FILE, new byte[0], null, 5000));
            assertEquals(ErrorCode.UNKNOWN_OPCODE, metadataOpcode.code());

            NodeHandlers handlers = new NodeHandlers(node.store(), node);
            ScpException numeric = assertThrows(ScpException.class, () -> handlers.handle(
                    new Frame((short) 0x7FFF, (short) 1, (short) 0, 99,
                            ByteBuffer.allocate(0), ByteBuffer.allocate(0))));
            assertEquals(ErrorCode.UNKNOWN_OPCODE, numeric.code());
        }
    }

    @Test
    void sealChunkPassesPayloadFooterToStore() throws Exception {
        ChunkId chunk = new ChunkId(FileId.random(), 0);
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(chunk, 1, (byte) 0, (byte) 0, (byte) 0,
                    1 << 20, 1L).encode(), null, 5000);

            ScpException malformedFooter = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK,
                    new Messages.SealChunk(chunk, 1, 0).encode(), ByteBuffer.wrap(new byte[] {1}), 5000));
            assertEquals(ErrorCode.PRECONDITION_FAILED, malformedFooter.code());
        }
    }

    @Test
    void nodeRestartRecoversChunksOverTheWire() throws Exception {
        UUID incarnation;
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "t")) {
            incarnation = node.incarnation();
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, (byte) 0, (byte) 0, (byte) 0,
                    1 << 20, 1L).encode(), null, 5000);
            client.call(Opcode.APPEND, new Messages.Append(id, 1, 0, 0).encode(),
                    ByteBuffer.wrap("persistent".getBytes()), 5000);
        }
        try (StorageNode node2 = new StorageNode(NodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node2.port(), ScpClient.KIND_BROKER, "t")) {
            ByteBuffer sth = client.call(Opcode.STAT_CHUNK, new Messages.StatChunk(id).encode(), null, 5000);
            var stat = Messages.StatResp.decode(sth);
            assertEquals(ChunkState.OPEN, stat.state());
            assertEquals("persistent".length(), stat.localEndOffset());
            // identity survives restart (volume-bound)
            assertEquals(incarnation, node2.incarnation());
        }
    }

    @Test
    void repairFetchRejectsSourceThatAdvertisesOversizedFile() throws Exception {
        ChunkId repairChunk = new ChunkId(FileId.random(), 0);
        try (ScpServer source = new ScpServer(0, 1, 0, 0, req -> {
            if (Opcode.fromCode(req.opcode()) != Opcode.FETCH_CHUNK) {
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected opcode");
            }
            return ScpServer.ok(req,
                    new Messages.FetchResp(Long.MAX_VALUE, ChunkState.SEALED).encode(),
                    ByteBuffer.wrap(new byte[] {1}));
        });
             ScpClient src = new ScpClient("127.0.0.1", source.port(),
                     ScpClient.KIND_STORAGE_NODE, "repair-test")) {
            ControlLoop loop = new ControlLoop(null, null, null);
            var cmd = new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0, 4);

            ScpException e = assertThrows(ScpException.class, () -> loop.fetchWholeFile(src, cmd));
            assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
        }
    }

    @Test
    void repairFetchAcceptsValidSealedFileBytes() throws Exception {
        ChunkId repairChunk = new ChunkId(FileId.random(), 0);
        byte[] fileBytes = new byte[4096 + 4 + 64];
        for (int i = 0; i < fileBytes.length; i++) fileBytes[i] = (byte) i;
        try (ScpServer source = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new Messages.FetchResp(fileBytes.length, ChunkState.SEALED).encode(),
                ByteBuffer.wrap(fileBytes)));
             ScpClient src = new ScpClient("127.0.0.1", source.port(),
                     ScpClient.KIND_STORAGE_NODE, "repair-test")) {
            ControlLoop loop = new ControlLoop(null, null, null);
            var cmd = new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0, 4);

            assertArrayEquals(fileBytes, loop.fetchWholeFile(src, cmd));
        }
    }

    @Test
    void repairFetchRejectsMalformedSourceProgress() throws Exception {
        ChunkId repairChunk = new ChunkId(FileId.random(), 0);
        ControlLoop loop = new ControlLoop(null, null, null);
        var cmd = new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0, 4);

        try (ScpServer openSource = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new Messages.FetchResp(4096 + 4 + 64, ChunkState.OPEN).encode(),
                ByteBuffer.wrap(new byte[] {1})));
             ScpClient src = new ScpClient("127.0.0.1", openSource.port(),
                     ScpClient.KIND_STORAGE_NODE, "repair-test")) {
            assertEquals(ErrorCode.INTERNAL,
                    assertThrows(ScpException.class, () -> loop.fetchWholeFile(src, cmd)).code());
        }

        try (ScpServer malformedHeader = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new byte[] {0}, ByteBuffer.wrap(new byte[] {1})));
             ScpClient src = new ScpClient("127.0.0.1", malformedHeader.port(),
                     ScpClient.KIND_STORAGE_NODE, "repair-test")) {
            assertEquals(ErrorCode.CORRUPT_CHUNK,
                    assertThrows(ScpException.class, () -> loop.fetchWholeFile(src, cmd)).code());
        }

        AtomicInteger calls = new AtomicInteger();
        try (ScpServer changingLength = new ScpServer(0, 1, 0, 0, req -> {
            int call = calls.incrementAndGet();
            long len = call == 1 ? 4096 + 4 + 64 : 4096 + 5 + 64;
            return ScpServer.ok(req, new Messages.FetchResp(len, ChunkState.SEALED).encode(),
                    ByteBuffer.wrap(new byte[] {1}));
        });
             ScpClient src = new ScpClient("127.0.0.1", changingLength.port(),
                     ScpClient.KIND_STORAGE_NODE, "repair-test")) {
            assertEquals(ErrorCode.CORRUPT_CHUNK,
                    assertThrows(ScpException.class, () -> loop.fetchWholeFile(src, cmd)).code());
        }

        try (ScpServer pastEof = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new Messages.FetchResp(4096 + 64, ChunkState.SEALED).encode(),
                ByteBuffer.wrap(new byte[4096 + 65])));
             ScpClient src = new ScpClient("127.0.0.1", pastEof.port(),
                     ScpClient.KIND_STORAGE_NODE, "repair-test")) {
            assertEquals(ErrorCode.CORRUPT_CHUNK,
                    assertThrows(ScpException.class, () -> loop.fetchWholeFile(src, cmd)).code());
        }

        byte[] overFetchLimit = new byte[(4 * 1024 * 1024) + 1];
        try (ScpServer ignoresRequestLimit = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new Messages.FetchResp(4096L + overFetchLimit.length + 64, ChunkState.SEALED).encode(),
                ByteBuffer.wrap(overFetchLimit)));
             ScpClient src = new ScpClient("127.0.0.1", ignoresRequestLimit.port(),
                     ScpClient.KIND_STORAGE_NODE, "repair-test")) {
            assertEquals(ErrorCode.CORRUPT_CHUNK,
                    assertThrows(ScpException.class, () -> loop.fetchWholeFile(src, cmd)).code());
        }

        try (ScpServer shortFetch = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new Messages.FetchResp(4096 + 4 + 64, ChunkState.SEALED).encode(),
                ByteBuffer.wrap(new byte[0])));
             ScpClient src = new ScpClient("127.0.0.1", shortFetch.port(),
                     ScpClient.KIND_STORAGE_NODE, "repair-test")) {
            assertEquals(ErrorCode.INTERNAL,
                    assertThrows(ScpException.class, () -> loop.fetchWholeFile(src, cmd)).code());
        }
    }

    @Test
    void repairFetchRejectsInvalidExpectedLengthsBeforeNetworkUse() {
        ControlLoop loop = new ControlLoop(null, null, null);
        ChunkId repairChunk = new ChunkId(FileId.random(), 0);

        assertEquals(ErrorCode.CORRUPT_CHUNK,
                assertThrows(ScpException.class, () -> loop.fetchWholeFile(null,
                        new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0, -1))).code());
        assertEquals(ErrorCode.CORRUPT_CHUNK,
                assertThrows(ScpException.class, () -> loop.fetchWholeFile(null,
                        new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0,
                                Long.MAX_VALUE))).code());
        assertEquals(ErrorCode.INTERNAL,
                assertThrows(ScpException.class, () -> loop.fetchWholeFile(null,
                        new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0,
                                Integer.MAX_VALUE))).code());
    }
}
