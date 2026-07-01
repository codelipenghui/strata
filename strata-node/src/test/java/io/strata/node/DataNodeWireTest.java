package io.strata.node;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.format.ChunkFormats;
import io.strata.format.ChunkStore;
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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Single-node data-plane test over the real wire (standalone mode, no metadata plane). */
class DataNodeWireTest {

    @TempDir
    Path dir;

    private final ChunkId id = new ChunkId(FileId.of(1), 0);
    private static final StrataNamespace TEST_NS = StrataNamespace.of("test");

    @Test
    void failedServerBindLeavesDataDirReusable() throws Exception {
        Path nodeDir = dir.resolve("node");
        try (ServerSocket blocker = new ServerSocket(0)) {
            DataNodeConfig config = DataNodeConfig.standalone(nodeDir).withListenPort(blocker.getLocalPort());
            assertThrows(IOException.class, () -> new DataNode(config));
        }

        try (DataNode node = new DataNode(DataNodeConfig.standalone(nodeDir));
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

        IOException e = assertThrows(IOException.class, () -> new DataNode(DataNodeConfig.standalone(dir)));
        assertTrue(e.getMessage().contains("identity"));
    }

    @Test
    void identityFileMustContainBothNodeIdAndIncarnation() throws Exception {
        Files.writeString(dir.resolve("identity.properties"), "nodeId=1\n");

        IOException e = assertThrows(IOException.class, () -> new DataNode(DataNodeConfig.standalone(dir)));
        assertTrue(e.getMessage().contains("missing nodeId or incarnation"));
    }

    @Test
    void identityRejectsNodeIdsBelowUnassignedSentinel() throws Exception {
        Files.writeString(dir.resolve("identity.properties"),
                "nodeId=-2\nincarnation=" + UUID.randomUUID() + "\n");

        IOException e = assertThrows(IOException.class, () -> new DataNode(DataNodeConfig.standalone(dir)));
        assertTrue(e.getMessage().contains("invalid data node identity"));
    }

    @Test
    void standaloneNodeIdentityAndAdvertisedEndpointAreVolumeBound() throws Exception {
        DataNodeConfig config = DataNodeConfig.standalone(dir).withAdvertisedEndpoint("published-node:19000");
        UUID incarnation;
        try (DataNode node = new DataNode(config)) {
            assertEquals("published-node:19000", node.endpoint());
            assertEquals(-1, node.nodeId(), "standalone volume has no externally-supplied id");
            assertFalse(node.isDraining());
            assertEquals(config, node.config());
            assertNotNull(node.store());
            incarnation = node.incarnation();
        }

        // the standalone identity (id -1, incarnation) is volume-bound and survives a reopen
        try (DataNode reopened = new DataNode(DataNodeConfig.standalone(dir))) {
            assertEquals(-1, reopened.nodeId());
            assertEquals(incarnation, reopened.incarnation());
        }
    }

    @Test
    void configuredNodeIdIsPersistedAndVolumeBound() throws Exception {
        // A fresh volume started with STRATA_NODE_ID=42 persists (42, incarnation)...
        UUID incarnation;
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir).withNodeId(42))) {
            assertEquals(42, node.nodeId());
            incarnation = node.incarnation();
        }

        // ...and reopening with the same id resolves to the same id AND the same incarnation.
        try (DataNode reopened = new DataNode(DataNodeConfig.standalone(dir).withNodeId(42))) {
            assertEquals(42, reopened.nodeId());
            assertEquals(incarnation, reopened.incarnation(),
                    "incarnation is minted once and stays volume-bound across restarts");
        }
    }

    @Test
    void reopeningVolumeWithDifferentConfiguredNodeIdRefusesToStart() throws Exception {
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir).withNodeId(42))) {
            assertEquals(42, node.nodeId());
        }

        // A configured id that disagrees with the volume's recorded id would let this process
        // impersonate another node — the constructor must refuse to start.
        IOException e = assertThrows(IOException.class,
                () -> new DataNode(DataNodeConfig.standalone(dir).withNodeId(43)));
        assertTrue(e.getMessage().contains("does not match this volume's recorded node id"));
    }

    @Test
    void appendStoresTheClientFramePayloadCrc() throws Exception {
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, false,
                    1 << 20, 1718000000000L, TEST_NS).encode(), null, 5000);

            byte[] payload = "client-computed".getBytes();
            client.call(Opcode.APPEND, new Messages.Append(id, 1, 0, 0, TEST_NS).encode(),
                    ByteBuffer.wrap(payload), 5000);

            ByteBuffer lh = client.call(Opcode.READ_LEDGER,
                    new Messages.ReadLedger(id, 0, TEST_NS).encode(), null, 5000);
            var ledger = Messages.ReadLedgerResp.decode(lh);
            assertEquals(1, ledger.entries().size());
            assertEquals(io.strata.common.Crc.of(ByteBuffer.wrap(payload)),
                    ledger.entries().get(0).payloadCrc());
        }
    }

    @Test
    void nonEmptyAppendWithoutPayloadCrcFlagIsRejected() throws Exception {
        // The per-record digest is writer-origin: a non-empty append must carry the client's payload CRC
        // (FLAG_PAYLOAD_CRC), which the node stores verbatim as the ledger digest. The wire encoder always
        // sets that flag for a non-empty payload, so a frame lacking it is a malformed/non-conforming
        // client; the node must reject it rather than silently store a 0 digest that defeats torn-tail
        // recovery. Frame.request leaves flags=0, reproducing exactly that case at the handler boundary.
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ChunkStore store = new ChunkStore(dir.resolve("guard-chunks"))) {
            DataNodeHandlers handlers = dataNodeHandlers(store, node);
            Frame malformed = Frame.request(Opcode.APPEND,
                    new Messages.Append(id, 1, 0, 0, TEST_NS).encode(),
                    ByteBuffer.wrap("no-crc-flag".getBytes()), 7L);
            ScpException e = assertThrows(ScpException.class, () -> handlers.handleAsync(malformed));
            assertEquals(ErrorCode.PRECONDITION_FAILED, e.code());
        }
    }

    @Test
    void fullChunkLifecycleOverTheWire() throws Exception {
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {

            // open
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, false,
                    1 << 20, 1718000000000L, TEST_NS).encode(), null, 5000);

            // pipelined appends
            byte[] a = "first-batch-".getBytes(), b = "second-batch".getBytes();
            CompletableFuture<Frame> f1 = client.send(Opcode.APPEND,
                    new Messages.Append(id, 1, 0, 0, TEST_NS).encode(), ByteBuffer.wrap(a));
            CompletableFuture<Frame> f2 = client.send(Opcode.APPEND,
                    new Messages.Append(id, 1, a.length, 0, TEST_NS).encode(), ByteBuffer.wrap(b));
            ByteBuffer h1 = f1.get().headerSlice();
            Resp.check(h1);
            assertEquals(a.length, Messages.AppendResp.decode(h1).endOffset());
            ByteBuffer h2 = f2.get().headerSlice();
            Resp.check(h2);
            assertEquals(a.length + b.length, Messages.AppendResp.decode(h2).endOffset());
            ByteBuffer h3 = client.call(Opcode.APPEND,
                    new Messages.Append(id, 1, a.length + b.length, a.length + b.length, TEST_NS).encode(),
                    ByteBuffer.allocate(0), 5000);
            assertEquals(a.length + b.length, Messages.AppendResp.decode(h3).endOffset());

            // read with payload
            Frame readFrame = client.callFrame(Opcode.READ,
                    new Messages.Read(id, 0, 1 << 20, TEST_NS).encode(), null, 5000);
            ByteBuffer rh = readFrame.headerSlice();
            Resp.check(rh);
            var readResp = Messages.ReadResp.decode(rh);
            assertEquals(a.length + b.length, readResp.localEndOffset());
            byte[] got = new byte[readFrame.payloadLength()];
            readFrame.payloadSlice().get(got);
            assertArrayEquals("first-batch-second-batch".getBytes(), got);

            // ledger over the wire
            ByteBuffer lh = client.call(Opcode.READ_LEDGER, new Messages.ReadLedger(id, 0, TEST_NS).encode(), null, 5000);
            var ledger = Messages.ReadLedgerResp.decode(lh);
            assertEquals(2, ledger.entries().size());
            assertEquals(a.length, ledger.entries().get(0).endOffset());

            // fence at 2 -> epoch-1 append rejected with typed error
            client.call(Opcode.FENCE, new Messages.Fence(id, 2, TEST_NS).encode(), null, 5000);
            ScpException fenced = assertThrows(ScpException.class, () -> client.call(Opcode.APPEND,
                    new Messages.Append(id, 1, a.length + b.length, 0, TEST_NS).encode(),
                    ByteBuffer.wrap("x".getBytes()), 5000));
            assertEquals(ErrorCode.FENCED_EPOCH, fenced.code());
            assertEquals(2, fenced.detail());

            // seal with the post-fence epoch
            ByteBuffer sh = client.call(Opcode.SEAL_CHUNK,
                    new Messages.SealChunk(id, 2, a.length + b.length, TEST_NS).encode(), null, 5000);
            var sealResp = Messages.SealResp.decode(sh);
            assertEquals(a.length + b.length, sealResp.finalLength());

            // stat reflects sealed state
            ByteBuffer sth = client.call(Opcode.STAT_CHUNK, new Messages.StatChunk(id, TEST_NS).encode(), null, 5000);
            var stat = Messages.StatResp.decode(sth);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(a.length + b.length, stat.sealedLength());

            // fetch whole file and delete
            Frame fetch = client.callFrame(Opcode.FETCH_CHUNK,
                    new Messages.FetchChunk(id, 0, Integer.MAX_VALUE, TEST_NS).encode(), null, 5000);
            ByteBuffer fh = fetch.headerSlice();
            Resp.check(fh);
            assertEquals(Messages.FetchResp.decode(fh).fileLength(), fetch.payloadLength());

            ByteBuffer dh = client.call(Opcode.DELETE_CHUNKS,
                    new Messages.DeleteChunks(List.of(id), TEST_NS).encode(), null, 5000);
            var del = Messages.DeleteChunksResp.decode(dh);
            assertEquals((short) 0, del.codes().get(0));
        }
    }

    @Test
    void openReadIsClampedToReplicaDurableHighWatermark() throws Exception {
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, false,
                    1 << 20, 1718000000000L, TEST_NS).encode(), null, 5000);
            client.call(Opcode.APPEND, new Messages.Append(id, 1, 0, 0, TEST_NS).encode(),
                    ByteBuffer.wrap("SAFE".getBytes()), 5000);
            client.call(Opcode.APPEND, new Messages.Append(id, 1, 4, 4, TEST_NS).encode(),
                    ByteBuffer.wrap("TAIL".getBytes()), 5000);

            Frame prefix = client.callFrame(Opcode.READ,
                    new Messages.Read(id, 0, 1024, TEST_NS).encode(), null, 5000);
            ByteBuffer header = prefix.headerSlice();
            Resp.check(header);
            var resp = Messages.ReadResp.decode(header);
            assertEquals(8, resp.localEndOffset());
            assertEquals(4, resp.durableOffset());
            byte[] got = new byte[prefix.payloadLength()];
            prefix.payloadSlice().get(got);
            assertArrayEquals("SAFE".getBytes(), got);

            Frame tail = client.callFrame(Opcode.READ,
                    new Messages.Read(id, 4, 1024, TEST_NS).encode(), null, 5000);
            ByteBuffer tailHeader = tail.headerSlice();
            Resp.check(tailHeader);
            var tailResp = Messages.ReadResp.decode(tailHeader);
            assertEquals(8, tailResp.localEndOffset());
            assertEquals(4, tailResp.durableOffset());
            assertEquals(0, tail.payloadLength());
        }
    }

    @Test
    void recoveryReadServesTheUndurableTailThatClientReadsHide() throws Exception {
        // Seal recovery must re-read the never-acked tail above the durable high watermark to decide
        // whether a quorum still holds it (tech design §7.3). The client READ path clamps that tail
        // away; READ_RECOVERY must expose it (up to localEnd) so recovery does not seal short.
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_TOOL, "recovery")) {
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, false,
                    1 << 20, 1718000000000L, TEST_NS).encode(), null, 5000);
            client.call(Opcode.APPEND, new Messages.Append(id, 1, 0, 0, TEST_NS).encode(),
                    ByteBuffer.wrap("SAFE".getBytes()), 5000);
            client.call(Opcode.APPEND, new Messages.Append(id, 1, 4, 4, TEST_NS).encode(),
                    ByteBuffer.wrap("TAIL".getBytes()), 5000);

            // recovery reads the un-acked tail [4,8) that the clamped client READ refuses to serve
            Frame tail = client.callFrame(Opcode.READ_RECOVERY,
                    new Messages.Read(id, 4, 1024, TEST_NS).encode(), null, 5000);
            ByteBuffer tailHeader = tail.headerSlice();
            Resp.check(tailHeader);
            var tailResp = Messages.ReadResp.decode(tailHeader);
            assertEquals(8, tailResp.localEndOffset());
            assertEquals(4, tailResp.durableOffset());
            byte[] got = new byte[tail.payloadLength()];
            tail.payloadSlice().get(got);
            assertArrayEquals("TAIL".getBytes(), got);

            // and the full range is served from offset 0, verified against the integrity ledger
            Frame full = client.callFrame(Opcode.READ_RECOVERY,
                    new Messages.Read(id, 0, 1024, TEST_NS).encode(), null, 5000);
            Resp.check(full.headerSlice());
            byte[] all = new byte[full.payloadLength()];
            full.payloadSlice().get(all);
            assertArrayEquals("SAFETAIL".getBytes(), all);
        }
    }

    @Test
    void sealedReadRegionIsZeroCopyAndUnverified() throws Exception {
        // Trade-off (sealed reads reverted to zero-copy for throughput): the fast client read path
        // (readRegion / wire READ) hands back a zero-copy region and does NOT re-CRC per read — a
        // corrupt sealed data region is served as-is. Integrity is caught by the verified read() /
        // fetch() / import paths and background scrub, not at read time.
        byte[] payload = "sealed-payload".getBytes();
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, false,
                    1 << 20, 1718000000000L, TEST_NS).encode(), null, 5000);
            client.call(Opcode.APPEND, new Messages.Append(id, 1, 0, 0, TEST_NS).encode(),
                    ByteBuffer.wrap(payload), 5000);
            client.call(Opcode.SEAL_CHUNK, new Messages.SealChunk(id, 1, payload.length, TEST_NS).encode(), null, 5000);

            corruptChunkDataByte(dir.resolve("chunks"), id, 3);

            // the verified materialize path still rejects corruption at read time
            assertEquals(ErrorCode.CRC_MISMATCH,
                    assertThrows(ScpException.class, () -> node.store().read(TEST_NS, id, 0, payload.length)).code());

            // zero-copy readRegion does NOT verify — it returns a channel region over the bytes
            try (var region = node.store().readRegion(TEST_NS, id, 0, payload.length)) {
                assertEquals(payload.length, region.length());
                assertNotNull(region.channel(), "sealed readRegion must be a zero-copy channel region");
            }

            // the wire READ (zero-copy) succeeds without a read-time CRC error
            Frame frame = client.callFrame(Opcode.READ,
                    new Messages.Read(id, 0, payload.length, TEST_NS).encode(), null, 5000);
            Resp.check(frame.headerSlice());
        }
    }

    @Test
    void pingDrainingAndUnsupportedDataNodeOpcodesAreHandled() throws Exception {
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {
            byte[] payload = "data-node-ping".getBytes();
            Frame ping = client.callFrame(Opcode.PING, Messages.okHeader(), ByteBuffer.wrap(payload), 5000);
            Resp.check(ping.headerSlice());
            byte[] echoed = new byte[ping.payloadLength()];
            ping.payloadSlice().get(echoed);
            assertArrayEquals(payload, echoed);

            node.setDraining(true);
            ScpException draining = assertThrows(ScpException.class, () -> client.call(Opcode.OPEN_CHUNK,
                    new Messages.OpenChunk(new ChunkId(FileId.of(2), 0), 1, false,
                            1 << 20, 1L, TEST_NS).encode(), null, 5000));
            assertEquals(ErrorCode.NO_CAPACITY, draining.code());

            ScpException metadataOpcode = assertThrows(ScpException.class,
                    () -> client.call(Opcode.CREATE_FILE, new byte[0], null, 5000));
            assertEquals(ErrorCode.UNKNOWN_OPCODE, metadataOpcode.code());

            DataNodeHandlers handlers = dataNodeHandlers(node.store(), node);
            ScpException numeric = assertThrows(ScpException.class, () -> handlers.handle(
                    new Frame((short) 0x7FFF, (short) 1, (short) 0, 99,
                            ByteBuffer.allocate(0), ByteBuffer.allocate(0))));
            assertEquals(ErrorCode.UNKNOWN_OPCODE, numeric.code());
        }
    }

    @Test
    void sealChunkPassesPayloadFooterToStore() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(3), 0);
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(chunk, 1, false,
                    1 << 20, 1L, TEST_NS).encode(), null, 5000);

            ScpException malformedFooter = assertThrows(ScpException.class, () -> client.call(Opcode.SEAL_CHUNK,
                    new Messages.SealChunk(chunk, 1, 0, TEST_NS).encode(), ByteBuffer.wrap(new byte[] {1}), 5000));
            assertEquals(ErrorCode.PRECONDITION_FAILED, malformedFooter.code());
        }
    }

    @Test
    void nodeRestartRecoversChunksOverTheWire() throws Exception {
        UUID incarnation;
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "t")) {
            incarnation = node.incarnation();
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, false,
                    1 << 20, 1L, TEST_NS).encode(), null, 5000);
            client.call(Opcode.APPEND, new Messages.Append(id, 1, 0, 0, TEST_NS).encode(),
                    ByteBuffer.wrap("persistent".getBytes()), 5000);
        }
        try (DataNode node2 = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node2.port(), ScpClient.KIND_BROKER, "t")) {
            ByteBuffer sth = client.call(Opcode.STAT_CHUNK, new Messages.StatChunk(id, TEST_NS).encode(), null, 5000);
            var stat = Messages.StatResp.decode(sth);
            assertEquals(ChunkState.OPEN, stat.state());
            assertEquals("persistent".length(), stat.localEndOffset());
            // identity survives restart (volume-bound)
            assertEquals(incarnation, node2.incarnation());
        }
    }

    @Test
    void repairFetchRejectsSourceThatAdvertisesOversizedFile() throws Exception {
        ChunkId repairChunk = new ChunkId(FileId.of(4), 0);
        try (ScpServer source = new ScpServer(0, 1, 0, 0, req -> {
            if (Opcode.fromCode(req.opcode()) != Opcode.FETCH_CHUNK) {
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected opcode");
            }
            return ScpServer.ok(req,
                    new Messages.FetchResp(Long.MAX_VALUE, ChunkState.SEALED).encode(),
                    ByteBuffer.wrap(new byte[] {1}));
        });
             ScpClient src = new ScpClient("127.0.0.1", source.port(),
                     ScpClient.KIND_DATA_NODE, "repair-test");
             ChunkStore store = new ChunkStore(dir.resolve("oversized-repair-store"))) {
            ControlLoop loop = controlLoop(null, DataNodeConfig.standalone(dir), store);
            var cmd = new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0, 4, TEST_NS);
            Path output = dir.resolve("oversized-repair-fetch.chunk");

            ScpException e = assertThrows(ScpException.class, () -> loop.fetchWholeFile(src, cmd, output));
            assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
        }
    }

    @Test
    void repairFetchAcceptsValidSealedFileBytes() throws Exception {
        ChunkId repairChunk = new ChunkId(FileId.of(5), 0);
        byte[] fileBytes = new byte[4096 + 4 + 64];
        for (int i = 0; i < fileBytes.length; i++) fileBytes[i] = (byte) i;
        try (ScpServer source = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new Messages.FetchResp(fileBytes.length, ChunkState.SEALED).encode(),
                ByteBuffer.wrap(fileBytes)));
             ScpClient src = new ScpClient("127.0.0.1", source.port(),
                     ScpClient.KIND_DATA_NODE, "repair-test");
             ChunkStore store = new ChunkStore(dir.resolve("valid-repair-store"))) {
            ControlLoop loop = controlLoop(null, DataNodeConfig.standalone(dir), store);
            var cmd = new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0, 4, TEST_NS);
            Path output = dir.resolve("valid-repair-fetch.chunk");

            assertEquals(fileBytes.length, loop.fetchWholeFile(src, cmd, output));
            assertArrayEquals(fileBytes, Files.readAllBytes(output));
        }
    }

    @Test
    void repairFetchRejectsMalformedSourceProgress() throws Exception {
        ChunkId repairChunk = new ChunkId(FileId.of(6), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("malformed-source-store"))) {
            ControlLoop loop = controlLoop(null, DataNodeConfig.standalone(dir), store);
            var cmd = new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0, 4, TEST_NS);

            try (ScpServer openSource = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new Messages.FetchResp(4096 + 4 + 64, ChunkState.OPEN).encode(),
                ByteBuffer.wrap(new byte[] {1})));
                 ScpClient src = new ScpClient("127.0.0.1", openSource.port(),
                         ScpClient.KIND_DATA_NODE, "repair-test")) {
                assertEquals(ErrorCode.INTERNAL,
                        assertThrows(ScpException.class,
                                () -> loop.fetchWholeFile(src, cmd, dir.resolve("open-source-fetch.chunk"))).code());
            }

            try (ScpServer malformedHeader = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new byte[] {0}, ByteBuffer.wrap(new byte[] {1})));
                 ScpClient src = new ScpClient("127.0.0.1", malformedHeader.port(),
                         ScpClient.KIND_DATA_NODE, "repair-test")) {
                assertEquals(ErrorCode.CORRUPT_CHUNK,
                        assertThrows(ScpException.class,
                                () -> loop.fetchWholeFile(src, cmd, dir.resolve("malformed-source-fetch.chunk"))).code());
            }

            AtomicInteger calls = new AtomicInteger();
            try (ScpServer changingLength = new ScpServer(0, 1, 0, 0, req -> {
                int call = calls.incrementAndGet();
                long len = call == 1 ? 4096 + 4 + 64 : 4096 + 5 + 64;
                return ScpServer.ok(req, new Messages.FetchResp(len, ChunkState.SEALED).encode(),
                        ByteBuffer.wrap(new byte[] {1}));
            });
                 ScpClient src = new ScpClient("127.0.0.1", changingLength.port(),
                         ScpClient.KIND_DATA_NODE, "repair-test")) {
                assertEquals(ErrorCode.CORRUPT_CHUNK,
                        assertThrows(ScpException.class,
                                () -> loop.fetchWholeFile(src, cmd, dir.resolve("changing-source-fetch.chunk"))).code());
            }

            try (ScpServer pastEof = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new Messages.FetchResp(4096 + 64, ChunkState.SEALED).encode(),
                ByteBuffer.wrap(new byte[4096 + 65])));
                 ScpClient src = new ScpClient("127.0.0.1", pastEof.port(),
                         ScpClient.KIND_DATA_NODE, "repair-test")) {
                assertEquals(ErrorCode.CORRUPT_CHUNK,
                        assertThrows(ScpException.class,
                                () -> loop.fetchWholeFile(src, cmd, dir.resolve("past-eof-fetch.chunk"))).code());
            }

            byte[] overFetchLimit = new byte[(4 * 1024 * 1024) + 1];
            try (ScpServer ignoresRequestLimit = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new Messages.FetchResp(4096L + overFetchLimit.length + 64, ChunkState.SEALED).encode(),
                ByteBuffer.wrap(overFetchLimit)));
                 ScpClient src = new ScpClient("127.0.0.1", ignoresRequestLimit.port(),
                         ScpClient.KIND_DATA_NODE, "repair-test")) {
                assertEquals(ErrorCode.CORRUPT_CHUNK,
                        assertThrows(ScpException.class,
                                () -> loop.fetchWholeFile(src, cmd, dir.resolve("over-limit-fetch.chunk"))).code());
            }

            try (ScpServer shortFetch = new ScpServer(0, 1, 0, 0, req -> ScpServer.ok(req,
                new Messages.FetchResp(4096 + 4 + 64, ChunkState.SEALED).encode(),
                ByteBuffer.wrap(new byte[0])));
                 ScpClient src = new ScpClient("127.0.0.1", shortFetch.port(),
                         ScpClient.KIND_DATA_NODE, "repair-test")) {
                assertEquals(ErrorCode.INTERNAL,
                        assertThrows(ScpException.class,
                                () -> loop.fetchWholeFile(src, cmd, dir.resolve("short-fetch.chunk"))).code());
            }
        }
    }

    @Test
    void repairFetchRejectsInvalidExpectedLengthsBeforeNetworkUse() throws Exception {
        ChunkId repairChunk = new ChunkId(FileId.of(7), 0);
        Path output = dir.resolve("invalid-expected-length-fetch.chunk");
        try (ChunkStore store = new ChunkStore(dir.resolve("invalid-length-store"))) {
            ControlLoop loop = controlLoop(null, DataNodeConfig.standalone(dir), store);

            assertEquals(ErrorCode.CORRUPT_CHUNK,
                    assertThrows(ScpException.class, () -> loop.fetchWholeFile(null,
                            new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0, -1, TEST_NS), output)).code());
            assertEquals(ErrorCode.CORRUPT_CHUNK,
                    assertThrows(ScpException.class, () -> loop.fetchWholeFile(null,
                            new Messages.ReplicateCmd(1, repairChunk, List.of(), (byte) 1, 0,
                                    Long.MAX_VALUE, TEST_NS), output)).code());
        }
    }

    private static DataNodeHandlers dataNodeHandlers(ChunkStore store, DataNode node) {
        return new DataNodeHandlers(store, node, new ChunkDeleteService(store, 1, 0));
    }

    private static ControlLoop controlLoop(DataNode node, DataNodeConfig config, ChunkStore store) {
        return new ControlLoop(node, config, store, new ChunkDeleteService(store, 1, 0));
    }

    private static void corruptChunkDataByte(Path dir, ChunkId chunkId, long dataOffset) throws IOException {
        Path dataPath = dir.resolve(ChunkFormats.chunkRelativePath(TEST_NS, chunkId) + ".chunk");
        try (FileChannel channel = FileChannel.open(dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            channel.read(one, ChunkFormats.DATA_START + dataOffset);
            one.flip();
            byte corrupted = (byte) (one.get(0) ^ 0x01);
            channel.write(ByteBuffer.wrap(new byte[] {corrupted}), ChunkFormats.DATA_START + dataOffset);
        }
    }
}
