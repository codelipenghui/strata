package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Executable SCP v0 compatibility fixtures. The frozen hex below represents the peer side of the
 * protocol; these tests intentionally avoid FrameIO.write for scripted peer output so current
 * client/server code is tested against independently materialized v0 frames.
 */
class ScpV0CompatibilityTest {
    private static final FileId FILE_ID = FileId.fromHex("1111111122223333");
    private static final ChunkId CHUNK_ID = new ChunkId(FILE_ID, 3);
    private static final long OP_ID_MSB = 0x0123456789abcdefL;
    private static final long OP_ID_LSB = 0xfedcba9876543210L;
    private static final StrataNamespace NS = StrataNamespace.of("test");

    private static final String OK_RESPONSE = "000000";
    private static final String EMPTY_REQUEST = "00";
    private static final String HELLO_REQUEST_FROM_V0_CLIENT =
            "000100010110203040506070800d676f6c64656e2d636c69656e7400";
    private static final String HELLO_REQUEST_FROM_CURRENT_CLIENT =
            "000100010100000000000000000d676f6c64656e2d636c69656e7400";
    private static final String HELLO_RESPONSE_FROM_V0_SERVER =
            "0000000101020304050607080000002a11112222333344445555666677778888"
                    + "040000000000000040000000170001000100100001001100010012"
                    + "00010013000100140001001500010016000100170001001800010019"
                    + "00010101000101020001010300010201000102020001020300010204"
                    + "0001020500010206000102070001020800010209000100";

    private static final byte[] APPEND_PAYLOAD = "append-v0".getBytes(StandardCharsets.UTF_8);
    private static final byte[] READ_PAYLOAD = "read-v0".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FETCH_PAYLOAD = "fetch-v0-image".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PING_PAYLOAD = "ping-v0".getBytes(StandardCharsets.UTF_8);

    @Test
    void currentServerAcceptsFrozenV0ClientFrames() throws Exception {
        ArrayDeque<V0Exchange> expected = new ArrayDeque<>(exchanges());
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();

        try (ScpServer server = new ScpServer(0, 42, 0x1111222233334444L, 0x5555666677778888L, req -> {
            try {
                V0Exchange exchange = expected.removeFirst();
                exchange.assertRequest(req);
                return ScpServer.ok(req, exchange.responseHeader(), exchange.responsePayloadBuffer());
            } catch (Throwable t) {
                serverFailure.compareAndSet(null, t);
                return Frame.response(req, Resp.error(ErrorCode.INTERNAL, t.toString(), 0), null);
            }
        });
             Socket socket = new Socket("127.0.0.1", server.port());
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            socket.setSoTimeout(5_000);

            writeV0Frame(out, Opcode.HELLO, false, 0, HELLO_REQUEST_FROM_V0_CLIENT, null);
            RawFrame hello = readV0Frame(in);
            assertEquals(Opcode.HELLO.code, hello.opcode());
            assertTrue(hello.isResponse());
            ByteBuffer helloHeader = ByteBuffer.wrap(hello.header());
            Resp.check(helloHeader);
            Messages.HelloResp helloResp = Messages.HelloResp.decode(helloHeader);
            assertEquals(42, helloResp.nodeId());
            assertEquals(0x1111222233334444L, helloResp.incMsb());
            assertEquals(0x5555666677778888L, helloResp.incLsb());

            long correlation = 1;
            for (V0Exchange exchange : exchanges()) {
                writeV0Frame(out, exchange.opcode(), false, correlation, exchange.requestHeaderHex(),
                        exchange.requestPayload());
                RawFrame response = readV0Frame(in);
                assertEquals(exchange.opcode().code, response.opcode(), exchange.name());
                assertEquals(correlation, response.correlationId(), exchange.name());
                assertTrue(response.isResponse(), exchange.name());
                assertEquals(exchange.responseHeaderHex(), hex(response.header()), exchange.name());
                assertArrayEquals(exchange.responsePayload(), response.payload(), exchange.name());
                exchange.assertResponse(ByteBuffer.wrap(response.header()), ByteBuffer.wrap(response.payload()));
                correlation++;
            }

            assertNull(serverFailure.get());
            assertTrue(expected.isEmpty());
        }
    }

    @Test
    void currentClientAcceptsFrozenV0ServerFrames() throws Exception {
        AtomicReference<Throwable> scriptFailure = new AtomicReference<>();
        Thread serverThread;
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            serverSocket.setSoTimeout(5_000);
            serverThread = Thread.ofVirtual().name("scp-v0-scripted-server").start(
                    () -> runFrozenV0Server(serverSocket, scriptFailure));

            try (ScpClient client = new ScpClient("127.0.0.1", serverSocket.getLocalPort(),
                    ScpClient.KIND_BROKER, "golden-client")) {
                assertEquals(42, client.serverHello().nodeId());
                assertEquals(0x0102030405060708L, client.serverHello().featureBits());
                assertEquals(0x1111222233334444L, client.serverHello().incMsb());
                assertEquals(0x5555666677778888L, client.serverHello().incLsb());

                for (V0Exchange exchange : exchanges()) {
                    Frame response = client.callFrame(exchange.opcode(), exchange.requestHeader(),
                            exchange.requestPayloadBuffer(), 5_000);
                    assertEquals(exchange.opcode().code, response.opcode(), exchange.name());
                    assertTrue(response.isResponse(), exchange.name());
                    assertEquals(exchange.responseHeaderHex(), hex(response.headerSlice()), exchange.name());
                    assertArrayEquals(exchange.responsePayload(), bytes(response.payloadSlice()), exchange.name());
                    exchange.assertResponse(response.headerSlice(), response.payloadSlice());
                }
            }
        }

        serverThread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(serverThread.isAlive(), "scripted v0 server did not finish");
        assertNull(scriptFailure.get());
    }

    private static void runFrozenV0Server(ServerSocket serverSocket, AtomicReference<Throwable> failure) {
        try (Socket socket = serverSocket.accept();
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            socket.setSoTimeout(5_000);

            RawFrame hello = readV0Frame(in);
            check(Opcode.HELLO.code == hello.opcode(), "expected HELLO");
            check(!hello.isResponse(), "HELLO must be a request");
            check(HELLO_REQUEST_FROM_CURRENT_CLIENT.equals(hex(hello.header())),
                    "unexpected current-client HELLO: " + hex(hello.header()));
            Messages.Hello decodedHello = Messages.Hello.decode(ByteBuffer.wrap(hello.header()));
            check(new Messages.Hello(ScpClient.KIND_BROKER, 0, "golden-client").equals(decodedHello),
                    "decoded HELLO mismatch: " + decodedHello);
            writeV0Frame(out, Opcode.HELLO, true, hello.correlationId(), HELLO_RESPONSE_FROM_V0_SERVER, null);

            for (V0Exchange exchange : exchanges()) {
                RawFrame request = readV0Frame(in);
                check(exchange.opcode().code == request.opcode(), exchange.name() + " opcode mismatch");
                check(!request.isResponse(), exchange.name() + " must be a request");
                check(exchange.requestHeaderHex().equals(hex(request.header())), exchange.name() + " header mismatch");
                check(hex(exchange.requestPayload()).equals(hex(request.payload())),
                        exchange.name() + " payload mismatch");
                exchange.assertRequest(ByteBuffer.wrap(request.header()), ByteBuffer.wrap(request.payload()));
                writeV0Frame(out, exchange.opcode(), true, request.correlationId(),
                        exchange.responseHeaderHex(), exchange.responsePayload());
            }
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    private static List<V0Exchange> exchanges() {
        return List.of(
                exchange("ping", Opcode.PING, EMPTY_REQUEST, PING_PAYLOAD, "empty-request", ScpV0CompatibilityTest::decodeEmptyRequest,
                        OK_RESPONSE, PING_PAYLOAD, "ok", ScpV0CompatibilityTest::decodeOkResponse),
                exchange("openChunk", Opcode.OPEN_CHUNK,
                        "111111112222333300000003000000050100000000400000000000019000c79c000474657374" + "00",
                        null,
                        new Messages.OpenChunk(CHUNK_ID, 5, true, 1L << 30, 1_718_000_000_000L, NS),
                        b -> Messages.OpenChunk.decode(b),
                        OK_RESPONSE, null, "ok", ScpV0CompatibilityTest::decodeOkResponse),
                exchange("append", Opcode.APPEND,
                        "1111111122223333000000030000000500000000000004000000000000000200" + "0474657374" + "00",
                        APPEND_PAYLOAD,
                        new Messages.Append(CHUNK_ID, 5, 1024, 512, NS),
                        b -> Messages.Append.decode(b),
                        "0000000000000000080000", null,
                        new Messages.AppendResp(2048),
                        b -> decodeResp(b, Messages.AppendResp::decode)),
                exchange("read", Opcode.READ,
                        "111111112222333300000003000000000000006300010000" + "0474657374" + "00",
                        null,
                        new Messages.Read(CHUNK_ID, 99, 65_536, NS),
                        b -> Messages.Read.decode(b),
                        "00000000000000001000000000000000080000", READ_PAYLOAD,
                        new Messages.ReadResp(4096, 2048),
                        b -> decodeResp(b, Messages.ReadResp::decode)),
                exchange("fence", Opcode.FENCE,
                        "111111112222333300000003000000060474657374" + "00",
                        null,
                        new Messages.Fence(CHUNK_ID, 6, NS),
                        b -> Messages.Fence.decode(b),
                        "000000000007000000000000006400000000000000500000", null,
                        new Messages.FenceResp(7, 100, 80, ChunkState.OPEN),
                        b -> decodeResp(b, Messages.FenceResp::decode)),
                exchange("statChunk", Opcode.STAT_CHUNK,
                        "1111111122223333000000030474657374" + "00",
                        null,
                        new Messages.StatChunk(CHUNK_ID, NS),
                        b -> Messages.StatChunk.decode(b),
                        "00000100000000000000640000000000000064000000050000000700000000000000640000cafe00",
                        null,
                        new Messages.StatResp(ChunkState.SEALED, 100, 100, 5, 7, 100, 0xCAFE),
                        b -> decodeResp(b, Messages.StatResp::decode)),
                exchange("sealChunk", Opcode.SEAL_CHUNK,
                        "1111111122223333000000030000000500000000000010000474657374" + "00",
                        null,
                        new Messages.SealChunk(CHUNK_ID, 5, 4096, NS),
                        b -> Messages.SealChunk.decode(b),
                        "000000000000000010000000beef00", null,
                        new Messages.SealResp(4096, 0xBEEF),
                        b -> decodeResp(b, Messages.SealResp::decode)),
                exchange("deleteChunks", Opcode.DELETE_CHUNKS,
                        "021111111122223333000000031111111122223333000000040474657374" + "00",
                        null,
                        new Messages.DeleteChunks(List.of(CHUNK_ID, new ChunkId(FILE_ID, 4)), NS),
                        b -> Messages.DeleteChunks.decode(b),
                        "000001111111112222333300000003000000", null,
                        new Messages.DeleteChunksResp(List.of(CHUNK_ID), List.of((short) 0)),
                        b -> decodeResp(b, Messages.DeleteChunksResp::decode)),
                exchange("fetchChunk", Opcode.FETCH_CHUNK,
                        "111111112222333300000003" + "0000000000000000" + "7fffffff" + "0474657374" + "00",
                        null,
                        new Messages.FetchChunk(CHUNK_ID, 0, Integer.MAX_VALUE, NS),
                        b -> Messages.FetchChunk.decode(b),
                        "000000000000000020000100", FETCH_PAYLOAD,
                        new Messages.FetchResp(8192, ChunkState.SEALED),
                        b -> decodeResp(b, Messages.FetchResp::decode)),
                exchange("readLedger", Opcode.READ_LEDGER,
                        "11111111222233330000000300000000000008000474657374" + "00",
                        null,
                        new Messages.ReadLedger(CHUNK_ID, 2048, NS),
                        b -> Messages.ReadLedger.decode(b),
                        "0000020000000000000064000000010000000500000000000000c8000000020000000500",
                        null,
                        new Messages.ReadLedgerResp(List.of(
                                new Messages.LedgerEntry(100, 1, 5),
                                new Messages.LedgerEntry(200, 2, 5))),
                        b -> decodeResp(b, Messages.ReadLedgerResp::decode)),
                exchange("registerDataNode", Opcode.REGISTER_NODE,
                        "0000000700000000000000010000000000000002020768313a393030300768323a39303030027a3102723105686f73743101000001000000000000000001000000000000000000",
                        null,
                        new Messages.RegisterNode(7, 1, 2, List.of("h1:9000", "h2:9000"),
                                "z1", "r1", "host1",
                                List.of(new Messages.StorageCapacity(1L << 40)), 1, 0),
                        b -> Messages.RegisterNode.decode(b),
                        "00000000002a0000000000000009000003e80000271000", null,
                        new Messages.RegisterResp(42, 9, 1000, 10_000),
                        b -> decodeResp(b, Messages.RegisterResp::decode)),
                exchange("nodeHeartbeat", Opcode.NODE_HEARTBEAT,
                        "0000002a00000000000000010000000000000002000000000000000901000000000000006400000000000003840000000301000b0100000000000000070000",
                        null,
                        new Messages.NodeHeartbeat(42, 1, 2, 9,
                                List.of(new Messages.StorageUsage(100, 900)), 3,
                                List.of(new Messages.CompletedCommand(7, (short) 0))),
                        b -> Messages.NodeHeartbeat.decode(b),
                        "0000000000000001e240030000000000000001011111111122223333000000030100000007"
                                + "0768373a3930303001000000aa00000000000010000474657374"
                                + "000000000000000202011111111122223333000000030474657374"
                                + "0000000000000003" + "03" + "00",
                        null,
                        new Messages.HeartbeatResp(123_456, List.of(
                                new Messages.ReplicateCmd(1, CHUNK_ID,
                                        List.of(new Messages.Replica(7, "h7:9000")),
                                        (byte) 1, 0xAA, 4096, NS),
                                new Messages.DeleteCmd(2, List.of(CHUNK_ID), NS),
                                new Messages.DrainCmd(3))),
                        b -> decodeResp(b, Messages.HeartbeatResp::decode)),
                exchange("inventoryReport", Opcode.INVENTORY_REPORT,
                        "0000002a000000000000000100000000000000020000000000000009000000000000000101111111112222333300000003010000000000001000000000ab" + "0474657374" + "00",
                        null,
                        new Messages.InventoryReport(42, 1, 2, 9, 0, 1,
                                List.of(new Messages.InventoryEntry(CHUNK_ID, ChunkState.SEALED,
                                        4096, 0xAB, NS))),
                        b -> Messages.InventoryReport.decode(b),
                        OK_RESPONSE, null, "ok", ScpV0CompatibilityTest::decodeOkResponse),
                exchange("createFile", Opcode.CREATE_FILE,
                        "0474657374242f6b61666b612f746f706963412f302f3030303030303030303030303030303030303030000000030000000200" +
                                "0123456789abcdeffedcba987654321000",
                        null,
                        new Messages.CreateFile("test", "/kafka/topicA/0/00000000000000000000",
                                OP_ID_MSB, OP_ID_LSB),
                        b -> Messages.CreateFile.decode(b),
                        "0000111111112222333300", null,
                        new Messages.CreateFileResp(FILE_ID),
                        b -> decodeResp(b, Messages.CreateFileResp::decode)),
                exchange("createFileCustomPolicy", Opcode.CREATE_FILE,
                        "0874656e616e742d611a2f636c75737465722f746f7069632f706172746974696f6e2d300000000500000003010123456789abcdeffedcba987654321000",
                        null,
                        new Messages.CreateFile(StrataNamespace.of("tenant-a"),
                                StrataPath.of("/cluster/topic/partition-0"),
                                new Messages.WritePolicy(5, 3, true), OP_ID_MSB, OP_ID_LSB),
                        b -> Messages.CreateFile.decode(b),
                        "0000111111112222333300", null,
                        new Messages.CreateFileResp(FILE_ID),
                        b -> decodeResp(b, Messages.CreateFileResp::decode)),
                exchange("createChunk", Opcode.CREATE_CHUNK,
                        "04746573741111111122223333000000050123456789abcdeffedcba987654321000",
                        null,
                        new Messages.CreateChunk(NS, FILE_ID, 5, OP_ID_MSB, OP_ID_LSB),
                        b -> Messages.CreateChunk.decode(b),
                        "000011111111222233330000000300000005030000000103613a310000000203623a320000000303633a3300",
                        null,
                        new Messages.CreateChunkResp(CHUNK_ID, 5,
                                List.of(new Messages.Replica(1, "a:1"),
                                        new Messages.Replica(2, "b:2"),
                                        new Messages.Replica(3, "c:3"))),
                        b -> decodeResp(b, Messages.CreateChunkResp::decode)),
                exchange("allocateWriterEpochAppend", Opcode.ALLOCATE_WRITER_EPOCH,
                        "04746573741111111122223333" + "0100",
                        null,
                        Messages.AllocateWriterEpoch.forAppend(NS, FILE_ID),
                        b -> Messages.AllocateWriterEpoch.decode(b),
                        "00000000000700", null,
                        new Messages.AllocateWriterEpochResp(7),
                        b -> decodeResp(b, Messages.AllocateWriterEpochResp::decode)),
                exchange("allocateWriterEpochRecovery", Opcode.ALLOCATE_WRITER_EPOCH,
                        "04746573741111111122223333" + "0200",
                        null,
                        Messages.AllocateWriterEpoch.forRecovery(NS, FILE_ID),
                        b -> Messages.AllocateWriterEpoch.decode(b),
                        "00000000000700", null,
                        new Messages.AllocateWriterEpochResp(7),
                        b -> decodeResp(b, Messages.AllocateWriterEpochResp::decode)),
                exchange("sealChunkMeta", Opcode.SEAL_CHUNK_META,
                        "0474657374111111112222333300000003000000050000000000001000000000dd02000000010000000200",
                        null,
                        new Messages.SealChunkMeta(NS, CHUNK_ID, 5, 4096, 0xDD, List.of(1, 2)),
                        b -> Messages.SealChunkMeta.decode(b),
                        OK_RESPONSE, null, "ok", ScpV0CompatibilityTest::decodeOkResponse),
                exchange("abortChunkMeta", Opcode.ABORT_CHUNK_META,
                        "0474657374111111112222333300000003000000050000000000000001000000000000000200",
                        null,
                        new Messages.AbortChunkMeta(NS, CHUNK_ID, 5, 1, 2),
                        b -> Messages.AbortChunkMeta.decode(b),
                        OK_RESPONSE, null, "ok", ScpV0CompatibilityTest::decodeOkResponse),
                exchange("lookupFile", Opcode.LOOKUP_FILE,
                        "04746573741111111122223333" + "00",
                        null,
                        new Messages.LookupFile(NS, FILE_ID),
                        b -> Messages.LookupFile.decode(b),
                        "00000474657374242f6b61666b612f746f706963412f302f303030303030303030303030303030303030303000000003000000020000011111111122223333000000030000000000000000000000000000000005010000000103613a3100",
                        null,
                        new Messages.LookupFileResp("test", "/kafka/topicA/0/00000000000000000000",
                                Messages.WritePolicy.DEFAULT, (byte) 0,
                                List.of(new Messages.ChunkInfo(CHUNK_ID, ChunkState.OPEN, 0, 0,
                                        5, List.of(new Messages.Replica(1, "a:1"))))),
                        b -> decodeResp(b, Messages.LookupFileResp::decode)),
                exchange("lookupPath", Opcode.LOOKUP_PATH,
                        "0474657374242f6b61666b612f746f706963412f302f303030303030303030303030303030303030303000",
                        null,
                        new Messages.LookupPath("test", "/kafka/topicA/0/00000000000000000000"),
                        b -> Messages.LookupPath.decode(b),
                        "0000111111112222333300", null,
                        new Messages.LookupPathResp(FILE_ID),
                        b -> decodeResp(b, Messages.LookupPathResp::decode)),
                exchange("deleteFiles", Opcode.DELETE_FILES,
                        "047465737401" + "1111111122223333" + "00",
                        null,
                        new Messages.DeleteFiles(NS, List.of(FILE_ID)),
                        b -> Messages.DeleteFiles.decode(b),
                        "0000011111111122223333000000", null,
                        new Messages.DeleteFilesResp(List.of(FILE_ID), List.of((short) 0)),
                        b -> decodeResp(b, Messages.DeleteFilesResp::decode)),
                exchange("sealFile", Opcode.SEAL_FILE,
                        "0474657374" + "1111111122223333" + "000000000010000000",
                        null,
                        new Messages.SealFile(NS, FILE_ID, 1L << 20),
                        b -> Messages.SealFile.decode(b),
                        OK_RESPONSE, null, "ok", ScpV0CompatibilityTest::decodeOkResponse));
    }

    private static V0Exchange exchange(String name, Opcode opcode, String requestHeaderHex, byte[] requestPayload,
                                       Object expectedRequest, Function<ByteBuffer, Object> requestDecoder,
                                       String responseHeaderHex, byte[] responsePayload,
                                       Object expectedResponse, Function<ByteBuffer, Object> responseDecoder) {
        return new V0Exchange(name, opcode, requestHeaderHex, requestPayload == null ? new byte[0] : requestPayload,
                expectedRequest, requestDecoder, responseHeaderHex,
                responsePayload == null ? new byte[0] : responsePayload, expectedResponse, responseDecoder);
    }

    private static Object decodeEmptyRequest(ByteBuffer b) {
        TaggedFields.readFrom(b);
        return "empty-request";
    }

    private static Object decodeOkResponse(ByteBuffer b) {
        Resp.check(b);
        Messages.decodeOkHeader(b);
        return "ok";
    }

    private static Object decodeResp(ByteBuffer b, Function<ByteBuffer, ?> decoder) {
        Resp.check(b);
        return decoder.apply(b);
    }

    private static void writeV0Frame(DataOutputStream out, Opcode opcode, boolean response, long correlationId,
                                     String headerHex, byte[] payload) throws IOException {
        byte[] header = hexBytes(headerHex);
        byte[] body = payload == null ? new byte[0] : payload;
        int flags = response ? Frame.FLAG_RESPONSE : 0;
        int payloadCrc = 0;
        if (body.length > 0) {
            flags |= Frame.FLAG_PAYLOAD_CRC;
            payloadCrc = Crc.of(ByteBuffer.wrap(body));
        }
        int frameLen = Frame.PREAMBLE_AFTER_LEN + header.length + body.length;

        out.writeInt(frameLen);
        out.writeByte(Frame.MAGIC);
        out.writeByte(Frame.FRAME_VERSION);
        out.writeShort(opcode.code);
        out.writeShort(1);
        out.writeShort(flags);
        out.writeLong(correlationId);
        out.writeInt(body.length);
        out.writeInt(payloadCrc);
        out.writeShort(header.length);
        out.write(header);
        out.write(body);
        out.flush();
    }

    private static RawFrame readV0Frame(DataInputStream in) throws IOException {
        int frameLen;
        try {
            frameLen = in.readInt();
        } catch (EOFException e) {
            throw new EOFException("unexpected EOF before v0 frame");
        }
        check(frameLen >= Frame.PREAMBLE_AFTER_LEN, "bad frame length " + frameLen);
        byte magic = in.readByte();
        byte version = in.readByte();
        check(magic == Frame.MAGIC, "bad magic 0x" + Integer.toHexString(magic & 0xFF));
        check(version == Frame.FRAME_VERSION, "bad frame version " + version);
        short opcode = in.readShort();
        short apiVersion = in.readShort();
        short flags = in.readShort();
        long correlationId = in.readLong();
        int payloadLen = in.readInt();
        int payloadCrc = in.readInt();
        int headerLen = in.readUnsignedShort();
        check(payloadLen >= 0, "negative payload length " + payloadLen);
        check(Frame.PREAMBLE_AFTER_LEN + headerLen + payloadLen == frameLen,
                "frame length mismatch");
        byte[] header = new byte[headerLen];
        byte[] payload = new byte[payloadLen];
        in.readFully(header);
        in.readFully(payload);
        if ((flags & Frame.FLAG_PAYLOAD_CRC) != 0 && payloadLen > 0) {
            int actual = Crc.of(ByteBuffer.wrap(payload));
            check(payloadCrc == actual, "payload crc mismatch: " + payloadCrc + " != " + actual);
        }
        return new RawFrame(opcode, apiVersion, flags, correlationId, header, payload);
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] out = new byte[copy.remaining()];
        copy.get(out);
        return out;
    }

    private static String hex(ByteBuffer buffer) {
        return hex(bytes(buffer));
    }

    private static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            out.append(String.format("%02x", b & 0xFF));
        }
        return out.toString();
    }

    private static byte[] hexBytes(String hex) {
        if ((hex.length() & 1) != 0) {
            throw new IllegalArgumentException("odd hex length");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(hex.length() / 2);
        for (int i = 0; i < hex.length(); i += 2) {
            out.write(Integer.parseInt(hex.substring(i, i + 2), 16));
        }
        return out.toByteArray();
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private record RawFrame(short opcode, short apiVersion, short flags, long correlationId,
                            byte[] header, byte[] payload) {
        private RawFrame {
            Objects.requireNonNull(header, "header");
            Objects.requireNonNull(payload, "payload");
        }

        boolean isResponse() {
            return (flags & Frame.FLAG_RESPONSE) != 0;
        }
    }

    private record V0Exchange(String name, Opcode opcode, String requestHeaderHex, byte[] requestPayload,
                              Object expectedRequest, Function<ByteBuffer, Object> requestDecoder,
                              String responseHeaderHex, byte[] responsePayload,
                              Object expectedResponse, Function<ByteBuffer, Object> responseDecoder) {
        private V0Exchange {
            requestPayload = requestPayload.clone();
            responsePayload = responsePayload.clone();
        }

        byte[] requestHeader() {
            return hexBytes(requestHeaderHex);
        }

        byte[] responseHeader() {
            return hexBytes(responseHeaderHex);
        }

        ByteBuffer requestPayloadBuffer() {
            return requestPayload.length == 0 ? null : ByteBuffer.wrap(requestPayload).asReadOnlyBuffer();
        }

        ByteBuffer responsePayloadBuffer() {
            return responsePayload.length == 0 ? null : ByteBuffer.wrap(responsePayload).asReadOnlyBuffer();
        }

        public byte[] requestPayload() {
            return requestPayload.clone();
        }

        public byte[] responsePayload() {
            return responsePayload.clone();
        }

        void assertRequest(Frame frame) {
            assertEquals(opcode.code, frame.opcode(), name);
            assertEquals(1, frame.apiVersion(), name);
            assertFalse(frame.isResponse(), name);
            assertEquals(requestHeaderHex, hex(frame.headerSlice()), name);
            assertArrayEquals(requestPayload, bytes(frame.payloadSlice()), name);
            assertRequest(frame.headerSlice(), frame.payloadSlice());
        }

        void assertRequest(ByteBuffer header, ByteBuffer payload) {
            assertEquals(expectedRequest, requestDecoder.apply(header), name);
            assertArrayEquals(requestPayload, bytes(payload), name);
        }

        void assertResponse(ByteBuffer header, ByteBuffer payload) {
            assertEquals(expectedResponse, responseDecoder.apply(header), name);
            assertArrayEquals(responsePayload, bytes(payload), name);
        }
    }
}
