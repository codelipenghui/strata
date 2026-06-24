package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * V0 wire golden corpus. These tests intentionally assert exact bytes, not only object
 * round-trips, so mixed-version clients/nodes cannot silently drift on encodings.
 */
class MessageGoldenCorpusTest {
    private static final FileId FILE_ID = FileId.fromHex("1111111122223333");
    private static final ChunkId CHUNK_ID = new ChunkId(FILE_ID, 3);
    private static final long OP_ID_MSB = 0x0123456789abcdefL;
    private static final long OP_ID_LSB = 0xfedcba9876543210L;
    private static final StrataNamespace NS = StrataNamespace.of("test");

    @Test
    void opcodeTableIsAppendOnlyGoldenCorpus() {
        assertEquals(List.of(
                "HELLO=0x0001",
                "OPEN_CHUNK=0x0010",
                "APPEND=0x0011",
                "READ=0x0012",
                "FENCE=0x0013",
                "STAT_CHUNK=0x0014",
                "SEAL_CHUNK=0x0015",
                "DELETE_CHUNKS=0x0016",
                "FETCH_CHUNK=0x0017",
                "PING=0x0018",
                "READ_LEDGER=0x0019",
                "READ_RECOVERY=0x001a",
                "REGISTER_NODE=0x0101",
                "NODE_HEARTBEAT=0x0102",
                "INVENTORY_REPORT=0x0103",
                "CREATE_FILE=0x0201",
                "CREATE_CHUNK=0x0202",
                "SEAL_CHUNK_META=0x0203",
                "LOOKUP_FILE=0x0204",
                "DELETE_FILES=0x0205",
                "SEAL_FILE=0x0206",
                "ABORT_CHUNK_META=0x0207",
                "LOOKUP_PATH=0x0208",
                "ALLOCATE_WRITER_EPOCH=0x0209",
                "EXEC_REPLICATE=0x020a"),
                Arrays.stream(Opcode.values())
                        .map(op -> op.name() + "=0x" + String.format("%04x", op.code & 0xFFFF))
                        .toList());
    }

    @Test
    void errorCodeTableIsAppendOnlyGoldenCorpus() {
        assertEquals(List.of(
                "OK=0:false",
                "UNKNOWN_OPCODE=1:false",
                "UNSUPPORTED_VERSION=2:false",
                "FENCED_EPOCH=3:false",
                "OFFSET_GAP=4:true",
                "CHUNK_NOT_FOUND=5:false",
                "CHUNK_SEALED=6:false",
                "CHUNK_ALREADY_EXISTS=7:false",
                "OUT_OF_SPACE=8:false",
                "CRC_MISMATCH=9:false",
                "NOT_REGISTERED=10:false",
                "LEASE_EXPIRED=11:false",
                "THROTTLED=12:true",
                "CORRUPT_CHUNK=13:false",
                "INTERNAL=14:true",
                "NOT_LEADER=15:true",
                "NO_CAPACITY=16:true",
                "FILE_NOT_FOUND=17:false",
                "FILE_SEALED=18:false",
                "PRECONDITION_FAILED=19:false"),
                Arrays.stream(ErrorCode.values())
                        .map(code -> code.name() + "=" + code.code + ":" + code.retriable)
                        .toList());
    }

    @Test
    void messageHeadersMatchGoldenCorpus() {
        for (GoldenCase<?> c : goldenCases()) {
            c.assertStable();
        }
    }

    @Test
    void okAndErrorHeadersMatchGoldenCorpus() {
        assertEquals("000000", hex(Messages.okHeader()));

        String expected = "00061062616420657870656374656420656e6401000800000000000004d2";
        assertEquals(expected, hex(Resp.error(ErrorCode.CHUNK_SEALED, "bad expected end", 1234)));

        ScpException e = assertThrows(ScpException.class, () -> Resp.check(buf(expected)));
        assertEquals(ErrorCode.CHUNK_SEALED, e.code());
        assertEquals(1234, e.detail());
    }

    @Test
    void completeFrameMatchesGoldenCorpus() throws Exception {
        Frame frame = Frame.request(Opcode.READ, new Messages.Read(CHUNK_ID, 99, 65_536, NS).encode(),
                null, 0x0102030405060708L);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        FrameIO.write(new DataOutputStream(bytes), frame);

        String expected = "000000385c0100120001000001020304050607080000000000000000001e"
                + "111111112222333300000003000000000000006300010000" + "0474657374" + "00";
        assertEquals(expected, hex(bytes.toByteArray()));

        Frame decoded = FrameIO.read(new DataInputStream(new ByteArrayInputStream(hexBytes(expected))));
        assertEquals(Opcode.READ.code, decoded.opcode());
        assertEquals(1, decoded.apiVersion());
        assertFalse(decoded.isResponse());
        assertEquals(0x0102030405060708L, decoded.correlationId());
        assertEquals(new Messages.Read(CHUNK_ID, 99, 65_536, NS),
                Messages.Read.decode(decoded.headerSlice()));
        assertEquals(0, decoded.payloadLength());
    }

    private static List<GoldenCase<?>> goldenCases() {
        return List.of(
                request("hello",
                        new Messages.Hello(ScpClient.KIND_BROKER, 0x1020304050607080L,
                                "golden-client"),
                        () -> new Messages.Hello(ScpClient.KIND_BROKER, 0x1020304050607080L,
                                "golden-client").encode(),
                        Messages.Hello::decode,
                        "000100010110203040506070800d676f6c64656e2d636c69656e7400"),
                response("helloResp",
                        new Messages.HelloResp(0x0102030405060708L, 42, 0x1111222233334444L,
                                0x5555666677778888L, FrameIO.MAX_FRAME_BYTES, 1L << 30),
                        () -> new Messages.HelloResp(0x0102030405060708L, 42, 0x1111222233334444L,
                                0x5555666677778888L, FrameIO.MAX_FRAME_BYTES, 1L << 30).encode(),
                        Messages.HelloResp::decode,
                        "0000000101020304050607080000002a11112222333344445555666677778888"
                                + "04000000000000004000000019000100010010000100110001001200010013"
                                + "0001001400010015000100160001001700010018000100190001001a0001"
                                + "010100010102000101030001020100010202000102030001020400010205"
                                + "000102060001020700010208000102090001020a000100"),
                request("openChunk",
                        new Messages.OpenChunk(CHUNK_ID, 5, true, 1L << 30, 1_718_000_000_000L, NS),
                        () -> new Messages.OpenChunk(CHUNK_ID, 5, true, 1L << 30, 1_718_000_000_000L, NS).encode(),
                        Messages.OpenChunk::decode,
                        "11111111222233330000000300000005010000000040000000"
                                + "0000019000c79c000474657374" + "00"),
                request("append",
                        new Messages.Append(CHUNK_ID, 5, 1024, 512, NS),
                        () -> new Messages.Append(CHUNK_ID, 5, 1024, 512, NS).encode(),
                        Messages.Append::decode,
                        "111111112222333300000003000000050000000000000400"
                                + "0000000000000200" + "0474657374" + "00"),
                response("appendResp",
                        new Messages.AppendResp(2048),
                        () -> new Messages.AppendResp(2048).encode(),
                        Messages.AppendResp::decode,
                        "0000000000000000080000"),
                request("read",
                        new Messages.Read(CHUNK_ID, 99, 65_536, NS),
                        () -> new Messages.Read(CHUNK_ID, 99, 65_536, NS).encode(),
                        Messages.Read::decode,
                        "111111112222333300000003000000000000006300010000" + "0474657374" + "00"),
                response("readResp",
                        new Messages.ReadResp(4096, 2048),
                        () -> new Messages.ReadResp(4096, 2048).encode(),
                        Messages.ReadResp::decode,
                        "00000000000000001000000000000000080000"),
                request("fence",
                        new Messages.Fence(CHUNK_ID, 6, NS),
                        () -> new Messages.Fence(CHUNK_ID, 6, NS).encode(),
                        Messages.Fence::decode,
                        "11111111222233330000000300000006" + "0474657374" + "00"),
                response("fenceResp",
                        new Messages.FenceResp(7, 100, 80, ChunkState.OPEN),
                        () -> new Messages.FenceResp(7, 100, 80, ChunkState.OPEN).encode(),
                        Messages.FenceResp::decode,
                        "000000000007000000000000006400000000000000500000"),
                request("statChunk",
                        new Messages.StatChunk(CHUNK_ID, NS),
                        () -> new Messages.StatChunk(CHUNK_ID, NS).encode(),
                        Messages.StatChunk::decode,
                        "111111112222333300000003" + "0474657374" + "00"),
                response("statResp",
                        new Messages.StatResp(ChunkState.SEALED, 100, 100, 5, 7, 100, 0xCAFE),
                        () -> new Messages.StatResp(ChunkState.SEALED, 100, 100, 5, 7, 100,
                                0xCAFE).encode(),
                        Messages.StatResp::decode,
                        "000001000000000000006400000000000000640000000500000007000000000000"
                                + "00640000cafe00"),
                request("sealChunk",
                        new Messages.SealChunk(CHUNK_ID, 5, 4096, NS),
                        () -> new Messages.SealChunk(CHUNK_ID, 5, 4096, NS).encode(),
                        Messages.SealChunk::decode,
                        "1111111122223333000000030000000500000000000010000474657374" + "00"),
                response("sealResp",
                        new Messages.SealResp(4096, 0xBEEF),
                        () -> new Messages.SealResp(4096, 0xBEEF).encode(),
                        Messages.SealResp::decode,
                        "000000000000000010000000beef00"),
                request("deleteChunks",
                        new Messages.DeleteChunks(List.of(CHUNK_ID, new ChunkId(FILE_ID, 4)), NS),
                        () -> new Messages.DeleteChunks(List.of(CHUNK_ID, new ChunkId(FILE_ID, 4)), NS).encode(),
                        Messages.DeleteChunks::decode,
                        "02111111112222333300000003111111112222333300000004" + "0474657374" + "00"),
                response("deleteChunksResp",
                        new Messages.DeleteChunksResp(List.of(CHUNK_ID), List.of((short) 0)),
                        () -> new Messages.DeleteChunksResp(List.of(CHUNK_ID), List.of((short) 0)).encode(),
                        Messages.DeleteChunksResp::decode,
                        "000001111111112222333300000003000000"),
                request("fetchChunk",
                        new Messages.FetchChunk(CHUNK_ID, 0, Integer.MAX_VALUE, NS),
                        () -> new Messages.FetchChunk(CHUNK_ID, 0, Integer.MAX_VALUE, NS).encode(),
                        Messages.FetchChunk::decode,
                        "11111111222233330000000300000000000000007fffffff" + "0474657374" + "00"),
                response("fetchResp",
                        new Messages.FetchResp(8192, ChunkState.SEALED),
                        () -> new Messages.FetchResp(8192, ChunkState.SEALED).encode(),
                        Messages.FetchResp::decode,
                        "000000000000000020000100"),
                request("readLedger",
                        new Messages.ReadLedger(CHUNK_ID, 2048, NS),
                        () -> new Messages.ReadLedger(CHUNK_ID, 2048, NS).encode(),
                        Messages.ReadLedger::decode,
                        "11111111222233330000000300000000000008000474657374" + "00"),
                response("readLedgerResp",
                        new Messages.ReadLedgerResp(List.of(
                                new Messages.LedgerEntry(100, 1, 5),
                                new Messages.LedgerEntry(200, 2, 5))),
                        () -> new Messages.ReadLedgerResp(List.of(
                                new Messages.LedgerEntry(100, 1, 5),
                                new Messages.LedgerEntry(200, 2, 5))).encode(),
                        Messages.ReadLedgerResp::decode,
                        "0000020000000000000064000000010000000500000000000000c800000002"
                                + "0000000500"),
                request("registerDataNode",
                        new Messages.RegisterNode(7, 1, 2, List.of("h1:9000", "h2:9000"), "z1",
                                "r1", "host1", List.of(new Messages.StorageCapacity(1L << 40)),
                                1, 0),
                        () -> new Messages.RegisterNode(7, 1, 2, List.of("h1:9000", "h2:9000"),
                                "z1", "r1", "host1",
                                List.of(new Messages.StorageCapacity(1L << 40)), 1, 0).encode(),
                        Messages.RegisterNode::decode,
                        "0000000700000000000000010000000000000002020768313a393030300768323a39303030"
                                + "027a3102723105686f73743101000001000000000000000001000000000000000000"),
                response("registerResp",
                        new Messages.RegisterResp(42, 9, 1000, 10_000),
                        () -> new Messages.RegisterResp(42, 9, 1000, 10_000).encode(),
                        Messages.RegisterResp::decode,
                        "00000000002a0000000000000009000003e80000271000"),
                request("nodeHeartbeat",
                        new Messages.NodeHeartbeat(42, 1, 2, 9,
                                List.of(new Messages.StorageUsage(100, 900)), 3,
                                List.of(new Messages.CompletedCommand(7, (short) 0))),
                        () -> new Messages.NodeHeartbeat(42, 1, 2, 9,
                                List.of(new Messages.StorageUsage(100, 900)), 3,
                                List.of(new Messages.CompletedCommand(7, (short) 0))).encode(),
                        Messages.NodeHeartbeat::decode,
                        "0000002a000000000000000100000000000000020000000000000009010000000000"
                                + "00006400000000000003840000000301000b0100000000000000070000"),
                request("nodeHeartbeatEmpty",
                        new Messages.NodeHeartbeat(42, 1, 2, 9, List.of(), 0, List.of()),
                        () -> new Messages.NodeHeartbeat(42, 1, 2, 9, List.of(), 0, List.of()).encode(),
                        Messages.NodeHeartbeat::decode,
                        "0000002a000000000000000100000000000000020000000000000009000000000000"),
                response("heartbeatResp",
                        new Messages.HeartbeatResp(123_456, List.of(
                                new Messages.ReplicateCmd(1, CHUNK_ID,
                                        List.of(new Messages.Replica(7, "h7:9000")),
                                        (byte) 1, 0xAA, 4096, NS),
                                new Messages.DeleteCmd(2, List.of(CHUNK_ID), NS),
                                new Messages.DrainCmd(3))),
                        () -> new Messages.HeartbeatResp(123_456, List.of(
                                new Messages.ReplicateCmd(1, CHUNK_ID,
                                        List.of(new Messages.Replica(7, "h7:9000")),
                                        (byte) 1, 0xAA, 4096, NS),
                                new Messages.DeleteCmd(2, List.of(CHUNK_ID), NS),
                                new Messages.DrainCmd(3))).encode(),
                        Messages.HeartbeatResp::decode,
                        "0000000000000001e240030000000000000001011111111122223333000000030100000007"
                                + "0768373a3930303001000000aa00000000000010000474657374"
                                + "000000000000000202011111111122223333000000030474657374" + "0000000000000003" + "03" + "00"),
                request("inventoryReport",
                        new Messages.InventoryReport(42, 1, 2, 9, 0, 1,
                                List.of(new Messages.InventoryEntry(CHUNK_ID, ChunkState.SEALED,
                                        4096, 0xAB, NS))),
                        () -> new Messages.InventoryReport(42, 1, 2, 9, 0, 1,
                                List.of(new Messages.InventoryEntry(CHUNK_ID, ChunkState.SEALED,
                                        4096, 0xAB, NS))).encode(),
                        Messages.InventoryReport::decode,
                        "0000002a000000000000000100000000000000020000000000000009000000000000"
                                + "000101111111112222333300000003010000000000001000"
                                + "000000ab" + "0474657374" + "00"),
                request("createFile",
                        new Messages.CreateFile("test", "/kafka/topicA/0/00000000000000000000",
                                OP_ID_MSB, OP_ID_LSB),
                        () -> new Messages.CreateFile("test", "/kafka/topicA/0/00000000000000000000",
                                OP_ID_MSB, OP_ID_LSB).encode(),
                        Messages.CreateFile::decode,
                        "0474657374242f6b61666b612f746f706963412f302f3030303030303030303030303030303030303030000000030000000200"
                                + "0123456789abcdeffedcba987654321000"),
                request("createFileCustomPolicy",
                        new Messages.CreateFile(StrataNamespace.of("tenant-a"),
                                StrataPath.of("/cluster/topic/partition-0"),
                                new Messages.WritePolicy(5, 3, true), OP_ID_MSB, OP_ID_LSB),
                        () -> new Messages.CreateFile(StrataNamespace.of("tenant-a"),
                                StrataPath.of("/cluster/topic/partition-0"),
                                new Messages.WritePolicy(5, 3, true), OP_ID_MSB, OP_ID_LSB).encode(),
                        Messages.CreateFile::decode,
                        "0874656e616e742d611a2f636c75737465722f746f7069632f706172746974696f"
                                + "6e2d300000000500000003010123456789abcdeffedcba987654321000"),
                response("createFileResp",
                        new Messages.CreateFileResp(FILE_ID),
                        () -> new Messages.CreateFileResp(FILE_ID).encode(),
                        Messages.CreateFileResp::decode,
                        "0000111111112222333300"),
                request("createChunk",
                        new Messages.CreateChunk(NS, FILE_ID, 5, OP_ID_MSB, OP_ID_LSB),
                        () -> new Messages.CreateChunk(NS, FILE_ID, 5, OP_ID_MSB, OP_ID_LSB).encode(),
                        Messages.CreateChunk::decode,
                        "04746573741111111122223333000000050123456789abcdeffedcba987654321000"),
                response("createChunkResp",
                        new Messages.CreateChunkResp(CHUNK_ID, 5,
                                List.of(new Messages.Replica(1, "a:1"),
                                        new Messages.Replica(2, "b:2"),
                                        new Messages.Replica(3, "c:3"))),
                        () -> new Messages.CreateChunkResp(CHUNK_ID, 5,
                                List.of(new Messages.Replica(1, "a:1"),
                                        new Messages.Replica(2, "b:2"),
                                        new Messages.Replica(3, "c:3"))).encode(),
                        Messages.CreateChunkResp::decode,
                        "00001111111122223333000000030000000503000000010361"
                                + "3a310000000203623a320000000303633a3300"),
                request("allocateWriterEpochAppend",
                        Messages.AllocateWriterEpoch.forAppend(NS, FILE_ID),
                        () -> Messages.AllocateWriterEpoch.forAppend(NS, FILE_ID).encode(),
                        Messages.AllocateWriterEpoch::decode,
                        "0474657374111111112222333301" + "00"),
                request("allocateWriterEpochRecovery",
                        Messages.AllocateWriterEpoch.forRecovery(NS, FILE_ID),
                        () -> Messages.AllocateWriterEpoch.forRecovery(NS, FILE_ID).encode(),
                        Messages.AllocateWriterEpoch::decode,
                        "0474657374111111112222333302" + "00"),
                response("allocateWriterEpochResp",
                        new Messages.AllocateWriterEpochResp(7),
                        () -> new Messages.AllocateWriterEpochResp(7).encode(),
                        Messages.AllocateWriterEpochResp::decode,
                        "00000000000700"),
                request("sealChunkMeta",
                        new Messages.SealChunkMeta(NS, CHUNK_ID, 5, 4096, 0xDD, List.of(1, 2)),
                        () -> new Messages.SealChunkMeta(NS, CHUNK_ID, 5, 4096, 0xDD,
                                List.of(1, 2)).encode(),
                        Messages.SealChunkMeta::decode,
                        "0474657374111111112222333300000003000000050000000000001000"
                                + "000000dd02000000010000000200"),
                request("abortChunkMeta",
                        new Messages.AbortChunkMeta(NS, CHUNK_ID, 5, 1, 2),
                        () -> new Messages.AbortChunkMeta(NS, CHUNK_ID, 5, 1, 2).encode(),
                        Messages.AbortChunkMeta::decode,
                        "0474657374111111112222333300000003000000050000000000000001"
                                + "000000000000000200"),
                request("lookupFile",
                        new Messages.LookupFile(NS, FILE_ID),
                        () -> new Messages.LookupFile(NS, FILE_ID).encode(),
                        Messages.LookupFile::decode,
                        "04746573741111111122223333" + "00"),
                request("lookupPath",
                        new Messages.LookupPath("test", "/kafka/topicA/0/00000000000000000000"),
                        () -> new Messages.LookupPath("test",
                                "/kafka/topicA/0/00000000000000000000").encode(),
                        Messages.LookupPath::decode,
                        "0474657374242f6b61666b612f746f706963412f302f303030303030303030303030303030303030303000"),
                response("lookupPathResp",
                        new Messages.LookupPathResp(FILE_ID),
                        () -> new Messages.LookupPathResp(FILE_ID).encode(),
                        Messages.LookupPathResp::decode,
                        "0000111111112222333300"),
                response("lookupFileResp",
                        new Messages.LookupFileResp("test", "/kafka/topicA/0/00000000000000000000",
                                Messages.WritePolicy.DEFAULT, (byte) 0,
                                List.of(new Messages.ChunkInfo(CHUNK_ID, ChunkState.OPEN, 0, 0,
                                        5, List.of(new Messages.Replica(1, "a:1"))))),
                        () -> new Messages.LookupFileResp("test",
                                "/kafka/topicA/0/00000000000000000000",
                                Messages.WritePolicy.DEFAULT, (byte) 0,
                                List.of(new Messages.ChunkInfo(CHUNK_ID, ChunkState.OPEN, 0, 0,
                                        5, List.of(new Messages.Replica(1, "a:1"))))).encode(),
                        Messages.LookupFileResp::decode,
                        "00000474657374242f6b61666b612f746f706963412f302f303030303030303030303030303030303030303000000003000000020000011111111122223333000000030000000000000000000000000000000005010000000103613a3100"),
                request("deleteFiles",
                        new Messages.DeleteFiles(NS, List.of(FILE_ID)),
                        () -> new Messages.DeleteFiles(NS, List.of(FILE_ID)).encode(),
                        Messages.DeleteFiles::decode,
                        "047465737401" + "1111111122223333" + "00"),
                response("deleteFilesResp",
                        new Messages.DeleteFilesResp(List.of(FILE_ID), List.of((short) 0)),
                        () -> new Messages.DeleteFilesResp(List.of(FILE_ID), List.of((short) 0)).encode(),
                        Messages.DeleteFilesResp::decode,
                        "0000011111111122223333000000"),
                request("sealFile",
                        new Messages.SealFile(NS, FILE_ID, 1L << 20),
                        () -> new Messages.SealFile(NS, FILE_ID, 1L << 20).encode(),
                        Messages.SealFile::decode,
                        "0474657374" + "1111111122223333" + "000000000010000000"));
    }

    private static <T> GoldenCase<T> request(String name, T expected, Supplier<byte[]> encoder,
                                             Function<ByteBuffer, T> decoder, String expectedHex) {
        return new GoldenCase<>(name, expectedHex, encoder, decoder, expected);
    }

    private static <T> GoldenCase<T> response(String name, T expected, Supplier<byte[]> encoder,
                                              Function<ByteBuffer, T> decoder, String expectedHex) {
        return new GoldenCase<>(name, expectedHex, encoder, b -> decodeResp(b, decoder), expected);
    }

    private static <T> T decodeResp(ByteBuffer b, Function<ByteBuffer, T> decoder) {
        Resp.check(b);
        return decoder.apply(b);
    }

    private static ByteBuffer buf(String hex) {
        return ByteBuffer.wrap(hexBytes(hex));
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
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    private record GoldenCase<T>(String name, String expectedHex, Supplier<byte[]> encoder,
                                 Function<ByteBuffer, T> decoder, T expected) {
        void assertStable() {
            assertEquals(expectedHex, hex(encoder.get()), name + " encoding");
            assertEquals(expected, decoder.apply(buf(expectedHex)), name + " decoding");
        }
    }
}
