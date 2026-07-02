package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolCoverageTest {

    @Test
    void writePolicyRejectsNonIntersectingQuorum() {
        assertThrows(IllegalArgumentException.class, () -> new Messages.WritePolicy(4, 2, false));
    }

    @Test
    void frameIoRejectsMalformedLengthsAndVersionFields() throws Exception {
        assertThrows(IOException.class, () -> FrameIO.write(new DataOutputStream(new ByteArrayOutputStream()),
                Frame.request(Opcode.PING, new byte[0x1_0000], null, 1)));

        ByteBuffer tooLargePayload = ByteBuffer.allocate(FrameIO.MAX_FRAME_BYTES);
        assertThrows(IOException.class, () -> FrameIO.write(new DataOutputStream(new ByteArrayOutputStream()),
                Frame.request(Opcode.PING, Messages.okHeader(), tooLargePayload, 1)));

        assertNull(FrameIO.read(new DataInputStream(new ByteArrayInputStream(new byte[0]))));

        assertFrameReadFails(frameWithLength(Frame.PREAMBLE_AFTER_LEN - 1, new byte[Frame.PREAMBLE_AFTER_LEN - 1]),
                "length");
        assertFrameReadFails(frameWithLength(FrameIO.MAX_FRAME_BYTES + 1, new byte[0]), "length");

        byte[] badMagic = minimalFrameBody((byte) 0x00, Frame.FRAME_VERSION, 0, 0, (short) 0);
        assertFrameReadFails(frameWithLength(badMagic.length, badMagic), "magic");

        byte[] badVersion = minimalFrameBody(Frame.MAGIC, (byte) 99, 0, 0, (short) 0);
        assertFrameReadFails(frameWithLength(badVersion.length, badVersion), "version");

        byte[] mismatch = minimalFrameBody(Frame.MAGIC, Frame.FRAME_VERSION, 1, 0, (short) 0);
        assertFrameReadFails(frameWithLength(mismatch.length, mismatch), "mismatch");
    }

    @Test
    void frameIoWritesDirectPayloadsAndIgnoresEmptyPayloadCrcFlag() throws Exception {
        ByteBuffer directPayload = ByteBuffer.allocateDirect(3);
        directPayload.put(new byte[]{4, 5, 6}).flip();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameIO.write(new DataOutputStream(out), Frame.request(Opcode.PING, Messages.okHeader(), directPayload, 9));
        Frame read = FrameIO.read(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
        byte[] payload = new byte[read.payloadLength()];
        read.payloadSlice().get(payload);
        assertArrayEquals(new byte[]{4, 5, 6}, payload);

        byte[] emptyCrcFlag = minimalFrameBody(Frame.MAGIC, Frame.FRAME_VERSION,
                0, 0, Frame.FLAG_PAYLOAD_CRC);
        Frame noPayload = FrameIO.read(new DataInputStream(new ByteArrayInputStream(
                frameWithLength(emptyCrcFlag.length, emptyCrcFlag))));
        assertEquals(0, noPayload.payloadLength());
    }

    @Test
    void frameIoPrivateWriteFullyHandlesArrayBackedBuffers() throws Exception {
        Method writeFully = FrameIO.class.getDeclaredMethod("writeFully", DataOutputStream.class, ByteBuffer.class);
        writeFully.setAccessible(true);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        buffer.position(1);
        buffer.limit(3);

        writeFully.invoke(null, new DataOutputStream(bytes), buffer);

        assertArrayEquals(new byte[] {2, 3}, bytes.toByteArray());
    }

    @Test
    void timeoutDetectionUnwrapsCompletionExceptions() throws Exception {
        Method isTimeout = ScpClient.class.getDeclaredMethod("isTimeout", Throwable.class);
        isTimeout.setAccessible(true);

        assertEquals(true, isTimeout.invoke(null, new CompletionException(new TimeoutException())));
        assertEquals(true, isTimeout.invoke(null,
                new CompletionException(new CompletionException(new TimeoutException()))));
        assertEquals(false, isTimeout.invoke(null, new CompletionException(new IOException("no timeout"))));
        assertEquals(false, isTimeout.invoke(null, new RuntimeException("no timeout")));
    }

    @Test
    void taggedFieldsValidateCountAndSize() {
        BufWriter tooMany = new BufWriter();
        tooMany.varint(1025);
        var countError = assertThrows(IllegalArgumentException.class,
                () -> TaggedFields.readFrom(ByteBuffer.wrap(tooMany.toBytes())));
        assertTrue(countError.getMessage().contains("count"));

        BufWriter negativeCount = new BufWriter();
        negativeCount.varint(-1L);
        var negativeCountError = assertThrows(IllegalArgumentException.class,
                () -> TaggedFields.readFrom(ByteBuffer.wrap(negativeCount.toBytes())));
        assertTrue(negativeCountError.getMessage().contains("count"));

        BufWriter badSize = new BufWriter();
        badSize.varint(1).varint(7).varint(3).u8(1);
        var sizeError = assertThrows(IllegalArgumentException.class,
                () -> TaggedFields.readFrom(ByteBuffer.wrap(badSize.toBytes())));
        assertTrue(sizeError.getMessage().contains("size"));

        BufWriter badTag = new BufWriter();
        badTag.varint(1).varint(-1L).varint(0);
        var tagError = assertThrows(IllegalArgumentException.class,
                () -> TaggedFields.readFrom(ByteBuffer.wrap(badTag.toBytes())));
        assertTrue(tagError.getMessage().contains("tag"));

        BufWriter negativeSize = new BufWriter();
        negativeSize.varint(1).varint(7).varint(-1L);
        var negativeSizeError = assertThrows(IllegalArgumentException.class,
                () -> TaggedFields.readFrom(ByteBuffer.wrap(negativeSize.toBytes())));
        assertTrue(negativeSizeError.getMessage().contains("size"));

        BufWriter trailing = new BufWriter();
        trailing.varint(0).u8(1);
        var trailingError = assertThrows(IllegalArgumentException.class,
                () -> TaggedFields.readFrom(ByteBuffer.wrap(trailing.toBytes())));
        assertTrue(trailingError.getMessage().contains("trailing"));

        assertThrows(IllegalArgumentException.class, () -> TaggedFields.of(Map.of(-1, new byte[] {1})));

        TaggedFields fields = TaggedFields.of(Map.of(2, new byte[]{9}, 1, new byte[]{8}));
        BufWriter encoded = new BufWriter();
        fields.writeTo(encoded);
        TaggedFields decoded = TaggedFields.readFrom(ByteBuffer.wrap(encoded.toBytes()));
        assertArrayEquals(new byte[]{8}, decoded.get(1));
        assertArrayEquals(new byte[]{9}, decoded.get(2));

        byte[] mutable = new byte[] {1};
        TaggedFields immutable = TaggedFields.of(Map.of(3, mutable));
        mutable[0] = 99;
        assertArrayEquals(new byte[] {1}, immutable.get(3));
        byte[] returned = immutable.get(3);
        returned[0] = 42;
        assertArrayEquals(new byte[] {1}, immutable.get(3));
    }

    @Test
    void responseHelpersCoverOkAndErrorVariants() {
        ByteBuffer ok = ByteBuffer.wrap(Messages.okHeader());
        Resp.check(ok);
        Messages.decodeOkHeader(ok);

        byte[] err = Resp.error(ErrorCode.INTERNAL, null, 0);
        ScpException e = assertThrows(ScpException.class, () -> Resp.check(ByteBuffer.wrap(err)));
        assertEquals(ErrorCode.INTERNAL, e.code());
        assertEquals("INTERNAL: ", e.getMessage());
        assertEquals(0, e.detail());

        BufWriter w = new BufWriter();
        w.u16(ErrorCode.INTERNAL.code).string("bad");
        TaggedFields.of(Map.of(Resp.TAG_DETAIL, new byte[]{1, 2, 3})).writeTo(w);
        ScpException badDetail = assertThrows(ScpException.class, () -> Resp.check(ByteBuffer.wrap(w.toBytes())));
        assertEquals(0, badDetail.detail());
    }

    @Test
    void messageAndOpcodeErrorBranchesAreCovered() {
        BufWriter hello = new BufWriter();
        hello.u16(0).u16(0).u8(ScpClient.KIND_TOOL).u64(0).string("old").noTags();
        var version = assertThrows(IllegalArgumentException.class,
                () -> Messages.Hello.decode(ByteBuffer.wrap(hello.toBytes())));
        assertTrue(version.getMessage().contains("frame version"));

        ByteBuffer command = ByteBuffer.allocate(Long.BYTES + 1);
        command.putLong(9).put((byte) 99).flip();
        var commandError = assertThrows(IllegalArgumentException.class, () -> Messages.Command.read(command));
        assertTrue(commandError.getMessage().contains("command type"));

        assertEquals(Opcode.PING, Opcode.fromCode(Opcode.PING.code));
        assertNull(Opcode.fromCode((short) 0x7FFF));
    }

    @Test
    void listBackedMessagesDefensivelyCopyAndValidateCounts() {
        ChunkId chunkId = new ChunkId(FileId.of(1), 0);
        List<Messages.Replica> replicas = new ArrayList<>(List.of(new Messages.Replica(1, "node:9000")));
        Messages.CreateChunkResp createChunk = new Messages.CreateChunkResp(chunkId, 7, replicas);
        replicas.clear();
        assertEquals(1, createChunk.replicas().size());
        assertThrows(UnsupportedOperationException.class,
                () -> createChunk.replicas().add(new Messages.Replica(2, "node:9001")));

        List<Messages.Command> commands = new ArrayList<>(List.of(new Messages.DrainCmd(9)));
        Messages.HeartbeatResp heartbeat = new Messages.HeartbeatResp(100, commands);
        commands.clear();
        assertEquals(1, heartbeat.commands().size());

        assertThrows(IllegalArgumentException.class,
                () -> new Messages.DeleteChunksResp(List.of(chunkId), List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new Messages.DeleteFilesResp(List.of(FileId.of(2)), List.of()));
    }

    @Test
    void smallFrameAndBufferHelpers() throws Exception {
        Frame request = Frame.request(Opcode.PING, Messages.okHeader(), null, 11);
        assertFalse(request.isResponse());
        assertThrows(ReadOnlyBufferException.class, () -> request.headerSlice().put((byte) 0));
        Frame response = Frame.response(request, Messages.okHeader(), null);
        assertTrue(response.isResponse());
        Frame nullHeader = Frame.request(Opcode.PING, null, null, 12);
        assertEquals(0, nullHeader.headerSlice().remaining());
        assertEquals(0, Frame.response(nullHeader, null, null).headerSlice().remaining());
        Frame nullBuffers = new Frame(Opcode.PING.code, (short) 1, (short) 0, 13, null, null);
        assertEquals(0, nullBuffers.payloadLength());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameIO.write(new DataOutputStream(out), nullHeader);
        Frame decoded = FrameIO.read(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
        assertEquals(0, decoded.headerSlice().remaining());

        BufWriter w = new BufWriter();
        assertEquals(0, w.size());
        w.u8(1).u16(2);
        assertEquals(3, w.size());

        ExecutionException wrapped = new ExecutionException(new TimeoutException());
        // This future shape is what CompletableFuture#get produces for timed pipelined calls.
        assertEquals(TimeoutException.class, wrapped.getCause().getClass());
    }

    private static void assertFrameReadFails(byte[] wire, String messageFragment) {
        IOException e = assertThrows(IOException.class,
                () -> FrameIO.read(new DataInputStream(new ByteArrayInputStream(wire))));
        assertTrue(e.getMessage().contains(messageFragment), "got: " + e.getMessage());
    }

    private static byte[] frameWithLength(int frameLen, byte[] body) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(out);
        data.writeInt(frameLen);
        data.write(body);
        return out.toByteArray();
    }

    private static byte[] minimalFrameBody(byte magic, byte version, int payloadLen, int headerLen,
                                           short flags) {
        ByteBuffer body = ByteBuffer.allocate(Frame.PREAMBLE_AFTER_LEN + Math.max(0, headerLen));
        body.put(magic);
        body.put(version);
        body.putShort(Opcode.PING.code);
        body.putShort((short) 1);
        body.putShort(flags);
        body.putLong(1);
        body.putInt(payloadLen);
        body.putInt(0);
        body.putShort((short) headerLen);
        for (int i = 0; i < headerLen; i++) {
            body.put((byte) i);
        }
        return body.array();
    }
}
