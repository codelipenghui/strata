package io.strata.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.strata.common.ChunkId;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
    void lookupFileRespRejectsMalformedOwnerEpochTag() {
        byte[] base = new Messages.LookupFileResp("test", "/f",
                Messages.WritePolicy.DEFAULT, (byte) 0, List.of()).encode();
        BufWriter malformedTags = new BufWriter();
        TaggedFields.of(Map.of(0, new byte[] {1})).writeTo(malformedTags);
        byte[] tags = malformedTags.toBytes();
        byte[] malformed = new byte[base.length - 1 + tags.length];
        System.arraycopy(base, 0, malformed, 0, base.length - 1);
        System.arraycopy(tags, 0, malformed, base.length - 1, tags.length);

        ByteBuffer buffer = ByteBuffer.wrap(malformed);
        Resp.check(buffer);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> Messages.LookupFileResp.decode(buffer));
        assertTrue(error.getMessage().contains("ownerEpoch"), "got: " + error.getMessage());
    }

    @Test
    void confirmOrphanRejectsAmbiguousOrExtendedWireValues() {
        StrataNamespace namespace = StrataNamespace.of("test");
        ChunkId chunkId = new ChunkId(FileId.of(0x0102030405060708L), 3);
        Messages.ConfirmOrphan request = new Messages.ConfirmOrphan(namespace, chunkId, 7);

        byte[] requestBytes = request.encode();
        assertThrows(BufferUnderflowException.class, () -> Messages.ConfirmOrphan.decode(
                ByteBuffer.wrap(Arrays.copyOf(requestBytes, requestBytes.length - 1))));
        assertThrows(IllegalArgumentException.class, () -> Messages.ConfirmOrphan.decode(
                ByteBuffer.wrap(Arrays.copyOf(requestBytes, requestBytes.length + 1))));

        assertThrows(NullPointerException.class,
                () -> new Messages.ConfirmOrphan(null, chunkId, 7));
        assertThrows(NullPointerException.class,
                () -> new Messages.ConfirmOrphan(namespace, null, 7));
        assertThrows(IllegalArgumentException.class,
                () -> new Messages.ConfirmOrphan(namespace, chunkId, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new Messages.ConfirmOrphan(namespace, chunkId, -1));
        BufWriter zeroNodeId = new BufWriter();
        zeroNodeId.namespace(namespace).chunkId(chunkId).u32(0).noTags();
        assertThrows(IllegalArgumentException.class,
                () -> Messages.ConfirmOrphan.decode(ByteBuffer.wrap(zeroNodeId.toBytes())));
        assertThrows(IllegalArgumentException.class,
                () -> new Messages.ConfirmOrphanResp(false, true, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new Messages.ConfirmOrphanResp(true, false, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new Messages.ConfirmOrphanResp(true, false, -1));

        assertConfirmOrphanRespDecodeFails(new byte[] {2, 0, 0, 0, 0, 0, 0, 0, 0, 1});
        assertConfirmOrphanRespDecodeFails(new byte[] {1, 2, 0, 0, 0, 0, 0, 0, 0, 1});
        assertConfirmOrphanRespDecodeFails(new byte[] {0, 1, 0, 0, 0, 0, 0, 0, 0, 1});
        assertConfirmOrphanRespDecodeFails(new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        assertConfirmOrphanRespDecodeFails(new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0});
        assertThrows(BufferUnderflowException.class,
                () -> Messages.ConfirmOrphanResp.decode(ByteBuffer.wrap(new byte[] {1, 0})));

        BufWriter extended = new BufWriter();
        extended.u8(1).u8(0).u64(7);
        TaggedFields.of(Map.of(99, new byte[] {4, 5})).writeTo(extended);
        assertEquals(new Messages.ConfirmOrphanResp(true, false, 7),
                Messages.ConfirmOrphanResp.decode(ByteBuffer.wrap(extended.toBytes())));

        BufWriter malformedTags = new BufWriter();
        malformedTags.u8(1).u8(0).u64(7).varint(1).varint(99).varint(2).u8(4);
        assertConfirmOrphanRespDecodeFails(malformedTags.toBytes());
    }

    @Test
    void installOwnerEpochRequiresAnExactPositiveEpochAndExtensibleTags() {
        StrataNamespace namespace = StrataNamespace.of("test");
        Messages.InstallOwnerEpoch request = new Messages.InstallOwnerEpoch(namespace, 7);
        byte[] encoded = request.encode();

        assertEquals(request, Messages.InstallOwnerEpoch.decode(ByteBuffer.wrap(encoded)));
        assertThrows(BufferUnderflowException.class, () -> Messages.InstallOwnerEpoch.decode(
                ByteBuffer.wrap(Arrays.copyOf(encoded, encoded.length - 1))));
        assertThrows(NullPointerException.class,
                () -> new Messages.InstallOwnerEpoch(null, 7));
        assertThrows(IllegalArgumentException.class,
                () -> new Messages.InstallOwnerEpoch(namespace, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new Messages.InstallOwnerEpoch(namespace, -1));

        BufWriter zeroEpoch = new BufWriter();
        zeroEpoch.namespace(namespace).u64(0).noTags();
        assertThrows(IllegalArgumentException.class,
                () -> Messages.InstallOwnerEpoch.decode(ByteBuffer.wrap(zeroEpoch.toBytes())));

        BufWriter extended = new BufWriter();
        extended.namespace(namespace).u64(7);
        TaggedFields.of(Map.of(99, new byte[] {4, 5})).writeTo(extended);
        assertEquals(request, Messages.InstallOwnerEpoch.decode(ByteBuffer.wrap(extended.toBytes())));
    }

    @Test
    void sealChunkRejectsMalformedOwnerEpochTag() {
        StrataNamespace namespace = StrataNamespace.of("test");
        ChunkId chunkId = new ChunkId(FileId.of(0x0102030405060708L), 3);
        byte[] base = new Messages.SealChunk(chunkId, 7, 11, namespace).encode();
        BufWriter malformedTags = new BufWriter();
        TaggedFields.of(Map.of(0, new byte[] {1})).writeTo(malformedTags);
        byte[] tags = malformedTags.toBytes();
        byte[] malformed = new byte[base.length - 1 + tags.length];
        System.arraycopy(base, 0, malformed, 0, base.length - 1);
        System.arraycopy(tags, 0, malformed, base.length - 1, tags.length);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> Messages.SealChunk.decode(ByteBuffer.wrap(malformed)));
        assertTrue(error.getMessage().contains("ownerEpoch"), "got: " + error.getMessage());
    }

    @Test
    void appendDecodeReadsOwnedDirectHeader() {
        StrataNamespace namespace = StrataNamespace.of("test");
        ChunkId chunkId = new ChunkId(FileId.of(0x0102030405060708L), 3);
        BufWriter header = new BufWriter();
        header.chunkId(chunkId).i32(7).u64(11).u64(9).namespace(namespace);
        TaggedFields.of(Map.of(0, new byte[] {1}, 99, new byte[] {8, 9})).writeTo(header);
        byte[] encoded = header.toBytes();

        ByteBuf owner = Unpooled.directBuffer(encoded.length + 5);
        owner.writeZero(3);
        int headerIndex = owner.writerIndex();
        owner.writeBytes(encoded);
        owner.writeZero(2);
        Frame frame = Frame.fromOwnedBuffer(Opcode.APPEND.code, (short) 1, (short) 0, 17L,
                owner, headerIndex, encoded.length, headerIndex + encoded.length, 0, 0);
        try {
            Messages.Append.AppendFields fields = Messages.Append.decodeFields(frame);
            assertEquals(chunkId, fields.chunkId());
            assertEquals(7, fields.writeEpoch());
            assertEquals(11, fields.baseOffset());
            assertEquals(9, fields.durableOffset());
            assertEquals(namespace, fields.namespace());
            assertTrue(fields.recovery());

            Messages.Append decoded = Messages.Append.decode(frame);

            assertEquals(new Messages.Append(chunkId, 7, 11, 9, namespace, true), decoded);
        } finally {
            frame.close();
        }
    }

    @Test
    void decoderVerifiesAppendPayloadWithoutCachedInternalReadBuffer() {
        StrataNamespace namespace = StrataNamespace.of("test");
        ChunkId chunkId = new ChunkId(FileId.of(0x0102030405060708L), 3);
        byte[] header = new Messages.Append(chunkId, 7, 11, 9, namespace).encode();
        byte[] payload = new byte[] {1, 2, 3, 4};
        Frame request = Frame.request(Opcode.APPEND, header, ByteBuffer.wrap(payload), 17L);
        EmbeddedChannel channel = new EmbeddedChannel(new NettyFrameCodec.Encoder(), new NettyFrameCodec.Decoder());
        try {
            assertTrue(channel.writeOutbound(request));
            ByteBuf wire = channel.readOutbound();
            assertTrue(channel.writeInbound(wire));
            Frame decoded = channel.readInbound();
            try {
                assertTrue(decoded.ownsBuffer());
                ByteBuffer internal = decoded.payloadInternalReadBuffer();
                assertNotSame(internal, decoded.payloadInternalReadBuffer());
                assertEquals(payload.length, internal.remaining());
                byte[] got = new byte[payload.length];
                internal.duplicate().get(got);
                assertArrayEquals(payload, got);

                byte[] publicRead = new byte[payload.length];
                decoded.payloadReadBuffer().get(publicRead);
                assertArrayEquals(payload, publicRead);
            } finally {
                decoded.close();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void decoderRetainsParentCumulationForMultipleFrames() {
        StrataNamespace namespace = StrataNamespace.of("test");
        ChunkId chunkId = new ChunkId(FileId.of(0x0102030405060708L), 3);
        byte[] header = new Messages.Append(chunkId, 7, 11, 9, namespace).encode();
        byte[] firstPayload = new byte[] {1, 2, 3, 4};
        byte[] secondPayload = new byte[] {5, 6, 7};
        Frame firstRequest = Frame.request(Opcode.APPEND, header, ByteBuffer.wrap(firstPayload), 17L);
        Frame secondRequest = Frame.request(Opcode.APPEND, header, ByteBuffer.wrap(secondPayload), 18L);
        EmbeddedChannel channel = new EmbeddedChannel(new NettyFrameCodec.Encoder(), new NettyFrameCodec.Decoder());
        ByteBuf firstWire = null;
        ByteBuf secondWire = null;
        ByteBuf combined = null;
        Frame first = null;
        Frame second = null;
        try {
            assertTrue(channel.writeOutbound(firstRequest));
            assertTrue(channel.writeOutbound(secondRequest));
            firstWire = channel.readOutbound();
            secondWire = channel.readOutbound();
            combined = Unpooled.directBuffer(firstWire.readableBytes() + secondWire.readableBytes());
            combined.writeBytes(firstWire, firstWire.readerIndex(), firstWire.readableBytes());
            combined.writeBytes(secondWire, secondWire.readerIndex(), secondWire.readableBytes());
            firstWire.release();
            firstWire = null;
            secondWire.release();
            secondWire = null;

            assertTrue(channel.writeInbound(combined));
            combined = null;
            first = channel.readInbound();
            second = channel.readInbound();
            assertTrue(first != null);
            assertTrue(second != null);
            assertTrue(first.ownsBuffer());
            assertTrue(second.ownsBuffer());
            assertEquals(2, first.ownerRefCnt());
            assertEquals(2, second.ownerRefCnt());

            byte[] gotFirst = new byte[firstPayload.length];
            first.payloadInternalReadBuffer().duplicate().get(gotFirst);
            assertArrayEquals(firstPayload, gotFirst);
            first.close();
            assertEquals(1, second.ownerRefCnt());

            byte[] gotSecond = new byte[secondPayload.length];
            second.payloadInternalReadBuffer().duplicate().get(gotSecond);
            assertArrayEquals(secondPayload, gotSecond);
        } finally {
            if (first != null) first.close();
            if (second != null) second.close();
            if (firstWire != null) firstWire.release();
            if (secondWire != null) secondWire.release();
            if (combined != null) combined.release();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void openChunkDecodeReadsOwnedDirectHeader() {
        StrataNamespace namespace = StrataNamespace.of("test");
        ChunkId chunkId = new ChunkId(FileId.of(0x0102030405060708L), 3);
        Messages.OpenChunk expected = new Messages.OpenChunk(
                chunkId, 7, true, 1L << 30, 1_718_000_000_000L, namespace);
        BufWriter header = new BufWriter();
        header.chunkId(chunkId).i32(7).u8(1).u64(1L << 30).u64(1_718_000_000_000L)
                .namespace(namespace);
        TaggedFields.of(Map.of(99, new byte[] {8, 9})).writeTo(header);
        byte[] encoded = header.toBytes();

        ByteBuf owner = Unpooled.directBuffer(encoded.length + 5);
        owner.writeZero(3);
        int headerIndex = owner.writerIndex();
        owner.writeBytes(encoded);
        owner.writeZero(2);
        Frame frame = Frame.fromOwnedBuffer(Opcode.OPEN_CHUNK.code, (short) 1, (short) 0, 17L,
                owner, headerIndex, encoded.length, headerIndex + encoded.length, 0, 0);
        try {
            assertEquals(expected, Messages.OpenChunk.decode(frame));
        } finally {
            frame.close();
        }
    }

    @Test
    void openChunkOwnedDecodeRejectsBadBoolean() {
        StrataNamespace namespace = StrataNamespace.of("test");
        ChunkId chunkId = new ChunkId(FileId.of(0x0102030405060708L), 3);
        BufWriter header = new BufWriter();
        header.chunkId(chunkId).i32(7).u8(2).u64(1L << 30).u64(1_718_000_000_000L)
                .namespace(namespace).noTags();
        byte[] encoded = header.toBytes();

        ByteBuf owner = Unpooled.directBuffer(encoded.length);
        owner.writeBytes(encoded);
        Frame frame = Frame.fromOwnedBuffer(Opcode.OPEN_CHUNK.code, (short) 1, (short) 0, 17L,
                owner, 0, encoded.length, encoded.length, 0, 0);
        try {
            var error = assertThrows(IllegalArgumentException.class, () -> Messages.OpenChunk.decode(frame));
            assertTrue(error.getMessage().contains("boolean"));
        } finally {
            frame.close();
        }
    }

    @Test
    void readDecodeReadsOwnedDirectHeader() {
        StrataNamespace namespace = StrataNamespace.of("test");
        ChunkId chunkId = new ChunkId(FileId.of(0x0102030405060708L), 3);
        BufWriter header = new BufWriter();
        header.chunkId(chunkId).u64(11).u32(65536).namespace(namespace);
        TaggedFields.of(Map.of(99, new byte[] {8, 9})).writeTo(header);
        byte[] encoded = header.toBytes();

        ByteBuf owner = Unpooled.directBuffer(encoded.length + 5);
        owner.writeZero(3);
        int headerIndex = owner.writerIndex();
        owner.writeBytes(encoded);
        owner.writeZero(2);
        Frame frame = Frame.fromOwnedBuffer(Opcode.READ.code, (short) 1, (short) 0, 17L,
                owner, headerIndex, encoded.length, headerIndex + encoded.length, 0, 0);
        try {
            Messages.Read.ReadFields fields = Messages.Read.decodeFields(frame);
            assertEquals(chunkId.fileId().id(), fields.fileId());
            assertEquals(chunkId.index(), fields.chunkIndex());
            assertEquals(11, fields.offset());
            assertEquals(65536, fields.maxBytes());
            assertEquals(namespace, fields.namespace());
            assertEquals(chunkId, fields.chunkId());
        } finally {
            frame.close();
        }
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
        assertNull(Opcode.fromCode((short) 0x020A));
        assertEquals(Opcode.CONFIRM_ORPHAN, Opcode.fromCode((short) 0x020B));
        assertNull(Opcode.fromCode((short) 0x7FFF));
    }

    @Test
    void commandRequestAndHeartbeatOwnerEpochTagsRejectMalformedLayouts() {
        ChunkId chunkId = new ChunkId(FileId.of(1), 0);
        StrataNamespace namespace = StrataNamespace.of("test");

        BufWriter legacyCommand = new BufWriter();
        Messages.Command.write(legacyCommand, new Messages.DeleteCmd(9, List.of(chunkId), namespace));
        assertThrows(RuntimeException.class,
                () -> Messages.Command.readRequest(ByteBuffer.wrap(legacyCommand.toBytes())));

        BufWriter truncated = new BufWriter();
        truncated.varint(1).u64(9);
        assertThrows(IllegalArgumentException.class,
                () -> Messages.HeartbeatResp.decode(heartbeatWithOwnerEpochTag(chunkId, namespace,
                        truncated.toBytes())));

        BufWriter duplicate = new BufWriter();
        duplicate.varint(2).u64(9).u64(7).u64(9).u64(8);
        var duplicateError = assertThrows(IllegalArgumentException.class,
                () -> Messages.HeartbeatResp.decode(heartbeatWithOwnerEpochTag(chunkId, namespace,
                        duplicate.toBytes())));
        assertTrue(duplicateError.getMessage().contains("duplicate"));

        BufWriter unknown = new BufWriter();
        unknown.varint(1).u64(10).u64(7);
        var unknownError = assertThrows(IllegalArgumentException.class,
                () -> Messages.HeartbeatResp.decode(heartbeatWithOwnerEpochTag(chunkId, namespace,
                        unknown.toBytes())));
        assertTrue(unknownError.getMessage().contains("unknown command ids"));
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

    private static ByteBuffer heartbeatWithOwnerEpochTag(ChunkId chunkId, StrataNamespace namespace,
                                                         byte[] ownerEpochTag) {
        BufWriter w = new BufWriter();
        w.u64(100).varint(1);
        Messages.Command.write(w, new Messages.DeleteCmd(9, List.of(chunkId), namespace));
        TaggedFields.of(Map.of(Messages.HeartbeatResp.TAG_COMMAND_OWNER_EPOCHS, ownerEpochTag)).writeTo(w);
        return ByteBuffer.wrap(w.toBytes());
    }

    @Test
    void smallFrameAndBufferHelpers() throws Exception {
        Frame request = Frame.request(Opcode.PING, Messages.okHeader(), null, 11);
        assertFalse(request.isResponse());
        assertThrows(ReadOnlyBufferException.class, () -> request.headerSlice().put((byte) 0));
        Frame response = Frame.response(request, Messages.okHeader(), null);
        assertTrue(response.isResponse());
        byte[] borrowed = "borrowed".getBytes();
        AtomicBoolean released = new AtomicBoolean(false);
        Frame borrowedResponse = Frame.response(request, Messages.okHeader(), ByteBuffer.wrap(borrowed),
                () -> released.set(true));
        assertTrue(borrowedResponse.payloadView().hasArray());
        assertThrows(ReadOnlyBufferException.class, () -> borrowedResponse.payloadSlice().put((byte) 0));
        borrowedResponse.close();
        assertTrue(released.get());

        byte[] borrowedBytes = {1, 2, 3, 4};
        AtomicBoolean bytesReleased = new AtomicBoolean(false);
        Frame bytesResponse = Frame.responseBytes(request, Messages.okHeader(), borrowedBytes, 3,
                () -> bytesReleased.set(true));
        assertEquals(3, bytesResponse.payloadLength());
        byte[] bytesPayload = new byte[3];
        bytesResponse.payloadSlice().get(bytesPayload);
        assertArrayEquals(new byte[]{1, 2, 3}, bytesPayload);
        ByteArrayOutputStream encodedBytesResponse = new ByteArrayOutputStream();
        FrameIO.write(new DataOutputStream(encodedBytesResponse), bytesResponse);
        Frame decodedBytesResponse = FrameIO.read(new DataInputStream(
                new ByteArrayInputStream(encodedBytesResponse.toByteArray())));
        assertEquals(Crc.of(borrowedBytes, 0, 3), decodedBytesResponse.payloadCrc());
        bytesResponse.close();
        assertTrue(bytesReleased.get());

        ByteBuf directBytes = NettyFrameCodec.encodeBytesResponse(
                UnpooledByteBufAllocator.DEFAULT, request, Messages.okHeader(), borrowedBytes, 3);
        try {
            byte[] directWire = new byte[directBytes.readableBytes()];
            directBytes.readBytes(directWire);
            Frame decodedDirectBytes = FrameIO.read(new DataInputStream(new ByteArrayInputStream(directWire)));
            assertEquals(Crc.of(borrowedBytes, 0, 3), decodedDirectBytes.payloadCrc());
            byte[] directPayload = new byte[decodedDirectBytes.payloadLength()];
            decodedDirectBytes.payloadSlice().get(directPayload);
            assertArrayEquals(new byte[]{1, 2, 3}, directPayload);
        } finally {
            directBytes.release();
        }

        ByteBuf directReadBytes = NettyFrameCodec.encodeTwoU64BytesResponse(
                UnpooledByteBufAllocator.DEFAULT, request, 9, 7, borrowedBytes, 3);
        try {
            byte[] directWire = new byte[directReadBytes.readableBytes()];
            directReadBytes.readBytes(directWire);
            Frame decodedReadBytes = FrameIO.read(new DataInputStream(new ByteArrayInputStream(directWire)));
            ByteBuffer readHeader = decodedReadBytes.headerSlice();
            Resp.check(readHeader);
            assertEquals(new Messages.ReadResp(9, 7), Messages.ReadResp.decode(readHeader));
            assertEquals(Crc.of(borrowedBytes, 0, 3), decodedReadBytes.payloadCrc());
            byte[] directPayload = new byte[decodedReadBytes.payloadLength()];
            decodedReadBytes.payloadSlice().get(directPayload);
            assertArrayEquals(new byte[]{1, 2, 3}, directPayload);
        } finally {
            directReadBytes.release();
        }

        ByteBuf compositeBytes = NettyFrameCodec.encodeBytesResponseComposite(
                UnpooledByteBufAllocator.DEFAULT, request, Messages.okHeader(), borrowedBytes, 3);
        try {
            byte[] directWire = new byte[compositeBytes.readableBytes()];
            compositeBytes.readBytes(directWire);
            Frame decodedCompositeBytes = FrameIO.read(new DataInputStream(new ByteArrayInputStream(directWire)));
            assertEquals(Crc.of(borrowedBytes, 0, 3), decodedCompositeBytes.payloadCrc());
            byte[] compositePayload = new byte[decodedCompositeBytes.payloadLength()];
            decodedCompositeBytes.payloadSlice().get(compositePayload);
            assertArrayEquals(new byte[]{1, 2, 3}, compositePayload);
        } finally {
            compositeBytes.release();
        }

        ByteBuf compositeReadBytes = NettyFrameCodec.encodeTwoU64BytesResponseComposite(
                UnpooledByteBufAllocator.DEFAULT, request, 9, 7, borrowedBytes, 3);
        try {
            byte[] directWire = new byte[compositeReadBytes.readableBytes()];
            compositeReadBytes.readBytes(directWire);
            Frame decodedCompositeRead = FrameIO.read(new DataInputStream(new ByteArrayInputStream(directWire)));
            ByteBuffer readHeader = decodedCompositeRead.headerSlice();
            Resp.check(readHeader);
            assertEquals(new Messages.ReadResp(9, 7), Messages.ReadResp.decode(readHeader));
            assertEquals(Crc.of(borrowedBytes, 0, 3), decodedCompositeRead.payloadCrc());
            byte[] compositePayload = new byte[decodedCompositeRead.payloadLength()];
            decodedCompositeRead.payloadSlice().get(compositePayload);
            assertArrayEquals(new byte[]{1, 2, 3}, compositePayload);
        } finally {
            compositeReadBytes.release();
        }

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

    @Test
    void ownedFramesReadFromOwnerBufferAndReleaseIt() throws Exception {
        byte[] bytes = {9, 1, 2, 3, 4, 5, 9};
        ByteBuf owner = Unpooled.wrappedBuffer(bytes);
        Frame frame = Frame.fromOwnedBuffer(Opcode.PING.code, (short) 1, Frame.FLAG_PAYLOAD_CRC, 99,
                owner, 1, 2, 3, 3, Crc.of(bytes, 3, 3));

        assertTrue(frame.ownsBuffer());
        assertEquals(1, frame.ownerRefCnt());
        assertEquals(2, frame.headerLength());
        assertEquals(3, frame.payloadLength());
        assertEquals(Crc.of(bytes, 3, 3), frame.payloadCrc());

        byte[] header = new byte[2];
        frame.headerSlice().get(header);
        assertArrayEquals(new byte[]{1, 2}, header);
        byte[] payload = new byte[3];
        frame.payloadSlice().get(payload);
        assertArrayEquals(new byte[]{3, 4, 5}, payload);
        assertThrows(ReadOnlyBufferException.class, () -> frame.payloadSlice().put((byte) 0));
        byte[] copied = new byte[5];
        frame.copyPayloadTo(1, copied, 2, 2);
        assertArrayEquals(new byte[] {0, 0, 4, 5, 0}, copied);

        Path tmp = Files.createTempFile("strata-frame-payload", ".bin");
        try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            frame.writePayloadTo(channel, 0);
            ByteBuffer filePayload = ByteBuffer.allocate(3);
            channel.read(filePayload, 0);
            assertArrayEquals(new byte[] {3, 4, 5}, filePayload.array());
        } finally {
            Files.deleteIfExists(tmp);
        }

        frame.close();
        assertEquals(0, frame.ownerRefCnt());
    }

    @Test
    void ownedRequestFramesReuseWrapperAfterClose() {
        assertOwnedRequestFrameReused(Opcode.APPEND);
        assertOwnedRequestFrameReused(Opcode.READ);
        assertOwnedRequestFrameReused(Opcode.READ_RECOVERY);
    }

    @Test
    void ownedRequestFrameClosedOnDifferentThreadDoesNotEnterWrapperPool() throws Exception {
        byte[] firstBytes = {9, 1, 2, 3, 4, 5, 9};
        ByteBuf firstOwner = Unpooled.wrappedBuffer(firstBytes);
        Frame first = Frame.fromOwnedBuffer(Opcode.APPEND.code, (short) 1, (short) 0, 99,
                firstOwner, 1, 2, 3, 3, 0);
        Thread closer = new Thread(first::close);
        closer.start();
        closer.join();
        assertEquals(0, first.ownerRefCnt());

        byte[] secondBytes = {8, 6, 7, 5, 3, 0, 9};
        ByteBuf secondOwner = Unpooled.wrappedBuffer(secondBytes);
        Frame second = Frame.fromOwnedBuffer(Opcode.APPEND.code, (short) 1, (short) 0, 100,
                secondOwner, 1, 2, 3, 3, 0);
        try {
            assertNotSame(first, second);
        } finally {
            second.close();
        }
    }

    private static void assertOwnedRequestFrameReused(Opcode opcode) {
        byte[] firstBytes = {9, 1, 2, 3, 4, 5, 9};
        ByteBuf firstOwner = Unpooled.wrappedBuffer(firstBytes);
        Frame first = Frame.fromOwnedBuffer(opcode.code, (short) 1, (short) 0, 99,
                firstOwner, 1, 2, 3, 3, 0);
        assertTrue(first.ownsBuffer());
        assertEquals(1, first.ownerRefCnt());
        first.close();
        assertEquals(0, first.ownerRefCnt());

        byte[] secondBytes = {8, 6, 7, 5, 3, 0, 9};
        ByteBuf secondOwner = Unpooled.wrappedBuffer(secondBytes);
        Frame second = Frame.fromOwnedBuffer(opcode.code, (short) 1, (short) 0, 100,
                secondOwner, 1, 2, 3, 3, 0);
        try {
            assertSame(first, second);
            assertTrue(second.ownsBuffer());
            assertEquals(1, second.ownerRefCnt());
            byte[] payload = new byte[3];
            second.payloadSlice().get(payload);
            assertArrayEquals(new byte[] {5, 3, 0}, payload);
        } finally {
            second.close();
        }
        assertEquals(0, second.ownerRefCnt());
    }

    @Test
    void ownedNonDataHotPathFramesDoNotEnterRequestWrapperPool() {
        assertOwnedFrameNotReused(Opcode.PING, (short) 0);
        assertOwnedFrameNotReused(Opcode.READ, Frame.FLAG_RESPONSE);
    }

    private static void assertOwnedFrameNotReused(Opcode opcode, short flags) {
        byte[] firstBytes = {9, 1, 2, 3, 4, 5, 9};
        ByteBuf firstOwner = Unpooled.wrappedBuffer(firstBytes);
        Frame first = Frame.fromOwnedBuffer(opcode.code, (short) 1, flags, 99,
                firstOwner, 1, 2, 3, 3, 0);
        first.close();
        assertEquals(0, first.ownerRefCnt());

        byte[] secondBytes = {8, 6, 7, 5, 3, 0, 9};
        ByteBuf secondOwner = Unpooled.wrappedBuffer(secondBytes);
        Frame second = Frame.fromOwnedBuffer(opcode.code, (short) 1, flags, 100,
                secondOwner, 1, 2, 3, 3, 0);
        try {
            assertNotSame(first, second);
            assertTrue(second.ownsBuffer());
            assertEquals(1, second.ownerRefCnt());
        } finally {
            second.close();
        }
        assertEquals(0, second.ownerRefCnt());
    }

    @Test
    void ownedInternalReadBuffersExposeCorrectRangesWithoutChangingPublicSlices() {
        byte[] bytes = {9, 1, 2, 3, 4, 5, 9};
        ByteBuf owner = Unpooled.directBuffer(bytes.length);
        owner.writeBytes(bytes);
        Frame frame = Frame.fromOwnedBuffer(Opcode.PING.code, (short) 1, Frame.FLAG_PAYLOAD_CRC, 99,
                owner, 1, 2, 3, 3, Crc.of(bytes, 3, 3));
        try {
            ByteBuffer headerRead = frame.headerReadBuffer();
            assertEquals(2, headerRead.remaining());
            int headerPosition = headerRead.position();
            int headerLimit = headerRead.limit();
            assertEquals(1, headerRead.get(headerPosition));

            ByteBuffer payloadRead = frame.payloadReadBuffer();
            assertEquals(3, payloadRead.remaining());
            assertEquals(headerPosition, headerRead.position());
            assertEquals(headerLimit, headerRead.limit());
            assertEquals(1, headerRead.get(headerPosition));
            byte[] payload = new byte[3];
            payloadRead.get(payload);
            assertArrayEquals(new byte[] {3, 4, 5}, payload);

            byte[] header = new byte[2];
            frame.headerSlice().get(header);
            assertArrayEquals(new byte[] {1, 2}, header);
            assertThrows(ReadOnlyBufferException.class, () -> frame.payloadSlice().put((byte) 0));

            Frame heapCopy = frame.copyToHeap();
            frame.close();
            assertFalse(heapCopy.ownsBuffer());
            assertEquals(Crc.of(bytes, 3, 3), heapCopy.payloadCrc());
            byte[] copiedHeader = new byte[2];
            heapCopy.headerSlice().get(copiedHeader);
            assertArrayEquals(new byte[] {1, 2}, copiedHeader);
            byte[] copiedPayload = new byte[3];
            heapCopy.payloadSlice().get(copiedPayload);
            assertArrayEquals(new byte[] {3, 4, 5}, copiedPayload);
            heapCopy.close();
        } finally {
            frame.close();
        }
        assertEquals(0, frame.ownerRefCnt());
    }

    private static void assertFrameReadFails(byte[] wire, String messageFragment) {
        IOException e = assertThrows(IOException.class,
                () -> FrameIO.read(new DataInputStream(new ByteArrayInputStream(wire))));
        assertTrue(e.getMessage().contains(messageFragment), "got: " + e.getMessage());
    }

    private static void assertConfirmOrphanRespDecodeFails(byte[] wire) {
        assertThrows(IllegalArgumentException.class,
                () -> Messages.ConfirmOrphanResp.decode(ByteBuffer.wrap(wire)));
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
