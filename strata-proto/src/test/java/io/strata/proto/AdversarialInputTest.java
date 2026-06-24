package io.strata.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.strata.common.ChunkId;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Malformed/adversarial wire input must fail as typed protocol errors (IOException at the frame
 * layer, error responses at the message layer) — never as unchecked exceptions that silently
 * kill a connection thread with no log and no response.
 */
class AdversarialInputTest {

    @Test
    void negativePayloadLengthIsRejectedAsIOException() {
        // frameLen=26+1-1=26 passes the equality check unless payloadLen<0 is guarded
        ByteBuffer frame = ByteBuffer.allocate(4 + 26 + 1);
        frame.putInt(26 + 1 + -1);              // frameLength
        frame.put(Frame.MAGIC).put(Frame.FRAME_VERSION);
        frame.putShort(Opcode.PING.code).putShort((short) 1).putShort((short) 0);
        frame.putLong(7L);                       // correlationId
        frame.putInt(-1);                        // payloadLength: NEGATIVE
        frame.putInt(0);                         // payloadCrc
        frame.putShort((short) 1);               // headerLength
        frame.put((byte) 0);                     // header byte
        IOException e = assertThrows(IOException.class,
                () -> FrameIO.read(new DataInputStream(new ByteArrayInputStream(frame.array()))));
        assertTrue(e.getMessage().contains("payload"), "got: " + e.getMessage());
    }

    @Test
    void adversarialListCountIsRejectedNotNegativeCapacity() {
        // varint 0xFFFFFFFF narrows to (int) -1: must be a typed protocol error, not
        // ArrayList's "Illegal Capacity: -1"
        BufWriter w = new BufWriter();
        w.varint(0xFFFFFFFFL); // count
        w.noTags();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Messages.DeleteChunks.decode(ByteBuffer.wrap(w.toBytes())));
        assertTrue(e.getMessage().contains("count"), "got: " + e.getMessage());

        // sane messages still decode
        var del = new Messages.DeleteChunks(java.util.List.of(new ChunkId(FileId.of(1), 0)));
        assertEquals(del, Messages.DeleteChunks.decode(ByteBuffer.wrap(del.encode())));
    }

    @Test
    void adversarialCompletedCommandCountIsRejected() {
        BufWriter completed = new BufWriter();
        completed.varint(1_000_001);

        BufWriter heartbeat = new BufWriter();
        heartbeat.u32(1).u64(2).u64(3).u64(4);
        heartbeat.varint(0);
        heartbeat.u32(0);
        TaggedFields.of(Map.of(Messages.NodeHeartbeat.TAG_COMPLETED_COMMANDS, completed.toBytes()))
                .writeTo(heartbeat);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Messages.NodeHeartbeat.decode(ByteBuffer.wrap(heartbeat.toBytes())));
        assertTrue(e.getMessage().contains("count"), "got: " + e.getMessage());
    }

    @Test
    void helloWithIncompatibleFrameVersionGetsTypedErrorResponse() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0,
                req -> ScpServer.ok(req, Messages.okHeader(), null))) {
            try (Socket s = new Socket("127.0.0.1", server.port())) {
                s.setSoTimeout(5_000);
                // HELLO advertising frame versions [99,99] — no overlap with version 1
                BufWriter h = new BufWriter();
                h.u16(99).u16(99).u8(ScpClient.KIND_TOOL).u64(0).string("bad-client");
                h.noTags();
                FrameIO.write(new DataOutputStream(s.getOutputStream()),
                        Frame.request(Opcode.HELLO, h.toBytes(), null, 1));

                Frame resp = FrameIO.read(new DataInputStream(s.getInputStream()));
                assertNotNull(resp, "server must answer with a typed error, not a silent close");
                ByteBuffer rh = resp.headerSlice();
                var e = assertThrows(io.strata.common.ScpException.class, () -> Resp.check(rh));
                assertEquals(ErrorCode.UNSUPPORTED_VERSION, e.code());
            }
        }
    }

    @Test
    void nettyDecoderBuffersPartialFramesAndRejectsMalformedFrames() {
        EmbeddedChannel partialLength = new EmbeddedChannel(new NettyFrameCodec.Decoder());
        try {
            assertFalse(partialLength.writeInbound(Unpooled.wrappedBuffer(new byte[] {0, 1})));
            assertNull(partialLength.readInbound());
        } finally {
            partialLength.finishAndReleaseAll();
        }

        EmbeddedChannel partialFrame = new EmbeddedChannel(new NettyFrameCodec.Decoder());
        try {
            ByteBuf buf = Unpooled.buffer();
            buf.writeInt(Frame.PREAMBLE_AFTER_LEN);
            buf.writeByte(Frame.MAGIC);
            assertFalse(partialFrame.writeInbound(buf));
            assertNull(partialFrame.readInbound());
        } finally {
            partialFrame.finishAndReleaseAll();
        }

        assertDecoderRejects(rawLengthFrame(Frame.PREAMBLE_AFTER_LEN - 1), "bad frame length");
        assertDecoderRejects(rawLengthFrame(FrameIO.MAX_FRAME_BYTES + 1), "bad frame length");
        assertDecoderRejects(rawFrame(Frame.PREAMBLE_AFTER_LEN, (byte) 0, Frame.FRAME_VERSION,
                (short) 0, 0, 0, 0, new byte[0]), "bad magic");
        assertDecoderRejects(rawFrame(Frame.PREAMBLE_AFTER_LEN, Frame.MAGIC, (byte) 99,
                (short) 0, 0, 0, 0, new byte[0]), "unsupported frame version");
        assertDecoderRejects(rawFrame(Frame.PREAMBLE_AFTER_LEN, Frame.MAGIC, Frame.FRAME_VERSION,
                (short) 0, -1, 0, 0, new byte[0]), "negative payload length");
        assertDecoderRejects(rawFrame(Frame.PREAMBLE_AFTER_LEN + 1, Frame.MAGIC, Frame.FRAME_VERSION,
                (short) 0, 0, 0, 0, new byte[] {0}), "frame length mismatch");
        assertDecoderRejects(rawFrame(Frame.PREAMBLE_AFTER_LEN + 1, Frame.MAGIC, Frame.FRAME_VERSION,
                Frame.FLAG_PAYLOAD_CRC, 1, Crc.of(ByteBuffer.wrap(new byte[] {2})), 0, new byte[] {1}),
                "payload crc mismatch");
    }

    @Test
    void nettyDecoderEmitsOwnedFrameReleasedByFrameClose() {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyFrameCodec.Decoder());
        byte[] payload = new byte[] {1, 2, 3};
        try {
            assertTrue(channel.writeInbound(rawFrame(Frame.PREAMBLE_AFTER_LEN + payload.length,
                    Frame.MAGIC, Frame.FRAME_VERSION, Frame.FLAG_PAYLOAD_CRC, payload.length,
                    Crc.of(ByteBuffer.wrap(payload)), 0, payload)));
            Frame frame = channel.readInbound();
            assertTrue(frame.ownsBuffer());
            assertEquals(1, frame.ownerRefCnt());
            byte[] decoded = new byte[frame.payloadLength()];
            frame.payloadSlice().get(decoded);
            assertArrayEquals(payload, decoded);

            frame.close();
            assertEquals(0, frame.ownerRefCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void nettyEncoderRejectsOversizedHeadersBeforeWriting() {
        EmbeddedChannel encoder = new EmbeddedChannel(new NettyFrameCodec.Encoder());
        try {
            EncoderException e = assertThrows(EncoderException.class, () -> encoder.writeOutbound(
                    Frame.request(Opcode.PING, new byte[0x1_0000], null, 1)));
            assertTrue(e.getCause() instanceof IOException);
            assertTrue(e.getCause().getMessage().contains("header too large"));
        } finally {
            encoder.finishAndReleaseAll();
        }
    }

    private static void assertDecoderRejects(ByteBuf frame, String expectedMessage) {
        EmbeddedChannel channel = new EmbeddedChannel(new NettyFrameCodec.Decoder());
        try {
            DecoderException e = assertThrows(DecoderException.class, () -> channel.writeInbound(frame));
            assertTrue(e.getCause() instanceof IOException);
            assertTrue(e.getCause().getMessage().contains(expectedMessage), "got: " + e.getCause().getMessage());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static ByteBuf rawFrame(int frameLen, byte magic, byte version, short flags, int payloadLen,
                                    int payloadCrc, int headerLen, byte[] body) {
        ByteBuf out = Unpooled.buffer(Integer.BYTES + Frame.PREAMBLE_AFTER_LEN + body.length);
        out.writeInt(frameLen);
        out.writeByte(magic);
        out.writeByte(version);
        out.writeShort(Opcode.PING.code);
        out.writeShort(1);
        out.writeShort(flags);
        out.writeLong(7);
        out.writeInt(payloadLen);
        out.writeInt(payloadCrc);
        out.writeShort(headerLen);
        out.writeBytes(body);
        return out;
    }

    private static ByteBuf rawLengthFrame(int frameLen) {
        ByteBuf out = Unpooled.buffer(Integer.BYTES);
        out.writeInt(frameLen);
        return out;
    }
}
