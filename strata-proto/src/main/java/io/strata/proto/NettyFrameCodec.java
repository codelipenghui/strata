package io.strata.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.strata.common.Crc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

final class NettyFrameCodec {
    private NettyFrameCodec() {}

    static final class Encoder extends MessageToByteEncoder<Frame> {
        @Override
        protected void encode(ChannelHandlerContext ctx, Frame f, ByteBuf out) throws Exception {
            if (f.hasFilePayload()) {
                throw new IOException("file payload frames must be written as a frame prefix plus FileRegion");
            }
            ByteBuffer header = f.headerSlice();
            ByteBuffer payload = f.payloadSlice();
            int headerLen = header.remaining();
            int payloadLen = payload.remaining();

            short flags = f.flags();
            int payloadCrc = 0;
            if (payloadLen > 0) {
                payloadCrc = Crc.of(payload.duplicate());
                flags |= Frame.FLAG_PAYLOAD_CRC;
            }

            writePrefix(out, f, header, payloadLen, payloadCrc, flags);
            out.writeBytes(payload.duplicate());
        }
    }

    static ByteBuf encodeFilePrefix(ByteBufAllocator allocator, Frame f) throws IOException {
        if (!f.hasFilePayload()) {
            throw new IOException("frame has no file payload");
        }
        ByteBuffer header = f.headerSlice();
        int headerLen = header.remaining();
        ByteBuf out = allocator.buffer(Integer.BYTES + Frame.PREAMBLE_AFTER_LEN + headerLen);
        boolean success = false;
        try {
            writePrefix(out, f, header, f.payloadLength(), 0, f.flags());
            success = true;
            return out;
        } finally {
            if (!success) {
                out.release();
            }
        }
    }

    private static void writePrefix(ByteBuf out, Frame f, ByteBuffer header, int payloadLen,
                                    int payloadCrc, short flags) throws IOException {
        int headerLen = header.remaining();
        int frameLen = FrameIO.checkedFrameLength(headerLen, payloadLen);

        out.writeInt(frameLen);
        out.writeByte(Frame.MAGIC);
        out.writeByte(Frame.FRAME_VERSION);
        out.writeShort(f.opcode());
        out.writeShort(f.apiVersion());
        out.writeShort(flags);
        out.writeLong(f.correlationId());
        out.writeInt(payloadLen);
        out.writeInt(payloadCrc);
        out.writeShort(headerLen);
        out.writeBytes(header.duplicate());
    }

    static final class Decoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            if (in.readableBytes() < Integer.BYTES) {
                return;
            }
            in.markReaderIndex();
            int frameLen = in.readInt();
            FrameIO.checkFrameLength(frameLen);
            if (in.readableBytes() < frameLen) {
                in.resetReaderIndex();
                return;
            }

            ByteBuf frame = in.readRetainedSlice(frameLen);
            boolean emitted = false;
            try {
                int base = frame.readerIndex();
                FrameIO.checkMagicAndVersion(frame.getByte(base), frame.getByte(base + 1));
                short opcode = frame.getShort(base + 2);
                short apiVersion = frame.getShort(base + 4);
                short flags = frame.getShort(base + 6);
                long correlationId = frame.getLong(base + 8);
                int payloadLen = frame.getInt(base + 16);
                int payloadCrc = frame.getInt(base + 20);
                int headerLen = frame.getUnsignedShort(base + 24);
                FrameIO.checkBodyGeometry(frameLen, headerLen, payloadLen);

                int headerIndex = base + Frame.PREAMBLE_AFTER_LEN;
                int payloadIndex = headerIndex + headerLen;
                if ((flags & Frame.FLAG_PAYLOAD_CRC) != 0 && payloadLen > 0) {
                    FrameIO.checkPayloadCrc(payloadCrc, Crc.of(frame.nioBuffer(payloadIndex, payloadLen)));
                }
                out.add(Frame.fromOwnedBuffer(opcode, apiVersion, flags, correlationId,
                        frame, headerIndex, headerLen, payloadIndex, payloadLen));
                emitted = true;
            } finally {
                if (!emitted) {
                    frame.release();
                }
            }
        }
    }
}
