package io.strata.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.strata.common.Crc;
import io.strata.common.FailureInjector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

final class NettyFrameCodec {
    private NettyFrameCodec() {}

    static final class Encoder extends MessageToByteEncoder<Frame> {
        @Override
        protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, Frame f, boolean preferDirect) throws Exception {
            if (f.hasFilePayload()) {
                return super.allocateBuffer(ctx, f, preferDirect);
            }
            int headerLen = f.headerLength();
            int payloadLen = f.payloadLength();
            int capacity = Integer.BYTES + FrameIO.checkedFrameLength(headerLen, payloadLen);
            return preferDirect ? ctx.alloc().ioBuffer(capacity, capacity) : ctx.alloc().heapBuffer(capacity, capacity);
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Frame f, ByteBuf out) throws Exception {
            if (f.hasFilePayload()) {
                throw new IOException("file payload frames must be written as a frame prefix plus FileRegion");
            }
            FailureInjector.point("scp.encoder.beforeHeader");
            ByteBuffer payload = f.payloadView();
            int headerLen = f.headerLength();
            int payloadLen = payload.remaining();

            short flags = f.flags();
            int payloadCrc = 0;
            if (payloadLen > 0) {
                payloadCrc = Crc.of(payload);
                flags |= Frame.FLAG_PAYLOAD_CRC;
            }

            writePrefix(out, f, headerLen, payloadLen, payloadCrc, flags);
            writeHeader(out, f);
            FailureInjector.point("scp.encoder.beforePayload");
            writeBytes(out, payload);
        }
    }

    static ByteBuf encodeFilePrefix(ByteBufAllocator allocator, Frame f) throws IOException {
        if (!f.hasFilePayload()) {
            throw new IOException("frame has no file payload");
        }
        int headerLen = f.headerLength();
        ByteBuf out = allocator.buffer(Integer.BYTES + Frame.PREAMBLE_AFTER_LEN + headerLen);
        boolean success = false;
        try {
            writePrefix(out, f, headerLen, f.payloadLength(), 0, f.flags());
            writeHeader(out, f);
            success = true;
            return out;
        } finally {
            if (!success) {
                out.release();
            }
        }
    }

    private static void writePrefix(ByteBuf out, Frame f, int headerLen, int payloadLen,
                                    int payloadCrc, short flags) throws IOException {
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
    }

    private static void writeHeader(ByteBuf out, Frame f) {
        if (f.hasOkU64Header()) {
            out.writeShort(0);
            out.writeLong(f.okU64HeaderValue());
            out.writeByte(0);
        } else if (f.hasHeaderBytes()) {
            out.writeBytes(f.headerBytes());
        } else {
            writeBytes(out, f.headerView());
        }
    }

    private static void writeBytes(ByteBuf out, ByteBuffer source) {
        int position = source.position();
        try {
            out.writeBytes(source);
        } finally {
            source.position(position);
        }
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

            int sourceBase = in.readerIndex();
            ByteBuf frame = in.readRetainedSlice(frameLen);
            boolean emitted = false;
            try {
                int frameBase = frame.readerIndex();
                FrameIO.checkMagicAndVersion(frame.getByte(frameBase), frame.getByte(frameBase + 1));
                short opcode = frame.getShort(frameBase + 2);
                short apiVersion = frame.getShort(frameBase + 4);
                short flags = frame.getShort(frameBase + 6);
                long correlationId = frame.getLong(frameBase + 8);
                int payloadLen = frame.getInt(frameBase + 16);
                int payloadCrc = frame.getInt(frameBase + 20);
                int headerLen = frame.getUnsignedShort(frameBase + 24);
                FrameIO.checkBodyGeometry(frameLen, headerLen, payloadLen);

                int headerIndex = Frame.PREAMBLE_AFTER_LEN;
                int payloadIndex = headerIndex + headerLen;
                if ((flags & Frame.FLAG_PAYLOAD_CRC) != 0 && payloadLen > 0) {
                    FrameIO.checkPayloadCrc(payloadCrc, payloadCrc(in, sourceBase + payloadIndex, payloadLen));
                }
                // Frame normalizes payloadCrc to 0 on an unflagged/empty frame (the accessor contract)
                out.add(Frame.fromOwnedBuffer(opcode, apiVersion, flags, correlationId,
                        frame, frameBase + headerIndex, headerLen,
                        frameBase + payloadIndex, payloadLen, payloadCrc));
                emitted = true;
            } finally {
                if (!emitted) {
                    frame.release();
                }
            }
        }

        private static int payloadCrc(ByteBuf buf, int index, int length) {
            // The retained frame is a sliced ByteBuf whose nioBuffer() path allocates a NIO view per APPEND.
            if (buf.nioBufferCount() == 1) {
                return Crc.of(buf.internalNioBuffer(index, length));
            }
            return Crc.of(buf.nioBuffer(index, length));
        }
    }
}
