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
            int headerLen = f.headerLength();
            ByteBuffer payload = f.hasPayloadBytes() ? null : f.payloadView();
            int payloadLen = f.hasPayloadBytes() ? f.payloadBytesLength() : payload.remaining();

            short flags = f.flags();
            int payloadCrc = 0;
            if (payloadLen > 0) {
                payloadCrc = f.hasPayloadBytes()
                        ? Crc.of(f.payloadBytes(), f.payloadBytesOffset(), payloadLen)
                        : Crc.of(payload);
                flags |= Frame.FLAG_PAYLOAD_CRC;
            }

            writePrefix(out, f, headerLen, payloadLen, payloadCrc, flags);
            writeHeader(out, f);
            FailureInjector.point("scp.encoder.beforePayload");
            if (f.hasPayloadBytes()) {
                out.writeBytes(f.payloadBytes(), f.payloadBytesOffset(), payloadLen);
            } else {
                writeBytes(out, payload);
            }
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

    static ByteBuf encodeOkU64Response(ByteBufAllocator allocator, Frame req, long value) throws IOException {
        int headerLen = Frame.OK_U64_HEADER_LENGTH;
        int capacity = Integer.BYTES + FrameIO.checkedFrameLength(headerLen, 0);
        ByteBuf out = allocator.ioBuffer(capacity, capacity);
        boolean success = false;
        try {
            writePrefix(out, req.opcode(), req.apiVersion(), Frame.FLAG_RESPONSE, req.correlationId(),
                    headerLen, 0, 0);
            out.writeShort(0);
            out.writeLong(value);
            out.writeByte(0);
            success = true;
            return out;
        } finally {
            if (!success) {
                out.release();
            }
        }
    }

    static ByteBuf encodeBytesResponse(ByteBufAllocator allocator, Frame req, byte[] header, byte[] payload,
                                       int payloadLen) throws IOException {
        int headerLen = header == null ? 0 : header.length;
        if (payloadLen < 0) {
            throw new IllegalArgumentException("negative payload length: " + payloadLen);
        }
        if (payloadLen > 0 && (payload == null || payloadLen > payload.length)) {
            throw new IllegalArgumentException("invalid payload length " + payloadLen);
        }
        int capacity = Integer.BYTES + FrameIO.checkedFrameLength(headerLen, payloadLen);
        ByteBuf out = allocator.ioBuffer(capacity, capacity);
        boolean success = false;
        try {
            int payloadCrc = payloadLen > 0 ? Crc.of(payload, 0, payloadLen) : 0;
            short flags = payloadLen > 0
                    ? (short) (Frame.FLAG_RESPONSE | Frame.FLAG_PAYLOAD_CRC)
                    : Frame.FLAG_RESPONSE;
            writePrefix(out, req.opcode(), req.apiVersion(), flags, req.correlationId(),
                    headerLen, payloadLen, payloadCrc);
            if (headerLen > 0) {
                out.writeBytes(header);
            }
            if (payloadLen > 0) {
                out.writeBytes(payload, 0, payloadLen);
            }
            success = true;
            return out;
        } finally {
            if (!success) {
                out.release();
            }
        }
    }

    static ByteBuf encodeTwoU64BytesResponse(ByteBufAllocator allocator, Frame req, long first, long second,
                                             byte[] payload, int payloadLen) throws IOException {
        if (payloadLen < 0) {
            throw new IllegalArgumentException("negative payload length: " + payloadLen);
        }
        if (payloadLen > 0 && (payload == null || payloadLen > payload.length)) {
            throw new IllegalArgumentException("invalid payload length " + payloadLen);
        }
        int capacity = Integer.BYTES + FrameIO.checkedFrameLength(Frame.OK_TWO_U64_HEADER_LENGTH, payloadLen);
        ByteBuf out = allocator.ioBuffer(capacity, capacity);
        boolean success = false;
        try {
            int payloadCrc = payloadLen > 0 ? Crc.of(payload, 0, payloadLen) : 0;
            short flags = payloadLen > 0
                    ? (short) (Frame.FLAG_RESPONSE | Frame.FLAG_PAYLOAD_CRC)
                    : Frame.FLAG_RESPONSE;
            writePrefix(out, req.opcode(), req.apiVersion(), flags, req.correlationId(),
                    Frame.OK_TWO_U64_HEADER_LENGTH, payloadLen, payloadCrc);
            out.writeShort(0);
            out.writeLong(first);
            out.writeLong(second);
            out.writeByte(0);
            if (payloadLen > 0) {
                out.writeBytes(payload, 0, payloadLen);
            }
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
        writePrefix(out, f.opcode(), f.apiVersion(), flags, f.correlationId(), headerLen, payloadLen, payloadCrc);
    }

    private static void writePrefix(ByteBuf out, short opcode, short apiVersion, short flags, long correlationId,
                                    int headerLen, int payloadLen, int payloadCrc) throws IOException {
        int frameLen = FrameIO.checkedFrameLength(headerLen, payloadLen);

        out.writeInt(frameLen);
        out.writeByte(Frame.MAGIC);
        out.writeByte(Frame.FRAME_VERSION);
        out.writeShort(opcode);
        out.writeShort(apiVersion);
        out.writeShort(flags);
        out.writeLong(correlationId);
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
