package io.strata.proto;

import io.netty.buffer.ByteBuf;
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
            ByteBuffer header = f.headerSlice();
            ByteBuffer payload = f.payloadSlice();
            int headerLen = header.remaining();
            int payloadLen = payload.remaining();
            if (headerLen > 0xFFFF) {
                throw new IOException("header too large: " + headerLen);
            }
            long computedFrameLen = (long) Frame.PREAMBLE_AFTER_LEN + headerLen + payloadLen;
            if (computedFrameLen > Integer.MAX_VALUE) {
                throw new IOException("frame too large: " + computedFrameLen);
            }
            int frameLen = (int) computedFrameLen;
            if (frameLen > FrameIO.MAX_FRAME_BYTES) {
                throw new IOException("frame too large: " + frameLen);
            }

            short flags = f.flags();
            int payloadCrc = 0;
            if (payloadLen > 0) {
                payloadCrc = Crc.of(payload.duplicate());
                flags |= Frame.FLAG_PAYLOAD_CRC;
            }

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
            out.writeBytes(payload.duplicate());
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
            if (frameLen < Frame.PREAMBLE_AFTER_LEN || frameLen > FrameIO.MAX_FRAME_BYTES) {
                throw new IOException("bad frame length " + frameLen);
            }
            if (in.readableBytes() < frameLen) {
                in.resetReaderIndex();
                return;
            }

            ByteBuf frame = in.readRetainedSlice(frameLen);
            boolean emitted = false;
            try {
                int base = frame.readerIndex();
                byte magic = frame.getByte(base);
                byte version = frame.getByte(base + 1);
                if (magic != Frame.MAGIC) {
                    throw new IOException("bad magic 0x" + Integer.toHexString(magic & 0xFF));
                }
                if (version != Frame.FRAME_VERSION) {
                    throw new IOException("unsupported frame version " + version);
                }
                short opcode = frame.getShort(base + 2);
                short apiVersion = frame.getShort(base + 4);
                short flags = frame.getShort(base + 6);
                long correlationId = frame.getLong(base + 8);
                int payloadLen = frame.getInt(base + 16);
                int payloadCrc = frame.getInt(base + 20);
                int headerLen = frame.getUnsignedShort(base + 24);
                if (payloadLen < 0) {
                    throw new IOException("negative payload length " + payloadLen);
                }
                if (Frame.PREAMBLE_AFTER_LEN + headerLen + payloadLen != frameLen) {
                    throw new IOException("frame length mismatch: " + frameLen
                            + " vs header=" + headerLen + " payload=" + payloadLen);
                }

                int headerIndex = base + Frame.PREAMBLE_AFTER_LEN;
                int payloadIndex = headerIndex + headerLen;
                if ((flags & Frame.FLAG_PAYLOAD_CRC) != 0 && payloadLen > 0) {
                    int actual = Crc.of(frame.nioBuffer(payloadIndex, payloadLen));
                    if (actual != payloadCrc) {
                        throw new IOException("payload crc mismatch: expected " + payloadCrc + " got " + actual);
                    }
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
