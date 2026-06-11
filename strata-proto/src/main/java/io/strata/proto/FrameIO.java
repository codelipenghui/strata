package io.strata.proto;

import io.strata.common.Crc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Blocking frame reader/writer over socket streams. Payload CRC verified when present. */
public final class FrameIO {
    public static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    private FrameIO() {}

    public static void write(DataOutputStream out, Frame f) throws IOException {
        ByteBuffer header = f.headerSlice();
        ByteBuffer payload = f.payloadSlice();
        int headerLen = header.remaining();
        int payloadLen = payload.remaining();
        if (headerLen > 0xFFFF) throw new IOException("header too large: " + headerLen);
        long computedFrameLen = (long) Frame.PREAMBLE_AFTER_LEN + headerLen + payloadLen;
        if (computedFrameLen > Integer.MAX_VALUE) throw new IOException("frame too large: " + computedFrameLen);
        int frameLen = (int) computedFrameLen;
        if (frameLen > MAX_FRAME_BYTES) throw new IOException("frame too large: " + frameLen);

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
        writeFully(out, header);
        writeFully(out, payload);
        out.flush();
    }

    private static void writeFully(DataOutputStream out, ByteBuffer buf) throws IOException {
        if (buf.hasArray()) {
            out.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        } else {
            byte[] tmp = new byte[buf.remaining()];
            buf.duplicate().get(tmp);
            out.write(tmp);
        }
    }

    /** Reads one frame; returns null on clean EOF at a frame boundary. */
    public static Frame read(DataInputStream in) throws IOException {
        int frameLen;
        try {
            frameLen = in.readInt();
        } catch (EOFException e) {
            return null;
        }
        if (frameLen < Frame.PREAMBLE_AFTER_LEN || frameLen > MAX_FRAME_BYTES) {
            throw new IOException("bad frame length " + frameLen);
        }
        byte[] body = new byte[frameLen];
        in.readFully(body);
        ByteBuffer buf = ByteBuffer.wrap(body);
        byte magic = buf.get();
        byte version = buf.get();
        if (magic != Frame.MAGIC) throw new IOException("bad magic 0x" + Integer.toHexString(magic & 0xFF));
        if (version != Frame.FRAME_VERSION) throw new IOException("unsupported frame version " + version);
        short opcode = buf.getShort();
        short apiVersion = buf.getShort();
        short flags = buf.getShort();
        long correlationId = buf.getLong();
        int payloadLen = buf.getInt();
        int payloadCrc = buf.getInt();
        int headerLen = buf.getShort() & 0xFFFF;
        if (payloadLen < 0) {
            // a negative payload length can satisfy the equality check below (26+1+(-1)=26) and
            // would blow up later as an unchecked IndexOutOfBounds — reject it as protocol error
            throw new IOException("negative payload length " + payloadLen);
        }
        if (Frame.PREAMBLE_AFTER_LEN + headerLen + payloadLen != frameLen) {
            throw new IOException("frame length mismatch: " + frameLen + " vs header=" + headerLen + " payload=" + payloadLen);
        }
        ByteBuffer header = ByteBuffer.wrap(body, Frame.PREAMBLE_AFTER_LEN, headerLen).slice();
        ByteBuffer payload = ByteBuffer.wrap(body, Frame.PREAMBLE_AFTER_LEN + headerLen, payloadLen).slice();
        if ((flags & Frame.FLAG_PAYLOAD_CRC) != 0 && payloadLen > 0) {
            int actual = Crc.of(payload.duplicate());
            if (actual != payloadCrc) {
                throw new IOException("payload crc mismatch: expected " + payloadCrc + " got " + actual);
            }
        }
        return new Frame(opcode, apiVersion, flags, correlationId, header, payload);
    }
}
