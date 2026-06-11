package io.strata.proto;

import java.nio.ByteBuffer;

/**
 * One SCP frame (tech design §10.2). Header and payload are read-only slices.
 * The payload is always the trailing suffix of the frame on the wire.
 */
public record Frame(short opcode, short apiVersion, short flags, long correlationId,
                    ByteBuffer header, ByteBuffer payload) {

    public static final byte MAGIC = 0x5C;
    public static final byte FRAME_VERSION = 1;
    public static final short FLAG_RESPONSE = 0x0001;
    public static final short FLAG_PAYLOAD_CRC = 0x0002;
    /** Fixed bytes after the leading u32 length field. */
    public static final int PREAMBLE_AFTER_LEN = 26;

    public Frame {
        header = readOnlySlice(header);
        payload = readOnlySlice(payload);
    }

    public boolean isResponse() {
        return (flags & FLAG_RESPONSE) != 0;
    }

    public ByteBuffer headerSlice() {
        return header.duplicate();
    }

    public ByteBuffer payloadSlice() {
        return payload.duplicate();
    }

    public int payloadLength() {
        return payload.remaining();
    }

    private static ByteBuffer readOnlySlice(ByteBuffer buffer) {
        return (buffer == null ? ByteBuffer.allocate(0) : buffer.slice()).asReadOnlyBuffer();
    }

    private static ByteBuffer headerBuffer(byte[] header) {
        return header != null ? ByteBuffer.wrap(header) : ByteBuffer.allocate(0);
    }

    public static Frame request(Opcode op, byte[] header, ByteBuffer payload, long correlationId) {
        return new Frame(op.code, (short) 1, (short) 0, correlationId,
                headerBuffer(header), payload != null ? payload : ByteBuffer.allocate(0));
    }

    public static Frame response(Frame req, byte[] header, ByteBuffer payload) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                headerBuffer(header), payload != null ? payload : ByteBuffer.allocate(0));
    }
}
