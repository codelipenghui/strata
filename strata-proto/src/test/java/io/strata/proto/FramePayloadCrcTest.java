package io.strata.proto;

import io.strata.common.Crc;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FramePayloadCrcTest {

    private static Frame roundTrip(Frame f) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        FrameIO.write(new DataOutputStream(bytes), f);
        return FrameIO.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    }

    @Test
    void decodedFrameExposesTheClientPayloadCrc() throws Exception {
        byte[] payload = "client-computed-payload".getBytes();
        Frame sent = Frame.request(Opcode.APPEND, new byte[] {1, 2, 3}, ByteBuffer.wrap(payload), 7L);

        Frame decoded = roundTrip(sent);

        assertEquals(Crc.of(ByteBuffer.wrap(payload)), decoded.payloadCrc());
    }

    @Test
    void emptyPayloadFrameReportsZeroCrc() throws Exception {
        Frame sent = Frame.request(Opcode.PING, new byte[] {1}, ByteBuffer.allocate(0), 1L);

        Frame decoded = roundTrip(sent);

        assertEquals(0, decoded.payloadCrc());
    }

    @Test
    void unflaggedPayloadCrcFieldIsIgnored() throws Exception {
        // a frame with no FLAG_PAYLOAD_CRC but stale/garbage in the payload-CRC field (a malformed
        // or adversarial peer) must still report 0 — the accessor's documented contract.
        Frame sent = Frame.request(Opcode.PING, new byte[] {1}, ByteBuffer.allocate(0), 1L);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        FrameIO.write(new DataOutputStream(bytes), sent);
        byte[] raw = bytes.toByteArray();
        // payload-CRC field sits at output offset 24:
        // 4 len + 1 magic + 1 ver + 2 opcode + 2 apiVersion + 2 flags + 8 correlationId + 4 payloadLen
        raw[24] = (byte) 0xDE;
        raw[25] = (byte) 0xAD;
        raw[26] = (byte) 0xBE;
        raw[27] = (byte) 0xEF;

        Frame decoded = FrameIO.read(new DataInputStream(new ByteArrayInputStream(raw)));

        assertEquals(0, decoded.payloadCrc());
    }
}
