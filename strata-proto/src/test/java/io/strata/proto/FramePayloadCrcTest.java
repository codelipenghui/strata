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
}
