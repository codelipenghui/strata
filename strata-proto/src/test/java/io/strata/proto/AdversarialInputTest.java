package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        var del = new Messages.DeleteChunks(java.util.List.of(new ChunkId(FileId.random(), 0)));
        assertEquals(del, Messages.DeleteChunks.decode(ByteBuffer.wrap(del.encode())));
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
}
