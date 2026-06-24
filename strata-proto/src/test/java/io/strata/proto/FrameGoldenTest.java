package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Freezes the wire format (tech design §10.2). If these bytes change, the protocol changed —
 * that requires a frameVersion/apiVersion decision, not a code tweak.
 */
class FrameGoldenTest {

    @Test
    void appendFrameGoldenBytes() throws IOException {
        FileId f = FileId.of(0x0102030405060708L);
        var append = new Messages.Append(new ChunkId(f, 2), 5, 100, 50);
        ByteBuffer payload = ByteBuffer.wrap(new byte[]{(byte) 0xDE, (byte) 0xAD});

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        FrameIO.write(new DataOutputStream(bos), Frame.request(Opcode.APPEND, append.encode(), payload, 7));
        byte[] wire = bos.toByteArray();

        String expectedHex =
                // u32 frameLength = 26 (preamble) + 41 (header) + 2 (payload) = 69 = 0x45
                "00000045"
                // magic 0x5C, frameVersion 1
                + "5c01"
                // opcode APPEND = 0x0011, apiVersion 1
                + "00110001"
                // flags: PAYLOAD_CRC = 0x0002
                + "0002"
                // correlationId 7
                + "0000000000000007"
                // payloadLength 2
                + "00000002"
                // payloadCrc32c of DE AD
                + HexFormat.of().toHexDigits(io.strata.common.Crc.of(new byte[]{(byte) 0xDE, (byte) 0xAD}))
                // headerLength 41 = chunkId 20 + epoch 4 + base 8 + do 8 + tags 1
                + "0029"
                // header: chunkId(fileId msb/lsb + index), epoch 5, baseOffset 100, durableOffset 50, empty tags
                + "0102030405060708" + "090a0b0c0d0e0f10" + "00000002"
                + "00000005"
                + "0000000000000064"
                + "0000000000000032"
                + "00"
                // payload
                + "dead";
        assertEquals(expectedHex, HexFormat.of().formatHex(wire));

        // and it reads back identically
        Frame read = FrameIO.read(new DataInputStream(new ByteArrayInputStream(wire)));
        assertEquals(Opcode.APPEND.code, read.opcode());
        assertEquals(7, read.correlationId());
        assertEquals(append, Messages.Append.decode(read.headerSlice()));
        byte[] p = new byte[read.payloadLength()];
        read.payloadSlice().get(p);
        assertArrayEquals(new byte[]{(byte) 0xDE, (byte) 0xAD}, p);
    }

    @Test
    void corruptPayloadRejected() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        var append = new Messages.Append(new ChunkId(FileId.of(1), 0), 1, 0, 0);
        FrameIO.write(new DataOutputStream(bos), Frame.request(Opcode.APPEND, append.encode(),
                ByteBuffer.wrap(new byte[]{1, 2, 3, 4}), 1));
        byte[] wire = bos.toByteArray();
        wire[wire.length - 1] ^= 0x01; // flip one payload bit
        assertThrows(IOException.class, () -> FrameIO.read(new DataInputStream(new ByteArrayInputStream(wire))));
    }

    @Test
    void errorHeaderCarriesCodeAndDetail() {
        byte[] err = Resp.error(ErrorCode.OFFSET_GAP, "expected 100", 100);
        ScpException e = assertThrows(ScpException.class, () -> Resp.check(ByteBuffer.wrap(err)));
        assertEquals(ErrorCode.OFFSET_GAP, e.code());
        assertEquals(100, e.detail());
        assertEquals(true, e.retriable());
    }

    @Test
    void unknownTaggedFieldsIgnored() {
        // a future writer adds tag 99 to Append — current reader must ignore it
        BufWriter w = new BufWriter();
        FileId f = FileId.of(1);
        w.chunkId(new ChunkId(f, 0)).i32(1).u64(0).u64(0);
        TaggedFields.of(java.util.Map.of(99, new byte[]{1, 2, 3})).writeTo(w);
        var decoded = Messages.Append.decode(ByteBuffer.wrap(w.toBytes()));
        assertEquals(new Messages.Append(new ChunkId(f, 0), 1, 0, 0), decoded);
    }
}
