package io.strata.proto;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientServerTest {

    @Test
    void pipelinedEchoAndErrors() throws Exception {
        ScpServer.Handler handler = req -> {
            if (req.opcode() == Opcode.PING.code) {
                // echo payload back
                return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "nope");
        };
        try (ScpServer server = new ScpServer(0, 1, 0xA, 0xB, handler)) {
            try (ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
                assertEquals(1, client.serverHello().nodeId());

                // pipeline 100 pings with distinct payloads
                List<CompletableFuture<Frame>> futures = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    byte[] p = ("payload-" + i).getBytes();
                    futures.add(client.send(Opcode.PING, emptyHeader(), ByteBuffer.wrap(p)));
                }
                for (int i = 0; i < 100; i++) {
                    Frame resp = futures.get(i).get();
                    ByteBuffer hb = resp.headerSlice();
                    Resp.check(hb);
                    byte[] p = new byte[resp.payloadLength()];
                    resp.payloadSlice().get(p);
                    assertArrayEquals(("payload-" + i).getBytes(), p);
                }

                // server-side ScpException becomes a typed client-side exception
                ScpException e = assertThrows(ScpException.class,
                        () -> client.call(Opcode.READ, new Messages.Read(
                                new io.strata.common.ChunkId(io.strata.common.FileId.random(), 0), 0, 1).encode(), null, 2000));
                assertEquals(ErrorCode.UNKNOWN_OPCODE, e.code());
            }
        }
    }

    private static byte[] emptyHeader() {
        BufWriter w = new BufWriter(4);
        w.noTags();
        return w.toBytes();
    }
}
