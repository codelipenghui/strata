package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Seal recovery must refuse to commit divergent sealed replicas (invariant §14.6): when the
 * replicas of an open chunk hold same-length-different-bytes content, recovery's quorum seal sees
 * differing CRCs and must fail rather than record one of them in metadata — the same check the
 * appender's seal path performs.
 */
class RecoveryDivergenceTest {

    @Test
    void recoveryRefusesToSealDivergentReplicas() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = new StrataClient(ClientConfig.of(cluster.metaEndpoint()))) {

            // create file + chunk via metadata, then write DIVERGENT same-length content to the
            // replicas directly (simulating undetected corruption / a writer bug)
            String[] hp = cluster.metaEndpoint().split(":");
            Messages.CreateChunkResp chunk;
            io.strata.common.FileId fileId;
            try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
                var file = Messages.CreateFileResp.decode(meta.call(Opcode.CREATE_FILE,
                        new Messages.CreateFile((byte) 0, (byte) 0, (byte) 0, "diverge").encode(), null, 5000));
                fileId = file.fileId();
                chunk = Messages.CreateChunkResp.decode(meta.call(Opcode.CREATE_CHUNK,
                        new Messages.CreateChunk(fileId, 1, (byte) 0xFF).encode(), null, 5000));
            }
            assertEquals(3, chunk.replicas().size());

            for (int i = 0; i < 3; i++) {
                var replica = chunk.replicas().get(i);
                String[] nhp = replica.endpoint().split(":");
                try (ScpClient node = new ScpClient(nhp[0], Integer.parseInt(nhp[1]),
                        ScpClient.KIND_TOOL, "writer")) {
                    node.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(chunk.chunkId(), 1,
                            (byte) 0, (byte) 0, (byte) 0, 1 << 20, 1L).encode(), null, 5000);
                    byte[] payload = (i < 2 ? "AAAA" : "BBBB").getBytes(); // same length, replica 3 diverges
                    node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 0, 0).encode(),
                            ByteBuffer.wrap(payload), 5000);
                }
            }

            // recovery walks the ledger boundaries, seals all reachable replicas, and MUST detect
            // that the seal responses disagree on the chunk CRC
            ScpException e = assertThrows(ScpException.class, () -> client.recoverAndSeal(fileId, 2));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(e.getMessage().contains("divergence"),
                    "expected a divergence failure, got: " + e.getMessage());

            // and metadata must NOT have recorded a sealed chunk
            try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
                var lookup = Messages.LookupFileResp.decode(meta.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(fileId).encode(), null, 5000));
                assertEquals(io.strata.common.ChunkState.OPEN, lookup.chunks().get(0).state(),
                        "metadata must not commit a divergent seal");
            }
        }
    }
}
