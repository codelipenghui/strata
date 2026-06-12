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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Seal recovery must commit only byte-identical sealed replicas (invariant §14.6): one divergent
 * outlier can be dropped if an agreeing quorum exists, but a three-way split must not be committed.
 */
class RecoveryDivergenceTest {

    @Test
    void recoveryCommitsAgreeingSealQuorumAndDropsOutlier() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            var setup = createOpenChunkWithReplicaPayloads(cluster, "/majority",
                    List.of("AAAA".getBytes(), "AAAA".getBytes(), "BBBB".getBytes()));

            String[] hp = cluster.metaEndpoint().split(":");
            var sealed = client.openById(setup.fileId()).recoverAndSeal(2);
            assertEquals(4, sealed.sealedLength());

            try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
                var lookup = Messages.LookupFileResp.decode(meta.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(setup.fileId()).encode(), null, 5000));
                assertEquals(io.strata.common.ChunkState.SEALED, lookup.chunks().get(0).state());
                assertEquals(2, lookup.chunks().get(0).replicas().size(),
                        "metadata must retain only the agreeing seal quorum");
            }

            try (var reader = client.openById(setup.fileId()).openForRead()) {
                assertArrayEquals("AAAA".getBytes(), reader.read(0, 4).data());
            }
        }
    }

    @Test
    void recoveryRefusesToSealDivergenceWithoutQuorum() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            var setup = createOpenChunkWithReplicaPayloads(cluster, "/split",
                    List.of("AAAA".getBytes(), "BBBB".getBytes(), "CCCC".getBytes()));

            ScpException e = assertThrows(ScpException.class, () -> client.openById(setup.fileId()).recoverAndSeal(2));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(e.getMessage().contains("divergence"),
                    "expected a divergence failure, got: " + e.getMessage());

            // and metadata must NOT have recorded a sealed chunk
            String[] hp = cluster.metaEndpoint().split(":");
            try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
                var lookup = Messages.LookupFileResp.decode(meta.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(setup.fileId()).encode(), null, 5000));
                assertEquals(io.strata.common.ChunkState.OPEN, lookup.chunks().get(0).state(),
                        "metadata must not commit a divergent seal");
            }
        }
    }

    private record OpenChunkSetup(io.strata.common.FileId fileId, Messages.CreateChunkResp chunk) {}

    private static OpenChunkSetup createOpenChunkWithReplicaPayloads(MiniCluster cluster, String path,
                                                                    List<byte[]> payloads) throws Exception {
        String[] hp = cluster.metaEndpoint().split(":");
        Messages.CreateChunkResp chunk;
        io.strata.common.FileId fileId;
        try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
            var file = Messages.CreateFileResp.decode(meta.call(Opcode.CREATE_FILE,
                    new Messages.CreateFile("test", path, (byte) 0, (byte) 0, (byte) 0).encode(), null, 5000));
            fileId = file.fileId();
            chunk = Messages.CreateChunkResp.decode(meta.call(Opcode.CREATE_CHUNK,
                    new Messages.CreateChunk(fileId, 1, (byte) 0xFF).encode(), null, 5000));
        }
        assertEquals(payloads.size(), chunk.replicas().size());

        for (int i = 0; i < chunk.replicas().size(); i++) {
            var replica = chunk.replicas().get(i);
            String[] nhp = replica.endpoint().split(":");
            try (ScpClient node = new ScpClient(nhp[0], Integer.parseInt(nhp[1]),
                    ScpClient.KIND_TOOL, "writer")) {
                node.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(chunk.chunkId(), 1,
                        (byte) 0, (byte) 0, (byte) 0, 1 << 20, 1L).encode(), null, 5000);
                node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 0, 0).encode(),
                        ByteBuffer.wrap(payloads.get(i)), 5000);
            }
        }
        return new OpenChunkSetup(fileId, chunk);
    }
}
