package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Seal recovery must commit only byte-identical sealed replicas (invariant §14.6): one divergent
 * outlier can be dropped if an agreeing quorum exists, but a three-way split above the durable
 * floor must be truncated rather than committed.
 */
class RecoveryDivergenceTest {

    @Test
    void recoveryCommitsAgreeingSealQuorumAndDropsOutlier() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            var setup = createOpenChunkWithReplicaPayloads(cluster, "/majority",
                    List.of("AAAA".getBytes(), "AAAA".getBytes(), "BBBB".getBytes()));

            String[] hp = cluster.metaEndpoint().split(":");
            var sealed = client.openById(StrataNamespace.of("test"), setup.fileId()).recoverAndSeal();
            assertEquals(4, sealed.sealedLength());

            try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
                var lookup = Messages.LookupFileResp.decode(meta.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(StrataNamespace.of("test"), setup.fileId()).encode(), null, 5000));
                assertEquals(io.strata.common.ChunkState.SEALED, lookup.chunks().get(0).state());
                assertEquals(2, lookup.chunks().get(0).replicas().size(),
                        "metadata must retain only the agreeing seal quorum");
            }

            try (var reader = client.openById(StrataNamespace.of("test"), setup.fileId()).openForRead()) {
                try (StrataFile.ReadResult rr = reader.read(0, 4)) {
                    byte[] got = new byte[rr.length()];
                    rr.buffer().get(got);
                    assertArrayEquals("AAAA".getBytes(), got);
                }
            }
        }
    }

    @Test
    void recoveryTruncatesDivergentTailWithoutQuorum() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            var setup = createOpenChunkWithReplicaPayloads(cluster, "/split",
                    List.of("AAAA".getBytes(), "BBBB".getBytes(), "CCCC".getBytes()));

            var sealed = client.openById(StrataNamespace.of("test"), setup.fileId()).recoverAndSeal();
            assertEquals(0, sealed.sealedLength(),
                    "recovery must not commit a tail without an agreeing quorum");

            String[] hp = cluster.metaEndpoint().split(":");
            try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
                var lookup = Messages.LookupFileResp.decode(meta.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(StrataNamespace.of("test"), setup.fileId()).encode(), null, 5000));
                assertEquals(io.strata.common.ChunkState.SEALED, lookup.chunks().get(0).state());
                assertEquals(0, lookup.chunks().get(0).length());
            }
        }
    }

    @Test
    void recoveryDoesNotManufactureQuorumByCatchingUpFromOneDivergentDonor() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            var setup = createOpenChunkWithReplicaPayloadsAndDurableFloor(cluster, "/floor-split",
                    List.of("BBBB".getBytes(), "AAAA".getBytes(), new byte[0]));

            assertThrows(ScpException.class, () -> client.openById(StrataNamespace.of("test"), setup.fileId()).recoverAndSeal(),
                    "recovery must not copy one divergent donor into a lagging replica and then "
                            + "commit that manufactured quorum below the durable floor");
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
                    new Messages.CreateFile("test", path).encode(), null, 5000));
            fileId = file.fileId();
            chunk = Messages.CreateChunkResp.decode(meta.call(Opcode.CREATE_CHUNK,
                    new Messages.CreateChunk(StrataNamespace.of("test"), fileId, 1).encode(), null, 5000));
        }
        assertEquals(payloads.size(), chunk.replicas().size());

        for (int i = 0; i < chunk.replicas().size(); i++) {
            var replica = chunk.replicas().get(i);
            String[] nhp = replica.endpoint().split(":");
            try (ScpClient node = new ScpClient(nhp[0], Integer.parseInt(nhp[1]),
                    ScpClient.KIND_TOOL, "writer")) {
                node.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(chunk.chunkId(), 1,
                        false, 1 << 20, 1L).encode(), null, 5000);
                node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 0, 0).encode(),
                        ByteBuffer.wrap(payloads.get(i)), 5000);
            }
        }
        return new OpenChunkSetup(fileId, chunk);
    }

    private static OpenChunkSetup createOpenChunkWithReplicaPayloadsAndDurableFloor(
            MiniCluster cluster, String path, List<byte[]> payloads) throws Exception {
        OpenChunkSetup setup = createEmptyOpenChunk(cluster, path);
        Messages.CreateChunkResp chunk = setup.chunk();
        assertEquals(payloads.size(), chunk.replicas().size());

        for (int i = 0; i < chunk.replicas().size(); i++) {
            var replica = chunk.replicas().get(i);
            String[] nhp = replica.endpoint().split(":");
            byte[] payload = payloads.get(i);
            try (ScpClient node = new ScpClient(nhp[0], Integer.parseInt(nhp[1]),
                    ScpClient.KIND_TOOL, "writer")) {
                node.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(chunk.chunkId(), 1,
                        false, 1 << 20, 1L).encode(), null, 5000);
                if (payload.length > 0) {
                    node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 0, 0).encode(),
                            ByteBuffer.wrap(payload), 5000);
                    node.call(Opcode.APPEND,
                            new Messages.Append(chunk.chunkId(), 1, payload.length, payload.length).encode(),
                            ByteBuffer.allocate(0), 5000);
                }
            }
        }
        return setup;
    }

    private static OpenChunkSetup createEmptyOpenChunk(MiniCluster cluster, String path) throws Exception {
        String[] hp = cluster.metaEndpoint().split(":");
        try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
            var file = Messages.CreateFileResp.decode(meta.call(Opcode.CREATE_FILE,
                    new Messages.CreateFile("test", path).encode(), null, 5000));
            var chunk = Messages.CreateChunkResp.decode(meta.call(Opcode.CREATE_CHUNK,
                    new Messages.CreateChunk(StrataNamespace.of("test"), file.fileId(), 1).encode(), null, 5000));
            return new OpenChunkSetup(file.fileId(), chunk);
        }
    }
}
