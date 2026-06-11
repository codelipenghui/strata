package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Seal recovery must CATCH UP replicas that are behind the durable offset (tech design §7.3):
 * recovery starts at p = max piggybacked DO, but a reachable replica may hold less than p; the
 * naive walk would issue an APPEND at base=p which that replica rejects (OFFSET_GAP), leaving it
 * permanently short while recovery "succeeds" — a corrupt-by-omission replica in the descriptor.
 * After recovery, EVERY descriptor replica must serve the full sealed chunk, byte-identical.
 */
class RecoveryCatchUpTest {

    @Test
    void laggingReplicaIsCaughtUpBeforeSeal() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = new StrataClient(ClientConfig.of(cluster.metaEndpoint()))) {

            String[] hp = cluster.metaEndpoint().split(":");
            FileId fileId;
            Messages.CreateChunkResp chunk;
            try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
                fileId = Messages.CreateFileResp.decode(meta.call(Opcode.CREATE_FILE,
                        new Messages.CreateFile((byte) 0, (byte) 0, (byte) 0, "lag").encode(), null, 5000))
                        .fileId();
                chunk = Messages.CreateChunkResp.decode(meta.call(Opcode.CREATE_CHUNK,
                        new Messages.CreateChunk(fileId, 1, (byte) 0xFF).encode(), null, 5000));
            }

            byte[] a = "AAAA".getBytes(), b = "BBBB".getBytes(), c = "CCCC".getBytes();
            // replicas 0,1: full history [A][B][C] with DO piggyback advancing to 8
            // replica 2: only [A] — reachable but BEHIND the durable offset
            for (int i = 0; i < 3; i++) {
                var replica = chunk.replicas().get(i);
                String[] nhp = replica.endpoint().split(":");
                try (ScpClient node = new ScpClient(nhp[0], Integer.parseInt(nhp[1]),
                        ScpClient.KIND_TOOL, "w")) {
                    node.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(chunk.chunkId(), 1,
                            (byte) 0, (byte) 0, (byte) 0, 1 << 20, 1L).encode(), null, 5000);
                    node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 0, 0).encode(),
                            ByteBuffer.wrap(a), 5000);
                    if (i < 2) {
                        node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 4, 4).encode(),
                                ByteBuffer.wrap(b), 5000);
                        node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 8, 8).encode(),
                                ByteBuffer.wrap(c), 5000);
                    }
                }
            }

            var sealed = client.recoverAndSeal(fileId, 2);
            assertEquals(12, sealed.sealedLength(), "all quorum-durable bytes must be preserved");

            // EVERY replica in the descriptor must now serve the full sealed chunk byte-identically
            Set<String> hashes = new HashSet<>();
            for (var replica : chunk.replicas()) {
                String[] nhp = replica.endpoint().split(":");
                try (ScpClient node = new ScpClient(nhp[0], Integer.parseInt(nhp[1]),
                        ScpClient.KIND_TOOL, "v")) {
                    var stat = Messages.StatResp.decode(node.call(Opcode.STAT_CHUNK,
                            new Messages.StatChunk(chunk.chunkId()).encode(), null, 5000));
                    assertEquals(ChunkState.SEALED, stat.state(),
                            "replica " + replica.nodeId() + " must be sealed after recovery");
                    assertEquals(12, stat.sealedLength());

                    var frame = node.callFrame(Opcode.FETCH_CHUNK,
                            new Messages.FetchChunk(chunk.chunkId(), 0, Integer.MAX_VALUE).encode(),
                            null, 5000);
                    ByteBuffer h = frame.headerSlice();
                    Resp.check(h);
                    byte[] bytes = new byte[frame.payloadLength()];
                    frame.payloadSlice().get(bytes);
                    hashes.add(java.util.HexFormat.of().formatHex(
                            java.security.MessageDigest.getInstance("SHA-256").digest(bytes)));
                }
            }
            assertEquals(1, hashes.size(), "replicas must be byte-identical after recovery");
            assertTrue(true);
        }
    }
}
