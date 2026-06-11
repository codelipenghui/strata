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
import java.nio.charset.StandardCharsets;
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
             StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {

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

            var sealed = client.open(fileId).recoverAndSeal(2);
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

    @Test
    void sealedReplicaShorterThanDurableFloorIsEvicted() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {

            String[] hp = cluster.metaEndpoint().split(":");
            FileId fileId;
            Messages.CreateChunkResp chunk;
            try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
                fileId = Messages.CreateFileResp.decode(meta.call(Opcode.CREATE_FILE,
                                new Messages.CreateFile((byte) 0, (byte) 0, (byte) 0, "short-sealed")
                                        .encode(), null, 5000))
                        .fileId();
                chunk = Messages.CreateChunkResp.decode(meta.call(Opcode.CREATE_CHUNK,
                        new Messages.CreateChunk(fileId, 1, (byte) 0xFF).encode(), null, 5000));
            }

            byte[] a = "AAAA".getBytes(StandardCharsets.UTF_8);
            byte[] b = "BBBB".getBytes(StandardCharsets.UTF_8);
            int shortReplica = chunk.replicas().get(0).nodeId();
            for (int i = 0; i < 3; i++) {
                var replica = chunk.replicas().get(i);
                String[] nhp = replica.endpoint().split(":");
                try (ScpClient node = new ScpClient(nhp[0], Integer.parseInt(nhp[1]),
                        ScpClient.KIND_TOOL, "w")) {
                    node.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(chunk.chunkId(), 1,
                            (byte) 0, (byte) 0, (byte) 0, 1 << 20, 1L).encode(), null, 5000);
                    node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 0, 0).encode(),
                            ByteBuffer.wrap(a), 5000);
                    if (i == 0) {
                        node.call(Opcode.SEAL_CHUNK, new Messages.SealChunk(chunk.chunkId(), 1, 4).encode(),
                                null, 5000);
                    } else {
                        node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 4, 4).encode(),
                                ByteBuffer.wrap(b), 5000);
                        node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 8, 8).encode(),
                                ByteBuffer.allocate(0), 5000);
                    }
                }
            }

            var sealed = client.open(fileId).recoverAndSeal(2);
            assertEquals(8, sealed.sealedLength(),
                    "a short sealed replica must not truncate quorum-durable bytes");

            Messages.LookupFileResp lookup;
            try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
                lookup = Messages.LookupFileResp.decode(meta.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(fileId).encode(), null, 5000));
            }
            assertEquals(2, lookup.chunks().get(0).replicas().size(),
                    "the short sealed copy must be excluded from the descriptor");
            assertTrue(lookup.chunks().get(0).replicas().stream().noneMatch(r -> r.nodeId() == shortReplica));

            try (var reader = client.open(fileId).openForRead()) {
                assertEquals("AAAABBBB", new String(reader.read(0, 16).data(), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void recoveryPrefersFullValidBatchOverShorterPartialBoundary() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {

            String[] hp = cluster.metaEndpoint().split(":");
            FileId fileId;
            Messages.CreateChunkResp chunk;
            try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
                fileId = Messages.CreateFileResp.decode(meta.call(Opcode.CREATE_FILE,
                        new Messages.CreateFile((byte) 0, (byte) 0, (byte) 0, "partial").encode(), null, 5000))
                        .fileId();
                chunk = Messages.CreateChunkResp.decode(meta.call(Opcode.CREATE_CHUNK,
                        new Messages.CreateChunk(fileId, 1, (byte) 0xFF).encode(), null, 5000));
            }

            byte[] a = "AAAA".getBytes(StandardCharsets.UTF_8);
            byte[] b = "BBBB".getBytes(StandardCharsets.UTF_8);
            byte[] full = "CDEF".getBytes(StandardCharsets.UTF_8);
            byte[] partial = "CD".getBytes(StandardCharsets.UTF_8);

            for (int i = 0; i < 3; i++) {
                var replica = chunk.replicas().get(i);
                String[] nhp = replica.endpoint().split(":");
                try (ScpClient node = new ScpClient(nhp[0], Integer.parseInt(nhp[1]),
                        ScpClient.KIND_TOOL, "w")) {
                    node.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(chunk.chunkId(), 1,
                            (byte) 0, (byte) 0, (byte) 0, 1 << 20, 1L).encode(), null, 5000);
                    node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 0, 0).encode(),
                            ByteBuffer.wrap(a), 5000);
                    node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 4, 4).encode(),
                            ByteBuffer.wrap(b), 5000);
                    node.call(Opcode.APPEND, new Messages.Append(chunk.chunkId(), 1, 8, 8).encode(),
                            ByteBuffer.wrap(i < 2 ? full : partial), 5000);
                }
            }

            var sealed = client.open(fileId).recoverAndSeal(2);
            assertEquals(12, sealed.sealedLength(),
                    "recovery must preserve the full valid batch, not stop at a shorter partial boundary");

            for (var replica : chunk.replicas()) {
                String[] nhp = replica.endpoint().split(":");
                try (ScpClient node = new ScpClient(nhp[0], Integer.parseInt(nhp[1]),
                        ScpClient.KIND_TOOL, "v")) {
                    var frame = node.callFrame(Opcode.READ,
                            new Messages.Read(chunk.chunkId(), 0, 64).encode(), null, 5000);
                    ByteBuffer h = frame.headerSlice();
                    Resp.check(h);
                    byte[] bytes = new byte[frame.payloadLength()];
                    frame.payloadSlice().get(bytes);
                    assertEquals("AAAABBBBCDEF", new String(bytes, StandardCharsets.UTF_8));
                }
            }
        }
    }
}
