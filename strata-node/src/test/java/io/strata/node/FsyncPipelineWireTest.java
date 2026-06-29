package io.strata.node;

import io.strata.common.ChunkId;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Group commit must survive the WIRE path, not just direct engine calls: pipelined APPENDs on one
 * connection are written/validated in order by the connection thread while their acks defer to
 * coalesced forces. If the server serializes ack-before-next-read, force count ≈ append count and
 * this test fails.
 */
class FsyncPipelineWireTest {

    @TempDir
    Path dir;

    @Test
    void pipelinedWireAppendsCoalesceForces() throws Exception {
        int appends = 300;
        byte[] payload = new byte[512];
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "perf")) {

            ChunkId id = new ChunkId(FileId.of(1), 0);
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, true /* fsync */,
                    1 << 24, 1L, StrataNamespace.of("test")).encode(), null, 5000);

            long start = System.nanoTime();
            List<CompletableFuture<Frame>> futures = new ArrayList<>(appends);
            long offset = 0;
            for (int i = 0; i < appends; i++) {
                futures.add(client.send(Opcode.APPEND,
                        new Messages.Append(id, 1, offset, offset, StrataNamespace.of("test")).encode(), ByteBuffer.wrap(payload)));
                offset += payload.length;
            }
            for (CompletableFuture<Frame> f : futures) {
                Resp.check(f.get(30, TimeUnit.SECONDS).headerSlice());
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            long forces = node.store().fsyncForceCount();
            System.out.printf("wire pipeline: %d fsync appends in %d ms, %d forces (%.1f appends/force)%n",
                    appends, elapsedMs, forces, forces == 0 ? 0 : (double) appends / forces);

            assertTrue(forces < appends / 2,
                    "group commit broken over the wire: " + forces + " forces for " + appends + " appends");
        }
    }
}
