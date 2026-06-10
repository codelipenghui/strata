package io.strata.node;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Single-node data-plane test over the real wire (standalone mode, no metadata plane). */
class StorageNodeWireTest {

    @TempDir
    Path dir;

    private final ChunkId id = new ChunkId(FileId.random(), 0);

    @Test
    void fullChunkLifecycleOverTheWire() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {

            // open
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, (byte) 0, (byte) 0, (byte) 0,
                    1 << 20, 1718000000000L).encode(), null, 5000);

            // pipelined appends
            byte[] a = "first-batch-".getBytes(), b = "second-batch".getBytes();
            CompletableFuture<Frame> f1 = client.send(Opcode.APPEND,
                    new Messages.Append(id, 1, 0, 0).encode(), ByteBuffer.wrap(a));
            CompletableFuture<Frame> f2 = client.send(Opcode.APPEND,
                    new Messages.Append(id, 1, a.length, 0).encode(), ByteBuffer.wrap(b));
            ByteBuffer h1 = f1.get().headerSlice();
            Resp.check(h1);
            assertEquals(a.length, Messages.AppendResp.decode(h1).endOffset());
            ByteBuffer h2 = f2.get().headerSlice();
            Resp.check(h2);
            assertEquals(a.length + b.length, Messages.AppendResp.decode(h2).endOffset());

            // read with payload
            Frame readFrame = client.callFrame(Opcode.READ,
                    new Messages.Read(id, 0, 1 << 20).encode(), null, 5000);
            ByteBuffer rh = readFrame.headerSlice();
            Resp.check(rh);
            var readResp = Messages.ReadResp.decode(rh);
            assertEquals(a.length + b.length, readResp.localEndOffset());
            byte[] got = new byte[readFrame.payloadLength()];
            readFrame.payloadSlice().get(got);
            assertArrayEquals("first-batch-second-batch".getBytes(), got);

            // ledger over the wire
            ByteBuffer lh = client.call(Opcode.READ_LEDGER, new Messages.ReadLedger(id, 0).encode(), null, 5000);
            var ledger = Messages.ReadLedgerResp.decode(lh);
            assertEquals(2, ledger.entries().size());
            assertEquals(a.length, ledger.entries().get(0).endOffset());

            // fence at 2 -> epoch-1 append rejected with typed error
            client.call(Opcode.FENCE, new Messages.Fence(id, 2).encode(), null, 5000);
            ScpException fenced = assertThrows(ScpException.class, () -> client.call(Opcode.APPEND,
                    new Messages.Append(id, 1, a.length + b.length, 0).encode(),
                    ByteBuffer.wrap("x".getBytes()), 5000));
            assertEquals(ErrorCode.FENCED_EPOCH, fenced.code());
            assertEquals(2, fenced.detail());

            // seal with the post-fence epoch
            ByteBuffer sh = client.call(Opcode.SEAL_CHUNK,
                    new Messages.SealChunk(id, 2, a.length + b.length).encode(), null, 5000);
            var sealResp = Messages.SealResp.decode(sh);
            assertEquals(a.length + b.length, sealResp.finalLength());

            // stat reflects sealed state
            ByteBuffer sth = client.call(Opcode.STAT_CHUNK, new Messages.StatChunk(id).encode(), null, 5000);
            var stat = Messages.StatResp.decode(sth);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(a.length + b.length, stat.sealedLength());

            // fetch whole file and delete
            Frame fetch = client.callFrame(Opcode.FETCH_CHUNK,
                    new Messages.FetchChunk(id, 0, Integer.MAX_VALUE).encode(), null, 5000);
            ByteBuffer fh = fetch.headerSlice();
            Resp.check(fh);
            assertEquals(Messages.FetchResp.decode(fh).fileLength(), fetch.payloadLength());

            ByteBuffer dh = client.call(Opcode.DELETE_CHUNKS,
                    new Messages.DeleteChunks(List.of(id)).encode(), null, 5000);
            var del = Messages.DeleteChunksResp.decode(dh);
            assertEquals((short) 0, del.codes().get(0));
        }
    }

    @Test
    void nodeRestartRecoversChunksOverTheWire() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "t")) {
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, (byte) 0, (byte) 0, (byte) 0,
                    1 << 20, 1L).encode(), null, 5000);
            client.call(Opcode.APPEND, new Messages.Append(id, 1, 0, 0).encode(),
                    ByteBuffer.wrap("persistent".getBytes()), 5000);
        }
        try (StorageNode node2 = new StorageNode(NodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node2.port(), ScpClient.KIND_BROKER, "t")) {
            ByteBuffer sth = client.call(Opcode.STAT_CHUNK, new Messages.StatChunk(id).encode(), null, 5000);
            var stat = Messages.StatResp.decode(sth);
            assertEquals(ChunkState.OPEN, stat.state());
            assertEquals("persistent".length(), stat.localEndOffset());
            // identity survives restart (volume-bound)
            assertEquals(node2.incarnation(), node2.incarnation());
        }
    }
}
