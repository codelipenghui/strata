package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageRoundtripTest {

    private final FileId f = FileId.fromString("11111111-2222-3333-4444-555555555555");
    private final ChunkId c = new ChunkId(f, 3);

    private static ByteBuffer buf(byte[] b) {
        return ByteBuffer.wrap(b);
    }

    @Test
    void dataPlaneRoundtrips() {
        var open = new Messages.OpenChunk(c, 5, true, 1 << 30, 1718000000000L);
        assertEquals(open, Messages.OpenChunk.decode(buf(open.encode())));

        var append = new Messages.Append(c, 5, 1024, 512);
        assertEquals(append, Messages.Append.decode(buf(append.encode())));

        var read = new Messages.Read(c, 99, 65536);
        assertEquals(read, Messages.Read.decode(buf(read.encode())));

        var fence = new Messages.Fence(c, 6);
        assertEquals(fence, Messages.Fence.decode(buf(fence.encode())));

        var stat = new Messages.StatChunk(c);
        assertEquals(stat, Messages.StatChunk.decode(buf(stat.encode())));

        var seal = new Messages.SealChunk(c, 5, 4096);
        assertEquals(seal, Messages.SealChunk.decode(buf(seal.encode())));

        var del = new Messages.DeleteChunks(List.of(c, new ChunkId(f, 4)));
        assertEquals(del, Messages.DeleteChunks.decode(buf(del.encode())));

        var fetch = new Messages.FetchChunk(c, 0, Integer.MAX_VALUE);
        assertEquals(fetch, Messages.FetchChunk.decode(buf(fetch.encode())));

        var rl = new Messages.ReadLedger(c, 2048);
        assertEquals(rl, Messages.ReadLedger.decode(buf(rl.encode())));
    }

    @Test
    void responseRoundtrips() {
        var ar = new Messages.AppendResp(2048);
        assertEquals(ar, decodeResp(ar.encode(), Messages.AppendResp::decode));

        var rr = new Messages.ReadResp(4096, 2048);
        assertEquals(rr, decodeResp(rr.encode(), Messages.ReadResp::decode));

        var fr = new Messages.FenceResp(7, 100, 80, ChunkState.OPEN);
        assertEquals(fr, decodeResp(fr.encode(), Messages.FenceResp::decode));

        var sr = new Messages.StatResp(ChunkState.SEALED, 100, 100, 5, 7, 100, 0xCAFE);
        assertEquals(sr, decodeResp(sr.encode(), Messages.StatResp::decode));

        var slr = new Messages.SealResp(4096, 0xBEEF);
        assertEquals(slr, decodeResp(slr.encode(), Messages.SealResp::decode));

        var rlr = new Messages.ReadLedgerResp(List.of(
                new Messages.LedgerEntry(100, 1, 5), new Messages.LedgerEntry(200, 2, 5)));
        assertEquals(rlr, decodeResp(rlr.encode(), Messages.ReadLedgerResp::decode));

        var dcr = new Messages.DeleteChunksResp(List.of(c), List.of((short) 0));
        assertEquals(dcr, decodeResp(dcr.encode(), Messages.DeleteChunksResp::decode));

        var fer = new Messages.FetchResp(8192, ChunkState.SEALED);
        assertEquals(fer, decodeResp(fer.encode(), Messages.FetchResp::decode));
    }

    @Test
    void controlPlaneRoundtrips() {
        var reg = new Messages.RegisterNode(1L, 2L, List.of("h1:9000", "h2:9000"), "z1", "r1", "host1",
                List.of(new Messages.StorageCapacity(1L << 40)), 1, 0);
        assertEquals(reg, Messages.RegisterNode.decode(buf(reg.encode())));

        var regResp = new Messages.RegisterResp(42, 9, 1000, 10000);
        assertEquals(regResp, decodeResp(regResp.encode(), Messages.RegisterResp::decode));

        var hb = new Messages.NodeHeartbeat(42, 1, 2, 9,
                List.of(new Messages.StorageUsage(100, 900)), 3,
                List.of(new Messages.CompletedCommand(7, (short) 0)));
        assertEquals(hb, Messages.NodeHeartbeat.decode(buf(hb.encode())));

        var hbEmpty = new Messages.NodeHeartbeat(42, 1, 2, 9, List.of(), 0, List.of());
        assertEquals(hbEmpty, Messages.NodeHeartbeat.decode(buf(hbEmpty.encode())));

        var hbResp = new Messages.HeartbeatResp(123456, List.of(
                new Messages.ReplicateCmd(1, c, List.of(new Messages.Replica(7, "h7:9000")), (byte) 1, 0xAA, 4096),
                new Messages.DeleteCmd(2, List.of(c)),
                new Messages.DrainCmd(3)));
        assertEquals(hbResp, decodeResp(hbResp.encode(), Messages.HeartbeatResp::decode));

        var inv = new Messages.InventoryReport(42, 1, 2, 9, 0, 1,
                List.of(new Messages.InventoryEntry(c, ChunkState.SEALED, 4096, 0xAB)));
        assertEquals(inv, Messages.InventoryReport.decode(buf(inv.encode())));
    }

    @Test
    void nodeHeartbeatRejectsTrailingBytesInCompletedCommandTag() {
        BufWriter completed = new BufWriter();
        completed.varint(1).u64(7).u16(0).u8(99);

        BufWriter heartbeat = new BufWriter();
        heartbeat.u32(42).u64(1).u64(2).u64(9).varint(0).u32(0);
        TaggedFields.of(Map.of(Messages.NodeHeartbeat.TAG_COMPLETED_COMMANDS, completed.toBytes()))
                .writeTo(heartbeat);

        assertThrows(IllegalArgumentException.class,
                () -> Messages.NodeHeartbeat.decode(buf(heartbeat.toBytes())));
    }

    @Test
    void clientMetaRoundtrips() {
        var cf = new Messages.CreateFile("test", "/kafka/topicA/0/00000000000000000000");
        assertEquals(cf, Messages.CreateFile.decode(buf(cf.encode())));

        var cfr = new Messages.CreateFileResp(f);
        assertEquals(cfr, decodeResp(cfr.encode(), Messages.CreateFileResp::decode));

        var cc = new Messages.CreateChunk(f, 5);
        assertEquals(cc, Messages.CreateChunk.decode(buf(cc.encode())));

        var ccExcluded = new Messages.CreateChunk(f, 5, 11, 12, List.of(2, 4));
        assertEquals(ccExcluded, Messages.CreateChunk.decode(buf(ccExcluded.encode())));

        var ccr = new Messages.CreateChunkResp(c, 5,
                List.of(new Messages.Replica(1, "a:1"), new Messages.Replica(2, "b:2"), new Messages.Replica(3, "c:3")));
        assertEquals(ccr, decodeResp(ccr.encode(), Messages.CreateChunkResp::decode));

        var appendEpoch = Messages.AllocateWriterEpoch.forAppend(f);
        assertEquals(appendEpoch, Messages.AllocateWriterEpoch.decode(buf(appendEpoch.encode())));

        var recoveryEpoch = Messages.AllocateWriterEpoch.forRecovery(f);
        assertEquals(recoveryEpoch, Messages.AllocateWriterEpoch.decode(buf(recoveryEpoch.encode())));

        assertThrows(IllegalArgumentException.class,
                () -> new Messages.AllocateWriterEpoch(f, (byte) 99));

        var epochResp = new Messages.AllocateWriterEpochResp(7);
        assertEquals(epochResp, decodeResp(epochResp.encode(), Messages.AllocateWriterEpochResp::decode));

        var scm = new Messages.SealChunkMeta(c, 5, 4096, 0xDD, List.of(1, 2));
        assertEquals(scm, Messages.SealChunkMeta.decode(buf(scm.encode())));

        var abort = new Messages.AbortChunkMeta(c, 5, 1, 2);
        assertEquals(abort, Messages.AbortChunkMeta.decode(buf(abort.encode())));

        var lf = new Messages.LookupFile(f);
        assertEquals(lf, Messages.LookupFile.decode(buf(lf.encode())));

        var lp = new Messages.LookupPath("test", "/kafka/topicA/0/00000000000000000000");
        assertEquals(lp, Messages.LookupPath.decode(buf(lp.encode())));

        var lpr = new Messages.LookupPathResp(f);
        assertEquals(lpr, decodeResp(lpr.encode(), Messages.LookupPathResp::decode));

        var lfr = new Messages.LookupFileResp("test", "/kafka/topicA/0/00000000000000000000",
                Messages.WritePolicy.DEFAULT, (byte) 0, List.of(
                        new Messages.ChunkInfo(c, ChunkState.OPEN, 0, 0, 5,
                                List.of(new Messages.Replica(1, "a:1")))));
        assertEquals(lfr, decodeResp(lfr.encode(), Messages.LookupFileResp::decode));

        var df = new Messages.DeleteFiles(List.of(f));
        assertEquals(df, Messages.DeleteFiles.decode(buf(df.encode())));

        var dfr = new Messages.DeleteFilesResp(List.of(f), List.of((short) 0));
        assertEquals(dfr, decodeResp(dfr.encode(), Messages.DeleteFilesResp::decode));

        var sf = new Messages.SealFile(f, 1 << 20);
        assertEquals(sf, Messages.SealFile.decode(buf(sf.encode())));
    }

    @Test
    void helloRoundtrip() {
        var h = new Messages.Hello(ScpClient.KIND_BROKER, 0, "test-client");
        assertEquals(h, Messages.Hello.decode(buf(h.encode())));

        var hr = new Messages.HelloResp(0, 42, 1, 2, FrameIO.MAX_FRAME_BYTES, 1 << 30);
        assertEquals(hr, decodeResp(hr.encode(), Messages.HelloResp::decode));
    }

    private static <T> T decodeResp(byte[] bytes, java.util.function.Function<ByteBuffer, T> decoder) {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        Resp.check(b);
        return decoder.apply(b);
    }
}
