package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MessagesNamespaceRoundTripTest {
    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");
    private static final FileId FID = FileId.of(1);
    private static final ChunkId CID = new ChunkId(FID, 3);

    @Test
    void createChunkCarriesNamespace() {
        var m = new Messages.CreateChunk(NS, FID, 7, 1L, 2L, List.of(5, 9));
        var d = Messages.CreateChunk.decode(ByteBuffer.wrap(m.encode()));
        assertEquals(NS, d.namespace());
        assertEquals(FID, d.fileId());
        assertEquals(7, d.writeEpoch());
        assertEquals(List.of(5, 9), d.excludedNodeIds());
    }

    @Test
    void sealChunkMetaCarriesNamespace() {
        var m = new Messages.SealChunkMeta(NS, CID, 7, 100L, 42, List.of(1, 2));
        var d = Messages.SealChunkMeta.decode(ByteBuffer.wrap(m.encode()));
        assertEquals(NS, d.namespace());
        assertEquals(CID, d.chunkId());
        assertEquals(List.of(1, 2), d.sealedReplicas());
    }

    @Test
    void allocateWriterEpochCarriesNamespace() {
        var m = Messages.AllocateWriterEpoch.forAppend(NS, FID);
        var d = Messages.AllocateWriterEpoch.decode(ByteBuffer.wrap(m.encode()));
        assertEquals(NS, d.namespace());
        assertEquals(FID, d.fileId());
        assertEquals(Messages.AllocateWriterEpoch.FOR_APPEND, d.purpose());
    }

    @Test
    void verifyChunksRequestRoundTrips() {
        var m = new Messages.VerifyChunks(NS, "127.0.0.1:9301", List.of(CID, new ChunkId(FileId.of(8), 0)));
        var d = Messages.VerifyChunks.decode(ByteBuffer.wrap(m.encode()));
        assertEquals(NS, d.namespace());
        assertEquals("127.0.0.1:9301", d.verifierEndpoint());
        assertEquals(List.of(CID, new ChunkId(FileId.of(8), 0)), d.chunkIds());
    }

    @Test
    void verifyChunksResponseRoundTrips() {
        var results = List.of(
                new Messages.VerifyChunkResult(CID, true, ChunkState.SEALED, 1234L, 0x55aa55aa),
                new Messages.VerifyChunkResult(new ChunkId(FileId.of(8), 0), false, ChunkState.OPEN, 0L, 0));
        var enc = new Messages.VerifyChunksResp(results).encode();
        var b = ByteBuffer.wrap(enc);
        Resp.check(b); // strip the OK status the framing layer would have consumed
        var d = Messages.VerifyChunksResp.decode(b);
        assertEquals(results, d.results());
    }

    @Test
    void abortSealLookupDeleteCarryNamespace() {
        var abort = Messages.AbortChunkMeta.decode(
                ByteBuffer.wrap(new Messages.AbortChunkMeta(NS, CID, 7, 1L, 2L).encode()));
        assertEquals(NS, abort.namespace());

        var seal = Messages.SealFile.decode(ByteBuffer.wrap(new Messages.SealFile(NS, FID, 64L).encode()));
        assertEquals(NS, seal.namespace());

        var lf = Messages.LookupFile.decode(ByteBuffer.wrap(new Messages.LookupFile(NS, FID).encode()));
        assertEquals(NS, lf.namespace());

        var del = Messages.DeleteFiles.decode(
                ByteBuffer.wrap(new Messages.DeleteFiles(NS, List.of(FID)).encode()));
        assertEquals(NS, del.namespace());
        assertEquals(List.of(FID), del.fileIds());
    }
}
