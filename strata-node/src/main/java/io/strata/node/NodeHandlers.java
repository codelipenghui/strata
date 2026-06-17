package io.strata.node;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.format.ChunkFormats;
import io.strata.format.ChunkStore;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Maps SCP data-plane opcodes onto the ChunkStore engine (tech design §10.3). */
final class NodeHandlers implements ScpServer.Handler {
    private final ChunkStore store;
    private final StorageNode node;

    NodeHandlers(ChunkStore store, StorageNode node) {
        this.store = store;
        this.node = node;
    }

    @Override
    public java.util.concurrent.CompletableFuture<Frame> handleAsync(Frame req) throws Exception {
        if (req.opcode() == Opcode.APPEND.code) {
            // validation + write run synchronously here (per-chunk ordering preserved); the ack
            // defers until durability per the chunk's policy — for ack-on-fsync that means a
            // covering group-commit force, while this connection keeps processing frames
            var m = Messages.Append.decode(req.headerSlice());
            return store.appendAsync(m.chunkId(), m.writeEpoch(), m.baseOffset(), m.durableOffset(),
                            req.payloadSlice())
                    .thenApply(r -> ScpServer.ok(req, new Messages.AppendResp(r.endOffset()).encode(), null));
        }
        return java.util.concurrent.CompletableFuture.completedFuture(handle(req));
    }

    @Override
    public Frame handle(Frame req) throws Exception {
        Opcode op = Opcode.fromCode(req.opcode());
        if (op == null) throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "0x" + Integer.toHexString(req.opcode()));
        ByteBuffer h = req.headerSlice();
        return switch (op) {
            case PING -> ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());

            case OPEN_CHUNK -> {
                var m = Messages.OpenChunk.decode(h);
                if (node.isDraining()) {
                    throw new ScpException(ErrorCode.NO_CAPACITY, "node draining");
                }
                store.open(m.chunkId(), m.fsyncOnAck(), m.writeEpoch(), m.createdAtMs());
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            // APPEND is served by handleAsync (deferred ack for group commit)

            case READ -> {
                // Client read: open reads are bounded to the replica-known durable high watermark and
                // materialized under the chunk lock so a concurrent seal-truncate cannot alter the wire
                // payload. Sealed chunks can use the zero-copy region.
                var m = Messages.Read.decode(h);
                yield readRegionResponse(req, store.readRegion(m.chunkId(), m.offset(), m.maxBytes()));
            }

            case READ_RECOVERY -> {
                // Seal recovery reads the never-acked tail above the durable watermark (clamped away
                // from client READs) to re-prove and re-replicate bytes a quorum still holds.
                var m = Messages.Read.decode(h);
                yield readRegionResponse(req, store.readRegionForRecovery(m.chunkId(), m.offset(), m.maxBytes()));
            }

            case FENCE -> {
                var m = Messages.Fence.decode(h);
                var r = store.fence(m.chunkId(), m.fenceEpoch());
                yield ScpServer.ok(req, new Messages.FenceResp(r.persistedFenceEpoch(), r.localEndOffset(),
                        r.lastKnownDO(), r.state()).encode(), null);
            }

            case STAT_CHUNK -> {
                var m = Messages.StatChunk.decode(h);
                var r = store.stat(m.chunkId());
                yield ScpServer.ok(req, new Messages.StatResp(r.state(), r.localEndOffset(), r.lastKnownDO(),
                        r.writeEpoch(), r.fenceEpoch(), r.sealedLength(), r.dataCrc()).encode(), null);
            }

            case SEAL_CHUNK -> {
                var m = Messages.SealChunk.decode(h);
                var r = store.seal(m.chunkId(), m.writeEpoch(), m.dataLength(),
                        req.payloadLength() > 0 ? req.payloadSlice() : null);
                yield ScpServer.ok(req, new Messages.SealResp(r.finalLength(), r.dataCrc()).encode(), null);
            }

            case DELETE_CHUNKS -> {
                var m = Messages.DeleteChunks.decode(h);
                List<Short> codes = new ArrayList<>(m.chunkIds().size());
                for (var id : m.chunkIds()) codes.add(store.delete(id).code);
                yield ScpServer.ok(req, new Messages.DeleteChunksResp(m.chunkIds(), codes).encode(), null);
            }

            case FETCH_CHUNK -> {
                var m = Messages.FetchChunk.decode(h);
                var r = store.fetch(m.chunkId(), m.offset(), m.maxBytes());
                yield ScpServer.ok(req, new Messages.FetchResp(r.fileLength(), r.state()).encode(),
                        ByteBuffer.wrap(r.bytes()));
            }

            case READ_LEDGER -> {
                var m = Messages.ReadLedger.decode(h);
                List<ChunkFormats.LedgerEntry> entries = store.readLedger(m.chunkId(), m.fromOffset());
                List<Messages.LedgerEntry> wire = new ArrayList<>(entries.size());
                for (var e : entries) wire.add(new Messages.LedgerEntry(e.endOffset(), e.payloadCrc(), e.writeEpoch()));
                yield ScpServer.ok(req, new Messages.ReadLedgerResp(wire).encode(), null);
            }

            default -> throw new ScpException(ErrorCode.UNKNOWN_OPCODE, op + " not served by storage node");
        };
    }

    /** Wire-encodes a {@link ChunkStore.ReadRegionResult}: a zero-copy region (sealed) or materialized
     * bytes (open). readRegion owns and closes any returned channel via the server. */
    private static Frame readRegionResponse(Frame req, ChunkStore.ReadRegionResult r) {
        byte[] header = new Messages.ReadResp(r.localEndOffset(), r.lastKnownDO()).encode();
        if (r.channel() != null) {
            return ScpServer.okFileRegion(req, header, r.channel(), r.filePosition(), r.length());
        }
        byte[] bytes = r.bytes();
        return ScpServer.ok(req, header, bytes != null && bytes.length > 0 ? ByteBuffer.wrap(bytes) : null);
    }
}
