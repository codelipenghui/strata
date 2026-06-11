package io.strata.node;

import io.strata.common.ChunkState;
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
                store.open(m.chunkId(), m.fileKind(), m.mediaClass(), m.ackPolicy(), m.writeEpoch(), m.createdAtMs());
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            // APPEND is served by handleAsync (deferred ack for group commit)

            case READ -> {
                var m = Messages.Read.decode(h);
                var r = store.read(m.chunkId(), m.offset(), m.maxBytes());
                yield ScpServer.ok(req, new Messages.ReadResp(r.localEndOffset(), r.lastKnownDO()).encode(),
                        ByteBuffer.wrap(r.bytes()));
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
                yield ScpServer.ok(req, new Messages.FetchResp(r.fileLength(),
                        r.state() == null ? ChunkState.SEALED : r.state()).encode(), ByteBuffer.wrap(r.bytes()));
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
}
