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
final class DataNodeHandlers implements ScpServer.Handler {
    private final ChunkStore store;
    private final DataNode node;
    private volatile ControlLoop controlLoop; // wired after the control loop starts (owner-repair path)

    DataNodeHandlers(ChunkStore store, DataNode node) {
        this.store = store;
        this.node = node;
    }

    /** Wires the control loop so this node can serve direct owner-driven EXEC_REPLICATE repairs. */
    void controlLoop(ControlLoop controlLoop) {
        this.controlLoop = controlLoop;
    }

    @Override
    public java.util.concurrent.CompletableFuture<Frame> handleAsync(Frame req) throws Exception {
        if (req.opcode() == Opcode.APPEND.code) {
            // validation + write run synchronously here (per-chunk ordering preserved); the ack
            // defers until durability per the chunk's policy — for ack-on-fsync that means a
            // covering group-commit force, while this connection keeps processing frames
            var m = Messages.Append.decode(req.headerSlice());
            io.strata.proto.RequestContext.setNamespace(m.namespace().value());
            return store.appendAsync(m.namespace(), m.chunkId(), m.writeEpoch(), m.baseOffset(), m.durableOffset(),
                            req.payloadSlice(), req.payloadCrc())
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
                io.strata.proto.RequestContext.setNamespace(m.namespace().value());
                if (node.isDraining()) {
                    throw new ScpException(ErrorCode.NO_CAPACITY, "node draining");
                }
                store.open(m.namespace(), m.chunkId(), m.fsyncOnAck(), m.writeEpoch(), m.createdAtMs());
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            // APPEND is served by handleAsync (deferred ack for group commit)

            case READ -> {
                // Client read: open reads are bounded to the replica-known durable high watermark, so
                // both open durable-prefix reads and sealed reads can use a zero-copy file region.
                var m = Messages.Read.decode(h);
                io.strata.proto.RequestContext.setNamespace(m.namespace().value());
                yield readRegionResponse(req, store.readRegion(m.namespace(), m.chunkId(), m.offset(), m.maxBytes()));
            }

            case READ_RECOVERY -> {
                // Seal recovery reads the never-acked tail above the durable watermark (clamped away
                // from client READs) to re-prove and re-replicate bytes a quorum still holds.
                var m = Messages.Read.decode(h);
                io.strata.proto.RequestContext.setNamespace(m.namespace().value());
                yield readRegionResponse(req, store.readRegionForRecovery(m.namespace(), m.chunkId(), m.offset(), m.maxBytes()));
            }

            case FENCE -> {
                var m = Messages.Fence.decode(h);
                io.strata.proto.RequestContext.setNamespace(m.namespace().value());
                var r = store.fence(m.namespace(), m.chunkId(), m.fenceEpoch());
                yield ScpServer.ok(req, new Messages.FenceResp(r.persistedFenceEpoch(), r.localEndOffset(),
                        r.lastKnownDO(), r.state()).encode(), null);
            }

            case STAT_CHUNK -> {
                var m = Messages.StatChunk.decode(h);
                io.strata.proto.RequestContext.setNamespace(m.namespace().value());
                var r = store.stat(m.namespace(), m.chunkId());
                yield ScpServer.ok(req, new Messages.StatResp(r.state(), r.localEndOffset(), r.lastKnownDO(),
                        r.writeEpoch(), r.fenceEpoch(), r.sealedLength(), r.dataCrc()).encode(), null);
            }

            case SEAL_CHUNK -> {
                var m = Messages.SealChunk.decode(h);
                io.strata.proto.RequestContext.setNamespace(m.namespace().value());
                var r = store.seal(m.namespace(), m.chunkId(), m.writeEpoch(), m.dataLength(),
                        req.payloadLength() > 0 ? req.payloadSlice() : null);
                yield ScpServer.ok(req, new Messages.SealResp(r.finalLength(), r.dataCrc()).encode(), null);
            }

            case DELETE_CHUNKS -> {
                var m = Messages.DeleteChunks.decode(h);
                io.strata.proto.RequestContext.setNamespace(m.namespace().value());
                List<Short> codes = new ArrayList<>(m.chunkIds().size());
                for (var id : m.chunkIds()) codes.add(store.delete(m.namespace(), id).code);
                yield ScpServer.ok(req, new Messages.DeleteChunksResp(m.chunkIds(), codes).encode(), null);
            }

            case FETCH_CHUNK -> {
                var m = Messages.FetchChunk.decode(h);
                io.strata.proto.RequestContext.setNamespace(m.namespace().value());
                var r = store.fetch(m.namespace(), m.chunkId(), m.offset(), m.maxBytes());
                yield ScpServer.ok(req, new Messages.FetchResp(r.fileLength(), r.state()).encode(),
                        ByteBuffer.wrap(r.bytes()));
            }

            case READ_LEDGER -> {
                var m = Messages.ReadLedger.decode(h);
                io.strata.proto.RequestContext.setNamespace(m.namespace().value());
                List<ChunkFormats.LedgerEntry> entries = store.readLedger(m.namespace(), m.chunkId(), m.fromOffset());
                List<Messages.LedgerEntry> wire = new ArrayList<>(entries.size());
                for (var e : entries) wire.add(new Messages.LedgerEntry(e.endOffset(), e.payloadCrc(), e.writeEpoch()));
                yield ScpServer.ok(req, new Messages.ReadLedgerResp(wire).encode(), null);
            }

            case EXEC_REPLICATE -> {
                // A namespace owner that is not the cluster controller drives repair directly: pull the
                // chunk from a live source via the proven control-loop path (design §11). Synchronous —
                // the response confirms the pull+import completed.
                ControlLoop loop = controlLoop;
                if (loop == null) {
                    throw new ScpException(ErrorCode.INTERNAL, "control loop unavailable for EXEC_REPLICATE");
                }
                if (!(Messages.Command.read(h) instanceof Messages.ReplicateCmd cmd)) {
                    throw new ScpException(ErrorCode.PRECONDITION_FAILED, "EXEC_REPLICATE requires a ReplicateCmd");
                }
                io.strata.proto.RequestContext.setNamespace(cmd.namespace().value());
                loop.replicate(cmd);
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            case VERIFY_CHUNKS -> {
                // Owner-pull durability verification (design §20.3): report the local state of each
                // requested chunk (present/state/length/crc) and stamp the present ones as freshly
                // verified, feeding node-local orphan GC (§20.4). The owner judges missing/corrupt.
                var m = Messages.VerifyChunks.decode(h);
                io.strata.proto.RequestContext.setNamespace(m.namespace().value());
                node.noteVerifiedBy(m.verifierEndpoint());
                List<Messages.VerifyChunkResult> results = new ArrayList<>(m.chunkIds().size());
                for (ChunkStore.VerifyResult r : store.verify(m.namespace(), m.chunkIds())) {
                    results.add(new Messages.VerifyChunkResult(r.chunkId(), r.present(), r.state(),
                            r.length(), r.crc()));
                }
                yield ScpServer.ok(req, new Messages.VerifyChunksResp(results).encode(), null);
            }

            default -> throw new ScpException(ErrorCode.UNKNOWN_OPCODE, op + " not served by data node");
        };
    }

    /** Wire-encodes a {@link ChunkStore.ReadRegionResult}: a zero-copy region or materialized
     * bytes. The server releases the read resource via Frame.close() — a channel-cache lease for
     * sealed chunks, or the transient FD for open chunks. */
    private static Frame readRegionResponse(Frame req, ChunkStore.ReadRegionResult r) {
        byte[] header = new Messages.ReadResp(r.localEndOffset(), r.lastKnownDO()).encode();
        if (r.channel() != null) {
            return ScpServer.okFileRegion(req, header, r.channel(), r.filePosition(), r.length(),
                    r.releaser());
        }
        byte[] bytes = r.bytes();
        return ScpServer.ok(req, header, bytes != null && bytes.length > 0 ? ByteBuffer.wrap(bytes) : null);
    }
}
