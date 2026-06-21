package io.strata.client;

import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import static io.strata.common.Checks.addChunkLength;

/**
 * Reader (tech design §6): sealed chunks from any replica; open-chunk reads are clamped to the
 * replica-known durable offset, so a reader never sees un-quorum-acked bytes (invariant §14.3).
 */
final class ReaderImpl implements StrataFile.Reader {
    private final ControllerClient controller;
    private final NodePool pool;
    private final ClientConfig config;
    private final io.strata.common.FileId fileId;
    private final Map<String, ManagedScpConnection> pinnedConnections = new ConcurrentHashMap<>();

    private volatile Messages.LookupFileResp file;

    ReaderImpl(ControllerClient controller, NodePool pool, ClientConfig config, io.strata.common.FileId fileId) {
        this.controller = controller;
        this.pool = pool;
        this.config = config;
        this.fileId = fileId;
        refresh();
    }

    @Override
    public void refresh() {
        this.file = controller.lookupFile(fileId);
    }

    @Override
    public StrataFile.ReadResult read(long fileOffset, int maxBytes) {
        if (fileOffset < 0) {
            throw new ScpException(ErrorCode.INTERNAL, "negative read offset " + fileOffset);
        }
        if (maxBytes < 0) {
            throw new ScpException(ErrorCode.INTERNAL, "negative read maxBytes " + maxBytes);
        }
        Messages.LookupFileResp f = file;
        long base = 0;
        for (int i = 0; i < f.chunks().size(); i++) {
            Messages.ChunkInfo chunk = f.chunks().get(i);
            boolean last = i == f.chunks().size() - 1;
            if (chunk.state() == ChunkState.SEALED) {
                long chunkEnd = addChunkLength(base, chunk.length());
                if (fileOffset < chunkEnd) {
                    long chunkOffset = fileOffset - base;
                    int want = (int) Math.min(maxBytes, chunk.length() - chunkOffset);
                    Borrowed b = readFromReplicas(chunk, chunkOffset, want, false);
                    // Ownership of b.owner() has transferred to us here; everything between this
                    // point and wrapping it in the (AutoCloseable) ReadResult must stay
                    // allocation/throw-free, or the borrowed pooled buffer leaks before any caller
                    // can close it.
                    boolean eof = f.fileState() == FileState.SEALED.value && last
                            && fileOffset + b.view().remaining() == chunkEnd;
                    return new StrataFile.ReadResult(b.view(), eof, b.owner());
                }
                base = chunkEnd;
            } else {
                long chunkOffset = fileOffset - base;
                Borrowed b = readFromReplicas(chunk, chunkOffset, maxBytes, true);
                return new StrataFile.ReadResult(b.view(), false, b.owner());
            }
        }
        boolean eof = f.fileState() == FileState.SEALED.value;
        return StrataFile.ReadResult.empty(eof);
    }

    private record Borrowed(Frame owner, ByteBuffer view) {}

    private Borrowed readFromReplicas(Messages.ChunkInfo chunk, long offset, int maxBytes, boolean open) {
        List<Messages.Replica> replicas = chunk.replicas();
        if (replicas.isEmpty()) {
            throw new ScpException(ErrorCode.INTERNAL, "no readable replica");
        }
        int start = ThreadLocalRandom.current().nextInt(replicas.size());
        byte[] readHeader = new Messages.Read(chunk.chunkId(), offset, maxBytes).encode();
        ScpException last = null;
        for (int i = 0; i < replicas.size(); i++) {
            Messages.Replica r = replicas.get((start + i) % replicas.size());
            if (r.endpoint().isEmpty()) continue;
            // The borrowed call stays INSIDE the try so a per-replica connection/RPC failure is
            // caught below (last = e) and the loop fails over to the next replica — matching the
            // pre-borrow read path. Hoisting it out would abort the whole read on the first
            // unreachable replica instead of failing over.
            Frame frame = null;
            boolean transferred = false;
            try {
                frame = connectionFor(r.endpoint()).callFrameBorrowed(Opcode.READ,
                        readHeader, null, config.callTimeoutMs());
                ByteBuffer h = frame.headerSlice();
                Resp.check(h);
                var resp = Messages.ReadResp.decode(h);
                if (resp.localEndOffset() < 0 || resp.durableOffset() < 0
                        || resp.durableOffset() > resp.localEndOffset()) {
                    last = new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "bad read offsets from replica " + r.nodeId());
                    continue;
                }
                if (!open && resp.localEndOffset() < chunk.length()) {
                    last = new ScpException(io.strata.common.ErrorCode.CORRUPT_CHUNK,
                            "replica " + r.nodeId() + " short for sealed chunk " + chunk.chunkId()
                                    + ": " + resp.localEndOffset() + " < " + chunk.length());
                    continue;
                }
                if (frame.payloadLength() > maxBytes) {
                    last = new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "replica " + r.nodeId() + " returned " + frame.payloadLength()
                                    + " bytes for max " + maxBytes);
                    continue;
                }
                if (!open && frame.payloadLength() != maxBytes) {
                    last = new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "replica " + r.nodeId() + " returned short sealed read "
                                    + frame.payloadLength() + " != " + maxBytes);
                    continue;
                }
                ByteBuffer view = frame.payloadSlice();
                if (open) {
                    // never expose bytes above the replica-known durable offset (limit, not copy)
                    long readable = Math.min(resp.localEndOffset(), resp.durableOffset());
                    long visible = Math.max(0, readable - offset);
                    if (view.remaining() > visible) {
                        view.limit(view.position() + (int) visible);
                    }
                }
                transferred = true;
                return new Borrowed(frame, view);
            } catch (ScpException e) {
                last = e;
            } catch (RuntimeException e) {
                last = new ScpException(ErrorCode.CORRUPT_CHUNK,
                        "malformed read response from replica " + r.nodeId() + ": " + e);
            } finally {
                // frame is null when callFrameBorrowed itself threw (nothing to release); otherwise
                // release on validation failure / exception / continue. Success sets transferred.
                if (!transferred && frame != null) frame.close();
            }
        }
        throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "no readable replica");
    }

    private ManagedScpConnection connectionFor(String endpoint) {
        return pinnedConnections.computeIfAbsent(endpoint, pool::get);
    }

    @Override
    public void close() {
    }
}
