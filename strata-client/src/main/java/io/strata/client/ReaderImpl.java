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
    private final MetaClient meta;
    private final NodePool pool;
    private final ClientConfig config;
    private final io.strata.common.FileId fileId;
    private final Map<String, ManagedScpConnection> pinnedConnections = new ConcurrentHashMap<>();

    private volatile Messages.LookupFileResp file;

    ReaderImpl(MetaClient meta, NodePool pool, ClientConfig config, io.strata.common.FileId fileId) {
        this.meta = meta;
        this.pool = pool;
        this.config = config;
        this.fileId = fileId;
        refresh();
    }

    @Override
    public void refresh() {
        this.file = meta.lookupFile(fileId);
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
                    byte[] data = readFromReplicas(chunk, chunkOffset, want, false);
                    boolean eof = f.fileState() == FileState.SEALED.value && last
                            && fileOffset + data.length == chunkEnd;
                    return new StrataFile.ReadResult(data, eof);
                }
                base = chunkEnd;
            } else {
                // open chunk: serve [fileOffset-base, durableOffset)
                long chunkOffset = fileOffset - base;
                byte[] data = readFromReplicas(chunk, chunkOffset, maxBytes, true);
                return new StrataFile.ReadResult(data, false);
            }
        }
        boolean eof = f.fileState() == FileState.SEALED.value;
        return new StrataFile.ReadResult(new byte[0], eof);
    }

    private byte[] readFromReplicas(Messages.ChunkInfo chunk, long offset, int maxBytes, boolean open) {
        List<Messages.Replica> replicas = chunk.replicas();
        if (replicas.isEmpty()) {
            throw new ScpException(ErrorCode.INTERNAL, "no readable replica");
        }
        // spread the first attempt across replicas without copying/shuffling the list per read
        int start = ThreadLocalRandom.current().nextInt(replicas.size());
        byte[] readHeader = new Messages.Read(chunk.chunkId(), offset, maxBytes).encode();
        ScpException last = null;
        for (int i = 0; i < replicas.size(); i++) {
            Messages.Replica r = replicas.get((start + i) % replicas.size());
            if (r.endpoint().isEmpty()) continue;
            try {
                Frame frame = connectionFor(r.endpoint()).callFrame(Opcode.READ,
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
                    // a sealed chunk must be served whole: a replica shorter than the descriptor
                    // (seal straggler not yet reconciled away) would return truncated data
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
                byte[] data = new byte[frame.payloadLength()];
                frame.payloadSlice().get(data);
                if (open) {
                    // never expose bytes above the replica-known durable offset
                    long readable = Math.min(resp.localEndOffset(), resp.durableOffset());
                    long visible = Math.max(0, readable - offset);
                    if (data.length > visible) {
                        data = java.util.Arrays.copyOf(data, (int) visible);
                    }
                }
                return data;
            } catch (ScpException e) {
                last = e;
            } catch (RuntimeException e) {
                last = new ScpException(ErrorCode.CORRUPT_CHUNK,
                        "malformed read response from replica " + r.nodeId() + ": " + e);
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
