package io.strata.client;

import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reader (tech design §6): sealed chunks from any replica; open-chunk reads are clamped to the
 * replica-known durable offset, so a reader never sees un-quorum-acked bytes (invariant §14.3).
 */
final class ReaderImpl implements SegmentStore.Reader {
    private final MetaClient meta;
    private final NodePool pool;
    private final ClientConfig config;
    private final io.strata.common.FileId fileId;

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
    public SegmentStore.ReadResult read(long fileOffset, int maxBytes) {
        Messages.LookupFileResp f = file;
        long base = 0;
        for (int i = 0; i < f.chunks().size(); i++) {
            Messages.ChunkInfo chunk = f.chunks().get(i);
            boolean last = i == f.chunks().size() - 1;
            if (chunk.state() == ChunkState.SEALED) {
                if (fileOffset < base + chunk.length()) {
                    long chunkOffset = fileOffset - base;
                    int want = (int) Math.min(maxBytes, chunk.length() - chunkOffset);
                    byte[] data = readFromReplicas(chunk, chunkOffset, want, false);
                    boolean eof = f.fileState() == 1 /* SEALED */ && last
                            && fileOffset + data.length == base + chunk.length();
                    return new SegmentStore.ReadResult(data, eof);
                }
                base += chunk.length();
            } else {
                // open chunk: serve [fileOffset-base, durableOffset)
                long chunkOffset = fileOffset - base;
                byte[] data = readFromReplicas(chunk, chunkOffset, maxBytes, true);
                return new SegmentStore.ReadResult(data, false);
            }
        }
        boolean eof = f.fileState() == 1;
        return new SegmentStore.ReadResult(new byte[0], eof);
    }

    private byte[] readFromReplicas(Messages.ChunkInfo chunk, long offset, int maxBytes, boolean open) {
        List<Messages.Replica> replicas = new ArrayList<>(chunk.replicas());
        Collections.shuffle(replicas);
        ScpException last = null;
        for (Messages.Replica r : replicas) {
            if (r.endpoint().isEmpty()) continue;
            try {
                Frame frame = pool.get(r.endpoint()).callFrame(Opcode.READ,
                        new Messages.Read(chunk.chunkId(), offset, maxBytes).encode(), null,
                        config.callTimeoutMs());
                ByteBuffer h = frame.headerSlice();
                Resp.check(h);
                var resp = Messages.ReadResp.decode(h);
                if (!open && resp.localEndOffset() < chunk.length()) {
                    // a sealed chunk must be served whole: a replica shorter than the descriptor
                    // (seal straggler not yet reconciled away) would return truncated data
                    last = new ScpException(io.strata.common.ErrorCode.CORRUPT_CHUNK,
                            "replica " + r.nodeId() + " short for sealed chunk " + chunk.chunkId()
                                    + ": " + resp.localEndOffset() + " < " + chunk.length());
                    continue;
                }
                byte[] data = new byte[frame.payloadLength()];
                frame.payloadSlice().get(data);
                if (open) {
                    // never expose bytes above the replica-known durable offset
                    long visible = Math.max(0, resp.durableOffset() - offset);
                    if (data.length > visible) {
                        data = java.util.Arrays.copyOf(data, (int) visible);
                    }
                }
                return data;
            } catch (ScpException e) {
                last = e;
            }
        }
        throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "no readable replica");
    }

    @Override
    public void close() {
    }
}
