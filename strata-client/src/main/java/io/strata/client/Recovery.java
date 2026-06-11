package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Seal recovery (tech design §7.3): fence reachable replicas at the new epoch, start from the
 * highest piggybacked DO, walk the integrity-ledger boundaries forward, preserve any batch found
 * CRC-valid on any reachable replica by re-replicating it to quorum, seal at the first gap.
 *
 * Tolerance: requires >=2 reachable replicas (same fault model as Kafka min.insync.replicas=2).
 * Bytes beyond the seal point were never producer-acked; discarding them is correct.
 */
final class Recovery {
    private static final Logger log = LoggerFactory.getLogger(Recovery.class);

    private final MetaClient meta;
    private final NodePool pool;
    private final ClientConfig config;

    Recovery(MetaClient meta, NodePool pool, ClientConfig config) {
        this.meta = meta;
        this.pool = pool;
        this.config = config;
    }

    SegmentStore.SealInfo recoverAndSeal(io.strata.common.FileId fileId, int newEpoch) {
        Messages.LookupFileResp file = meta.lookupFile(fileId);
        long total = 0;
        for (Messages.ChunkInfo chunk : file.chunks()) {
            if (chunk.state() == ChunkState.SEALED) {
                total += chunk.length();
            } else {
                total += recoverChunk(chunk, newEpoch);
            }
        }
        meta.sealFile(fileId, total);
        return new SegmentStore.SealInfo(total);
    }

    private long recoverChunk(Messages.ChunkInfo chunk, int newEpoch) {
        ChunkId chunkId = chunk.chunkId();

        // 1. fence all reachable replicas; collect their state
        record ReplicaView(Messages.Replica replica, Messages.FenceResp fence) {}
        List<ReplicaView> reachable = new ArrayList<>();
        for (Messages.Replica r : chunk.replicas()) {
            if (r.endpoint().isEmpty()) continue;
            try {
                ByteBuffer h = pool.get(r.endpoint()).call(Opcode.FENCE,
                        new Messages.Fence(chunkId, newEpoch).encode(), null, config.callTimeoutMs());
                reachable.add(new ReplicaView(r, Messages.FenceResp.decode(h)));
            } catch (ScpException e) {
                log.warn("fence {} on {} failed: {}", chunkId, r.endpoint(), e.getMessage());
            }
        }
        if (reachable.size() < 2) {
            throw new ScpException(ErrorCode.INTERNAL,
                    "chunk " + chunkId + " unavailable: " + reachable.size() + " reachable replicas");
        }

        List<Messages.Replica> reachableReplicas = reachable.stream().map(ReplicaView::replica).toList();

        // a replica may already hold a sealed copy (writer died mid-seal)
        for (ReplicaView v : reachable) {
            if (v.fence().state() == ChunkState.SEALED) {
                long len = v.fence().localEndOffset();
                log.info("chunk {} found sealed at {} on {}", chunkId, len, v.replica().endpoint());
                return finishSeal(chunkId, newEpoch, len, reachableReplicas);
            }
        }

        // 2. start from the highest piggybacked DO — everything below is known quorum-durable
        long p = 0;
        for (ReplicaView v : reachable) {
            p = Math.max(p, v.fence().lastKnownDO());
        }

        // 3. merge ledger boundaries above p from all reachable replicas
        TreeMap<Long, Messages.LedgerEntry> boundaries = new TreeMap<>();
        for (ReplicaView v : reachable) {
            try {
                ByteBuffer h = pool.get(v.replica().endpoint()).call(Opcode.READ_LEDGER,
                        new Messages.ReadLedger(chunkId, p).encode(), null, config.callTimeoutMs());
                for (Messages.LedgerEntry e : Messages.ReadLedgerResp.decode(h).entries()) {
                    boundaries.putIfAbsent(e.endOffset(), e);
                }
            } catch (ScpException e) {
                log.warn("read ledger {} on {} failed: {}", chunkId, v.replica().endpoint(), e.getMessage());
            }
        }

        // 4. walk forward: re-replicate every batch found CRC-valid anywhere
        for (var entry : boundaries.entrySet()) {
            long end = entry.getKey();
            if (end <= p) continue;
            byte[] batch = null;
            for (ReplicaView v : reachable) {
                if (v.fence().localEndOffset() < end) continue;
                try {
                    Frame frame = pool.get(v.replica().endpoint()).callFrame(Opcode.READ,
                            new Messages.Read(chunkId, p, (int) (end - p)).encode(), null,
                            config.callTimeoutMs());
                    ByteBuffer h = frame.headerSlice();
                    Resp.check(h);
                    byte[] data = new byte[frame.payloadLength()];
                    frame.payloadSlice().get(data);
                    if (data.length == end - p && Crc.of(data) == entry.getValue().payloadCrc()) {
                        batch = data;
                        break;
                    }
                } catch (ScpException e) {
                    log.warn("recovery read {}@{} from {} failed: {}", chunkId, p,
                            v.replica().endpoint(), e.getMessage());
                }
            }
            if (batch == null) {
                break; // gap: nothing reachable holds these bytes — they were never quorum-acked
            }
            // re-replicate to replicas whose end is below this boundary
            for (ReplicaView v : reachable) {
                if (v.fence().localEndOffset() >= end) continue;
                try {
                    pool.get(v.replica().endpoint()).call(Opcode.APPEND,
                            new Messages.Append(chunkId, newEpoch, p, p).encode(),
                            ByteBuffer.wrap(batch), config.callTimeoutMs());
                    // track its new end locally
                    reachable.set(reachable.indexOf(v), new ReplicaView(v.replica(),
                            new Messages.FenceResp(v.fence().persistedFenceEpoch(), end,
                                    v.fence().lastKnownDO(), v.fence().state())));
                } catch (ScpException e) {
                    log.warn("recovery re-replicate {}@{} to {} failed: {}", chunkId, p,
                            v.replica().endpoint(), e.getMessage());
                }
            }
            p = end;
        }

        log.info("seal-recovery: chunk {} sealing at {}", chunkId, p);
        return finishSeal(chunkId, newEpoch, p, reachableReplicas);
    }

    private long finishSeal(ChunkId chunkId, int epoch,
                            long dataLength, List<Messages.Replica> replicas) {
        // seal every reachable replica; require quorum AND agreement — committing metadata over
        // replicas that sealed the same length with different bytes would violate invariant
        // §14.6 (the appender's seal path performs the same check)
        int ok = 0;
        long finalLength = -1;
        int crc = 0;
        ScpException last = null;
        for (Messages.Replica r : replicas) {
            Messages.SealResp resp;
            try {
                ByteBuffer h = pool.get(r.endpoint()).call(Opcode.SEAL_CHUNK,
                        new Messages.SealChunk(chunkId, epoch, dataLength).encode(), null,
                        config.callTimeoutMs());
                resp = Messages.SealResp.decode(h);
            } catch (ScpException e) {
                last = e;
                log.warn("recovery seal {} on {} failed: {}", chunkId, r.endpoint(), e.getMessage());
                continue;
            }
            if (ok > 0 && (resp.finalLength() != finalLength || resp.chunkCrc() != crc)) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "replica seal divergence on " + chunkId + " during recovery: " + r.endpoint()
                                + " returned len=" + resp.finalLength() + " crc=" + resp.chunkCrc()
                                + " vs len=" + finalLength + " crc=" + crc);
            }
            finalLength = resp.finalLength();
            crc = resp.chunkCrc();
            ok++;
        }
        if (ok < 2) {
            throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "recovery seal quorum lost");
        }
        meta.sealChunkMeta(chunkId, epoch, finalLength, crc);
        return finalLength;
    }
}
