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
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * Seal recovery (tech design §7.3): fence reachable replicas at the new epoch, start from the
 * highest piggybacked DO, CATCH UP any replica that is behind that point (a replica's piggybacked
 * DO is clamped to its own end, so p can exceed a lagging replica's data), then walk the
 * integrity-ledger boundaries forward, preserving any batch found CRC-valid on any reachable
 * replica by re-replicating it. A replica that cannot be brought to the seal point is EVICTED
 * from the recovery set — sealing it would either fail or leave a short copy in the descriptor.
 *
 * Tolerance: requires >=2 replicas at the seal point (same fault model as min.insync.replicas=2).
 * Bytes beyond the seal point were never producer-acked; discarding them is correct.
 */
final class Recovery {
    private static final Logger log = LoggerFactory.getLogger(Recovery.class);
    private static final int COPY_CHUNK_BYTES = 4 * 1024 * 1024;

    private final MetaClient meta;
    private final NodePool pool;
    private final ClientConfig config;

    Recovery(MetaClient meta, NodePool pool, ClientConfig config) {
        this.meta = meta;
        this.pool = pool;
        this.config = config;
    }

    /** Mutable per-replica recovery state; `end` tracks our view of its local end offset. */
    private static final class ReplicaState {
        final Messages.Replica replica;
        long end;
        long durable;
        ChunkState state;

        ReplicaState(Messages.Replica replica, Messages.FenceResp fence) {
            this.replica = replica;
            this.end = fence.localEndOffset();
            this.durable = fence.lastKnownDO();
            this.state = fence.state();
        }
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
        List<ReplicaState> reachable = new ArrayList<>();
        for (Messages.Replica r : chunk.replicas()) {
            if (r.endpoint().isEmpty()) continue;
            try {
                ByteBuffer h = pool.get(r.endpoint()).call(Opcode.FENCE,
                        new Messages.Fence(chunkId, newEpoch).encode(), null, config.callTimeoutMs());
                reachable.add(new ReplicaState(r, Messages.FenceResp.decode(h)));
            } catch (ScpException e) {
                log.warn("fence {} on {} failed: {}", chunkId, r.endpoint(), e.getMessage());
            }
        }
        requireQuorum(chunkId, reachable);

        // a replica may already hold a sealed copy (writer died mid-seal): its length is the
        // authoritative seal point — bring the others up to it and seal them too
        for (ReplicaState rs : reachable) {
            if (rs.state == ChunkState.SEALED) {
                long len = rs.end;
                log.info("chunk {} found sealed at {} on {}", chunkId, len, rs.replica.endpoint());
                catchUp(chunkId, newEpoch, reachable, len);
                return finishSeal(chunkId, newEpoch, len, reachable);
            }
        }

        // 2. start from the highest piggybacked DO — everything below is known quorum-durable —
        //    and catch lagging replicas up to it before walking boundaries
        long p = 0;
        for (ReplicaState rs : reachable) {
            p = Math.max(p, rs.durable);
        }
        catchUp(chunkId, newEpoch, reachable, p);

        // 3. merge ledger boundaries above p from all reachable replicas
        TreeMap<Long, Messages.LedgerEntry> boundaries = new TreeMap<>();
        for (ReplicaState rs : reachable) {
            try {
                ByteBuffer h = pool.get(rs.replica.endpoint()).call(Opcode.READ_LEDGER,
                        new Messages.ReadLedger(chunkId, p).encode(), null, config.callTimeoutMs());
                for (Messages.LedgerEntry e : Messages.ReadLedgerResp.decode(h).entries()) {
                    boundaries.putIfAbsent(e.endOffset(), e);
                }
            } catch (ScpException e) {
                log.warn("read ledger {} on {} failed: {}", chunkId, rs.replica.endpoint(), e.getMessage());
            }
        }

        // 4. walk forward: re-replicate every batch found CRC-valid anywhere
        for (var entry : boundaries.entrySet()) {
            long end = entry.getKey();
            if (end <= p) continue;
            byte[] batch = null;
            for (ReplicaState rs : reachable) {
                if (rs.end < end) continue;
                byte[] data = readRange(chunkId, rs, p, end);
                if (data != null && Crc.of(data) == entry.getValue().payloadCrc()) {
                    batch = data;
                    break;
                }
            }
            if (batch == null) {
                break; // gap: nothing reachable holds these bytes — they were never quorum-acked
            }
            // re-replicate to replicas behind this boundary; evict any that cannot take it
            Iterator<ReplicaState> it = reachable.iterator();
            while (it.hasNext()) {
                ReplicaState rs = it.next();
                if (rs.end >= end) continue;
                try {
                    pool.get(rs.replica.endpoint()).call(Opcode.APPEND,
                            new Messages.Append(chunkId, newEpoch, rs.end, p).encode(),
                            ByteBuffer.wrap(batch), config.callTimeoutMs());
                    rs.end = end;
                } catch (ScpException e) {
                    log.warn("recovery re-replicate {}@{} to {} failed: {} — evicting replica",
                            chunkId, rs.end, rs.replica.endpoint(), e.getMessage());
                    it.remove();
                }
            }
            requireQuorum(chunkId, reachable);
            p = end;
        }

        log.info("seal-recovery: chunk {} sealing at {}", chunkId, p);
        return finishSeal(chunkId, newEpoch, p, reachable);
    }

    /**
     * Brings every reachable replica's end up to {@code target} by copying from a replica that
     * already holds the bytes. A replica that cannot be caught up is evicted — it must not be
     * sealed short. The donor region is below the durable offset (or a sealed copy), so the
     * bytes are quorum-trusted; wire integrity is covered by frame payload CRCs.
     */
    private void catchUp(ChunkId chunkId, int newEpoch, List<ReplicaState> reachable, long target) {
        Iterator<ReplicaState> it = reachable.iterator();
        while (it.hasNext()) {
            ReplicaState rs = it.next();
            if (rs.state == ChunkState.SEALED || rs.end >= target) continue;
            try {
                while (rs.end < target) {
                    int want = (int) Math.min(COPY_CHUNK_BYTES, target - rs.end);
                    byte[] data = null;
                    for (ReplicaState donor : reachable) {
                        if (donor == rs || donor.end < target) continue;
                        data = readRange(chunkId, donor, rs.end, rs.end + want);
                        if (data != null) break;
                    }
                    if (data == null) {
                        throw new ScpException(ErrorCode.INTERNAL, "no donor for catch-up");
                    }
                    pool.get(rs.replica.endpoint()).call(Opcode.APPEND,
                            new Messages.Append(chunkId, newEpoch, rs.end, rs.end).encode(),
                            ByteBuffer.wrap(data), config.callTimeoutMs());
                    rs.end += data.length;
                }
                log.info("recovery caught up {} on {} to {}", chunkId, rs.replica.endpoint(), target);
            } catch (ScpException e) {
                log.warn("catch-up of {} on {} failed: {} — evicting replica",
                        chunkId, rs.replica.endpoint(), e.getMessage());
                it.remove();
            }
        }
        requireQuorum(chunkId, reachable);
    }

    /** Reads [from, to) from one replica; null on failure or short read. */
    private byte[] readRange(ChunkId chunkId, ReplicaState source, long from, long to) {
        try {
            Frame frame = pool.get(source.replica.endpoint()).callFrame(Opcode.READ,
                    new Messages.Read(chunkId, from, (int) (to - from)).encode(), null,
                    config.callTimeoutMs());
            ByteBuffer h = frame.headerSlice();
            Resp.check(h);
            byte[] data = new byte[frame.payloadLength()];
            frame.payloadSlice().get(data);
            return data.length == to - from ? data : null;
        } catch (ScpException e) {
            log.warn("recovery read {}@{} from {} failed: {}", chunkId, from,
                    source.replica.endpoint(), e.getMessage());
            return null;
        }
    }

    private static void requireQuorum(ChunkId chunkId, List<ReplicaState> reachable) {
        if (reachable.size() < 2) {
            throw new ScpException(ErrorCode.INTERNAL,
                    "chunk " + chunkId + " unavailable: " + reachable.size()
                            + " usable replicas (need 2)");
        }
    }

    private long finishSeal(ChunkId chunkId, int epoch, long dataLength, List<ReplicaState> replicas) {
        // seal every usable replica; require quorum AND agreement — committing metadata over
        // replicas that sealed the same length with different bytes would violate invariant
        // §14.6 (the appender's seal path performs the same check)
        int ok = 0;
        long finalLength = -1;
        int crc = 0;
        ScpException last = null;
        List<Integer> sealedReplicas = new ArrayList<>(replicas.size());
        for (ReplicaState rs : replicas) {
            Messages.SealResp resp;
            try {
                ByteBuffer h = pool.get(rs.replica.endpoint()).call(Opcode.SEAL_CHUNK,
                        new Messages.SealChunk(chunkId, epoch, dataLength).encode(), null,
                        config.callTimeoutMs());
                resp = Messages.SealResp.decode(h);
            } catch (ScpException e) {
                last = e;
                log.warn("recovery seal {} on {} failed: {}", chunkId, rs.replica.endpoint(), e.getMessage());
                continue;
            }
            if (ok > 0 && (resp.finalLength() != finalLength || resp.chunkCrc() != crc)) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "replica seal divergence on " + chunkId + " during recovery: " + rs.replica.endpoint()
                                + " returned len=" + resp.finalLength() + " crc=" + resp.chunkCrc()
                                + " vs len=" + finalLength + " crc=" + crc);
            }
            finalLength = resp.finalLength();
            crc = resp.chunkCrc();
            sealedReplicas.add(rs.replica.nodeId());
            ok++;
        }
        if (ok < 2) {
            throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "recovery seal quorum lost");
        }
        meta.sealChunkMeta(chunkId, epoch, finalLength, crc, sealedReplicas);
        return finalLength;
    }
}
