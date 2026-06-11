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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    private static final byte FILE_OPEN = 0;
    private static final byte FILE_SEALED = 1;
    private static final byte FILE_DELETING = 2;

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

    StrataFile.SealInfo recoverAndSeal(io.strata.common.FileId fileId, int newEpoch) {
        Messages.LookupFileResp file = meta.lookupFile(fileId);
        if (file.fileState() == FILE_DELETING) {
            throw new ScpException(ErrorCode.PRECONDITION_FAILED, "file is DELETING");
        }
        if (file.fileState() != FILE_OPEN && file.fileState() != FILE_SEALED) {
            throw new ScpException(ErrorCode.INTERNAL, "unknown file state " + file.fileState());
        }
        long total = 0;
        for (Messages.ChunkInfo chunk : file.chunks()) {
            if (chunk.state() == ChunkState.SEALED) {
                total = addFileLength(total, chunk.length());
            } else {
                total = addFileLength(total, recoverChunk(chunk, newEpoch));
            }
        }
        meta.sealFile(fileId, total);
        return new StrataFile.SealInfo(total);
    }

    private static long addFileLength(long total, long delta) {
        if (delta < 0) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "negative chunk length " + delta);
        }
        try {
            return Math.addExact(total, delta);
        } catch (ArithmeticException e) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "file length overflow");
        }
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
                Messages.FenceResp fence = Messages.FenceResp.decode(h);
                validateFenceResp(chunkId, r, fence);
                reachable.add(new ReplicaState(r, fence));
            } catch (ScpException e) {
                if (e.code() == ErrorCode.FENCED_EPOCH) {
                    throw e;
                }
                log.warn("fence {} on {} failed: {}", chunkId, r.endpoint(), e.getMessage());
            } catch (RuntimeException e) {
                log.warn("fence {} on {} returned malformed response: {}", chunkId, r.endpoint(), e.toString());
            }
        }
        requireQuorum(chunkId, reachable);

        // The highest piggybacked DO is the recovery floor: bytes below it were quorum-durable.
        // A sealed replica shorter than this floor is not an authoritative mid-seal remnant; it
        // is stale/corrupt and must be evicted, otherwise it can truncate durable bytes.
        long p = 0;
        for (ReplicaState rs : reachable) {
            p = Math.max(p, rs.durable);
        }
        Iterator<ReplicaState> reachableIt = reachable.iterator();
        while (reachableIt.hasNext()) {
            ReplicaState rs = reachableIt.next();
            if (rs.state == ChunkState.SEALED && rs.end < p) {
                log.warn("sealed replica {} on {} is shorter than durable floor {} — evicting",
                        chunkId, rs.replica.endpoint(), p);
                reachableIt.remove();
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

        // 2. start from the durable floor and catch lagging replicas up to it before walking
        //    boundaries above it
        catchUp(chunkId, newEpoch, reachable, p);

        // 3. merge ledger boundaries above p from all reachable replicas
        TreeMap<Long, List<LedgerCandidate>> boundaries = new TreeMap<>();
        for (ReplicaState rs : reachable) {
            try {
                ByteBuffer h = pool.get(rs.replica.endpoint()).call(Opcode.READ_LEDGER,
                        new Messages.ReadLedger(chunkId, p).encode(), null, config.callTimeoutMs());
                long previousEnd = p;
                for (Messages.LedgerEntry e : Messages.ReadLedgerResp.decode(h).entries()) {
                    boundaries.computeIfAbsent(e.endOffset(), ignored -> new ArrayList<>())
                            .add(new LedgerCandidate(previousEnd, e));
                    previousEnd = e.endOffset();
                }
            } catch (ScpException e) {
                log.warn("read ledger {} on {} failed: {}", chunkId, rs.replica.endpoint(), e.getMessage());
            } catch (RuntimeException e) {
                log.warn("read ledger {} on {} returned malformed response: {}",
                        chunkId, rs.replica.endpoint(), e.toString());
            }
        }

        // 4. walk forward: re-replicate every batch found CRC-valid anywhere. At each point, prefer
        // the farthest valid continuation; a shorter valid boundary from one replica must not block
        // a larger intact append held by another replica.
        while (true) {
            Candidate candidate = bestContinuation(chunkId, reachable, boundaries, p);
            if (candidate == null) {
                break; // gap: nothing reachable holds these bytes — they were never quorum-acked
            }
            long end = candidate.end();
            byte[] batch = candidate.bytes();
            // re-replicate to replicas behind this boundary; evict any that cannot take it
            Iterator<ReplicaState> it = reachable.iterator();
            while (it.hasNext()) {
                ReplicaState rs = it.next();
                if (rs.end >= end) continue;
                try {
                    int suffixOffset = (int) (rs.end - p);
                    int suffixLength = (int) (end - rs.end);
                    appendAndVerify(chunkId, newEpoch, rs, p,
                            ByteBuffer.wrap(batch, suffixOffset, suffixLength), end);
                } catch (ScpException e) {
                    if (e.code() == ErrorCode.FENCED_EPOCH) {
                        throw e;
                    }
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

    private record Candidate(long end, byte[] bytes) {}

    private record LedgerCandidate(long previousEnd, Messages.LedgerEntry entry) {}

    private record SealKey(long finalLength, int crc) {}

    private Candidate bestContinuation(ChunkId chunkId, List<ReplicaState> reachable,
                                       TreeMap<Long, List<LedgerCandidate>> boundaries, long p) {
        Candidate best = null;
        for (var entry : boundaries.tailMap(p, false).entrySet()) {
            long end = entry.getKey();
            for (LedgerCandidate ledger : entry.getValue()) {
                if (ledger.previousEnd() != p) continue;
                for (ReplicaState rs : reachable) {
                    if (rs.end < end) continue;
                    byte[] data = readRange(chunkId, rs, p, end);
                    if (data != null && Crc.of(data) == ledger.entry().payloadCrc()) {
                        best = new Candidate(end, data);
                        break;
                    }
                }
                if (best != null && best.end() == end) break;
            }
        }
        return best;
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
                    long expectedEnd = addFileLength(rs.end, data.length);
                    appendAndVerify(chunkId, newEpoch, rs, rs.end, ByteBuffer.wrap(data), expectedEnd);
                }
                log.info("recovery caught up {} on {} to {}", chunkId, rs.replica.endpoint(), target);
            } catch (ScpException e) {
                if (e.code() == ErrorCode.FENCED_EPOCH) {
                    throw e;
                }
                log.warn("catch-up of {} on {} failed: {} — evicting replica",
                        chunkId, rs.replica.endpoint(), e.getMessage());
                it.remove();
            }
        }
        requireQuorum(chunkId, reachable);
    }

    /** Reads [from, to) from one replica; null on failure or short read. */
    private byte[] readRange(ChunkId chunkId, ReplicaState source, long from, long to) {
        long len = to - from;
        if (from < 0 || to < 0 || len <= 0 || len > Integer.MAX_VALUE) {
            log.warn("recovery read {} invalid range [{}..{}) from {}", chunkId, from, to,
                    source.replica.endpoint());
            return null;
        }
        try {
            Frame frame = pool.get(source.replica.endpoint()).callFrame(Opcode.READ,
                    new Messages.Read(chunkId, from, (int) len).encode(), null,
                    config.callTimeoutMs());
            ByteBuffer h = frame.headerSlice();
            Resp.check(h);
            Messages.ReadResp resp = Messages.ReadResp.decode(h);
            if (resp.localEndOffset() < to || resp.durableOffset() < 0
                    || resp.durableOffset() > resp.localEndOffset() || frame.payloadLength() != (int) len) {
                return null;
            }
            byte[] data = new byte[frame.payloadLength()];
            frame.payloadSlice().get(data);
            return data;
        } catch (ScpException e) {
            log.warn("recovery read {}@{} from {} failed: {}", chunkId, from,
                    source.replica.endpoint(), e.getMessage());
            return null;
        } catch (RuntimeException e) {
            log.warn("recovery read {}@{} from {} returned malformed response: {}", chunkId, from,
                    source.replica.endpoint(), e.toString());
            return null;
        }
    }

    private static void validateFenceResp(ChunkId chunkId, Messages.Replica replica, Messages.FenceResp fence) {
        if (fence.localEndOffset() < 0 || fence.lastKnownDO() < 0
                || fence.lastKnownDO() > fence.localEndOffset()) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                    "bad fence offsets from replica " + replica.nodeId() + " for " + chunkId);
        }
    }

    private void appendAndVerify(ChunkId chunkId, int epoch, ReplicaState target, long durableOffset,
                                 ByteBuffer payload, long expectedEnd) {
        ByteBuffer h = pool.get(target.replica.endpoint()).call(Opcode.APPEND,
                new Messages.Append(chunkId, epoch, target.end, durableOffset).encode(),
                payload, config.callTimeoutMs());
        long actualEnd;
        try {
            actualEnd = Messages.AppendResp.decode(h).endOffset();
        } catch (RuntimeException e) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                    "malformed recovery append response from replica " + target.replica.nodeId() + ": " + e);
        }
        if (actualEnd != expectedEnd) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                    "recovery append to replica " + target.replica.nodeId() + " ended at "
                            + actualEnd + " != " + expectedEnd);
        }
        target.end = actualEnd;
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
        ScpException last = null;
        Map<SealKey, List<Integer>> votes = new LinkedHashMap<>();
        for (ReplicaState rs : replicas) {
            Messages.SealResp resp;
            try {
                ByteBuffer h = pool.get(rs.replica.endpoint()).call(Opcode.SEAL_CHUNK,
                        new Messages.SealChunk(chunkId, epoch, dataLength).encode(), null,
                        config.callTimeoutMs());
                resp = Messages.SealResp.decode(h);
            } catch (ScpException e) {
                if (e.code() == ErrorCode.FENCED_EPOCH) {
                    throw e;
                }
                last = e;
                log.warn("recovery seal {} on {} failed: {}", chunkId, rs.replica.endpoint(), e.getMessage());
                continue;
            } catch (RuntimeException e) {
                last = new ScpException(ErrorCode.INTERNAL, "malformed recovery seal response: " + e);
                log.warn("recovery seal {} on {} returned malformed response: {}",
                        chunkId, rs.replica.endpoint(), e.toString());
                continue;
            }
            if (resp.finalLength() != dataLength) {
                last = new ScpException(ErrorCode.CORRUPT_CHUNK,
                        "recovery replica sealed at " + resp.finalLength() + " != requested " + dataLength);
                log.warn("recovery seal {} on {} returned bad final length: {}",
                        chunkId, rs.replica.endpoint(), last.getMessage());
                continue;
            }
            SealKey key = new SealKey(resp.finalLength(), resp.chunkCrc());
            votes.computeIfAbsent(key, ignored -> new ArrayList<>()).add(rs.replica.nodeId());
        }
        int ok = votes.values().stream().mapToInt(List::size).sum();
        if (ok < 2) {
            throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "recovery seal quorum lost");
        }
        Map.Entry<SealKey, List<Integer>> quorum = bestSealQuorum(votes);
        if (quorum == null) {
            throw new ScpException(ErrorCode.INTERNAL,
                    "replica seal divergence on " + chunkId + " during recovery");
        }
        if (votes.size() > 1) {
            log.warn("recovery seal divergence on {} — committing agreeing quorum {} of {} successful seals",
                    chunkId, quorum.getValue().size(), ok);
        }
        meta.sealChunkMeta(chunkId, epoch, quorum.getKey().finalLength(), quorum.getKey().crc(),
                List.copyOf(quorum.getValue()));
        return quorum.getKey().finalLength();
    }

    private static Map.Entry<SealKey, List<Integer>> bestSealQuorum(Map<SealKey, List<Integer>> votes) {
        Map.Entry<SealKey, List<Integer>> best = null;
        for (Map.Entry<SealKey, List<Integer>> entry : votes.entrySet()) {
            if (entry.getValue().size() < 2) continue;
            if (best == null || entry.getValue().size() > best.getValue().size()) {
                best = entry;
            }
        }
        return best;
    }
}
