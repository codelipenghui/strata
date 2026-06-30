package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FileState;
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
import java.util.Map;
import java.util.TreeMap;

import static io.strata.common.Checks.addChunkLength;

/**
 * Seal recovery (tech design §7.3): fence reachable replicas at the new epoch, start from the
 * highest piggybacked DO, CATCH UP any replica that is behind that point (a replica's piggybacked
 * DO is clamped to its own end, so p can exceed a lagging replica's data), then walk only
 * quorum-recoverable integrity-ledger boundaries forward. A replica that cannot be brought to the
 * seal point is EVICTED from the recovery set — sealing it would either fail or leave a short copy
 * in the descriptor.
 *
 * Tolerance: requires the file's configured ack quorum at the seal point. Bytes beyond the seal
 * point were never producer-acked; discarding them is correct.
 */
final class Recovery {
    private static final Logger log = LoggerFactory.getLogger(Recovery.class);
    private final ControllerClient controller;
    private final NodePool appendPool;
    private final NodePool readPool;
    private final ClientConfig config;
    private final io.strata.common.StrataNamespace namespace;

    Recovery(ControllerClient controller, NodePool pool, ClientConfig config,
             io.strata.common.StrataNamespace namespace) {
        this(controller, pool, pool, config, namespace);
    }

    Recovery(ControllerClient controller, NodePool appendPool, NodePool readPool, ClientConfig config,
             io.strata.common.StrataNamespace namespace) {
        this.controller = controller;
        this.appendPool = appendPool;
        this.readPool = readPool;
        this.config = config;
        this.namespace = namespace;
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

    StrataFile.SealInfo recoverAndSeal(io.strata.common.FileId fileId) {
        Messages.LookupFileResp file = controller.lookupFile(namespace, fileId);
        boolean needsRecovery = file.chunks().stream().anyMatch(c -> c.state() != ChunkState.SEALED);
        int writerEpoch = 0;
        if (needsRecovery) {
            writerEpoch = controller.allocateWriterEpochForRecovery(namespace, fileId);
            file = controller.lookupFile(namespace, fileId);
        }
        return recoverAndSeal(fileId, file, writerEpoch);
    }

    StrataFile.SealInfo recoverAndSeal(io.strata.common.FileId fileId, int writerEpoch) {
        Messages.LookupFileResp file = controller.lookupFile(namespace, fileId);
        return recoverAndSeal(fileId, file, writerEpoch);
    }

    private StrataFile.SealInfo recoverAndSeal(io.strata.common.FileId fileId,
                                               Messages.LookupFileResp file,
                                               int writerEpoch) {
        if (file.fileState() == FileState.DELETING.value) {
            throw new ScpException(ErrorCode.PRECONDITION_FAILED, "file is DELETING");
        }
        if (file.fileState() != FileState.OPEN.value && file.fileState() != FileState.SEALED.value) {
            throw new ScpException(ErrorCode.INTERNAL, "unknown file state " + file.fileState());
        }
        long total = 0;
        List<Messages.ChunkInfo> chunks = file.chunks();
        for (int i = 0; i < chunks.size(); i++) {
            Messages.ChunkInfo chunk = chunks.get(i);
            if (chunk.state() == ChunkState.SEALED) {
                total = addChunkLength(total, chunk.length());
            } else {
                if (writerEpoch <= 0) {
                    throw new ScpException(ErrorCode.INTERNAL, "missing writer epoch for open chunk recovery");
                }
                boolean maySealAbandonedEmptyTail = i > 0 && i == chunks.size() - 1;
                total = addChunkLength(total, recoverChunk(chunk, writerEpoch, file.writePolicy().ackQuorum(),
                        maySealAbandonedEmptyTail));
            }
        }
        controller.sealFile(namespace, fileId, total);
        return new StrataFile.SealInfo(total);
    }

    private long recoverChunk(Messages.ChunkInfo chunk, int writerEpoch, int ackQuorum,
                              boolean maySealAbandonedEmptyTail) {
        ChunkId chunkId = chunk.chunkId();

        // 1. fence all reachable replicas; collect their state
        List<ReplicaState> reachable = new ArrayList<>();
        for (Messages.Replica r : chunk.replicas()) {
            if (r.endpoint().isEmpty()) continue;
            try {
                ByteBuffer h = appendPool.get(r.endpoint()).call(Opcode.FENCE,
                        new Messages.Fence(chunkId, writerEpoch, namespace).encode(), null, config.callTimeoutMs());
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
        if (maySealAbandonedEmptyTail && reachable.size() < ackQuorum) {
            log.warn("seal-recovery: final tail {} has only {} reachable replica(s), below quorum {}; "
                            + "refusing to infer an empty tail from open-chunk metadata",
                    chunkId, reachable.size(), ackQuorum);
        }
        requireQuorum(chunkId, reachable, ackQuorum);

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
        requireQuorum(chunkId, reachable, ackQuorum);

        // a replica may already hold a sealed copy (writer died mid-seal): its length is the
        // authoritative seal point — bring the others up to it and seal them too
        for (ReplicaState rs : reachable) {
            if (rs.state == ChunkState.SEALED) {
                long len = rs.end;
                log.info("chunk {} found sealed at {} on {}", chunkId, len, rs.replica.endpoint());
                catchUp(chunkId, writerEpoch, reachable, len, ackQuorum);
                return finishSeal(chunkId, writerEpoch, len, reachable, ackQuorum);
            }
        }

        // 2. start from the durable floor and catch lagging replicas up to it before walking
        //    boundaries above it
        catchUp(chunkId, writerEpoch, reachable, p, ackQuorum);

        // 3. merge ledger boundaries above p from all reachable replicas
        TreeMap<Long, List<LedgerCandidate>> boundaries = new TreeMap<>();
        for (ReplicaState rs : reachable) {
            try {
                ByteBuffer h = readPool.get(rs.replica.endpoint()).call(Opcode.READ_LEDGER,
                        new Messages.ReadLedger(chunkId, p, namespace).encode(), null, config.callTimeoutMs());
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

        // 4. walk forward: re-replicate every batch that an agreeing quorum can still prove.
        // At each point, prefer the farthest valid continuation; a shorter valid boundary from
        // one replica must not block a larger intact append held by a quorum of replicas.
        while (true) {
            Candidate candidate = bestContinuation(chunkId, reachable, boundaries, p, ackQuorum);
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
                    appendAndVerify(chunkId, writerEpoch, rs, p,
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
            requireQuorum(chunkId, reachable, ackQuorum);
            p = end;
        }

        log.info("seal-recovery: chunk {} sealing at {}", chunkId, p);
        return finishSeal(chunkId, writerEpoch, p, reachable, ackQuorum);
    }

    private record Candidate(long end, byte[] bytes) {}

    private record LedgerCandidate(long previousEnd, Messages.LedgerEntry entry) {}

    private static final class CandidateCount {
        final byte[] bytes;
        int count;

        CandidateCount(byte[] bytes) {
            this.bytes = bytes;
            this.count = 1;
        }
    }

    private Candidate bestContinuation(ChunkId chunkId, List<ReplicaState> reachable,
                                       TreeMap<Long, List<LedgerCandidate>> boundaries, long p,
                                       int ackQuorum) {
        Candidate best = null;
        for (var entry : boundaries.tailMap(p, false).entrySet()) {
            long end = entry.getKey();
            for (LedgerCandidate ledger : entry.getValue()) {
                if (ledger.previousEnd() != p) continue;
                Candidate candidate = quorumCandidate(chunkId, reachable, p, end,
                        ledger.entry().payloadCrc(), ackQuorum);
                if (candidate != null) {
                    best = candidate;
                    break;
                }
            }
        }
        return best;
    }

    private Candidate quorumCandidate(ChunkId chunkId, List<ReplicaState> reachable, long from, long to,
                                      int payloadCrc, int ackQuorum) {
        List<CandidateCount> counts = new ArrayList<>();
        for (ReplicaState rs : reachable) {
            if (rs.end < to) continue;
            byte[] data = readRange(chunkId, rs, from, to);
            if (data == null || Crc.of(data) != payloadCrc) {
                continue;
            }
            for (CandidateCount candidate : counts) {
                if (java.util.Arrays.equals(candidate.bytes, data)) {
                    candidate.count++;
                    if (candidate.count >= ackQuorum) {
                        return new Candidate(to, candidate.bytes);
                    }
                    data = null;
                    break;
                }
            }
            if (data != null) {
                if (ackQuorum <= 1) {
                    return new Candidate(to, data);
                }
                counts.add(new CandidateCount(data));
            }
        }
        return null;
    }

    /**
     * Brings every reachable replica's end up to {@code target} by copying from a replica that
     * already holds the bytes. A replica that cannot be caught up is evicted — it must not be
     * sealed short. If multiple possible donors already disagree on a catch-up range, recovery
     * must not copy one of them into a lagging replica and manufacture a seal quorum.
     */
    private void catchUp(ChunkId chunkId, int writerEpoch, List<ReplicaState> reachable, long target,
                         int ackQuorum) {
        Iterator<ReplicaState> it = reachable.iterator();
        while (it.hasNext()) {
            ReplicaState rs = it.next();
            if (rs.state == ChunkState.SEALED || rs.end >= target) continue;
            try {
                while (rs.end < target) {
                    int want = (int) Math.min(config.recoveryCopyChunkBytes(), target - rs.end);
                    byte[] data = catchUpBytes(chunkId, reachable, rs, rs.end, rs.end + want, ackQuorum);
                    if (data == null) {
                        throw new ScpException(ErrorCode.INTERNAL, "no donor for catch-up");
                    }
                    long expectedEnd = addChunkLength(rs.end, data.length);
                    appendAndVerify(chunkId, writerEpoch, rs, rs.end, ByteBuffer.wrap(data), expectedEnd);
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
        requireQuorum(chunkId, reachable, ackQuorum);
    }

    private byte[] catchUpBytes(ChunkId chunkId, List<ReplicaState> reachable, ReplicaState target,
                                long from, long to, int ackQuorum) {
        List<CandidateCount> counts = new ArrayList<>();
        for (ReplicaState donor : reachable) {
            if (donor == target || donor.end < to) continue;
            byte[] data = readRange(chunkId, donor, from, to);
            if (data == null) {
                continue;
            }
            for (CandidateCount candidate : counts) {
                if (java.util.Arrays.equals(candidate.bytes, data)) {
                    candidate.count++;
                    if (candidate.count >= ackQuorum) {
                        return candidate.bytes;
                    }
                    data = null;
                    break;
                }
            }
            if (data != null) {
                counts.add(new CandidateCount(data));
            }
        }
        if (counts.size() > 1) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                    "divergent catch-up donors for " + chunkId + " range [" + from + ".." + to + ")");
        }
        return counts.isEmpty() ? null : counts.get(0).bytes;
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
            // READ_RECOVERY (not client READ): recovery must see the never-acked tail above the
            // donor's durable high watermark — that is exactly the range it is re-proving for seal.
            Frame frame = readPool.get(source.replica.endpoint()).callFrame(Opcode.READ_RECOVERY,
                    new Messages.Read(chunkId, from, (int) len, namespace).encode(), null,
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
        ByteBuffer h = appendPool.get(target.replica.endpoint()).call(Opcode.APPEND,
                new Messages.Append(chunkId, epoch, target.end, durableOffset, namespace).encode(),
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

    private static void requireQuorum(ChunkId chunkId, List<ReplicaState> reachable, int ackQuorum) {
        if (reachable.size() < ackQuorum) {
            throw new ScpException(ErrorCode.INTERNAL,
                    "chunk " + chunkId + " unavailable: " + reachable.size()
                            + " usable replicas (need " + ackQuorum + ")");
        }
    }

    private long finishSeal(ChunkId chunkId, int epoch, long dataLength, List<ReplicaState> replicas,
                            int ackQuorum) {
        // seal every usable replica; require quorum AND agreement — committing metadata over
        // replicas that sealed the same length with different bytes would violate invariant
        // §14.6 (the appender's seal path performs the same check)
        ScpException last = null;
        SealVotes votes = new SealVotes();
        for (ReplicaState rs : replicas) {
            Messages.SealResp resp;
            try {
                ByteBuffer h = appendPool.get(rs.replica.endpoint()).call(Opcode.SEAL_CHUNK,
                        new Messages.SealChunk(chunkId, epoch, dataLength, namespace).encode(), null,
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
            votes.add(resp.finalLength(), resp.chunkCrc(), rs.replica.nodeId());
        }
        int ok = votes.total();
        if (ok < ackQuorum) {
            throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "recovery seal quorum lost");
        }
        Map.Entry<SealVotes.Key, List<Integer>> quorum = votes.best(ackQuorum);
        if (quorum == null) {
            throw new ScpException(ErrorCode.INTERNAL,
                    "replica seal divergence on " + chunkId + " during recovery");
        }
        if (votes.divergent()) {
            log.warn("recovery seal divergence on {} — committing agreeing quorum {} of {} successful seals",
                    chunkId, quorum.getValue().size(), ok);
        }
        // recovery seals at a strictly higher (recovery-allocated) epoch than the chunk's create
        // epoch, so the write-epoch fence alone separates it from any stale same-epoch seal — it
        // does not (and cannot) pin the original create-op, hence the (0,0) no-pin sentinel.
        controller.sealChunkMeta(namespace, chunkId, epoch, quorum.getKey().finalLength(), quorum.getKey().crc(),
                List.copyOf(quorum.getValue()), 0L, 0L);
        return quorum.getKey().finalLength();
    }
}
