package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static io.strata.common.Checks.addChunkLength;

/**
 * Seal recovery (tech design §7.3): fence reachable replicas at the new epoch, start from the
 * highest piggybacked DO, CATCH UP any replica that is behind that point (a replica's piggybacked
 * DO is clamped to its own end, so p can exceed a lagging replica's data), then walk only
 * quorum-recoverable integrity-ledger boundaries forward. A replica that cannot be brought to the
 * seal point is EVICTED from the recovery set — sealing it would either fail or leave a short copy
 * in the descriptor.
 *
 * Tolerance: a batch above the durable floor is re-replicated to quorum when it could still have
 * been producer-acked — i.e. it is held by an ackQuorum once the replicas we could not fence are
 * counted as possible holders (§7.3 step 3 / issue #29). This preserves an acked batch whose other
 * holder is merely unreachable, even when RF &gt; ackQuorum. A batch that — with every replica
 * reachable — never reached ackQuorum (a never-acked dirty tail), and a divergent split, are
 * truncated at the floor.
 */
final class Recovery {
    private static final Logger log = LoggerFactory.getLogger(Recovery.class);
    private static final int RECOVERY_READ_MIN_ATTEMPTS = 2;
    private static final int RECOVERY_READ_MIN_PROGRESS_BYTES = 4 * 1024;
    private final ControllerClient controller;
    private final NodePool appendPool;
    private final NodePool readPool;
    private final ClientConfig config;
    private final StrataNamespace namespace;

    Recovery(ControllerClient controller, NodePool pool, ClientConfig config,
             StrataNamespace namespace) {
        this(controller, pool, pool, config, namespace);
    }

    Recovery(ControllerClient controller, NodePool appendPool, NodePool readPool, ClientConfig config,
             StrataNamespace namespace) {
        this.controller = controller;
        this.appendPool = appendPool;
        this.readPool = readPool;
        this.config = config;
        this.namespace = namespace;
    }

    /** Mutable per-replica recovery state; `end` tracks our view of its local end offset. */
    static final class ReplicaState {
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

    StrataFile.SealInfo recoverAndSeal(FileId fileId) {
        Messages.LookupFileResp file = controller.lookupFile(namespace, fileId);
        boolean needsRecovery = file.chunks().stream().anyMatch(c -> c.state() != ChunkState.SEALED);
        int writerEpoch = 0;
        if (needsRecovery) {
            writerEpoch = controller.allocateWriterEpochForRecovery(namespace, fileId);
            file = controller.lookupFile(namespace, fileId);
        }
        return recoverAndSeal(fileId, file, writerEpoch);
    }

    StrataFile.SealInfo recoverAndSeal(FileId fileId, int writerEpoch) {
        Messages.LookupFileResp file = controller.lookupFile(namespace, fileId);
        return recoverAndSeal(fileId, file, writerEpoch);
    }

    private StrataFile.SealInfo recoverAndSeal(FileId fileId,
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
        int placedReplicas = 0;
        List<ReplicaState> reachable = new ArrayList<>();
        for (Messages.Replica r : chunk.replicas()) {
            if (r.endpoint().isEmpty()) continue;
            placedReplicas++;
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

        // Replicas we could not fence may still hold bytes we cannot see. A batch above the floor
        // could therefore have reached ackQuorum (i.e. been producer-acked) even if fewer than
        // ackQuorum reachable replicas hold it (issue #29). Captured before any later eviction so
        // it counts only genuinely-unreachable replicas, not stale ones we inspected and dropped.
        final int unreachableReplicas = placedReplicas - reachable.size();

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
            Candidate candidate = bestContinuation(chunkId, reachable, boundaries, p, ackQuorum,
                    unreachableReplicas);
            if (candidate == null) {
                break; // no agreed continuation: a true gap, a torn/CRC-invalid tail, or a divergent split
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
                                       int ackQuorum, int unreachableReplicas) {
        // Farthest boundary first: a longer continuation that is still provable — a reachable quorum,
        // or (issue #29) a single CRC-valid copy that could still have been acked — should win over a
        // shorter one. A sub-quorum single is committed only if no reachable replica holds conflicting
        // bytes over the overlap; otherwise it is a divergent split with no quorum to resolve it, so we
        // fall back to a nearer boundary and ultimately truncate at the floor.
        for (var entry : boundaries.tailMap(p, false).descendingMap().entrySet()) {
            long end = entry.getKey();
            // The CRC(s) a batch [p, end) is allowed to have, per any reachable replica's ledger.
            Set<Integer> validCrcs = new HashSet<>();
            for (LedgerCandidate ledger : entry.getValue()) {
                if (ledger.previousEnd() == p) {
                    validCrcs.add(ledger.entry().payloadCrc());
                }
            }
            if (validCrcs.isEmpty()) continue;
            Agreed agreed = agreedContinuation(chunkId, reachable, p, end, validCrcs, ackQuorum,
                    unreachableReplicas);
            if (agreed == null) continue;
            Candidate candidate = new Candidate(end, agreed.bytes());
            if (agreed.quorum()) {
                return candidate; // a reachable quorum wins; the seal quorum drops any outlier (§14.6)
            }
            if (!conflictsAboveFloor(chunkId, reachable, p, candidate)) {
                return candidate;
            }
        }
        return null;
    }

    /**
     * True if any reachable replica holds bytes above the floor {@code p} that disagree with
     * {@code candidate} on their overlap. This gates the sub-quorum (single-holder) acceptance from
     * issue #29: with no agreeing quorum to drop an outlier at seal time, re-replicating a continuation
     * that conflicts with a replica's existing prefix would graft mismatched bytes and fail the seal
     * instead of truncating cleanly at the floor.
     */
    private boolean conflictsAboveFloor(ChunkId chunkId, List<ReplicaState> reachable, long p,
                                        Candidate candidate) {
        for (ReplicaState rs : reachable) {
            long overlapEnd = Math.min(rs.end, candidate.end());
            if (overlapEnd <= p) continue; // holds nothing above the floor within the candidate range
            byte[] held = readRange(chunkId, rs, p, overlapEnd);
            if (held == null) continue; // an unreadable replica is not positive evidence of divergence
            int len = (int) (overlapEnd - p);
            if (!Arrays.equals(held, 0, len, candidate.bytes(), 0, len)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Decides the batch {@code [from, to)} above the durable floor. A reachable replica contributes
     * its bytes only if they match one of the batch's CRC-valid ledger entries, so torn/corrupt tails
     * are ignored. A byte-value held by {@code >= ackQuorum} reachable replicas is returned as a
     * {@code quorum} result (it wins outright, dropping divergent outliers, §14.6). Otherwise a single
     * CRC-valid value (no competitor) is returned as a non-quorum result only if it could still have
     * been producer-acked — its holders plus the replicas we could not fence, or that claimed the
     * range at fence time but could not be byte-verified, reach {@code ackQuorum} (issues #29/#42);
     * the caller then verifies that single copy against the other readable replicas before committing
     * it. Multiple distinct CRC-valid values (a split with no quorum) yield {@code null}, so the seal
     * stops at the floor.
     */
    private Agreed agreedContinuation(ChunkId chunkId, List<ReplicaState> reachable, long from, long to,
                                      Set<Integer> validCrcs, int ackQuorum, int unreachableReplicas) {
        List<CandidateCount> counts = new ArrayList<>();
        int unverifiedHolders = 0;
        for (ReplicaState rs : reachable) {
            if (rs.end < to) continue;
            byte[] data = readRange(chunkId, rs, from, to);
            if (data == null) {
                // Null includes transient failures and short responses; both retain fence-time holder credit.
                unverifiedHolders++;
                continue;
            }
            if (!validCrcs.contains(Crc.of(data))) {
                continue;
            }
            boolean merged = false;
            for (CandidateCount candidate : counts) {
                if (Arrays.equals(candidate.bytes, data)) {
                    if (++candidate.count >= ackQuorum) {
                        return new Agreed(candidate.bytes, true);
                    }
                    merged = true;
                    break;
                }
            }
            if (!merged) {
                counts.add(new CandidateCount(data));
            }
        }
        if (counts.size() == 1
                && counts.get(0).count + unreachableReplicas + unverifiedHolders >= ackQuorum) {
            return new Agreed(counts.get(0).bytes, false);
        }
        return null;
    }

    private record Agreed(byte[] bytes, boolean quorum) {}

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
                if (Arrays.equals(candidate.bytes, data)) {
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

    /** Reads [from, to) from one replica; null on failure or malformed/zero-progress reads. */
    private byte[] readRange(ChunkId chunkId, ReplicaState source, long from, long to) {
        long len = to - from;
        if (from < 0 || to < 0 || len <= 0 || len > Integer.MAX_VALUE) {
            log.warn("recovery read {} invalid range [{}..{}) from {}", chunkId, from, to,
                    source.replica.endpoint());
            return null;
        }
        byte[] data = new byte[(int) len];
        int filled = 0;
        int attempts = 0;
        int maxAttempts = maxRecoveryReadAttempts(len);
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.callTimeoutMs());
        try {
            while (filled < data.length) {
                if (attempts++ >= maxAttempts) {
                    return null;
                }
                long timeoutMs = remainingTimeoutMs(deadlineNanos);
                if (timeoutMs <= 0) {
                    return null;
                }
                int want = data.length - filled;
                // READ_RECOVERY (not client READ): recovery must see the never-acked tail above the
                // donor's durable high watermark — that is exactly the range it is re-proving for seal.
                try (Frame frame = readPool.get(source.replica.endpoint()).callFrame(Opcode.READ_RECOVERY,
                        new Messages.Read(chunkId, from + filled, want, namespace).encode(), null,
                        timeoutMs)) {
                    ByteBuffer h = frame.headerSlice();
                    Resp.check(h);
                    Messages.ReadResp resp = Messages.ReadResp.decode(h);
                    int payloadLength = frame.payloadLength();
                    if (resp.localEndOffset() < to || resp.durableOffset() < 0
                            || resp.durableOffset() > resp.localEndOffset()
                            || payloadLength <= 0 || payloadLength > want) {
                        return null;
                    }
                    frame.payloadSlice().get(data, filled, payloadLength);
                    filled += payloadLength;
                }
            }
            return data;
        } catch (ScpException e) {
            log.warn("recovery read {}@{} from {} failed: {}", chunkId, from + filled,
                    source.replica.endpoint(), e.getMessage());
            return null;
        } catch (RuntimeException e) {
            log.warn("recovery read {}@{} from {} returned malformed response: {}", chunkId, from + filled,
                    source.replica.endpoint(), e.toString());
            return null;
        }
    }

    private static int maxRecoveryReadAttempts(long len) {
        return Math.max(RECOVERY_READ_MIN_ATTEMPTS,
                (int) ((len + RECOVERY_READ_MIN_PROGRESS_BYTES - 1L) / RECOVERY_READ_MIN_PROGRESS_BYTES));
    }

    private static long remainingTimeoutMs(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return 0;
        }
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
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
