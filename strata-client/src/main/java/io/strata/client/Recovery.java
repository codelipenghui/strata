package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpConnectionException;
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
 * been producer-acked — i.e. it is held by an ackQuorum once known-endpoint replicas that could not
 * be fenced are counted as possible holders (§7.3 step 3 / issue #29). This preserves an acked batch
 * whose other holder is merely transport-unreachable, even when RF &gt; ackQuorum. Empty descriptor
 * endpoints, replicas reporting no installed chunk, and untrusted FENCE responses do not promote a
 * sub-quorum tail; when any such outcome leaves truncation versus promotion ambiguous, recovery
 * blocks for retry or an explicit unsafe override (issue #120).
 */
final class Recovery {
    private static final Logger log = LoggerFactory.getLogger(Recovery.class);
    static final String UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY = "strata.recovery.unsafeSealOverrideChunks";
    static final String UNSAFE_SEAL_OVERRIDE_CHUNKS_ENV = "STRATA_RECOVERY_UNSAFE_SEAL_OVERRIDE_CHUNKS";
    private static final Set<String> CONSUMED_ENV_UNSAFE_SEAL_OVERRIDES = new HashSet<>();
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
        final int recoveryEpoch;

        ReplicaState(Messages.Replica replica, Messages.FenceResp fence) {
            this.replica = replica;
            this.end = fence.localEndOffset();
            this.durable = fence.lastKnownDO();
            this.state = fence.state();
            this.recoveryEpoch = fence.persistedFenceEpoch();
        }

        int recoveryEpoch() {
            return recoveryEpoch;
        }
    }

    /** Explicit FENCE classifications; only transport-unreachable replicas retain issue #29 credit. */
    private static final class FenceOutcomes {
        final List<ReplicaState> reachable = new ArrayList<>();
        final List<Integer> transportUnreachableNodeIds = new ArrayList<>();
        final List<Integer> unresolvedDescriptorNodeIds = new ArrayList<>();
        final List<Integer> currentlyAbsentNodeIds = new ArrayList<>();
        final List<Integer> untrustedFenceNodeIds = new ArrayList<>();

        int promotionCredits() {
            return transportUnreachableNodeIds.size();
        }

        int unresolvedClassificationSlots() {
            return unresolvedDescriptorNodeIds.size()
                    + currentlyAbsentNodeIds.size()
                    + untrustedFenceNodeIds.size();
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

        // 1. fence all reachable replicas; collect their state. Keep non-reachable outcomes
        // explicit: issue #29 gives known-endpoint FENCE failures legacy possible-holder credit,
        // while issue #120 requires empty descriptors, CHUNK_NOT_FOUND, and untrusted responses to
        // remain fail-closed ambiguity instead of either disappearing or promoting a sub-quorum tail.
        FenceOutcomes fenceOutcomes = new FenceOutcomes();
        List<ReplicaState> reachable = fenceOutcomes.reachable;
        for (Messages.Replica r : chunk.replicas()) {
            if (r.endpoint().isEmpty()) {
                fenceOutcomes.unresolvedDescriptorNodeIds.add(r.nodeId());
                log.warn("fence {} cannot resolve endpoint for descriptor replica {}; retaining ambiguity",
                        chunkId, r.nodeId());
                continue;
            }
            try {
                ByteBuffer h = appendPool.get(r.endpoint()).call(Opcode.FENCE,
                        new Messages.Fence(chunkId, writerEpoch, namespace).encode(), null, config.callTimeoutMs());
                Messages.FenceResp fence = Messages.FenceResp.decode(h);
                validateFenceResp(chunkId, r, fence, writerEpoch);
                reachable.add(new ReplicaState(r, fence));
            } catch (ScpException e) {
                if (e.code() == ErrorCode.FENCED_EPOCH) {
                    throw e;
                }
                if (e.code() == ErrorCode.CHUNK_NOT_FOUND) {
                    fenceOutcomes.currentlyAbsentNodeIds.add(r.nodeId());
                    log.warn("fence {} on {} reports no installed chunk; excluding replica {} from holder "
                                    + "credit and retaining historical ambiguity",
                            chunkId, r.endpoint(), r.nodeId());
                    continue;
                }
                if (e instanceof ScpConnectionException) {
                    fenceOutcomes.transportUnreachableNodeIds.add(r.nodeId());
                    log.warn("fence {} on {} is transport-unreachable: {}; retaining issue #29 holder credit",
                            chunkId, r.endpoint(), e.getMessage());
                    continue;
                }
                fenceOutcomes.untrustedFenceNodeIds.add(r.nodeId());
                if (e.code() == ErrorCode.PRECONDITION_FAILED) {
                    log.error("fence {} on {} returned an inconsistent persisted epoch: {}",
                            chunkId, r.endpoint(), e.getMessage());
                } else {
                    log.warn("fence {} on {} failed without a trustworthy holder classification: {}",
                            chunkId, r.endpoint(), e.getMessage());
                }
            } catch (RuntimeException e) {
                fenceOutcomes.untrustedFenceNodeIds.add(r.nodeId());
                log.warn("fence {} on {} returned malformed response; retaining ambiguity: {}",
                        chunkId, r.endpoint(), e.toString());
            }
        }
        if (maySealAbandonedEmptyTail && reachable.size() < ackQuorum) {
            log.warn("seal-recovery: final tail {} has only {} reachable replica(s), below quorum {}; "
                            + "refusing to infer an empty tail from open-chunk metadata",
                    chunkId, reachable.size(), ackQuorum);
        }
        requireQuorum(chunkId, reachable, ackQuorum);

        // Known-endpoint replicas we could not fence may still hold bytes we cannot see. A batch
        // above the floor could therefore have reached ackQuorum even if fewer than ackQuorum
        // reachable replicas hold it (issue #29). Empty descriptors, CHUNK_NOT_FOUND, and untrusted
        // responses are not promotion credit; the final ambiguity gate handles them without choosing a tail.
        final int unreachableReplicas = fenceOutcomes.promotionCredits();

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
                // A sealed copy is authoritative when the remaining outcomes do not leave a higher
                // quorum-possible claim unresolved. The gate also protects upgrades from a partial
                // floor-seal produced by an older recovery attempt.
                rejectOrOverrideAmbiguousSeal(chunkId, reachable, Set.of(), fenceOutcomes,
                        true, len, ackQuorum);
                catchUp(chunkId, writerEpoch, reachable, len, ackQuorum);
                return finishSeal(chunkId, writerEpoch, len, reachable, ackQuorum);
            }
        }

        // 2. start from the durable floor and catch lagging replicas up to it before walking
        //    boundaries above it
        catchUp(chunkId, writerEpoch, reachable, p, ackQuorum);

        // 3. merge ledger boundaries above p from all reachable replicas
        TreeMap<Long, List<LedgerCandidate>> boundaries = new TreeMap<>();
        Set<ReplicaState> unverifiedAboveFloorHolders = new HashSet<>();
        for (ReplicaState rs : reachable) {
            try {
                ByteBuffer h = readPool.get(rs.replica.endpoint()).call(Opcode.READ_LEDGER,
                        new Messages.ReadLedger(chunkId, p, namespace, rs.recoveryEpoch()).encode(), null,
                        config.callTimeoutMs());
                long previousEnd = p;
                for (Messages.LedgerEntry e : Messages.ReadLedgerResp.decode(h).entries()) {
                    boundaries.computeIfAbsent(e.endOffset(), ignored -> new ArrayList<>())
                            .add(new LedgerCandidate(rs, previousEnd, e));
                    previousEnd = e.endOffset();
                }
            } catch (ScpException e) {
                if (e.code() == ErrorCode.FENCED_EPOCH) {
                    throw e;
                }
                log.warn("read ledger {} on {} failed: {}", chunkId, rs.replica.endpoint(), e.getMessage());
                markUnverifiedAboveFloorHolder(unverifiedAboveFloorHolders, rs, p);
            } catch (RuntimeException e) {
                log.warn("read ledger {} on {} returned malformed response: {}",
                        chunkId, rs.replica.endpoint(), e.toString());
                markUnverifiedAboveFloorHolder(unverifiedAboveFloorHolders, rs, p);
            }
        }

        // 4. walk forward: re-replicate every batch that an agreeing quorum can still prove.
        // At each point, prefer the farthest valid continuation; a shorter valid boundary from
        // one replica must not block a larger intact append held by a quorum of replicas.
        while (true) {
            Candidate candidate = bestContinuation(chunkId, reachable, boundaries, p, ackQuorum,
                    unreachableReplicas, unverifiedAboveFloorHolders);
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

        rejectOrOverrideAmbiguousSeal(chunkId, reachable, unverifiedAboveFloorHolders,
                fenceOutcomes, false, p, ackQuorum);
        log.info("seal-recovery: chunk {} sealing at {}", chunkId, p);
        return finishSeal(chunkId, writerEpoch, p, reachable, ackQuorum);
    }

    private record Candidate(long end, byte[] bytes) {}

    private record LedgerCandidate(ReplicaState replica, long previousEnd, Messages.LedgerEntry entry) {}

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
                                       int ackQuorum, int unreachableReplicas,
                                       Set<ReplicaState> unverifiedAboveFloorHolders) {
        // Farthest boundary first: a longer continuation that is still provable — a reachable quorum,
        // or (issue #29) a single CRC-valid copy that could still have been acked — should win over a
        // shorter one. A sub-quorum single is committed only if no reachable replica holds conflicting
        // bytes over the overlap; otherwise it is a divergent split with no quorum to resolve it, so we
        // fall back to a nearer boundary and ultimately truncate at the floor.
        for (var entry : boundaries.tailMap(p, false).descendingMap().entrySet()) {
            long end = entry.getKey();
            // The CRC(s) a batch [p, end) is allowed to have, per any reachable replica's ledger.
            Set<Integer> validCrcs = new HashSet<>();
            Set<ReplicaState> exactLedgerHolders = new HashSet<>();
            Set<ReplicaState> coveringLedgerHolders = new HashSet<>();
            for (var boundary : boundaries.tailMap(end, true).entrySet()) {
                for (LedgerCandidate ledger : boundary.getValue()) {
                    if (ledger.previousEnd() == p) {
                        coveringLedgerHolders.add(ledger.replica());
                    }
                }
            }
            for (LedgerCandidate ledger : entry.getValue()) {
                if (ledger.previousEnd() == p) {
                    validCrcs.add(ledger.entry().payloadCrc());
                    exactLedgerHolders.add(ledger.replica());
                }
            }
            if (validCrcs.isEmpty()) continue;
            Agreed agreed = agreedContinuation(chunkId, reachable, p, end, validCrcs, ackQuorum,
                    unreachableReplicas, exactLedgerHolders, coveringLedgerHolders,
                    unverifiedAboveFloorHolders);
            if (agreed == null) continue;
            Candidate candidate = new Candidate(end, agreed.bytes());
            if (agreed.quorum()) {
                return candidate; // a reachable quorum wins; the seal quorum drops any outlier (§14.6)
            }
            if (!conflictsAboveFloor(chunkId, reachable, p, candidate, unverifiedAboveFloorHolders)) {
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
                                        Candidate candidate, Set<ReplicaState> unverifiedAboveFloorHolders) {
        for (ReplicaState rs : reachable) {
            long overlapEnd = Math.min(rs.end, candidate.end());
            if (overlapEnd <= p) continue; // holds nothing above the floor within the candidate range
            byte[] held = readRange(chunkId, rs, p, overlapEnd);
            if (held == null) {
                markUnverifiedAboveFloorHolder(unverifiedAboveFloorHolders, rs, p);
                // Not positive evidence of divergence, but not agreement either: if no later
                // continuation covers this claim, the final seal must fail closed below it.
                continue;
            }
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
                                      Set<Integer> validCrcs, int ackQuorum, int unreachableReplicas,
                                      Set<ReplicaState> exactLedgerHolders,
                                      Set<ReplicaState> coveringLedgerHolders,
                                      Set<ReplicaState> unverifiedAboveFloorHolders) {
        List<CandidateCount> counts = new ArrayList<>();
        List<ReplicaState> crcInvalidLedgerHolders = new ArrayList<>();
        int unverifiedHolders = 0;
        for (ReplicaState rs : reachable) {
            if (rs.end < to) continue;
            byte[] data = readRange(chunkId, rs, from, to);
            if (data == null) {
                // Null includes transient failures and short responses; both retain fence-time holder
                // credit and mark the replica unverified, so recovery never seals below a claimed end
                // that it could not read.
                unverifiedHolders++;
                markUnverifiedAboveFloorHolder(unverifiedAboveFloorHolders, rs, from);
                continue;
            }
            if (!validCrcs.contains(Crc.of(data))) {
                if (exactLedgerHolders.contains(rs) || !coveringLedgerHolders.contains(rs)) {
                    log.warn("recovery read {} range [{}..{}) from {} mismatched all viable ledger CRC candidates",
                            chunkId, from, to, rs.replica.endpoint());
                    crcInvalidLedgerHolders.add(rs);
                }
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
        int strongestValidCount = 0;
        for (CandidateCount count : counts) {
            strongestValidCount = Math.max(strongestValidCount, count.count);
        }
        // Counting other-valued valid holders over-approximates ack possibility on purpose:
        // over-marking aborts for retry, under-marking can lose producer-acked data.
        if (strongestValidCount + crcInvalidLedgerHolders.size() + unverifiedHolders + unreachableReplicas
                >= ackQuorum) {
            for (ReplicaState rs : crcInvalidLedgerHolders) {
                markUnverifiedAboveFloorHolder(unverifiedAboveFloorHolders, rs, from);
            }
        }
        if (counts.size() == 1
                && counts.get(0).count + unreachableReplicas + unverifiedHolders >= ackQuorum) {
            return new Agreed(counts.get(0).bytes, false);
        }
        return null;
    }

    private record Agreed(byte[] bytes, boolean quorum) {}

    private static void markUnverifiedAboveFloorHolder(Set<ReplicaState> unverifiedAboveFloorHolders,
                                                       ReplicaState rs, long floor) {
        if (rs.end > floor) {
            unverifiedAboveFloorHolders.add(rs);
        }
    }

    /**
     * Issue #84/#102/#120 fail-closed gate. A holder whose fence response claimed bytes above the final
     * seal point may still contain producer-acked data; losing those bytes by floor-sealing is
     * permanent, while aborting leaves the chunk OPEN for a later recovery retry. This check
     * intentionally uses the final seal point, not the floor at marking time, so a holder is
     * forgiven when a later accepted continuation covers its claim.
     *
     * <p>An empty descriptor endpoint, FENCE {@code CHUNK_NOT_FOUND}, or untrusted FENCE response is
     * never agreement for promoting a sub-quorum tail. None proves that the replica never acknowledged
     * bytes before its registry record, installed handle, local data, or trustworthy response became
     * unavailable, though. If those unresolved slots plus the remaining possible holders could have
     * formed {@code ackQuorum} above the chosen seal point, recovery blocks instead of choosing
     * truncation or promotion.
     *
     * <p>Issue #102 adds the explicit escape hatch we can make without pretending the bytes were
     * proven: an operator may name one exact namespace/chunk and force recovery to evict unreadable
     * blockers and/or accept the unresolved classification, then seal the remaining quorum at the
     * verified point. The matching token is consumed after one use. That is intentionally per-chunk
     * and loud because it can discard bytes a blocker or unresolved replica may have acknowledged.
     */
    private void rejectOrOverrideAmbiguousSeal(ChunkId chunkId, List<ReplicaState> reachable,
                                               Set<ReplicaState> unverifiedAboveFloorHolders,
                                               FenceOutcomes fenceOutcomes,
                                               boolean sealedFastPath, long sealPoint, int ackQuorum) {
        List<ReplicaState> blockers = new ArrayList<>();
        for (ReplicaState rs : unverifiedAboveFloorHolders) {
            if (rs.end > sealPoint) {
                blockers.add(rs);
            }
        }

        int reachableClaimsAboveSealPoint = 0;
        for (ReplicaState rs : reachable) {
            if (rs.end > sealPoint) {
                reachableClaimsAboveSealPoint++;
            }
        }
        int unresolvedReplicaSlots = fenceOutcomes.unresolvedClassificationSlots();
        boolean unresolvedClassificationBlocks = unresolvedReplicaSlots > 0
                && unresolvedReplicaSlots + fenceOutcomes.promotionCredits() + reachableClaimsAboveSealPoint
                >= ackQuorum;
        boolean sealedHigherTailBlocks = sealedFastPath
                && reachableClaimsAboveSealPoint > 0
                && unresolvedReplicaSlots + fenceOutcomes.promotionCredits() + reachableClaimsAboveSealPoint
                >= ackQuorum;
        boolean ambiguousSealBlocks = unresolvedClassificationBlocks || sealedHigherTailBlocks;
        if (blockers.isEmpty() && !ambiguousSealBlocks) {
            return;
        }

        String overrideKey = unsafeSealOverrideKey(chunkId);
        if (!consumeUnsafeSealOverride(overrideKey)) {
            for (ReplicaState rs : blockers) {
                log.warn("seal-recovery: chunk {} blocked before seal at {} because replica {} claimed "
                                + "unverified end {}; override key {}",
                        chunkId, sealPoint, rs.replica.nodeId(), rs.end, overrideKey);
            }
            if (unresolvedClassificationBlocks) {
                log.warn("seal-recovery: chunk {} blocked before seal at {} because unresolved replica "
                                + "classification could complete quorum {}: empty-endpoint nodes {}, "
                                + "chunk-not-found nodes {}, untrusted-fence nodes {}, transport-unreachable "
                                + "nodes {}, reachable claims above seal point {}; override key {}",
                        chunkId, sealPoint, ackQuorum, fenceOutcomes.unresolvedDescriptorNodeIds,
                        fenceOutcomes.currentlyAbsentNodeIds, fenceOutcomes.untrustedFenceNodeIds,
                        fenceOutcomes.transportUnreachableNodeIds, reachableClaimsAboveSealPoint, overrideKey);
            }
            if (sealedHigherTailBlocks) {
                log.warn("seal-recovery: chunk {} blocked before accepting sealed point {} because {} "
                                + "reachable replica(s) claim a higher end and, with transport-unreachable "
                                + "nodes {} plus {} unresolved slot(s), could reach quorum {}; override key {}",
                        chunkId, sealPoint, reachableClaimsAboveSealPoint,
                        fenceOutcomes.transportUnreachableNodeIds, unresolvedReplicaSlots, ackQuorum, overrideKey);
            }
            String reason;
            if (blockers.isEmpty()) {
                reason = unresolvedClassificationBlocks
                        ? "unresolved replica classification"
                        : "quorum-possible higher tail above an existing sealed point";
            } else if (unresolvedClassificationBlocks) {
                reason = "unverified above-floor holder(s) and unresolved replica classification";
            } else if (sealedHigherTailBlocks) {
                reason = "unverified above-floor holder(s) and a quorum-possible higher tail";
            } else {
                reason = "unverified above-floor holder(s)";
            }
            throw new ScpException(ErrorCode.SEAL_RECOVERY_BLOCKED,
                    "chunk " + chunkId + " has " + reason + " above seal point "
                            + sealPoint + "; set " + UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY
                            + " or " + UNSAFE_SEAL_OVERRIDE_CHUNKS_ENV + " to " + overrideKey
                            + " only to accept the potential data loss and force seal at the verified point");
        }

        for (ReplicaState rs : blockers) {
            log.error("UNSAFE seal-recovery override {}: evicting replica {} which claimed unverified end {} "
                            + "above seal point {} for chunk {}",
                    overrideKey, rs.replica.nodeId(), rs.end, sealPoint, chunkId);
        }
        if (unresolvedClassificationBlocks) {
            log.error("UNSAFE seal-recovery override {}: forcing chunk {} to seal at {} despite unresolved "
                            + "replica classification; empty-endpoint nodes {}, chunk-not-found nodes {}, "
                            + "untrusted-fence nodes {}, transport-unreachable nodes {}, reachable claims "
                            + "above seal point {}. This may truncate a producer-acked tail",
                    overrideKey, chunkId, sealPoint, fenceOutcomes.unresolvedDescriptorNodeIds,
                    fenceOutcomes.currentlyAbsentNodeIds, fenceOutcomes.untrustedFenceNodeIds,
                    fenceOutcomes.transportUnreachableNodeIds, reachableClaimsAboveSealPoint);
        }
        if (sealedHigherTailBlocks) {
            log.error("UNSAFE seal-recovery override {}: accepting sealed point {} for chunk {} despite {} "
                            + "reachable higher-end claim(s) and transport-unreachable nodes {}. This may "
                            + "truncate a producer-acked tail",
                    overrideKey, sealPoint, chunkId, reachableClaimsAboveSealPoint,
                    fenceOutcomes.transportUnreachableNodeIds);
        }
        reachable.removeAll(blockers);
        if (reachable.size() < ackQuorum) {
            throw new ScpException(ErrorCode.SEAL_RECOVERY_BLOCKED,
                    "unsafe seal-recovery override " + overrideKey + " evicted "
                            + blockers.size() + " blocking holder(s), leaving " + reachable.size()
                            + " usable replica(s), need " + ackQuorum);
        }
    }

    private String unsafeSealOverrideKey(ChunkId chunkId) {
        return namespace + ":" + chunkId;
    }

    private static boolean consumeUnsafeSealOverride(String overrideKey) {
        synchronized (CONSUMED_ENV_UNSAFE_SEAL_OVERRIDES) {
            String property = System.getProperty(UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY);
            if (overrideTokensContain(property, overrideKey)) {
                removeUnsafeSealOverridePropertyToken(property, overrideKey);
                return true;
            }

            String env = System.getenv(UNSAFE_SEAL_OVERRIDE_CHUNKS_ENV);
            if (overrideTokensContain(env, overrideKey)
                    && !CONSUMED_ENV_UNSAFE_SEAL_OVERRIDES.contains(overrideKey)) {
                CONSUMED_ENV_UNSAFE_SEAL_OVERRIDES.add(overrideKey);
                return true;
            }
            return false;
        }
    }

    private static boolean overrideTokensContain(String configured, String overrideKey) {
        if (configured == null || configured.isBlank()) {
            return false;
        }
        for (String token : configured.split("[,\\s]+")) {
            if (overrideKey.equals(token)) {
                return true;
            }
        }
        return false;
    }

    private static void removeUnsafeSealOverridePropertyToken(String configured, String overrideKey) {
        List<String> remaining = new ArrayList<>();
        for (String token : configured.split("[,\\s]+")) {
            if (!token.isBlank() && !overrideKey.equals(token)) {
                remaining.add(token);
            }
        }
        if (remaining.isEmpty()) {
            System.clearProperty(UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY);
        } else {
            System.setProperty(UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY, String.join(",", remaining));
        }
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
                        Messages.Read.recovery(chunkId, from + filled, want, namespace,
                                source.recoveryEpoch()).encode(), null,
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
            if (e.code() == ErrorCode.FENCED_EPOCH) {
                throw e;
            }
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

    private static void validateFenceResp(ChunkId chunkId, Messages.Replica replica, Messages.FenceResp fence,
                                          int recoveryEpoch) {
        if (fence.localEndOffset() < 0 || fence.lastKnownDO() < 0
                || fence.lastKnownDO() > fence.localEndOffset()) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                    "bad fence offsets from replica " + replica.nodeId() + " for " + chunkId);
        }
        if (fence.persistedFenceEpoch() != recoveryEpoch) {
            if (fence.persistedFenceEpoch() > recoveryEpoch) {
                throw new ScpException(ErrorCode.FENCED_EPOCH,
                        "recovery epoch " + recoveryEpoch + " < replica fence "
                                + fence.persistedFenceEpoch(), fence.persistedFenceEpoch());
            }
            // A correct FENCE response cannot report less than the requested epoch: fence() persists max().
            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                    "replica " + replica.nodeId() + " reported fence " + fence.persistedFenceEpoch()
                            + " for recovery epoch " + recoveryEpoch);
        }
    }

    private void appendAndVerify(ChunkId chunkId, int epoch, ReplicaState target, long durableOffset,
                                 ByteBuffer payload, long expectedEnd) {
        ByteBuffer h = appendPool.get(target.replica.endpoint()).call(Opcode.APPEND,
                Messages.Append.recovery(chunkId, epoch, target.end, durableOffset, namespace).encode(),
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
