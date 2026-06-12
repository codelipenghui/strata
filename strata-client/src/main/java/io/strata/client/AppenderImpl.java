package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.strata.common.Checks.checkedAdd;

/**
 * The quorum appender (tech design §5): fan-out to the file's replica set, ack to the caller
 * once the file's ack quorum has accepted the append, and piggyback that durable offset on
 * subsequent appends.
 * Replica failure triggers seal-and-roll while the remaining replicas can still satisfy quorum
 * (§7.2 fast path: roll IS the ensemble change);
 * a FENCED_EPOCH from any replica kills the appender permanently (§12 guarantees).
 *
 * Locking: ReentrantLock + Condition, never `synchronized` — virtual threads blocking inside a
 * monitor pin their carrier (JDK 21), and replica callbacks ARE virtual threads; pinned carriers
 * can starve every other virtual thread in the process (observed as a full-JVM stall).
 */
final class AppenderImpl implements StrataFile.Appender {
    private static final Logger log = LoggerFactory.getLogger(AppenderImpl.class);
    /** Replica responses are dispatched off transport event-loop threads. */
    private static final java.util.concurrent.Executor CALLBACKS =
            java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();

    private final MetaClient meta;
    private final NodePool pool;
    private final ClientConfig config;
    private final io.strata.common.FileId fileId;
    private final int epoch;
    private final int replicationFactor;
    private final int ackQuorum;
    private final boolean fsyncOnAck;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition progress = lock.newCondition();

    // guarded by `lock`
    private ChunkSession session;
    private long fileBase;            // file-logical offset where the current chunk starts
    private boolean rolling;
    private boolean dead;
    private ScpException deathCause;

    private static final class ChunkSession {
        final ChunkId chunkId;
        final List<Messages.Replica> replicas;
        final ManagedScpConnection[] connections;
        final long[] acked;
        final long[] connectionGenerations;
        final boolean[] failed;
        long end;                      // chunk-local next append offset
        long durable;                  // chunk-local DO (ack-quorum threshold)
        final ArrayDeque<Pending> pending = new ArrayDeque<>();
        boolean needRoll;

        ChunkSession(ChunkId chunkId, List<Messages.Replica> replicas) {
            this.chunkId = chunkId;
            this.replicas = replicas;
            this.connections = new ManagedScpConnection[replicas.size()];
            this.acked = new long[replicas.size()];
            this.connectionGenerations = new long[replicas.size()];
            Arrays.fill(this.connectionGenerations, -1);
            this.failed = new boolean[replicas.size()];
        }

        int failedCount() {
            int n = 0;
            for (boolean f : failed) if (f) n++;
            return n;
        }
    }

    private record Pending(long chunkEnd, CompletableFuture<StrataFile.AppendAck> future) {}

    AppenderImpl(MetaClient meta, NodePool pool, ClientConfig config, io.strata.common.FileId fileId,
                 int epoch, Messages.WritePolicy writePolicy, long existingFileLength) {
        this.meta = meta;
        this.pool = pool;
        this.config = config;
        this.fileId = fileId;
        this.epoch = epoch;
        this.replicationFactor = writePolicy.replicationFactor();
        this.ackQuorum = writePolicy.ackQuorum();
        this.fsyncOnAck = writePolicy.fsyncOnAck();
        this.fileBase = existingFileLength;
    }

    @Override
    public CompletableFuture<StrataFile.AppendAck> append(ByteBuffer data) {
        lock.lock();
        try {
            awaitNotRolling();
            throwIfDead();
            int len = data.remaining();
            if (len == 0) {
                if (session == null) {
                    return CompletableFuture.completedFuture(new StrataFile.AppendAck(fileBase, fileBase));
                }
                if (session.end <= session.durable) {
                    return CompletableFuture.completedFuture(
                            new StrataFile.AppendAck(fileOffset(session.end), fileOffset(session.durable)));
                }
                CompletableFuture<StrataFile.AppendAck> callerFuture = new CompletableFuture<>();
                session.pending.addLast(new Pending(session.end, callerFuture));
                return callerFuture;
            }
            if (session == null || session.needRoll || session.end >= config.chunkRollBytes()) {
                roll();
                throwIfDead();
            }
            ChunkSession s = session;
            long base = s.end;
            long newEnd = checkedAdd(base, len, "chunk offset");
            s.end = newEnd;
            CompletableFuture<StrataFile.AppendAck> callerFuture = new CompletableFuture<>();
            s.pending.addLast(new Pending(newEnd, callerFuture));

            byte[] header = new Messages.Append(s.chunkId, epoch, base, s.durable).encode();
            for (int i = 0; i < s.replicas.size(); i++) {
                if (dead) break;
                if (s.failed[i]) continue;
                final int replicaIndex = i;
                CompletableFuture<Frame> f;
                try {
                    ManagedScpConnection client = s.connections[i] != null
                            ? s.connections[i] : pool.get(s.replicas.get(i).endpoint());
                    f = s.connectionGenerations[i] >= 0
                            ? client.sendWithTimeout(Opcode.APPEND, header, data.duplicate(), config.callTimeoutMs(),
                                    s.connectionGenerations[i])
                            : client.sendWithTimeout(Opcode.APPEND, header, data.duplicate(), config.callTimeoutMs());
                } catch (ScpException e) {
                    onReplicaFailureLocked(s, replicaIndex, e);
                    continue;
                }
                // per-replica timeout: a black-holed connection must fail THIS replica (seal-and-
                // roll path), not stall the whole appender into quorum loss
                f.whenCompleteAsync((frame, err) -> onReplicaResponse(s, replicaIndex, newEnd, frame, err), CALLBACKS);
            }
            return callerFuture;
        } finally {
            lock.unlock();
        }
    }

    private void onReplicaResponse(ChunkSession s, int replicaIndex, long expectedEnd, Frame frame, Throwable err) {
        lock.lock();
        try {
            if (s != session) {
                handleStaleReplicaResponseLocked(frame, err);
                return;
            }
            if (err != null) {
                ScpException e = asScpException(err);
                if (e != null && e.code() == ErrorCode.FENCED_EPOCH) {
                    dieLocked(e);
                } else {
                    onReplicaFailureLocked(s, replicaIndex, e != null ? e
                            : new ScpException(ErrorCode.INTERNAL, String.valueOf(err)));
                }
                return;
            }
            try {
                if (frame == null) {
                    throw new ScpException(ErrorCode.INTERNAL, "null append response");
                }
                ByteBuffer h = frame.headerSlice();
                Resp.check(h);
                long end = Messages.AppendResp.decode(h).endOffset();
                if (end != expectedEnd) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "replica append end " + end + " != expected " + expectedEnd);
                }
                s.acked[replicaIndex] = Math.max(s.acked[replicaIndex], end);
                advanceDurableLocked(s);
            } catch (ScpException e) {
                if (e.code() == ErrorCode.FENCED_EPOCH) {
                    dieLocked(e);
                } else {
                    onReplicaFailureLocked(s, replicaIndex, e);
                }
            } catch (RuntimeException e) {
                onReplicaFailureLocked(s, replicaIndex,
                        new ScpException(ErrorCode.INTERNAL, "malformed append response: " + e));
            }
        } finally {
            lock.unlock();
        }
    }

    private void handleStaleReplicaResponseLocked(Frame frame, Throwable err) {
        if (dead) return;
        ScpException error = asScpException(err);
        if (error != null && error.code() == ErrorCode.FENCED_EPOCH) {
            dieLocked(error);
            return;
        }
        if (frame == null) return;
        try {
            Resp.check(frame.headerSlice());
        } catch (ScpException e) {
            if (e.code() == ErrorCode.FENCED_EPOCH) {
                dieLocked(e);
            }
        } catch (RuntimeException ignored) {
            // Stale malformed responses are already detached from caller state.
        }
    }

    private static ScpException asScpException(Throwable err) {
        Throwable t = err;
        while (t instanceof java.util.concurrent.CompletionException && t.getCause() != null) {
            t = t.getCause();
        }
        return t instanceof ScpException e ? e : null;
    }

    private void onReplicaFailureLocked(ChunkSession s, int replicaIndex, ScpException cause) {
        if (s.failed[replicaIndex]) return;
        s.failed[replicaIndex] = true;
        log.warn("replica {} ({}) failed for chunk {}: {}", replicaIndex,
                s.replicas.get(replicaIndex).endpoint(), s.chunkId, cause.getMessage());
        if (s.replicas.size() - s.failedCount() < ackQuorum) {
            dieLocked(new ScpException(ErrorCode.INTERNAL, "quorum lost on chunk " + s.chunkId + ": " + cause));
            return;
        }
        // failure leaves a short replica set; once the pipeline drains we roll to a fresh set
        // (roll IS the ensemble change)
        s.needRoll = true;
        advanceDurableLocked(s);
        progress.signalAll();
    }

    /** DO = ackQuorum-th highest acked end across replicas (failed replicas keep their frozen value). */
    private void advanceDurableLocked(ChunkSession s) {
        long quorumEnd = quorumAckedEnd(s.acked);
        if (quorumEnd > s.durable) {
            s.durable = quorumEnd;
            while (!s.pending.isEmpty() && s.pending.peekFirst().chunkEnd() <= s.durable) {
                Pending p = s.pending.peekFirst();
                StrataFile.AppendAck ack;
                try {
                    ack = new StrataFile.AppendAck(fileOffset(p.chunkEnd()), fileOffset(s.durable));
                } catch (ScpException e) {
                    dieLocked(e);
                    return;
                }
                s.pending.pollFirst();
                p.future().complete(ack);
            }
            progress.signalAll();
        }
    }

    private long quorumAckedEnd(long[] acked) {
        long[] sorted = acked.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length - ackQuorum];
    }

    /** Seals the current chunk (if any) at its fully-acked end and opens a successor. Lock held. */
    private void roll() {
        rolling = true;
        try {
            if (session != null) {
                drainPendingLocked(session);
                if (dead) return;
                long sealAt = session.end; // pipeline drained => durable == end
                sealChunkLocked(session, sealAt);
                if (dead) return;
                try {
                    fileBase = fileOffset(sealAt);
                } catch (ScpException e) {
                    dieLocked(e);
                    return;
                }
                session = null;
            }
            openNewChunkLocked();
        } finally {
            rolling = false;
            progress.signalAll();
        }
    }

    private void drainPendingLocked(ChunkSession s) {
        while (!s.pending.isEmpty() && !dead) {
            try {
                if (!progress.await(config.callTimeoutMs(), TimeUnit.MILLISECONDS)) {
                    dieLocked(new ScpException(ErrorCode.INTERNAL,
                            "timed out draining " + s.pending.size() + " pending appends on " + s.chunkId));
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                dieLocked(new ScpException(ErrorCode.INTERNAL, "interrupted"));
                return;
            }
        }
    }

    /** Network calls are made WITHOUT the lock held — blocked callbacks must never wait on us. */
    private void sealChunkLocked(ChunkSession s, long dataLength) {
        byte[] header = new Messages.SealChunk(s.chunkId, epoch, dataLength).encode();
        ScpException lastErr = null;
        SealVotes votes = new SealVotes();
        for (int i = 0; i < s.replicas.size(); i++) {
            if (s.failed[i]) continue;
            String endpoint = s.replicas.get(i).endpoint();
            lock.unlock();
            ByteBuffer h = null;
            ScpException err = null;
            try {
                ManagedScpConnection client = s.connections[i] != null
                        ? s.connections[i] : pool.get(endpoint);
                h = s.connectionGenerations[i] >= 0
                        ? client.callWithGeneration(Opcode.SEAL_CHUNK, header, null, config.callTimeoutMs(),
                                s.connectionGenerations[i]).header()
                        : client.call(Opcode.SEAL_CHUNK, header, null, config.callTimeoutMs());
            } catch (ScpException e) {
                err = e;
            } finally {
                lock.lock();
            }
            if (err != null) {
                lastErr = err;
                if (err.code() == ErrorCode.FENCED_EPOCH) {
                    dieLocked(err);
                    return;
                }
                continue;
            }
            Messages.SealResp resp;
            try {
                resp = Messages.SealResp.decode(h);
            } catch (RuntimeException e) {
                lastErr = new ScpException(ErrorCode.INTERNAL, "malformed seal response: " + e);
                log.warn("seal {} on replica {} returned malformed response: {}",
                        s.chunkId, endpoint, e.toString());
                continue;
            }
            if (resp.finalLength() != dataLength) {
                lastErr = new ScpException(ErrorCode.CORRUPT_CHUNK,
                        "replica sealed at " + resp.finalLength() + " != requested " + dataLength);
                log.warn("seal {} on replica {} returned bad final length: {}",
                        s.chunkId, endpoint, lastErr.getMessage());
                continue;
            }
            votes.add(resp.finalLength(), resp.chunkCrc(), s.replicas.get(i).nodeId());
        }
        int okCount = votes.total();
        if (okCount < ackQuorum) {
            dieLocked(lastErr != null ? lastErr : new ScpException(ErrorCode.INTERNAL, "seal quorum lost"));
            return;
        }
        Map.Entry<SealVotes.Key, List<Integer>> quorum = votes.best(ackQuorum);
        if (quorum == null) {
            dieLocked(new ScpException(ErrorCode.INTERNAL, "replica seal divergence on " + s.chunkId));
            return;
        }
        if (votes.divergent()) {
            log.warn("replica seal divergence on {} — committing agreeing quorum {} of {} successful seals",
                    s.chunkId, quorum.getValue().size(), okCount);
        }
        long fl = quorum.getKey().finalLength();
        int fcrc = quorum.getKey().crc();
        List<Integer> sealedReplicas = List.copyOf(quorum.getValue());
        ChunkId id = s.chunkId;
        ScpException metaFailure = null;
        lock.unlock();
        try {
            // commit only the replicas that ACTUALLY sealed: a failed/skipped replica left in a
            // SEALED descriptor would serve short reads forever (alive, so repair never fires);
            // the under-replication scan add-repairs the descriptor back to RF afterwards
            meta.sealChunkMeta(id, epoch, fl, fcrc, sealedReplicas);
        } catch (ScpException e) {
            metaFailure = e; // handled under the lock below
        } finally {
            lock.lock();
        }
        if (metaFailure != null) {
            // replicas are sealed but metadata isn't: the session is unusable — die cleanly with
            // the real cause instead of leaking a half-sealed session into the next append
            // (recoverAndSeal commits the metadata idempotently later)
            dieLocked(metaFailure);
        }
    }

    private void openNewChunkLocked() {
        // failover resilience lives in MetaClient.call (deadline-based retry); a failure here
        // means the metadata plane stayed unreachable past the deadline
        Messages.CreateChunkResp created;
        UUID createOp = UUID.randomUUID();
        lock.unlock();
        try {
            created = meta.createChunk(fileId, epoch,
                    createOp.getMostSignificantBits(), createOp.getLeastSignificantBits());
        } catch (ScpException e) {
            lock.lock();
            dieLocked(e);
            return;
        } finally {
            if (!lock.isHeldByCurrentThread()) lock.lock();
        }
        if (dead) {
            abortCreatedChunkLocked(created.chunkId(), createOp, List.of());
            return;
        }
        if (!validCreatedChunk(created)) {
            abortCreatedChunkLocked(created.chunkId(), createOp, List.of());
            dieLocked(new ScpException(ErrorCode.INTERNAL,
                    "metadata returned invalid created chunk for " + created.chunkId()));
            return;
        }
        ChunkSession s = new ChunkSession(created.chunkId(), created.replicas());
        byte[] header = new Messages.OpenChunk(created.chunkId(), epoch, fsyncOnAck,
                config.chunkRollBytes(), System.currentTimeMillis()).encode();
        int ok = 0;
        List<Messages.Replica> opened = new ArrayList<>(s.replicas.size());
        for (int i = 0; i < s.replicas.size(); i++) {
            String endpoint = s.replicas.get(i).endpoint();
            lock.unlock();
            ScpException err = null;
            try {
                ManagedScpConnection client = pool.pin(endpoint);
                ManagedScpConnection.CallResult openedResult =
                        client.callWithGeneration(Opcode.OPEN_CHUNK, header, null, config.callTimeoutMs());
                s.connections[i] = client;
                s.connectionGenerations[i] = openedResult.generation();
            } catch (ScpException e) {
                err = e;
            } finally {
                lock.lock();
            }
            if (err != null) {
                s.failed[i] = true;
                log.warn("open {} on replica {} failed: {}", s.chunkId, endpoint, err.getMessage());
                if (err.code() == ErrorCode.FENCED_EPOCH) {
                    dieLocked(err);
                }
            } else {
                opened.add(s.replicas.get(i));
                ok++;
            }
            if (dead) {
                abortCreatedChunkLocked(s.chunkId, createOp, opened);
                return;
            }
        }
        if (ok < ackQuorum) {
            abortCreatedChunkLocked(s.chunkId, createOp, opened);
            dieLocked(new ScpException(ErrorCode.INTERNAL, "cannot open chunk on a quorum"));
            return;
        }
        if (ok < s.replicas.size()) {
            s.needRoll = true;
        }
        session = s;
    }

    private boolean validCreatedChunk(Messages.CreateChunkResp created) {
        return created.writeEpoch() == epoch
                && created.chunkId().fileId().equals(fileId)
                && validReplicaSet(created.replicas());
    }

    private boolean validReplicaSet(List<Messages.Replica> replicas) {
        if (replicas.size() != replicationFactor) return false;
        java.util.HashSet<Integer> nodeIds = new java.util.HashSet<>();
        java.util.HashSet<String> endpoints = new java.util.HashSet<>();
        for (Messages.Replica r : replicas) {
            if (r.nodeId() <= 0) return false;
            if (r.endpoint() == null || r.endpoint().isBlank()) return false;
            if (!nodeIds.add(r.nodeId())) return false;
            if (!endpoints.add(r.endpoint())) return false;
        }
        return true;
    }

    private void abortCreatedChunkLocked(ChunkId chunkId, UUID createOp, List<Messages.Replica> opened) {
        ScpException abortErr = null;
        lock.unlock();
        try {
            meta.abortChunkMeta(chunkId, epoch, createOp.getMostSignificantBits(), createOp.getLeastSignificantBits());
        } catch (ScpException e) {
            abortErr = e;
        } finally {
            lock.lock();
        }
        if (abortErr != null) {
            dieLocked(abortErr);
            return;
        }
        byte[] deleteHeader = new Messages.DeleteChunks(List.of(chunkId)).encode();
        for (Messages.Replica r : opened) {
            lock.unlock();
            try {
                pool.get(r.endpoint()).call(Opcode.DELETE_CHUNKS, deleteHeader, null, config.callTimeoutMs());
            } catch (ScpException e) {
                log.warn("cleanup delete of aborted {} on {} failed: {}",
                        chunkId, r.endpoint(), e.getMessage());
            } finally {
                lock.lock();
            }
        }
    }

    private void awaitNotRolling() {
        while (rolling && !dead) {
            try {
                progress.await(config.callTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ScpException(ErrorCode.INTERNAL, "interrupted");
            }
        }
    }

    private void dieLocked(ScpException cause) {
        if (dead) return;
        dead = true;
        deathCause = cause;
        if (session != null) {
            for (Pending p : session.pending) {
                p.future().completeExceptionally(cause);
            }
            session.pending.clear();
        }
        progress.signalAll();
    }

    private void throwIfDead() {
        if (dead) throw deathCause != null ? deathCause
                : new ScpException(ErrorCode.INTERNAL, "appender closed");
    }

    private long fileOffset(long chunkOffset) {
        return checkedAdd(fileBase, chunkOffset, "file offset");
    }

    @Override
    public long durableOffset() {
        lock.lock();
        try {
            return session == null ? fileBase : fileOffset(session.durable);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public StrataFile.SealInfo seal() {
        lock.lock();
        try {
            awaitNotRolling();
            throwIfDead();
            rolling = true;
            try {
                long total = fileBase;
                if (session != null) {
                    drainPendingLocked(session);
                    throwIfDead();
                    long sealAt = session.end;
                    sealChunkLocked(session, sealAt);
                    throwIfDead();
                    try {
                        total = fileOffset(sealAt);
                    } catch (ScpException e) {
                        dieLocked(e);
                        throw e;
                    }
                    fileBase = total;
                    session = null;
                }
                long t = total;
                ScpException sealFileFailure = null;
                lock.unlock();
                try {
                    meta.sealFile(fileId, t);
                } catch (ScpException e) {
                    sealFileFailure = e;
                } finally {
                    lock.lock();
                }
                if (sealFileFailure != null) {
                    dieLocked(sealFileFailure);
                    throw sealFileFailure;
                }
                dead = true;
                deathCause = new ScpException(ErrorCode.FILE_SEALED, "appender sealed the file");
                return new StrataFile.SealInfo(total);
            } finally {
                rolling = false;
                progress.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (!dead) {
                dieLocked(new ScpException(ErrorCode.INTERNAL, "appender closed"));
            }
        } finally {
            lock.unlock();
        }
    }
}
