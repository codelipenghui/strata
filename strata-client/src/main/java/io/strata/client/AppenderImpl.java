package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The quorum appender (tech design §5): fan-out to 3 replicas, ack to the caller at 2-of-3,
 * durable offset = second-highest replica-acked end, piggybacked on subsequent appends.
 * Single-replica failure triggers seal-and-roll (§7.2 fast path: roll IS the ensemble change);
 * a FENCED_EPOCH from any replica kills the appender permanently (§12 guarantees).
 *
 * Locking: ReentrantLock + Condition, never `synchronized` — virtual threads blocking inside a
 * monitor pin their carrier (JDK 21), and replica callbacks ARE virtual threads; pinned carriers
 * can starve every other virtual thread in the process (observed as a full-JVM stall).
 */
final class AppenderImpl implements SegmentStore.Appender {
    private static final Logger log = LoggerFactory.getLogger(AppenderImpl.class);
    private static final int OPEN_QUORUM = 2;
    /** Replica responses are dispatched off the connection reader threads. */
    private static final java.util.concurrent.Executor CALLBACKS =
            java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();

    private final MetaClient meta;
    private final NodePool pool;
    private final ClientConfig config;
    private final io.strata.common.FileId fileId;
    private final int epoch;
    private final byte fileKind;
    private final byte ackPolicy;

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
        final long[] acked = new long[3];
        final boolean[] failed = new boolean[3];
        long end;                      // chunk-local next append offset
        long durable;                  // chunk-local DO (second-highest acked)
        final ArrayDeque<Pending> pending = new ArrayDeque<>();
        boolean needRoll;

        ChunkSession(ChunkId chunkId, List<Messages.Replica> replicas) {
            this.chunkId = chunkId;
            this.replicas = replicas;
        }

        int failedCount() {
            int n = 0;
            for (boolean f : failed) if (f) n++;
            return n;
        }
    }

    private record Pending(long chunkEnd, CompletableFuture<SegmentStore.AppendAck> future) {}

    AppenderImpl(MetaClient meta, NodePool pool, ClientConfig config, io.strata.common.FileId fileId,
                 int epoch, byte fileKind, byte ackPolicy, long existingFileLength) {
        this.meta = meta;
        this.pool = pool;
        this.config = config;
        this.fileId = fileId;
        this.epoch = epoch;
        this.fileKind = fileKind;
        this.ackPolicy = ackPolicy;
        this.fileBase = existingFileLength;
    }

    @Override
    public CompletableFuture<SegmentStore.AppendAck> append(ByteBuffer data) {
        lock.lock();
        try {
            awaitNotRolling();
            throwIfDead();
            if (session == null || session.needRoll || session.end >= config.chunkRollBytes()) {
                roll();
                throwIfDead();
            }
            ChunkSession s = session;
            long base = s.end;
            int len = data.remaining();
            s.end = base + len;
            CompletableFuture<SegmentStore.AppendAck> callerFuture = new CompletableFuture<>();
            s.pending.addLast(new Pending(s.end, callerFuture));

            byte[] header = new Messages.Append(s.chunkId, epoch, base, s.durable).encode();
            for (int i = 0; i < s.replicas.size(); i++) {
                if (s.failed[i]) continue;
                final int replicaIndex = i;
                CompletableFuture<Frame> f;
                try {
                    f = pool.get(s.replicas.get(i).endpoint()).send(Opcode.APPEND, header, data.duplicate());
                } catch (ScpException e) {
                    onReplicaFailureLocked(s, replicaIndex, e);
                    continue;
                }
                // per-replica timeout: a black-holed connection must fail THIS replica (seal-and-
                // roll path), not stall the whole appender into quorum loss
                f.orTimeout(config.callTimeoutMs(), java.util.concurrent.TimeUnit.MILLISECONDS)
                        .whenCompleteAsync((frame, err) -> onReplicaResponse(s, replicaIndex, frame, err), CALLBACKS);
            }
            return callerFuture;
        } finally {
            lock.unlock();
        }
    }

    private void onReplicaResponse(ChunkSession s, int replicaIndex, Frame frame, Throwable err) {
        lock.lock();
        try {
            if (err != null) {
                onReplicaFailureLocked(s, replicaIndex, new ScpException(ErrorCode.INTERNAL, String.valueOf(err)));
                return;
            }
            try {
                ByteBuffer h = frame.headerSlice();
                Resp.check(h);
                long end = Messages.AppendResp.decode(h).endOffset();
                s.acked[replicaIndex] = Math.max(s.acked[replicaIndex], end);
                advanceDurableLocked(s);
            } catch (ScpException e) {
                if (e.code() == ErrorCode.FENCED_EPOCH) {
                    dieLocked(e);
                } else {
                    onReplicaFailureLocked(s, replicaIndex, e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void onReplicaFailureLocked(ChunkSession s, int replicaIndex, ScpException cause) {
        if (s.failed[replicaIndex]) return;
        s.failed[replicaIndex] = true;
        log.warn("replica {} ({}) failed for chunk {}: {}", replicaIndex,
                s.replicas.get(replicaIndex).endpoint(), s.chunkId, cause.getMessage());
        if (s.failedCount() >= 2) {
            dieLocked(new ScpException(ErrorCode.INTERNAL, "quorum lost on chunk " + s.chunkId + ": " + cause));
            return;
        }
        // single failure: the remaining two replicas must both ack — DO still advances; once the
        // pipeline drains we roll to a fresh replica set (roll IS the ensemble change)
        s.needRoll = true;
        advanceDurableLocked(s);
        progress.signalAll();
    }

    /** DO = second-highest acked end across replicas (failed replicas keep their frozen value). */
    private void advanceDurableLocked(ChunkSession s) {
        long a = s.acked[0], b = s.acked[1], c = s.acked[2];
        long max = Math.max(a, Math.max(b, c));
        long second = (a == max) ? Math.max(b, c)
                : (b == max) ? Math.max(a, c)
                : Math.max(a, b);
        if (second > s.durable) {
            s.durable = second;
            while (!s.pending.isEmpty() && s.pending.peekFirst().chunkEnd() <= s.durable) {
                Pending p = s.pending.pollFirst();
                p.future().complete(new SegmentStore.AppendAck(fileBase + p.chunkEnd(), fileBase + s.durable));
            }
            progress.signalAll();
        }
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
                fileBase += sealAt;
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
        int okCount = 0;
        long finalLength = -1;
        int crc = 0;
        ScpException lastErr = null;
        for (int i = 0; i < s.replicas.size(); i++) {
            if (s.failed[i]) continue;
            String endpoint = s.replicas.get(i).endpoint();
            lock.unlock();
            ByteBuffer h = null;
            ScpException err = null;
            try {
                h = pool.get(endpoint).call(Opcode.SEAL_CHUNK, header, null, config.callTimeoutMs());
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
            var resp = Messages.SealResp.decode(h);
            if (finalLength >= 0 && (resp.finalLength() != finalLength || resp.chunkCrc() != crc)) {
                dieLocked(new ScpException(ErrorCode.INTERNAL, "replica seal divergence on " + s.chunkId));
                return;
            }
            finalLength = resp.finalLength();
            crc = resp.chunkCrc();
            okCount++;
        }
        if (okCount < OPEN_QUORUM) {
            dieLocked(lastErr != null ? lastErr : new ScpException(ErrorCode.INTERNAL, "seal quorum lost"));
            return;
        }
        long fl = finalLength;
        int fcrc = crc;
        ChunkId id = s.chunkId;
        lock.unlock();
        try {
            meta.sealChunkMeta(id, epoch, fl, fcrc);
        } finally {
            lock.lock();
        }
    }

    private void openNewChunkLocked() {
        // failover resilience lives in MetaClient.call (deadline-based retry); a failure here
        // means the metadata plane stayed unreachable past the deadline
        Messages.CreateChunkResp created;
        lock.unlock();
        try {
            created = meta.createChunk(fileId, epoch);
        } catch (ScpException e) {
            lock.lock();
            dieLocked(e);
            return;
        } finally {
            if (!lock.isHeldByCurrentThread()) lock.lock();
        }
        ChunkSession s = new ChunkSession(created.chunkId(), created.replicas());
        byte[] header = new Messages.OpenChunk(created.chunkId(), epoch, ackPolicy, (byte) 0, fileKind,
                config.chunkRollBytes(), System.currentTimeMillis()).encode();
        int ok = 0;
        for (int i = 0; i < s.replicas.size(); i++) {
            String endpoint = s.replicas.get(i).endpoint();
            lock.unlock();
            ScpException err = null;
            try {
                pool.get(endpoint).call(Opcode.OPEN_CHUNK, header, null, config.callTimeoutMs());
            } catch (ScpException e) {
                err = e;
            } finally {
                lock.lock();
            }
            if (err != null) {
                s.failed[i] = true;
                log.warn("open {} on replica {} failed: {}", s.chunkId, endpoint, err.getMessage());
            } else {
                ok++;
            }
        }
        if (ok < OPEN_QUORUM) {
            dieLocked(new ScpException(ErrorCode.INTERNAL, "cannot open chunk on a quorum"));
            return;
        }
        session = s;
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

    @Override
    public long durableOffset() {
        lock.lock();
        try {
            return session == null ? fileBase : fileBase + session.durable;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SegmentStore.SealInfo seal() {
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
                    total = fileBase + sealAt;
                    fileBase = total;
                    session = null;
                }
                long t = total;
                lock.unlock();
                try {
                    meta.sealFile(fileId, t);
                } finally {
                    lock.lock();
                }
                dead = true;
                deathCause = new ScpException(ErrorCode.FILE_SEALED, "appender sealed the file");
                return new SegmentStore.SealInfo(total);
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
