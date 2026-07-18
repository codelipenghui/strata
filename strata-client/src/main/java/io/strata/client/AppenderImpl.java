package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.strata.common.Checks.checkedAdd;

/**
 * The quorum appender (tech design §5): fan-out to the file's replica set, ack to the caller
 * once the file's ack quorum has accepted the append, and piggyback that durable offset on
 * subsequent appends. When the payload stream becomes idle, a debounced zero-payload APPEND
 * publishes the final durable offset to every active replica.
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
    private static final Executor CALLBACKS =
            Executors.newVirtualThreadPerTaskExecutor();
    private static final long APPEND_BACKPRESSURE_WARN_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final int DURABLE_BEACON_MAX_ATTEMPTS = 2;

    private final ControllerClient controller;
    private final NodePool pool;
    private final ClientConfig config;
    private final FileId fileId;
    private final StrataNamespace namespace;
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
    private boolean closing;
    private boolean dead;
    private ScpException deathCause;
    private final Set<Integer> excludedPlacementNodes = new HashSet<>();

    static final class ChunkSession {
        final ChunkId chunkId;
        final List<Messages.Replica> replicas;
        // the create-op id this appender minted for this chunk incarnation; echoed on seal so the
        // controller can reject a stale seal that survived an abort + same-epoch recreate.
        final long createOpMsb;
        final long createOpLsb;
        final ManagedScpConnection[] connections;
        final long[] acked;
        final long[] publishedDurable;
        final long[] connectionGenerations;
        final boolean[] failed;
        final boolean[] beaconInFlight;
        final int[] inFlight;
        long end;                      // chunk-local next append offset
        long durable;                  // chunk-local DO (ack-quorum threshold)
        int recordCount;               // non-empty append records admitted to this chunk
        final ArrayDeque<Pending> pending = new ArrayDeque<>();
        boolean needRoll;
        long lastPayloadAdmissionNanos = System.nanoTime();
        boolean beaconCheckScheduled;
        boolean idleSealScheduled;

        ChunkSession(ChunkId chunkId, List<Messages.Replica> replicas, long createOpMsb, long createOpLsb) {
            this.chunkId = chunkId;
            this.replicas = replicas;
            this.createOpMsb = createOpMsb;
            this.createOpLsb = createOpLsb;
            this.connections = new ManagedScpConnection[replicas.size()];
            this.acked = new long[replicas.size()];
            this.publishedDurable = new long[replicas.size()];
            this.connectionGenerations = new long[replicas.size()];
            Arrays.fill(this.connectionGenerations, -1);
            this.failed = new boolean[replicas.size()];
            this.beaconInFlight = new boolean[replicas.size()];
            this.inFlight = new int[replicas.size()];
        }

        int failedCount() {
            int n = 0;
            for (boolean f : failed) if (f) n++;
            return n;
        }
    }

    record Pending(long chunkEnd, CompletableFuture<Long> future) {}

    AppenderImpl(ControllerClient controller, NodePool pool, ClientConfig config, FileId fileId,
                 StrataNamespace namespace, int epoch, Messages.WritePolicy writePolicy,
                 long existingFileLength) {
        this.controller = controller;
        this.pool = pool;
        this.config = config;
        this.fileId = fileId;
        this.namespace = namespace;
        this.epoch = epoch;
        this.replicationFactor = writePolicy.replicationFactor();
        this.ackQuorum = writePolicy.ackQuorum();
        this.fsyncOnAck = writePolicy.fsyncOnAck();
        this.fileBase = existingFileLength;
    }

    @Override
    public CompletableFuture<Long> append(ByteBuffer data) {
        lock.lock();
        try {
            awaitNotRolling();
            throwIfDead();
            int len = data.remaining();
            if (len == 0) {
                if (session == null) {
                    return CompletableFuture.completedFuture(fileBase);
                }
                if (session.end <= session.durable) {
                    scheduleIdleDurableCheckLocked(session);
                    return CompletableFuture.completedFuture(fileOffset(session.end));
                }
                CompletableFuture<Long> callerFuture = new CompletableFuture<>();
                session.pending.addLast(new Pending(session.end, callerFuture));
                return callerFuture;
            }
            while (true) {
                awaitNotRolling();
                throwIfDead();
                if (session == null || shouldRollBeforeAppend(session)) {
                    roll();
                    throwIfDead();
                    continue;
                }
                ChunkSession s = session;
                awaitAppendConnectionCapacityLocked(s);
                throwIfDead();
                if (rolling || s != session || shouldRollBeforeAppend(s)) {
                    continue;
                }
                long base = s.end;
                long newEnd = checkedAdd(base, len, "chunk offset");
                s.end = newEnd;
                s.recordCount++;
                s.lastPayloadAdmissionNanos = System.nanoTime();
                CompletableFuture<Long> callerFuture = new CompletableFuture<>();
                s.pending.addLast(new Pending(newEnd, callerFuture));

                long advertisedDurable = s.durable;
                byte[] header = new Messages.Append(s.chunkId, epoch, base, advertisedDurable, namespace).encode();
                for (int i = 0; i < s.replicas.size(); i++) {
                    if (dead) break;
                    if (s.failed[i]) continue;
                    final int replicaIndex = i;
                    CompletableFuture<Frame> f;
                    try {
                        ManagedScpConnection client = s.connections[i] != null
                                ? s.connections[i] : pool.get(s.replicas.get(i).endpoint());
                        beginReplicaRequestLocked(s, replicaIndex);
                        f = s.connectionGenerations[i] >= 0
                                ? client.sendWithTimeout(Opcode.APPEND, header, data.duplicate(), config.callTimeoutMs(),
                                        s.connectionGenerations[i])
                                : client.sendWithTimeout(Opcode.APPEND, header, data.duplicate(), config.callTimeoutMs());
                    } catch (ScpException e) {
                        completeReplicaRequestLocked(s, replicaIndex);
                        onReplicaFailureLocked(s, replicaIndex, e);
                        continue;
                    }
                    // per-replica timeout: a black-holed connection must fail THIS replica (seal-and-
                    // roll path), not stall the whole appender into quorum loss
                    f.whenCompleteAsync((frame, err) ->
                            onReplicaResponse(s, replicaIndex, newEnd, advertisedDurable, frame, err), CALLBACKS);
                }
                return callerFuture;
            }
        } finally {
            progress.signalAll();
            lock.unlock();
        }
    }

    private boolean shouldRollBeforeAppend(ChunkSession s) {
        // A freshly opened partial-quorum chunk must accept the append that opened it; rolling at
        // end==0 immediately asks metadata for another full-RF placement and can loop on NO_CAPACITY.
        return (s.needRoll && s.end > 0)
                || s.end >= config.chunkRollBytes()
                || s.recordCount >= config.maxChunkRecords();
    }

    private void onReplicaResponse(ChunkSession s, int replicaIndex, long expectedEnd, Frame frame, Throwable err) {
        onReplicaResponse(s, replicaIndex, expectedEnd, 0, frame, err);
    }

    private void onReplicaResponse(ChunkSession s, int replicaIndex, long expectedEnd,
                                   long advertisedDurable, Frame frame, Throwable err) {
        lock.lock();
        try {
            completeReplicaRequestLocked(s, replicaIndex);
            if (s != session) {
                handleStaleReplicaResponseLocked(frame, err);
                return;
            }
            if (err != null) {
                ScpException e = asScpException(err);
                if (e != null && e.code() == ErrorCode.FENCED_EPOCH) {
                    dieLocked(e);
                } else if (e != null && e.code() == ErrorCode.CHUNK_SEALED) {
                    onChunkSealedLocked(s);
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
                s.publishedDurable[replicaIndex] = Math.max(
                        s.publishedDurable[replicaIndex], advertisedDurable);
                s.acked[replicaIndex] = Math.max(s.acked[replicaIndex], end);
                advanceDurableLocked(s);
                scheduleIdleDurableCheckLocked(s);
            } catch (ScpException e) {
                if (e.code() == ErrorCode.FENCED_EPOCH) {
                    dieLocked(e);
                } else if (e.code() == ErrorCode.CHUNK_SEALED) {
                    onChunkSealedLocked(s);
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

    /**
     * Schedules at most one debounced check per chunk session. A payload admitted during the idle
     * interval does not allocate another timer: the existing check re-arms for the remaining delay.
     */
    private void scheduleIdleDurableCheckLocked(ChunkSession s) {
        if (dead || closing || rolling || s != session || s.durable <= 0 || s.beaconCheckScheduled) {
            return;
        }
        s.beaconCheckScheduled = true;
        scheduleIdleDurableCheckTask(s, remainingDurableBeaconIdleNanos(s));
    }

    private long remainingDurableBeaconIdleNanos(ChunkSession s) {
        long idleNanos = TimeUnit.MILLISECONDS.toNanos(config.durableBeaconIdleMs());
        long elapsed = Math.max(0L, System.nanoTime() - s.lastPayloadAdmissionNanos);
        return Math.max(0L, idleNanos - elapsed);
    }

    private void scheduleIdleDurableCheckTask(ChunkSession s, long delayNanos) {
        CompletableFuture.delayedExecutor(delayNanos, TimeUnit.NANOSECONDS, CALLBACKS)
                .execute(() -> onIdleDurableCheck(s));
    }

    private void onIdleDurableCheck(ChunkSession s) {
        lock.lock();
        try {
            if (dead || rolling || s != session) {
                s.beaconCheckScheduled = false;
                return;
            }
            long remaining = remainingDurableBeaconIdleNanos(s);
            if (remaining > 0) {
                scheduleIdleDurableCheckTask(s, remaining);
                return;
            }
            s.beaconCheckScheduled = false;
            if (s.durable <= 0) {
                return;
            }
            publishDurableBeaconsLocked(s, s.durable);
        } finally {
            lock.unlock();
        }
    }

    private void publishDurableBeaconsLocked(ChunkSession s, long target) {
        if (dead || rolling || target <= 0 || s != session || target != s.durable || target > s.end) {
            return;
        }
        long base = s.end;
        for (int i = 0; i < s.replicas.size(); i++) {
            if (s.failed[i] || s.beaconInFlight[i] || s.publishedDurable[i] >= target) {
                continue;
            }
            if (s.connections[i] == null || s.connectionGenerations[i] < 0) {
                // Successful production OPEN_CHUNK always pins both. Reflective unit sessions may
                // omit them; never fall back to a fresh NodePool connection for an active chunk.
                continue;
            }
            s.beaconInFlight[i] = true;
            sendDurableBeaconAttemptLocked(s, i, base, target, 1);
        }
    }

    private void sendDurableBeaconAttemptLocked(ChunkSession s, int replicaIndex,
                                                long base, long target, int attempt) {
        ManagedScpConnection connection = s.connections[replicaIndex];
        long generation = s.connectionGenerations[replicaIndex];
        byte[] header = new Messages.Append(s.chunkId, epoch, base, target, namespace).encode();
        CompletableFuture<Frame> future;
        try {
            beginReplicaRequestLocked(s, replicaIndex);
            // A beacon retry owns no caller payload and may never reconnect/replay into a new
            // OPEN_CHUNK generation. The exact connection object and generation are both pinned.
            future = connection.sendWithTimeout(
                    Opcode.APPEND, header, null, config.callTimeoutMs(), generation);
        } catch (ScpException e) {
            completeReplicaRequestLocked(s, replicaIndex);
            onDurableBeaconFailureLocked(s, replicaIndex, base, target, attempt, e);
            return;
        }
        future.whenCompleteAsync((frame, err) ->
                onDurableBeaconResponse(s, replicaIndex, base, target, attempt, frame, err), CALLBACKS);
    }

    /** Beacon replies publish replica knowledge only; they must never acknowledge payload bytes. */
    private void onDurableBeaconResponse(ChunkSession s, int replicaIndex, long base, long target,
                                         int attempt, Frame frame, Throwable err) {
        lock.lock();
        try {
            completeReplicaRequestLocked(s, replicaIndex);
            if (s != session) {
                handleStaleReplicaResponseLocked(frame, err);
                return;
            }
            if (dead) {
                s.beaconInFlight[replicaIndex] = false;
                return;
            }
            if (err != null) {
                ScpException e = asScpException(err);
                onDurableBeaconFailureLocked(s, replicaIndex, base, target, attempt,
                        e != null ? e : new ScpException(ErrorCode.INTERNAL, String.valueOf(err)));
                return;
            }
            try {
                if (frame == null) {
                    throw new ScpException(ErrorCode.INTERNAL, "null durable beacon response");
                }
                ByteBuffer h = frame.headerSlice();
                Resp.check(h);
                long end = Messages.AppendResp.decode(h).endOffset();
                if (end != base) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "replica durable beacon end " + end + " != expected " + base);
                }
                s.publishedDurable[replicaIndex] = Math.max(s.publishedDurable[replicaIndex], target);
                s.beaconInFlight[replicaIndex] = false;
                scheduleIdleDurableCheckLocked(s);
            } catch (ScpException e) {
                onDurableBeaconFailureLocked(s, replicaIndex, base, target, attempt, e);
            } catch (RuntimeException e) {
                onDurableBeaconFailureLocked(s, replicaIndex, base, target, attempt,
                        new ScpException(ErrorCode.INTERNAL, "malformed durable beacon response: " + e));
            }
        } finally {
            lock.unlock();
        }
    }

    private void onDurableBeaconFailureLocked(ChunkSession s, int replicaIndex, long base, long target,
                                              int attempt, ScpException cause) {
        if (dead || s != session) {
            s.beaconInFlight[replicaIndex] = false;
            return;
        }
        if (cause.code() == ErrorCode.FENCED_EPOCH) {
            s.beaconInFlight[replicaIndex] = false;
            dieLocked(cause);
            return;
        }
        if (cause.code() == ErrorCode.CHUNK_SEALED) {
            s.beaconInFlight[replicaIndex] = false;
            onChunkSealedLocked(s);
            scheduleIdleShortSetSealLocked(s);
            return;
        }
        // A beacon for an older target can fail after a newer payload has already been admitted.
        // It no longer says anything about the current tail, so replace it after that tail reaches
        // quorum instead of failing a healthy replica for an obsolete publication attempt.
        if (s.end != base || s.durable != target) {
            s.beaconInFlight[replicaIndex] = false;
            scheduleIdleDurableCheckLocked(s);
            progress.signalAll();
            return;
        }
        if (attempt < DURABLE_BEACON_MAX_ATTEMPTS
                && !rolling && !s.failed[replicaIndex]
                && s.end == base && s.durable == target) {
            CompletableFuture.delayedExecutor(
                            config.durableBeaconIdleMs(), TimeUnit.MILLISECONDS, CALLBACKS)
                    .execute(() -> retryDurableBeacon(s, replicaIndex, base, target, attempt + 1));
            return;
        }
        s.beaconInFlight[replicaIndex] = false;
        onReplicaFailureLocked(s, replicaIndex, cause);
        scheduleIdleShortSetSealLocked(s);
        progress.signalAll();
    }

    private void retryDurableBeacon(ChunkSession s, int replicaIndex,
                                    long base, long target, int attempt) {
        lock.lock();
        try {
            if (dead || rolling || s != session || s.failed[replicaIndex]
                    || !s.beaconInFlight[replicaIndex]
                    || s.end != base || s.durable != target) {
                s.beaconInFlight[replicaIndex] = false;
                scheduleIdleDurableCheckLocked(s);
                progress.signalAll();
                return;
            }
            ManagedScpConnection connection = s.connections[replicaIndex];
            long generation = s.connectionGenerations[replicaIndex];
            if (connection == null || generation < 0 || connection.generation() != generation) {
                s.beaconInFlight[replicaIndex] = false;
                onReplicaFailureLocked(s, replicaIndex,
                        new ScpException(ErrorCode.INTERNAL,
                                "replica connection generation changed before durable beacon retry"));
                scheduleIdleShortSetSealLocked(s);
                progress.signalAll();
                return;
            }
            sendDurableBeaconAttemptLocked(s, replicaIndex, base, target, attempt);
        } finally {
            lock.unlock();
        }
    }

    /**
     * A failed beacon cannot be replayed on a replacement connection generation. Seal the idle
     * short set instead, so metadata stops offering the stale OPEN replica to readers. A later
     * payload lazily opens the successor chunk.
     */
    private void scheduleIdleShortSetSealLocked(ChunkSession s) {
        if (dead || closing || rolling || s != session || !s.needRoll
                || s.durable != s.end || s.idleSealScheduled || !hasPinnedSealQuorum(s)) {
            return;
        }
        s.idleSealScheduled = true;
        CALLBACKS.execute(() -> sealIdleShortSet(s));
    }

    private void sealIdleShortSet(ChunkSession s) {
        lock.lock();
        try {
            s.idleSealScheduled = false;
            if (dead || closing || rolling || s != session || !s.needRoll || s.durable != s.end) {
                return;
            }
            sealCurrentSessionWithoutSuccessorLocked(s);
        } finally {
            lock.unlock();
        }
    }

    /** Seals and detaches the current session; the next non-empty append opens its successor. */
    private void sealCurrentSessionWithoutSuccessorLocked(ChunkSession s) {
        rolling = true;
        try {
            drainPendingLocked(s);
            if (dead || s != session) {
                return;
            }
            long sealAt = s.end;
            sealChunkLocked(s, sealAt);
            if (dead || s != session) {
                return;
            }
            try {
                fileBase = fileOffset(sealAt);
            } catch (ScpException e) {
                dieLocked(e);
                return;
            }
            session = null;
        } finally {
            rolling = false;
            progress.signalAll();
        }
    }

    private boolean hasDurableBeaconInFlight(ChunkSession s) {
        for (boolean inFlight : s.beaconInFlight) {
            if (inFlight) {
                return true;
            }
        }
        return false;
    }

    private boolean hasPinnedSealQuorum(ChunkSession s) {
        int pinned = 0;
        for (int i = 0; i < s.replicas.size(); i++) {
            if (!s.failed[i] && s.connections[i] != null && s.connectionGenerations[i] >= 0) {
                pinned++;
            }
        }
        return pinned >= ackQuorum;
    }

    private boolean awaitDurableBeaconsOnCloseLocked(ChunkSession s) {
        boolean interrupted = false;
        while (!dead && s == session) {
            // A previous-target beacon may already be in flight when close begins, and outstanding
            // payload callbacks may advance durable while close waits. Re-run publication after
            // every terminal callback until the latest acknowledged target owns all active sends.
            publishDurableBeaconsLocked(s, s.durable);
            if (!hasDurableBeaconInFlight(s)) {
                break;
            }
            try {
                // Each attempt has its own callTimeoutMs and retries are finite. Waiting for their
                // terminal callbacks avoids racing a duplicate outer deadline at the exact timeout
                // boundary, where close could otherwise kill the callback that must seal a short set.
                progress.await();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        return interrupted;
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
        return ScpException.rootCause(err) instanceof ScpException e ? e : null;
    }

    private void awaitAppendConnectionCapacityLocked(ChunkSession s) {
        long waitStart = System.nanoTime();
        long nextWarn = waitStart + APPEND_BACKPRESSURE_WARN_NANOS;
        while (!dead && !rolling && !hasAppendConnectionCapacity(s)) {
            try {
                progress.await(1, TimeUnit.SECONDS);
                long now = System.nanoTime();
                if (now >= nextWarn && !hasAppendConnectionCapacity(s)) {
                    log.warn("append pipeline backpressure on {} for {}ms (inFlight={})",
                            s.chunkId, (now - waitStart) / 1_000_000.0, totalInFlight(s));
                    nextWarn = now + APPEND_BACKPRESSURE_WARN_NANOS;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                dieLocked(new ScpException(ErrorCode.INTERNAL, "interrupted"));
                return;
            }
        }
    }

    private static int totalInFlight(ChunkSession s) {
        int total = 0;
        for (int n : s.inFlight) {
            total += n;
        }
        return total;
    }

    private boolean hasAppendConnectionCapacity(ChunkSession s) {
        // A live replica must receive every append for this open chunk; otherwise a later append would
        // leave a hole in that replica's chunk. Backpressure on any live replica instead of continuing
        // to enqueue into its SCP connection until the hard 1024-pending limit trips.
        int live = 0;
        for (int i = 0; i < s.replicas.size(); i++) {
            if (s.failed[i]) continue;
            live++;
            if (!replicaHasAppendCapacity(s, i)) return false;
        }
        return live >= ackQuorum;
    }

    private boolean replicaHasAppendCapacity(ChunkSession s, int replicaIndex) {
        if (s.failed[replicaIndex]) return false;
        if (s.inFlight[replicaIndex] >= config.appendReplicaInflightHighWatermark()) return false;
        ManagedScpConnection connection = s.connections[replicaIndex];
        return connection == null
                || connection.pendingCount() < config.appendConnectionPendingHighWatermark();
    }

    private void beginReplicaRequestLocked(ChunkSession s, int replicaIndex) {
        s.inFlight[replicaIndex]++;
    }

    private void completeReplicaRequestLocked(ChunkSession s, int replicaIndex) {
        if (replicaIndex >= 0 && replicaIndex < s.inFlight.length && s.inFlight[replicaIndex] > 0) {
            s.inFlight[replicaIndex]--;
            progress.signalAll();
        }
    }

    private void onReplicaFailureLocked(ChunkSession s, int replicaIndex, ScpException cause) {
        if (s.failed[replicaIndex]) return;
        s.failed[replicaIndex] = true;
        excludedPlacementNodes.add(s.replicas.get(replicaIndex).nodeId());
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

    private void onChunkSealedLocked(ChunkSession s) {
        s.needRoll = true;
        scheduleIdleShortSetSealLocked(s);
        progress.signalAll();
    }

    /** DO = ackQuorum-th highest acked end across replicas (failed replicas keep their frozen value). */
    private void advanceDurableLocked(ChunkSession s) {
        long quorumEnd = quorumAckedEnd(s.acked);
        if (quorumEnd > s.durable) {
            s.durable = quorumEnd;
            while (!s.pending.isEmpty() && s.pending.peekFirst().chunkEnd() <= s.durable) {
                Pending p = s.pending.peekFirst();
                long endOffset;
                try {
                    endOffset = fileOffset(p.chunkEnd());
                } catch (ScpException e) {
                    dieLocked(e);
                    return;
                }
                s.pending.pollFirst();
                p.future().complete(endOffset);
            }
            progress.signalAll();
        }
        scheduleIdleShortSetSealLocked(s);
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
        long waitStart = System.nanoTime();
        long deadline = waitStart + TimeUnit.MILLISECONDS.toNanos(config.callTimeoutMs());
        long nextWarn = waitStart + APPEND_BACKPRESSURE_WARN_NANOS;
        while (!s.pending.isEmpty() && !dead) {
            long now = System.nanoTime();
            long remaining = deadline - now;
            if (remaining <= 0) {
                dieLocked(new ScpException(ErrorCode.INTERNAL,
                        "timed out draining append pipeline for " + s.chunkId
                                + " pending=" + s.pending.size()
                                + " inFlight=" + totalInFlight(s)));
                return;
            }
            try {
                progress.await(Math.min(TimeUnit.SECONDS.toNanos(1), remaining), TimeUnit.NANOSECONDS);
                now = System.nanoTime();
                if (now >= nextWarn && !s.pending.isEmpty()) {
                    log.warn("append pipeline drain waiting on {} for {}ms (pending={} inFlight={})",
                            s.chunkId, (now - waitStart) / 1_000_000.0, s.pending.size(), totalInFlight(s));
                    nextWarn = now + APPEND_BACKPRESSURE_WARN_NANOS;
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
        byte[] header = new Messages.SealChunk(s.chunkId, epoch, dataLength, namespace).encode();
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
            controller.sealChunkMeta(namespace, id, epoch, fl, fcrc, sealedReplicas, s.createOpMsb, s.createOpLsb);
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
        ScpException firstFailure = null;
        ScpException lastFailure = null;
        for (int attempt = 0; attempt <= replicationFactor; attempt++) {
            OpenChunkResult result = tryOpenNewChunkLocked();
            if (result.opened()) {
                return;
            }
            if (firstFailure == null) {
                firstFailure = result.failure();
            }
            lastFailure = result.failure();
            if (dead) {
                return;
            }
            if (result.failedNodeIds().isEmpty()) {
                break;
            }
            excludedPlacementNodes.addAll(result.failedNodeIds());
        }
        ScpException failure = firstFailure != null ? firstFailure : lastFailure;
        dieLocked(failure != null ? failure
                : new ScpException(ErrorCode.INTERNAL, "cannot open chunk on a quorum"));
    }

    private OpenChunkResult tryOpenNewChunkLocked() {
        // failover resilience lives in ControllerClient.call (deadline-based retry); a failure here
        // means the metadata plane stayed unreachable past the deadline
        Messages.CreateChunkResp created;
        UUID createOp = UUID.randomUUID();
        Set<Integer> placementExclusions = Set.copyOf(excludedPlacementNodes);
        lock.unlock();
        try {
            created = controller.createChunk(namespace, fileId, epoch,
                    createOp.getMostSignificantBits(), createOp.getLeastSignificantBits(),
                    placementExclusions);
        } catch (ScpException e) {
            if (e.code() != ErrorCode.NO_CAPACITY || placementExclusions.isEmpty()) {
                lock.lock();
                dieLocked(e);
                return OpenChunkResult.failed(e, Set.of());
            }
            try {
                created = controller.createChunk(namespace, fileId, epoch,
                        createOp.getMostSignificantBits(), createOp.getLeastSignificantBits());
            } catch (ScpException retryFailure) {
                lock.lock();
                return OpenChunkResult.failed(retryFailure, Set.of());
            }
        } finally {
            if (!lock.isHeldByCurrentThread()) lock.lock();
        }
        if (dead) {
            abortCreatedChunkLocked(created.chunkId(), createOp, List.of());
            return OpenChunkResult.failed(deathCause, Set.of());
        }
        if (!validCreatedChunk(created)) {
            abortCreatedChunkLocked(created.chunkId(), createOp, List.of());
            ScpException failure = new ScpException(ErrorCode.INTERNAL,
                    "metadata returned invalid created chunk for " + created.chunkId());
            dieLocked(failure);
            return OpenChunkResult.failed(failure, Set.of());
        }
        ChunkSession s = new ChunkSession(created.chunkId(), created.replicas(),
                createOp.getMostSignificantBits(), createOp.getLeastSignificantBits());
        byte[] header = new Messages.OpenChunk(created.chunkId(), epoch, fsyncOnAck,
                config.chunkRollBytes(), System.currentTimeMillis(), namespace).encode();
        int ok = 0;
        List<Messages.Replica> opened = new ArrayList<>(s.replicas.size());
        Set<Integer> failedNodeIds = new HashSet<>();
        for (int i = 0; i < s.replicas.size(); i++) {
            String endpoint = s.replicas.get(i).endpoint();
            lock.unlock();
            ScpException err = null;
            try {
                ManagedScpConnection client = pool.get(endpoint);
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
                failedNodeIds.add(s.replicas.get(i).nodeId());
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
                return OpenChunkResult.failed(deathCause, failedNodeIds);
            }
        }
        if (ok < ackQuorum) {
            abortCreatedChunkLocked(s.chunkId, createOp, opened);
            return OpenChunkResult.failed(
                    new ScpException(ErrorCode.INTERNAL, "cannot open chunk on a quorum"), failedNodeIds);
        }
        excludedPlacementNodes.addAll(failedNodeIds);
        if (ok < s.replicas.size()) {
            s.needRoll = true;
        }
        session = s;
        return OpenChunkResult.success();
    }

    private record OpenChunkResult(boolean opened, ScpException failure, Set<Integer> failedNodeIds) {
        static OpenChunkResult success() {
            return new OpenChunkResult(true, null, Set.of());
        }

        static OpenChunkResult failed(ScpException failure, Set<Integer> failedNodeIds) {
            return new OpenChunkResult(false, failure, Set.copyOf(failedNodeIds));
        }
    }

    private boolean validCreatedChunk(Messages.CreateChunkResp created) {
        return created.writeEpoch() == epoch
                && created.chunkId().fileId().equals(fileId)
                && validReplicaSet(created.replicas());
    }

    private boolean validReplicaSet(List<Messages.Replica> replicas) {
        if (replicas.size() != replicationFactor) return false;
        HashSet<Integer> nodeIds = new HashSet<>();
        HashSet<String> endpoints = new HashSet<>();
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
            controller.abortChunkMeta(namespace, chunkId, epoch, createOp.getMostSignificantBits(), createOp.getLeastSignificantBits());
        } catch (ScpException e) {
            abortErr = e;
        } finally {
            lock.lock();
        }
        if (abortErr != null) {
            dieLocked(abortErr);
            return;
        }
        byte[] deleteHeader = new Messages.DeleteChunks(List.of(chunkId), namespace).encode();
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
        if (dead) {
            throw deathCause != null ? deathCause
                    : new ScpException(ErrorCode.INTERNAL, "appender closed");
        }
        if (closing) {
            throw new ScpException(ErrorCode.INTERNAL, "appender is closing");
        }
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
                    controller.sealFile(namespace, fileId, t);
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
        boolean interrupted = false;
        try {
            if (!dead) {
                closing = true;
                if (rolling) {
                    // close is also the escape hatch for a blocked create/seal/roll. Preserve the
                    // existing terminal behavior instead of waiting for an operation that may need
                    // this death signal in order to unwind.
                    dieLocked(new ScpException(ErrorCode.INTERNAL, "appender closed"));
                    return;
                }
                if (session != null && session.durable > 0) {
                    ChunkSession closingSession = session;
                    // Normal try-with-resources often closes immediately after append().get(). Queue
                    // the final publication behind every replica's payload on its pinned FIFO
                    // generation, and do not return while a healthy send can still be canceled by a
                    // subsequent StrataClient.close().
                    interrupted = awaitDurableBeaconsOnCloseLocked(closingSession);
                    // If publication exhausted its pinned-generation retries, remove the failed
                    // replica from the readable descriptor before abandoning the writer.
                    if (!dead && session == closingSession && closingSession.needRoll
                            && closingSession.durable == closingSession.end) {
                        sealCurrentSessionWithoutSuccessorLocked(closingSession);
                    }
                }
                dieLocked(new ScpException(ErrorCode.INTERNAL, "appender closed"));
            }
        } finally {
            lock.unlock();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
