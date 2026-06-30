package io.strata.format;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Group commit for ack-on-fsync chunks (tech design §5.3 / §17.11): appends write to the page
 * cache and register a waiter; one flusher thread per open chunk runs force() in a loop, each
 * force covering every append written since the previous one. Ack latency becomes ~1–2 force
 * times regardless of pipeline depth, and force frequency is amortized across the whole window.
 *
 * Safety: a waiter completes only after a force that covers its end offset (data + ledger are
 * both forced — either alone is safe for recovery, both are required before acking). Force
 * failure poisons the committer: current and future waiters fail, the replica stops acking.
 *
 * Locking: ReentrantLock + Condition (virtual-thread flusher; no monitor pinning), futures
 * completed OUTSIDE the lock.
 */
final class GroupCommitter implements AutoCloseable {

    interface Syncer {
        void force() throws IOException;
    }

    private final Syncer syncer;
    private final AtomicLong forceCounter;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition work = lock.newCondition();
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>(); // end offsets are monotonic
    private final Thread flusher;

    private long requestedEnd;
    private long flushedEnd;
    private boolean closed;
    private boolean poisoned;

    private record Waiter(long endOffset, CompletableFuture<Void> future) {}

    GroupCommitter(String name, Syncer syncer, AtomicLong forceCounter,
                   long drainTimeoutMs, long minAccumulationNanos, long maxAccumulationNanos) {
        this.syncer = syncer;
        this.forceCounter = forceCounter;
        this.drainTimeoutMs = drainTimeoutMs;
        this.minAccumulationNanos = minAccumulationNanos;
        this.maxAccumulationNanos = maxAccumulationNanos;
        this.accumulationNanos = minAccumulationNanos;
        this.flusher = Thread.ofVirtual().name("group-commit-" + name).start(this::run);
    }

    /** Completes once a force covering endOffset has happened. Called after the write landed. */
    CompletableFuture<Void> awaitFlush(long endOffset) {
        lock.lock();
        try {
            if (poisoned) {
                return CompletableFuture.failedFuture(
                        new ScpException(ErrorCode.INTERNAL, "fsync failed previously; replica stopped acking"));
            }
            if (endOffset <= flushedEnd) {
                return CompletableFuture.completedFuture(null);
            }
            if (closed) {
                // raced with seal/close: the flusher is gone — fail rather than hang; the writer
                // treats it as a replica failure (its data is either covered or never acked)
                return CompletableFuture.failedFuture(
                        new ScpException(ErrorCode.CHUNK_SEALED, "committer closed at " + flushedEnd));
            }
            CompletableFuture<Void> f = new CompletableFuture<>();
            waiters.addLast(new Waiter(endOffset, f));
            requestedEnd = Math.max(requestedEnd, endOffset);
            work.signal();
            return f;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Accumulation gap before each force: in-flight fsyncs interfere with concurrent data/ledger
     * writes at the OS level, so a clean gap lets queued writes land and each force covers a real
     * batch. The gap is ADAPTIVE — it tracks the observed force duration (clamped to
     * [minAccumulationNanos, maxAccumulationNanos]). Rationale: a force amortizes a large fixed
     * cost (syscall + device flush, and under N concurrent open chunks N flushers contend so each
     * force is slow); accumulating for ~one force-time keeps the duty cycle near 50% and makes
     * the batch grow in proportion to the force cost. When forces are cheap (light load / few
     * chunks) the gap shrinks toward MIN for low ack latency; when many chunks contend it grows
     * toward MAX so a single big force replaces many small ones — measured ~3.75x fsync throughput
     * AND lower latency vs a fixed 5ms gap on a shared SSD with 24 open chunks. Convergent
     * (capped at MAX), correctness-neutral (a force still covers every in-flight append; the
     * durability invariant is unchanged).
     */
    private final long drainTimeoutMs;
    private final long minAccumulationNanos;
    private final long maxAccumulationNanos;
    private long accumulationNanos; // flusher-thread-only; adapts per force; init to minAccumulationNanos

    private void run() {
        while (true) {
            long target;
            boolean accumulate;
            lock.lock();
            try {
                while (waiters.isEmpty() && !closed) {
                    try {
                        work.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failRemainingLocked("flusher interrupted");
                        return;
                    }
                }
                if (waiters.isEmpty()) {
                    return; // closed and drained
                }
                accumulate = !closed;
            } finally {
                lock.unlock();
            }

            if (accumulate) {
                java.util.concurrent.locks.LockSupport.parkNanos(accumulationNanos);
            }

            lock.lock();
            try {
                target = requestedEnd; // one force covers everything written so far
            } finally {
                lock.unlock();
            }

            IOException failure = null;
            long forceStart = System.nanoTime();
            try {
                syncer.force();
                forceCounter.incrementAndGet();
            } catch (IOException e) {
                failure = e;
            }
            // self-tune the next accumulation gap to this force's cost (see field doc): bigger
            // forces (more chunks contending) -> longer gap -> bigger amortizing batch.
            if (failure == null) {
                accumulationNanos = Math.min(maxAccumulationNanos,
                        Math.max(minAccumulationNanos, System.nanoTime() - forceStart));
            }

            List<Waiter> done = new ArrayList<>();
            lock.lock();
            try {
                if (failure != null) {
                    poisoned = true;
                    done.addAll(waiters);
                    waiters.clear();
                } else {
                    flushedEnd = Math.max(flushedEnd, target);
                    while (!waiters.isEmpty() && waiters.peekFirst().endOffset() <= flushedEnd) {
                        done.add(waiters.pollFirst());
                    }
                }
            } finally {
                lock.unlock();
            }
            // complete outside the lock: dependents write socket responses
            for (Waiter w : done) {
                if (failure != null) {
                    w.future().completeExceptionally(
                            new ScpException(ErrorCode.INTERNAL, "fsync failed: " + failure));
                } else {
                    w.future().complete(null);
                }
            }
            if (failure != null) {
                return;
            }
        }
    }

    /** Fails every queued waiter; caller must hold {@code lock}. Future waiters fail too. */
    private void failRemainingLocked(String reason) {
        poisoned = true;
        List<Waiter> drained = new ArrayList<>(waiters);
        waiters.clear();
        // completing under the lock is acceptable here: this is a terminal path
        for (Waiter w : drained) {
            w.future().completeExceptionally(new ScpException(ErrorCode.INTERNAL, reason));
        }
    }

    /**
     * Final force, drain remaining waiters, stop the flusher. Idempotent.
     *
     * @return false if the flusher could not be confirmed terminated — the caller must NOT
     * mutate the underlying files (truncate/close/delete) while a force may still be in flight.
     */
    @Override
    public void close() {
        if (!closeAndConfirm()) {
            throw new IllegalStateException("group-commit flusher did not terminate");
        }
    }

    boolean closeAndConfirm() {
        lock.lock();
        try {
            if (closed && !flusher.isAlive()) return true;
            closed = true;
            work.signal();
        } finally {
            lock.unlock();
        }
        try {
            flusher.join(drainTimeoutMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (flusher.isAlive()) {
            // a force stuck past 10s: interrupt and give it a moment — if it STILL lives, the
            // caller must treat the chunk as failed rather than race file mutations with it
            flusher.interrupt();
            try {
                flusher.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return !flusher.isAlive();
    }

    boolean isPoisoned() {
        lock.lock();
        try {
            return poisoned;
        } finally {
            lock.unlock();
        }
    }
}
