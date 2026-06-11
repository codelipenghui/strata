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

    GroupCommitter(String name, Syncer syncer, AtomicLong forceCounter) {
        this.syncer = syncer;
        this.forceCounter = forceCounter;
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
     * Accumulation window before each force: in-flight fsyncs interfere with concurrent
     * data/ledger writes at the OS level, throttling append arrival to a trickle. A clean gap
     * lets queued writes land so each force covers a real batch. Calibrated on a shared-SSD
     * laptop (400 pipelined appends, 3 nodes): 0ms -> ~3 appends/force; 1ms -> ~6; 3ms -> ~16;
     * 5ms -> ~44 (near the single-node ceiling). The cost is a ~5ms ack-latency floor for
     * fsync mode — the right trade for a durability mode; revisit as a config on real hardware.
     */
    private static final long ACCUMULATION_NANOS = 5_000_000;

    private void run() {
        while (true) {
            long target;
            lock.lock();
            try {
                while (waiters.isEmpty() && !closed) {
                    try {
                        work.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                if (waiters.isEmpty()) {
                    return; // closed and drained
                }
            } finally {
                lock.unlock();
            }

            if (!closed) {
                java.util.concurrent.locks.LockSupport.parkNanos(ACCUMULATION_NANOS);
            }

            lock.lock();
            try {
                target = requestedEnd; // one force covers everything written so far
            } finally {
                lock.unlock();
            }

            IOException failure = null;
            try {
                syncer.force();
                forceCounter.incrementAndGet();
            } catch (IOException e) {
                failure = e;
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

    /** Final force, drain remaining waiters, stop the flusher. Idempotent. */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) return;
            closed = true;
            work.signal();
        } finally {
            lock.unlock();
        }
        try {
            flusher.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
