package io.strata.node;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.StrataNamespace;
import io.strata.format.ChunkStore;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Node-local QoS gate for physical chunk deletion. Foreground appends/reads and background reclaim share
 * the same data files and disk, so all node delete callers must pass through one limiter.
 */
final class ChunkDeleteService {
    private final ChunkStore store;
    private final Semaphore permits;
    private final long minStartIntervalNanos;
    private final ReentrantLock pacingLock = new ReentrantLock(true);
    private final Condition pacingAdvanced = pacingLock.newCondition();
    private long nextStartNanos;

    private final AtomicInteger waiting = new AtomicInteger();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final LongAdder ok = new LongAdder();
    private final LongAdder notFound = new LongAdder();
    private final LongAdder failed = new LongAdder();

    ChunkDeleteService(ChunkStore store, int maxConcurrent, long minIntervalMs) {
        if (maxConcurrent <= 0) {
            throw new IllegalArgumentException("maxConcurrent must be positive: " + maxConcurrent);
        }
        if (minIntervalMs < 0) {
            throw new IllegalArgumentException("minIntervalMs must be non-negative: " + minIntervalMs);
        }
        this.store = java.util.Objects.requireNonNull(store, "store");
        this.permits = new Semaphore(maxConcurrent, true);
        this.minStartIntervalNanos = TimeUnit.MILLISECONDS.toNanos(minIntervalMs);
    }

    ErrorCode delete(StrataNamespace namespace, ChunkId chunkId) {
        waiting.incrementAndGet();
        boolean acquired = false;
        try {
            permits.acquire();
            acquired = true;
            waiting.decrementAndGet();
            awaitStartSlot();
            inFlight.incrementAndGet();
            try {
                ErrorCode result = store.delete(namespace, chunkId);
                record(result);
                return result;
            } finally {
                inFlight.decrementAndGet();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failed.increment();
            return ErrorCode.INTERNAL;
        } finally {
            if (!acquired) {
                waiting.decrementAndGet();
            } else {
                permits.release();
            }
        }
    }

    private void awaitStartSlot() throws InterruptedException {
        if (minStartIntervalNanos == 0) {
            return;
        }
        pacingLock.lockInterruptibly();
        try {
            while (true) {
                long now = System.nanoTime();
                long waitNanos = nextStartNanos - now;
                if (waitNanos <= 0) {
                    nextStartNanos = now + minStartIntervalNanos;
                    pacingAdvanced.signalAll();
                    return;
                }
                pacingAdvanced.awaitNanos(waitNanos);
            }
        } finally {
            pacingLock.unlock();
        }
    }

    private void record(ErrorCode result) {
        if (result == ErrorCode.OK) {
            ok.increment();
        } else if (result == ErrorCode.CHUNK_NOT_FOUND) {
            notFound.increment();
        } else {
            failed.increment();
        }
    }

    int waitingDeletes() {
        return waiting.get();
    }

    int inFlightDeletes() {
        return inFlight.get();
    }

    long okDeletes() {
        return ok.sum();
    }

    long notFoundDeletes() {
        return notFound.sum();
    }

    long failedDeletes() {
        return failed.sum();
    }
}
