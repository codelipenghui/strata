package io.strata.meta;

import java.util.concurrent.atomic.LongAdder;

/**
 * Process-wide counters for the namespace-log metadata backend, surfaced as Prometheus metrics through
 * {@code Controller} / {@code ServerMetrics}. Plain monotonic {@link LongAdder}s (lock-free, no
 * hot-path cost). Held on the backend rather than per-repository so they survive namespace
 * re-recovery — a repository is rebuilt on every failover/restart, but the counters must not reset.
 *
 * <p>What each one observes operationally:
 * <ul>
 *   <li>{@code appendRecords}/{@code appendBytes} — the metadata-mutation rate and write throughput of
 *       the metadata log itself ({@code rate()} of these is "metadata ops/s" and "metadata bytes/s").</li>
 *   <li>{@code compactions} — snapshot + open-log roll cycles; cadence shows how fast the open log is
 *       being truncated (design §10).</li>
 *   <li>{@code recoveries} — namespace repositories (re)opened from a published manifest; a spike means
 *       failover/restart churn (design §13).</li>
 * </ul>
 * The durability of the metadata log's own chunks is already covered by {@code strata_chunks_*} because
 * the reserved {@code strata-meta} namespace is in the repair scan — no separate gauge needed here.
 */
final class NamespaceLogMetrics {
    private final LongAdder appendRecords = new LongAdder();
    private final LongAdder appendBytes = new LongAdder();
    private final LongAdder compactions = new LongAdder();
    private final LongAdder recoveries = new LongAdder();
    private final LongAdder reacquisitions = new LongAdder();

    void recordAppend(long bytes) {
        appendRecords.increment();
        appendBytes.add(bytes);
    }

    void recordCompaction() {
        compactions.increment();
    }

    void recordRecovery() {
        recoveries.increment();
    }

    /** A fenced (stale-epoch) meta-log append forced this node to re-acquire and retry — owner churn. */
    void recordReacquire() {
        reacquisitions.increment();
    }

    long appendRecords() {
        return appendRecords.sum();
    }

    long appendBytes() {
        return appendBytes.sum();
    }

    long compactions() {
        return compactions.sum();
    }

    long recoveries() {
        return recoveries.sum();
    }

    long reacquisitions() {
        return reacquisitions.sum();
    }
}
