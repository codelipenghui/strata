package io.strata.meta;

import io.strata.common.StrataNamespace;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Per-namespace counters for the namespace-log metadata backend, surfaced as Prometheus metrics through
 * {@code Controller} / {@code ServerMetrics}. Lock-free {@link LongAdder}s (no hot-path cost). Held on the
 * backend rather than per-repository so they survive namespace re-recovery — a repository is rebuilt on
 * every failover/restart, but the counters must not reset.
 *
 * <p>{@link #stats()} returns a per-namespace snapshot in this fixed index order:
 * <ol start="0">
 *   <li>appendRecords / <li>appendBytes — metadata mutation rate + write throughput of the log
 *   <li>readRecords / <li>readBytes — records/bytes REPLAYED from the open log during recovery (read-log)
 *   <li>compactions — snapshot + open-log roll cycles (design §10)
 *   <li>recoveries — repositories (re)opened from a published manifest (failover/restart churn, §13)
 *   <li>reacquisitions — stale-epoch meta-log re-acquisitions (ownership contention)
 *   <li>ownerChanges — cold acquisitions of a namespace by this node (ownership handoffs, §6)
 *   <li>snapshotFallbacks — CRC-bad published snapshots recovered from a previous generation plus current log
 * </ol>
 * The aggregate accessors ({@link #appendRecords()} etc.) sum across namespaces.
 */
final class NamespaceLogMetrics {
    static final int APPEND_RECORDS = 0, APPEND_BYTES = 1, READ_RECORDS = 2, READ_BYTES = 3,
            COMPACTIONS = 4, RECOVERIES = 5, REACQUISITIONS = 6, OWNER_CHANGES = 7, SNAPSHOT_FALLBACKS = 8;
    private static final int N = 9;

    private final ConcurrentHashMap<String, LongAdder[]> byNs = new ConcurrentHashMap<>();

    private LongAdder[] of(StrataNamespace ns) {
        return byNs.computeIfAbsent(ns.value(), k -> {
            LongAdder[] a = new LongAdder[N];
            for (int i = 0; i < N; i++) {
                a[i] = new LongAdder();
            }
            return a;
        });
    }

    void recordAppend(StrataNamespace ns, long bytes) {
        LongAdder[] a = of(ns);
        a[APPEND_RECORDS].increment();
        a[APPEND_BYTES].add(bytes);
    }

    /** Records/bytes replayed from the open log during recovery — the namespace's read-log throughput. */
    void recordLogRead(StrataNamespace ns, long records, long bytes) {
        LongAdder[] a = of(ns);
        a[READ_RECORDS].add(records);
        a[READ_BYTES].add(bytes);
    }

    void recordCompaction(StrataNamespace ns) {
        of(ns)[COMPACTIONS].increment();
    }

    void recordRecovery(StrataNamespace ns) {
        of(ns)[RECOVERIES].increment();
    }

    void recordSnapshotFallback(StrataNamespace ns) {
        of(ns)[SNAPSHOT_FALLBACKS].increment();
    }

    /** A fenced (stale-epoch) meta-log append forced this node to re-acquire and retry — owner churn. */
    void recordReacquire(StrataNamespace ns) {
        of(ns)[REACQUISITIONS].increment();
    }

    /** This node cold-acquired the namespace (first repository open) — an ownership handoff to this node. */
    void recordOwnerAcquired(StrataNamespace ns) {
        of(ns)[OWNER_CHANGES].increment();
    }

    /** Per-namespace snapshot keyed by namespace value; value is the N-element index order documented above. */
    Map<String, long[]> stats() {
        Map<String, long[]> out = new HashMap<>(byNs.size());
        byNs.forEach((ns, a) -> {
            long[] v = new long[N];
            for (int i = 0; i < N; i++) {
                v[i] = a[i].sum();
            }
            out.put(ns, v);
        });
        return out;
    }

    /** Namespaces with recorded activity — drives lazy per-namespace meter registration (no snapshot alloc). */
    Set<String> namespaces() {
        return byNs.keySet();
    }

    /** One per-namespace counter by index (see class doc); 0 if absent. O(1) — bound per Micrometer
     *  FunctionCounter so each scrape is a single {@code LongAdder.sum()} rather than a full-map rebuild. */
    long value(String namespace, int index) {
        LongAdder[] a = byNs.get(namespace);
        return a == null ? 0L : a[index].sum();
    }

    private long sum(int idx) {
        long total = 0;
        for (LongAdder[] a : byNs.values()) {
            total += a[idx].sum();
        }
        return total;
    }

    long appendRecords() {
        return sum(APPEND_RECORDS);
    }

    long appendBytes() {
        return sum(APPEND_BYTES);
    }

    long compactions() {
        return sum(COMPACTIONS);
    }

    long recoveries() {
        return sum(RECOVERIES);
    }

    long reacquisitions() {
        return sum(REACQUISITIONS);
    }

    long snapshotFallbacks() {
        return sum(SNAPSHOT_FALLBACKS);
    }
}
