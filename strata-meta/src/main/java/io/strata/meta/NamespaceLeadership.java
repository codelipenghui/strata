package io.strata.meta;

import io.strata.common.StrataNamespace;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Package-private view of per-namespace leadership used by repair/verify loops.
 *
 * <p>The namespace-log backend implements the real state. Other metadata-store backends use the repair
 * coordinator's local fallback locks and are treated as already active by their global leader gate.
 */
interface NamespaceLeadership {
    NamespaceLeaderState leaderState(StrataNamespace namespace);

    boolean isNamespaceActive(StrataNamespace namespace);

    long namespaceActiveSinceMs(StrataNamespace namespace);

    ReentrantLock namespaceReconcileLock(StrataNamespace namespace);
}
