package io.strata.meta;

import io.strata.common.StrataNamespace;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Package-private view of per-namespace leadership used by repair/verify loops.
 *
 * <p>The namespace-log backend implements the real user-namespace state. The system namespace has no
 * per-namespace recovery barrier because its descriptors live directly in the root store, so it is always
 * active. Other metadata-store backends use the repair coordinator's local fallback locks and are treated as
 * already active by their global leader gate.
 */
interface NamespaceLeadership {
    NamespaceLeaderState leaderState(StrataNamespace namespace);

    boolean isNamespaceActive(StrataNamespace namespace);

    /** Wall-clock activation time used only by destructive verify settle gates. */
    long namespaceActiveSinceMs(StrataNamespace namespace);

    /**
     * Current metadata-log owner epoch for this locally owned namespace. Returns 0 unless the namespace is
     * ACTIVE; callers must skip owner-fenced RPCs while a namespace is STANDBY, RECOVERING, or FENCED.
     */
    long namespaceOwnerEpoch(StrataNamespace namespace);

    /**
     * Revalidates this local owner against the consensus manifest and returns its epoch only when the
     * local repository is still the exactly-published authority. Destructive passes fail closed when this
     * throws or returns zero. Implementations without a consensus-backed authority check must not silently
     * fall back to the local ACTIVE view.
     */
    default long authoritativeOwnerEpoch(StrataNamespace namespace) throws Exception {
        throw new UnsupportedOperationException(
                "backend has no authoritative owner read; destructive confirmation unsupported");
    }

    /** Reconcile lock for a locally active user namespace; callers must not request one for inactive namespaces. */
    ReentrantLock namespaceReconcileLock(StrataNamespace namespace);
}
