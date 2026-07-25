package io.strata.meta;

/**
 * Explicit per-namespace leadership lifecycle (tech design §4.5).
 *
 * <p>Ownership comes from the incarnation-bound persisted assignment; this state describes whether the local
 * owner has completed the exact-term, epoch-fenced recovery barrier and may serve the namespace.
 */
enum NamespaceLeaderState {
    STANDBY,
    RECOVERING,
    ACTIVE,
    FENCED
}
