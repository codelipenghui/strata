package io.strata.meta;

/**
 * Explicit per-namespace leadership lifecycle (tech design §4.5).
 *
 * <p>Ownership still comes from rendezvous hashing; this state describes whether the local owner has completed
 * the epoch-fenced recovery barrier and may serve the namespace.
 */
enum NamespaceLeaderState {
    STANDBY,
    RECOVERING,
    ACTIVE,
    FENCED
}
