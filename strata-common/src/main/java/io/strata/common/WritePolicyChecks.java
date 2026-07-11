package io.strata.common;

/** Shared validation for replicated-file write policies at API, wire, and metadata boundaries. */
public final class WritePolicyChecks {
    private WritePolicyChecks() {}

    public static void validate(int replicationFactor, int ackQuorum) {
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replicationFactor must be positive: " + replicationFactor);
        }
        if (ackQuorum <= 0 || ackQuorum > replicationFactor) {
            throw new IllegalArgumentException("ackQuorum must be in 1..replicationFactor: " + ackQuorum);
        }
        if (ackQuorum <= replicationFactor / 2) {
            throw new IllegalArgumentException("ackQuorum must intersect any other quorum: "
                    + ackQuorum + " for replicationFactor " + replicationFactor);
        }
    }
}
