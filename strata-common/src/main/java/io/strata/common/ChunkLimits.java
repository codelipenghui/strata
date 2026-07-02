package io.strata.common;

/** Shared chunk-entry guardrails used by clients and data nodes. */
public final class ChunkLimits {
    public static final int DEFAULT_MAX_OPEN_CHUNK_LEDGER_ENTRIES = 262_144;

    /*
     * Keep the client roll threshold below the data-node hard cap: recovery and cap-skew paths
     * must have room to complete seal/catch-up without ordinary writers hitting the backstop.
     */
    public static final int DEFAULT_MAX_CLIENT_CHUNK_RECORDS =
            DEFAULT_MAX_OPEN_CHUNK_LEDGER_ENTRIES / 2;

    private ChunkLimits() {
    }
}
