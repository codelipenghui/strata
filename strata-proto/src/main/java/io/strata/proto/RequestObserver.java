package io.strata.proto;

/**
 * Notified once per served request when its response is ready, with the latency and outcome. Lets
 * the server emit per-opcode request-duration metrics WITHOUT a metrics dependency in the transport:
 * {@code ScpServer} times each request and calls this seam; the metrics layer (strata-metrics /
 * strata-server) supplies a Micrometer-backed implementation.
 */
@FunctionalInterface
public interface RequestObserver {
    /**
     * @param opcode       the request opcode name (e.g. "APPEND"), or "unknown"
     * @param namespace    the request's namespace (via {@link RequestContext}), or "-" for cluster-scope ops
     * @param durationNanos handler latency: from when the request was picked up to response-ready
     *                      (for async ops like a group-committed APPEND, this includes the fsync wait)
     * @param success      false if the handler threw or its future completed exceptionally
     */
    void observe(String opcode, String namespace, long durationNanos, boolean success);
}
