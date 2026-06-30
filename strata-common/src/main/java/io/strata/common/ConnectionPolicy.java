package io.strata.common;

/** Connection lifecycle policy for SCP clients. */
public record ConnectionPolicy(
        int connectTimeoutMs,
        int heartbeatIntervalMs,
        int heartbeatTimeoutMs,
        int idleTimeoutMs,
        int reconnectInitialBackoffMs,
        int reconnectMaxBackoffMs
) {
    public static final ConnectionPolicy DEFAULT = new ConnectionPolicy(
            EnvConfig.intEnv("STRATA_SCP_CONNECT_TIMEOUT_MS", 5_000),
            10_000,
            2_000,
            60_000,
            100,
            5_000);

    public ConnectionPolicy {
        requirePositive(connectTimeoutMs, "connectTimeoutMs");
        requirePositive(heartbeatIntervalMs, "heartbeatIntervalMs");
        requirePositive(heartbeatTimeoutMs, "heartbeatTimeoutMs");
        requirePositive(idleTimeoutMs, "idleTimeoutMs");
        requirePositive(reconnectInitialBackoffMs, "reconnectInitialBackoffMs");
        requirePositive(reconnectMaxBackoffMs, "reconnectMaxBackoffMs");
        if (reconnectMaxBackoffMs < reconnectInitialBackoffMs) {
            throw new IllegalArgumentException("reconnectMaxBackoffMs must be >= reconnectInitialBackoffMs");
        }
    }

    public ConnectionPolicy withConnectTimeoutMs(int value) {
        return new ConnectionPolicy(value, heartbeatIntervalMs, heartbeatTimeoutMs, idleTimeoutMs,
                reconnectInitialBackoffMs, reconnectMaxBackoffMs);
    }

    public ConnectionPolicy withHeartbeatIntervalMs(int value) {
        return new ConnectionPolicy(connectTimeoutMs, value, heartbeatTimeoutMs, idleTimeoutMs,
                reconnectInitialBackoffMs, reconnectMaxBackoffMs);
    }

    public ConnectionPolicy withHeartbeatTimeoutMs(int value) {
        return new ConnectionPolicy(connectTimeoutMs, heartbeatIntervalMs, value, idleTimeoutMs,
                reconnectInitialBackoffMs, reconnectMaxBackoffMs);
    }

    public ConnectionPolicy withIdleTimeoutMs(int value) {
        return new ConnectionPolicy(connectTimeoutMs, heartbeatIntervalMs, heartbeatTimeoutMs, value,
                reconnectInitialBackoffMs, reconnectMaxBackoffMs);
    }

    private static void requirePositive(int value, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " must be positive: " + value);
        }
    }
}
