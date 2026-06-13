package io.strata.common;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Bounded exponential reconnect backoff with jitter. Holds the current delay between an initial
 * and a maximum; {@link #nextMs()} advances it (doubling, capped) and returns the jittered delay
 * to sleep, while {@link #reset()} returns to the initial delay after a successful connection.
 * Not thread-safe: each owner uses its own instance from a single control thread.
 */
public final class Backoff {
    private final int initialMs;
    private final int maxMs;
    private int currentMs;

    public Backoff(int initialMs, int maxMs) {
        this.initialMs = initialMs;
        this.maxMs = maxMs;
        this.currentMs = initialMs;
    }

    public void reset() {
        currentMs = initialMs;
    }

    /** Advances the backoff (doubling, capped at max) and returns the jittered delay to sleep. */
    public int nextMs() {
        int base = currentMs;
        int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, base / 10 + 1));
        currentMs = (int) Math.min((long) maxMs, Math.max((long) base * 2, base + 1L));
        return Math.min(maxMs, base + jitter);
    }
}
