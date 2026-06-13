package io.strata.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A process-wide hook for deterministically perturbing concurrent code at named points, used to
 * surface race conditions in tests by widening otherwise-microscopic interleaving windows.
 *
 * <p>Production code calls {@link #point(String)} at a race-prone seam. When nothing is armed the
 * call costs a single {@code volatile} read and returns — there is no allocation, no map lookup,
 * and no behavioural change. Tests {@link #arm(String, Action)} an action (a fixed {@link
 * #delay(String, long) delay}, or a rendezvous that blocks on a latch/barrier) so the thread that
 * reaches the point pauses exactly where a competing thread must interleave to trigger the bug.
 *
 * <p>An armed {@link Action} runs on the thread that hit the point. It may sleep or block; an
 * {@link InterruptedException} is caught and the interrupt flag restored (the seam must not change
 * the caller's exception contract). Unchecked exceptions thrown by an action propagate, which lets
 * a test also inject a fault — not just a delay — at the seam.
 *
 * <p>The injector is global mutable state; tests must {@link #reset()} in a {@code finally} block.
 */
public final class FailureInjector {

    /** An action run when a {@link #point(String)} fires. Receives the point name. */
    @FunctionalInterface
    public interface Action {
        void run(String point) throws InterruptedException;
    }

    // Hot-path gate: false in production, so point() returns after one volatile read. Flipped on
    // by arm()/delay() and off by reset() (and by disarm() once the last action is removed).
    private static volatile boolean armed = false;
    private static final Map<String, Action> actions = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> hits = new ConcurrentHashMap<>();

    private FailureInjector() {
    }

    /**
     * Production seam. If an action is armed for {@code name} it runs on the calling thread,
     * otherwise this returns immediately. Inert (one volatile read) when nothing is armed.
     */
    public static void point(String name) {
        if (!armed) {
            return;
        }
        Action action = actions.get(name);
        if (action == null) {
            return;
        }
        hits.computeIfAbsent(name, k -> new AtomicLong()).incrementAndGet();
        try {
            action.run(name);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Arms {@code action} at {@code name}, replacing any previous action there. */
    public static void arm(String name, Action action) {
        actions.put(name, action);
        armed = true;
    }

    /** Arms a fixed sleep of {@code millis} at {@code name}. */
    public static void delay(String name, long millis) {
        arm(name, p -> Thread.sleep(millis));
    }

    /** Removes the action at {@code name}; disarms the hot path once no actions remain. */
    public static void disarm(String name) {
        actions.remove(name);
        if (actions.isEmpty()) {
            armed = false;
        }
    }

    /** Number of times {@code name} fired with an action armed — for asserting a seam was reached. */
    public static long hits(String name) {
        AtomicLong count = hits.get(name);
        return count == null ? 0 : count.get();
    }

    /** Clears all armed actions and hit counts and disarms the hot path. Call from test teardown. */
    public static void reset() {
        actions.clear();
        hits.clear();
        armed = false;
    }
}
