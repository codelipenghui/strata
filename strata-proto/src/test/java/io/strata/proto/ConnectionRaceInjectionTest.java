package io.strata.proto;

import io.strata.common.ConnectionPolicy;
import io.strata.common.ErrorCode;
import io.strata.common.FailureInjector;
import io.strata.common.ScpException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Uses {@link FailureInjector} to widen, into a deterministic window, the otherwise-microscopic
 * race in {@link ManagedScpConnection}'s idle monitor: it reads the idle/in-flight conditions
 * WITHOUT the lock, then closes the connection under the lock. A call that acquires the live
 * client in between must not have it closed out from under it.
 */
class ConnectionRaceInjectionTest {

    @AfterEach
    void clearInjector() {
        FailureInjector.reset();
    }

    @Test
    void idleMonitorDoesNotCloseConnectionRacingWithAcquire() throws Exception {
        CountDownLatch monitorAtClosePoint = new CountDownLatch(1);
        CountDownLatch proceedToClose = new CountDownLatch(1);
        CountDownLatch monitorPassedSection = new CountDownLatch(1);
        CountDownLatch serverGotRacingPing = new CountDownLatch(1);
        CountDownLatch serverMayRespond = new CountDownLatch(1);

        // Park the idle monitor exactly between its (unlocked) idle-condition check and the close,
        // and only release it once a racing application call has acquired the live client.
        FailureInjector.arm("scp.monitor.idleClose.beforeLock", p -> {
            monitorAtClosePoint.countDown();
            proceedToClose.await();
        });
        // Signalled once the monitor has finished its (now lock-guarded) close-or-skip decision.
        FailureInjector.arm("scp.monitor.idleClose.afterSection", p -> monitorPassedSection.countDown());

        ScpServer.Handler handler = req -> {
            if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                // hold the racing ping in flight (the in-flight counter stays > 0) while the
                // monitor runs its close decision
                serverGotRacingPing.countDown();
                try {
                    serverMayRespond.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
        };

        try (ScpServer server = new ScpServer(0, 1, 0, 0, handler);
             ManagedScpConnection conn = idleEvictingConnection("127.0.0.1:" + server.port())) {

            // The monitor finds the connection idle (no call has been made yet) and parks before
            // the close, having decided — on the unlocked snapshot — that it is safe to evict.
            assertTrue(monitorAtClosePoint.await(3, TimeUnit.SECONDS), "monitor never reached idle-close");

            AtomicReference<Throwable> callFailure = new AtomicReference<>();
            Thread caller = new Thread(() -> {
                try {
                    conn.call(Opcode.PING, emptyHeader(), null, 2_000);
                } catch (Throwable t) {
                    callFailure.set(t);
                }
            }, "racing-caller");
            caller.start();

            // The racing call has acquired the (newly created) live client and its ping is parked
            // in the server handler — exactly the in-flight state the monitor must not destroy.
            assertTrue(serverGotRacingPing.await(3, TimeUnit.SECONDS), "racing ping never reached server");

            // Let the monitor run its close decision to completion against this in-flight call.
            proceedToClose.countDown();
            assertTrue(monitorPassedSection.await(3, TimeUnit.SECONDS), "monitor never finished close section");

            // Release the response and confirm the in-flight call survived the monitor's decision.
            serverMayRespond.countDown();
            caller.join(TimeUnit.SECONDS.toMillis(3));
            assertNull(callFailure.get(),
                    "idle monitor closed the connection out from under an in-flight call: "
                            + callFailure.get());
        }
    }

    /**
     * Many callers hammer one connection while the heartbeat monitor runs on a short interval, so
     * application threads and the monitor contend for the lock that now also guards Backoff. Guards
     * against a deadlock or lost-progress regression from routing the monitor's backoff access
     * through that lock (the Backoff data-race fix).
     */
    @Test
    void concurrentCallersWithActiveMonitorDoNotDeadlock() throws Exception {
        ScpServer.Handler handler = req -> {
            if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
        };
        int threads = 16;
        int callsPerThread = 50;
        // heartbeat ON with a short interval keeps the monitor actively contending for the lock;
        // a generous idle timeout keeps it from evicting mid-test
        ConnectionPolicy policy = new ConnectionPolicy(1_000, 5, 200, 5_000, 5, 50);
        try (ScpServer server = new ScpServer(0, 1, 0, 0, handler);
             ManagedScpConnection conn = new ManagedScpConnection(
                     List.of("127.0.0.1:" + server.port()), policy, ScpClient.KIND_TOOL,
                     "stress-test", "test endpoint", false, true)) {
            AtomicInteger ok = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch start = new CountDownLatch(1);
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                Thread w = new Thread(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < callsPerThread; i++) {
                            conn.call(Opcode.PING, emptyHeader(), null, 2_000);
                            ok.incrementAndGet();
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                }, "stress-caller-" + t);
                w.start();
                workers.add(w);
            }
            start.countDown();
            for (Thread w : workers) {
                w.join(TimeUnit.SECONDS.toMillis(20));
                assertFalse(w.isAlive(), "caller thread did not finish — possible deadlock");
            }
            assertNull(failure.get(), "concurrent call failed: " + failure.get());
            assertEquals(threads * callsPerThread, ok.get());
        }
    }

    private static ManagedScpConnection idleEvictingConnection(String endpoint) {
        // tiny idle timeout so the monitor wants to evict almost immediately; heartbeat OFF
        // (genericHeartbeat=false, last arg) so the only PING is the application's racing one
        ConnectionPolicy policy = new ConnectionPolicy(500, 20, 50, 30, 20, 50);
        return new ManagedScpConnection(List.of(endpoint), policy, ScpClient.KIND_TOOL,
                "race-test", "test endpoint", false, false);
    }

    private static byte[] emptyHeader() {
        BufWriter w = new BufWriter(4);
        w.noTags();
        return w.toBytes();
    }
}
